use super::*;

impl Phase0JobHandler {
    pub(super) async fn handle_pipeline_ingest(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: PipelineIngestPayload = match serde_json::from_value(job.payload_json.clone())
        {
            Ok(value) => value,
            Err(err) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("invalid pipeline_ingest payload: {err}"),
                    kind: "payload".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }
        };

        let run = match self.pipeline.get_run(payload.run_id).await {
            Ok(Some(value)) => value,
            Ok(None) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("pipeline run {} not found", payload.run_id),
                    kind: "not_found".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }
            Err(err) => {
                return retryable_error(
                    format!("failed to load pipeline run {}: {err}", payload.run_id),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        if run.state != "running" {
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "run_id": run.id,
                    "skipped": format!("run is in state {}", run.state),
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        let list_root = Path::new(&self.settings.mail.mirror_root).join(&run.list_key);
        let relpaths = match discover_repo_relpaths(&list_root) {
            Ok(value) => value,
            Err(err) => {
                return retryable_error(
                    format!(
                        "failed to discover repos under {}: {err}",
                        list_root.display()
                    ),
                    "io",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        for relpath in &relpaths {
            if let Err(err) = self
                .catalog
                .ensure_repo(run.mailing_list_id, relpath, relpath)
                .await
            {
                return retryable_error(
                    format!(
                        "failed to ensure repo {} exists for list {}: {err}",
                        relpath, run.list_key
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        }

        let repos = match self.catalog.list_repos_for_list(&run.list_key).await {
            Ok(value) => order_repos_for_ingest(value),
            Err(err) => {
                return retryable_error(
                    format!("failed to list repos for list {}: {err}", run.list_key),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let repos_total = repos.len() as u64;
        let stage_started_at = Utc::now();
        let batch_size = self.settings.mail.commit_batch_size.max(1);
        let checkpoint_interval = self.settings.worker.progress_checkpoint_interval.max(1) as u64;

        let mut repos_done = 0u64;
        let mut commit_count = 0u64;
        let mut rows_written = 0u64;
        let mut bytes_read = 0u64;
        let mut parse_errors = 0u64;
        let mut non_mail_commits = 0u64;
        let mut header_rejections = 0u64;
        let mut date_rejections = 0u64;
        let mut sanitization_rejections = 0u64;
        let mut other_rejections = 0u64;
        let mut items_since_checkpoint = 0u64;

        for repo in repos {
            if let Err(err) = ctx.heartbeat().await {
                warn!(job_id = job.id, error = %err, "heartbeat update failed");
            }
            match ctx.is_cancel_requested().await {
                Ok(true) => {
                    return JobExecutionOutcome::Cancelled {
                        reason: "cancel requested".to_string(),
                        metrics: JobStoreMetrics {
                            duration_ms: started.elapsed().as_millis(),
                            rows_written,
                            bytes_read,
                            commit_count,
                            parse_errors,
                        },
                    };
                }
                Ok(false) => {}
                Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
            }

            let repo_path = list_root.join(&repo.repo_relpath);
            let since_commit_oid = match self.catalog.get_watermark(repo.id).await {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!("failed to read watermark for repo {}: {err}", repo.repo_key),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };

            let mut stream = match stream_new_commit_oid_chunks(
                &repo_path,
                since_commit_oid.as_deref(),
                batch_size,
            ) {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!("repo scan failed for {}: {err}", repo.repo_key),
                        "io",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };

            loop {
                if let Err(err) = ctx.heartbeat().await {
                    warn!(job_id = job.id, error = %err, "heartbeat update failed");
                }
                match ctx.is_cancel_requested().await {
                    Ok(true) => {
                        return JobExecutionOutcome::Cancelled {
                            reason: "cancel requested".to_string(),
                            metrics: JobStoreMetrics {
                                duration_ms: started.elapsed().as_millis(),
                                rows_written,
                                bytes_read,
                                commit_count,
                                parse_errors,
                            },
                        };
                    }
                    Ok(false) => {}
                    Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
                }

                let chunk = match stream.next_chunk() {
                    Ok(Some(value)) => value,
                    Ok(None) => break,
                    Err(err) => {
                        return retryable_error(
                            format!("repo scan failed for {}: {err}", repo.repo_key),
                            "io",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                };

                commit_count += chunk.len() as u64;
                let chunk_len = chunk.len() as u64;
                let last_commit = chunk.last().cloned();
                let workers = configured_parse_worker_count(chunk.len(), &self.settings);
                let parsed_outcome =
                    match parse_commit_rows(repo_path.clone(), chunk, workers).await {
                        Ok(value) => value,
                        Err(err) => {
                            return retryable_error(
                                format!(
                                    "failed to parse ingest batch for repo {}: {err}",
                                    repo.repo_key
                                ),
                                "io",
                                &job,
                                started.elapsed().as_millis(),
                                &self.settings,
                            );
                        }
                    };
                parse_errors += parsed_outcome.parse_errors;
                non_mail_commits += parsed_outcome.non_mail_commits;
                bytes_read += parsed_outcome.bytes_read;
                header_rejections += parsed_outcome.header_rejections;
                date_rejections += parsed_outcome.date_rejections;
                sanitization_rejections += parsed_outcome.sanitization_rejections;
                other_rejections += parsed_outcome.other_rejections;

                let batch_outcome = match self.write_ingest_rows(&repo, &parsed_outcome.rows).await
                {
                    Ok(value) => value,
                    Err(err) => {
                        return retryable_error(
                            format!(
                                "ingest batch write failed for repo {}: error={err}",
                                repo.repo_key,
                            ),
                            "db",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                };

                rows_written += batch_outcome.inserted_instances;

                if let Some(last_commit) = last_commit
                    && let Err(err) = self
                        .catalog
                        .update_watermark(repo.id, Some(&last_commit))
                        .await
                {
                    return retryable_error(
                        format!(
                            "failed to update watermark for repo {}: {err}",
                            repo.repo_key
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }

                items_since_checkpoint += chunk_len;
                if items_since_checkpoint >= checkpoint_interval {
                    items_since_checkpoint = 0;
                    let progress = serde_json::json!({
                        "stage": STAGE_INGEST,
                        "repos_total": repos_total,
                        "repos_done": repos_done,
                        "commits_processed": commit_count,
                        "rows_written": rows_written,
                        "parse_errors": parse_errors,
                        "non_mail_commits": non_mail_commits,
                        "header_rejections": header_rejections,
                        "date_rejections": date_rejections,
                        "sanitization_rejections": sanitization_rejections,
                        "other_rejections": other_rejections,
                        "bytes_read": bytes_read,
                    });
                    if let Err(err) = self.pipeline.update_run_progress(run.id, progress).await {
                        warn!(run_id = run.id, error = %err, "progress checkpoint failed");
                    }
                    info!(
                        target: "worker_job",
                        run_id = run.id,
                        list_key = %run.list_key,
                        stage = STAGE_INGEST,
                        items_processed = commit_count,
                        rows_written = rows_written,
                        parse_errors = parse_errors,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "pipeline checkpoint"
                    );
                }
            }

            repos_done += 1;
        }

        let stage_completed_at = Utc::now();
        if let Err(err) = self
            .pipeline
            .set_run_ingest_window(run.id, stage_started_at, stage_completed_at)
            .await
        {
            return retryable_error(
                format!("failed to set ingest window for run {}: {err}", run.id),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        if let Err(err) = self
            .enqueue_next_stage(run.id, &run.list_key, STAGE_THREADING)
            .await
        {
            return retryable_error(
                format!(
                    "failed to enqueue threading stage for run {}: {err}",
                    run.id
                ),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }
        if let Err(err) = self
            .pipeline
            .set_run_current_stage(run.id, STAGE_THREADING)
            .await
        {
            return retryable_error(
                format!("failed to move run {} to threading stage: {err}", run.id),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "run_id": run.id,
                "list_key": run.list_key,
                "stage": STAGE_INGEST,
                "repos_total": repos_total,
                "repos_done": repos_done,
                "commits_processed": commit_count,
                "rows_written": rows_written,
                "parse_errors": parse_errors,
                "non_mail_commits": non_mail_commits,
                "header_rejections": header_rejections,
                "date_rejections": date_rejections,
                "sanitization_rejections": sanitization_rejections,
                "other_rejections": other_rejections,
                "next_stage": STAGE_THREADING,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written,
                bytes_read,
                commit_count,
                parse_errors,
            },
        }
    }
}
