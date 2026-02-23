use super::*;

impl Phase0JobHandler {
    pub(super) async fn handle_pipeline_threading(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: PipelineThreadingPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(value) => value,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid pipeline_threading payload: {err}"),
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

        let (window_from, window_to) = match (run.ingest_window_from, run.ingest_window_to) {
            (Some(f), Some(t)) => (f, t),
            _ => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("run {} has no ingest window set", run.id),
                    kind: "pipeline_state".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }
        };

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
        if repos.is_empty() {
            return JobExecutionOutcome::Terminal {
                reason: format!("no repos found for list {}", run.list_key),
                kind: "pipeline_state".to_string(),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        let touched_repo_ids = match self
            .pipeline
            .query_ingest_window_repo_ids(run.mailing_list_id, window_from, window_to)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                return retryable_error(
                    format!(
                        "failed to query touched repos for threading stage run {}: {err}",
                        run.id
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };
        let touched_repo_set: HashSet<i64> = touched_repo_ids.into_iter().collect();

        let mut min_touched_epoch: Option<i64> = None;
        for repo in &repos {
            if !touched_repo_set.contains(&repo.id) {
                continue;
            }
            let Some(epoch) = parse_epoch_repo_relpath(&repo.repo_relpath) else {
                continue;
            };
            min_touched_epoch = Some(match min_touched_epoch {
                Some(current) => current.min(epoch),
                None => epoch,
            });
        }
        let start_epoch = min_touched_epoch.map(|value| value.saturating_sub(1));

        let windows = if touched_repo_set.is_empty() {
            Vec::new()
        } else {
            match build_threading_epoch_windows(&repos, start_epoch) {
                Ok(value) => value,
                Err(reason) => {
                    return JobExecutionOutcome::Terminal {
                        reason,
                        kind: "pipeline_state".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            }
        };

        let epoch_windows_total = windows.len() as u64;
        let mut epoch_windows_done = 0u64;
        let mut processed_windows = 0u64;
        let mut last_window_epochs: Vec<i64> = Vec::new();
        let mut anchors_scanned = 0u64;
        let mut messages_processed = 0u64;
        let mut threads_updated = 0u64;
        let mut apply_stats = ThreadingApplyStats::default();
        let mut source_messages_read = 0u64;
        let mut anchors_deduped = 0u64;

        for window in windows {
            if let Err(err) = ctx.heartbeat().await {
                warn!(job_id = job.id, error = %err, "heartbeat update failed");
            }
            match ctx.is_cancel_requested().await {
                Ok(true) => {
                    return JobExecutionOutcome::Cancelled {
                        reason: "cancel requested".to_string(),
                        metrics: JobStoreMetrics {
                            duration_ms: started.elapsed().as_millis(),
                            rows_written: apply_stats.threads_rebuilt
                                + apply_stats.nodes_written
                                + apply_stats.messages_written
                                + apply_stats.stale_threads_removed,
                            bytes_read: 0,
                            commit_count: messages_processed,
                            parse_errors: 0,
                        },
                    };
                }
                Ok(false) => {}
                Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
            }

            let anchors = match self
                .pipeline
                .query_ingest_window_message_pks_for_repo_ids(
                    run.mailing_list_id,
                    window_from,
                    window_to,
                    &window.repo_ids,
                )
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!(
                            "failed to query messages for threading stage run {}: {err}",
                            run.id
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };

            // Missing-reference fallback is handled inside apply_threading_for_anchors:
            // reference chains are resolved through message_id_map, and impacted existing
            // thread members are loaded from DB before rebuilding components.
            let outcome = if anchors.is_empty() {
                ThreadingWindowOutcome::default()
            } else {
                match self
                    .apply_threading_for_anchors(run.mailing_list_id, &run.list_key, anchors)
                    .await
                {
                    Ok(value) => value,
                    Err(err) => {
                        return retryable_error(
                            format!("failed to apply threading stage for run {}: {err}", run.id),
                            "db",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                }
            };

            processed_windows += 1;
            epoch_windows_done += 1;
            last_window_epochs = window.epochs.clone();
            anchors_scanned += outcome.anchors_scanned;
            messages_processed = anchors_scanned;
            anchors_deduped += outcome.anchors_deduped;
            source_messages_read += outcome.source_messages_read;
            threads_updated += outcome.affected_threads;

            apply_stats.threads_rebuilt += outcome.apply_stats.threads_rebuilt;
            apply_stats.threads_unchanged_skipped += outcome.apply_stats.threads_unchanged_skipped;
            apply_stats.nodes_written += outcome.apply_stats.nodes_written;
            apply_stats.dummy_nodes_written += outcome.apply_stats.dummy_nodes_written;
            apply_stats.messages_written += outcome.apply_stats.messages_written;
            apply_stats.stale_threads_removed += outcome.apply_stats.stale_threads_removed;

            let progress = serde_json::json!({
                "stage": STAGE_THREADING,
                "mode": "epoch_window",
                "start_epoch": start_epoch,
                "epoch_windows_total": epoch_windows_total,
                "epoch_windows_done": epoch_windows_done,
                "window_epochs": window.epochs,
                "processed_windows": processed_windows,
                "processed_chunks": processed_windows,
                "messages_total": messages_processed,
                "messages_processed": messages_processed,
                "threads_created": apply_stats.threads_rebuilt,
                "threads_updated": threads_updated,
                "anchors_scanned": anchors_scanned,
                "anchors_deduped": anchors_deduped,
                "source_messages_read": source_messages_read,
                "threads_rewritten": apply_stats.threads_rebuilt,
                "threads_unchanged_skipped": apply_stats.threads_unchanged_skipped,
                "stale_threads_removed": apply_stats.stale_threads_removed,
            });
            if let Err(err) = self.pipeline.update_run_progress(run.id, progress).await {
                warn!(run_id = run.id, error = %err, "progress checkpoint failed");
            }
            info!(
                target: "worker_job",
                run_id = run.id,
                list_key = %run.list_key,
                stage = STAGE_THREADING,
                items_processed = messages_processed,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "pipeline checkpoint"
            );
        }

        if let Err(err) = ctx.heartbeat().await {
            warn!(job_id = job.id, error = %err, "heartbeat update failed");
        }
        match ctx.is_cancel_requested().await {
            Ok(true) => {
                return JobExecutionOutcome::Cancelled {
                    reason: "cancel requested".to_string(),
                    metrics: JobStoreMetrics {
                        duration_ms: started.elapsed().as_millis(),
                        rows_written: apply_stats.threads_rebuilt
                            + apply_stats.nodes_written
                            + apply_stats.messages_written
                            + apply_stats.stale_threads_removed,
                        bytes_read: 0,
                        commit_count: messages_processed,
                        parse_errors: 0,
                    },
                };
            }
            Ok(false) => {}
            Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
        }

        let final_progress = serde_json::json!({
            "stage": STAGE_THREADING,
            "mode": "epoch_window",
            "start_epoch": start_epoch,
            "epoch_windows_total": epoch_windows_total,
            "epoch_windows_done": epoch_windows_done,
            "window_epochs": last_window_epochs,
            "processed_windows": processed_windows,
            "processed_chunks": processed_windows,
            "messages_total": messages_processed,
            "messages_processed": messages_processed,
            "threads_created": apply_stats.threads_rebuilt,
            "threads_updated": threads_updated,
            "anchors_scanned": anchors_scanned,
            "anchors_deduped": anchors_deduped,
            "source_messages_read": source_messages_read,
            "threads_rewritten": apply_stats.threads_rebuilt,
            "threads_unchanged_skipped": apply_stats.threads_unchanged_skipped,
            "stale_threads_removed": apply_stats.stale_threads_removed,
        });
        if let Err(err) = self
            .pipeline
            .update_run_progress(run.id, final_progress)
            .await
        {
            warn!(run_id = run.id, error = %err, "progress checkpoint failed");
        }
        info!(
            target: "worker_job",
            run_id = run.id,
            list_key = %run.list_key,
            stage = STAGE_THREADING,
            items_processed = messages_processed,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "pipeline checkpoint"
        );

        if let Err(err) = self
            .enqueue_next_stage(run.id, &run.list_key, STAGE_LINEAGE)
            .await
        {
            return retryable_error(
                format!("failed to enqueue lineage stage for run {}: {err}", run.id),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }
        if let Err(err) = self
            .pipeline
            .set_run_current_stage(run.id, STAGE_LINEAGE)
            .await
        {
            return retryable_error(
                format!("failed to move run {} to lineage stage: {err}", run.id),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        let rows_written = apply_stats.threads_rebuilt
            + apply_stats.nodes_written
            + apply_stats.messages_written
            + apply_stats.stale_threads_removed;

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "run_id": run.id,
                "list_key": run.list_key,
                "stage": STAGE_THREADING,
                "mode": "epoch_window",
                "start_epoch": start_epoch,
                "epoch_windows_total": epoch_windows_total,
                "epoch_windows_done": epoch_windows_done,
                "window_epochs": last_window_epochs,
                "processed_windows": processed_windows,
                "processed_chunks": processed_windows,
                "messages_total": messages_processed,
                "messages_processed": messages_processed,
                "threads_created": apply_stats.threads_rebuilt,
                "threads_updated": threads_updated,
                "anchors_scanned": anchors_scanned,
                "anchors_deduped": anchors_deduped,
                "source_messages_read": source_messages_read,
                "threads_rewritten": apply_stats.threads_rebuilt,
                "threads_unchanged_skipped": apply_stats.threads_unchanged_skipped,
                "stale_threads_removed": apply_stats.stale_threads_removed,
                "next_stage": STAGE_LINEAGE,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written,
                bytes_read: 0,
                commit_count: messages_processed,
                parse_errors: 0,
            },
        }
    }
}
