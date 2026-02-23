use super::*;

impl Phase0JobHandler {
    pub(super) async fn handle_pipeline_lexical(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: PipelineLexicalPayload = match serde_json::from_value(job.payload_json.clone())
        {
            Ok(value) => value,
            Err(err) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("invalid pipeline_lexical payload: {err}"),
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

        let mut total_docs_upserted = 0u64;
        let mut total_ids_seen = 0u64;
        let mut family_results: Vec<SearchFamilyOutcome> = Vec::new();
        let chunk_limit = self.settings.meili.upsert_batch_size.max(1);

        for index in [MeiliIndexKind::ThreadDocs, MeiliIndexKind::PatchSeriesDocs] {
            let index_spec = index.spec();
            let settings = self.meili_index_settings(index);
            if let Err(err) = self
                .ensure_index_settings(index_spec, &settings, &ctx)
                .await
            {
                return retryable_error(
                    format!("failed to prepare index {}: {err}", index.uid()),
                    "meili",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }

            let mut docs_upserted = 0u64;
            let mut docs_bytes = 0u64;
            let mut tasks_submitted = 0u64;
            let mut chunks_done = 0u64;
            let impacted_ids = match index {
                MeiliIndexKind::ThreadDocs => {
                    self.pipeline
                        .query_impacted_thread_ids(run.mailing_list_id, window_from, window_to)
                        .await
                }
                MeiliIndexKind::PatchSeriesDocs => {
                    self.pipeline
                        .query_impacted_series_ids(run.mailing_list_id, window_from, window_to)
                        .await
                }
                MeiliIndexKind::PatchItemDocs => {
                    unreachable!("patch_item_docs indexing is disabled in pipeline_lexical")
                }
            };
            let impacted_ids = match impacted_ids {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!("failed to query impacted ids for {}: {err}", index.uid()),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };
            let ids_seen = impacted_ids.len() as u64;

            for ids in impacted_ids.chunks(chunk_limit) {
                if let Err(err) = ctx.heartbeat().await {
                    warn!(job_id = job.id, error = %err, "heartbeat update failed");
                }

                chunks_done += 1;

                let docs = match self
                    .build_docs_for_index(index, ids, VectorAttachmentMode::NullPlaceholder)
                    .await
                {
                    Ok(value) => value,
                    Err(err) => {
                        return retryable_error(
                            format!("failed to build docs for {}: {err}", index.uid()),
                            "db",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                };
                if docs.is_empty() {
                    continue;
                }

                let task_uid = match self.meili.upsert_documents(index_spec.uid, &docs).await {
                    Ok(value) => value,
                    Err(err) => {
                        return retryable_error(
                            format!("failed to upsert docs for {}: {err}", index.uid()),
                            "meili",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                };
                if let Err(err) = self.wait_for_task(task_uid, &ctx).await {
                    return retryable_error(
                        format!("meili task {} failed for {}: {err}", task_uid, index.uid()),
                        "meili",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }

                docs_upserted += docs.len() as u64;
                docs_bytes += serde_json::to_vec(&docs)
                    .map(|bytes| bytes.len() as u64)
                    .unwrap_or(0);
                tasks_submitted += 1;
            }

            total_docs_upserted += docs_upserted;
            total_ids_seen += ids_seen;
            family_results.push(SearchFamilyOutcome {
                artifact_kind: index.uid().to_string(),
                index_uid: index.uid().to_string(),
                chunks_done,
                ids_seen,
                docs_upserted,
                docs_bytes,
                tasks_submitted,
            });
        }

        // Mark run succeeded
        if let Err(err) = self.pipeline.mark_run_succeeded(run.id).await {
            return retryable_error(
                format!(
                    "failed to mark run {} succeeded after lexical stage: {err}",
                    run.id
                ),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        // Activate next pending list in the batch
        if let Some(batch_id) = run.batch_id
            && let Err(err) = self.activate_and_enqueue_next_list(batch_id).await
        {
            warn!(
                run_id = run.id,
                batch_id,
                error = %err,
                "failed to activate next pending list in batch"
            );
        }

        let mut embedding_stage_enqueued = false;
        if self.settings.embeddings.enabled {
            match self
                .enqueue_pipeline_embedding(
                    run.id,
                    &run.list_key,
                    window_from,
                    window_to,
                    Some(job.id),
                )
                .await
            {
                Ok(true) => embedding_stage_enqueued = true,
                Ok(false) => {}
                Err(err) => {
                    warn!(
                        run_id = run.id,
                        list_key = %run.list_key,
                        error = %err,
                        "failed to enqueue background embedding stage after lexical completion"
                    );
                }
            }
        }

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "run_id": run.id,
                "list_key": run.list_key,
                "stage": STAGE_LEXICAL,
                "total_docs_upserted": total_docs_upserted,
                "total_ids_seen": total_ids_seen,
                "families": family_results,
                "embedding_stage_enqueued": embedding_stage_enqueued,
                "run_state": "succeeded",
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written: total_docs_upserted,
                bytes_read: 0,
                commit_count: total_ids_seen,
                parse_errors: 0,
            },
        }
    }
}
