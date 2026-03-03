use super::*;

impl Phase0JobHandler {
    pub(super) async fn handle_embedding_backfill_run(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: EmbeddingBackfillRunPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(value) => value,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid embedding_backfill_run payload: {err}"),
                        kind: "payload".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };

        let run = match self.embeddings.get_backfill_run(payload.run_id).await {
            Ok(Some(value)) => value,
            Ok(None) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("embedding_backfill_run {} not found", payload.run_id),
                    kind: "not_found".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }
            Err(err) => {
                return retryable_error(
                    format!(
                        "failed to load embedding_backfill_run {}: {err}",
                        payload.run_id
                    ),
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
                    "skipped": format!("run is in terminal state {}", run.state),
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        let Some(scope) = parse_embedding_scope(&run.scope) else {
            return JobExecutionOutcome::Terminal {
                reason: format!("invalid embedding scope on run {}: {}", run.id, run.scope),
                kind: "payload".to_string(),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        };
        let chunk_limit = usize_to_i64(self.settings.embeddings.enqueue_batch_size.max(1));
        let list_key = run.list_key.clone().unwrap_or_else(|| "global".to_string());
        let total_candidates = match self
            .embeddings
            .count_candidate_ids(
                scope.as_str(),
                run.list_key.as_deref(),
                run.from_seen_at,
                run.to_seen_at,
            )
            .await
        {
            Ok(value) => value,
            Err(err) => {
                return retryable_error(
                    format!(
                        "failed to count embedding candidates for run {}: {err}",
                        run.id
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let mut cursor = run.cursor_id;
        let mut processed = run.processed_count.max(0);
        let mut jobs_enqueued = 0i64;

        loop {
            if let Err(err) = ctx.heartbeat().await {
                warn!(job_id = job.id, error = %err, "heartbeat update failed");
            }
            match ctx.is_cancel_requested().await {
                Ok(true) => {
                    let _ = self
                        .embeddings
                        .mark_backfill_cancelled(run.id, "cancel requested")
                        .await;
                    return JobExecutionOutcome::Cancelled {
                        reason: "cancel requested".to_string(),
                        metrics: JobStoreMetrics {
                            duration_ms: started.elapsed().as_millis(),
                            rows_written: 0,
                            bytes_read: 0,
                            commit_count: i64_to_u64(processed),
                            parse_errors: 0,
                        },
                    };
                }
                Ok(false) => {}
                Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
            }

            let ids = match self
                .embeddings
                .list_candidate_ids(
                    scope.as_str(),
                    run.list_key.as_deref(),
                    run.from_seen_at,
                    run.to_seen_at,
                    cursor,
                    chunk_limit,
                )
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    let _ = self
                        .embeddings
                        .mark_backfill_failed(run.id, &format!("failed listing candidates: {err}"))
                        .await;
                    return retryable_error(
                        format!(
                            "failed to list embedding candidates for run {}: {err}",
                            run.id
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };
            if ids.is_empty() {
                break;
            }
            cursor = *ids.last().unwrap_or(&cursor);
            processed += usize_to_i64(ids.len());

            match self
                .enqueue_embedding_generate_batch(
                    &list_key,
                    scope,
                    &ids,
                    Some(job.id),
                    PRIORITY_EMBEDDING_BATCH,
                )
                .await
            {
                Ok(true) => jobs_enqueued += 1,
                Ok(false) => {}
                Err(err) => {
                    let _ = self
                        .embeddings
                        .mark_backfill_failed(
                            run.id,
                            &format!("failed enqueueing embedding batch: {err}"),
                        )
                        .await;
                    return retryable_error(
                        format!("failed enqueueing embedding_generate_batch: {err}"),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            }

            if let Err(err) = self
                .embeddings
                .update_backfill_progress(
                    run.id,
                    BackfillProgressUpdate {
                        cursor_id: cursor,
                        processed_count: processed,
                        embedded_count: processed,
                        failed_count: 0,
                        total_candidates,
                        progress_json: serde_json::json!({
                            "cursor_id": cursor,
                            "processed_count": processed,
                            "jobs_enqueued": jobs_enqueued,
                            "total_candidates": total_candidates,
                        }),
                    },
                )
                .await
            {
                return retryable_error(
                    format!(
                        "failed to update embedding_backfill_run progress for {}: {err}",
                        run.id
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        }

        if let Err(err) = self
            .embeddings
            .mark_backfill_succeeded(
                run.id,
                serde_json::json!({
                    "scope": scope.as_str(),
                    "model_key": run.model_key,
                    "processed_count": processed,
                    "jobs_enqueued": jobs_enqueued,
                    "total_candidates": total_candidates,
                }),
            )
            .await
        {
            return retryable_error(
                format!(
                    "failed to mark embedding_backfill_run {} succeeded: {err}",
                    run.id
                ),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "run_id": run.id,
                "scope": scope.as_str(),
                "model_key": run.model_key,
                "processed_count": processed,
                "jobs_enqueued": jobs_enqueued,
                "total_candidates": total_candidates,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written: i64_to_u64(jobs_enqueued),
                bytes_read: 0,
                commit_count: i64_to_u64(processed),
                parse_errors: 0,
            },
        }
    }
}
