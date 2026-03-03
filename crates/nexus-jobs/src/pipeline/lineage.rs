use super::*;

impl Phase0JobHandler {
    pub(super) async fn handle_pipeline_lineage(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: PipelineLineagePayload = match serde_json::from_value(job.payload_json.clone())
        {
            Ok(value) => value,
            Err(err) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("invalid pipeline_lineage payload: {err}"),
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

        let batch_limit = usize_to_i64(self.settings.mail.commit_batch_size.max(1));
        let checkpoint_interval =
            usize_to_u64(self.settings.worker.progress_checkpoint_interval.max(1));
        let discovery_mode_label = "thread_first";
        let work_item_kind = "threads";

        let mut cursor = 0i64;
        let mut processed_chunks = 0u64;
        let mut work_items_processed = 0u64;
        let mut messages_processed = 0u64;
        let mut threads_scanned = 0u64;
        let mut series_versions_written = 0u64;
        let mut patch_items_written = 0u64;
        let mut patch_item_files_written = 0u64;
        let mut patch_items_hydrated = 0u64;
        let mut patch_items_fallback_patch_id = 0u64;
        let mut patch_items_fallback_diff_parse = 0u64;

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
                            rows_written: series_versions_written
                                + patch_items_written
                                + patch_item_files_written,
                            bytes_read: 0,
                            commit_count: messages_processed,
                            parse_errors: 0,
                        },
                    };
                }
                Ok(false) => {}
                Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
            }

            let thread_chunk = match self
                .pipeline
                .query_ingest_window_thread_ids_chunk(
                    run.mailing_list_id,
                    window_from,
                    window_to,
                    cursor,
                    batch_limit,
                )
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!(
                            "failed to query threads for lineage stage run {}: {err}",
                            run.id
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };
            if thread_chunk.is_empty() {
                break;
            }
            cursor = *thread_chunk.last().unwrap_or(&cursor);

            let extract_outcome = match process_patch_extract_threads(
                &self.lineage,
                run.mailing_list_id,
                &thread_chunk,
            )
            .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!("patch lineage extraction failed for run {}: {err}", run.id),
                        "parse",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };
            let chunk_work_items = usize_to_u64(thread_chunk.len());

            if !extract_outcome.patch_item_ids.is_empty() {
                match process_patch_enrichment_batch(&self.lineage, &extract_outcome.patch_item_ids)
                    .await
                {
                    Ok(enrichment_outcome) => {
                        patch_item_files_written += enrichment_outcome.patch_item_files_written;
                        patch_items_hydrated += enrichment_outcome.patch_items_hydrated;
                        patch_items_fallback_patch_id +=
                            enrichment_outcome.patch_items_fallback_patch_id;
                        patch_items_fallback_diff_parse +=
                            enrichment_outcome.patch_items_fallback_diff_parse;
                    }
                    Err(err) => {
                        return retryable_error(
                            format!("patch enrichment failed for run {}: {err}", run.id),
                            "parse",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                }
            }

            processed_chunks += 1;
            work_items_processed += chunk_work_items;
            messages_processed += extract_outcome.source_messages_read;
            threads_scanned += extract_outcome.source_threads_scanned;
            series_versions_written += extract_outcome.series_versions_written;
            patch_items_written += extract_outcome.patch_items_written;

            if work_items_processed % checkpoint_interval < chunk_work_items {
                let progress = serde_json::json!({
                    "stage": STAGE_LINEAGE,
                    "discovery_mode": discovery_mode_label,
                    "processed_chunks": processed_chunks,
                    "work_item_kind": work_item_kind,
                    "work_items_processed": work_items_processed,
                    "messages_processed": messages_processed,
                    "threads_scanned": threads_scanned,
                    "series_versions_written": series_versions_written,
                    "patch_items_written": patch_items_written,
                    "patch_files_written": patch_item_files_written,
                    "patch_items_hydrated": patch_items_hydrated,
                    "patch_items_fallback_patch_id": patch_items_fallback_patch_id,
                    "patch_items_fallback_diff_parse": patch_items_fallback_diff_parse,
                });
                if let Err(err) = self.pipeline.update_run_progress(run.id, progress).await {
                    warn!(run_id = run.id, error = %err, "progress checkpoint failed");
                }
                info!(
                    target: "worker_job",
                    run_id = run.id,
                    list_key = %run.list_key,
                    stage = STAGE_LINEAGE,
                    discovery_mode = discovery_mode_label,
                    work_item_kind,
                    items_processed = work_items_processed,
                    messages_processed,
                    threads_scanned,
                    elapsed_ms = u128_to_u64(started.elapsed().as_millis()),
                    "pipeline checkpoint"
                );
            }
        }

        if let Err(err) = self
            .enqueue_next_stage(run.id, &run.list_key, STAGE_LEXICAL)
            .await
        {
            return retryable_error(
                format!("failed to enqueue lexical stage for run {}: {err}", run.id),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }
        if let Err(err) = self
            .pipeline
            .set_run_current_stage(run.id, STAGE_LEXICAL)
            .await
        {
            return retryable_error(
                format!("failed to move run {} to lexical stage: {err}", run.id),
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
                "stage": STAGE_LINEAGE,
                "discovery_mode": discovery_mode_label,
                "processed_chunks": processed_chunks,
                "work_item_kind": work_item_kind,
                "work_items_processed": work_items_processed,
                "messages_processed": messages_processed,
                "threads_scanned": threads_scanned,
                "series_versions_written": series_versions_written,
                "patch_items_written": patch_items_written,
                "patch_item_files_written": patch_item_files_written,
                "patch_items_hydrated": patch_items_hydrated,
                "patch_items_fallback_patch_id": patch_items_fallback_patch_id,
                "patch_items_fallback_diff_parse": patch_items_fallback_diff_parse,
                "next_stage": STAGE_LEXICAL,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written: series_versions_written
                    + patch_items_written
                    + patch_item_files_written,
                bytes_read: 0,
                commit_count: messages_processed,
                parse_errors: 0,
            },
        }
    }
}
