use super::*;

impl Phase0JobHandler {
    pub(super) async fn handle_meili_bootstrap_run(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: MeiliBootstrapRunPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(value) => value,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid meili_bootstrap_run payload: {err}"),
                        kind: "payload".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };

        let mut run = match self
            .embeddings
            .get_meili_bootstrap_run(payload.run_id)
            .await
        {
            Ok(Some(value)) => value,
            Ok(None) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("meili_bootstrap_run {} not found", payload.run_id),
                    kind: "not_found".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }
            Err(err) => {
                return retryable_error(
                    format!(
                        "failed to load meili_bootstrap_run {}: {err}",
                        payload.run_id
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        if run.state == "queued" {
            if let Err(err) = self.embeddings.mark_meili_bootstrap_running(run.id).await {
                return retryable_error(
                    format!(
                        "failed to mark meili_bootstrap_run {} running: {err}",
                        run.id
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            run.state = "running".to_string();
        }

        if run.state != "running" {
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "run_id": run.id,
                    "skipped": format!("run is in state {}", run.state),
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        if run.scope != "embedding_indexes" {
            let _ = self
                .embeddings
                .mark_meili_bootstrap_failed(
                    run.id,
                    format!("unsupported meili bootstrap scope {}", run.scope).as_str(),
                )
                .await;
            return JobExecutionOutcome::Terminal {
                reason: format!("unsupported meili bootstrap scope {}", run.scope),
                kind: "payload".to_string(),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }
        if run.model_key != self.settings.embeddings.model {
            let _ = self
                .embeddings
                .mark_meili_bootstrap_failed(
                    run.id,
                    format!(
                        "unsupported model_key {} (configured model is {})",
                        run.model_key, self.settings.embeddings.model
                    )
                    .as_str(),
                )
                .await;
            return JobExecutionOutcome::Terminal {
                reason: format!(
                    "unsupported model_key {} (configured model is {})",
                    run.model_key, self.settings.embeddings.model
                ),
                kind: "payload".to_string(),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }
        if run.embedder_name != self.settings.embeddings.embedder_name {
            let _ = self
                .embeddings
                .mark_meili_bootstrap_failed(
                    run.id,
                    format!(
                        "unsupported embedder_name {} (configured embedder is {})",
                        run.embedder_name, self.settings.embeddings.embedder_name
                    )
                    .as_str(),
                )
                .await;
            return JobExecutionOutcome::Terminal {
                reason: format!(
                    "unsupported embedder_name {} (configured embedder is {})",
                    run.embedder_name, self.settings.embeddings.embedder_name
                ),
                kind: "payload".to_string(),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        for index in [MeiliIndexKind::ThreadDocs, MeiliIndexKind::PatchSeriesDocs] {
            if let Err(err) = self.ensure_index_exists_only(index.spec(), &ctx).await {
                return retryable_error(
                    format!("failed to ensure index {} exists: {err}", index.uid()),
                    "meili",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        }

        let chunk_limit = self.settings.embeddings.enqueue_batch_size.max(1) as i64;
        let list_key = run.list_key.clone();
        let total_candidates_thread = match self
            .embeddings
            .count_candidate_ids("thread", list_key.as_deref(), None, None)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                return retryable_error(
                    format!(
                        "failed to count thread bootstrap candidates for run {}: {err}",
                        run.id
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };
        let total_candidates_series = match self
            .embeddings
            .count_candidate_ids("series", list_key.as_deref(), None, None)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                return retryable_error(
                    format!(
                        "failed to count series bootstrap candidates for run {}: {err}",
                        run.id
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let mut thread_cursor_id = run.thread_cursor_id.max(0);
        let mut series_cursor_id = run.series_cursor_id.max(0);
        let mut processed_thread = run.processed_thread.max(0);
        let mut processed_series = run.processed_series.max(0);
        let mut docs_upserted = run.docs_upserted.max(0);
        let mut vectors_attached = run.vectors_attached.max(0);
        let mut placeholders_written = run.placeholders_written.max(0);
        let mut failed_batches = run.failed_batches.max(0);

        let persist_progress = |thread_cursor_id: i64,
                                series_cursor_id: i64,
                                processed_thread: i64,
                                processed_series: i64,
                                docs_upserted: i64,
                                vectors_attached: i64,
                                placeholders_written: i64,
                                failed_batches: i64| {
            serde_json::json!({
                "scope": run.scope,
                "list_key": run.list_key,
                "thread_cursor_id": thread_cursor_id,
                "series_cursor_id": series_cursor_id,
                "total_candidates_thread": total_candidates_thread,
                "total_candidates_series": total_candidates_series,
                "processed_thread": processed_thread,
                "processed_series": processed_series,
                "docs_upserted": docs_upserted,
                "vectors_attached": vectors_attached,
                "placeholders_written": placeholders_written,
                "failed_batches": failed_batches,
            })
        };

        loop {
            if let Err(err) = ctx.heartbeat().await {
                warn!(job_id = job.id, error = %err, "heartbeat update failed");
            }
            match ctx.is_cancel_requested().await {
                Ok(true) => {
                    let _ = self
                        .embeddings
                        .mark_meili_bootstrap_cancelled(run.id, "cancel requested")
                        .await;
                    return JobExecutionOutcome::Cancelled {
                        reason: "cancel requested".to_string(),
                        metrics: JobStoreMetrics {
                            duration_ms: started.elapsed().as_millis(),
                            rows_written: docs_upserted as u64,
                            bytes_read: 0,
                            commit_count: (processed_thread + processed_series) as u64,
                            parse_errors: failed_batches as u64,
                        },
                    };
                }
                Ok(false) => {}
                Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
            }

            let ids = match self
                .embeddings
                .list_candidate_ids(
                    "thread",
                    list_key.as_deref(),
                    None,
                    None,
                    thread_cursor_id,
                    chunk_limit,
                )
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    failed_batches += 1;
                    let progress_json = persist_progress(
                        thread_cursor_id,
                        series_cursor_id,
                        processed_thread,
                        processed_series,
                        docs_upserted,
                        vectors_attached,
                        placeholders_written,
                        failed_batches,
                    );
                    let _ = self
                        .embeddings
                        .update_meili_bootstrap_progress(
                            run.id,
                            thread_cursor_id,
                            series_cursor_id,
                            total_candidates_thread,
                            total_candidates_series,
                            processed_thread,
                            processed_series,
                            docs_upserted,
                            vectors_attached,
                            placeholders_written,
                            failed_batches,
                            progress_json,
                        )
                        .await;
                    return retryable_error(
                        format!("failed to list thread bootstrap candidates: {err}"),
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

            match self
                .upsert_meili_bootstrap_chunk(MeiliIndexKind::ThreadDocs, &ids, &ctx)
                .await
            {
                Ok((rows, vectors, placeholders)) => {
                    docs_upserted += rows as i64;
                    vectors_attached += vectors;
                    placeholders_written += placeholders;
                    processed_thread += ids.len() as i64;
                    if let Some(last_id) = ids.last() {
                        thread_cursor_id = *last_id;
                    }
                }
                Err(err) => {
                    failed_batches += 1;
                    let progress_json = persist_progress(
                        thread_cursor_id,
                        series_cursor_id,
                        processed_thread,
                        processed_series,
                        docs_upserted,
                        vectors_attached,
                        placeholders_written,
                        failed_batches,
                    );
                    let _ = self
                        .embeddings
                        .update_meili_bootstrap_progress(
                            run.id,
                            thread_cursor_id,
                            series_cursor_id,
                            total_candidates_thread,
                            total_candidates_series,
                            processed_thread,
                            processed_series,
                            docs_upserted,
                            vectors_attached,
                            placeholders_written,
                            failed_batches,
                            progress_json,
                        )
                        .await;
                    return retryable_error(
                        format!("failed to upsert thread bootstrap chunk: {err}"),
                        "meili",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            }

            let progress_json = persist_progress(
                thread_cursor_id,
                series_cursor_id,
                processed_thread,
                processed_series,
                docs_upserted,
                vectors_attached,
                placeholders_written,
                failed_batches,
            );
            if let Err(err) = self
                .embeddings
                .update_meili_bootstrap_progress(
                    run.id,
                    thread_cursor_id,
                    series_cursor_id,
                    total_candidates_thread,
                    total_candidates_series,
                    processed_thread,
                    processed_series,
                    docs_upserted,
                    vectors_attached,
                    placeholders_written,
                    failed_batches,
                    progress_json,
                )
                .await
            {
                return retryable_error(
                    format!(
                        "failed to update meili bootstrap progress for run {}: {err}",
                        run.id
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        }

        loop {
            if let Err(err) = ctx.heartbeat().await {
                warn!(job_id = job.id, error = %err, "heartbeat update failed");
            }
            match ctx.is_cancel_requested().await {
                Ok(true) => {
                    let _ = self
                        .embeddings
                        .mark_meili_bootstrap_cancelled(run.id, "cancel requested")
                        .await;
                    return JobExecutionOutcome::Cancelled {
                        reason: "cancel requested".to_string(),
                        metrics: JobStoreMetrics {
                            duration_ms: started.elapsed().as_millis(),
                            rows_written: docs_upserted as u64,
                            bytes_read: 0,
                            commit_count: (processed_thread + processed_series) as u64,
                            parse_errors: failed_batches as u64,
                        },
                    };
                }
                Ok(false) => {}
                Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
            }

            let ids = match self
                .embeddings
                .list_candidate_ids(
                    "series",
                    list_key.as_deref(),
                    None,
                    None,
                    series_cursor_id,
                    chunk_limit,
                )
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    failed_batches += 1;
                    let progress_json = persist_progress(
                        thread_cursor_id,
                        series_cursor_id,
                        processed_thread,
                        processed_series,
                        docs_upserted,
                        vectors_attached,
                        placeholders_written,
                        failed_batches,
                    );
                    let _ = self
                        .embeddings
                        .update_meili_bootstrap_progress(
                            run.id,
                            thread_cursor_id,
                            series_cursor_id,
                            total_candidates_thread,
                            total_candidates_series,
                            processed_thread,
                            processed_series,
                            docs_upserted,
                            vectors_attached,
                            placeholders_written,
                            failed_batches,
                            progress_json,
                        )
                        .await;
                    return retryable_error(
                        format!("failed to list series bootstrap candidates: {err}"),
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

            match self
                .upsert_meili_bootstrap_chunk(MeiliIndexKind::PatchSeriesDocs, &ids, &ctx)
                .await
            {
                Ok((rows, vectors, placeholders)) => {
                    docs_upserted += rows as i64;
                    vectors_attached += vectors;
                    placeholders_written += placeholders;
                    processed_series += ids.len() as i64;
                    if let Some(last_id) = ids.last() {
                        series_cursor_id = *last_id;
                    }
                }
                Err(err) => {
                    failed_batches += 1;
                    let progress_json = persist_progress(
                        thread_cursor_id,
                        series_cursor_id,
                        processed_thread,
                        processed_series,
                        docs_upserted,
                        vectors_attached,
                        placeholders_written,
                        failed_batches,
                    );
                    let _ = self
                        .embeddings
                        .update_meili_bootstrap_progress(
                            run.id,
                            thread_cursor_id,
                            series_cursor_id,
                            total_candidates_thread,
                            total_candidates_series,
                            processed_thread,
                            processed_series,
                            docs_upserted,
                            vectors_attached,
                            placeholders_written,
                            failed_batches,
                            progress_json,
                        )
                        .await;
                    return retryable_error(
                        format!("failed to upsert series bootstrap chunk: {err}"),
                        "meili",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            }

            let progress_json = persist_progress(
                thread_cursor_id,
                series_cursor_id,
                processed_thread,
                processed_series,
                docs_upserted,
                vectors_attached,
                placeholders_written,
                failed_batches,
            );
            if let Err(err) = self
                .embeddings
                .update_meili_bootstrap_progress(
                    run.id,
                    thread_cursor_id,
                    series_cursor_id,
                    total_candidates_thread,
                    total_candidates_series,
                    processed_thread,
                    processed_series,
                    docs_upserted,
                    vectors_attached,
                    placeholders_written,
                    failed_batches,
                    progress_json,
                )
                .await
            {
                return retryable_error(
                    format!(
                        "failed to update meili bootstrap progress for run {}: {err}",
                        run.id
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        }

        for index in [MeiliIndexKind::ThreadDocs, MeiliIndexKind::PatchSeriesDocs] {
            let settings = self.meili_index_settings(index);
            if let Err(err) = self
                .ensure_index_settings(index.spec(), &settings, &ctx)
                .await
            {
                failed_batches += 1;
                let progress_json = persist_progress(
                    thread_cursor_id,
                    series_cursor_id,
                    processed_thread,
                    processed_series,
                    docs_upserted,
                    vectors_attached,
                    placeholders_written,
                    failed_batches,
                );
                let _ = self
                    .embeddings
                    .update_meili_bootstrap_progress(
                        run.id,
                        thread_cursor_id,
                        series_cursor_id,
                        total_candidates_thread,
                        total_candidates_series,
                        processed_thread,
                        processed_series,
                        docs_upserted,
                        vectors_attached,
                        placeholders_written,
                        failed_batches,
                        progress_json,
                    )
                    .await;
                return retryable_error(
                    format!(
                        "failed to apply final index settings for {} during bootstrap: {err}",
                        index.uid()
                    ),
                    "meili",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        }

        let result_json = serde_json::json!({
            "run_id": run.id,
            "scope": run.scope,
            "list_key": run.list_key,
            "total_candidates_thread": total_candidates_thread,
            "total_candidates_series": total_candidates_series,
            "processed_thread": processed_thread,
            "processed_series": processed_series,
            "docs_upserted": docs_upserted,
            "vectors_attached": vectors_attached,
            "placeholders_written": placeholders_written,
            "failed_batches": failed_batches,
            "model_key": run.model_key,
            "embedder_name": run.embedder_name,
        });

        if let Err(err) = self
            .embeddings
            .mark_meili_bootstrap_succeeded(run.id, result_json.clone())
            .await
        {
            return retryable_error(
                format!(
                    "failed to mark meili bootstrap run {} succeeded: {err}",
                    run.id
                ),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        JobExecutionOutcome::Success {
            result_json,
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written: docs_upserted as u64,
                bytes_read: 0,
                commit_count: (processed_thread + processed_series) as u64,
                parse_errors: failed_batches as u64,
            },
        }
    }
}
