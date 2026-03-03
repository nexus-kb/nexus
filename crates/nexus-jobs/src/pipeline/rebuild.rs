use super::*;

impl Phase0JobHandler {
    pub(super) async fn handle_threading_rebuild_list(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();

        let payload: ThreadingRebuildListPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(v) => v,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid threading_rebuild_list payload: {err}"),
                        kind: "payload".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };

        // Check there is no active pipeline run for this list
        match self
            .pipeline
            .get_active_run_for_list(&payload.list_key)
            .await
        {
            Ok(Some(_)) => {
                return retryable_error(
                    format!(
                        "pipeline is running for list {}, deferring threading_rebuild_list",
                        payload.list_key
                    ),
                    "stage_barrier",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            Err(err) => {
                return retryable_error(
                    format!(
                        "failed to check active pipeline for list {}: {err}",
                        payload.list_key
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            Ok(None) => {}
        }

        let list = match self.catalog.get_mailing_list(&payload.list_key).await {
            Ok(Some(v)) => v,
            Ok(None) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("mailing list not found for list_key={}", payload.list_key),
                    kind: "not_found".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }
            Err(err) => {
                return retryable_error(
                    format!("failed to load mailing list: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let repos = match self.catalog.list_repos_for_list(&payload.list_key).await {
            Ok(value) => order_repos_for_ingest(value),
            Err(err) => {
                return retryable_error(
                    format!("failed to list repos for list {}: {err}", payload.list_key),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };
        if repos.is_empty() {
            return JobExecutionOutcome::Terminal {
                reason: format!("no repos found for list {}", payload.list_key),
                kind: "pipeline_state".to_string(),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        let touched_repo_ids = match self
            .threading
            .list_repo_ids_for_rebuild(list.id, payload.from_seen_at, payload.to_seen_at)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                return retryable_error(
                    format!(
                        "failed to query touched repos for threading_rebuild_list {}: {err}",
                        payload.list_key
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

        let epoch_windows_total = usize_to_u64(windows.len());
        let mut epoch_windows_done = 0u64;
        let mut processed_chunks = 0u64;
        let mut processed_messages = 0u64;
        let mut affected_threads = 0u64;
        let mut anchors_scanned = 0u64;
        let mut anchors_deduped = 0u64;
        let mut source_messages_read = 0u64;
        let mut meili_docs_upserted = 0u64;
        let mut apply_stats = ThreadingApplyStats::default();
        let mut last_progress_log = Instant::now();
        let mut last_window_epochs: Vec<i64> = Vec::new();

        // Prepare meili index for inline upserts
        let index = MeiliIndexKind::ThreadDocs;
        let index_spec = index.spec();
        let settings = self.meili_index_settings(index);
        if let Err(err) = self
            .ensure_index_settings(index_spec, &settings, &ctx)
            .await
        {
            return retryable_error(
                format!("failed to prepare thread_docs index: {err}"),
                "meili",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

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
                            commit_count: processed_messages,
                            parse_errors: 0,
                        },
                    };
                }
                Ok(false) => {}
                Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
            }

            let chunk = match self
                .threading
                .list_message_pks_for_rebuild_repo_ids(
                    list.id,
                    payload.from_seen_at,
                    payload.to_seen_at,
                    &window.repo_ids,
                )
                .await
            {
                Ok(v) => v,
                Err(err) => {
                    return retryable_error(
                        format!("failed to load rebuild window anchors: {err}"),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };

            let outcome = if chunk.is_empty() {
                ThreadingWindowOutcome::default()
            } else {
                match self
                    .apply_threading_for_anchors(list.id, &payload.list_key, chunk.clone())
                    .await
                {
                    Ok(v) => v,
                    Err(err) => {
                        return retryable_error(
                            format!("failed to apply threading rebuild chunk: {err}"),
                            "db",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                }
            };

            processed_chunks += 1;
            epoch_windows_done += 1;
            last_window_epochs = window.epochs.clone();
            anchors_scanned += outcome.anchors_scanned;
            processed_messages = anchors_scanned;
            anchors_deduped += outcome.anchors_deduped;
            source_messages_read += outcome.source_messages_read;
            affected_threads += outcome.affected_threads;
            apply_stats.threads_rebuilt += outcome.apply_stats.threads_rebuilt;
            apply_stats.threads_unchanged_skipped += outcome.apply_stats.threads_unchanged_skipped;
            apply_stats.nodes_written += outcome.apply_stats.nodes_written;
            apply_stats.dummy_nodes_written += outcome.apply_stats.dummy_nodes_written;
            apply_stats.messages_written += outcome.apply_stats.messages_written;
            apply_stats.stale_threads_removed += outcome.apply_stats.stale_threads_removed;

            // Inline meili upsert for impacted threads
            if !outcome.impacted_thread_ids.is_empty() {
                let lexical_chunk_limit = self.settings.meili.upsert_batch_size.max(1);
                match self
                    .upsert_meili_docs_inline(
                        index,
                        &outcome.impacted_thread_ids,
                        lexical_chunk_limit,
                        &ctx,
                    )
                    .await
                {
                    Ok(n) => meili_docs_upserted += n,
                    Err(err) => {
                        return retryable_error(
                            format!("failed to upsert thread docs to meili: {err}"),
                            "meili",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                }
            }

            if processed_chunks.is_multiple_of(10)
                || last_progress_log.elapsed() >= Duration::from_secs(30)
            {
                info!(
                    job_id = job.id,
                    list_key = %payload.list_key,
                    mode = "epoch_window",
                    window_epochs = ?window.epochs,
                    start_epoch,
                    epoch_windows_total,
                    epoch_windows_done,
                    processed_chunks,
                    processed_messages,
                    affected_threads,
                    anchors_scanned,
                    anchors_deduped,
                    source_messages_read,
                    threads_rebuilt = apply_stats.threads_rebuilt,
                    threads_unchanged_skipped = apply_stats.threads_unchanged_skipped,
                    meili_docs_upserted,
                    "threading_rebuild_list progress"
                );
                last_progress_log = Instant::now();
            }
        }

        info!(
            list_key = %payload.list_key,
            mode = "epoch_window",
            start_epoch,
            epoch_windows_total,
            epoch_windows_done,
            window_epochs = ?last_window_epochs,
            processed_chunks,
            processed_messages,
            affected_threads,
            anchors_scanned,
            anchors_deduped,
            source_messages_read,
            threads_rebuilt = apply_stats.threads_rebuilt,
            threads_unchanged_skipped = apply_stats.threads_unchanged_skipped,
            nodes_written = apply_stats.nodes_written,
            dummy_nodes_written = apply_stats.dummy_nodes_written,
            messages_written = apply_stats.messages_written,
            stale_threads_removed = apply_stats.stale_threads_removed,
            meili_docs_upserted,
            "threading_rebuild_list applied update chunks"
        );

        let rows_written = apply_stats.threads_rebuilt
            + apply_stats.nodes_written
            + apply_stats.messages_written
            + apply_stats.stale_threads_removed;

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "list_key": payload.list_key,
                "mode": "epoch_window",
                "start_epoch": start_epoch,
                "epoch_windows_total": epoch_windows_total,
                "epoch_windows_done": epoch_windows_done,
                "window_epochs": last_window_epochs,
                "processed_chunks": processed_chunks,
                "processed_messages": processed_messages,
                "affected_threads": affected_threads,
                "anchors_scanned": anchors_scanned,
                "anchors_deduped": anchors_deduped,
                "source_messages_read": source_messages_read,
                "threads_rebuilt": apply_stats.threads_rebuilt,
                "threads_unchanged_skipped": apply_stats.threads_unchanged_skipped,
                "nodes_written": apply_stats.nodes_written,
                "dummy_nodes_written": apply_stats.dummy_nodes_written,
                "messages_written": apply_stats.messages_written,
                "stale_threads_removed": apply_stats.stale_threads_removed,
                "meili_docs_upserted": meili_docs_upserted,
                "from_seen_at": payload.from_seen_at,
                "to_seen_at": payload.to_seen_at,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written,
                bytes_read: 0,
                commit_count: processed_messages,
                parse_errors: 0,
            },
        }
    }

    pub(super) async fn handle_lineage_rebuild_list(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();

        let payload: LineageRebuildListPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(v) => v,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid lineage_rebuild_list payload: {err}"),
                        kind: "payload".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };

        // Check there is no active pipeline run for this list
        match self
            .pipeline
            .get_active_run_for_list(&payload.list_key)
            .await
        {
            Ok(Some(_)) => {
                return retryable_error(
                    format!(
                        "pipeline is running for list {}, deferring lineage_rebuild_list",
                        payload.list_key
                    ),
                    "stage_barrier",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            Err(err) => {
                return retryable_error(
                    format!(
                        "failed to check active pipeline for list {}: {err}",
                        payload.list_key
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            Ok(None) => {}
        }

        let list = match self.catalog.get_mailing_list(&payload.list_key).await {
            Ok(Some(v)) => v,
            Ok(None) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("mailing list not found for list_key={}", payload.list_key),
                    kind: "not_found".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }
            Err(err) => {
                return retryable_error(
                    format!("failed to load mailing list: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let discovery_mode_label = "thread_first";
        let work_item_kind = "threads";

        let mut cursor = 0i64;
        let mut processed_chunks = 0u64;
        let mut work_items_processed = 0u64;
        let mut processed_messages = 0u64;
        let mut threads_scanned = 0u64;
        let mut series_versions_written = 0u64;
        let mut patch_items_written = 0u64;
        let mut patch_item_files_written = 0u64;
        let mut patch_items_hydrated = 0u64;
        let mut patch_items_fallback_patch_id = 0u64;
        let mut patch_items_fallback_diff_parse = 0u64;
        let mut last_progress_log = Instant::now();
        let batch_limit = usize_to_i64(self.settings.mail.commit_batch_size.max(1));

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
                            commit_count: processed_messages,
                            parse_errors: 0,
                        },
                    };
                }
                Ok(false) => {}
                Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
            }

            let thread_chunk = match self
                .threading
                .list_thread_ids_for_rebuild(
                    list.id,
                    payload.from_seen_at,
                    payload.to_seen_at,
                    cursor,
                    batch_limit,
                )
                .await
            {
                Ok(v) => v,
                Err(err) => {
                    return retryable_error(
                        format!("failed to load lineage rebuild thread chunk: {err}"),
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
            let extract_outcome =
                match process_patch_extract_threads(&self.lineage, list.id, &thread_chunk).await {
                    Ok(v) => v,
                    Err(err) => {
                        return retryable_error(
                            format!("patch lineage extraction failed: {err}"),
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
                            format!("patch enrichment failed: {err}"),
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
            processed_messages += extract_outcome.source_messages_read;
            threads_scanned += extract_outcome.source_threads_scanned;
            series_versions_written += extract_outcome.series_versions_written;
            patch_items_written += extract_outcome.patch_items_written;

            if processed_chunks.is_multiple_of(10)
                || last_progress_log.elapsed() >= Duration::from_secs(30)
            {
                info!(
                    job_id = job.id,
                    list_key = %payload.list_key,
                    discovery_mode = discovery_mode_label,
                    work_item_kind,
                    processed_chunks,
                    work_items_processed,
                    processed_messages,
                    threads_scanned,
                    series_versions_written,
                    patch_items_written,
                    patch_item_files_written,
                    patch_items_hydrated,
                    patch_items_fallback_patch_id,
                    patch_items_fallback_diff_parse,
                    "lineage_rebuild_list progress"
                );
                last_progress_log = Instant::now();
            }
        }

        info!(
            list_key = %payload.list_key,
            discovery_mode = discovery_mode_label,
            work_item_kind,
            processed_chunks,
            work_items_processed,
            processed_messages,
            threads_scanned,
            series_versions_written,
            patch_items_written,
            patch_item_files_written,
            patch_items_hydrated,
            patch_items_fallback_patch_id,
            patch_items_fallback_diff_parse,
            "lineage_rebuild_list completed inline"
        );

        let rows_written = series_versions_written + patch_items_written + patch_item_files_written;

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "list_key": payload.list_key,
                "discovery_mode": discovery_mode_label,
                "work_item_kind": work_item_kind,
                "processed_chunks": processed_chunks,
                "work_items_processed": work_items_processed,
                "processed_messages": processed_messages,
                "threads_scanned": threads_scanned,
                "series_versions_written": series_versions_written,
                "patch_items_written": patch_items_written,
                "patch_item_files_written": patch_item_files_written,
                "patch_items_hydrated": patch_items_hydrated,
                "patch_items_fallback_patch_id": patch_items_fallback_patch_id,
                "patch_items_fallback_diff_parse": patch_items_fallback_diff_parse,
                "from_seen_at": payload.from_seen_at,
                "to_seen_at": payload.to_seen_at,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written,
                bytes_read: 0,
                commit_count: processed_messages,
                parse_errors: 0,
            },
        }
    }
}
