use super::*;

impl Phase0JobHandler {
    pub(super) async fn handle_pipeline_embedding(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: PipelineEmbeddingPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(value) => value,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid pipeline_embedding payload: {err}"),
                        kind: "payload".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };

        let list = match self.catalog.get_mailing_list(&payload.list_key).await {
            Ok(Some(value)) => value,
            Ok(None) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("mailing list not found for list_key={}", payload.list_key),
                    kind: "not_found".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }
            Err(err) => {
                return retryable_error(
                    format!("failed to load list {}: {err}", payload.list_key),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let mut jobs_enqueued = 0u64;
        let mut ids_seen = 0u64;
        let chunk_limit = self.settings.embeddings.enqueue_batch_size.max(1);

        for scope in [EmbeddingScope::Thread, EmbeddingScope::Series] {
            if let Err(err) = ctx.heartbeat().await {
                warn!(job_id = job.id, error = %err, "heartbeat update failed");
            }
            match ctx.is_cancel_requested().await {
                Ok(true) => {
                    return JobExecutionOutcome::Cancelled {
                        reason: "cancel requested".to_string(),
                        metrics: JobStoreMetrics {
                            duration_ms: started.elapsed().as_millis(),
                            rows_written: jobs_enqueued,
                            bytes_read: 0,
                            commit_count: ids_seen,
                            parse_errors: 0,
                        },
                    };
                }
                Ok(false) => {}
                Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
            }

            let impacted_ids = match scope {
                EmbeddingScope::Thread => {
                    self.pipeline
                        .query_impacted_thread_ids(list.id, payload.window_from, payload.window_to)
                        .await
                }
                EmbeddingScope::Series => {
                    self.pipeline
                        .query_impacted_series_ids(list.id, payload.window_from, payload.window_to)
                        .await
                }
            };
            let impacted_ids = match impacted_ids {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!(
                            "failed to query impacted {} ids for run {}: {err}",
                            scope.as_str(),
                            payload.run_id,
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };

            for ids in impacted_ids.chunks(chunk_limit) {
                if let Err(err) = ctx.heartbeat().await {
                    warn!(job_id = job.id, error = %err, "heartbeat update failed");
                }
                match ctx.is_cancel_requested().await {
                    Ok(true) => {
                        return JobExecutionOutcome::Cancelled {
                            reason: "cancel requested".to_string(),
                            metrics: JobStoreMetrics {
                                duration_ms: started.elapsed().as_millis(),
                                rows_written: jobs_enqueued,
                                bytes_read: 0,
                                commit_count: ids_seen,
                                parse_errors: 0,
                            },
                        };
                    }
                    Ok(false) => {}
                    Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
                }
                ids_seen += ids.len() as u64;
                match self
                    .enqueue_embedding_generate_batch(
                        &payload.list_key,
                        scope,
                        ids,
                        Some(job.id),
                        PRIORITY_EMBEDDING_BATCH,
                    )
                    .await
                {
                    Ok(true) => jobs_enqueued += 1,
                    Ok(false) => {}
                    Err(err) => {
                        return retryable_error(
                            format!(
                                "failed to enqueue embedding_generate_batch for scope {}: {err}",
                                scope.as_str()
                            ),
                            "db",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                }
            }
        }

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "run_id": payload.run_id,
                "list_key": payload.list_key,
                "stage": STAGE_EMBEDDING,
                "window_from": payload.window_from,
                "window_to": payload.window_to,
                "ids_seen": ids_seen,
                "embedding_jobs_enqueued": jobs_enqueued,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written: jobs_enqueued,
                bytes_read: 0,
                commit_count: ids_seen,
                parse_errors: 0,
            },
        }
    }

    // ── Stage chaining helpers ─────────────────────────────────────

    pub(super) async fn enqueue_next_stage(
        &self,
        run_id: i64,
        list_key: &str,
        stage: &str,
    ) -> Result<(), sqlx::Error> {
        let (job_type, payload_json, priority) = match stage {
            STAGE_THREADING => (
                "pipeline_threading",
                serde_json::to_value(PipelineThreadingPayload { run_id })
                    .unwrap_or_else(|_| serde_json::json!({})),
                PRIORITY_THREADING,
            ),
            STAGE_LINEAGE => (
                "pipeline_lineage",
                serde_json::to_value(PipelineLineagePayload { run_id })
                    .unwrap_or_else(|_| serde_json::json!({})),
                PRIORITY_LINEAGE,
            ),
            STAGE_LEXICAL => (
                "pipeline_lexical",
                serde_json::to_value(PipelineLexicalPayload { run_id })
                    .unwrap_or_else(|_| serde_json::json!({})),
                PRIORITY_LEXICAL,
            ),
            _ => return Ok(()),
        };

        self.jobs
            .enqueue(EnqueueJobParams {
                job_type: job_type.to_string(),
                payload_json,
                priority,
                dedupe_scope: Some(format!("list:{list_key}:pipeline")),
                dedupe_key: Some(format!("run:{run_id}:stage:{stage}")),
                run_after: None,
                max_attempts: Some(8),
            })
            .await?;
        Ok(())
    }

    pub(super) async fn enqueue_pipeline_embedding(
        &self,
        run_id: i64,
        list_key: &str,
        window_from: chrono::DateTime<Utc>,
        window_to: chrono::DateTime<Utc>,
        source_job_id: Option<i64>,
    ) -> Result<bool, sqlx::Error> {
        let payload = PipelineEmbeddingPayload {
            run_id,
            list_key: list_key.to_string(),
            window_from,
            window_to,
        };
        let source = source_job_id.unwrap_or_default();
        let dedupe_key = format!(
            "run:{}:{}:{}:{}",
            run_id,
            source,
            window_from.timestamp_millis(),
            window_to.timestamp_millis()
        );

        self.jobs
            .enqueue(EnqueueJobParams {
                job_type: "pipeline_embedding".to_string(),
                payload_json: serde_json::to_value(payload)
                    .unwrap_or_else(|_| serde_json::json!({})),
                priority: PRIORITY_PIPELINE_EMBEDDING,
                dedupe_scope: Some(format!("list:{list_key}:pipeline")),
                dedupe_key: Some(dedupe_key),
                run_after: None,
                max_attempts: Some(8),
            })
            .await?;
        Ok(true)
    }

    /// Activate the next pending run in the same batch, then fall back to the global pending queue.
    pub(super) async fn activate_and_enqueue_next_list(
        &self,
        batch_id: i64,
    ) -> Result<(), sqlx::Error> {
        if self
            .activate_and_enqueue_next_list_in_batch(batch_id)
            .await?
        {
            return Ok(());
        }
        let _ = self.activate_and_enqueue_next_list_global().await?;
        Ok(())
    }

    pub(super) async fn activate_and_enqueue_next_list_in_batch(
        &self,
        batch_id: i64,
    ) -> Result<bool, sqlx::Error> {
        loop {
            let Some(next_run) = self.pipeline.activate_next_pending_run(batch_id).await? else {
                return Ok(false);
            };

            let payload = PipelineIngestPayload {
                run_id: next_run.id,
            };
            match self
                .jobs
                .enqueue(EnqueueJobParams {
                    job_type: "pipeline_ingest".to_string(),
                    payload_json: serde_json::to_value(payload)
                        .unwrap_or_else(|_| serde_json::json!({})),
                    priority: PRIORITY_INGEST,
                    dedupe_scope: Some(format!("pipeline:run:{}", next_run.id)),
                    dedupe_key: Some("ingest".to_string()),
                    run_after: None,
                    max_attempts: Some(8),
                })
                .await
            {
                Ok(_) => {
                    info!(
                        run_id = next_run.id,
                        list_key = %next_run.list_key,
                        batch_id,
                        "activated next pending list in batch"
                    );
                    return Ok(true);
                }
                Err(err) => {
                    let reason =
                        format!("failed to enqueue pipeline_ingest after activation: {err}");
                    warn!(
                        run_id = next_run.id,
                        list_key = %next_run.list_key,
                        batch_id,
                        error = %err,
                        "marking activated run failed and trying next pending run"
                    );
                    self.pipeline.mark_run_failed(next_run.id, &reason).await?;
                }
            }
        }
    }

    pub(super) async fn activate_and_enqueue_next_list_global(&self) -> Result<bool, sqlx::Error> {
        loop {
            let Some(next_run) = self.pipeline.activate_next_pending_run_any().await? else {
                return Ok(false);
            };

            let payload = PipelineIngestPayload {
                run_id: next_run.id,
            };
            match self
                .jobs
                .enqueue(EnqueueJobParams {
                    job_type: "pipeline_ingest".to_string(),
                    payload_json: serde_json::to_value(payload)
                        .unwrap_or_else(|_| serde_json::json!({})),
                    priority: PRIORITY_INGEST,
                    dedupe_scope: Some(format!("pipeline:run:{}", next_run.id)),
                    dedupe_key: Some("ingest".to_string()),
                    run_after: None,
                    max_attempts: Some(8),
                })
                .await
            {
                Ok(_) => {
                    info!(
                        run_id = next_run.id,
                        list_key = %next_run.list_key,
                        "activated next pending list from global queue"
                    );
                    return Ok(true);
                }
                Err(err) => {
                    let reason =
                        format!("failed to enqueue pipeline_ingest after activation: {err}");
                    warn!(
                        run_id = next_run.id,
                        list_key = %next_run.list_key,
                        error = %err,
                        "marking globally activated run failed and trying next pending run"
                    );
                    self.pipeline.mark_run_failed(next_run.id, &reason).await?;
                }
            }
        }
    }

    // ── Shared helpers ─────────────────────────────────────────────

    pub(super) async fn write_ingest_rows(
        &self,
        repo: &nexus_db::MailingListRepo,
        rows: &[IngestCommitRow],
    ) -> nexus_db::Result<nexus_db::BatchWriteOutcome> {
        self.ingest.ingest_messages_copy_batch(repo, rows).await
    }

    pub(super) async fn apply_threading_for_anchors(
        &self,
        list_id: i64,
        _list_key: &str,
        anchors: Vec<i64>,
    ) -> nexus_db::Result<ThreadingWindowOutcome> {
        let mut anchors = anchors;
        let anchors_scanned = anchors.len() as u64;
        anchors.sort_unstable();
        anchors.dedup();
        anchors.retain(|v| *v > 0);
        let anchors_deduped = anchors.len() as u64;

        if anchors.is_empty() {
            return Ok(ThreadingWindowOutcome {
                anchors_scanned,
                anchors_deduped,
                ..ThreadingWindowOutcome::default()
            });
        }

        let mut threading_context = ThreadingRunContext::default();
        let mut message_set: BTreeSet<i64> = self
            .threading
            .expand_ancestor_closure_cached(list_id, &anchors, &mut threading_context)
            .await?
            .into_iter()
            .collect();

        let impacted_thread_ids = self
            .threading
            .find_impacted_thread_ids(list_id, &message_set.iter().copied().collect::<Vec<_>>())
            .await?;

        if !impacted_thread_ids.is_empty() {
            let prior_members = self
                .threading
                .list_message_pks_for_threads(list_id, &impacted_thread_ids)
                .await?;
            for message_pk in prior_members {
                message_set.insert(message_pk);
            }
        }

        let message_seed: Vec<i64> = message_set.iter().copied().collect();
        let expanded_message_set = self
            .threading
            .expand_ancestor_closure_cached(list_id, &message_seed, &mut threading_context)
            .await?;

        let source_messages = self
            .threading
            .load_source_messages_cached(list_id, &expanded_message_set, &mut threading_context)
            .await?;
        if source_messages.is_empty() {
            return Ok(ThreadingWindowOutcome {
                affected_threads: impacted_thread_ids.len() as u64,
                source_messages_read: 0,
                anchors_scanned,
                anchors_deduped,
                impacted_thread_ids,
                ..ThreadingWindowOutcome::default()
            });
        }

        let build_outcome = build_threads(
            source_messages
                .iter()
                .map(|src| ThreadingInputMessage {
                    message_pk: src.message_pk,
                    message_id_primary: src.message_id_primary.clone(),
                    subject_raw: src.subject_raw.clone(),
                    subject_norm: src.subject_norm.clone(),
                    date_utc: src.date_utc,
                    references_ids: src.references_ids.clone(),
                    in_reply_to_ids: src.in_reply_to_ids.clone(),
                })
                .collect(),
        );

        let components: Vec<ThreadComponentWrite> = build_outcome
            .components
            .into_iter()
            .map(|component| ThreadComponentWrite {
                summary: ThreadSummaryWrite {
                    root_node_key: component.summary.root_node_key,
                    root_message_pk: component.summary.root_message_pk,
                    subject_norm: component.summary.subject_norm,
                    created_at: component.summary.created_at,
                    last_activity_at: component.summary.last_activity_at,
                    message_count: component.summary.message_count,
                    membership_hash: component.summary.membership_hash,
                },
                nodes: component
                    .nodes
                    .into_iter()
                    .map(|node| ThreadNodeWrite {
                        node_key: node.node_key,
                        message_pk: node.message_pk,
                        parent_node_key: node.parent_node_key,
                        depth: node.depth,
                        sort_key: node.sort_key,
                        is_dummy: node.is_dummy,
                    })
                    .collect(),
                messages: component
                    .messages
                    .into_iter()
                    .map(|msg| ThreadMessageWrite {
                        message_pk: msg.message_pk,
                        parent_message_pk: msg.parent_message_pk,
                        depth: msg.depth,
                        sort_key: msg.sort_key,
                        is_dummy: msg.is_dummy,
                    })
                    .collect(),
            })
            .collect();

        let apply_stats = self
            .threading
            .apply_components(list_id, &impacted_thread_ids, &components)
            .await?;

        Ok(ThreadingWindowOutcome {
            affected_threads: impacted_thread_ids.len() as u64,
            source_messages_read: source_messages.len() as u64,
            anchors_scanned,
            anchors_deduped,
            apply_stats,
            impacted_thread_ids,
        })
    }
}
