use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::Path;
use std::time::Instant;

use gix::hash::ObjectId;
use nexus_core::config::Settings;
use nexus_db::{
    CatalogStore, EnqueueJobParams, IngestStore, Job, JobStore, JobStoreMetrics, LineageStore,
    ParsedBodyInput, ParsedMessageInput, ThreadComponentWrite, ThreadMessageWrite, ThreadNodeWrite,
    ThreadSummaryWrite, ThreadingStore,
};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::lineage::{
    process_diff_parse_patch_items, process_patch_extract_window, process_patch_id_compute_batch,
};
use crate::mail::{ParseEmailError, parse_email};
use crate::payloads::{
    DiffParsePatchItemsPayload, IngestCommitBatchPayload, LineageRebuildListPayload,
    PatchExtractWindowPayload, PatchIdComputeBatchPayload, RepoScanPayload,
    ThreadingRebuildListPayload, ThreadingUpdateWindowPayload,
};
use crate::scanner::{chunk_commit_oids, collect_new_commit_oids};
use crate::threading::{ThreadingInputMessage, build_threads};
use crate::{ExecutionContext, JobExecutionOutcome};

#[derive(Clone)]
pub struct Phase0JobHandler {
    settings: Settings,
    catalog: CatalogStore,
    ingest: IngestStore,
    threading: ThreadingStore,
    lineage: LineageStore,
    jobs: JobStore,
}

impl Phase0JobHandler {
    pub fn new(
        settings: Settings,
        catalog: CatalogStore,
        ingest: IngestStore,
        threading: ThreadingStore,
        lineage: LineageStore,
        jobs: JobStore,
    ) -> Self {
        Self {
            settings,
            catalog,
            ingest,
            threading,
            lineage,
            jobs,
        }
    }

    pub async fn handle(&self, job: Job, ctx: ExecutionContext) -> JobExecutionOutcome {
        match job.job_type.as_str() {
            "repo_scan" => self.handle_repo_scan(job, ctx).await,
            "ingest_commit_batch" => self.handle_ingest_commit_batch(job, ctx).await,
            "threading_update_window" => self.handle_threading_update_window(job, ctx).await,
            "threading_rebuild_list" => self.handle_threading_rebuild_list(job, ctx).await,
            "lineage_rebuild_list" => self.handle_lineage_rebuild_list(job, ctx).await,
            "patch_extract_window" => self.handle_patch_extract_window(job, ctx).await,
            "patch_id_compute_batch" => self.handle_patch_id_compute_batch(job, ctx).await,
            "diff_parse_patch_items" => self.handle_diff_parse_patch_items(job, ctx).await,
            other => JobExecutionOutcome::Terminal {
                reason: format!("unknown job type: {other}"),
                kind: "invalid_job_type".to_string(),
                metrics: empty_metrics(0),
            },
        }
    }

    async fn handle_repo_scan(&self, job: Job, _ctx: ExecutionContext) -> JobExecutionOutcome {
        let started = Instant::now();

        let payload: RepoScanPayload = match serde_json::from_value(job.payload_json.clone()) {
            Ok(v) => v,
            Err(err) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("invalid repo_scan payload: {err}"),
                    kind: "payload".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }
        };

        let list = match self.catalog.ensure_mailing_list(&payload.list_key).await {
            Ok(v) => v,
            Err(err) => {
                return retryable_error(
                    format!("failed to ensure mailing list: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let repo = match self
            .catalog
            .ensure_repo(list.id, &payload.repo_key, &payload.repo_key)
            .await
        {
            Ok(v) => v,
            Err(err) => {
                return retryable_error(
                    format!("failed to ensure repo: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let watermark = match self.catalog.get_watermark(repo.id).await {
            Ok(v) => v,
            Err(err) => {
                return retryable_error(
                    format!("failed to read watermark: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let since_commit_oid = payload.since_commit_oid.as_ref().or(watermark.as_ref());

        let commits = match collect_new_commit_oids(
            Path::new(&payload.mirror_path),
            since_commit_oid.map(|v| v.as_str()),
        ) {
            Ok(v) => v,
            Err(err) => {
                return retryable_error(
                    format!("repo scan failed: {err}"),
                    "io",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let chunks = chunk_commit_oids(&commits, self.settings.mail.commit_batch_size.max(1));

        let mut expected_prev = since_commit_oid.cloned();
        for (index, chunk) in chunks.iter().enumerate() {
            let chunk_first = chunk.first().cloned().unwrap_or_default();
            let chunk_last = chunk.last().cloned().unwrap_or_default();
            let dedupe_key = format!("{}:{}:{}", payload.repo_key, chunk_first, chunk_last);

            let ingest_payload = IngestCommitBatchPayload {
                list_key: payload.list_key.clone(),
                repo_key: payload.repo_key.clone(),
                chunk_index: index as u32,
                expected_prev_commit_oid: expected_prev.clone(),
                commit_oids: chunk.clone(),
            };

            if let Err(err) = self
                .jobs
                .enqueue(EnqueueJobParams {
                    job_type: "ingest_commit_batch".to_string(),
                    payload_json: serde_json::to_value(ingest_payload)
                        .unwrap_or_else(|_| serde_json::json!({})),
                    priority: 10,
                    dedupe_scope: Some(format!("repo:{}:scan:{}", repo.id, job.id)),
                    dedupe_key: Some(dedupe_key),
                    run_after: None,
                    max_attempts: Some(8),
                })
                .await
            {
                return retryable_error(
                    format!("failed to enqueue ingest chunk: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }

            expected_prev = Some(chunk_last);
        }

        let duration_ms = started.elapsed().as_millis();
        info!(
            list_key = %payload.list_key,
            repo_key = %payload.repo_key,
            commit_count = commits.len(),
            chunk_count = chunks.len(),
            "repo_scan queued ingest jobs"
        );

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "commit_count": commits.len(),
                "chunk_count": chunks.len(),
            }),
            metrics: JobStoreMetrics {
                duration_ms,
                rows_written: chunks.len() as u64,
                bytes_read: 0,
                commit_count: commits.len() as u64,
                parse_errors: 0,
            },
        }
    }

    async fn handle_ingest_commit_batch(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();

        let payload: IngestCommitBatchPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(v) => v,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid ingest payload: {err}"),
                        kind: "payload".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };

        let repo = match self
            .catalog
            .get_repo(&payload.list_key, &payload.repo_key)
            .await
        {
            Ok(Some(v)) => v,
            Ok(None) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!(
                        "repo not found for list_key={} repo_key={}",
                        payload.list_key, payload.repo_key
                    ),
                    kind: "not_found".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }
            Err(err) => {
                return retryable_error(
                    format!("failed to load repo metadata: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let current_watermark = match self.catalog.get_watermark(repo.id).await {
            Ok(v) => v,
            Err(err) => {
                return retryable_error(
                    format!("failed to read watermark: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        if current_watermark != payload.expected_prev_commit_oid {
            return retryable_error(
                format!(
                    "watermark mismatch for repo {}: expected {:?}, found {:?}",
                    payload.repo_key, payload.expected_prev_commit_oid, current_watermark
                ),
                "ordering",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        let repo_path = Path::new(&self.settings.mail.mirror_root)
            .join(&payload.list_key)
            .join(&repo.repo_relpath);

        let mut gix_repo = match gix::open(&repo_path) {
            Ok(v) => v,
            Err(err) => {
                return retryable_error(
                    format!("failed to open repo {:?}: {err}", repo_path),
                    "io",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        gix_repo.object_cache_size_if_unset(64 * 1024 * 1024);

        let mut rows_written = 0u64;
        let mut bytes_read = 0u64;
        let mut parse_errors = 0u64;
        let mut processed_commits = 0u64;
        let mut last_success_commit: Option<String> = None;
        let mut anchor_message_pks = BTreeSet::new();

        for (idx, commit_oid) in payload.commit_oids.iter().enumerate() {
            if idx % 8 == 0 {
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
                                commit_count: processed_commits,
                                parse_errors,
                            },
                        };
                    }
                    Ok(false) => {}
                    Err(err) => {
                        warn!(job_id = job.id, error = %err, "cancel check failed");
                    }
                }
            }

            let raw_mail = match read_mail_blob(&gix_repo, commit_oid) {
                Ok(Some(v)) => v,
                Ok(None) => {
                    parse_errors += 1;
                    continue;
                }
                Err(err) => {
                    return retryable_error(
                        format!("failed to read commit blob {commit_oid}: {err}"),
                        "io",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };

            bytes_read += raw_mail.len() as u64;
            processed_commits += 1;

            let parsed = match parse_email(&raw_mail) {
                Ok(v) => v,
                Err(ParseEmailError::MissingMessageId | ParseEmailError::MissingAuthorEmail) => {
                    parse_errors += 1;
                    continue;
                }
                Err(err) => {
                    parse_errors += 1;
                    warn!(commit_oid = %commit_oid, error = %err, "mail parse error");
                    continue;
                }
            };

            let parsed_input = ParsedMessageInput {
                content_hash_sha256: parsed.content_hash_sha256,
                subject_raw: parsed.subject_raw,
                subject_norm: parsed.subject_norm,
                from_name: parsed.from_name,
                from_email: parsed.from_email,
                date_utc: parsed.date_utc,
                to_raw: parsed.to_raw,
                cc_raw: parsed.cc_raw,
                message_ids: parsed.message_ids,
                message_id_primary: parsed.message_id_primary,
                in_reply_to_ids: parsed.in_reply_to_ids,
                references_ids: parsed.references_ids,
                mime_type: parsed.mime_type,
                body: ParsedBodyInput {
                    raw_rfc822: raw_mail,
                    body_text: parsed.body_text,
                    diff_text: parsed.diff_text,
                    search_text: parsed.search_text,
                    has_diff: parsed.has_diff,
                    has_attachments: parsed.has_attachments,
                },
            };

            match self
                .ingest
                .ingest_message(&repo, commit_oid, &parsed_input)
                .await
            {
                Ok(outcome) => {
                    if outcome.instance_inserted {
                        rows_written += 1;
                    }
                    if let Some(message_pk) = outcome.message_pk {
                        anchor_message_pks.insert(message_pk);
                    }
                    last_success_commit = Some(commit_oid.clone());
                }
                Err(err) => {
                    return retryable_error(
                        format!("ingest write failed for commit {commit_oid}: {err}"),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            }
        }

        let anchor_message_pks: Vec<i64> = anchor_message_pks.into_iter().collect();
        let mut threading_enqueued = false;
        if !anchor_message_pks.is_empty() {
            let dedupe_key = hash_message_pks(&anchor_message_pks);
            let threading_payload = ThreadingUpdateWindowPayload {
                list_key: payload.list_key.clone(),
                anchor_message_pks: anchor_message_pks.clone(),
                source_job_id: Some(job.id),
            };

            if let Err(err) = self
                .jobs
                .enqueue(EnqueueJobParams {
                    job_type: "threading_update_window".to_string(),
                    payload_json: serde_json::to_value(threading_payload)
                        .unwrap_or_else(|_| serde_json::json!({})),
                    priority: 12,
                    dedupe_scope: Some(format!("list:{}", payload.list_key)),
                    dedupe_key: Some(dedupe_key),
                    run_after: None,
                    max_attempts: Some(8),
                })
                .await
            {
                return retryable_error(
                    format!("failed to enqueue threading update: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            threading_enqueued = true;
        }

        if let Some(last_commit) = last_success_commit.as_deref()
            && let Err(err) = self
                .catalog
                .update_watermark(repo.id, Some(last_commit))
                .await
        {
            return retryable_error(
                format!("failed to update watermark: {err}"),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "list_key": payload.list_key,
                "repo_key": payload.repo_key,
                "chunk_index": payload.chunk_index,
                "commit_count": processed_commits,
                "rows_written": rows_written,
                "parse_errors": parse_errors,
                "last_success_commit": last_success_commit,
                "threading_enqueued": threading_enqueued,
                "anchor_message_count": anchor_message_pks.len(),
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written,
                bytes_read,
                commit_count: processed_commits,
                parse_errors,
            },
        }
    }

    async fn handle_threading_update_window(
        &self,
        job: Job,
        _ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();

        let payload: ThreadingUpdateWindowPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(v) => v,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid threading_update_window payload: {err}"),
                        kind: "payload".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };

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

        let mut anchors = payload.anchor_message_pks;
        anchors.sort_unstable();
        anchors.dedup();
        anchors.retain(|v| *v > 0);

        if anchors.is_empty() {
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "list_key": payload.list_key,
                    "affected_threads": 0,
                    "threads_rebuilt": 0,
                    "nodes_written": 0,
                    "dummy_nodes_written": 0,
                    "messages_written": 0,
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        let mut message_set: BTreeSet<i64> = match self
            .threading
            .expand_ancestor_closure(list.id, &anchors)
            .await
        {
            Ok(values) => values.into_iter().collect(),
            Err(err) => {
                return retryable_error(
                    format!("failed to build ancestor closure: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let impacted_thread_ids = match self
            .threading
            .find_impacted_thread_ids(list.id, &message_set.iter().copied().collect::<Vec<_>>())
            .await
        {
            Ok(v) => v,
            Err(err) => {
                return retryable_error(
                    format!("failed to load impacted threads: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        if !impacted_thread_ids.is_empty() {
            let prior_members = match self
                .threading
                .list_message_pks_for_threads(list.id, &impacted_thread_ids)
                .await
            {
                Ok(v) => v,
                Err(err) => {
                    return retryable_error(
                        format!("failed to load prior thread members: {err}"),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };
            for message_pk in prior_members {
                message_set.insert(message_pk);
            }
        }

        let message_seed: Vec<i64> = message_set.iter().copied().collect();
        let expanded_message_set: Vec<i64> = match self
            .threading
            .expand_ancestor_closure(list.id, &message_seed)
            .await
        {
            Ok(v) => v,
            Err(err) => {
                return retryable_error(
                    format!("failed to expand ancestor closure after thread merge: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let source_messages = match self
            .threading
            .load_source_messages(list.id, &expanded_message_set)
            .await
        {
            Ok(v) => v,
            Err(err) => {
                return retryable_error(
                    format!("failed to load source messages: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        if source_messages.is_empty() {
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "list_key": payload.list_key,
                    "affected_threads": impacted_thread_ids.len(),
                    "threads_rebuilt": 0,
                    "nodes_written": 0,
                    "dummy_nodes_written": 0,
                    "messages_written": 0,
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
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

        let apply_stats = match self
            .threading
            .apply_components(list.id, &impacted_thread_ids, &components)
            .await
        {
            Ok(v) => v,
            Err(err) => {
                return retryable_error(
                    format!("failed to persist threading results: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        info!(
            list_key = %payload.list_key,
            source_job_id = payload.source_job_id,
            affected_threads = impacted_thread_ids.len(),
            threads_rebuilt = apply_stats.threads_rebuilt,
            nodes_written = apply_stats.nodes_written,
            dummy_nodes_written = apply_stats.dummy_nodes_written,
            messages_written = apply_stats.messages_written,
            stale_threads_removed = apply_stats.stale_threads_removed,
            "threading_update_window applied"
        );

        let rows_written = apply_stats.threads_rebuilt
            + apply_stats.nodes_written
            + apply_stats.messages_written
            + apply_stats.stale_threads_removed;

        let mut lineage_enqueued = false;
        if !anchors.is_empty() {
            let patch_extract_payload = PatchExtractWindowPayload {
                list_key: payload.list_key.clone(),
                anchor_message_pks: anchors.clone(),
                source_job_id: Some(job.id),
            };

            if let Err(err) = self
                .jobs
                .enqueue(EnqueueJobParams {
                    job_type: "patch_extract_window".to_string(),
                    payload_json: serde_json::to_value(patch_extract_payload)
                        .unwrap_or_else(|_| serde_json::json!({})),
                    priority: 11,
                    dedupe_scope: Some(format!("list:{}", payload.list_key)),
                    dedupe_key: Some(hash_message_pks(&anchors)),
                    run_after: None,
                    max_attempts: Some(8),
                })
                .await
            {
                return retryable_error(
                    format!("failed to enqueue patch_extract_window: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            lineage_enqueued = true;
        }

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "list_key": payload.list_key,
                "source_job_id": payload.source_job_id,
                "affected_threads": impacted_thread_ids.len(),
                "threads_rebuilt": apply_stats.threads_rebuilt,
                "nodes_written": apply_stats.nodes_written,
                "dummy_nodes_written": apply_stats.dummy_nodes_written,
                "messages_written": apply_stats.messages_written,
                "stale_threads_removed": apply_stats.stale_threads_removed,
                "lineage_enqueued": lineage_enqueued,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written,
                bytes_read: source_messages.len() as u64,
                commit_count: components.len() as u64,
                parse_errors: 0,
            },
        }
    }

    async fn handle_threading_rebuild_list(
        &self,
        job: Job,
        _ctx: ExecutionContext,
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

        let mut cursor = 0i64;
        let mut queued_chunks = 0u64;
        let mut queued_messages = 0u64;
        let batch_limit = self.settings.mail.commit_batch_size.max(1) as i64;

        loop {
            let chunk = match self
                .threading
                .list_message_pks_for_rebuild(
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
                        format!("failed to load rebuild chunk: {err}"),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };

            if chunk.is_empty() {
                break;
            }

            cursor = *chunk.last().unwrap_or(&cursor);
            let dedupe_key = hash_message_pks(&chunk);
            let update_payload = ThreadingUpdateWindowPayload {
                list_key: payload.list_key.clone(),
                anchor_message_pks: chunk.clone(),
                source_job_id: Some(job.id),
            };

            if let Err(err) = self
                .jobs
                .enqueue(EnqueueJobParams {
                    job_type: "threading_update_window".to_string(),
                    payload_json: serde_json::to_value(update_payload)
                        .unwrap_or_else(|_| serde_json::json!({})),
                    priority: 9,
                    dedupe_scope: Some(format!("list:{}", payload.list_key)),
                    dedupe_key: Some(dedupe_key),
                    run_after: None,
                    max_attempts: Some(8),
                })
                .await
            {
                return retryable_error(
                    format!("failed to enqueue threading update chunk: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }

            queued_chunks += 1;
            queued_messages += chunk.len() as u64;
        }

        info!(
            list_key = %payload.list_key,
            queued_chunks,
            queued_messages,
            "threading_rebuild_list enqueued update jobs"
        );

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "list_key": payload.list_key,
                "queued_chunks": queued_chunks,
                "queued_messages": queued_messages,
                "from_seen_at": payload.from_seen_at,
                "to_seen_at": payload.to_seen_at,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written: queued_chunks,
                bytes_read: 0,
                commit_count: queued_messages,
                parse_errors: 0,
            },
        }
    }

    async fn handle_lineage_rebuild_list(
        &self,
        job: Job,
        _ctx: ExecutionContext,
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

        let mut cursor = 0i64;
        let mut queued_chunks = 0u64;
        let mut queued_messages = 0u64;
        let batch_limit = self.settings.mail.commit_batch_size.max(1) as i64;

        loop {
            let chunk = match self
                .threading
                .list_message_pks_for_rebuild(
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
                        format!("failed to load lineage rebuild chunk: {err}"),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };

            if chunk.is_empty() {
                break;
            }

            cursor = *chunk.last().unwrap_or(&cursor);
            let dedupe_key = hash_message_pks(&chunk);
            let patch_extract_payload = PatchExtractWindowPayload {
                list_key: payload.list_key.clone(),
                anchor_message_pks: chunk.clone(),
                source_job_id: Some(job.id),
            };

            if let Err(err) = self
                .jobs
                .enqueue(EnqueueJobParams {
                    job_type: "patch_extract_window".to_string(),
                    payload_json: serde_json::to_value(patch_extract_payload)
                        .unwrap_or_else(|_| serde_json::json!({})),
                    priority: 10,
                    dedupe_scope: Some(format!("list:{}", payload.list_key)),
                    dedupe_key: Some(dedupe_key),
                    run_after: None,
                    max_attempts: Some(8),
                })
                .await
            {
                return retryable_error(
                    format!("failed to enqueue patch_extract_window chunk: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }

            queued_chunks += 1;
            queued_messages += chunk.len() as u64;
        }

        info!(
            list_key = %payload.list_key,
            queued_chunks,
            queued_messages,
            "lineage_rebuild_list enqueued patch extraction jobs"
        );

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "list_key": payload.list_key,
                "queued_chunks": queued_chunks,
                "queued_messages": queued_messages,
                "from_seen_at": payload.from_seen_at,
                "to_seen_at": payload.to_seen_at,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written: queued_chunks,
                bytes_read: 0,
                commit_count: queued_messages,
                parse_errors: 0,
            },
        }
    }

    async fn handle_patch_extract_window(
        &self,
        job: Job,
        _ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();

        let payload: PatchExtractWindowPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(v) => v,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid patch_extract_window payload: {err}"),
                        kind: "payload".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };

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

        let mut anchors = payload.anchor_message_pks.clone();
        anchors.sort_unstable();
        anchors.dedup();
        anchors.retain(|v| *v > 0);

        let extract_outcome =
            match process_patch_extract_window(&self.lineage, list.id, &anchors).await {
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

        let mut patch_id_compute_enqueued = false;
        let mut diff_parse_enqueued = false;
        if !extract_outcome.patch_item_ids.is_empty() {
            let dedupe_key = hash_i64_list(&extract_outcome.patch_item_ids);
            let patch_id_payload = PatchIdComputeBatchPayload {
                patch_item_ids: extract_outcome.patch_item_ids.clone(),
                source_job_id: Some(job.id),
            };

            if let Err(err) = self
                .jobs
                .enqueue(EnqueueJobParams {
                    job_type: "patch_id_compute_batch".to_string(),
                    payload_json: serde_json::to_value(patch_id_payload)
                        .unwrap_or_else(|_| serde_json::json!({})),
                    priority: 10,
                    dedupe_scope: Some(format!("list:{}", payload.list_key)),
                    dedupe_key: Some(dedupe_key.clone()),
                    run_after: None,
                    max_attempts: Some(8),
                })
                .await
            {
                return retryable_error(
                    format!("failed to enqueue patch_id_compute_batch: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            patch_id_compute_enqueued = true;

            let diff_parse_payload = DiffParsePatchItemsPayload {
                patch_item_ids: extract_outcome.patch_item_ids.clone(),
                source_job_id: Some(job.id),
            };

            if let Err(err) = self
                .jobs
                .enqueue(EnqueueJobParams {
                    job_type: "diff_parse_patch_items".to_string(),
                    payload_json: serde_json::to_value(diff_parse_payload)
                        .unwrap_or_else(|_| serde_json::json!({})),
                    priority: 10,
                    dedupe_scope: Some(format!("list:{}", payload.list_key)),
                    dedupe_key: Some(dedupe_key),
                    run_after: None,
                    max_attempts: Some(8),
                })
                .await
            {
                return retryable_error(
                    format!("failed to enqueue diff_parse_patch_items: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            diff_parse_enqueued = true;
        }

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "list_key": payload.list_key,
                "source_job_id": payload.source_job_id,
                "series_versions_written": extract_outcome.series_versions_written,
                "patch_items_written": extract_outcome.patch_items_written,
                "series_ids": extract_outcome.series_ids,
                "patch_item_ids_count": extract_outcome.patch_item_ids.len(),
                "patch_id_compute_enqueued": patch_id_compute_enqueued,
                "diff_parse_enqueued": diff_parse_enqueued,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written: extract_outcome.series_versions_written
                    + extract_outcome.patch_items_written,
                bytes_read: anchors.len() as u64,
                commit_count: extract_outcome.series_ids.len() as u64,
                parse_errors: 0,
            },
        }
    }

    async fn handle_patch_id_compute_batch(
        &self,
        job: Job,
        _ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();

        let payload: PatchIdComputeBatchPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(v) => v,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid patch_id_compute_batch payload: {err}"),
                        kind: "payload".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };

        let mut patch_item_ids = payload.patch_item_ids.clone();
        patch_item_ids.sort_unstable();
        patch_item_ids.dedup();
        patch_item_ids.retain(|v| *v > 0);

        let outcome = match process_patch_id_compute_batch(&self.lineage, &patch_item_ids).await {
            Ok(v) => v,
            Err(err) => {
                return retryable_error(
                    format!("patch-id compute failed: {err}"),
                    "parse",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "source_job_id": payload.source_job_id,
                "patch_item_ids_count": patch_item_ids.len(),
                "patch_items_updated": outcome.patch_items_updated,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written: outcome.patch_items_updated,
                bytes_read: 0,
                commit_count: patch_item_ids.len() as u64,
                parse_errors: 0,
            },
        }
    }

    async fn handle_diff_parse_patch_items(
        &self,
        job: Job,
        _ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();

        let payload: DiffParsePatchItemsPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(v) => v,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid diff_parse_patch_items payload: {err}"),
                        kind: "payload".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };

        let mut patch_item_ids = payload.patch_item_ids.clone();
        patch_item_ids.sort_unstable();
        patch_item_ids.dedup();
        patch_item_ids.retain(|v| *v > 0);

        let outcome = match process_diff_parse_patch_items(&self.lineage, &patch_item_ids).await {
            Ok(v) => v,
            Err(err) => {
                return retryable_error(
                    format!("diff metadata parse failed: {err}"),
                    "parse",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "source_job_id": payload.source_job_id,
                "patch_item_ids_count": patch_item_ids.len(),
                "patch_items_updated": outcome.patch_items_updated,
                "patch_item_files_written": outcome.patch_item_files_written,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written: outcome.patch_item_files_written,
                bytes_read: 0,
                commit_count: patch_item_ids.len() as u64,
                parse_errors: 0,
            },
        }
    }
}

fn empty_metrics(duration_ms: u128) -> JobStoreMetrics {
    JobStoreMetrics {
        duration_ms,
        rows_written: 0,
        bytes_read: 0,
        commit_count: 0,
        parse_errors: 0,
    }
}

fn retryable_error(
    reason: String,
    kind: &str,
    job: &Job,
    duration_ms: u128,
    settings: &Settings,
) -> JobExecutionOutcome {
    let backoff = nexus_db::JobStore::compute_backoff(
        settings.worker.base_backoff_ms,
        settings.worker.max_backoff_ms,
        job.attempt,
    );

    JobExecutionOutcome::Retryable {
        reason,
        kind: kind.to_string(),
        backoff_ms: backoff.num_milliseconds().max(1) as u64,
        metrics: empty_metrics(duration_ms),
    }
}

fn hash_message_pks(message_pks: &[i64]) -> String {
    hash_i64_list(message_pks)
}

fn hash_i64_list(values: &[i64]) -> String {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted.dedup();

    let mut hasher = Sha256::new();
    for value in &sorted {
        hasher.update(value.to_be_bytes());
    }

    let bytes = hasher.finalize();
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn read_mail_blob(repo: &gix::Repository, commit_oid: &str) -> anyhow::Result<Option<Vec<u8>>> {
    let commit_id = ObjectId::from_hex(commit_oid.as_bytes())?;
    let commit = repo
        .find_object(commit_id)?
        .try_into_commit()
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let tree = commit.tree()?;
    let mut blob_oid = None;

    for entry in tree.iter() {
        let entry = entry?;
        if entry.filename() == "m" {
            blob_oid = Some(entry.id().detach());
            break;
        }
    }

    let Some(blob_oid) = blob_oid else {
        return Ok(None);
    };

    let blob = repo
        .find_object(blob_oid)?
        .try_into_blob()
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    Ok(Some(blob.data.to_vec()))
}
