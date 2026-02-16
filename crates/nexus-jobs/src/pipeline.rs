use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::Utc;
use gix::hash::ObjectId;
use nexus_core::config::{IngestWriteMode, Settings};
use nexus_core::embeddings::{EmbeddingsClientError, OpenAiEmbeddingsClient};
use nexus_core::search::MeiliIndexKind;
use nexus_db::{
    CatalogStore, EmbeddingVectorUpsert, EmbeddingsStore, EnqueueJobParams, IngestCommitRow,
    IngestStore, Job, JobStore, JobStoreMetrics, LineageStore, MailingListRepo, ParsedBodyInput,
    ParsedMessageInput, PipelineStore, SearchStore, ThreadComponentWrite, ThreadMessageWrite,
    ThreadNodeWrite, ThreadSummaryWrite, ThreadingApplyStats, ThreadingRunContext, ThreadingStore,
};
use sha2::{Digest, Sha256};
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

use crate::lineage::{
    process_diff_parse_patch_items, process_patch_extract_window, process_patch_id_compute_batch,
};
use crate::mail::{ParseEmailError, parse_email};
use crate::meili::{MeiliClient, MeiliClientError, settings_differ};
use crate::payloads::{
    EmbeddingBackfillRunPayload, EmbeddingGenerateBatchPayload, EmbeddingScope,
    LineageRebuildListPayload, PipelineIngestPayload, PipelineLineagePayload,
    PipelineSearchPayload, PipelineThreadingPayload, ThreadingRebuildListPayload,
};
use crate::scanner::stream_new_commit_oid_chunks;
use crate::threading::{ThreadingInputMessage, build_threads};
use crate::{ExecutionContext, JobExecutionOutcome};

#[derive(Clone)]
pub struct Phase0JobHandler {
    settings: Settings,
    catalog: CatalogStore,
    ingest: IngestStore,
    threading: ThreadingStore,
    lineage: LineageStore,
    pipeline: PipelineStore,
    search: SearchStore,
    embeddings: EmbeddingsStore,
    jobs: JobStore,
    meili: MeiliClient,
    embedding_client: Option<OpenAiEmbeddingsClient>,
}

const STAGE_INGEST: &str = "ingest";
const STAGE_THREADING: &str = "threading";
const STAGE_LINEAGE: &str = "lineage";
const STAGE_SEARCH: &str = "search";

impl Phase0JobHandler {
    pub fn new(
        settings: Settings,
        catalog: CatalogStore,
        ingest: IngestStore,
        threading: ThreadingStore,
        lineage: LineageStore,
        pipeline: PipelineStore,
        search: SearchStore,
        embeddings: EmbeddingsStore,
        jobs: JobStore,
    ) -> Self {
        let meili = MeiliClient::from_settings(&settings);
        let embedding_client = OpenAiEmbeddingsClient::from_settings(&settings);
        Self {
            settings,
            catalog,
            ingest,
            threading,
            lineage,
            pipeline,
            search,
            embeddings,
            jobs,
            meili,
            embedding_client,
        }
    }

    pub async fn handle(&self, job: Job, ctx: ExecutionContext) -> JobExecutionOutcome {
        match job.job_type.as_str() {
            "pipeline_ingest" => self.handle_pipeline_ingest(job, ctx).await,
            "pipeline_threading" => self.handle_pipeline_threading(job, ctx).await,
            "pipeline_lineage" => self.handle_pipeline_lineage(job, ctx).await,
            "pipeline_search" => self.handle_pipeline_search(job, ctx).await,
            "threading_rebuild_list" => self.handle_threading_rebuild_list(job, ctx).await,
            "lineage_rebuild_list" => self.handle_lineage_rebuild_list(job, ctx).await,
            "embedding_backfill_run" => self.handle_embedding_backfill_run(job, ctx).await,
            "embedding_generate_batch" => self.handle_embedding_generate_batch(job, ctx).await,
            other => JobExecutionOutcome::Terminal {
                reason: format!("unknown job type: {other}"),
                kind: "invalid_job_type".to_string(),
                metrics: empty_metrics(0),
            },
        }
    }

    // ── Pipeline stage handlers ──────────────────────────────────

    async fn handle_pipeline_ingest(&self, job: Job, ctx: ExecutionContext) -> JobExecutionOutcome {
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

                let batch_outcome = match self
                    .write_ingest_rows(
                        &repo,
                        &parsed_outcome.rows,
                        self.settings.worker.db_relaxed_durability,
                    )
                    .await
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

    async fn handle_pipeline_threading(
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

        let batch_limit = self.settings.mail.commit_batch_size.max(1) as i64;
        let checkpoint_interval = self.settings.worker.progress_checkpoint_interval.max(1) as u64;
        let mut cursor = 0i64;
        let mut anchors = Vec::new();
        let mut anchors_scanned = 0u64;
        let mut messages_processed = 0u64;
        let mut threads_created = 0u64;
        let mut threads_updated = 0u64;
        let mut apply_stats = ThreadingApplyStats::default();
        let mut source_messages_read = 0u64;
        let mut anchors_deduped = 0u64;
        let mut items_since_checkpoint = 0u64;

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
                            rows_written: apply_stats.threads_rebuilt
                                + apply_stats.nodes_written
                                + apply_stats.messages_written,
                            bytes_read: 0,
                            commit_count: messages_processed,
                            parse_errors: 0,
                        },
                    };
                }
                Ok(false) => {}
                Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
            }

            let chunk = match self
                .pipeline
                .query_ingest_window_message_pks(
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

            if chunk.is_empty() {
                break;
            }
            let chunk_len = chunk.len() as u64;
            cursor = *chunk.last().unwrap_or(&cursor);
            anchors_scanned += chunk_len;
            anchors.extend(chunk);
            messages_processed = anchors_scanned;

            items_since_checkpoint += chunk_len;
            if items_since_checkpoint >= checkpoint_interval {
                items_since_checkpoint = 0;
                let progress = serde_json::json!({
                    "stage": STAGE_THREADING,
                    "messages_total": anchors_scanned,
                    "messages_processed": messages_processed,
                    "threads_created": threads_created,
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
                            + apply_stats.messages_written,
                        bytes_read: 0,
                        commit_count: messages_processed,
                        parse_errors: 0,
                    },
                };
            }
            Ok(false) => {}
            Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
        }

        let outcome = match self
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
        };

        anchors_scanned = outcome.anchors_scanned;
        messages_processed = anchors_scanned;
        anchors_deduped = outcome.anchors_deduped;
        source_messages_read = outcome.source_messages_read;
        threads_created = outcome.apply_stats.threads_rebuilt;
        threads_updated = outcome.affected_threads;
        apply_stats = outcome.apply_stats;

        let final_progress = serde_json::json!({
            "stage": STAGE_THREADING,
            "messages_total": anchors_scanned,
            "messages_processed": messages_processed,
            "threads_created": threads_created,
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
                "messages_total": anchors_scanned,
                "messages_processed": messages_processed,
                "threads_created": threads_created,
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

    async fn handle_pipeline_lineage(
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

        let batch_limit = self.settings.mail.commit_batch_size.max(1) as i64;
        let checkpoint_interval = self.settings.worker.progress_checkpoint_interval.max(1) as u64;
        let mut cursor = 0i64;
        let mut messages_processed = 0u64;
        let mut series_versions_written = 0u64;
        let mut patch_items_written = 0u64;
        let mut patch_item_files_written = 0u64;

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

            let chunk = match self
                .pipeline
                .query_ingest_window_message_pks(
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
                            "failed to query messages for lineage stage run {}: {err}",
                            run.id
                        ),
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

            let extract_outcome = match process_patch_extract_window(
                &self.lineage,
                run.mailing_list_id,
                &chunk,
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

            if !extract_outcome.patch_item_ids.is_empty() {
                let patch_item_ids = extract_outcome.patch_item_ids.clone();
                let (patch_id_result, diff_result) = tokio::join!(
                    process_patch_id_compute_batch(&self.lineage, &patch_item_ids),
                    process_diff_parse_patch_items(&self.lineage, &patch_item_ids),
                );

                if let Err(err) = patch_id_result {
                    return retryable_error(
                        format!("patch_id compute failed for run {}: {err}", run.id),
                        "parse",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
                match diff_result {
                    Ok(diff_outcome) => {
                        patch_item_files_written += diff_outcome.patch_item_files_written;
                    }
                    Err(err) => {
                        return retryable_error(
                            format!("diff parse failed for run {}: {err}", run.id),
                            "parse",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                }
            }

            messages_processed += chunk.len() as u64;
            series_versions_written += extract_outcome.series_versions_written;
            patch_items_written += extract_outcome.patch_items_written;

            if messages_processed % checkpoint_interval < chunk.len() as u64 {
                let progress = serde_json::json!({
                    "stage": STAGE_LINEAGE,
                    "messages_processed": messages_processed,
                    "series_versions_written": series_versions_written,
                    "patch_items_written": patch_items_written,
                    "patch_files_written": patch_item_files_written,
                });
                if let Err(err) = self.pipeline.update_run_progress(run.id, progress).await {
                    warn!(run_id = run.id, error = %err, "progress checkpoint failed");
                }
                info!(
                    target: "worker_job",
                    run_id = run.id,
                    list_key = %run.list_key,
                    stage = STAGE_LINEAGE,
                    items_processed = messages_processed,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "pipeline checkpoint"
                );
            }
        }

        if let Err(err) = self
            .enqueue_next_stage(run.id, &run.list_key, STAGE_SEARCH)
            .await
        {
            return retryable_error(
                format!("failed to enqueue search stage for run {}: {err}", run.id),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }
        if let Err(err) = self
            .pipeline
            .set_run_current_stage(run.id, STAGE_SEARCH)
            .await
        {
            return retryable_error(
                format!("failed to move run {} to search stage: {err}", run.id),
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
                "messages_processed": messages_processed,
                "series_versions_written": series_versions_written,
                "patch_items_written": patch_items_written,
                "patch_item_files_written": patch_item_files_written,
                "next_stage": STAGE_SEARCH,
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

    async fn handle_pipeline_search(&self, job: Job, ctx: ExecutionContext) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: PipelineSearchPayload = match serde_json::from_value(job.payload_json.clone())
        {
            Ok(value) => value,
            Err(err) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("invalid pipeline_search payload: {err}"),
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
        let chunk_limit = self.settings.mail.commit_batch_size.max(1);
        let mut embedding_jobs_enqueued = 0u64;

        for index in [
            MeiliIndexKind::ThreadDocs,
            MeiliIndexKind::PatchSeriesDocs,
            MeiliIndexKind::PatchItemDocs,
        ] {
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
            let mut ids_seen = 0u64;
            let mut id_cursor = 0i64;

            loop {
                if let Err(err) = ctx.heartbeat().await {
                    warn!(job_id = job.id, error = %err, "heartbeat update failed");
                }

                let ids = match index {
                    MeiliIndexKind::ThreadDocs => {
                        self.pipeline
                            .query_impacted_thread_ids_chunk(
                                run.mailing_list_id,
                                window_from,
                                window_to,
                                id_cursor,
                                chunk_limit as i64,
                            )
                            .await
                    }
                    MeiliIndexKind::PatchSeriesDocs => {
                        self.pipeline
                            .query_impacted_series_ids_chunk(
                                run.mailing_list_id,
                                window_from,
                                window_to,
                                id_cursor,
                                chunk_limit as i64,
                            )
                            .await
                    }
                    MeiliIndexKind::PatchItemDocs => {
                        self.pipeline
                            .query_impacted_patch_item_ids_chunk(
                                run.mailing_list_id,
                                window_from,
                                window_to,
                                id_cursor,
                                chunk_limit as i64,
                            )
                            .await
                    }
                };
                let ids = match ids {
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
                if ids.is_empty() {
                    break;
                }
                id_cursor = *ids.last().unwrap_or(&id_cursor);
                chunks_done += 1;
                ids_seen += ids.len() as u64;

                if self.settings.embeddings.enabled {
                    let scope = match index {
                        MeiliIndexKind::ThreadDocs => Some(EmbeddingScope::Thread),
                        MeiliIndexKind::PatchSeriesDocs => Some(EmbeddingScope::Series),
                        MeiliIndexKind::PatchItemDocs => None,
                    };
                    if let Some(scope) = scope {
                        match self
                            .enqueue_embedding_generate_batch(
                                &run.list_key,
                                scope,
                                &ids,
                                Some(job.id),
                                4,
                            )
                            .await
                        {
                            Ok(true) => embedding_jobs_enqueued += 1,
                            Ok(false) => {}
                            Err(err) => {
                                warn!(
                                    run_id = run.id,
                                    scope = scope.as_str(),
                                    error = %err,
                                    "failed to enqueue embedding batch"
                                );
                            }
                        }
                    }
                }

                let docs = match self.build_docs_for_index(index, &ids).await {
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
                    "failed to mark run {} succeeded after search stage: {err}",
                    run.id
                ),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        // Activate next pending list in the batch
        if let Some(batch_id) = run.batch_id {
            if let Err(err) = self.activate_and_enqueue_next_list(batch_id).await {
                warn!(
                    run_id = run.id,
                    batch_id,
                    error = %err,
                    "failed to activate next pending list in batch"
                );
            }
        }

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "run_id": run.id,
                "list_key": run.list_key,
                "stage": STAGE_SEARCH,
                "total_docs_upserted": total_docs_upserted,
                "total_ids_seen": total_ids_seen,
                "families": family_results,
                "embedding_jobs_enqueued": embedding_jobs_enqueued,
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

    // ── Stage chaining helpers ─────────────────────────────────────

    async fn enqueue_next_stage(
        &self,
        run_id: i64,
        list_key: &str,
        stage: &str,
    ) -> Result<(), sqlx::Error> {
        let (job_type, payload_json) = match stage {
            STAGE_THREADING => (
                "pipeline_threading",
                serde_json::to_value(PipelineThreadingPayload { run_id })
                    .unwrap_or_else(|_| serde_json::json!({})),
            ),
            STAGE_LINEAGE => (
                "pipeline_lineage",
                serde_json::to_value(PipelineLineagePayload { run_id })
                    .unwrap_or_else(|_| serde_json::json!({})),
            ),
            STAGE_SEARCH => (
                "pipeline_search",
                serde_json::to_value(PipelineSearchPayload { run_id })
                    .unwrap_or_else(|_| serde_json::json!({})),
            ),
            _ => return Ok(()),
        };

        self.jobs
            .enqueue(EnqueueJobParams {
                job_type: job_type.to_string(),
                payload_json,
                priority: 10,
                dedupe_scope: Some(format!("list:{list_key}:pipeline")),
                dedupe_key: Some(format!("run:{run_id}:stage:{stage}")),
                run_after: None,
                max_attempts: Some(8),
            })
            .await?;
        Ok(())
    }

    /// Activate the next pending run in a batch and enqueue its ingest stage.
    async fn activate_and_enqueue_next_list(&self, batch_id: i64) -> Result<(), sqlx::Error> {
        loop {
            let Some(next_run) = self.pipeline.activate_next_pending_run(batch_id).await? else {
                return Ok(());
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
                    priority: 20,
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
                    return Ok(());
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

    // ── Shared helpers ─────────────────────────────────────────────

    async fn write_ingest_rows(
        &self,
        repo: &nexus_db::MailingListRepo,
        rows: &[IngestCommitRow],
        relaxed_durability: bool,
    ) -> nexus_db::Result<nexus_db::BatchWriteOutcome> {
        match self.settings.worker.ingest_write_mode {
            IngestWriteMode::Copy => {
                self.ingest
                    .ingest_messages_copy_batch(repo, rows, relaxed_durability)
                    .await
            }
            IngestWriteMode::BatchedSql => {
                self.ingest
                    .ingest_messages_batch(repo, rows, relaxed_durability)
                    .await
            }
        }
    }

    async fn apply_threading_for_anchors(
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
    async fn handle_threading_rebuild_list(
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

        let mut cursor = 0i64;
        let mut processed_chunks = 0u64;
        let mut processed_messages = 0u64;
        let mut affected_threads = 0u64;
        let mut meili_docs_upserted = 0u64;
        let mut apply_stats = ThreadingApplyStats::default();
        let batch_limit = self.settings.mail.commit_batch_size.max(1) as i64;

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
            let outcome = match self
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
            };

            processed_chunks += 1;
            processed_messages += chunk.len() as u64;
            affected_threads += outcome.affected_threads;
            apply_stats.threads_rebuilt += outcome.apply_stats.threads_rebuilt;
            apply_stats.threads_unchanged_skipped += outcome.apply_stats.threads_unchanged_skipped;
            apply_stats.nodes_written += outcome.apply_stats.nodes_written;
            apply_stats.dummy_nodes_written += outcome.apply_stats.dummy_nodes_written;
            apply_stats.messages_written += outcome.apply_stats.messages_written;
            apply_stats.stale_threads_removed += outcome.apply_stats.stale_threads_removed;

            // Inline meili upsert for impacted threads
            if !outcome.impacted_thread_ids.is_empty() {
                match self
                    .upsert_meili_docs_inline(index, &outcome.impacted_thread_ids, &ctx)
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
        }

        info!(
            list_key = %payload.list_key,
            processed_chunks,
            processed_messages,
            affected_threads,
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
                "processed_chunks": processed_chunks,
                "processed_messages": processed_messages,
                "affected_threads": affected_threads,
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

    async fn handle_lineage_rebuild_list(
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

        let mut cursor = 0i64;
        let mut processed_chunks = 0u64;
        let mut processed_messages = 0u64;
        let mut series_versions_written = 0u64;
        let mut patch_items_written = 0u64;
        let mut patch_item_files_written = 0u64;
        let batch_limit = self.settings.mail.commit_batch_size.max(1) as i64;

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

            // Inline patch extraction + patch-id compute + diff parse
            let extract_outcome =
                match process_patch_extract_window(&self.lineage, list.id, &chunk).await {
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

            if !extract_outcome.patch_item_ids.is_empty() {
                let patch_item_ids = extract_outcome.patch_item_ids.clone();
                let (patch_id_result, diff_result) = tokio::join!(
                    process_patch_id_compute_batch(&self.lineage, &patch_item_ids),
                    process_diff_parse_patch_items(&self.lineage, &patch_item_ids),
                );

                if let Err(err) = patch_id_result {
                    return retryable_error(
                        format!("patch_id compute failed: {err}"),
                        "parse",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
                match diff_result {
                    Ok(diff_outcome) => {
                        patch_item_files_written += diff_outcome.patch_item_files_written;
                    }
                    Err(err) => {
                        return retryable_error(
                            format!("diff parse failed: {err}"),
                            "parse",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                }
            }

            processed_chunks += 1;
            processed_messages += chunk.len() as u64;
            series_versions_written += extract_outcome.series_versions_written;
            patch_items_written += extract_outcome.patch_items_written;
        }

        info!(
            list_key = %payload.list_key,
            processed_chunks,
            processed_messages,
            series_versions_written,
            patch_items_written,
            patch_item_files_written,
            "lineage_rebuild_list completed inline"
        );

        let rows_written = series_versions_written + patch_items_written + patch_item_files_written;

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "list_key": payload.list_key,
                "processed_chunks": processed_chunks,
                "processed_messages": processed_messages,
                "series_versions_written": series_versions_written,
                "patch_items_written": patch_items_written,
                "patch_item_files_written": patch_item_files_written,
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

    async fn handle_embedding_backfill_run(
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

        if !self.settings.embeddings.enabled {
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "run_id": payload.run_id,
                    "skipped": "embeddings_disabled"
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

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
        let chunk_limit = self.settings.mail.commit_batch_size.max(1) as i64;
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
                            commit_count: processed as u64,
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
            processed += ids.len() as i64;

            match self
                .enqueue_embedding_generate_batch(&list_key, scope, &ids, Some(job.id), 4)
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
                    cursor,
                    processed,
                    processed,
                    0,
                    total_candidates,
                    serde_json::json!({
                        "cursor_id": cursor,
                        "processed_count": processed,
                        "jobs_enqueued": jobs_enqueued,
                        "total_candidates": total_candidates,
                    }),
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
                rows_written: jobs_enqueued as u64,
                bytes_read: 0,
                commit_count: processed as u64,
                parse_errors: 0,
            },
        }
    }

    async fn handle_embedding_generate_batch(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: EmbeddingGenerateBatchPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(value) => value,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid embedding_generate_batch payload: {err}"),
                        kind: "payload".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };

        if !self.settings.embeddings.enabled {
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "scope": payload.scope.as_str(),
                    "ids_count": payload.ids.len(),
                    "skipped": "embeddings_disabled",
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }
        if payload.model_key != self.settings.embeddings.model {
            return JobExecutionOutcome::Terminal {
                reason: format!(
                    "unsupported model_key {} (configured model is {})",
                    payload.model_key, self.settings.embeddings.model
                ),
                kind: "payload".to_string(),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        let mut ids = payload.ids.clone();
        ids.sort_unstable();
        ids.dedup();
        ids.retain(|value| *value > 0);
        if ids.is_empty() {
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "scope": payload.scope.as_str(),
                    "ids_count": 0,
                    "source_job_id": payload.source_job_id,
                    "skipped": "empty_id_set",
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        let inputs = match self
            .embeddings
            .build_embedding_inputs(payload.scope.as_str(), &ids)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                let stage_name = embedding_doc_build_stage(payload.scope.as_str());
                let failing_doc_id = self
                    .find_first_failing_embedding_doc_id(payload.scope.as_str(), &ids)
                    .await;
                let failing_source = if matches!(payload.scope, EmbeddingScope::Thread) {
                    if let Some(thread_id) = failing_doc_id {
                        match self.embeddings.lookup_thread_message_body(thread_id).await {
                            Ok(value) => value.map(|(message_pk, body_id)| {
                                format!("message_pk={message_pk} body_id={body_id}")
                            }),
                            Err(_) => None,
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                return retryable_error(
                    format!(
                        "failed to build embedding inputs for scope {}: stage={} ids_count={} failing_doc_id={:?} failing_source={:?} error={err}",
                        payload.scope.as_str(),
                        stage_name,
                        ids.len(),
                        failing_doc_id,
                        failing_source
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };
        if inputs.is_empty() {
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "scope": payload.scope.as_str(),
                    "ids_count": ids.len(),
                    "source_job_id": payload.source_job_id,
                    "skipped": "no_inputs_for_ids",
                }),
                metrics: JobStoreMetrics {
                    duration_ms: started.elapsed().as_millis(),
                    rows_written: 0,
                    bytes_read: 0,
                    commit_count: ids.len() as u64,
                    parse_errors: 0,
                },
            };
        }

        let batch_size = self
            .settings
            .embeddings
            .batch_size
            .max(1)
            .saturating_mul(self.settings.embeddings.max_inflight_requests.max(1));
        let min_interval = self.settings.embeddings.min_request_interval_ms;
        let chunk_count = inputs.chunks(batch_size).len();
        let mut upserts: Vec<EmbeddingVectorUpsert> = Vec::with_capacity(inputs.len());
        let mut bytes_read = 0u64;
        let mut request_count = 0u64;
        for (idx, chunk) in inputs.chunks(batch_size).enumerate() {
            if let Err(err) = ctx.heartbeat().await {
                warn!(job_id = job.id, error = %err, "heartbeat update failed");
            }
            let text_batch = chunk
                .iter()
                .map(|row| row.text.clone())
                .collect::<Vec<String>>();
            let vectors = match self.embed_text_batch_with_retry(&text_batch).await {
                Ok(value) => value,
                Err(err) => {
                    if err.is_transient() {
                        return retryable_error(
                            format!(
                                "embedding request failed for scope {} batch {}: {err}",
                                payload.scope.as_str(),
                                idx
                            ),
                            "embeddings",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                    return JobExecutionOutcome::Terminal {
                        reason: format!(
                            "embedding request failed for scope {} batch {}: {err}",
                            payload.scope.as_str(),
                            idx
                        ),
                        kind: "embeddings".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };
            if vectors.len() != chunk.len() {
                return JobExecutionOutcome::Terminal {
                    reason: format!(
                        "embedding vector count mismatch for scope {}: inputs={} vectors={}",
                        payload.scope.as_str(),
                        chunk.len(),
                        vectors.len()
                    ),
                    kind: "embeddings".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }

            request_count += 1;
            for (row, vector) in chunk.iter().zip(vectors.into_iter()) {
                if vector.len() != self.settings.embeddings.dimensions {
                    return JobExecutionOutcome::Terminal {
                        reason: format!(
                            "embedding dimensions mismatch for scope {} doc_id={} expected={} got={}",
                            payload.scope.as_str(),
                            row.doc_id,
                            self.settings.embeddings.dimensions,
                            vector.len()
                        ),
                        kind: "embeddings".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
                bytes_read += row.text.len() as u64;
                upserts.push(EmbeddingVectorUpsert {
                    doc_id: row.doc_id,
                    vector,
                    source_hash: row.source_hash.clone(),
                });
            }

            if min_interval > 0 && idx + 1 < chunk_count {
                sleep(Duration::from_millis(min_interval)).await;
            }
        }

        let rows_written = match self
            .embeddings
            .upsert_vectors(
                payload.scope.as_str(),
                &self.settings.embeddings.model,
                self.settings.embeddings.dimensions as i32,
                &upserts,
            )
            .await
        {
            Ok(value) => value,
            Err(err) => {
                return retryable_error(
                    format!(
                        "failed writing embeddings for scope {}: upserts={} error={err}",
                        payload.scope.as_str(),
                        upserts.len()
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let index = match payload.scope {
            EmbeddingScope::Thread => MeiliIndexKind::ThreadDocs,
            EmbeddingScope::Series => MeiliIndexKind::PatchSeriesDocs,
        };
        let meili_docs_upserted = match self.upsert_meili_docs_inline(index, &ids, &ctx).await {
            Ok(n) => n,
            Err(err) => {
                return retryable_error(
                    format!("failed to upsert meili docs after embeddings: {err}"),
                    "meili",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "scope": payload.scope.as_str(),
                "model_key": payload.model_key,
                "ids_count": ids.len(),
                "inputs_count": inputs.len(),
                "request_count": request_count,
                "vectors_written": rows_written,
                "source_job_id": payload.source_job_id,
                "meili_docs_upserted": meili_docs_upserted,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written,
                bytes_read,
                commit_count: ids.len() as u64,
                parse_errors: 0,
            },
        }
    }

    async fn ensure_index_settings(
        &self,
        index_spec: &'static nexus_core::search::MeiliIndexSpec,
        settings: &serde_json::Value,
        ctx: &ExecutionContext,
    ) -> Result<(), MeiliClientError> {
        let mut current_settings = self.meili.get_settings(index_spec.uid).await?;
        if current_settings.is_none() {
            if let Some(task_uid) = self.meili.ensure_index_exists(index_spec).await? {
                match self.wait_for_task(task_uid, ctx).await {
                    Ok(()) => {}
                    Err(MeiliClientError::Protocol(message))
                        if message.contains("code=index_already_exists") => {}
                    Err(err) => return Err(err),
                }
            }
            current_settings = self.meili.get_settings(index_spec.uid).await?;
        }

        if settings_differ(current_settings.as_ref(), settings) {
            let task_uid = self.meili.update_settings(index_spec.uid, settings).await?;
            self.wait_for_task(task_uid, ctx).await?;
        }

        Ok(())
    }

    async fn wait_for_task(
        &self,
        task_uid: i64,
        ctx: &ExecutionContext,
    ) -> Result<(), MeiliClientError> {
        loop {
            let _ = ctx.heartbeat().await;
            let task = self.meili.get_task(task_uid).await?;
            match task.status.as_str() {
                "enqueued" | "processing" => {
                    sleep(Duration::from_millis(300)).await;
                }
                "succeeded" => return Ok(()),
                "failed" | "canceled" => {
                    let code = task.error_code.unwrap_or_else(|| "unknown".to_string());
                    let message = task
                        .error_message
                        .unwrap_or_else(|| "unknown meili task failure".to_string());
                    return Err(MeiliClientError::Protocol(format!(
                        "task status={} code={} message={}",
                        task.status, code, message
                    )));
                }
                other => {
                    return Err(MeiliClientError::Protocol(format!(
                        "unexpected task status: {other}"
                    )));
                }
            }
        }
    }

    fn meili_index_settings(&self, index: MeiliIndexKind) -> serde_json::Value {
        let mut settings = index.spec().settings_json();
        if self.settings.embeddings.enabled
            && matches!(
                index,
                MeiliIndexKind::ThreadDocs | MeiliIndexKind::PatchSeriesDocs
            )
        {
            settings["embedders"] = serde_json::json!({
                self.settings.embeddings.embedder_name.clone(): {
                    "source": "userProvided",
                    "dimensions": self.settings.embeddings.dimensions
                }
            });
        }
        settings
    }

    async fn build_docs_for_index(
        &self,
        index: MeiliIndexKind,
        ids: &[i64],
    ) -> nexus_db::Result<Vec<serde_json::Value>> {
        let mut docs = match index {
            MeiliIndexKind::PatchItemDocs => self.search.build_patch_item_docs(ids).await?,
            MeiliIndexKind::PatchSeriesDocs => self.search.build_patch_series_docs(ids).await?,
            MeiliIndexKind::ThreadDocs => self.search.build_thread_docs(ids).await?,
        };

        if !self.settings.embeddings.enabled {
            return Ok(docs);
        }

        let scope = match index {
            MeiliIndexKind::ThreadDocs => Some(EmbeddingScope::Thread),
            MeiliIndexKind::PatchSeriesDocs => Some(EmbeddingScope::Series),
            MeiliIndexKind::PatchItemDocs => None,
        };
        let Some(scope) = scope else {
            return Ok(docs);
        };

        let vectors = self
            .embeddings
            .get_vectors(scope.as_str(), &self.settings.embeddings.model, ids)
            .await?;
        let embedder_name = self.settings.embeddings.embedder_name.clone();

        for doc in &mut docs {
            let Some(doc_obj) = doc.as_object_mut() else {
                continue;
            };
            let Some(id) = doc_obj.get("id").and_then(|value| value.as_i64()) else {
                continue;
            };
            let vector_value = vectors
                .get(&id)
                .map(|vector| serde_json::json!(vector))
                .unwrap_or(serde_json::Value::Null);
            doc_obj.insert(
                "_vectors".to_string(),
                serde_json::json!({
                    embedder_name.clone(): vector_value
                }),
            );
        }

        Ok(docs)
    }

    async fn find_first_failing_embedding_doc_id(&self, scope: &str, ids: &[i64]) -> Option<i64> {
        for doc_id in ids {
            if self
                .embeddings
                .build_embedding_inputs(scope, &[*doc_id])
                .await
                .is_err()
            {
                return Some(*doc_id);
            }
        }
        None
    }

    /// Build docs and upsert them into Meilisearch inline (replaces enqueue_meili_upsert_batch).
    async fn upsert_meili_docs_inline(
        &self,
        index: MeiliIndexKind,
        ids: &[i64],
        ctx: &ExecutionContext,
    ) -> Result<u64, MeiliClientError> {
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.dedup();
        ids.retain(|v| *v > 0);
        if ids.is_empty() {
            return Ok(0);
        }

        let index_spec = index.spec();
        let chunk_limit = self.settings.mail.commit_batch_size.max(1);
        let mut docs_upserted = 0u64;

        for chunk in ids.chunks(chunk_limit) {
            let docs = self
                .build_docs_for_index(index, chunk)
                .await
                .map_err(|err| MeiliClientError::Protocol(format!("doc build: {err}")))?;
            if docs.is_empty() {
                continue;
            }

            let task_uid = self.meili.upsert_documents(index_spec.uid, &docs).await?;
            self.wait_for_task(task_uid, ctx).await?;
            docs_upserted += docs.len() as u64;
        }

        Ok(docs_upserted)
    }

    async fn enqueue_embedding_generate_batch(
        &self,
        list_key: &str,
        scope: EmbeddingScope,
        ids: &[i64],
        source_job_id: Option<i64>,
        priority: i32,
    ) -> Result<bool, sqlx::Error> {
        if !self.settings.embeddings.enabled {
            return Ok(false);
        }

        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.dedup();
        ids.retain(|value| *value > 0);
        if ids.is_empty() {
            return Ok(false);
        }

        let source = source_job_id.unwrap_or_default();
        let dedupe_key = format!(
            "{}:{}:{}:{}",
            scope.as_str(),
            self.settings.embeddings.model,
            source,
            hash_i64_list(&ids)
        );
        let payload = EmbeddingGenerateBatchPayload {
            scope,
            list_key: Some(list_key.to_string()),
            ids,
            model_key: self.settings.embeddings.model.clone(),
            source_job_id,
        };

        self.jobs
            .enqueue(EnqueueJobParams {
                job_type: "embedding_generate_batch".to_string(),
                payload_json: serde_json::to_value(payload)
                    .unwrap_or_else(|_| serde_json::json!({})),
                priority,
                dedupe_scope: Some(format!("list:{list_key}:embeddings")),
                dedupe_key: Some(dedupe_key),
                run_after: None,
                max_attempts: Some(8),
            })
            .await?;
        Ok(true)
    }

    async fn embed_text_batch_with_retry(
        &self,
        texts: &[String],
    ) -> Result<Vec<Vec<f32>>, EmbeddingsClientError> {
        let Some(client) = self.embedding_client.as_ref() else {
            return Err(EmbeddingsClientError::Protocol(
                "embeddings client is not configured".to_string(),
            ));
        };

        let mut backoff_ms = self.settings.worker.base_backoff_ms.max(250);
        let mut attempts = 0u8;
        loop {
            attempts = attempts.saturating_add(1);
            match client.embed_texts(texts).await {
                Ok(vectors) => return Ok(vectors),
                Err(err) => {
                    if !err.is_transient() || attempts >= 4 {
                        return Err(err);
                    }
                    sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms.saturating_mul(2))
                        .min(self.settings.worker.max_backoff_ms.max(backoff_ms));
                }
            }
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, Default)]
struct SearchFamilyOutcome {
    artifact_kind: String,
    index_uid: String,
    chunks_done: u64,
    ids_seen: u64,
    docs_upserted: u64,
    docs_bytes: u64,
    tasks_submitted: u64,
}

#[derive(Default)]
struct ThreadingWindowOutcome {
    affected_threads: u64,
    source_messages_read: u64,
    anchors_scanned: u64,
    anchors_deduped: u64,
    apply_stats: ThreadingApplyStats,
    impacted_thread_ids: Vec<i64>,
}

struct RawCommitMail {
    index: usize,
    commit_oid: String,
    raw_rfc822: Vec<u8>,
}

struct ParsedCommitRows {
    rows: Vec<IngestCommitRow>,
    parse_errors: u64,
    non_mail_commits: u64,
    bytes_read: u64,
    header_rejections: u64,
    date_rejections: u64,
    sanitization_rejections: u64,
    other_rejections: u64,
}

struct IndexedParsedRow {
    index: usize,
    row: IngestCommitRow,
}

struct IndexedCommitOid {
    index: usize,
    commit_oid: String,
}

#[derive(Default)]
struct ParseChunkOutcome {
    parsed_rows: Vec<IndexedParsedRow>,
    parse_errors: u64,
    non_mail_commits: u64,
    bytes_read: u64,
    header_rejections: u64,
    date_rejections: u64,
    sanitization_rejections: u64,
    other_rejections: u64,
}

#[derive(Copy, Clone)]
enum ParseSkipKind {
    MissingHeaders,
    InvalidDate,
    Sanitization,
    Other,
}

enum ParseCommitResult {
    Parsed(Box<IndexedParsedRow>),
    Skipped {
        commit_oid: String,
        reason: String,
        kind: ParseSkipKind,
        warn: bool,
    },
}

async fn parse_commit_rows(
    repo_path: PathBuf,
    commit_oids: Vec<String>,
    worker_count: usize,
) -> anyhow::Result<ParsedCommitRows> {
    if commit_oids.is_empty() {
        return Ok(ParsedCommitRows {
            rows: Vec::new(),
            parse_errors: 0,
            non_mail_commits: 0,
            bytes_read: 0,
            header_rejections: 0,
            date_rejections: 0,
            sanitization_rejections: 0,
            other_rejections: 0,
        });
    }

    let partitions = partition_commit_oids(commit_oids, worker_count);
    let mut joinset = JoinSet::new();
    for partition in partitions {
        if partition.is_empty() {
            continue;
        }
        let worker_repo_path = repo_path.clone();
        joinset.spawn_blocking(move || parse_commit_partition(worker_repo_path, partition));
    }

    let mut parsed_rows = Vec::new();
    let mut parse_errors = 0u64;
    let mut non_mail_commits = 0u64;
    let mut bytes_read = 0u64;
    let mut header_rejections = 0u64;
    let mut date_rejections = 0u64;
    let mut sanitization_rejections = 0u64;
    let mut other_rejections = 0u64;

    while let Some(next) = joinset.join_next().await {
        match next {
            Ok(Ok(outcome)) => {
                parsed_rows.extend(outcome.parsed_rows);
                parse_errors += outcome.parse_errors;
                non_mail_commits += outcome.non_mail_commits;
                bytes_read += outcome.bytes_read;
                header_rejections += outcome.header_rejections;
                date_rejections += outcome.date_rejections;
                sanitization_rejections += outcome.sanitization_rejections;
                other_rejections += outcome.other_rejections;
            }
            Ok(Err(err)) => return Err(err),
            Err(err) => {
                return Err(anyhow::anyhow!("mail parse task failed: {err}"));
            }
        }
    }

    parsed_rows.sort_by_key(|row| row.index);

    Ok(ParsedCommitRows {
        rows: parsed_rows.into_iter().map(|row| row.row).collect(),
        parse_errors,
        non_mail_commits,
        bytes_read,
        header_rejections,
        date_rejections,
        sanitization_rejections,
        other_rejections,
    })
}

fn parse_commit_partition(
    repo_path: PathBuf,
    partition: Vec<IndexedCommitOid>,
) -> anyhow::Result<ParseChunkOutcome> {
    let mut gix_repo = gix::open(&repo_path)?;
    gix_repo.object_cache_size_if_unset(64 * 1024 * 1024);

    let mut outcome = ParseChunkOutcome::default();

    for commit in partition {
        let raw_mail = match read_mail_blob(&gix_repo, &commit.commit_oid) {
            Ok(Some(value)) => value,
            Ok(None) => {
                outcome.non_mail_commits += 1;
                continue;
            }
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "failed to read commit blob {}: {err}",
                    commit.commit_oid
                ));
            }
        };

        outcome.bytes_read += raw_mail.len() as u64;
        let raw_commit = RawCommitMail {
            index: commit.index,
            commit_oid: commit.commit_oid,
            raw_rfc822: raw_mail,
        };

        match parse_one_commit(raw_commit) {
            ParseCommitResult::Parsed(parsed) => outcome.parsed_rows.push(*parsed),
            ParseCommitResult::Skipped {
                commit_oid,
                reason,
                kind,
                warn,
            } => {
                outcome.parse_errors += 1;
                match kind {
                    ParseSkipKind::MissingHeaders => outcome.header_rejections += 1,
                    ParseSkipKind::InvalidDate => outcome.date_rejections += 1,
                    ParseSkipKind::Sanitization => outcome.sanitization_rejections += 1,
                    ParseSkipKind::Other => outcome.other_rejections += 1,
                }
                if warn {
                    warn!(commit_oid = %commit_oid, error = %reason, "mail parse error");
                }
            }
        }
    }

    Ok(outcome)
}

fn parse_one_commit(raw_commit: RawCommitMail) -> ParseCommitResult {
    let parsed = match parse_email(&raw_commit.raw_rfc822) {
        Ok(v) => v,
        Err(ParseEmailError::MissingMessageId | ParseEmailError::MissingAuthorEmail) => {
            return ParseCommitResult::Skipped {
                commit_oid: raw_commit.commit_oid,
                reason: "missing required message headers".to_string(),
                kind: ParseSkipKind::MissingHeaders,
                warn: false,
            };
        }
        Err(
            ParseEmailError::MissingDate { .. }
            | ParseEmailError::InvalidDate { .. }
            | ParseEmailError::FutureDate { .. },
        ) => {
            return ParseCommitResult::Skipped {
                commit_oid: raw_commit.commit_oid,
                reason: "invalid_or_missing_date".to_string(),
                kind: ParseSkipKind::InvalidDate,
                warn: false,
            };
        }
        Err(ParseEmailError::SanitizationInvariantViolation(field)) => {
            return ParseCommitResult::Skipped {
                commit_oid: raw_commit.commit_oid,
                reason: format!("sanitization_invariant_violation:{field}"),
                kind: ParseSkipKind::Sanitization,
                warn: true,
            };
        }
        Err(err) => {
            return ParseCommitResult::Skipped {
                commit_oid: raw_commit.commit_oid,
                reason: err.to_string(),
                kind: ParseSkipKind::Other,
                warn: true,
            };
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
            raw_rfc822: raw_commit.raw_rfc822,
            body_text: parsed.body_text,
            diff_text: parsed.diff_text,
            search_text: parsed.search_text,
            has_diff: parsed.has_diff,
            has_attachments: parsed.has_attachments,
        },
    };

    ParseCommitResult::Parsed(Box::new(IndexedParsedRow {
        index: raw_commit.index,
        row: IngestCommitRow {
            git_commit_oid: raw_commit.commit_oid,
            parsed_message: parsed_input,
        },
    }))
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

fn parse_embedding_scope(raw: &str) -> Option<EmbeddingScope> {
    match raw {
        "thread" => Some(EmbeddingScope::Thread),
        "series" => Some(EmbeddingScope::Series),
        _ => None,
    }
}

fn embedding_doc_build_stage(scope: &str) -> &'static str {
    match scope {
        "thread" => "thread_snippet_aggregate",
        "series" => "series_doc_build",
        _ => "unknown_scope_doc_build",
    }
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

fn configured_parse_worker_count(chunk_size: usize, settings: &Settings) -> usize {
    settings
        .worker
        .ingest_parse_concurrency
        .max(1)
        .min(chunk_size.max(1))
}

fn partition_commit_oids(
    commit_oids: Vec<String>,
    worker_count: usize,
) -> Vec<Vec<IndexedCommitOid>> {
    let lane_count = worker_count.max(1).min(commit_oids.len().max(1));
    let mut lanes: Vec<Vec<IndexedCommitOid>> = (0..lane_count).map(|_| Vec::new()).collect();

    for (idx, commit_oid) in commit_oids.into_iter().enumerate() {
        lanes[idx % lane_count].push(IndexedCommitOid {
            index: idx,
            commit_oid,
        });
    }

    lanes
}

fn discover_repo_relpaths(list_root: &Path) -> Result<Vec<String>, std::io::Error> {
    if !list_root.exists() {
        return Ok(Vec::new());
    }

    let mut epoch_repos = BTreeSet::new();
    let git_dir = list_root.join("git");
    if git_dir.is_dir() {
        for entry in fs::read_dir(&git_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir()
                && path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("")
                    .ends_with(".git")
            {
                let repo_name = path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("");
                epoch_repos.insert(format!("git/{repo_name}"));
            }
        }
    }

    if !epoch_repos.is_empty() {
        return Ok(epoch_repos.into_iter().collect());
    }

    let all_git = list_root.join("all.git");
    if all_git.is_dir() {
        return Ok(vec!["all.git".to_string()]);
    }

    if looks_like_bare_repo(list_root) {
        return Ok(vec![".".to_string()]);
    }

    Ok(Vec::new())
}

fn order_repos_for_ingest(mut repos: Vec<MailingListRepo>) -> Vec<MailingListRepo> {
    let has_epoch_repos = repos
        .iter()
        .any(|repo| parse_epoch_repo_relpath(&repo.repo_relpath).is_some());
    if has_epoch_repos {
        repos.retain(|repo| parse_epoch_repo_relpath(&repo.repo_relpath).is_some());
    }

    repos.sort_by(|left, right| compare_repo_relpaths(&left.repo_relpath, &right.repo_relpath));
    repos
}

fn compare_repo_relpaths(left: &str, right: &str) -> std::cmp::Ordering {
    match (
        parse_epoch_repo_relpath(left),
        parse_epoch_repo_relpath(right),
    ) {
        (Some(l), Some(r)) => l.cmp(&r),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.cmp(right),
    }
}

fn parse_epoch_repo_relpath(relpath: &str) -> Option<i64> {
    let trimmed = relpath.trim();
    let epoch_text = trimmed.strip_prefix("git/")?.strip_suffix(".git")?.trim();

    if epoch_text.is_empty() || !epoch_text.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    epoch_text.parse::<i64>().ok()
}

fn looks_like_bare_repo(path: &Path) -> bool {
    path.join("objects").exists() && path.join("refs").exists()
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

#[cfg(test)]
mod tests {
    use super::{parse_epoch_repo_relpath, partition_commit_oids};

    #[test]
    fn parse_epoch_repo_relpath_extracts_epoch() {
        assert_eq!(parse_epoch_repo_relpath("git/0.git"), Some(0));
        assert_eq!(parse_epoch_repo_relpath("git/12.git"), Some(12));
        assert_eq!(parse_epoch_repo_relpath("all.git"), None);
        assert_eq!(parse_epoch_repo_relpath("git/not-a-number.git"), None);
    }

    #[test]
    fn partition_commit_oids_distributes_evenly() {
        let commits = vec![
            "c1".to_string(),
            "c2".to_string(),
            "c3".to_string(),
            "c4".to_string(),
            "c5".to_string(),
            "c6".to_string(),
            "c7".to_string(),
            "c8".to_string(),
        ];
        let lanes = partition_commit_oids(commits, 4);
        assert_eq!(lanes.len(), 4);
        assert_eq!(
            lanes.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![2, 2, 2, 2]
        );
    }
}
