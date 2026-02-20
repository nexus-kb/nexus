use std::collections::{BTreeSet, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::Utc;
use gix::hash::ObjectId;
use nexus_core::config::{IngestWriteMode, LineageDiscoveryMode, Settings};
use nexus_core::embeddings::{EmbeddingsClientError, OpenAiEmbeddingsClient};
use nexus_core::search::MeiliIndexKind;
use nexus_db::{
    CatalogStore, EmbeddingVectorUpsert, EmbeddingsStore, EnqueueJobParams, IngestCommitRow,
    IngestStore, Job, JobStore, JobStoreMetrics, LineageStore, MailingListRepo, ParsedBodyInput,
    ParsedMessageInput, ParsedPatchFactsInput, ParsedPatchFileFactInput, PipelineStore,
    SearchStore, ThreadComponentWrite, ThreadMessageWrite, ThreadNodeWrite, ThreadSummaryWrite,
    ThreadingApplyStats, ThreadingRunContext, ThreadingStore,
};
use once_cell::sync::Lazy;
use regex::Regex;
use sha2::{Digest, Sha256};
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

use crate::diff_metadata::parse_diff_metadata;
use crate::lineage::{
    process_patch_enrichment_batch, process_patch_extract_threads, process_patch_extract_window,
};
use crate::mail::{ParseEmailError, parse_email};
use crate::meili::{MeiliClient, MeiliClientError, settings_differ};
use crate::patch_detect::extract_diff_text;
use crate::patch_id::compute_patch_id_stable;
use crate::payloads::{
    EmbeddingBackfillRunPayload, EmbeddingGenerateBatchPayload, EmbeddingScope,
    LineageRebuildListPayload, MeiliBootstrapRunPayload, PipelineEmbeddingPayload,
    PipelineIngestPayload, PipelineLexicalPayload, PipelineLineagePayload,
    PipelineThreadingPayload, ThreadingRebuildListPayload,
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
const STAGE_LEXICAL: &str = "lexical";
const STAGE_EMBEDDING: &str = "embedding";

const PRIORITY_INGEST: i32 = 20;
const PRIORITY_THREADING: i32 = 16;
const PRIORITY_LINEAGE: i32 = 14;
const PRIORITY_LEXICAL: i32 = 12;
const PRIORITY_PIPELINE_EMBEDDING: i32 = 3;
const PRIORITY_EMBEDDING_BATCH: i32 = 2;

static CHANGE_ID_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?im)^change-id:\s*(\S+)").expect("valid change-id regex"));
static BASE_COMMIT_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?im)^base-commit:\s*(\S+)").expect("valid base-commit regex"));

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
            "pipeline_search" => self.handle_pipeline_lexical(job, ctx).await,
            "pipeline_lexical" => self.handle_pipeline_lexical(job, ctx).await,
            "pipeline_embedding" => self.handle_pipeline_embedding(job, ctx).await,
            "threading_rebuild_list" => self.handle_threading_rebuild_list(job, ctx).await,
            "lineage_rebuild_list" => self.handle_lineage_rebuild_list(job, ctx).await,
            "embedding_backfill_run" => self.handle_embedding_backfill_run(job, ctx).await,
            "embedding_generate_batch" => self.handle_embedding_generate_batch(job, ctx).await,
            "meili_bootstrap_run" => self.handle_meili_bootstrap_run(job, ctx).await,
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
        let discovery_mode = self.settings.worker.lineage_discovery_mode;
        let discovery_mode_label = match discovery_mode {
            LineageDiscoveryMode::ThreadFirst => "thread_first",
            LineageDiscoveryMode::MessageLegacy => "message_legacy",
        };
        let work_item_kind = match discovery_mode {
            LineageDiscoveryMode::ThreadFirst => "threads",
            LineageDiscoveryMode::MessageLegacy => "messages",
        };

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

            let (chunk_work_items, extract_outcome) = match discovery_mode {
                LineageDiscoveryMode::ThreadFirst => {
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
                                format!(
                                    "patch lineage extraction failed for run {}: {err}",
                                    run.id
                                ),
                                "parse",
                                &job,
                                started.elapsed().as_millis(),
                                &self.settings,
                            );
                        }
                    };
                    (thread_chunk.len() as u64, extract_outcome)
                }
                LineageDiscoveryMode::MessageLegacy => {
                    let message_chunk = match self
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
                    if message_chunk.is_empty() {
                        break;
                    }
                    cursor = *message_chunk.last().unwrap_or(&cursor);

                    let extract_outcome = match process_patch_extract_window(
                        &self.lineage,
                        run.mailing_list_id,
                        &message_chunk,
                    )
                    .await
                    {
                        Ok(value) => value,
                        Err(err) => {
                            return retryable_error(
                                format!(
                                    "patch lineage extraction failed for run {}: {err}",
                                    run.id
                                ),
                                "parse",
                                &job,
                                started.elapsed().as_millis(),
                                &self.settings,
                            );
                        }
                    };
                    (message_chunk.len() as u64, extract_outcome)
                }
            };

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
                    elapsed_ms = started.elapsed().as_millis() as u64,
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

    async fn handle_pipeline_lexical(
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

    async fn handle_pipeline_embedding(
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

        if !self.settings.embeddings.enabled {
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "run_id": payload.run_id,
                    "list_key": payload.list_key,
                    "stage": STAGE_EMBEDDING,
                    "skipped": "embeddings disabled",
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

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

    async fn enqueue_next_stage(
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

    async fn enqueue_pipeline_embedding(
        &self,
        run_id: i64,
        list_key: &str,
        window_from: chrono::DateTime<Utc>,
        window_to: chrono::DateTime<Utc>,
        source_job_id: Option<i64>,
    ) -> Result<bool, sqlx::Error> {
        if !self.settings.embeddings.enabled {
            return Ok(false);
        }

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
    async fn activate_and_enqueue_next_list(&self, batch_id: i64) -> Result<(), sqlx::Error> {
        if self
            .activate_and_enqueue_next_list_in_batch(batch_id)
            .await?
        {
            return Ok(());
        }
        let _ = self.activate_and_enqueue_next_list_global().await?;
        Ok(())
    }

    async fn activate_and_enqueue_next_list_in_batch(
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

    async fn activate_and_enqueue_next_list_global(&self) -> Result<bool, sqlx::Error> {
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

        let epoch_windows_total = windows.len() as u64;
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

            if processed_chunks % 10 == 0 || last_progress_log.elapsed() >= Duration::from_secs(30)
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

        let discovery_mode = self.settings.worker.lineage_discovery_mode;
        let discovery_mode_label = match discovery_mode {
            LineageDiscoveryMode::ThreadFirst => "thread_first",
            LineageDiscoveryMode::MessageLegacy => "message_legacy",
        };
        let work_item_kind = match discovery_mode {
            LineageDiscoveryMode::ThreadFirst => "threads",
            LineageDiscoveryMode::MessageLegacy => "messages",
        };

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

            let (chunk_work_items, extract_outcome) = match discovery_mode {
                LineageDiscoveryMode::ThreadFirst => {
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
                        match process_patch_extract_threads(&self.lineage, list.id, &thread_chunk)
                            .await
                        {
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
                    (thread_chunk.len() as u64, extract_outcome)
                }
                LineageDiscoveryMode::MessageLegacy => {
                    let message_chunk = match self
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
                                format!("failed to load lineage rebuild message chunk: {err}"),
                                "db",
                                &job,
                                started.elapsed().as_millis(),
                                &self.settings,
                            );
                        }
                    };

                    if message_chunk.is_empty() {
                        break;
                    }

                    cursor = *message_chunk.last().unwrap_or(&cursor);
                    let extract_outcome =
                        match process_patch_extract_window(&self.lineage, list.id, &message_chunk)
                            .await
                        {
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
                    (message_chunk.len() as u64, extract_outcome)
                }
            };

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

            if processed_chunks % 10 == 0 || last_progress_log.elapsed() >= Duration::from_secs(30)
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

    async fn handle_meili_bootstrap_run(
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

        if !self.settings.embeddings.enabled {
            return JobExecutionOutcome::Terminal {
                reason: "meili bootstrap requires embeddings.enabled=true".to_string(),
                kind: "payload".to_string(),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

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
        let chunk_limit = self.settings.embeddings.enqueue_batch_size.max(1) as i64;
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

        let batch_size = self.settings.embeddings.batch_size.max(1);
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
        let embedding_chunk_limit = self.settings.embeddings.enqueue_batch_size.max(1);
        let meili_docs_upserted = match self
            .upsert_meili_docs_inline(index, &ids, embedding_chunk_limit, &ctx)
            .await
        {
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

    async fn ensure_index_exists_only(
        &self,
        index_spec: &'static nexus_core::search::MeiliIndexSpec,
        ctx: &ExecutionContext,
    ) -> Result<(), MeiliClientError> {
        if let Some(task_uid) = self.meili.ensure_index_exists(index_spec).await? {
            match self.wait_for_task(task_uid, ctx).await {
                Ok(()) => {}
                Err(MeiliClientError::Protocol(message))
                    if message.contains("code=index_already_exists") => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    async fn upsert_meili_bootstrap_chunk(
        &self,
        index: MeiliIndexKind,
        ids: &[i64],
        ctx: &ExecutionContext,
    ) -> Result<(u64, i64, i64), MeiliClientError> {
        let docs = self
            .build_docs_for_index(index, ids, VectorAttachmentMode::StoredVector)
            .await
            .map_err(|err| MeiliClientError::Protocol(format!("doc build: {err}")))?;
        if docs.is_empty() {
            return Ok((0, 0, 0));
        }

        let embedder_name = self.settings.embeddings.embedder_name.as_str();
        let mut vectors_attached = 0i64;
        let mut placeholders_written = 0i64;
        for doc in &docs {
            let Some(obj) = doc.as_object() else {
                continue;
            };
            let Some(vector_map) = obj.get("_vectors").and_then(serde_json::Value::as_object)
            else {
                continue;
            };
            let Some(entry) = vector_map.get(embedder_name) else {
                continue;
            };
            if entry.is_array() {
                vectors_attached += 1;
            } else if entry.is_null() {
                placeholders_written += 1;
            }
        }

        let task_uid = self.meili.upsert_documents(index.spec().uid, &docs).await?;
        self.wait_for_task(task_uid, ctx).await?;

        Ok((docs.len() as u64, vectors_attached, placeholders_written))
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
        vector_mode: VectorAttachmentMode,
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

        let vectors = match vector_mode {
            VectorAttachmentMode::StoredVector => Some(
                self.embeddings
                    .get_vectors(scope.as_str(), &self.settings.embeddings.model, ids)
                    .await?,
            ),
            VectorAttachmentMode::NullPlaceholder => None,
        };
        let embedder_name = self.settings.embeddings.embedder_name.clone();

        for doc in &mut docs {
            let Some(doc_obj) = doc.as_object_mut() else {
                continue;
            };
            let vector_value = match vector_mode {
                VectorAttachmentMode::StoredVector => {
                    let Some(id) = doc_obj.get("id").and_then(|value| value.as_i64()) else {
                        continue;
                    };
                    vectors
                        .as_ref()
                        .and_then(|map| map.get(&id))
                        .map(|vector| serde_json::json!(vector))
                        .unwrap_or(serde_json::Value::Null)
                }
                VectorAttachmentMode::NullPlaceholder => serde_json::Value::Null,
            };
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
        chunk_limit: usize,
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
        let chunk_limit = chunk_limit.max(1);
        let mut docs_upserted = 0u64;

        for chunk in ids.chunks(chunk_limit) {
            let docs = self
                .build_docs_for_index(index, chunk, VectorAttachmentMode::StoredVector)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VectorAttachmentMode {
    StoredVector,
    NullPlaceholder,
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

#[derive(Debug, Clone)]
struct ThreadingEpochWindow {
    epochs: Vec<i64>,
    repo_ids: Vec<i64>,
}

fn build_threading_epoch_windows(
    repos: &[MailingListRepo],
    start_epoch: Option<i64>,
) -> std::result::Result<Vec<ThreadingEpochWindow>, String> {
    if repos.is_empty() {
        return Err("no repositories available for epoch threading".to_string());
    }

    let mut epoch_groups: Vec<(i64, Vec<i64>)> = Vec::new();
    for repo in repos {
        let Some(epoch) = parse_epoch_repo_relpath(&repo.repo_relpath) else {
            return Err(format!(
                "repo {} is not epoch formatted (expected git/<n>.git)",
                repo.repo_relpath
            ));
        };

        if let Some((last_epoch, repo_ids)) = epoch_groups.last_mut()
            && *last_epoch == epoch
        {
            repo_ids.push(repo.id);
            continue;
        }
        epoch_groups.push((epoch, vec![repo.id]));
    }

    let start_idx = match start_epoch {
        Some(value) => epoch_groups
            .iter()
            .position(|(epoch, _)| *epoch >= value)
            .unwrap_or_else(|| epoch_groups.len().saturating_sub(1)),
        None => 0,
    };
    let scoped = &epoch_groups[start_idx..];
    if scoped.is_empty() {
        return Ok(Vec::new());
    }

    if scoped.len() == 1 {
        let (epoch, repo_ids) = &scoped[0];
        return Ok(vec![ThreadingEpochWindow {
            epochs: vec![*epoch],
            repo_ids: repo_ids.clone(),
        }]);
    }

    let mut windows = Vec::with_capacity(scoped.len() - 1);
    for idx in 0..(scoped.len() - 1) {
        let (left_epoch, left_ids) = &scoped[idx];
        let (right_epoch, right_ids) = &scoped[idx + 1];
        let mut repo_ids = Vec::with_capacity(left_ids.len() + right_ids.len());
        repo_ids.extend(left_ids.iter().copied());
        repo_ids.extend(right_ids.iter().copied());
        windows.push(ThreadingEpochWindow {
            epochs: vec![*left_epoch, *right_epoch],
            repo_ids,
        });
    }

    Ok(windows)
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

    let patch_facts = extract_patch_facts(
        parsed.body_text.as_deref(),
        parsed.diff_text.as_deref(),
        parsed.has_diff,
    );

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
        patch_facts,
    };

    ParseCommitResult::Parsed(Box::new(IndexedParsedRow {
        index: raw_commit.index,
        row: IngestCommitRow {
            git_commit_oid: raw_commit.commit_oid,
            parsed_message: parsed_input,
        },
    }))
}

fn extract_patch_facts(
    body_text: Option<&str>,
    diff_text: Option<&str>,
    has_diff: bool,
) -> Option<ParsedPatchFactsInput> {
    let extracted_diff = diff_text
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .or_else(|| body_text.and_then(extract_diff_text));

    let has_diff = has_diff
        || extracted_diff
            .as_deref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);

    let patch_id_stable = extracted_diff.as_deref().and_then(compute_patch_id_stable);

    let parsed_files = extracted_diff
        .as_deref()
        .map(parse_diff_metadata)
        .unwrap_or_default();

    let mut files = Vec::with_capacity(parsed_files.len());
    let mut additions = 0i32;
    let mut deletions = 0i32;
    let mut hunk_count = 0i32;

    for file in parsed_files {
        additions = additions.saturating_add(file.additions);
        deletions = deletions.saturating_add(file.deletions);
        hunk_count = hunk_count.saturating_add(file.hunk_count);
        files.push(ParsedPatchFileFactInput {
            old_path: file.old_path,
            new_path: file.new_path,
            change_type: file.change_type,
            is_binary: file.is_binary,
            additions: file.additions,
            deletions: file.deletions,
            hunk_count: file.hunk_count,
            diff_start: file.diff_start,
            diff_end: file.diff_end,
        });
    }

    let base_commit = body_text.and_then(extract_base_commit);
    let change_id = body_text.and_then(extract_change_id);

    if !has_diff
        && patch_id_stable.is_none()
        && base_commit.is_none()
        && change_id.is_none()
        && files.is_empty()
    {
        return None;
    }

    Some(ParsedPatchFactsInput {
        has_diff,
        patch_id_stable,
        base_commit,
        change_id,
        file_count: files.len() as i32,
        additions,
        deletions,
        hunk_count,
        files,
    })
}

fn extract_change_id(body_text: &str) -> Option<String> {
    CHANGE_ID_RE
        .captures(body_text)
        .and_then(|captures| captures.get(1))
        .map(|matched| matched.as_str().trim().to_ascii_lowercase())
}

fn extract_base_commit(body_text: &str) -> Option<String> {
    BASE_COMMIT_RE
        .captures(body_text)
        .and_then(|captures| captures.get(1))
        .map(|matched| matched.as_str().trim().to_ascii_lowercase())
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
    use chrono::Utc;
    use nexus_db::MailingListRepo;

    use super::{build_threading_epoch_windows, parse_epoch_repo_relpath, partition_commit_oids};

    fn make_repo(id: i64, relpath: &str) -> MailingListRepo {
        MailingListRepo {
            id,
            mailing_list_id: 1,
            repo_key: relpath.to_string(),
            repo_relpath: relpath.to_string(),
            active: true,
            created_at: Utc::now(),
        }
    }

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

    #[test]
    fn build_threading_epoch_windows_slides_by_one() {
        let repos = vec![
            make_repo(10, "git/0.git"),
            make_repo(11, "git/1.git"),
            make_repo(12, "git/2.git"),
        ];

        let windows = build_threading_epoch_windows(&repos, None).expect("build epoch windows");
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].epochs, vec![0, 1]);
        assert_eq!(windows[0].repo_ids, vec![10, 11]);
        assert_eq!(windows[1].epochs, vec![1, 2]);
        assert_eq!(windows[1].repo_ids, vec![11, 12]);
    }

    #[test]
    fn build_threading_epoch_windows_single_epoch_is_single_window() {
        let repos = vec![make_repo(42, "git/7.git")];

        let windows = build_threading_epoch_windows(&repos, None).expect("build epoch windows");
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].epochs, vec![7]);
        assert_eq!(windows[0].repo_ids, vec![42]);
    }

    #[test]
    fn build_threading_epoch_windows_can_start_from_prior_epoch() {
        let repos = vec![
            make_repo(10, "git/0.git"),
            make_repo(11, "git/1.git"),
            make_repo(12, "git/2.git"),
            make_repo(13, "git/3.git"),
        ];

        let windows = build_threading_epoch_windows(&repos, Some(2)).expect("build epoch windows");
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].epochs, vec![2, 3]);
        assert_eq!(windows[0].repo_ids, vec![12, 13]);
    }
}
