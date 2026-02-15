use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;
use std::time::Instant;

use chrono::Utc;
use gix::hash::ObjectId;
use nexus_core::config::{BackfillMode, IngestWriteMode, PipelineExecutionMode, Settings};
use nexus_core::embeddings::{EmbeddingsClientError, OpenAiEmbeddingsClient};
use nexus_core::search::MeiliIndexKind;
use nexus_db::{
    CatalogStore, EmbeddingVectorUpsert, EmbeddingsStore, EnqueueJobParams, IngestCommitRow,
    IngestStore, Job, JobStore, JobStoreMetrics, LineageStore, ParsedBodyInput, ParsedMessageInput,
    PipelineStore, SearchStore, ThreadComponentWrite, ThreadMessageWrite, ThreadNodeWrite,
    ThreadSummaryWrite, ThreadingApplyStats, ThreadingStore,
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
    DiffParsePatchItemsPayload, EmbeddingBackfillRunPayload, EmbeddingGenerateBatchPayload,
    EmbeddingScope, IngestCommitBatchPayload, LineageRebuildListPayload, MeiliUpsertBatchPayload,
    PatchExtractWindowPayload, PatchIdComputeBatchPayload, PipelineStageIngestPayload,
    PipelineStageLineageDiffPayload, PipelineStageSearchPayload, PipelineStageThreadingPayload,
    RepoIngestRunPayload, RepoScanPayload, ThreadingRebuildListPayload,
    ThreadingUpdateWindowPayload,
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
const STAGE_LINEAGE_DIFF: &str = "lineage_diff";
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
            "repo_scan" => self.handle_repo_scan(job, ctx).await,
            "repo_ingest_run" => self.handle_repo_ingest_run(job, ctx).await,
            "ingest_commit_batch" => self.handle_ingest_commit_batch(job, ctx).await,
            "pipeline_stage_ingest" => self.handle_pipeline_stage_ingest(job, ctx).await,
            "pipeline_stage_threading" => self.handle_pipeline_stage_threading(job, ctx).await,
            "pipeline_stage_lineage_diff" => {
                self.handle_pipeline_stage_lineage_diff(job, ctx).await
            }
            "pipeline_stage_search" => self.handle_pipeline_stage_search(job, ctx).await,
            "threading_update_window" => self.handle_threading_update_window(job, ctx).await,
            "threading_rebuild_list" => self.handle_threading_rebuild_list(job, ctx).await,
            "lineage_rebuild_list" => self.handle_lineage_rebuild_list(job, ctx).await,
            "patch_extract_window" => self.handle_patch_extract_window(job, ctx).await,
            "patch_id_compute_batch" => self.handle_patch_id_compute_batch(job, ctx).await,
            "diff_parse_patch_items" => self.handle_diff_parse_patch_items(job, ctx).await,
            "meili_upsert_batch" => self.handle_meili_upsert_batch(job, ctx).await,
            "embedding_backfill_run" => self.handle_embedding_backfill_run(job, ctx).await,
            "embedding_generate_batch" => self.handle_embedding_generate_batch(job, ctx).await,
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

        let since_commit_oid = payload.since_commit_oid.or(watermark);
        let repo_ingest_payload = RepoIngestRunPayload {
            list_key: payload.list_key.clone(),
            repo_key: payload.repo_key.clone(),
            mirror_path: payload.mirror_path.clone(),
            since_commit_oid: since_commit_oid.clone(),
        };

        let dedupe_key = format!(
            "{}:{}",
            payload.repo_key,
            since_commit_oid.as_deref().unwrap_or("-")
        );

        if let Err(err) = self
            .jobs
            .enqueue(EnqueueJobParams {
                job_type: "repo_ingest_run".to_string(),
                payload_json: serde_json::to_value(repo_ingest_payload)
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
                format!("failed to enqueue repo_ingest_run: {err}"),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        let duration_ms = started.elapsed().as_millis();
        info!(
            list_key = %payload.list_key,
            repo_key = %payload.repo_key,
            "repo_scan queued repo ingest run"
        );

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "queued_runs": 1,
                "repo_key": payload.repo_key,
                "since_commit_oid": since_commit_oid,
            }),
            metrics: JobStoreMetrics {
                duration_ms,
                rows_written: 1,
                bytes_read: 0,
                commit_count: 0,
                parse_errors: 0,
            },
        }
    }

    async fn handle_repo_ingest_run(&self, job: Job, ctx: ExecutionContext) -> JobExecutionOutcome {
        let started = Instant::now();

        let payload: RepoIngestRunPayload = match serde_json::from_value(job.payload_json.clone()) {
            Ok(v) => v,
            Err(err) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("invalid repo_ingest_run payload: {err}"),
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

        let since_commit_oid = current_watermark.clone();
        let batch_size = if matches!(self.settings.worker.backfill_mode, BackfillMode::IngestOnly) {
            self.settings.worker.backfill_batch_size.max(1)
        } else {
            self.settings.mail.commit_batch_size.max(1)
        };

        let mut stream = match stream_new_commit_oid_chunks(
            Path::new(&payload.mirror_path),
            since_commit_oid.as_deref(),
            batch_size,
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

        let mut gix_repo = match gix::open(Path::new(&payload.mirror_path)) {
            Ok(v) => v,
            Err(err) => {
                return retryable_error(
                    format!("failed to open repo path {}: {err}", payload.mirror_path),
                    "io",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };
        gix_repo.object_cache_size_if_unset(64 * 1024 * 1024);

        let mut chunk_count = 0u64;
        let mut processed_commits = 0u64;
        let mut rows_written = 0u64;
        let mut bytes_read = 0u64;
        let mut parse_errors = 0u64;
        let mut threading_enqueued = 0u64;
        let mut last_scanned_commit: Option<String> = None;

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
                            commit_count: processed_commits,
                            parse_errors,
                        },
                    };
                }
                Ok(false) => {}
                Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
            }

            let chunk = match stream.next_chunk() {
                Ok(Some(v)) => v,
                Ok(None) => break,
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

            chunk_count += 1;
            processed_commits += chunk.len() as u64;
            last_scanned_commit = chunk.last().cloned();

            let mut raw_commits = Vec::with_capacity(chunk.len());
            for (idx, commit_oid) in chunk.iter().enumerate() {
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
                raw_commits.push(RawCommitMail {
                    index: idx,
                    commit_oid: commit_oid.clone(),
                    raw_rfc822: raw_mail,
                });
            }

            let parsed_outcome = parse_commit_rows(
                raw_commits,
                self.settings.worker.ingest_parse_concurrency.max(1),
            )
            .await;
            parse_errors += parsed_outcome.parse_errors;

            let batch_outcome = match self
                .write_ingest_rows(
                    &repo,
                    &parsed_outcome.rows,
                    self.settings.worker.db_relaxed_durability,
                )
                .await
            {
                Ok(v) => v,
                Err(err) => {
                    let first_commit_oids: Vec<String> = parsed_outcome
                        .rows
                        .iter()
                        .take(5)
                        .map(|row| row.git_commit_oid.clone())
                        .collect();
                    return retryable_error(
                        format!(
                            "ingest batch write failed for repo {}: parsed_rows={} first_commit_oids={first_commit_oids:?} error={err}",
                            payload.repo_key,
                            parsed_outcome.rows.len()
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };

            rows_written += batch_outcome.inserted_instances;
            let mut anchor_message_pks: Vec<i64> = batch_outcome.message_pks;
            anchor_message_pks.sort_unstable();
            anchor_message_pks.dedup();

            let full_pipeline = matches!(
                self.settings.worker.backfill_mode,
                BackfillMode::FullPipeline
            ) && matches!(
                self.settings.worker.pipeline_execution_mode,
                PipelineExecutionMode::Legacy
            );
            if full_pipeline && !anchor_message_pks.is_empty() {
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
                        priority: 5,
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
                threading_enqueued += 1;
            }

            if let Some(last_commit) = chunk.last()
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
        }

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "list_key": payload.list_key,
                "repo_key": payload.repo_key,
                "batch_size": batch_size,
                "chunk_count": chunk_count,
                "commit_count": processed_commits,
                "rows_written": rows_written,
                "parse_errors": parse_errors,
                "threading_jobs_enqueued": threading_enqueued,
                "last_scanned_commit": last_scanned_commit,
                "start_watermark": since_commit_oid,
                "backfill_mode": match self.settings.worker.backfill_mode {
                    BackfillMode::FullPipeline => "full_pipeline",
                    BackfillMode::IngestOnly => "ingest_only",
                },
                "pipeline_execution_mode": match self.settings.worker.pipeline_execution_mode {
                    PipelineExecutionMode::Legacy => "legacy",
                    PipelineExecutionMode::Staged => "staged",
                },
                "ingest_write_mode": match self.settings.worker.ingest_write_mode {
                    IngestWriteMode::Copy => "copy",
                    IngestWriteMode::BatchedSql => "batched_sql",
                }
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
        let last_success_commit: Option<String> = payload.commit_oids.last().cloned();
        let mut anchor_message_pks: BTreeSet<i64> = BTreeSet::new();
        let mut raw_commits = Vec::new();

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

            raw_commits.push(RawCommitMail {
                index: idx,
                commit_oid: commit_oid.clone(),
                raw_rfc822: raw_mail,
            });
        }

        let parsed_outcome = parse_commit_rows(
            raw_commits,
            self.settings.worker.ingest_parse_concurrency.max(1),
        )
        .await;
        parse_errors += parsed_outcome.parse_errors;

        let batch_outcome = match self
            .write_ingest_rows(
                &repo,
                &parsed_outcome.rows,
                self.settings.worker.db_relaxed_durability,
            )
            .await
        {
            Ok(v) => v,
            Err(err) => {
                let first_commit_oids: Vec<String> = parsed_outcome
                    .rows
                    .iter()
                    .take(5)
                    .map(|row| row.git_commit_oid.clone())
                    .collect();
                return retryable_error(
                    format!(
                        "ingest batch write failed for repo {}: parsed_rows={} first_commit_oids={first_commit_oids:?} error={err}",
                        payload.repo_key,
                        parsed_outcome.rows.len()
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        rows_written += batch_outcome.inserted_instances;
        for message_pk in batch_outcome.message_pks {
            anchor_message_pks.insert(message_pk);
        }

        let anchor_message_pks: Vec<i64> = anchor_message_pks.into_iter().collect();
        let mut threading_enqueued = false;
        let full_pipeline = matches!(
            self.settings.worker.backfill_mode,
            BackfillMode::FullPipeline
        ) && matches!(
            self.settings.worker.pipeline_execution_mode,
            PipelineExecutionMode::Legacy
        );
        if full_pipeline && !anchor_message_pks.is_empty() {
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
                    priority: 5,
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
        } else if !full_pipeline && !anchor_message_pks.is_empty() {
            info!(
                list_key = %payload.list_key,
                repo_key = %payload.repo_key,
                chunk_index = payload.chunk_index,
                anchor_message_count = anchor_message_pks.len(),
                "skipping threading enqueue in ingest_only backfill mode"
            );
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
                "backfill_mode": match self.settings.worker.backfill_mode {
                    BackfillMode::FullPipeline => "full_pipeline",
                    BackfillMode::IngestOnly => "ingest_only",
                },
                "pipeline_execution_mode": match self.settings.worker.pipeline_execution_mode {
                    PipelineExecutionMode::Legacy => "legacy",
                    PipelineExecutionMode::Staged => "staged",
                },
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

    async fn handle_pipeline_stage_ingest(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: PipelineStageIngestPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(value) => value,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid pipeline_stage_ingest payload: {err}"),
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
                    "skipped": format!("run is in terminal state {}", run.state),
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }
        if run.current_stage != STAGE_INGEST {
            let Some(current_rank) = stage_rank(&run.current_stage) else {
                return JobExecutionOutcome::Terminal {
                    reason: format!(
                        "run {} has invalid current_stage={}",
                        run.id, run.current_stage
                    ),
                    kind: "pipeline_state".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            };
            let expected_rank = stage_rank(STAGE_INGEST).expect("known ingest stage");
            if current_rank < expected_rank {
                return retryable_error(
                    format!(
                        "stage barrier: run {} is at stage {} and cannot execute ingest yet",
                        run.id, run.current_stage
                    ),
                    "stage_barrier",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "run_id": run.id,
                    "skipped": format!(
                        "run stage already advanced to {}",
                        run.current_stage
                    ),
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        if let Err(err) = self
            .pipeline
            .ensure_stage_running(run.id, STAGE_INGEST)
            .await
        {
            return retryable_error(
                format!(
                    "failed to mark ingest stage running for run {}: {err}",
                    run.id
                ),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        let list_root = Path::new(&self.settings.mail.mirror_root).join(&run.list_key);
        let mut repos = match self.catalog.list_repos_for_list(&run.list_key).await {
            Ok(value) => value,
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
            repos = match self.catalog.list_repos_for_list(&run.list_key).await {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!(
                            "failed to list repos for list {} after discovery: {err}",
                            run.list_key
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };
        }

        let repos_total = repos.len() as u64;
        let stage_started_at = Utc::now();
        let batch_size = if matches!(self.settings.worker.backfill_mode, BackfillMode::IngestOnly) {
            self.settings.worker.backfill_batch_size.max(1)
        } else {
            self.settings.mail.commit_batch_size.max(1)
        };

        let mut repos_done = 0u64;
        let mut chunks_done = 0u64;
        let mut commit_count = 0u64;
        let mut rows_written = 0u64;
        let mut bytes_read = 0u64;
        let mut parse_errors = 0u64;
        let mut message_artifacts_written = 0u64;
        let mut parse_workers_effective = 0usize;

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

            let mut gix_repo = match gix::open(&repo_path) {
                Ok(value) => value,
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

                chunks_done += 1;
                commit_count += chunk.len() as u64;
                let mut raw_commits = Vec::with_capacity(chunk.len());
                for (idx, commit_oid) in chunk.iter().enumerate() {
                    let raw_mail = match read_mail_blob(&gix_repo, commit_oid) {
                        Ok(Some(value)) => value,
                        Ok(None) => {
                            parse_errors += 1;
                            continue;
                        }
                        Err(err) => {
                            return retryable_error(
                                format!("failed to read commit blob {}: {err}", commit_oid),
                                "io",
                                &job,
                                started.elapsed().as_millis(),
                                &self.settings,
                            );
                        }
                    };

                    bytes_read += raw_mail.len() as u64;
                    raw_commits.push(RawCommitMail {
                        index: idx,
                        commit_oid: commit_oid.clone(),
                        raw_rfc822: raw_mail,
                    });
                }

                let workers = effective_parse_worker_count(
                    self.settings.worker.ingest_parse_cpu_ratio,
                    self.settings.worker.ingest_parse_workers_min,
                    self.settings.worker.ingest_parse_workers_max,
                    raw_commits.len(),
                );
                parse_workers_effective = workers;
                let parsed_outcome = parse_commit_rows(raw_commits, workers).await;
                parse_errors += parsed_outcome.parse_errors;

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
                        let first_commit_oids: Vec<String> = parsed_outcome
                            .rows
                            .iter()
                            .take(5)
                            .map(|row| row.git_commit_oid.clone())
                            .collect();
                        return retryable_error(
                            format!(
                                "ingest batch write failed for repo {}: parsed_rows={} first_commit_oids={first_commit_oids:?} error={err}",
                                repo.repo_key,
                                parsed_outcome.rows.len()
                            ),
                            "db",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                };

                rows_written += batch_outcome.inserted_instances;
                let inserted_messages = match self
                    .pipeline
                    .insert_artifacts(run.id, "message_pk", &batch_outcome.message_pks)
                    .await
                {
                    Ok(value) => value,
                    Err(err) => {
                        return retryable_error(
                            format!(
                                "failed to insert ingest message artifacts for run {}: {err}",
                                run.id
                            ),
                            "db",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                };
                message_artifacts_written += inserted_messages;

                if let Some(last_commit) = chunk.last()
                    && let Err(err) = self
                        .catalog
                        .update_watermark(repo.id, Some(last_commit))
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

                if let Err(err) = self
                    .pipeline
                    .update_stage_progress(
                        run.id,
                        STAGE_INGEST,
                        serde_json::json!({
                            "repos_total": repos_total,
                            "repos_done": repos_done,
                            "chunks_done": chunks_done,
                            "commit_count": commit_count,
                            "rows_written": rows_written,
                            "parse_errors": parse_errors,
                            "parse_workers_effective": parse_workers_effective,
                            "message_artifacts_written": message_artifacts_written,
                        }),
                    )
                    .await
                {
                    return retryable_error(
                        format!(
                            "failed to update ingest stage progress for run {}: {err}",
                            run.id
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
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
            .pipeline
            .mark_stage_succeeded(
                run.id,
                STAGE_INGEST,
                serde_json::json!({
                    "repos_total": repos_total,
                    "repos_done": repos_done,
                    "chunks_done": chunks_done,
                    "commit_count": commit_count,
                    "rows_written": rows_written,
                    "parse_errors": parse_errors,
                    "parse_workers_effective": parse_workers_effective,
                    "message_artifacts_written": message_artifacts_written,
                }),
            )
            .await
        {
            return retryable_error(
                format!(
                    "failed to mark ingest stage succeeded for run {}: {err}",
                    run.id
                ),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        if let Err(err) = self
            .enqueue_pipeline_stage_job(run.id, &run.list_key, STAGE_THREADING, 9)
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
                "chunks_done": chunks_done,
                "commit_count": commit_count,
                "rows_written": rows_written,
                "parse_errors": parse_errors,
                "parse_workers_effective": parse_workers_effective,
                "message_artifacts_written": message_artifacts_written,
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

    async fn handle_pipeline_stage_threading(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: PipelineStageThreadingPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(value) => value,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid pipeline_stage_threading payload: {err}"),
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
                    "skipped": format!("run is in terminal state {}", run.state),
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }
        if run.current_stage != STAGE_THREADING {
            let Some(current_rank) = stage_rank(&run.current_stage) else {
                return JobExecutionOutcome::Terminal {
                    reason: format!(
                        "run {} has invalid current_stage={}",
                        run.id, run.current_stage
                    ),
                    kind: "pipeline_state".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            };
            let expected_rank = stage_rank(STAGE_THREADING).expect("known threading stage");
            if current_rank < expected_rank {
                return retryable_error(
                    format!(
                        "stage barrier: run {} is at stage {} and cannot execute threading yet",
                        run.id, run.current_stage
                    ),
                    "stage_barrier",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "run_id": run.id,
                    "skipped": format!("run stage already advanced to {}", run.current_stage),
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        if let Err(err) = self
            .pipeline
            .ensure_stage_running(run.id, STAGE_THREADING)
            .await
        {
            return retryable_error(
                format!(
                    "failed to mark threading stage running for run {}: {err}",
                    run.id
                ),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        let mut cursor = 0i64;
        let batch_limit = self.settings.mail.commit_batch_size.max(1) as i64;
        let mut chunks_done = 0u64;
        let mut message_count = 0u64;
        let mut source_messages_read = 0u64;
        let mut thread_artifacts_written = 0u64;
        let mut apply_stats = ThreadingApplyStats::default();

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
                            bytes_read: source_messages_read,
                            commit_count: message_count,
                            parse_errors: 0,
                        },
                    };
                }
                Ok(false) => {}
                Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
            }

            let chunk = match self
                .pipeline
                .list_artifact_ids_chunk(run.id, "message_pk", cursor, batch_limit)
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!(
                            "failed to list message artifacts for threading stage run {}: {err}",
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

            let outcome = match self
                .apply_threading_for_anchors(
                    run.mailing_list_id,
                    &run.list_key,
                    chunk.clone(),
                    Some(job.id),
                    false,
                )
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!(
                            "failed to apply threading stage chunk for run {}: {err}",
                            run.id
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };

            chunks_done += 1;
            message_count += chunk.len() as u64;
            source_messages_read += outcome.source_messages_read;
            apply_stats.threads_rebuilt += outcome.apply_stats.threads_rebuilt;
            apply_stats.nodes_written += outcome.apply_stats.nodes_written;
            apply_stats.dummy_nodes_written += outcome.apply_stats.dummy_nodes_written;
            apply_stats.messages_written += outcome.apply_stats.messages_written;
            apply_stats.stale_threads_removed += outcome.apply_stats.stale_threads_removed;

            let inserted = match self
                .pipeline
                .insert_artifacts(run.id, "thread_id", &outcome.impacted_thread_ids)
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!(
                            "failed to persist thread artifacts for run {}: {err}",
                            run.id
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };
            thread_artifacts_written += inserted;

            if let Err(err) = self
                .pipeline
                .update_stage_progress(
                    run.id,
                    STAGE_THREADING,
                    serde_json::json!({
                        "chunks_done": chunks_done,
                        "message_count": message_count,
                        "source_messages_read": source_messages_read,
                        "threads_rebuilt": apply_stats.threads_rebuilt,
                        "nodes_written": apply_stats.nodes_written,
                        "dummy_nodes_written": apply_stats.dummy_nodes_written,
                        "messages_written": apply_stats.messages_written,
                        "stale_threads_removed": apply_stats.stale_threads_removed,
                        "thread_artifacts_written": thread_artifacts_written,
                    }),
                )
                .await
            {
                return retryable_error(
                    format!(
                        "failed to update threading stage progress for run {}: {err}",
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
            .pipeline
            .mark_stage_succeeded(
                run.id,
                STAGE_THREADING,
                serde_json::json!({
                    "chunks_done": chunks_done,
                    "message_count": message_count,
                    "source_messages_read": source_messages_read,
                    "threads_rebuilt": apply_stats.threads_rebuilt,
                    "nodes_written": apply_stats.nodes_written,
                    "dummy_nodes_written": apply_stats.dummy_nodes_written,
                    "messages_written": apply_stats.messages_written,
                    "stale_threads_removed": apply_stats.stale_threads_removed,
                    "thread_artifacts_written": thread_artifacts_written,
                }),
            )
            .await
        {
            return retryable_error(
                format!(
                    "failed to mark threading stage succeeded for run {}: {err}",
                    run.id
                ),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }
        if let Err(err) = self
            .enqueue_pipeline_stage_job(run.id, &run.list_key, STAGE_LINEAGE_DIFF, 8)
            .await
        {
            return retryable_error(
                format!(
                    "failed to enqueue lineage_diff stage for run {}: {err}",
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
            .set_run_current_stage(run.id, STAGE_LINEAGE_DIFF)
            .await
        {
            return retryable_error(
                format!("failed to move run {} to lineage_diff stage: {err}", run.id),
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
                "chunks_done": chunks_done,
                "message_count": message_count,
                "source_messages_read": source_messages_read,
                "threads_rebuilt": apply_stats.threads_rebuilt,
                "nodes_written": apply_stats.nodes_written,
                "dummy_nodes_written": apply_stats.dummy_nodes_written,
                "messages_written": apply_stats.messages_written,
                "stale_threads_removed": apply_stats.stale_threads_removed,
                "thread_artifacts_written": thread_artifacts_written,
                "next_stage": STAGE_LINEAGE_DIFF,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written,
                bytes_read: source_messages_read,
                commit_count: message_count,
                parse_errors: 0,
            },
        }
    }

    async fn handle_pipeline_stage_lineage_diff(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: PipelineStageLineageDiffPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(value) => value,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid pipeline_stage_lineage_diff payload: {err}"),
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
                    "skipped": format!("run is in terminal state {}", run.state),
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }
        if run.current_stage != STAGE_LINEAGE_DIFF {
            let Some(current_rank) = stage_rank(&run.current_stage) else {
                return JobExecutionOutcome::Terminal {
                    reason: format!(
                        "run {} has invalid current_stage={}",
                        run.id, run.current_stage
                    ),
                    kind: "pipeline_state".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            };
            let expected_rank = stage_rank(STAGE_LINEAGE_DIFF).expect("known lineage_diff stage");
            if current_rank < expected_rank {
                return retryable_error(
                    format!(
                        "stage barrier: run {} is at stage {} and cannot execute lineage_diff yet",
                        run.id, run.current_stage
                    ),
                    "stage_barrier",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "run_id": run.id,
                    "skipped": format!("run stage already advanced to {}", run.current_stage),
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        if let Err(err) = self
            .pipeline
            .ensure_stage_running(run.id, STAGE_LINEAGE_DIFF)
            .await
        {
            return retryable_error(
                format!(
                    "failed to mark lineage_diff stage running for run {}: {err}",
                    run.id
                ),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        let mut cursor = 0i64;
        let batch_limit = self.settings.mail.commit_batch_size.max(1) as i64;
        let mut chunks_done = 0u64;
        let mut message_count = 0u64;
        let mut series_versions_written = 0u64;
        let mut patch_items_written = 0u64;
        let mut patch_items_updated = 0u64;
        let mut patch_item_files_written = 0u64;
        let mut patch_item_artifacts_written = 0u64;
        let mut patch_series_artifacts_written = 0u64;

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
                            commit_count: message_count,
                            parse_errors: 0,
                        },
                    };
                }
                Ok(false) => {}
                Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
            }

            let chunk = match self
                .pipeline
                .list_artifact_ids_chunk(run.id, "message_pk", cursor, batch_limit)
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!(
                            "failed to list message artifacts for lineage_diff stage run {}: {err}",
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

            let inserted_patch_items = match self
                .pipeline
                .insert_artifacts(run.id, "patch_item_id", &extract_outcome.patch_item_ids)
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!(
                            "failed to insert patch_item artifacts for run {}: {err}",
                            run.id
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };
            patch_item_artifacts_written += inserted_patch_items;

            let inserted_patch_series = match self
                .pipeline
                .insert_artifacts(run.id, "patch_series_id", &extract_outcome.series_ids)
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!(
                            "failed to insert patch_series artifacts for run {}: {err}",
                            run.id
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };
            patch_series_artifacts_written += inserted_patch_series;

            let mut patch_id_updates = 0u64;
            let mut diff_file_updates = 0u64;
            if !extract_outcome.patch_item_ids.is_empty() {
                let patch_item_ids = extract_outcome.patch_item_ids.clone();
                let (patch_id_result, diff_result) = tokio::join!(
                    process_patch_id_compute_batch(&self.lineage, &patch_item_ids),
                    process_diff_parse_patch_items(&self.lineage, &patch_item_ids),
                );

                let patch_id_outcome = match patch_id_result {
                    Ok(value) => value,
                    Err(err) => {
                        return retryable_error(
                            format!("patch_id compute failed for run {}: {err}", run.id),
                            "parse",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                };
                let diff_outcome = match diff_result {
                    Ok(value) => value,
                    Err(err) => {
                        return retryable_error(
                            format!("diff parse failed for run {}: {err}", run.id),
                            "parse",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                };
                patch_id_updates = patch_id_outcome.patch_items_updated;
                diff_file_updates = diff_outcome.patch_item_files_written;
            }

            chunks_done += 1;
            message_count += chunk.len() as u64;
            series_versions_written += extract_outcome.series_versions_written;
            patch_items_written += extract_outcome.patch_items_written;
            patch_items_updated += patch_id_updates;
            patch_item_files_written += diff_file_updates;

            if let Err(err) = self
                .pipeline
                .update_stage_progress(
                    run.id,
                    STAGE_LINEAGE_DIFF,
                    serde_json::json!({
                        "chunks_done": chunks_done,
                        "message_count": message_count,
                        "series_versions_written": series_versions_written,
                        "patch_items_written": patch_items_written,
                        "patch_items_updated": patch_items_updated,
                        "patch_item_files_written": patch_item_files_written,
                        "patch_item_artifacts_written": patch_item_artifacts_written,
                        "patch_series_artifacts_written": patch_series_artifacts_written,
                    }),
                )
                .await
            {
                return retryable_error(
                    format!(
                        "failed to update lineage_diff stage progress for run {}: {err}",
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
            .pipeline
            .mark_stage_succeeded(
                run.id,
                STAGE_LINEAGE_DIFF,
                serde_json::json!({
                    "chunks_done": chunks_done,
                    "message_count": message_count,
                    "series_versions_written": series_versions_written,
                    "patch_items_written": patch_items_written,
                    "patch_items_updated": patch_items_updated,
                    "patch_item_files_written": patch_item_files_written,
                    "patch_item_artifacts_written": patch_item_artifacts_written,
                    "patch_series_artifacts_written": patch_series_artifacts_written,
                }),
            )
            .await
        {
            return retryable_error(
                format!(
                    "failed to mark lineage_diff stage succeeded for run {}: {err}",
                    run.id
                ),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }
        if let Err(err) = self
            .enqueue_pipeline_stage_job(run.id, &run.list_key, STAGE_SEARCH, 7)
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
                "stage": STAGE_LINEAGE_DIFF,
                "chunks_done": chunks_done,
                "message_count": message_count,
                "series_versions_written": series_versions_written,
                "patch_items_written": patch_items_written,
                "patch_items_updated": patch_items_updated,
                "patch_item_files_written": patch_item_files_written,
                "patch_item_artifacts_written": patch_item_artifacts_written,
                "patch_series_artifacts_written": patch_series_artifacts_written,
                "next_stage": STAGE_SEARCH,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written: series_versions_written
                    + patch_items_written
                    + patch_item_files_written,
                bytes_read: 0,
                commit_count: message_count,
                parse_errors: 0,
            },
        }
    }

    async fn handle_pipeline_stage_search(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: PipelineStageSearchPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(value) => value,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid pipeline_stage_search payload: {err}"),
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
                    "skipped": format!("run is in terminal state {}", run.state),
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }
        if run.current_stage != STAGE_SEARCH {
            let Some(current_rank) = stage_rank(&run.current_stage) else {
                return JobExecutionOutcome::Terminal {
                    reason: format!(
                        "run {} has invalid current_stage={}",
                        run.id, run.current_stage
                    ),
                    kind: "pipeline_state".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            };
            let expected_rank = stage_rank(STAGE_SEARCH).expect("known search stage");
            if current_rank < expected_rank {
                return retryable_error(
                    format!(
                        "stage barrier: run {} is at stage {} and cannot execute search yet",
                        run.id, run.current_stage
                    ),
                    "stage_barrier",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "run_id": run.id,
                    "skipped": format!("run stage already advanced to {}", run.current_stage),
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        if let Err(err) = self
            .pipeline
            .ensure_stage_running(run.id, STAGE_SEARCH)
            .await
        {
            return retryable_error(
                format!(
                    "failed to mark search stage running for run {}: {err}",
                    run.id
                ),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        let plans = [
            ("thread_id", MeiliIndexKind::ThreadDocs),
            ("patch_series_id", MeiliIndexKind::PatchSeriesDocs),
            ("patch_item_id", MeiliIndexKind::PatchItemDocs),
        ];
        let max_inflight = self
            .settings
            .worker
            .stage_search_parallelism
            .max(1)
            .min(plans.len());
        let mut joinset = JoinSet::new();
        let mut next_plan = 0usize;
        let mut in_flight = 0usize;
        let mut family_results: Vec<SearchFamilyOutcome> = Vec::new();
        let mut total_docs_upserted = 0u64;
        let mut total_ids_seen = 0u64;
        let mut total_rows_written = 0u64;
        let mut total_bytes_read = 0u64;

        while next_plan < plans.len() || in_flight > 0 {
            while in_flight < max_inflight && next_plan < plans.len() {
                let (artifact_kind, index) = plans[next_plan];
                next_plan += 1;
                in_flight += 1;
                let handler = self.clone();
                let ctx_clone = ctx.clone();
                joinset.spawn(async move {
                    handler
                        .process_search_family(run.id, artifact_kind, index, ctx_clone)
                        .await
                });
            }

            let Some(joined) = joinset.join_next().await else {
                break;
            };
            in_flight = in_flight.saturating_sub(1);

            let family = match joined {
                Ok(Ok(value)) => value,
                Ok(Err(err)) => {
                    return retryable_error(
                        format!("search stage family failed for run {}: {err}", run.id),
                        "search_stage",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
                Err(err) => {
                    return retryable_error(
                        format!(
                            "search stage family task join failed for run {}: {err}",
                            run.id
                        ),
                        "runtime",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };

            total_docs_upserted += family.docs_upserted;
            total_ids_seen += family.ids_seen;
            total_rows_written += family.docs_upserted;
            total_bytes_read += family.docs_bytes;
            family_results.push(family);

            if let Err(err) = self
                .pipeline
                .update_stage_progress(
                    run.id,
                    STAGE_SEARCH,
                    serde_json::json!({
                        "families_completed": family_results.len(),
                        "families_total": plans.len(),
                        "total_docs_upserted": total_docs_upserted,
                        "total_ids_seen": total_ids_seen,
                    }),
                )
                .await
            {
                return retryable_error(
                    format!(
                        "failed to update search stage progress for run {}: {err}",
                        run.id
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        }

        family_results.sort_by(|a, b| a.index_uid.cmp(&b.index_uid));
        let mut embedding_jobs_enqueued = 0u64;
        let mut embedding_enqueue_errors: Vec<String> = Vec::new();
        if self.settings.embeddings.enabled {
            for (artifact_kind, scope) in [
                ("thread_id", EmbeddingScope::Thread),
                ("patch_series_id", EmbeddingScope::Series),
            ] {
                match self
                    .enqueue_embedding_jobs_for_artifact(
                        run.id,
                        &run.list_key,
                        artifact_kind,
                        scope,
                        Some(job.id),
                        4,
                    )
                    .await
                {
                    Ok(count) => embedding_jobs_enqueued += count,
                    Err(err) => {
                        warn!(
                            run_id = run.id,
                            artifact_kind,
                            error = %err,
                            "failed to enqueue incremental embedding batches"
                        );
                        embedding_enqueue_errors.push(err);
                    }
                }
            }
        }

        if let Err(err) = self
            .pipeline
            .mark_stage_succeeded(
                run.id,
                STAGE_SEARCH,
                serde_json::json!({
                    "families_completed": family_results.len(),
                    "total_docs_upserted": total_docs_upserted,
                    "total_ids_seen": total_ids_seen,
                    "families": family_results.clone(),
                    "embedding_jobs_enqueued": embedding_jobs_enqueued,
                    "embedding_enqueue_errors": embedding_enqueue_errors,
                }),
            )
            .await
        {
            return retryable_error(
                format!(
                    "failed to mark search stage succeeded for run {}: {err}",
                    run.id
                ),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }
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

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "run_id": run.id,
                "list_key": run.list_key,
                "stage": STAGE_SEARCH,
                "families_completed": family_results.len(),
                "total_docs_upserted": total_docs_upserted,
                "total_ids_seen": total_ids_seen,
                "families": family_results,
                "embedding_jobs_enqueued": embedding_jobs_enqueued,
                "embedding_enqueue_errors": embedding_enqueue_errors,
                "run_state": "succeeded",
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written: total_rows_written,
                bytes_read: total_bytes_read,
                commit_count: total_ids_seen,
                parse_errors: 0,
            },
        }
    }

    async fn process_search_family(
        &self,
        run_id: i64,
        artifact_kind: &'static str,
        index: MeiliIndexKind,
        ctx: ExecutionContext,
    ) -> Result<SearchFamilyOutcome, String> {
        let index_spec = index.spec();
        let settings = self.meili_index_settings(index);
        self.ensure_index_settings(index_spec, &settings, &ctx)
            .await
            .map_err(|err| format!("failed to prepare index {}: {err}", index.uid()))?;

        let chunk_limit = self.settings.mail.commit_batch_size.max(1) as i64;
        let mut cursor = 0i64;
        let mut chunks_done = 0u64;
        let mut ids_seen = 0u64;
        let mut docs_upserted = 0u64;
        let mut docs_bytes = 0u64;
        let mut tasks_submitted = 0u64;

        loop {
            let ids = self
                .pipeline
                .list_artifact_ids_chunk(run_id, artifact_kind, cursor, chunk_limit)
                .await
                .map_err(|err| {
                    format!(
                        "failed to list artifact ids for {} in run {}: {err}",
                        artifact_kind, run_id
                    )
                })?;
            if ids.is_empty() {
                break;
            }
            cursor = *ids.last().unwrap_or(&cursor);
            ids_seen += ids.len() as u64;
            chunks_done += 1;

            let docs = self
                .build_docs_for_index(index, &ids)
                .await
                .map_err(|err| format!("failed to build docs for {}: {err}", index.uid()))?;
            if docs.is_empty() {
                continue;
            }

            let task_uid = self
                .meili
                .upsert_documents(index_spec.uid, &docs)
                .await
                .map_err(|err| format!("failed to upsert docs for {}: {err}", index.uid()))?;
            self.wait_for_task(task_uid, &ctx).await.map_err(|err| {
                format!("meili task {} failed for {}: {err}", task_uid, index.uid())
            })?;

            docs_upserted += docs.len() as u64;
            docs_bytes += serde_json::to_vec(&docs)
                .map(|bytes| bytes.len() as u64)
                .unwrap_or(0);
            tasks_submitted += 1;
        }

        Ok(SearchFamilyOutcome {
            artifact_kind: artifact_kind.to_string(),
            index_uid: index.uid().to_string(),
            chunks_done,
            ids_seen,
            docs_upserted,
            docs_bytes,
            tasks_submitted,
        })
    }

    async fn enqueue_pipeline_stage_job(
        &self,
        run_id: i64,
        list_key: &str,
        stage: &str,
        priority: i32,
    ) -> Result<(), sqlx::Error> {
        let (job_type, payload_json) = match stage {
            STAGE_THREADING => (
                "pipeline_stage_threading",
                serde_json::to_value(PipelineStageThreadingPayload { run_id })
                    .unwrap_or_else(|_| serde_json::json!({})),
            ),
            STAGE_LINEAGE_DIFF => (
                "pipeline_stage_lineage_diff",
                serde_json::to_value(PipelineStageLineageDiffPayload { run_id })
                    .unwrap_or_else(|_| serde_json::json!({})),
            ),
            STAGE_SEARCH => (
                "pipeline_stage_search",
                serde_json::to_value(PipelineStageSearchPayload { run_id })
                    .unwrap_or_else(|_| serde_json::json!({})),
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
        list_key: &str,
        anchors: Vec<i64>,
        source_job_id: Option<i64>,
        enqueue_lineage: bool,
    ) -> nexus_db::Result<ThreadingWindowOutcome> {
        let mut anchors = anchors;
        anchors.sort_unstable();
        anchors.dedup();
        anchors.retain(|v| *v > 0);

        if anchors.is_empty() {
            return Ok(ThreadingWindowOutcome::default());
        }

        let mut message_set: BTreeSet<i64> = self
            .threading
            .expand_ancestor_closure(list_id, &anchors)
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
            .expand_ancestor_closure(list_id, &message_seed)
            .await?;

        let source_messages = self
            .threading
            .load_source_messages(list_id, &expanded_message_set)
            .await?;
        if source_messages.is_empty() {
            return Ok(ThreadingWindowOutcome {
                affected_threads: impacted_thread_ids.len() as u64,
                source_messages_read: 0,
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

        let mut lineage_enqueued = false;
        if enqueue_lineage {
            let patch_extract_payload = PatchExtractWindowPayload {
                list_key: list_key.to_string(),
                anchor_message_pks: anchors.clone(),
                source_job_id,
            };
            self.jobs
                .enqueue(EnqueueJobParams {
                    job_type: "patch_extract_window".to_string(),
                    payload_json: serde_json::to_value(patch_extract_payload)
                        .unwrap_or_else(|_| serde_json::json!({})),
                    priority: 11,
                    dedupe_scope: Some(format!("list:{list_key}")),
                    dedupe_key: Some(hash_message_pks(&anchors)),
                    run_after: None,
                    max_attempts: Some(8),
                })
                .await?;
            lineage_enqueued = true;
        }

        Ok(ThreadingWindowOutcome {
            affected_threads: impacted_thread_ids.len() as u64,
            source_messages_read: source_messages.len() as u64,
            apply_stats,
            lineage_enqueued,
            impacted_thread_ids,
        })
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

        if matches!(
            self.settings.worker.pipeline_execution_mode,
            PipelineExecutionMode::Staged
        ) {
            let ingest_active = match self
                .pipeline
                .is_list_ingest_stage_active(&payload.list_key)
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!(
                            "failed to evaluate stage barrier for list {}: {err}",
                            payload.list_key
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };
            if ingest_active {
                return retryable_error(
                    format!(
                        "ingest stage is active for list {}, deferring threading_update_window",
                        payload.list_key
                    ),
                    "stage_barrier",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
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
        let outcome = match self
            .apply_threading_for_anchors(
                list.id,
                &payload.list_key,
                payload.anchor_message_pks.clone(),
                Some(job.id),
                true,
            )
            .await
        {
            Ok(v) => v,
            Err(err) => {
                return retryable_error(
                    format!("failed to apply threading update window: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let thread_search_enqueued = match self
            .enqueue_meili_upsert_batch(
                &payload.list_key,
                MeiliIndexKind::ThreadDocs,
                &outcome.impacted_thread_ids,
                Some(job.id),
                6,
            )
            .await
        {
            Ok(value) => value,
            Err(err) => {
                return retryable_error(
                    format!("failed to enqueue meili thread upsert: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let rows_written = outcome.apply_stats.threads_rebuilt
            + outcome.apply_stats.nodes_written
            + outcome.apply_stats.messages_written
            + outcome.apply_stats.stale_threads_removed;

        info!(
            list_key = %payload.list_key,
            source_job_id = payload.source_job_id,
            affected_threads = outcome.affected_threads,
            threads_rebuilt = outcome.apply_stats.threads_rebuilt,
            nodes_written = outcome.apply_stats.nodes_written,
            dummy_nodes_written = outcome.apply_stats.dummy_nodes_written,
            messages_written = outcome.apply_stats.messages_written,
            stale_threads_removed = outcome.apply_stats.stale_threads_removed,
            lineage_enqueued = outcome.lineage_enqueued,
            thread_search_enqueued,
            "threading_update_window applied"
        );

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "list_key": payload.list_key,
                "source_job_id": payload.source_job_id,
                "affected_threads": outcome.affected_threads,
                "threads_rebuilt": outcome.apply_stats.threads_rebuilt,
                "nodes_written": outcome.apply_stats.nodes_written,
                "dummy_nodes_written": outcome.apply_stats.dummy_nodes_written,
                "messages_written": outcome.apply_stats.messages_written,
                "stale_threads_removed": outcome.apply_stats.stale_threads_removed,
                "lineage_enqueued": outcome.lineage_enqueued,
                "thread_search_enqueued": thread_search_enqueued,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written,
                bytes_read: outcome.source_messages_read,
                commit_count: outcome.apply_stats.threads_rebuilt,
                parse_errors: 0,
            },
        }
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

        if matches!(
            self.settings.worker.pipeline_execution_mode,
            PipelineExecutionMode::Staged
        ) {
            let ingest_active = match self
                .pipeline
                .is_list_ingest_stage_active(&payload.list_key)
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!(
                            "failed to evaluate stage barrier for list {}: {err}",
                            payload.list_key
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };
            if ingest_active {
                return retryable_error(
                    format!(
                        "ingest stage is active for list {}, deferring threading_rebuild_list",
                        payload.list_key
                    ),
                    "stage_barrier",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
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
        let mut meili_thread_jobs_enqueued = 0u64;
        let mut apply_stats = ThreadingApplyStats::default();
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
                .apply_threading_for_anchors(
                    list.id,
                    &payload.list_key,
                    chunk.clone(),
                    Some(job.id),
                    false,
                )
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
            apply_stats.nodes_written += outcome.apply_stats.nodes_written;
            apply_stats.dummy_nodes_written += outcome.apply_stats.dummy_nodes_written;
            apply_stats.messages_written += outcome.apply_stats.messages_written;
            apply_stats.stale_threads_removed += outcome.apply_stats.stale_threads_removed;

            match self
                .enqueue_meili_upsert_batch(
                    &payload.list_key,
                    MeiliIndexKind::ThreadDocs,
                    &outcome.impacted_thread_ids,
                    Some(job.id),
                    6,
                )
                .await
            {
                Ok(true) => meili_thread_jobs_enqueued += 1,
                Ok(false) => {}
                Err(err) => {
                    return retryable_error(
                        format!("failed to enqueue meili thread upsert: {err}"),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            }
        }

        info!(
            list_key = %payload.list_key,
            processed_chunks,
            processed_messages,
            affected_threads,
            threads_rebuilt = apply_stats.threads_rebuilt,
            nodes_written = apply_stats.nodes_written,
            dummy_nodes_written = apply_stats.dummy_nodes_written,
            messages_written = apply_stats.messages_written,
            stale_threads_removed = apply_stats.stale_threads_removed,
            meili_thread_jobs_enqueued,
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
                "nodes_written": apply_stats.nodes_written,
                "dummy_nodes_written": apply_stats.dummy_nodes_written,
                "messages_written": apply_stats.messages_written,
                "stale_threads_removed": apply_stats.stale_threads_removed,
                "meili_thread_jobs_enqueued": meili_thread_jobs_enqueued,
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

        if matches!(
            self.settings.worker.pipeline_execution_mode,
            PipelineExecutionMode::Staged
        ) {
            let ingest_active = match self
                .pipeline
                .is_list_ingest_stage_active(&payload.list_key)
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!(
                            "failed to evaluate stage barrier for list {}: {err}",
                            payload.list_key
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };
            if ingest_active {
                return retryable_error(
                    format!(
                        "ingest stage is active for list {}, deferring lineage_rebuild_list",
                        payload.list_key
                    ),
                    "stage_barrier",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
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

        if matches!(
            self.settings.worker.pipeline_execution_mode,
            PipelineExecutionMode::Staged
        ) {
            let ingest_active = match self
                .pipeline
                .is_list_ingest_stage_active(&payload.list_key)
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!(
                            "failed to evaluate stage barrier for list {}: {err}",
                            payload.list_key
                        ),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };
            if ingest_active {
                return retryable_error(
                    format!(
                        "ingest stage is active for list {}, deferring patch_extract_window",
                        payload.list_key
                    ),
                    "stage_barrier",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
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
        let mut patch_series_search_enqueued = false;
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

        if !extract_outcome.series_ids.is_empty() {
            match self
                .enqueue_meili_upsert_batch(
                    &payload.list_key,
                    MeiliIndexKind::PatchSeriesDocs,
                    &extract_outcome.series_ids,
                    Some(job.id),
                    6,
                )
                .await
            {
                Ok(value) => patch_series_search_enqueued = value,
                Err(err) => {
                    return retryable_error(
                        format!("failed to enqueue patch series meili upsert: {err}"),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            }
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
                "patch_series_search_enqueued": patch_series_search_enqueued,
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

        let patch_item_search_enqueued = match self
            .enqueue_meili_upsert_batch(
                "global",
                MeiliIndexKind::PatchItemDocs,
                &patch_item_ids,
                Some(job.id),
                6,
            )
            .await
        {
            Ok(value) => value,
            Err(err) => {
                return retryable_error(
                    format!("failed to enqueue patch item meili upsert: {err}"),
                    "db",
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
                "patch_item_search_enqueued": patch_item_search_enqueued,
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

    async fn handle_meili_upsert_batch(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();

        let payload: MeiliUpsertBatchPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(value) => value,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid meili_upsert_batch payload: {err}"),
                        kind: "payload".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };

        let mut ids = payload.ids.clone();
        ids.sort_unstable();
        ids.dedup();
        ids.retain(|value| *value > 0);
        if ids.is_empty() {
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "index": payload.index.uid(),
                    "ids_count": 0,
                    "source_job_id": payload.source_job_id,
                    "skipped": "empty_id_set"
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        let index_spec = payload.index.spec();
        let settings = self.meili_index_settings(payload.index);

        if let Err(err) = self
            .ensure_index_settings(index_spec, &settings, &ctx)
            .await
        {
            if err.is_transient() {
                return retryable_error(
                    format!("failed to prepare meili index {}: {err}", index_spec.uid),
                    "meili",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            return JobExecutionOutcome::Terminal {
                reason: format!("failed to prepare meili index {}: {err}", index_spec.uid),
                kind: "meili".to_string(),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        let docs = match self.build_docs_for_index(payload.index, &ids).await {
            Ok(value) => value,
            Err(err) => {
                let first_ids: Vec<i64> = ids.iter().take(5).copied().collect();
                return retryable_error(
                    format!(
                        "failed to build meili documents for {}: ids_count={} first_ids={first_ids:?} error={err}",
                        payload.index.uid(),
                        ids.len()
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };
        if docs.is_empty() {
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "index": payload.index.uid(),
                    "ids_count": ids.len(),
                    "docs_upserted": 0,
                    "source_job_id": payload.source_job_id,
                    "skipped": "no_docs_for_ids"
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

        let task_uid = match self.meili.upsert_documents(index_spec.uid, &docs).await {
            Ok(value) => value,
            Err(err) => {
                if err.is_transient() {
                    return retryable_error(
                        format!(
                            "meili upsert request failed for {}: {err}",
                            payload.index.uid()
                        ),
                        "meili",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
                return JobExecutionOutcome::Terminal {
                    reason: format!(
                        "meili upsert request failed for {}: {err}",
                        payload.index.uid()
                    ),
                    kind: "meili".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }
        };

        if let Err(err) = self.wait_for_task(task_uid, &ctx).await {
            if err.is_transient() {
                return retryable_error(
                    format!(
                        "meili task {task_uid} failed for {}: {err}",
                        payload.index.uid()
                    ),
                    "meili",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
            return JobExecutionOutcome::Terminal {
                reason: format!(
                    "meili task {task_uid} failed for {}: {err}",
                    payload.index.uid()
                ),
                kind: "meili".to_string(),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        let docs_bytes = serde_json::to_vec(&docs)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or(0);
        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "index": payload.index.uid(),
                "ids_count": ids.len(),
                "docs_upserted": docs.len(),
                "source_job_id": payload.source_job_id,
                "meili_task_id": task_uid
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written: docs.len() as u64,
                bytes_read: docs_bytes,
                commit_count: ids.len() as u64,
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
        let chunk_limit = self.settings.worker.backfill_batch_size.max(1) as i64;
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
        let list_key = payload
            .list_key
            .clone()
            .unwrap_or_else(|| "global".to_string());
        let meili_enqueued = match self
            .enqueue_meili_upsert_batch(&list_key, index, &ids, Some(job.id), 6)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                return retryable_error(
                    format!("failed to enqueue meili upsert after embeddings: {err}"),
                    "db",
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
                "meili_enqueued": meili_enqueued,
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

    async fn enqueue_meili_upsert_batch(
        &self,
        list_key: &str,
        index: MeiliIndexKind,
        ids: &[i64],
        source_job_id: Option<i64>,
        priority: i32,
    ) -> Result<bool, sqlx::Error> {
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.dedup();
        ids.retain(|value| *value > 0);
        if ids.is_empty() {
            return Ok(false);
        }

        let dedupe_key = format!("{}:{}", index.uid(), hash_i64_list(&ids));
        let payload = MeiliUpsertBatchPayload {
            index,
            ids,
            source_job_id,
        };

        self.jobs
            .enqueue(EnqueueJobParams {
                job_type: "meili_upsert_batch".to_string(),
                payload_json: serde_json::to_value(payload)
                    .unwrap_or_else(|_| serde_json::json!({})),
                priority,
                dedupe_scope: Some(format!("list:{list_key}")),
                dedupe_key: Some(dedupe_key),
                run_after: None,
                max_attempts: Some(8),
            })
            .await?;

        Ok(true)
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

    async fn enqueue_embedding_jobs_for_artifact(
        &self,
        run_id: i64,
        list_key: &str,
        artifact_kind: &'static str,
        scope: EmbeddingScope,
        source_job_id: Option<i64>,
        priority: i32,
    ) -> Result<u64, String> {
        if !self.settings.embeddings.enabled {
            return Ok(0);
        }

        let chunk_limit = self.settings.mail.commit_batch_size.max(1) as i64;
        let mut cursor = 0i64;
        let mut jobs_enqueued = 0u64;
        loop {
            let ids = self
                .pipeline
                .list_artifact_ids_chunk(run_id, artifact_kind, cursor, chunk_limit)
                .await
                .map_err(|err| {
                    format!(
                        "failed to list {} artifacts for run {}: {err}",
                        artifact_kind, run_id
                    )
                })?;
            if ids.is_empty() {
                break;
            }
            cursor = *ids.last().unwrap_or(&cursor);
            let enqueued = self
                .enqueue_embedding_generate_batch(list_key, scope, &ids, source_job_id, priority)
                .await
                .map_err(|err| {
                    format!(
                        "failed to enqueue embedding_generate_batch for {} in run {}: {err}",
                        artifact_kind, run_id
                    )
                })?;
            if enqueued {
                jobs_enqueued += 1;
            }
        }

        Ok(jobs_enqueued)
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
    apply_stats: ThreadingApplyStats,
    lineage_enqueued: bool,
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
}

struct IndexedParsedRow {
    index: usize,
    row: IngestCommitRow,
}

enum ParseCommitResult {
    Parsed(Box<IndexedParsedRow>),
    Skipped {
        commit_oid: String,
        reason: String,
        warn: bool,
    },
}

async fn parse_commit_rows(
    raw_commits: Vec<RawCommitMail>,
    concurrency: usize,
) -> ParsedCommitRows {
    if raw_commits.is_empty() {
        return ParsedCommitRows {
            rows: Vec::new(),
            parse_errors: 0,
        };
    }

    let mut joinset = JoinSet::new();
    let mut iter = raw_commits.into_iter();
    let max_concurrency = concurrency.max(1);

    let mut parsed_rows = Vec::new();
    let mut parse_errors = 0u64;

    loop {
        while joinset.len() < max_concurrency {
            let Some(raw_commit) = iter.next() else {
                break;
            };
            joinset.spawn_blocking(move || parse_one_commit(raw_commit));
        }

        let Some(next) = joinset.join_next().await else {
            break;
        };

        match next {
            Ok(ParseCommitResult::Parsed(parsed)) => parsed_rows.push(*parsed),
            Ok(ParseCommitResult::Skipped {
                commit_oid,
                reason,
                warn,
            }) => {
                parse_errors += 1;
                if warn {
                    warn!(commit_oid = %commit_oid, error = %reason, "mail parse error");
                }
            }
            Err(err) => {
                parse_errors += 1;
                warn!(error = %err, "mail parse task failed");
            }
        }
    }

    parsed_rows.sort_by_key(|row| row.index);

    ParsedCommitRows {
        rows: parsed_rows.into_iter().map(|row| row.row).collect(),
        parse_errors,
    }
}

fn parse_one_commit(raw_commit: RawCommitMail) -> ParseCommitResult {
    let parsed = match parse_email(&raw_commit.raw_rfc822) {
        Ok(v) => v,
        Err(ParseEmailError::MissingMessageId | ParseEmailError::MissingAuthorEmail) => {
            return ParseCommitResult::Skipped {
                commit_oid: raw_commit.commit_oid,
                reason: "missing required message headers".to_string(),
                warn: false,
            };
        }
        Err(ParseEmailError::SanitizationInvariantViolation(field)) => {
            return ParseCommitResult::Skipped {
                commit_oid: raw_commit.commit_oid,
                reason: format!("sanitization_invariant_violation:{field}"),
                warn: true,
            };
        }
        Err(err) => {
            return ParseCommitResult::Skipped {
                commit_oid: raw_commit.commit_oid,
                reason: err.to_string(),
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

fn hash_message_pks(message_pks: &[i64]) -> String {
    hash_i64_list(message_pks)
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

fn stage_rank(stage: &str) -> Option<u8> {
    match stage {
        STAGE_INGEST => Some(1),
        STAGE_THREADING => Some(2),
        STAGE_LINEAGE_DIFF => Some(3),
        STAGE_SEARCH => Some(4),
        _ => None,
    }
}

fn effective_parse_worker_count(
    ratio: f64,
    min_workers: usize,
    max_workers: usize,
    chunk_size: usize,
) -> usize {
    let available_cpus = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1);
    effective_parse_worker_count_with_cpus(
        available_cpus,
        ratio,
        min_workers,
        max_workers,
        chunk_size,
    )
}

fn effective_parse_worker_count_with_cpus(
    available_cpus: usize,
    ratio: f64,
    min_workers: usize,
    max_workers: usize,
    chunk_size: usize,
) -> usize {
    let mut min_workers = min_workers.max(1);
    let max_workers = max_workers.max(1);
    if min_workers > max_workers {
        min_workers = max_workers;
    }

    let target = ((available_cpus.max(1) as f64) * ratio).ceil() as usize;
    target
        .max(1)
        .clamp(min_workers, max_workers)
        .min(chunk_size.max(1))
}

fn discover_repo_relpaths(list_root: &Path) -> Result<Vec<String>, std::io::Error> {
    if !list_root.exists() {
        return Ok(Vec::new());
    }

    let mut repos = BTreeSet::new();
    let all_git = list_root.join("all.git");
    if all_git.is_dir() {
        repos.insert("all.git".to_string());
    }

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
                repos.insert(format!("git/{repo_name}"));
            }
        }
    }

    if repos.is_empty() && looks_like_bare_repo(list_root) {
        repos.insert(".".to_string());
    }

    Ok(repos.into_iter().collect())
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
    use super::effective_parse_worker_count_with_cpus;

    #[test]
    fn cpu_ratio_worker_count_matches_expected_rounding() {
        let workers = effective_parse_worker_count_with_cpus(16, 0.6, 2, 32, 10_000);
        assert_eq!(workers, 10);
    }

    #[test]
    fn cpu_ratio_worker_count_honors_chunk_size_cap() {
        let workers = effective_parse_worker_count_with_cpus(16, 0.6, 2, 32, 3);
        assert_eq!(workers, 3);
    }

    #[test]
    fn cpu_ratio_worker_count_honors_min_max_bounds() {
        let workers = effective_parse_worker_count_with_cpus(2, 0.1, 4, 8, 10_000);
        assert_eq!(workers, 4);

        let workers = effective_parse_worker_count_with_cpus(128, 0.9, 2, 32, 10_000);
        assert_eq!(workers, 32);
    }
}
