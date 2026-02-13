use std::path::Path;
use std::time::Instant;

use gix::hash::ObjectId;
use nexus_core::config::Settings;
use nexus_db::{
    CatalogStore, EnqueueJobParams, IngestStore, Job, JobStore, JobStoreMetrics, ParsedBodyInput,
    ParsedMessageInput,
};
use tracing::{info, warn};

use crate::mail::{ParseEmailError, parse_email};
use crate::payloads::{IngestCommitBatchPayload, RepoScanPayload};
use crate::scanner::{chunk_commit_oids, collect_new_commit_oids};
use crate::{ExecutionContext, JobExecutionOutcome};

#[derive(Clone)]
pub struct Phase0JobHandler {
    settings: Settings,
    catalog: CatalogStore,
    ingest: IngestStore,
    jobs: JobStore,
}

impl Phase0JobHandler {
    pub fn new(settings: Settings, catalog: CatalogStore, ingest: IngestStore, jobs: JobStore) -> Self {
        Self {
            settings,
            catalog,
            ingest,
            jobs,
        }
    }

    pub async fn handle(&self, job: Job, ctx: ExecutionContext) -> JobExecutionOutcome {
        match job.job_type.as_str() {
            "repo_scan" => self.handle_repo_scan(job, ctx).await,
            "ingest_commit_batch" => self.handle_ingest_commit_batch(job, ctx).await,
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

        let commits = match collect_new_commit_oids(Path::new(&payload.mirror_path), since_commit_oid.map(|v| v.as_str())) {
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
                    payload_json: serde_json::to_value(ingest_payload).unwrap_or(serde_json::json!({})),
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

    async fn handle_ingest_commit_batch(&self, job: Job, ctx: ExecutionContext) -> JobExecutionOutcome {
        let started = Instant::now();

        let payload: IngestCommitBatchPayload = match serde_json::from_value(job.payload_json.clone()) {
            Ok(v) => v,
            Err(err) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("invalid ingest payload: {err}"),
                    kind: "payload".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }
        };

        let repo = match self.catalog.get_repo(&payload.list_key, &payload.repo_key).await {
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

        if let Some(last_commit) = last_success_commit.as_deref() {
            if let Err(err) = self.catalog.update_watermark(repo.id, Some(last_commit)).await {
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
                "chunk_index": payload.chunk_index,
                "commit_count": processed_commits,
                "rows_written": rows_written,
                "parse_errors": parse_errors,
                "last_success_commit": last_success_commit,
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
