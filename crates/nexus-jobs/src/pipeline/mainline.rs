use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
};
use std::time::Instant;

use anyhow::Context;
use chrono::{DateTime, Utc};
use gix::bstr::ByteSlice;
use imara_diff::{Algorithm as DiffAlgorithm, UnifiedDiffBuilder, diff as build_unified_diff};
use nexus_db::MainlinePatchCandidate;
use regex::Regex;
use serde_json::json;
use tracing::warn;

use super::*;
use crate::patch_id::compute_patch_id_stable;
use crate::patch_subject::parse_patch_subject;

#[derive(Debug, Clone)]
struct MainlineCommitRecord {
    commit_oid: ObjectId,
    committed_at: DateTime<Utc>,
    subject: String,
    body: String,
    patch_id_stable: Option<String>,
}

#[derive(Debug, Clone)]
struct CommitMatch {
    patch_item_id: i64,
    patch_series_id: i64,
    match_method: String,
    matched_message_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct CommitTagInfo {
    first_containing_tag: Option<String>,
    first_final_release: Option<String>,
    first_tag_sort_key: Option<i64>,
    first_release_sort_key: Option<i64>,
}

#[derive(Debug)]
struct MainlineScanPreparation {
    head_commit_oid: String,
    commit_oids: Vec<ObjectId>,
    commit_tag_map: HashMap<ObjectId, CommitTagInfo>,
}

#[derive(Debug)]
struct IndexedMainlineCommitRecord {
    index: usize,
    record: MainlineCommitRecord,
}

#[derive(Debug)]
struct MainlineCommitPartitionItem {
    index: usize,
    commit_oid: String,
}

#[derive(Debug, Clone)]
struct ReleaseTag {
    name: String,
    target_oid: ObjectId,
    parsed: ParsedKernelTag,
}

const PREP_PHASE_OPENING_REPO: u8 = 0;
const PREP_PHASE_ENUMERATING_COMMITS: u8 = 1;
const PREP_PHASE_ASSIGNING_TAGS: u8 = 2;
const PREP_PHASE_DONE: u8 = 3;

#[derive(Debug, Default)]
struct MainlinePrepProgress {
    phase: AtomicU8,
    walked_commits: AtomicU64,
    selected_commits: AtomicU64,
    release_tags_total: AtomicU64,
    release_tags_processed: AtomicU64,
    tagged_commits: AtomicU64,
}

impl MainlinePrepProgress {
    fn phase_label(&self) -> &'static str {
        match self.phase.load(Ordering::Relaxed) {
            PREP_PHASE_ENUMERATING_COMMITS => "enumerating_commits",
            PREP_PHASE_ASSIGNING_TAGS => "assigning_release_tags",
            PREP_PHASE_DONE => "ready_for_commit_windows",
            _ => "opening_repo",
        }
    }
}

impl Phase0JobHandler {
    pub(super) async fn handle_mainline_scan_run(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: MainlineScanRunPayload = match serde_json::from_value(job.payload_json.clone())
        {
            Ok(value) => value,
            Err(err) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("invalid mainline_scan_run payload: {err}"),
                    kind: "payload".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }
        };

        let mut run = match self.mainline.get_run(payload.run_id).await {
            Ok(Some(value)) => value,
            Ok(None) => {
                return JobExecutionOutcome::Terminal {
                    reason: format!("mainline_scan_run {} not found", payload.run_id),
                    kind: "not_found".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }
            Err(err) => {
                return retryable_error(
                    format!("failed to load mainline_scan_run {}: {err}", payload.run_id),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        if run.state == "queued" {
            if let Err(err) = self.mainline.mark_run_running(run.id).await {
                return retryable_error(
                    format!("failed to mark mainline_scan_run {} running: {err}", run.id),
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
                result_json: json!({
                    "run_id": run.id,
                    "skipped": format!("run is in state {}", run.state),
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        let repo_path = PathBuf::from(self.settings.mainline.repo_path.trim());
        if repo_path.as_os_str().is_empty() {
            let reason = "mainline repo path is empty".to_string();
            let _ = self.mainline.mark_run_failed(run.id, &reason).await;
            return JobExecutionOutcome::Terminal {
                reason,
                kind: "config".to_string(),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        if !repo_path.exists() {
            let reason = format!("mainline repo path does not exist: {}", repo_path.display());
            let _ = self.mainline.mark_run_failed(run.id, &reason).await;
            return JobExecutionOutcome::Terminal {
                reason,
                kind: "config".to_string(),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        let state = match self
            .mainline
            .ensure_state(&run.repo_key, &run.ref_name)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                return retryable_error(
                    format!("failed to ensure mainline scan state: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        if run.mode == "bootstrap"
            && let Err(err) = self
                .mainline
                .set_public_filter_ready(&run.repo_key, false)
                .await
        {
            return retryable_error(
                format!("failed to reset mainline public filter readiness: {err}"),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        let since_commit_oid = run.cursor_commit_oid.clone().or_else(|| {
            if run.mode == "incremental" {
                state.last_scanned_commit_oid.clone()
            } else {
                None
            }
        });
        let chunk_size = self.settings.mainline.commit_window_size.max(1);
        let scan_parallelism = self
            .settings
            .mainline
            .scan_parallelism
            .max(1)
            .min(chunk_size);

        let cancel_flag = Arc::new(AtomicBool::new(false));
        let prep_progress = Arc::new(MainlinePrepProgress::default());
        let prep_path = repo_path.clone();
        let prep_ref_name = run.ref_name.clone();
        let prep_since = since_commit_oid.clone();
        let prep_cancel = Arc::clone(&cancel_flag);
        let prep_progress_handle = Arc::clone(&prep_progress);
        let mut prep_handle = tokio::task::spawn_blocking(move || {
            prepare_mainline_scan(
                &prep_path,
                &prep_ref_name,
                prep_since.as_deref(),
                prep_cancel,
                prep_progress_handle,
            )
        });

        let preparation = loop {
            tokio::select! {
                prep = &mut prep_handle => {
                    match prep {
                        Ok(Ok(value)) => break value,
                        Ok(Err(err)) => {
                            if cancel_flag.load(Ordering::Relaxed) {
                                let _ = self.mainline.mark_run_cancelled(run.id, "cancel requested").await;
                                return JobExecutionOutcome::Cancelled {
                                    reason: "cancel requested".to_string(),
                                    metrics: JobStoreMetrics {
                                        duration_ms: started.elapsed().as_millis(),
                                        rows_written: 0,
                                        bytes_read: 0,
                                        commit_count: 0,
                                        parse_errors: 0,
                                    },
                                };
                            }
                            return retryable_error(
                                format!("failed to prepare mainline scan: {err}"),
                                "git",
                                &job,
                                started.elapsed().as_millis(),
                                &self.settings,
                            );
                        }
                        Err(err) => {
                            return retryable_error(
                                format!("mainline scan prep task failed: {err}"),
                                "git",
                                &job,
                                started.elapsed().as_millis(),
                                &self.settings,
                            );
                        }
                    }
                }
                _ = sleep(Duration::from_secs(5)) => {
                    if let Err(err) = ctx.heartbeat().await {
                        warn!(job_id = job.id, error = %err, "heartbeat update failed");
                    }
                    if let Err(err) = self
                        .mainline
                        .update_run_progress(
                            run.id,
                            run.cursor_commit_oid.as_deref(),
                            0,
                            0,
                            0,
                            0,
                            prep_progress_json(
                                &run,
                                scan_parallelism,
                                chunk_size,
                                &prep_progress,
                            ),
                        )
                        .await
                    {
                        return retryable_error(
                            format!("failed to persist mainline prep progress: {err}"),
                            "db",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                    match ctx.is_cancel_requested().await {
                        Ok(true) => cancel_flag.store(true, Ordering::Relaxed),
                        Ok(false) => {}
                        Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
                    }
                }
            }
        };

        let head_commit_oid = preparation.head_commit_oid.clone();
        let mut scanned_commits = run.scanned_commits.max(0);
        let mut matched_commits = run.matched_commits.max(0);
        let mut matched_patch_items = run.matched_patch_items.max(0);
        let mut updated_series = run.updated_series.max(0);
        let mut last_processed_commit_oid = run
            .cursor_commit_oid
            .clone()
            .or_else(|| state.last_scanned_commit_oid.clone());
        let mut affected_series_ids = BTreeSet::new();
        let meili_chunk_limit = self.settings.meili.upsert_batch_size.max(1);

        for commit_chunk in preparation.commit_oids.chunks(chunk_size) {
            if let Err(err) = ctx.heartbeat().await {
                warn!(job_id = job.id, error = %err, "heartbeat update failed");
            }
            match ctx.is_cancel_requested().await {
                Ok(true) => {
                    let _ = self
                        .mainline
                        .mark_run_cancelled(run.id, "cancel requested")
                        .await;
                    return JobExecutionOutcome::Cancelled {
                        reason: "cancel requested".to_string(),
                        metrics: JobStoreMetrics {
                            duration_ms: started.elapsed().as_millis(),
                            rows_written: i64_to_u64(matched_patch_items + updated_series),
                            bytes_read: 0,
                            commit_count: i64_to_u64(scanned_commits),
                            parse_errors: 0,
                        },
                    };
                }
                Ok(false) => {}
                Err(err) => warn!(job_id = job.id, error = %err, "cancel check failed"),
            }

            let commits = match load_mainline_commit_batch(
                repo_path.clone(),
                commit_chunk.to_vec(),
                scan_parallelism,
            )
            .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!("failed to load mainline commit batch: {err}"),
                        "git",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };
            scanned_commits += i64::try_from(commits.len()).unwrap_or(i64::MAX);
            if let Some(last) = commits.last() {
                last_processed_commit_oid = Some(last.commit_oid.to_string());
            }

            let batch_matches = match self
                .resolve_commit_batch_matches(&commits, &preparation.commit_tag_map)
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    return retryable_error(
                        format!("failed to resolve mainline matches: {err}"),
                        "mainline_match",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            };

            if !batch_matches.commit_upserts.is_empty()
                && let Err(err) = self
                    .mainline
                    .upsert_mainline_commits(&batch_matches.commit_upserts)
                    .await
            {
                return retryable_error(
                    format!("failed to upsert mainline commit metadata: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }

            if !batch_matches.patch_matches.is_empty()
                && let Err(err) = self
                    .mainline
                    .replace_canonical_matches(&batch_matches.patch_matches)
                    .await
            {
                return retryable_error(
                    format!("failed to store canonical mainline matches: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }

            matched_commits +=
                i64::try_from(batch_matches.matched_commit_oids.len()).unwrap_or(i64::MAX);
            matched_patch_items +=
                i64::try_from(batch_matches.patch_matches.len()).unwrap_or(i64::MAX);
            affected_series_ids.extend(batch_matches.affected_series_ids);

            if let Err(err) = self
                .mainline
                .update_run_progress(
                    run.id,
                    last_processed_commit_oid.as_deref(),
                    scanned_commits,
                    matched_commits,
                    matched_patch_items,
                    updated_series,
                    json!({
                        "mode": run.mode,
                        "ref_name": run.ref_name,
                        "head_commit_oid": head_commit_oid,
                        "cursor_commit_oid": last_processed_commit_oid,
                        "scanned_commits": scanned_commits,
                        "matched_commits": matched_commits,
                        "matched_patch_items": matched_patch_items,
                        "updated_series": updated_series,
                        "scan_parallelism": scan_parallelism,
                        "commit_window_size": chunk_size,
                    }),
                )
                .await
            {
                return retryable_error(
                    format!("failed to persist mainline scan progress: {err}"),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        }

        if run.mode == "bootstrap" {
            let mut cursor = 0i64;
            loop {
                if let Err(err) = ctx.heartbeat().await {
                    warn!(job_id = job.id, error = %err, "heartbeat update failed");
                }
                let series_ids = match self.mainline.list_series_ids_after(cursor, 2_000).await {
                    Ok(value) => value,
                    Err(err) => {
                        return retryable_error(
                            format!("failed to list patch series ids for bootstrap rollup: {err}"),
                            "db",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                };
                if series_ids.is_empty() {
                    break;
                }

                if let Err(err) = self.mainline.recompute_rollups(&series_ids).await {
                    return retryable_error(
                        format!("failed to recompute bootstrap mainline rollups: {err}"),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }

                if let Err(err) = self
                    .upsert_meili_docs_inline(
                        MeiliIndexKind::PatchSeriesDocs,
                        &series_ids,
                        meili_chunk_limit,
                        &ctx,
                    )
                    .await
                {
                    return retryable_error(
                        format!("failed to rewrite patch_series_docs during bootstrap: {err}"),
                        "meili",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }

                updated_series += i64::try_from(series_ids.len()).unwrap_or(i64::MAX);
                cursor = *series_ids.last().unwrap_or(&cursor);
                if let Err(err) = self
                    .mainline
                    .update_run_progress(
                        run.id,
                        last_processed_commit_oid.as_deref(),
                        scanned_commits,
                        matched_commits,
                        matched_patch_items,
                        updated_series,
                        json!({
                            "mode": run.mode,
                            "ref_name": run.ref_name,
                            "head_commit_oid": head_commit_oid,
                            "cursor_commit_oid": last_processed_commit_oid,
                            "scanned_commits": scanned_commits,
                            "matched_commits": matched_commits,
                            "matched_patch_items": matched_patch_items,
                            "updated_series": updated_series,
                            "bootstrap_rewrite_cursor": cursor,
                            "scan_parallelism": scan_parallelism,
                            "commit_window_size": chunk_size,
                        }),
                    )
                    .await
                {
                    return retryable_error(
                        format!("failed to persist bootstrap rewrite progress: {err}"),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
            }
        } else if !affected_series_ids.is_empty() {
            let series_ids = affected_series_ids.into_iter().collect::<Vec<_>>();
            for chunk in series_ids.chunks(2_000) {
                if let Err(err) = self.mainline.recompute_rollups(chunk).await {
                    return retryable_error(
                        format!("failed to recompute incremental mainline rollups: {err}"),
                        "db",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
                if let Err(err) = self
                    .upsert_meili_docs_inline(
                        MeiliIndexKind::PatchSeriesDocs,
                        chunk,
                        meili_chunk_limit,
                        &ctx,
                    )
                    .await
                {
                    return retryable_error(
                        format!("failed to update patch_series_docs after mainline scan: {err}"),
                        "meili",
                        &job,
                        started.elapsed().as_millis(),
                        &self.settings,
                    );
                }
                updated_series += i64::try_from(chunk.len()).unwrap_or(i64::MAX);
            }
        }

        let public_filter_ready = run.mode == "bootstrap" || state.public_filter_ready;
        if let Err(err) = self
            .mainline
            .mark_state_succeeded(
                &run.repo_key,
                &head_commit_oid,
                last_processed_commit_oid.as_deref(),
                run.mode == "bootstrap",
                public_filter_ready,
            )
            .await
        {
            return retryable_error(
                format!("failed to persist mainline scan state success: {err}"),
                "db",
                &job,
                started.elapsed().as_millis(),
                &self.settings,
            );
        }

        let result_json = json!({
            "run_id": run.id,
            "mode": run.mode,
            "state": "succeeded",
            "head_commit_oid": head_commit_oid,
            "cursor_commit_oid": last_processed_commit_oid,
            "scanned_commits": scanned_commits,
            "matched_commits": matched_commits,
            "matched_patch_items": matched_patch_items,
            "updated_series": updated_series,
            "public_filter_ready": public_filter_ready,
            "scan_parallelism": scan_parallelism,
            "commit_window_size": chunk_size,
        });
        if let Err(err) = self
            .mainline
            .mark_run_succeeded(run.id, result_json.clone())
            .await
        {
            return retryable_error(
                format!("failed to mark mainline scan run succeeded: {err}"),
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
                rows_written: i64_to_u64(matched_patch_items + updated_series),
                bytes_read: 0,
                commit_count: i64_to_u64(scanned_commits),
                parse_errors: 0,
            },
        }
    }

    async fn resolve_commit_batch_matches(
        &self,
        commits: &[MainlineCommitRecord],
        commit_tag_map: &HashMap<ObjectId, CommitTagInfo>,
    ) -> anyhow::Result<ResolvedBatchMatches> {
        if commits.is_empty() {
            return Ok(ResolvedBatchMatches::default());
        }

        let mut unique_message_ids = BTreeSet::new();
        let mut unique_patch_ids = BTreeSet::new();
        let mut prepared = Vec::with_capacity(commits.len());

        for commit in commits {
            let link_message_ids = extract_link_message_ids(&commit.body);
            unique_message_ids.extend(link_message_ids.iter().cloned());
            if let Some(patch_id) = commit.patch_id_stable.as_ref() {
                unique_patch_ids.insert(patch_id.clone());
            }
            prepared.push(PreparedCommitMatch {
                commit: commit.clone(),
                subject_norm: normalize_commit_subject(&commit.subject),
                link_message_ids,
                patch_id: commit.patch_id_stable.clone(),
            });
        }

        let link_candidates = self
            .mainline
            .find_patch_candidates_by_message_ids(
                &unique_message_ids.into_iter().collect::<Vec<_>>(),
            )
            .await?;
        let patch_candidates = self
            .mainline
            .find_patch_candidates_by_patch_ids(&unique_patch_ids.into_iter().collect::<Vec<_>>())
            .await?;

        let mut link_candidates_by_message_id: HashMap<String, Vec<MainlinePatchCandidate>> =
            HashMap::new();
        for candidate in link_candidates {
            link_candidates_by_message_id
                .entry(candidate.message_id_primary.clone())
                .or_default()
                .push(candidate);
        }

        let mut patch_candidates_by_patch_id: HashMap<String, Vec<MainlinePatchCandidate>> =
            HashMap::new();
        for candidate in patch_candidates {
            if let Some(patch_id) = candidate.patch_id_stable.clone() {
                patch_candidates_by_patch_id
                    .entry(patch_id)
                    .or_default()
                    .push(candidate);
            }
        }

        let mut candidate_patch_item_ids = BTreeSet::new();
        for candidates in link_candidates_by_message_id.values() {
            candidate_patch_item_ids
                .extend(candidates.iter().map(|candidate| candidate.patch_item_id));
        }
        for candidates in patch_candidates_by_patch_id.values() {
            candidate_patch_item_ids
                .extend(candidates.iter().map(|candidate| candidate.patch_item_id));
        }
        let existing_canonical_ids = self
            .mainline
            .list_existing_canonical_patch_item_ids(
                &candidate_patch_item_ids.into_iter().collect::<Vec<_>>(),
            )
            .await?
            .into_iter()
            .collect::<HashSet<_>>();

        let mut commit_upserts = Vec::new();
        let mut patch_matches = Vec::new();
        let mut matched_commit_oids = BTreeSet::new();
        let mut affected_series_ids = BTreeSet::new();
        let mut claimed_patch_item_ids = HashSet::new();

        for prepared_commit in prepared {
            let link_matches = collect_link_matches(
                &prepared_commit.link_message_ids,
                &link_candidates_by_message_id,
                &prepared_commit.subject_norm,
            );
            let patch_id_matches = prepared_commit
                .patch_id
                .as_ref()
                .map(|patch_id| {
                    collect_patch_id_matches(
                        patch_candidates_by_patch_id
                            .get(patch_id)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]),
                        &prepared_commit.subject_norm,
                    )
                })
                .unwrap_or_default();

            let mut commit_matches = BTreeMap::new();
            for (candidate, matched_message_id) in link_matches {
                commit_matches
                    .entry(candidate.patch_item_id)
                    .or_insert(CommitMatch {
                        patch_item_id: candidate.patch_item_id,
                        patch_series_id: candidate.patch_series_id,
                        match_method: "link".to_string(),
                        matched_message_id: Some(matched_message_id),
                    });
            }
            for candidate in patch_id_matches {
                commit_matches
                    .entry(candidate.patch_item_id)
                    .or_insert(CommitMatch {
                        patch_item_id: candidate.patch_item_id,
                        patch_series_id: candidate.patch_series_id,
                        match_method: "patch_id".to_string(),
                        matched_message_id: None,
                    });
            }

            let filtered_matches = filter_new_canonical_matches(
                commit_matches.into_values().collect(),
                &existing_canonical_ids,
                &claimed_patch_item_ids,
            );
            if filtered_matches.is_empty() {
                continue;
            }

            let tag_info = commit_tag_map
                .get(&prepared_commit.commit.commit_oid)
                .cloned()
                .unwrap_or_default();
            let commit_oid = prepared_commit.commit.commit_oid.to_string();

            commit_upserts.push(nexus_db::UpsertMainlineCommitInput {
                commit_oid: commit_oid.clone(),
                subject: prepared_commit.commit.subject.clone(),
                committed_at: prepared_commit.commit.committed_at,
                first_containing_tag: tag_info.first_containing_tag.clone(),
                first_final_release: tag_info.first_final_release.clone(),
                first_tag_sort_key: tag_info.first_tag_sort_key,
                first_release_sort_key: tag_info.first_release_sort_key,
            });
            matched_commit_oids.insert(commit_oid.clone());

            for matched in filtered_matches {
                claimed_patch_item_ids.insert(matched.patch_item_id);
                affected_series_ids.insert(matched.patch_series_id);
                patch_matches.push(nexus_db::CanonicalPatchMatchInput {
                    patch_item_id: matched.patch_item_id,
                    commit_oid: commit_oid.clone(),
                    match_method: matched.match_method,
                    matched_message_id: matched.matched_message_id,
                });
            }
        }

        Ok(ResolvedBatchMatches {
            commit_upserts,
            patch_matches,
            matched_commit_oids,
            affected_series_ids,
        })
    }
}

#[derive(Default)]
struct ResolvedBatchMatches {
    commit_upserts: Vec<nexus_db::UpsertMainlineCommitInput>,
    patch_matches: Vec<nexus_db::CanonicalPatchMatchInput>,
    matched_commit_oids: BTreeSet<String>,
    affected_series_ids: BTreeSet<i64>,
}

#[derive(Debug, Clone)]
struct PreparedCommitMatch {
    commit: MainlineCommitRecord,
    subject_norm: String,
    link_message_ids: Vec<String>,
    patch_id: Option<String>,
}

fn prepare_mainline_scan(
    repo_path: &Path,
    ref_name: &str,
    since_commit_oid: Option<&str>,
    cancel_flag: Arc<AtomicBool>,
    prep_progress: Arc<MainlinePrepProgress>,
) -> anyhow::Result<MainlineScanPreparation> {
    prep_progress
        .phase
        .store(PREP_PHASE_OPENING_REPO, Ordering::Relaxed);
    let repo = open_mainline_repo(repo_path)?;
    let head_commit_oid = resolve_ref_oid(&repo, ref_name)?;

    let since_commit_oid = match since_commit_oid {
        Some(value) if !value.trim().is_empty() => Some(
            ObjectId::from_hex(value.trim().as_bytes())
                .with_context(|| format!("invalid mainline watermark oid {value}"))?,
        ),
        _ => None,
    };
    if let Some(stop) = since_commit_oid {
        repo.find_object(stop)
            .with_context(|| format!("unknown mainline watermark commit {stop}"))?;
    }

    let mut commit_rows = Vec::new();
    prep_progress
        .phase
        .store(PREP_PHASE_ENUMERATING_COMMITS, Ordering::Relaxed);
    let mut walk = repo
        .rev_walk([ObjectId::from_hex(head_commit_oid.as_bytes())?])
        .sorting(gix::traverse::commit::simple::Sorting::ByCommitTimeNewestFirst)
        .selected(|oid| {
            since_commit_oid
                .as_ref()
                .map(|stop| stop.as_ref() != oid)
                .unwrap_or(true)
        })?;

    while let Some(info) = walk.next() {
        if cancel_flag.load(Ordering::Relaxed) {
            anyhow::bail!("mainline scan preparation cancelled");
        }
        let info = info?;
        prep_progress.walked_commits.fetch_add(1, Ordering::Relaxed);
        if info.parent_ids.len() > 1 {
            continue;
        }
        commit_rows.push((info.commit_time(), info.id));
        prep_progress
            .selected_commits
            .store(commit_rows.len() as u64, Ordering::Relaxed);
    }

    commit_rows.sort_unstable_by(|(left_ts, left_oid), (right_ts, right_oid)| {
        left_ts.cmp(right_ts).then_with(|| left_oid.cmp(right_oid))
    });

    let commit_oids = commit_rows
        .into_iter()
        .map(|(_, oid)| oid)
        .collect::<Vec<_>>();
    let commit_targets = commit_oids.iter().copied().collect::<HashSet<_>>();
    let release_tags = collect_release_tags(&repo)?;
    prep_progress
        .phase
        .store(PREP_PHASE_ASSIGNING_TAGS, Ordering::Relaxed);
    prep_progress
        .release_tags_total
        .store(release_tags.len() as u64, Ordering::Relaxed);
    let commit_tag_map = assign_release_tags(
        &repo,
        &release_tags,
        &commit_targets,
        &cancel_flag,
        &prep_progress,
    )?;
    prep_progress
        .phase
        .store(PREP_PHASE_DONE, Ordering::Relaxed);

    Ok(MainlineScanPreparation {
        head_commit_oid,
        commit_oids,
        commit_tag_map,
    })
}

fn open_mainline_repo(repo_path: &Path) -> anyhow::Result<gix::Repository> {
    let mut repo = gix::open(repo_path)?;
    repo.object_cache_size_if_unset(64 * 1024 * 1024);
    Ok(repo)
}

fn resolve_ref_oid(repo: &gix::Repository, ref_name: &str) -> anyhow::Result<String> {
    Ok(repo
        .find_reference(ref_name)
        .with_context(|| format!("failed to find ref {ref_name}"))?
        .into_fully_peeled_id()
        .with_context(|| format!("failed to peel ref {ref_name}"))?
        .to_string())
}

fn collect_release_tags(repo: &gix::Repository) -> anyhow::Result<Vec<ReleaseTag>> {
    let mut tags = Vec::new();
    for reference in repo.references()?.tags()? {
        let reference = reference.map_err(anyhow::Error::msg)?;
        let tag_name = reference.name().shorten().to_str_lossy().to_string();
        let Some(parsed) = parse_kernel_tag(&tag_name) else {
            continue;
        };
        let target_oid = reference
            .into_fully_peeled_id()
            .with_context(|| format!("failed to fully peel tag {tag_name}"))?
            .detach();
        if repo.find_object(target_oid)?.try_into_commit().is_err() {
            warn!(
                tag = %tag_name,
                target_oid = %target_oid,
                "skipping non-commit kernel release tag"
            );
            continue;
        }
        tags.push(ReleaseTag {
            name: tag_name,
            target_oid,
            parsed,
        });
    }
    tags.sort_unstable_by(|left, right| {
        left.parsed
            .tag_sort_key()
            .cmp(&right.parsed.tag_sort_key())
            .then_with(|| left.name.cmp(&right.name))
    });
    Ok(tags)
}

fn assign_release_tags(
    repo: &gix::Repository,
    release_tags: &[ReleaseTag],
    commit_targets: &HashSet<ObjectId>,
    cancel_flag: &AtomicBool,
    prep_progress: &MainlinePrepProgress,
) -> anyhow::Result<HashMap<ObjectId, CommitTagInfo>> {
    let mut tag_map = HashMap::new();
    let mut covered = HashSet::new();

    for (idx, release_tag) in release_tags.iter().enumerate() {
        prep_progress
            .release_tags_processed
            .store(idx as u64, Ordering::Relaxed);
        let tag_info = CommitTagInfo {
            first_containing_tag: Some(release_tag.name.clone()),
            first_final_release: Some(format_release_tag(
                release_tag.parsed.major,
                release_tag.parsed.minor,
                release_tag.parsed.patch,
            )),
            first_tag_sort_key: Some(release_tag.parsed.tag_sort_key()),
            first_release_sort_key: Some(release_tag.parsed.release_sort_key()),
        };
        let mut stack = vec![release_tag.target_oid];
        while let Some(commit_oid) = stack.pop() {
            if cancel_flag.load(Ordering::Relaxed) {
                anyhow::bail!("mainline tag assignment cancelled");
            }
            if !covered.insert(commit_oid) {
                continue;
            }
            if commit_targets.contains(&commit_oid) {
                if tag_map.insert(commit_oid, tag_info.clone()).is_none() {
                    prep_progress.tagged_commits.fetch_add(1, Ordering::Relaxed);
                }
            }

            let commit = match repo.find_object(commit_oid)?.try_into_commit() {
                Ok(commit) => commit,
                Err(_) => {
                    warn!(
                        commit_oid = %commit_oid,
                        tag = %release_tag.name,
                        "skipping non-commit object encountered during release tag ancestry walk"
                    );
                    continue;
                }
            };
            stack.extend(commit.parent_ids().map(|parent| parent.detach()));
        }
        prep_progress
            .release_tags_processed
            .store((idx + 1) as u64, Ordering::Relaxed);
    }

    Ok(tag_map)
}

fn prep_progress_json(
    run: &nexus_db::MainlineScanRun,
    scan_parallelism: usize,
    chunk_size: usize,
    prep_progress: &MainlinePrepProgress,
) -> serde_json::Value {
    json!({
        "mode": run.mode,
        "ref_name": run.ref_name,
        "phase": "preparing",
        "prep_phase": prep_progress.phase_label(),
        "prep_walked_commits": prep_progress.walked_commits.load(Ordering::Relaxed),
        "prep_selected_commits": prep_progress.selected_commits.load(Ordering::Relaxed),
        "prep_release_tags_total": prep_progress.release_tags_total.load(Ordering::Relaxed),
        "prep_release_tags_processed": prep_progress.release_tags_processed.load(Ordering::Relaxed),
        "prep_tagged_commits": prep_progress.tagged_commits.load(Ordering::Relaxed),
        "scan_parallelism": scan_parallelism,
        "commit_window_size": chunk_size,
    })
}

async fn load_mainline_commit_batch(
    repo_path: PathBuf,
    commit_oids: Vec<ObjectId>,
    worker_count: usize,
) -> anyhow::Result<Vec<MainlineCommitRecord>> {
    if commit_oids.is_empty() {
        return Ok(Vec::new());
    }

    let partitions = partition_mainline_commit_oids(
        commit_oids.iter().map(ToString::to_string).collect(),
        worker_count.max(1),
    );
    let mut joinset = JoinSet::new();
    for partition in partitions {
        if partition.is_empty() {
            continue;
        }
        let worker_repo_path = repo_path.clone();
        joinset.spawn_blocking(move || load_mainline_commit_partition(worker_repo_path, partition));
    }

    let mut indexed_records = Vec::new();
    while let Some(next) = joinset.join_next().await {
        match next {
            Ok(Ok(records)) => indexed_records.extend(records),
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(anyhow::anyhow!("mainline parse task failed: {err}")),
        }
    }
    indexed_records.sort_by_key(|record| record.index);
    Ok(indexed_records
        .into_iter()
        .map(|record| record.record)
        .collect())
}

fn load_mainline_commit_partition(
    repo_path: PathBuf,
    partition: Vec<MainlineCommitPartitionItem>,
) -> anyhow::Result<Vec<IndexedMainlineCommitRecord>> {
    let repo = open_mainline_repo(&repo_path)?;
    let mut blob_cache =
        repo.diff_resource_cache(gix::diff::blob::pipeline::Mode::ToGit, Default::default())?;
    let mut records = Vec::with_capacity(partition.len());

    for indexed_commit in partition {
        let commit_oid = ObjectId::from_hex(indexed_commit.commit_oid.as_bytes())
            .with_context(|| format!("invalid commit oid {}", indexed_commit.commit_oid))?;
        let commit = repo
            .find_object(commit_oid)?
            .try_into_commit()
            .with_context(|| format!("object {commit_oid} was not a commit"))?;
        let body = commit.message_raw_sloppy().to_str_lossy().to_string();
        let subject = body.lines().next().unwrap_or_default().trim().to_string();
        let committed_at = DateTime::<Utc>::from_timestamp(commit.time()?.seconds, 0)
            .with_context(|| format!("invalid commit timestamp for {commit_oid}"))?;
        let patch_id_stable = compute_commit_patch_id(&repo, &commit, &mut blob_cache)
            .with_context(|| format!("failed to compute patch-id for {commit_oid}"))?;
        blob_cache.clear_resource_cache();

        records.push(IndexedMainlineCommitRecord {
            index: indexed_commit.index,
            record: MainlineCommitRecord {
                commit_oid,
                committed_at,
                subject,
                body,
                patch_id_stable,
            },
        });
    }

    Ok(records)
}

fn partition_mainline_commit_oids(
    commit_oids: Vec<String>,
    worker_count: usize,
) -> Vec<Vec<MainlineCommitPartitionItem>> {
    let lane_count = worker_count.max(1).min(commit_oids.len().max(1));
    let mut lanes: Vec<Vec<MainlineCommitPartitionItem>> =
        (0..lane_count).map(|_| Vec::new()).collect();

    for (idx, commit_oid) in commit_oids.into_iter().enumerate() {
        lanes[idx % lane_count].push(MainlineCommitPartitionItem {
            index: idx,
            commit_oid,
        });
    }

    lanes
}

fn compute_commit_patch_id(
    repo: &gix::Repository,
    commit: &gix::Commit<'_>,
    blob_cache: &mut gix::diff::blob::Platform,
) -> anyhow::Result<Option<String>> {
    let parent_tree = if let Some(parent_id) = commit.parent_ids().next() {
        parent_id
            .object()?
            .try_into_commit()
            .with_context(|| format!("parent {} was not a commit", parent_id))?
            .tree()?
    } else {
        repo.empty_tree()
    };
    let current_tree = commit.tree()?;
    let mut diff_text = String::new();

    let mut changes = parent_tree.changes()?;
    changes.track_path().track_rewrites(None);
    changes.for_each_to_obtain_tree(&current_tree, |change| {
        append_change_patch(change, blob_cache, &mut diff_text)?;
        Ok::<_, anyhow::Error>(gix::object::tree::diff::Action::Continue)
    })?;

    Ok(compute_patch_id_stable(&diff_text))
}

fn append_change_patch(
    change: gix::object::tree::diff::Change<'_, '_, '_>,
    blob_cache: &mut gix::diff::blob::Platform,
    diff_text: &mut String,
) -> anyhow::Result<()> {
    let path = change.location.to_str_lossy().to_string();
    if path.is_empty() {
        return Ok(());
    }

    let (old_path, new_path) = match change.event {
        gix::object::tree::diff::change::Event::Addition { entry_mode, .. }
            if entry_mode.is_blob() || entry_mode.is_link() =>
        {
            ("/dev/null".to_string(), format!("b/{path}"))
        }
        gix::object::tree::diff::change::Event::Deletion { entry_mode, .. }
            if entry_mode.is_blob() || entry_mode.is_link() =>
        {
            (format!("a/{path}"), "/dev/null".to_string())
        }
        gix::object::tree::diff::change::Event::Modification {
            previous_entry_mode,
            entry_mode,
            ..
        } if (previous_entry_mode.is_blob() || previous_entry_mode.is_link())
            && (entry_mode.is_blob() || entry_mode.is_link()) =>
        {
            (format!("a/{path}"), format!("b/{path}"))
        }
        gix::object::tree::diff::change::Event::Rewrite { .. } => {
            return Ok(());
        }
        _ => {
            return Ok(());
        }
    };

    let diff_platform = change.diff(blob_cache)?;
    let prep = diff_platform.resource_cache.prepare_diff()?;
    let gix::diff::blob::platform::prepare_diff::Operation::InternalDiff { .. } = prep.operation
    else {
        return Ok(());
    };

    let old_text =
        String::from_utf8_lossy(prep.old.data.as_slice().unwrap_or_default()).into_owned();
    let new_text =
        String::from_utf8_lossy(prep.new.data.as_slice().unwrap_or_default()).into_owned();
    let input = imara_diff::intern::InternedInput::new(old_text.as_str(), new_text.as_str());
    let body = build_unified_diff(
        DiffAlgorithm::Myers,
        &input,
        UnifiedDiffBuilder::new(&input),
    );
    if body.trim().is_empty() {
        return Ok(());
    }

    diff_text.push_str("--- ");
    diff_text.push_str(&old_path);
    diff_text.push('\n');
    diff_text.push_str("+++ ");
    diff_text.push_str(&new_path);
    diff_text.push('\n');
    diff_text.push_str(&body);
    if !body.ends_with('\n') {
        diff_text.push('\n');
    }
    Ok(())
}

fn extract_link_message_ids(body: &str) -> Vec<String> {
    static LORE_LINK_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(
            r"(?im)^\s*link:\s*https?://(?:[^/\s]+\.)?lore\.kernel\.org/(?:[^/\s]+/)?([^/?#\s]+)/?",
        )
        .expect("valid lore link regex")
    });
    static LKML_LINK_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?im)^\s*link:\s*https?://lkml\.kernel\.org/r/([^/?#\s]+)")
            .expect("valid lkml link regex")
    });

    let mut message_ids = BTreeSet::new();
    for captures in LORE_LINK_RE.captures_iter(body) {
        if let Some(message_id) = captures
            .get(1)
            .and_then(|value| normalize_message_id_token(&percent_decode(value.as_str())))
        {
            message_ids.insert(message_id);
        }
    }
    for captures in LKML_LINK_RE.captures_iter(body) {
        if let Some(message_id) = captures
            .get(1)
            .and_then(|value| normalize_message_id_token(&percent_decode(value.as_str())))
        {
            message_ids.insert(message_id);
        }
    }
    message_ids.into_iter().collect()
}

fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0usize;
    while idx < bytes.len() {
        if bytes[idx] == b'%'
            && idx + 2 < bytes.len()
            && let Ok(value) = u8::from_str_radix(&raw[idx + 1..idx + 3], 16)
        {
            out.push(value);
            idx += 3;
            continue;
        }
        out.push(bytes[idx]);
        idx += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn normalize_message_id_token(raw: &str) -> Option<String> {
    let mut value = raw.trim().trim_matches('"').trim_matches('\'').to_string();
    loop {
        let previous = value.clone();
        value = value
            .trim()
            .trim_matches('<')
            .trim_matches('>')
            .trim()
            .to_string();
        if value == previous {
            break;
        }
    }
    if value.is_empty() || !value.contains('@') {
        return None;
    }
    Some(value.to_ascii_lowercase())
}

fn normalize_commit_subject(subject: &str) -> String {
    let parsed = parse_patch_subject(subject);
    let normalized = parsed.subject_norm_base.trim();
    if normalized.is_empty() {
        subject
            .trim()
            .to_ascii_lowercase()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    } else {
        normalized.to_string()
    }
}

fn collect_link_matches(
    message_ids: &[String],
    by_message_id: &HashMap<String, Vec<nexus_db::MainlinePatchCandidate>>,
    subject_norm: &str,
) -> Vec<(nexus_db::MainlinePatchCandidate, String)> {
    let mut unique_subjects = BTreeSet::new();
    let mut candidates = Vec::new();

    for message_id in message_ids {
        for candidate in by_message_id.get(message_id).into_iter().flatten() {
            unique_subjects.insert(
                candidate
                    .commit_subject_norm
                    .clone()
                    .unwrap_or_else(|| candidate.subject_norm.clone()),
            );
            candidates.push((candidate.clone(), message_id.clone()));
        }
    }

    let mut deduped = BTreeMap::new();
    if unique_subjects.len() <= 1 {
        for (candidate, message_id) in candidates {
            deduped
                .entry(candidate.patch_item_id)
                .or_insert((candidate, message_id));
        }
        return deduped.into_values().collect();
    }

    for (candidate, message_id) in candidates {
        let candidate_subject = candidate
            .commit_subject_norm
            .as_deref()
            .unwrap_or(candidate.subject_norm.as_str());
        if candidate_subject == subject_norm {
            deduped
                .entry(candidate.patch_item_id)
                .or_insert((candidate, message_id));
        }
    }
    deduped.into_values().collect()
}

fn collect_patch_id_matches(
    candidates: &[nexus_db::MainlinePatchCandidate],
    subject_norm: &str,
) -> Vec<nexus_db::MainlinePatchCandidate> {
    if candidates.len() <= 1 {
        return candidates.to_vec();
    }

    let mut filtered = BTreeMap::new();
    for candidate in candidates {
        let candidate_subject = candidate
            .commit_subject_norm
            .as_deref()
            .unwrap_or(candidate.subject_norm.as_str());
        if candidate_subject == subject_norm {
            filtered
                .entry(candidate.patch_item_id)
                .or_insert_with(|| candidate.clone());
        }
    }
    filtered.into_values().collect()
}

fn filter_new_canonical_matches(
    matches: Vec<CommitMatch>,
    existing_canonical_ids: &HashSet<i64>,
    claimed_patch_item_ids: &HashSet<i64>,
) -> Vec<CommitMatch> {
    matches
        .into_iter()
        .filter(|matched| {
            !existing_canonical_ids.contains(&matched.patch_item_id)
                && !claimed_patch_item_ids.contains(&matched.patch_item_id)
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct ParsedKernelTag {
    major: i64,
    minor: i64,
    patch: i64,
    rc: Option<i64>,
}

impl ParsedKernelTag {
    fn release_sort_key(self) -> i64 {
        self.major * 1_000_000_000 + self.minor * 1_000_000 + self.patch * 1_000
    }

    fn tag_sort_key(self) -> i64 {
        self.release_sort_key() + self.rc.unwrap_or(999)
    }
}

fn parse_kernel_tag(tag: &str) -> Option<ParsedKernelTag> {
    static TAG_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"^v(?P<major>\d+)\.(?P<minor>\d+)(?:\.(?P<patch>\d+))?(?:-rc(?P<rc>\d+))?$")
            .expect("valid kernel tag regex")
    });

    let captures = TAG_RE.captures(tag.trim())?;
    Some(ParsedKernelTag {
        major: captures.name("major")?.as_str().parse().ok()?,
        minor: captures.name("minor")?.as_str().parse().ok()?,
        patch: captures
            .name("patch")
            .and_then(|value| value.as_str().parse().ok())
            .unwrap_or(0),
        rc: captures
            .name("rc")
            .and_then(|value| value.as_str().parse().ok()),
    })
}

fn format_release_tag(major: i64, minor: i64, patch: i64) -> String {
    if patch == 0 {
        format!("v{major}.{minor}")
    } else {
        format!("v{major}.{minor}.{patch}")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CommitMatch, MainlinePatchCandidate, collect_patch_id_matches, extract_link_message_ids,
        filter_new_canonical_matches, format_release_tag, parse_kernel_tag,
    };
    use std::collections::HashSet;

    #[test]
    fn extracts_and_normalizes_lore_message_ids() {
        let body = concat!(
            "Link: https://lore.kernel.org/all/%3Cabc%40example.com%3E/\n",
            "Link: https://lkml.kernel.org/r/def@example.com\n"
        );
        let ids = extract_link_message_ids(body);
        assert_eq!(ids, vec!["abc@example.com", "def@example.com"]);
    }

    #[test]
    fn parses_kernel_release_and_rc_tags() {
        let rc = parse_kernel_tag("v6.9-rc3").expect("rc tag");
        assert_eq!(rc.release_sort_key(), 6_009_000_000);
        assert_eq!(rc.tag_sort_key(), 6_009_000_003);

        let final_tag = parse_kernel_tag("v6.9").expect("final tag");
        assert_eq!(final_tag.release_sort_key(), 6_009_000_000);
        assert_eq!(final_tag.tag_sort_key(), 6_009_000_999);
    }

    #[test]
    fn patch_id_candidates_require_subject_match_when_ambiguous() {
        let matching = MainlinePatchCandidate {
            patch_item_id: 11,
            patch_series_id: 21,
            patch_series_version_id: 31,
            message_id_primary: "one@example.com".to_string(),
            patch_id_stable: Some("patchid".to_string()),
            commit_subject_norm: Some("mm: reclaim cleanups".to_string()),
            subject_norm: "ignored".to_string(),
        };
        let unrelated = MainlinePatchCandidate {
            patch_item_id: 12,
            patch_series_id: 22,
            patch_series_version_id: 32,
            message_id_primary: "two@example.com".to_string(),
            patch_id_stable: Some("patchid".to_string()),
            commit_subject_norm: Some("net: different patch".to_string()),
            subject_norm: "ignored".to_string(),
        };

        let resolved =
            collect_patch_id_matches(&[matching.clone(), unrelated], "mm: reclaim cleanups");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].patch_item_id, matching.patch_item_id);
    }

    #[test]
    fn formats_kernel_release_without_zero_patch() {
        assert_eq!(format_release_tag(6, 9, 0), "v6.9");
        assert_eq!(format_release_tag(6, 9, 3), "v6.9.3");
    }

    #[test]
    fn skips_patch_items_already_claimed_in_same_batch() {
        let existing = HashSet::new();
        let claimed = HashSet::from([11_i64]);
        let matches = vec![
            CommitMatch {
                patch_item_id: 11,
                patch_series_id: 21,
                match_method: "link".to_string(),
                matched_message_id: Some("one@example.com".to_string()),
            },
            CommitMatch {
                patch_item_id: 12,
                patch_series_id: 22,
                match_method: "patch_id".to_string(),
                matched_message_id: None,
            },
        ];

        let filtered = filter_new_canonical_matches(matches, &existing, &claimed);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].patch_item_id, 12);
    }
}
