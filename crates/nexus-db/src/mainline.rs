use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, QueryBuilder};

use crate::{MainlineScanRun, MainlineScanState, Result};

#[derive(Clone)]
pub struct MainlineStore {
    pool: PgPool,
}

#[derive(Debug, Clone, Default)]
pub struct ListMainlineScanRunsParams {
    pub state: Option<String>,
    pub limit: i64,
    pub cursor: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct CreateMainlineScanRunParams<'a> {
    pub repo_key: &'a str,
    pub mode: &'a str,
    pub ref_name: &'a str,
    pub head_commit_oid: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct UpsertMainlineCommitInput {
    pub commit_oid: String,
    pub subject: String,
    pub committed_at: DateTime<Utc>,
    pub first_containing_tag: Option<String>,
    pub first_final_release: Option<String>,
    pub first_tag_sort_key: Option<i64>,
    pub first_release_sort_key: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct CanonicalPatchMatchInput {
    pub patch_item_id: i64,
    pub commit_oid: String,
    pub match_method: String,
    pub matched_message_id: Option<String>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MainlinePatchCandidate {
    pub patch_item_id: i64,
    pub patch_series_id: i64,
    pub patch_series_version_id: i64,
    pub message_id_primary: String,
    pub patch_id_stable: Option<String>,
    pub commit_subject_norm: Option<String>,
    pub subject_norm: String,
}

#[derive(Debug, Clone)]
pub struct VersionMergeSummary {
    pub version_id: i64,
    pub patch_series_id: i64,
    pub state: String,
    pub matched_patch_count: i32,
    pub total_patch_count: i32,
    pub merged_in_tag: Option<String>,
    pub merged_in_release: Option<String>,
    pub single_patch_commit_oid: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SeriesMergeSummary {
    pub patch_series_id: i64,
    pub state: String,
    pub matched_patch_count: i32,
    pub total_patch_count: i32,
    pub merged_in_tag: Option<String>,
    pub merged_in_release: Option<String>,
    pub single_patch_commit_oid: Option<String>,
    pub merged_version_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct VersionSortKey {
    version_num: i32,
    sent_at: DateTime<Utc>,
    version_id: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct VersionRollupRow {
    patch_series_id: i64,
    version_id: i64,
    version_num: i32,
    sent_at: DateTime<Utc>,
    item_type: Option<String>,
    commit_oid: Option<String>,
    first_containing_tag: Option<String>,
    first_final_release: Option<String>,
    first_tag_sort_key: Option<i64>,
    first_release_sort_key: Option<i64>,
}

#[derive(Debug, Clone, Default)]
struct VersionAccumulator {
    patch_series_id: i64,
    version_num: i32,
    sent_at: Option<DateTime<Utc>>,
    total_patch_count: i32,
    matched_patch_count: i32,
    best_tag_sort_key: Option<i64>,
    best_release_sort_key: Option<i64>,
    merged_in_tag: Option<String>,
    merged_in_release: Option<String>,
    matched_commit_oids: Vec<String>,
}

impl MainlineStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn ensure_state(&self, repo_key: &str, ref_name: &str) -> Result<MainlineScanState> {
        sqlx::query_as::<_, MainlineScanState>(
            r#"INSERT INTO mainline_scan_state (repo_key, ref_name)
               VALUES ($1, $2)
               ON CONFLICT (repo_key) DO UPDATE
                   SET ref_name = EXCLUDED.ref_name
               RETURNING *"#,
        )
        .bind(repo_key)
        .bind(ref_name)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn get_state(&self, repo_key: &str) -> Result<Option<MainlineScanState>> {
        sqlx::query_as::<_, MainlineScanState>(
            "SELECT * FROM mainline_scan_state WHERE repo_key = $1 LIMIT 1",
        )
        .bind(repo_key)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn set_public_filter_ready(&self, repo_key: &str, ready: bool) -> Result<()> {
        sqlx::query(
            r#"UPDATE mainline_scan_state
               SET public_filter_ready = $2,
                   updated_at = now()
               WHERE repo_key = $1"#,
        )
        .bind(repo_key)
        .bind(ready)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_state_failed(&self, repo_key: &str, last_error: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE mainline_scan_state
               SET last_error = $2,
                   updated_at = now()
               WHERE repo_key = $1"#,
        )
        .bind(repo_key)
        .bind(last_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_state_succeeded(
        &self,
        repo_key: &str,
        head_commit_oid: &str,
        last_scanned_commit_oid: Option<&str>,
        bootstrap_completed: bool,
        public_filter_ready: bool,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE mainline_scan_state
               SET last_scanned_head_oid = $2,
                   last_scanned_commit_oid = COALESCE($3, last_scanned_commit_oid),
                   bootstrap_completed_at = CASE
                       WHEN $4 THEN COALESCE(bootstrap_completed_at, now())
                       ELSE bootstrap_completed_at
                   END,
                   public_filter_ready = $5,
                   last_successful_scan_at = now(),
                   last_error = NULL,
                   updated_at = now()
               WHERE repo_key = $1"#,
        )
        .bind(repo_key)
        .bind(head_commit_oid)
        .bind(last_scanned_commit_oid)
        .bind(bootstrap_completed)
        .bind(public_filter_ready)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn create_run(
        &self,
        params: CreateMainlineScanRunParams<'_>,
    ) -> Result<MainlineScanRun> {
        sqlx::query_as::<_, MainlineScanRun>(
            r#"INSERT INTO mainline_scan_runs
               (repo_key, mode, state, ref_name, head_commit_oid, progress_json)
               VALUES ($1, $2, 'queued', $3, $4, '{}'::jsonb)
               RETURNING *"#,
        )
        .bind(params.repo_key)
        .bind(params.mode)
        .bind(params.ref_name)
        .bind(params.head_commit_oid)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn set_run_job_id(&self, run_id: i64, job_id: i64) -> Result<()> {
        sqlx::query(
            r#"UPDATE mainline_scan_runs
               SET job_id = $2,
                   updated_at = now()
               WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(job_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_run(&self, run_id: i64) -> Result<Option<MainlineScanRun>> {
        sqlx::query_as::<_, MainlineScanRun>("SELECT * FROM mainline_scan_runs WHERE id = $1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await
    }

    pub async fn list_runs(
        &self,
        params: ListMainlineScanRunsParams,
    ) -> Result<Vec<MainlineScanRun>> {
        let mut qb = QueryBuilder::new("SELECT * FROM mainline_scan_runs WHERE true");
        if let Some(state) = params.state {
            qb.push(" AND state = ").push_bind(state);
        }
        if let Some(cursor) = params.cursor {
            qb.push(" AND id < ").push_bind(cursor);
        }
        qb.push(" ORDER BY id DESC LIMIT ")
            .push_bind(params.limit.clamp(1, 200));
        qb.build_query_as::<MainlineScanRun>()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_active_run(&self, repo_key: &str) -> Result<Option<MainlineScanRun>> {
        sqlx::query_as::<_, MainlineScanRun>(
            r#"SELECT *
               FROM mainline_scan_runs
               WHERE repo_key = $1
                 AND state IN ('queued', 'running')
               ORDER BY id DESC
               LIMIT 1"#,
        )
        .bind(repo_key)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn mark_run_running(&self, run_id: i64) -> Result<()> {
        sqlx::query(
            r#"UPDATE mainline_scan_runs
               SET state = 'running',
                   started_at = COALESCE(started_at, now()),
                   updated_at = now()
               WHERE id = $1"#,
        )
        .bind(run_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_run_progress(
        &self,
        run_id: i64,
        cursor_commit_oid: Option<&str>,
        scanned_commits: i64,
        matched_commits: i64,
        matched_patch_items: i64,
        updated_series: i64,
        progress_json: serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE mainline_scan_runs
               SET cursor_commit_oid = $2,
                   scanned_commits = $3,
                   matched_commits = $4,
                   matched_patch_items = $5,
                   updated_series = $6,
                   progress_json = $7,
                   updated_at = now()
               WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(cursor_commit_oid)
        .bind(scanned_commits)
        .bind(matched_commits)
        .bind(matched_patch_items)
        .bind(updated_series)
        .bind(progress_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_run_succeeded(
        &self,
        run_id: i64,
        result_json: serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE mainline_scan_runs
               SET state = 'succeeded',
                   completed_at = now(),
                   result_json = $2,
                   last_error = NULL,
                   updated_at = now()
               WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(result_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_run_failed(&self, run_id: i64, error: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE mainline_scan_runs
               SET state = 'failed',
                   completed_at = now(),
                   last_error = $2,
                   updated_at = now()
               WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_run_cancelled(&self, run_id: i64, reason: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE mainline_scan_runs
               SET state = 'cancelled',
                   completed_at = now(),
                   last_error = $2,
                   updated_at = now()
               WHERE id = $1"#,
        )
        .bind(run_id)
        .bind(reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn find_patch_candidates_by_message_ids(
        &self,
        message_ids: &[String],
    ) -> Result<Vec<MainlinePatchCandidate>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, MainlinePatchCandidate>(
            r#"SELECT
                   pi.id AS patch_item_id,
                   psv.patch_series_id,
                   pi.patch_series_version_id,
                   m.message_id_primary,
                   pi.patch_id_stable,
                   pi.commit_subject_norm,
                   pi.subject_norm
               FROM message_id_map mim
               JOIN patch_items pi
                 ON pi.message_pk = mim.message_pk
               JOIN patch_series_versions psv
                 ON psv.id = pi.patch_series_version_id
               JOIN messages m
                 ON m.id = pi.message_pk
               WHERE mim.message_id = ANY($1)
                 AND pi.item_type = 'patch'
               ORDER BY pi.id ASC"#,
        )
        .bind(message_ids)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn find_patch_candidates_by_patch_ids(
        &self,
        patch_ids: &[String],
    ) -> Result<Vec<MainlinePatchCandidate>> {
        if patch_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, MainlinePatchCandidate>(
            r#"SELECT
                   pi.id AS patch_item_id,
                   psv.patch_series_id,
                   pi.patch_series_version_id,
                   m.message_id_primary,
                   pi.patch_id_stable,
                   pi.commit_subject_norm,
                   pi.subject_norm
               FROM patch_items pi
               JOIN patch_series_versions psv
                 ON psv.id = pi.patch_series_version_id
               JOIN messages m
                 ON m.id = pi.message_pk
               WHERE pi.patch_id_stable = ANY($1)
                 AND pi.item_type = 'patch'
               ORDER BY pi.id ASC"#,
        )
        .bind(patch_ids)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_existing_canonical_patch_item_ids(
        &self,
        patch_item_ids: &[i64],
    ) -> Result<Vec<i64>> {
        if patch_item_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_scalar::<_, i64>(
            r#"SELECT patch_item_id
               FROM patch_item_mainline_matches
               WHERE patch_item_id = ANY($1)
                 AND is_canonical = true
               ORDER BY patch_item_id ASC"#,
        )
        .bind(patch_item_ids)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn upsert_mainline_commits(
        &self,
        commits: &[UpsertMainlineCommitInput],
    ) -> Result<()> {
        if commits.is_empty() {
            return Ok(());
        }

        let mut qb = QueryBuilder::new(
            "INSERT INTO mainline_commits \
             (commit_oid, subject, committed_at, first_containing_tag, first_final_release, first_tag_sort_key, first_release_sort_key) ",
        );
        qb.push_values(commits, |mut row, commit| {
            row.push_bind(&commit.commit_oid)
                .push_bind(&commit.subject)
                .push_bind(commit.committed_at)
                .push_bind(&commit.first_containing_tag)
                .push_bind(&commit.first_final_release)
                .push_bind(commit.first_tag_sort_key)
                .push_bind(commit.first_release_sort_key);
        });
        qb.push(
            " ON CONFLICT (commit_oid) DO UPDATE SET \
                subject = EXCLUDED.subject, \
                committed_at = EXCLUDED.committed_at, \
                first_containing_tag = EXCLUDED.first_containing_tag, \
                first_final_release = EXCLUDED.first_final_release, \
                first_tag_sort_key = EXCLUDED.first_tag_sort_key, \
                first_release_sort_key = EXCLUDED.first_release_sort_key",
        );
        qb.build().execute(&self.pool).await?;
        Ok(())
    }

    pub async fn replace_canonical_matches(
        &self,
        matches: &[CanonicalPatchMatchInput],
    ) -> Result<()> {
        if matches.is_empty() {
            return Ok(());
        }

        let patch_item_ids = matches.iter().map(|item| item.patch_item_id).collect::<Vec<_>>();
        sqlx::query(
            "UPDATE patch_item_mainline_matches SET is_canonical = false WHERE patch_item_id = ANY($1)",
        )
        .bind(&patch_item_ids)
        .execute(&self.pool)
        .await?;

        let mut qb = QueryBuilder::new(
            "INSERT INTO patch_item_mainline_matches \
             (patch_item_id, commit_oid, match_method, matched_message_id, is_canonical) ",
        );
        qb.push_values(matches, |mut row, item| {
            row.push_bind(item.patch_item_id)
                .push_bind(&item.commit_oid)
                .push_bind(&item.match_method)
                .push_bind(&item.matched_message_id)
                .push_bind(true);
        });
        qb.push(
            " ON CONFLICT (patch_item_id, commit_oid) DO UPDATE SET \
                match_method = EXCLUDED.match_method, \
                matched_message_id = EXCLUDED.matched_message_id, \
                is_canonical = EXCLUDED.is_canonical",
        );
        qb.build().execute(&self.pool).await?;
        Ok(())
    }

    pub async fn list_series_ids_after(&self, cursor: i64, limit: i64) -> Result<Vec<i64>> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT id
               FROM patch_series
               WHERE id > $1
               ORDER BY id ASC
               LIMIT $2"#,
        )
        .bind(cursor.max(0))
        .bind(limit.clamp(1, 5_000))
        .fetch_all(&self.pool)
        .await
    }

    pub async fn recompute_rollups(
        &self,
        series_ids: &[i64],
    ) -> Result<Vec<SeriesMergeSummary>> {
        if series_ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query_as::<_, VersionRollupRow>(
            r#"SELECT
                   psv.patch_series_id,
                   psv.id AS version_id,
                   psv.version_num,
                   psv.sent_at,
                   pi.item_type,
                   pim.commit_oid,
                   mc.first_containing_tag,
                   mc.first_final_release,
                   mc.first_tag_sort_key,
                   mc.first_release_sort_key
               FROM patch_series_versions psv
               LEFT JOIN patch_series_version_assembled_items psvai
                 ON psvai.patch_series_version_id = psv.id
               LEFT JOIN patch_items pi
                 ON pi.id = psvai.patch_item_id
               LEFT JOIN patch_item_mainline_matches pim
                 ON pim.patch_item_id = pi.id
                AND pim.is_canonical = true
               LEFT JOIN mainline_commits mc
                 ON mc.commit_oid = pim.commit_oid
               WHERE psv.patch_series_id = ANY($1)
               ORDER BY
                   psv.patch_series_id ASC,
                   psv.version_num DESC,
                   psv.sent_at DESC,
                   psv.id DESC,
                   psvai.ordinal ASC"#,
        )
        .bind(series_ids)
        .fetch_all(&self.pool)
        .await?;

        let mut version_accumulators: BTreeMap<i64, VersionAccumulator> = BTreeMap::new();
        for row in rows {
            let entry = version_accumulators
                .entry(row.version_id)
                .or_insert_with(|| VersionAccumulator {
                    patch_series_id: row.patch_series_id,
                    version_num: row.version_num,
                    sent_at: Some(row.sent_at),
                    ..VersionAccumulator::default()
                });
            entry.sent_at = Some(row.sent_at);
            if row.item_type.as_deref() != Some("patch") {
                continue;
            }
            entry.total_patch_count += 1;
            if let Some(commit_oid) = row.commit_oid {
                entry.matched_patch_count += 1;
                entry.matched_commit_oids.push(commit_oid);
                if let Some(sort_key) = row.first_tag_sort_key
                    && entry.best_tag_sort_key.is_none_or(|existing| sort_key > existing)
                {
                    entry.best_tag_sort_key = Some(sort_key);
                    entry.merged_in_tag = row.first_containing_tag.clone();
                }
                if let Some(sort_key) = row.first_release_sort_key
                    && entry
                        .best_release_sort_key
                        .is_none_or(|existing| sort_key > existing)
                {
                    entry.best_release_sort_key = Some(sort_key);
                    entry.merged_in_release = row.first_final_release.clone();
                }
            }
        }

        let mut version_summaries = Vec::with_capacity(version_accumulators.len());
        for (version_id, accumulator) in &version_accumulators {
            let state = if accumulator.total_patch_count == 0 || accumulator.matched_patch_count == 0 {
                "unmerged"
            } else if accumulator.matched_patch_count >= accumulator.total_patch_count {
                "merged"
            } else {
                "partial"
            };
            let single_patch_commit_oid = if accumulator.total_patch_count == 1
                && accumulator.matched_patch_count == 1
            {
                accumulator.matched_commit_oids.first().cloned()
            } else {
                None
            };
            version_summaries.push(VersionMergeSummary {
                version_id: *version_id,
                patch_series_id: accumulator.patch_series_id,
                state: state.to_string(),
                matched_patch_count: accumulator.matched_patch_count,
                total_patch_count: accumulator.total_patch_count,
                merged_in_tag: if state == "merged" {
                    accumulator.merged_in_tag.clone()
                } else {
                    None
                },
                merged_in_release: if state == "merged" {
                    accumulator.merged_in_release.clone()
                } else {
                    None
                },
                single_patch_commit_oid: if state == "merged" {
                    single_patch_commit_oid
                } else {
                    None
                },
            });
        }

        self.update_version_rollups(&version_summaries).await?;

        let mut by_series: BTreeMap<i64, Vec<&VersionMergeSummary>> = BTreeMap::new();
        let mut sort_keys: BTreeMap<i64, VersionSortKey> = BTreeMap::new();
        for (version_id, accumulator) in &version_accumulators {
            if let Some(sent_at) = accumulator.sent_at {
                sort_keys.insert(
                    *version_id,
                    VersionSortKey {
                        version_num: accumulator.version_num,
                        sent_at,
                        version_id: *version_id,
                    },
                );
            }
        }
        for summary in &version_summaries {
            by_series
                .entry(summary.patch_series_id)
                .or_default()
                .push(summary);
        }

        let mut series_summaries = Vec::with_capacity(by_series.len());
        for (patch_series_id, summaries) in by_series {
            let mut merged = summaries
                .iter()
                .filter(|summary| summary.state == "merged")
                .cloned()
                .collect::<Vec<_>>();
            merged.sort_by_key(|summary| sort_keys.get(&summary.version_id).copied());
            let series_summary = if let Some(selected) = merged.last() {
                SeriesMergeSummary {
                    patch_series_id,
                    state: "merged".to_string(),
                    matched_patch_count: selected.matched_patch_count,
                    total_patch_count: selected.total_patch_count,
                    merged_in_tag: selected.merged_in_tag.clone(),
                    merged_in_release: selected.merged_in_release.clone(),
                    single_patch_commit_oid: selected.single_patch_commit_oid.clone(),
                    merged_version_id: Some(selected.version_id),
                }
            } else {
                let mut partial = summaries
                    .iter()
                    .filter(|summary| summary.state == "partial")
                    .cloned()
                    .collect::<Vec<_>>();
                partial.sort_by(|left, right| {
                    left.matched_patch_count
                        .cmp(&right.matched_patch_count)
                        .then(left.total_patch_count.cmp(&right.total_patch_count))
                        .then(
                            sort_keys
                                .get(&left.version_id)
                                .copied()
                                .cmp(&sort_keys.get(&right.version_id).copied()),
                        )
                });
                if let Some(selected) = partial.last() {
                    SeriesMergeSummary {
                        patch_series_id,
                        state: "partial".to_string(),
                        matched_patch_count: selected.matched_patch_count,
                        total_patch_count: selected.total_patch_count,
                        merged_in_tag: None,
                        merged_in_release: None,
                        single_patch_commit_oid: None,
                        merged_version_id: None,
                    }
                } else {
                    let selected = summaries
                        .iter()
                        .max_by_key(|summary| sort_keys.get(&summary.version_id).copied())
                        .cloned();
                    SeriesMergeSummary {
                        patch_series_id,
                        state: "unmerged".to_string(),
                        matched_patch_count: selected.map(|summary| summary.matched_patch_count).unwrap_or(0),
                        total_patch_count: selected.map(|summary| summary.total_patch_count).unwrap_or(0),
                        merged_in_tag: None,
                        merged_in_release: None,
                        single_patch_commit_oid: None,
                        merged_version_id: None,
                    }
                }
            };
            series_summaries.push(series_summary);
        }

        self.update_series_rollups(&series_summaries).await?;
        Ok(series_summaries)
    }

    async fn update_version_rollups(&self, rows: &[VersionMergeSummary]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut qb = QueryBuilder::new(
            "UPDATE patch_series_versions AS psv \
             SET mainline_merge_state = vals.state, \
                 mainline_matched_patch_count = vals.matched_patch_count, \
                 mainline_total_patch_count = vals.total_patch_count, \
                 mainline_merged_in_tag = vals.merged_in_tag, \
                 mainline_merged_in_release = vals.merged_in_release, \
                 mainline_single_patch_commit_oid = vals.single_patch_commit_oid \
             FROM (",
        );
        qb.push_values(rows, |mut row, summary| {
            row.push_bind(summary.version_id)
                .push_bind(&summary.state)
                .push_bind(summary.matched_patch_count)
                .push_bind(summary.total_patch_count)
                .push_bind(&summary.merged_in_tag)
                .push_bind(&summary.merged_in_release)
                .push_bind(&summary.single_patch_commit_oid);
        });
        qb.push(
            ") AS vals(version_id, state, matched_patch_count, total_patch_count, merged_in_tag, merged_in_release, single_patch_commit_oid) \
             WHERE psv.id = vals.version_id",
        );
        qb.build().execute(&self.pool).await?;
        Ok(())
    }

    async fn update_series_rollups(&self, rows: &[SeriesMergeSummary]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut qb = QueryBuilder::new(
            "UPDATE patch_series AS ps \
             SET mainline_merge_state = vals.state, \
                 mainline_matched_patch_count = vals.matched_patch_count, \
                 mainline_total_patch_count = vals.total_patch_count, \
                 mainline_merged_in_tag = vals.merged_in_tag, \
                 mainline_merged_in_release = vals.merged_in_release, \
                 mainline_single_patch_commit_oid = vals.single_patch_commit_oid, \
                 mainline_merged_version_id = vals.merged_version_id \
             FROM (",
        );
        qb.push_values(rows, |mut row, summary| {
            row.push_bind(summary.patch_series_id)
                .push_bind(&summary.state)
                .push_bind(summary.matched_patch_count)
                .push_bind(summary.total_patch_count)
                .push_bind(&summary.merged_in_tag)
                .push_bind(&summary.merged_in_release)
                .push_bind(&summary.single_patch_commit_oid)
                .push_bind(summary.merged_version_id);
        });
        qb.push(
            ") AS vals(patch_series_id, state, matched_patch_count, total_patch_count, merged_in_tag, merged_in_release, single_patch_commit_oid, merged_version_id) \
             WHERE ps.id = vals.patch_series_id",
        );
        qb.build().execute(&self.pool).await?;
        Ok(())
    }
}
