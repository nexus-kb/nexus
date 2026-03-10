use super::*;

impl LineageStore {
    pub async fn get_series_version(
        &self,
        patch_series_id: i64,
        series_version_id: i64,
    ) -> Result<Option<PatchSeriesVersionRecord>> {
        sqlx::query_as::<_, PatchSeriesVersionRecord>(
            r#"SELECT
                id,
                patch_series_id,
                version_num,
                is_rfc,
                is_resend,
                is_partial_reroll,
                thread_mailing_list_id,
                thread_id,
                cover_message_pk,
                first_patch_message_pk,
                sent_at,
                subject_raw,
                subject_norm,
                base_commit,
                version_fingerprint,
                mainline_merge_state,
                mainline_matched_patch_count,
                mainline_total_patch_count,
                mainline_merged_in_tag,
                mainline_merged_in_release,
                mainline_single_patch_commit_oid
            FROM patch_series_versions
            WHERE patch_series_id = $1
              AND id = $2
            LIMIT 1"#,
        )
        .bind(patch_series_id)
        .bind(series_version_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn list_series(
        &self,
        list_key: Option<&str>,
        merged: Option<bool>,
        sort: &str,
        limit: i64,
        cursor_ts: Option<DateTime<Utc>>,
        cursor_id: Option<i64>,
    ) -> Result<Vec<SeriesListItemRecord>> {
        let descending = sort != "last_seen_asc";
        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT
                ps.id AS series_id,
                ps.canonical_subject_norm,
                ps.author_email,
                ps.author_name,
                ps.created_at AS first_seen_at,
                COALESCE(latest.sent_at, ps.last_seen_at) AS latest_patchset_at,
                ps.last_seen_at,
                COALESCE(latest.version_num, 1) AS latest_version_num,
                COALESCE(latest.is_rfc, false) AS is_rfc_latest,
                ps.mainline_merge_state,
                ps.mainline_matched_patch_count,
                ps.mainline_total_patch_count,
                ps.mainline_merged_in_tag,
                ps.mainline_merged_in_release,
                ps.mainline_merged_version_id,
                ps.mainline_single_patch_commit_oid
            FROM patch_series ps
            LEFT JOIN LATERAL (
                SELECT psv.version_num, psv.is_rfc, psv.sent_at
                FROM patch_series_versions psv
                WHERE psv.patch_series_id = ps.id
                ORDER BY psv.version_num DESC, psv.sent_at DESC, psv.id DESC
                LIMIT 1
            ) latest ON true"#,
        );

        if list_key.is_some() {
            qb.push(
                r#" JOIN patch_series_lists psl
                     ON psl.patch_series_id = ps.id
                    JOIN mailing_lists ml
                     ON ml.id = psl.mailing_list_id"#,
            );
        }

        qb.push(" WHERE 1=1");
        if let Some(list_key) = list_key {
            qb.push(" AND ml.list_key = ");
            qb.push_bind(list_key);
        }
        if let Some(merged) = merged {
            if merged {
                qb.push(" AND ps.mainline_merge_state = 'merged'");
            } else {
                qb.push(" AND ps.mainline_merge_state <> 'merged'");
            }
        }

        if let (Some(cursor_ts), Some(cursor_id)) = (cursor_ts, cursor_id) {
            if descending {
                qb.push(" AND (ps.last_seen_at, ps.id) < (");
            } else {
                qb.push(" AND (ps.last_seen_at, ps.id) > (");
            }
            qb.push_bind(cursor_ts);
            qb.push(", ");
            qb.push_bind(cursor_id);
            qb.push(")");
        }

        qb.push(" ORDER BY ps.last_seen_at ");
        if descending {
            qb.push("DESC, ps.id DESC LIMIT ");
        } else {
            qb.push("ASC, ps.id ASC LIMIT ");
        }
        qb.push_bind(limit.clamp(1, 500));

        qb.build_query_as::<SeriesListItemRecord>()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn count_series(&self, list_key: Option<&str>) -> Result<i64> {
        let mut qb: QueryBuilder<'_, sqlx::Postgres> =
            QueryBuilder::new("SELECT COUNT(*)::bigint FROM patch_series ps");

        if let Some(list_key) = list_key {
            qb.push(
                r#" JOIN patch_series_lists psl
                     ON psl.patch_series_id = ps.id
                    JOIN mailing_lists ml
                     ON ml.id = psl.mailing_list_id
                    WHERE ml.list_key = "#,
            );
            qb.push_bind(list_key);
        }

        qb.build_query_scalar::<i64>().fetch_one(&self.pool).await
    }

    pub async fn list_series_list_keys(&self, patch_series_id: i64) -> Result<Vec<String>> {
        sqlx::query_scalar::<_, String>(
            r#"SELECT ml.list_key
            FROM patch_series_lists psl
            JOIN mailing_lists ml
              ON ml.id = psl.mailing_list_id
            WHERE psl.patch_series_id = $1
            ORDER BY ml.list_key ASC"#,
        )
        .bind(patch_series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_series_versions_with_counts(
        &self,
        patch_series_id: i64,
    ) -> Result<Vec<SeriesVersionSummaryRecord>> {
        sqlx::query_as::<_, SeriesVersionSummaryRecord>(
            r#"SELECT
                psv.id,
                psv.patch_series_id,
                psv.version_num,
                psv.is_rfc,
                psv.is_resend,
                psv.is_partial_reroll,
                psv.cover_message_pk,
                psv.first_patch_message_pk,
                psv.sent_at,
                psv.subject_raw,
                psv.subject_norm,
                psv.base_commit,
                COUNT(*) FILTER (WHERE pi.item_type = 'patch') AS patch_count,
                psv.mainline_merge_state,
                psv.mainline_matched_patch_count,
                psv.mainline_total_patch_count,
                psv.mainline_merged_in_tag,
                psv.mainline_merged_in_release,
                psv.mainline_single_patch_commit_oid
            FROM patch_series_versions psv
            LEFT JOIN patch_items pi
              ON pi.patch_series_version_id = psv.id
            WHERE psv.patch_series_id = $1
            GROUP BY
                psv.id,
                psv.patch_series_id,
                psv.version_num,
                psv.is_rfc,
                psv.is_resend,
                psv.is_partial_reroll,
                psv.cover_message_pk,
                psv.first_patch_message_pk,
                psv.sent_at,
                psv.subject_raw,
                psv.subject_norm,
                psv.base_commit,
                psv.mainline_merge_state,
                psv.mainline_matched_patch_count,
                psv.mainline_total_patch_count,
                psv.mainline_merged_in_tag,
                psv.mainline_merged_in_release,
                psv.mainline_single_patch_commit_oid
            ORDER BY psv.version_num ASC, psv.sent_at ASC, psv.id ASC"#,
        )
        .bind(patch_series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_thread_refs_for_series_versions(
        &self,
        series_version_ids: &[i64],
    ) -> Result<Vec<ThreadRefRecord>> {
        if series_version_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, ThreadRefRecord>(
            r#"SELECT
                psvt.patch_series_version_id,
                t.id AS thread_id,
                ml.list_key,
                t.message_count::bigint AS message_count,
                t.last_activity_at
            FROM patch_series_version_threads psvt
            JOIN threads t
              ON t.mailing_list_id = psvt.mailing_list_id
             AND t.id = psvt.thread_id
            JOIN mailing_lists ml
              ON ml.id = t.mailing_list_id
            WHERE psvt.patch_series_version_id = ANY($1)
            ORDER BY psvt.patch_series_version_id ASC, ml.list_key ASC, t.id ASC"#,
        )
        .bind(series_version_ids)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_series_version_thread_ref_backfill_candidates(
        &self,
        mailing_list_id: i64,
        from_seen_at: Option<DateTime<Utc>>,
        to_seen_at: Option<DateTime<Utc>>,
        after_series_version_id: i64,
        limit: i64,
    ) -> Result<Vec<SeriesVersionThreadRefBackfillCandidate>> {
        sqlx::query_as::<_, SeriesVersionThreadRefBackfillCandidate>(
            r#"SELECT
                id AS patch_series_version_id,
                COALESCE(cover_message_pk, first_patch_message_pk) AS anchor_message_pk
            FROM patch_series_versions
            WHERE id > $1
              AND (
                  COALESCE(cover_message_pk, first_patch_message_pk) IS NULL
                  OR EXISTS (
                      SELECT 1
                      FROM list_message_instances lmi
                      WHERE lmi.mailing_list_id = $2
                        AND lmi.message_pk = COALESCE(cover_message_pk, first_patch_message_pk)
                        AND ($3::timestamptz IS NULL OR lmi.seen_at >= $3)
                        AND ($4::timestamptz IS NULL OR lmi.seen_at < $4)
                  )
              )
            ORDER BY id ASC
            LIMIT $5"#,
        )
        .bind(after_series_version_id)
        .bind(mailing_list_id)
        .bind(from_seen_at)
        .bind(to_seen_at)
        .bind(limit.clamp(1, 5_000))
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_message_pks_present_in_list(
        &self,
        mailing_list_id: i64,
        message_pks: &[i64],
        from_seen_at: Option<DateTime<Utc>>,
        to_seen_at: Option<DateTime<Utc>>,
    ) -> Result<Vec<i64>> {
        if message_pks.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT lmi.message_pk
            FROM list_message_instances lmi
            WHERE lmi.mailing_list_id = $1
              AND lmi.message_pk = ANY($2)
              AND ($3::timestamptz IS NULL OR lmi.seen_at >= $3)
              AND ($4::timestamptz IS NULL OR lmi.seen_at < $4)
            ORDER BY lmi.message_pk ASC"#,
        )
        .bind(mailing_list_id)
        .bind(message_pks)
        .bind(from_seen_at)
        .bind(to_seen_at)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_thread_matches_for_messages(
        &self,
        mailing_list_id: i64,
        message_pks: &[i64],
    ) -> Result<Vec<MessageThreadMatchRecord>> {
        if message_pks.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, MessageThreadMatchRecord>(
            r#"SELECT
                tm.message_pk,
                COUNT(DISTINCT tm.thread_id)::bigint AS thread_match_count,
                MIN(tm.thread_id) AS thread_id
            FROM thread_messages tm
            WHERE tm.mailing_list_id = $1
              AND tm.message_pk = ANY($2)
            GROUP BY tm.message_pk
            ORDER BY tm.message_pk ASC"#,
        )
        .bind(mailing_list_id)
        .bind(message_pks)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_series_version_patch_items(
        &self,
        patch_series_version_id: i64,
        assembled: bool,
    ) -> Result<Vec<SeriesVersionPatchItemRecord>> {
        if assembled {
            return sqlx::query_as::<_, SeriesVersionPatchItemRecord>(
                r#"SELECT
                    pi.id AS patch_item_id,
                    psvai.ordinal,
                    pi.total,
                    pi.message_pk,
                    m.message_id_primary,
                    pi.subject_raw,
                    pi.subject_norm,
                    pi.commit_subject,
                    pi.commit_subject_norm,
                    pi.item_type,
                    pi.has_diff,
                    pi.patch_id_stable,
                    pi.file_count,
                    pi.additions,
                    pi.deletions,
                    pi.hunk_count,
                    psvai.inherited_from_version_num,
                    pim.commit_oid AS mainline_commit_oid,
                    mc.first_containing_tag AS mainline_merged_in_tag,
                    mc.first_final_release AS mainline_merged_in_release,
                    pim.match_method AS mainline_match_method
                FROM patch_series_version_assembled_items psvai
                JOIN patch_items pi
                  ON pi.id = psvai.patch_item_id
                JOIN messages m
                  ON m.id = pi.message_pk
                LEFT JOIN patch_item_mainline_matches pim
                  ON pim.patch_item_id = pi.id
                 AND pim.is_canonical = true
                LEFT JOIN mainline_commits mc
                  ON mc.commit_oid = pim.commit_oid
                WHERE psvai.patch_series_version_id = $1
                ORDER BY psvai.ordinal ASC, pi.id ASC"#,
            )
            .bind(patch_series_version_id)
            .fetch_all(&self.pool)
            .await;
        }

        sqlx::query_as::<_, SeriesVersionPatchItemRecord>(
            r#"SELECT
                pi.id AS patch_item_id,
                pi.ordinal,
                pi.total,
                pi.message_pk,
                m.message_id_primary,
                pi.subject_raw,
                pi.subject_norm,
                pi.commit_subject,
                pi.commit_subject_norm,
                pi.item_type,
                pi.has_diff,
                pi.patch_id_stable,
                pi.file_count,
                pi.additions,
                pi.deletions,
                pi.hunk_count,
                NULL::int4 AS inherited_from_version_num,
                pim.commit_oid AS mainline_commit_oid,
                mc.first_containing_tag AS mainline_merged_in_tag,
                mc.first_final_release AS mainline_merged_in_release,
                pim.match_method AS mainline_match_method
            FROM patch_items pi
            JOIN messages m
              ON m.id = pi.message_pk
            LEFT JOIN patch_item_mainline_matches pim
              ON pim.patch_item_id = pi.id
             AND pim.is_canonical = true
            LEFT JOIN mainline_commits mc
              ON mc.commit_oid = pim.commit_oid
            WHERE pi.patch_series_version_id = $1
            ORDER BY pi.ordinal ASC, pi.id ASC"#,
        )
        .bind(patch_series_version_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_series_logical_compare(
        &self,
        patch_series_id: i64,
        v1_version_num: i32,
        v2_version_num: i32,
    ) -> Result<Vec<SeriesLogicalCompareRow>> {
        sqlx::query_as::<_, SeriesLogicalCompareRow>(
            r#"SELECT
                pl.slot,
                pl.title_norm,
                plv1.patch_item_id AS v1_patch_item_id,
                pi1.item_type AS v1_item_type,
                pi1.patch_id_stable AS v1_patch_id_stable,
                pi1.subject_raw AS v1_subject_raw,
                plv2.patch_item_id AS v2_patch_item_id,
                pi2.item_type AS v2_item_type,
                pi2.patch_id_stable AS v2_patch_id_stable,
                pi2.subject_raw AS v2_subject_raw
            FROM patch_logical pl
            LEFT JOIN patch_logical_versions plv1
              ON plv1.patch_logical_id = pl.id
             AND plv1.version_num = $2
            LEFT JOIN patch_logical_versions plv2
              ON plv2.patch_logical_id = pl.id
             AND plv2.version_num = $3
            LEFT JOIN patch_items pi1
              ON pi1.id = plv1.patch_item_id
            LEFT JOIN patch_items pi2
              ON pi2.id = plv2.patch_item_id
            WHERE pl.patch_series_id = $1
            ORDER BY pl.slot ASC"#,
        )
        .bind(patch_series_id)
        .bind(v1_version_num)
        .bind(v2_version_num)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn aggregate_patch_item_files(
        &self,
        patch_item_ids: &[i64],
    ) -> Result<Vec<PatchItemFileAggregateRecord>> {
        if patch_item_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, PatchItemFileAggregateRecord>(
            r#"SELECT
                pif.new_path AS path,
                SUM(pif.additions)::bigint AS additions,
                SUM(pif.deletions)::bigint AS deletions,
                SUM(pif.hunk_count)::bigint AS hunk_count
            FROM patch_item_files pif
            WHERE pif.patch_item_id = ANY($1)
            GROUP BY pif.new_path
            ORDER BY pif.new_path ASC"#,
        )
        .bind(patch_item_ids)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_patch_items_for_version(
        &self,
        patch_series_version_id: i64,
    ) -> Result<Vec<PatchItemRecord>> {
        sqlx::query_as::<_, PatchItemRecord>(
            r#"SELECT
                id,
                patch_series_version_id,
                ordinal,
                total,
                message_pk,
                subject_raw,
                subject_norm,
                commit_subject,
                commit_subject_norm,
                item_type,
                has_diff,
                patch_id_stable
            FROM patch_items
            WHERE patch_series_version_id = $1
            ORDER BY ordinal ASC"#,
        )
        .bind(patch_series_version_id)
        .fetch_all(&self.pool)
        .await
    }
}
