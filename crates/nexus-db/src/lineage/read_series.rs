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
                version_fingerprint
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
                COALESCE(latest.is_rfc, false) AS is_rfc_latest
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
                psv.thread_mailing_list_id,
                psv.thread_id,
                psv.cover_message_pk,
                psv.first_patch_message_pk,
                psv.sent_at,
                psv.subject_raw,
                psv.subject_norm,
                COUNT(*) FILTER (WHERE pi.item_type = 'patch') AS patch_count
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
                psv.thread_mailing_list_id,
                psv.thread_id,
                psv.cover_message_pk,
                psv.first_patch_message_pk,
                psv.sent_at,
                psv.subject_raw,
                psv.subject_norm
            ORDER BY psv.version_num ASC, psv.sent_at ASC, psv.id ASC"#,
        )
        .bind(patch_series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_thread_ref(
        &self,
        mailing_list_id: i64,
        thread_id: i64,
    ) -> Result<Option<ThreadRefRecord>> {
        sqlx::query_as::<_, ThreadRefRecord>(
            r#"SELECT
                t.id AS thread_id,
                ml.list_key,
                t.message_count::bigint AS message_count,
                t.last_activity_at
            FROM threads t
            JOIN mailing_lists ml
              ON ml.id = t.mailing_list_id
            WHERE t.mailing_list_id = $1
              AND t.id = $2
            LIMIT 1"#,
        )
        .bind(mailing_list_id)
        .bind(thread_id)
        .fetch_optional(&self.pool)
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
                    psvai.inherited_from_version_num
                FROM patch_series_version_assembled_items psvai
                JOIN patch_items pi
                  ON pi.id = psvai.patch_item_id
                JOIN messages m
                  ON m.id = pi.message_pk
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
                NULL::int4 AS inherited_from_version_num
            FROM patch_items pi
            JOIN messages m
              ON m.id = pi.message_pk
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
