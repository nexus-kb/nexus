use super::*;

impl LineageStore {
    pub async fn load_messages_for_threads(
        &self,
        mailing_list_id: i64,
        thread_ids: &[i64],
    ) -> Result<Vec<LineageSourceMessage>> {
        if thread_ids.is_empty() {
            return Ok(Vec::new());
        }

        let deduped_thread_ids = thread_ids
            .iter()
            .copied()
            .filter(|thread_id| *thread_id > 0)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if deduped_thread_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tx = self.pool.begin().await?;
        // Avoid container /dev/shm pressure from parallel workers for large lineage windows.
        sqlx::query("SET LOCAL max_parallel_workers_per_gather = 0")
            .execute(&mut *tx)
            .await?;

        let out = self
            .load_messages_for_thread_ids_in_tx(&mut tx, mailing_list_id, &deduped_thread_ids)
            .await?;

        tx.commit().await?;
        Ok(out)
    }

    pub async fn load_messages_for_anchors(
        &self,
        mailing_list_id: i64,
        anchor_message_pks: &[i64],
    ) -> Result<Vec<LineageSourceMessage>> {
        if anchor_message_pks.is_empty() {
            return Ok(Vec::new());
        }

        let mut tx = self.pool.begin().await?;
        // Avoid container /dev/shm pressure from parallel workers for large lineage windows.
        sqlx::query("SET LOCAL max_parallel_workers_per_gather = 0")
            .execute(&mut *tx)
            .await?;

        let thread_ids = sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT tm.thread_id
            FROM thread_messages tm
            WHERE tm.mailing_list_id = $1
              AND tm.message_pk = ANY($2)
            ORDER BY tm.thread_id ASC"#,
        )
        .bind(mailing_list_id)
        .bind(anchor_message_pks)
        .fetch_all(&mut *tx)
        .await?;

        if thread_ids.is_empty() {
            tx.commit().await?;
            return Ok(Vec::new());
        }

        let out = self
            .load_messages_for_thread_ids_in_tx(&mut tx, mailing_list_id, &thread_ids)
            .await?;

        tx.commit().await?;
        Ok(out)
    }

    async fn load_messages_for_thread_ids_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        mailing_list_id: i64,
        thread_ids: &[i64],
    ) -> Result<Vec<LineageSourceMessage>> {
        if thread_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for thread_chunk in thread_ids.chunks(LOAD_MESSAGES_THREAD_CHUNK_SIZE) {
            let mut rows = sqlx::query_as::<_, LineageSourceMessage>(
                r#"SELECT
                    tm.thread_id,
                    tm.message_pk,
                    tm.sort_key,
                    m.message_id_primary,
                    m.subject_raw,
                    m.subject_norm,
                    m.from_name,
                    m.from_email,
                    m.date_utc,
                    m.references_ids,
                    m.in_reply_to_ids,
                    mpf.base_commit,
                    mpf.change_id,
                    COALESCE(mpf.has_diff, mb.has_diff, false) AS has_diff,
                    mpf.patch_id_stable,
                    NULL::text AS body_text,
                    NULL::text AS diff_text
                FROM thread_messages tm
                JOIN messages m
                  ON m.id = tm.message_pk
                JOIN message_bodies mb
                  ON mb.id = m.body_id
                LEFT JOIN message_patch_facts mpf
                  ON mpf.message_pk = m.id
                WHERE tm.mailing_list_id = $1
                  AND tm.thread_id = ANY($2)
                ORDER BY tm.thread_id ASC, tm.sort_key ASC"#,
            )
            .bind(mailing_list_id)
            .bind(thread_chunk)
            .fetch_all(&mut **tx)
            .await?;
            out.append(&mut rows);
        }
        Ok(out)
    }

    pub async fn resolve_message_ids(
        &self,
        mailing_list_id: i64,
        message_ids: &[String],
    ) -> Result<Vec<(String, i64)>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query_as::<_, (String, i64)>(
            r#"SELECT DISTINCT ON (mim.message_id)
                mim.message_id,
                mim.message_pk
            FROM message_id_map mim
            JOIN list_message_instances lmi
              ON lmi.message_pk = mim.message_pk
             AND lmi.mailing_list_id = $1
            WHERE mim.message_id = ANY($2)
            ORDER BY mim.message_id ASC, mim.is_primary DESC, mim.message_pk ASC"#,
        )
        .bind(mailing_list_id)
        .bind(message_ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn find_series_ids_by_message_ids(
        &self,
        mailing_list_id: i64,
        message_ids: &[String],
    ) -> Result<Vec<i64>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT psv.patch_series_id
            FROM message_id_map mim
            JOIN list_message_instances lmi
              ON lmi.message_pk = mim.message_pk
             AND lmi.mailing_list_id = $1
            JOIN patch_items pi
              ON pi.message_pk = mim.message_pk
            JOIN patch_series_versions psv
              ON psv.id = pi.patch_series_version_id
            WHERE mim.message_id = ANY($2)
            ORDER BY psv.patch_series_id ASC"#,
        )
        .bind(mailing_list_id)
        .bind(message_ids)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_series_by_change_id(
        &self,
        change_id: &str,
    ) -> Result<Option<PatchSeriesRecord>> {
        sqlx::query_as::<_, PatchSeriesRecord>(
            r#"SELECT
                id,
                canonical_subject_norm,
                author_email,
                author_name,
                change_id,
                created_at,
                last_seen_at,
                latest_version_id
            FROM patch_series
            WHERE change_id = $1
            LIMIT 1"#,
        )
        .bind(change_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_series_by_id(&self, series_id: i64) -> Result<Option<PatchSeriesRecord>> {
        sqlx::query_as::<_, PatchSeriesRecord>(
            r#"SELECT
                id,
                canonical_subject_norm,
                author_email,
                author_name,
                change_id,
                created_at,
                last_seen_at,
                latest_version_id
            FROM patch_series
            WHERE id = $1
            LIMIT 1"#,
        )
        .bind(series_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn find_similarity_candidates(
        &self,
        canonical_subject_norm: &str,
        from_ts: DateTime<Utc>,
        to_ts: DateTime<Utc>,
    ) -> Result<Vec<PatchSeriesRecord>> {
        sqlx::query_as::<_, PatchSeriesRecord>(
            r#"SELECT
                id,
                canonical_subject_norm,
                author_email,
                author_name,
                change_id,
                created_at,
                last_seen_at,
                latest_version_id
            FROM patch_series
            WHERE canonical_subject_norm = $1
              AND last_seen_at >= $2
              AND last_seen_at <= $3
            ORDER BY last_seen_at DESC, id ASC"#,
        )
        .bind(canonical_subject_norm)
        .bind(from_ts)
        .bind(to_ts)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_latest_patch_ids_for_series(&self, series_id: i64) -> Result<Vec<String>> {
        sqlx::query_scalar::<_, String>(
            r#"WITH latest AS (
                SELECT id
                FROM patch_series_versions
                WHERE patch_series_id = $1
                ORDER BY version_num DESC, sent_at DESC, id DESC
                LIMIT 1
            )
            SELECT DISTINCT pi.patch_id_stable
            FROM patch_items pi
            JOIN latest l ON l.id = pi.patch_series_version_id
            WHERE pi.item_type = 'patch'
              AND pi.patch_id_stable IS NOT NULL
            ORDER BY pi.patch_id_stable ASC"#,
        )
        .bind(series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_latest_patch_ids_for_series_bulk(
        &self,
        series_ids: &[i64],
    ) -> Result<Vec<(i64, String)>> {
        if series_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, (i64, String)>(
            r#"WITH ranked AS (
                SELECT
                    id,
                    patch_series_id,
                    row_number() OVER (
                        PARTITION BY patch_series_id
                        ORDER BY version_num DESC, sent_at DESC, id DESC
                    ) AS rn
                FROM patch_series_versions
                WHERE patch_series_id = ANY($1)
            )
            SELECT
                ranked.patch_series_id,
                pi.patch_id_stable
            FROM ranked
            JOIN patch_items pi
              ON pi.patch_series_version_id = ranked.id
            WHERE ranked.rn = 1
              AND pi.item_type = 'patch'
              AND pi.patch_id_stable IS NOT NULL
            ORDER BY ranked.patch_series_id ASC, pi.patch_id_stable ASC"#,
        )
        .bind(series_ids)
        .fetch_all(&self.pool)
        .await
    }
}
