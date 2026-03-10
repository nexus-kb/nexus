use super::*;

impl LineageStore {
    pub async fn find_inherited_patch_item(
        &self,
        patch_series_id: i64,
        current_version_num: i32,
        ordinal: i32,
    ) -> Result<Option<(i64, i32)>> {
        sqlx::query_as::<_, (i64, i32)>(
            r#"SELECT
                pi.id,
                psv.version_num
            FROM patch_series_versions psv
            JOIN patch_items pi
              ON pi.patch_series_version_id = psv.id
            WHERE psv.patch_series_id = $1
              AND psv.version_num < $2
              AND pi.ordinal = $3
              AND pi.item_type = 'patch'
            ORDER BY psv.version_num DESC, psv.sent_at DESC, psv.id DESC
            LIMIT 1"#,
        )
        .bind(patch_series_id)
        .bind(current_version_num)
        .bind(ordinal)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn replace_assembled_items(
        &self,
        patch_series_version_id: i64,
        items: &[(i32, i64, Option<i32>)],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            "DELETE FROM patch_series_version_assembled_items WHERE patch_series_version_id = $1",
        )
        .bind(patch_series_version_id)
        .execute(&mut *tx)
        .await?;

        for (ordinal, patch_item_id, inherited_from_version_num) in items {
            sqlx::query(
                r#"INSERT INTO patch_series_version_assembled_items
                (patch_series_version_id, ordinal, patch_item_id, inherited_from_version_num)
                VALUES ($1, $2, $3, $4)"#,
            )
            .bind(patch_series_version_id)
            .bind(*ordinal)
            .bind(*patch_item_id)
            .bind(*inherited_from_version_num)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn set_partial_reroll_flag(
        &self,
        patch_series_version_id: i64,
        is_partial_reroll: bool,
    ) -> Result<()> {
        sqlx::query("UPDATE patch_series_versions SET is_partial_reroll = $2 WHERE id = $1")
            .bind(patch_series_version_id)
            .bind(is_partial_reroll)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn load_assembled_items_for_series(
        &self,
        patch_series_id: i64,
    ) -> Result<Vec<AssembledItemRecord>> {
        sqlx::query_as::<_, AssembledItemRecord>(
            r#"SELECT
                psv.version_num,
                psvai.ordinal,
                psvai.patch_item_id,
                pi.patch_id_stable,
                COALESCE(pi.commit_subject_norm, pi.subject_norm) AS title_norm
            FROM patch_series_versions psv
            JOIN patch_series_version_assembled_items psvai
              ON psvai.patch_series_version_id = psv.id
            JOIN patch_items pi
              ON pi.id = psvai.patch_item_id
            WHERE psv.patch_series_id = $1
            ORDER BY psv.version_num ASC, psvai.ordinal ASC"#,
        )
        .bind(patch_series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_patch_logicals_for_series(
        &self,
        patch_series_id: i64,
    ) -> Result<Vec<PatchLogicalRecord>> {
        sqlx::query_as::<_, PatchLogicalRecord>(
            r#"SELECT id, patch_series_id, slot, title_norm
            FROM patch_logical
            WHERE patch_series_id = $1
            ORDER BY slot ASC"#,
        )
        .bind(patch_series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn upsert_patch_logical(
        &self,
        patch_series_id: i64,
        slot: i32,
        title_norm: &str,
    ) -> Result<PatchLogicalRecord> {
        sqlx::query_as::<_, PatchLogicalRecord>(
            r#"INSERT INTO patch_logical (patch_series_id, slot, title_norm)
            VALUES ($1, $2, $3)
            ON CONFLICT (patch_series_id, slot)
            DO UPDATE SET title_norm = CASE
                WHEN patch_logical.title_norm = '' THEN EXCLUDED.title_norm
                ELSE patch_logical.title_norm
            END
            RETURNING id, patch_series_id, slot, title_norm"#,
        )
        .bind(patch_series_id)
        .bind(slot)
        .bind(title_norm)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn replace_patch_logical_versions(
        &self,
        patch_series_id: i64,
        mappings: &[(i32, i32, i64)],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"DELETE FROM patch_logical_versions
            WHERE patch_logical_id IN (
                SELECT id
                FROM patch_logical
                WHERE patch_series_id = $1
            )"#,
        )
        .bind(patch_series_id)
        .execute(&mut *tx)
        .await?;

        let logical_rows = sqlx::query_as::<_, PatchLogicalRecord>(
            r#"SELECT id, patch_series_id, slot, title_norm
            FROM patch_logical
            WHERE patch_series_id = $1"#,
        )
        .bind(patch_series_id)
        .fetch_all(&mut *tx)
        .await?;

        let mut slot_to_id = std::collections::HashMap::new();
        for logical in logical_rows {
            slot_to_id.insert(logical.slot, logical.id);
        }

        for (slot, version_num, patch_item_id) in mappings {
            if let Some(patch_logical_id) = slot_to_id.get(slot).copied() {
                sqlx::query(
                    r#"INSERT INTO patch_logical_versions
                    (patch_logical_id, patch_item_id, version_num)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (patch_logical_id, version_num)
                    DO UPDATE SET patch_item_id = EXCLUDED.patch_item_id"#,
                )
                .bind(patch_logical_id)
                .bind(*patch_item_id)
                .bind(*version_num)
                .execute(&mut *tx)
                .await?;
            }
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn list_versions_for_series(
        &self,
        patch_series_id: i64,
    ) -> Result<Vec<PatchSeriesVersionRecord>> {
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
            ORDER BY version_num ASC, sent_at ASC, id ASC"#,
        )
        .bind(patch_series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_patch_ids_by_series_and_version(
        &self,
        patch_series_id: i64,
    ) -> Result<Vec<SeriesVersionPatchRef>> {
        sqlx::query_as::<_, SeriesVersionPatchRef>(
            r#"SELECT
                psv.patch_series_id,
                psv.version_num,
                pi.patch_id_stable
            FROM patch_series_versions psv
            JOIN patch_items pi
              ON pi.patch_series_version_id = psv.id
            WHERE psv.patch_series_id = $1
              AND pi.item_type = 'patch'
            ORDER BY psv.version_num ASC, pi.ordinal ASC"#,
        )
        .bind(patch_series_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn load_patch_item_diffs(
        &self,
        patch_item_ids: &[i64],
    ) -> Result<Vec<PatchItemDiffRecord>> {
        if patch_item_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, PatchItemDiffRecord>(
            r#"SELECT
                pi.id AS patch_item_id,
                mb.diff_text
            FROM patch_items pi
            JOIN messages m ON m.id = pi.message_pk
            JOIN message_bodies mb ON mb.id = m.body_id
            WHERE pi.id = ANY($1)
            ORDER BY pi.id ASC"#,
        )
        .bind(patch_item_ids)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn set_patch_item_patch_id(
        &self,
        patch_item_id: i64,
        patch_id_stable: Option<&str>,
    ) -> Result<()> {
        sqlx::query("UPDATE patch_items SET patch_id_stable = $2 WHERE id = $1")
            .bind(patch_item_id)
            .bind(patch_id_stable)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn set_patch_item_patch_ids_batch(
        &self,
        updates: &[(i64, Option<String>)],
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "UPDATE patch_items pi \
             SET patch_id_stable = vals.patch_id_stable \
             FROM (",
        );
        qb.push_values(updates.iter(), |mut b, (patch_item_id, patch_id)| {
            b.push_bind(*patch_item_id).push_bind(patch_id.clone());
        });
        qb.push(") AS vals(patch_item_id, patch_id_stable) WHERE pi.id = vals.patch_item_id");
        qb.build().execute(&self.pool).await?;
        Ok(())
    }

    pub async fn list_patch_items_for_message_ids(&self, message_pks: &[i64]) -> Result<Vec<i64>> {
        if message_pks.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query_scalar::<_, i64>(
            r#"SELECT DISTINCT pi.id
            FROM patch_items pi
            WHERE pi.message_pk = ANY($1)
            ORDER BY pi.id ASC"#,
        )
        .bind(message_pks)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub fn unique_message_ids(
        references_ids: &[String],
        in_reply_to_ids: &[String],
    ) -> Vec<String> {
        let mut set = BTreeSet::new();
        for value in references_ids {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                set.insert(trimmed.to_string());
            }
        }
        for value in in_reply_to_ids {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                set.insert(trimmed.to_string());
            }
        }
        set.into_iter().collect()
    }
}
