use super::*;

impl LineageStore {
    pub async fn upsert_series(&self, input: &UpsertPatchSeriesInput) -> Result<PatchSeriesRecord> {
        if let Some(change_id) = input.change_id.as_deref() {
            return sqlx::query_as::<_, PatchSeriesRecord>(
                r#"INSERT INTO patch_series
                (canonical_subject_norm, author_email, author_name, change_id, created_at, last_seen_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (change_id) WHERE change_id IS NOT NULL
                DO UPDATE SET canonical_subject_norm = EXCLUDED.canonical_subject_norm,
                              author_email = EXCLUDED.author_email,
                              author_name = COALESCE(EXCLUDED.author_name, patch_series.author_name),
                              last_seen_at = GREATEST(EXCLUDED.last_seen_at, patch_series.last_seen_at)
                RETURNING id,
                          canonical_subject_norm,
                          author_email,
                          author_name,
                          change_id,
                          created_at,
                          last_seen_at,
                          latest_version_id"#,
            )
            .bind(&input.canonical_subject_norm)
            .bind(&input.author_email)
            .bind(&input.author_name)
            .bind(change_id)
            .bind(input.created_at)
            .bind(input.last_seen_at)
            .fetch_one(&self.pool)
            .await;
        }

        sqlx::query_as::<_, PatchSeriesRecord>(
            r#"INSERT INTO patch_series
            (canonical_subject_norm, author_email, author_name, created_at, last_seen_at)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id,
                      canonical_subject_norm,
                      author_email,
                      author_name,
                      change_id,
                      created_at,
                      last_seen_at,
                      latest_version_id"#,
        )
        .bind(&input.canonical_subject_norm)
        .bind(&input.author_email)
        .bind(&input.author_name)
        .bind(input.created_at)
        .bind(input.last_seen_at)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn touch_series_seen(
        &self,
        series_id: i64,
        last_seen_at: DateTime<Utc>,
        latest_version_id: Option<i64>,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE patch_series
            SET last_seen_at = GREATEST(last_seen_at, $2),
                latest_version_id = CASE
                    WHEN $3 IS NULL THEN latest_version_id
                    WHEN latest_version_id IS NULL THEN $3
                    WHEN (
                        SELECT version_num
                        FROM patch_series_versions
                        WHERE id = $3
                    ) >= (
                        SELECT version_num
                        FROM patch_series_versions
                        WHERE id = latest_version_id
                    ) THEN $3
                    ELSE latest_version_id
                END
            WHERE id = $1"#,
        )
        .bind(series_id)
        .bind(last_seen_at)
        .bind(latest_version_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn upsert_series_list_presence(
        &self,
        series_id: i64,
        mailing_list_id: i64,
        seen_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO patch_series_lists
            (patch_series_id, mailing_list_id, first_seen_at, last_seen_at)
            VALUES ($1, $2, $3, $3)
            ON CONFLICT (patch_series_id, mailing_list_id)
            DO UPDATE SET first_seen_at = LEAST(patch_series_lists.first_seen_at, EXCLUDED.first_seen_at),
                          last_seen_at = GREATEST(patch_series_lists.last_seen_at, EXCLUDED.last_seen_at)"#,
        )
        .bind(series_id)
        .bind(mailing_list_id)
        .bind(seen_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn upsert_series_version(
        &self,
        input: &UpsertPatchSeriesVersionInput,
    ) -> Result<PatchSeriesVersionRecord> {
        sqlx::query_as::<_, PatchSeriesVersionRecord>(
            r#"INSERT INTO patch_series_versions
            (patch_series_id, version_num, is_rfc, is_resend, thread_id,
             cover_message_pk, first_patch_message_pk, sent_at,
             subject_raw, subject_norm, base_commit, version_fingerprint)
            VALUES ($1, $2, $3, $4, $5,
                    $6, $7, $8,
                    $9, $10, $11, $12)
            ON CONFLICT (patch_series_id, version_num, version_fingerprint)
            DO UPDATE SET is_rfc = EXCLUDED.is_rfc,
                          is_resend = EXCLUDED.is_resend,
                          thread_id = COALESCE(EXCLUDED.thread_id, patch_series_versions.thread_id),
                          cover_message_pk = COALESCE(EXCLUDED.cover_message_pk, patch_series_versions.cover_message_pk),
                          first_patch_message_pk = COALESCE(EXCLUDED.first_patch_message_pk, patch_series_versions.first_patch_message_pk),
                          sent_at = LEAST(EXCLUDED.sent_at, patch_series_versions.sent_at),
                          subject_raw = EXCLUDED.subject_raw,
                          subject_norm = EXCLUDED.subject_norm,
                          base_commit = COALESCE(EXCLUDED.base_commit, patch_series_versions.base_commit)
            RETURNING id,
                      patch_series_id,
                      version_num,
                      is_rfc,
                      is_resend,
                      is_partial_reroll,
                      thread_id,
                      cover_message_pk,
                      first_patch_message_pk,
                      sent_at,
                      subject_raw,
                      subject_norm,
                      base_commit,
                      version_fingerprint"#,
        )
        .bind(input.patch_series_id)
        .bind(input.version_num)
        .bind(input.is_rfc)
        .bind(input.is_resend)
        .bind(input.thread_id)
        .bind(input.cover_message_pk)
        .bind(input.first_patch_message_pk)
        .bind(input.sent_at)
        .bind(&input.subject_raw)
        .bind(&input.subject_norm)
        .bind(&input.base_commit)
        .bind(&input.version_fingerprint)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn upsert_patch_item(
        &self,
        patch_series_version_id: i64,
        input: &UpsertPatchItemInput,
    ) -> Result<PatchItemRecord> {
        sqlx::query_as::<_, PatchItemRecord>(
            r#"INSERT INTO patch_items
            (patch_series_version_id, ordinal, total, message_pk,
             subject_raw, subject_norm, commit_subject, commit_subject_norm,
             commit_author_name, commit_author_email, item_type,
             has_diff, patch_id_stable, file_count, additions, deletions, hunk_count)
            VALUES ($1, $2, $3, $4,
                    $5, $6, $7, $8,
                    $9, $10, $11,
                    $12, $13, $14, $15, $16, $17)
            ON CONFLICT (patch_series_version_id, ordinal)
            DO UPDATE SET total = COALESCE(EXCLUDED.total, patch_items.total),
                          message_pk = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.message_pk
                              ELSE EXCLUDED.message_pk
                          END,
                          subject_raw = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.subject_raw
                              ELSE EXCLUDED.subject_raw
                          END,
                          subject_norm = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.subject_norm
                              ELSE EXCLUDED.subject_norm
                          END,
                          commit_subject = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_subject
                              ELSE EXCLUDED.commit_subject
                          END,
                          commit_subject_norm = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_subject_norm
                              ELSE EXCLUDED.commit_subject_norm
                          END,
                          commit_author_name = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_author_name
                              ELSE EXCLUDED.commit_author_name
                          END,
                          commit_author_email = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_author_email
                              ELSE EXCLUDED.commit_author_email
                          END,
                          item_type = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.item_type
                              ELSE EXCLUDED.item_type
                          END,
                          has_diff = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.has_diff
                              ELSE EXCLUDED.has_diff
                          END,
                          patch_id_stable = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.patch_id_stable
                              ELSE COALESCE(EXCLUDED.patch_id_stable, patch_items.patch_id_stable)
                          END,
                          file_count = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.file_count
                              ELSE EXCLUDED.file_count
                          END,
                          additions = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.additions
                              ELSE EXCLUDED.additions
                          END,
                          deletions = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.deletions
                              ELSE EXCLUDED.deletions
                          END,
                          hunk_count = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.hunk_count
                              ELSE EXCLUDED.hunk_count
                          END
            RETURNING id,
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
                      patch_id_stable"#,
        )
        .bind(patch_series_version_id)
        .bind(input.ordinal)
        .bind(input.total)
        .bind(input.message_pk)
        .bind(&input.subject_raw)
        .bind(&input.subject_norm)
        .bind(&input.commit_subject)
        .bind(&input.commit_subject_norm)
        .bind(&input.commit_author_name)
        .bind(&input.commit_author_email)
        .bind(&input.item_type)
        .bind(input.has_diff)
        .bind(&input.patch_id_stable)
        .bind(input.file_count)
        .bind(input.additions)
        .bind(input.deletions)
        .bind(input.hunk_count)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn upsert_patch_items_batch(
        &self,
        patch_series_version_id: i64,
        inputs: &[UpsertPatchItemInput],
    ) -> Result<Vec<PatchItemRecord>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO patch_items \
            (patch_series_version_id, ordinal, total, message_pk, \
             subject_raw, subject_norm, commit_subject, commit_subject_norm, \
             commit_author_name, commit_author_email, item_type, \
             has_diff, patch_id_stable, file_count, additions, deletions, hunk_count) ",
        );
        qb.push_values(inputs.iter(), |mut b, input| {
            b.push_bind(patch_series_version_id)
                .push_bind(input.ordinal)
                .push_bind(input.total)
                .push_bind(input.message_pk)
                .push_bind(&input.subject_raw)
                .push_bind(&input.subject_norm)
                .push_bind(&input.commit_subject)
                .push_bind(&input.commit_subject_norm)
                .push_bind(&input.commit_author_name)
                .push_bind(&input.commit_author_email)
                .push_bind(&input.item_type)
                .push_bind(input.has_diff)
                .push_bind(&input.patch_id_stable)
                .push_bind(input.file_count)
                .push_bind(input.additions)
                .push_bind(input.deletions)
                .push_bind(input.hunk_count);
        });
        qb.push(
            r#" ON CONFLICT (patch_series_version_id, ordinal)
            DO UPDATE SET total = COALESCE(EXCLUDED.total, patch_items.total),
                          message_pk = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.message_pk
                              ELSE EXCLUDED.message_pk
                          END,
                          subject_raw = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.subject_raw
                              ELSE EXCLUDED.subject_raw
                          END,
                          subject_norm = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.subject_norm
                              ELSE EXCLUDED.subject_norm
                          END,
                          commit_subject = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_subject
                              ELSE EXCLUDED.commit_subject
                          END,
                          commit_subject_norm = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_subject_norm
                              ELSE EXCLUDED.commit_subject_norm
                          END,
                          commit_author_name = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_author_name
                              ELSE EXCLUDED.commit_author_name
                          END,
                          commit_author_email = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.commit_author_email
                              ELSE EXCLUDED.commit_author_email
                          END,
                          item_type = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.item_type
                              ELSE EXCLUDED.item_type
                          END,
                          has_diff = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.has_diff
                              ELSE EXCLUDED.has_diff
                          END,
                          patch_id_stable = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.patch_id_stable
                              ELSE COALESCE(EXCLUDED.patch_id_stable, patch_items.patch_id_stable)
                          END,
                          file_count = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.file_count
                              ELSE EXCLUDED.file_count
                          END,
                          additions = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.additions
                              ELSE EXCLUDED.additions
                          END,
                          deletions = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.deletions
                              ELSE EXCLUDED.deletions
                          END,
                          hunk_count = CASE
                              WHEN patch_items.item_type = 'patch'
                               AND (EXCLUDED.item_type <> 'patch' OR EXCLUDED.has_diff = false)
                              THEN patch_items.hunk_count
                              ELSE EXCLUDED.hunk_count
                          END
            RETURNING id,
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
                      patch_id_stable"#,
        );

        qb.build_query_as::<PatchItemRecord>()
            .fetch_all(&self.pool)
            .await
    }
}
