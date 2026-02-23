use super::*;

impl LineageStore {
    pub async fn get_patch_item_detail(
        &self,
        patch_item_id: i64,
    ) -> Result<Option<PatchItemDetailRecord>> {
        sqlx::query_as::<_, PatchItemDetailRecord>(
            r#"SELECT
                pi.id AS patch_item_id,
                psv.patch_series_id,
                pi.patch_series_version_id,
                pi.ordinal,
                pi.total,
                pi.item_type,
                pi.subject_raw,
                pi.subject_norm,
                pi.commit_subject,
                pi.commit_subject_norm,
                pi.message_pk,
                m.message_id_primary,
                pi.patch_id_stable,
                pi.has_diff,
                pi.file_count,
                pi.additions,
                pi.deletions,
                pi.hunk_count
            FROM patch_items pi
            JOIN patch_series_versions psv
              ON psv.id = pi.patch_series_version_id
            JOIN messages m
              ON m.id = pi.message_pk
            WHERE pi.id = $1
            LIMIT 1"#,
        )
        .bind(patch_item_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn list_patch_item_files(
        &self,
        patch_item_id: i64,
    ) -> Result<Vec<PatchItemFileRecord>> {
        sqlx::query_as::<_, PatchItemFileRecord>(
            r#"SELECT
                patch_item_id,
                old_path,
                new_path,
                change_type,
                is_binary,
                additions,
                deletions,
                hunk_count,
                diff_start,
                diff_end
            FROM patch_item_files
            WHERE patch_item_id = $1
            ORDER BY new_path ASC"#,
        )
        .bind(patch_item_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_patch_item_file_diff_source(
        &self,
        patch_item_id: i64,
        path: &str,
    ) -> Result<Option<PatchItemFileDiffSliceSource>> {
        sqlx::query_as::<_, PatchItemFileDiffSliceSource>(
            r#"SELECT
                pif.new_path,
                pif.diff_start,
                pif.diff_end,
                mb.diff_text
            FROM patch_item_files pif
            JOIN patch_items pi
              ON pi.id = pif.patch_item_id
            JOIN messages m
              ON m.id = pi.message_pk
            JOIN message_bodies mb
              ON mb.id = m.body_id
            WHERE pif.patch_item_id = $1
              AND pif.new_path = $2
            LIMIT 1"#,
        )
        .bind(patch_item_id)
        .bind(path)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_patch_item_full_diff(&self, patch_item_id: i64) -> Result<Option<String>> {
        let row = sqlx::query_scalar::<_, Option<String>>(
            r#"SELECT mb.diff_text
            FROM patch_items pi
            JOIN messages m
              ON m.id = pi.message_pk
            JOIN message_bodies mb
              ON mb.id = m.body_id
            WHERE pi.id = $1
            LIMIT 1"#,
        )
        .bind(patch_item_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.flatten())
    }

    pub async fn get_message_body(&self, message_pk: i64) -> Result<Option<MessageBodyRecord>> {
        sqlx::query_as::<_, MessageBodyRecord>(
            r#"SELECT
                m.id AS message_pk,
                m.subject_raw,
                mb.body_text,
                mb.diff_text,
                mb.has_diff,
                mb.has_attachments
            FROM messages m
            JOIN message_bodies mb
              ON mb.id = m.body_id
            WHERE m.id = $1
            LIMIT 1"#,
        )
        .bind(message_pk)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_message_detail(&self, message_pk: i64) -> Result<Option<MessageDetailRecord>> {
        sqlx::query_as::<_, MessageDetailRecord>(
            r#"SELECT
                m.id AS message_pk,
                m.message_id_primary,
                m.subject_raw,
                m.subject_norm,
                m.from_name,
                m.from_email,
                m.date_utc,
                m.to_raw,
                m.cc_raw,
                m.references_ids,
                m.in_reply_to_ids,
                mb.has_diff,
                mb.has_attachments
            FROM messages m
            JOIN message_bodies mb
              ON mb.id = m.body_id
            WHERE m.id = $1
            LIMIT 1"#,
        )
        .bind(message_pk)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn resolve_message_pk_by_message_id(&self, message_id: &str) -> Result<Option<i64>> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT message_pk
            FROM message_id_map
            WHERE message_id = $1
            ORDER BY is_primary DESC, message_pk ASC
            LIMIT 1"#,
        )
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn list_threads(
        &self,
        mailing_list_id: i64,
        params: &ListThreadsParams,
    ) -> Result<Vec<ThreadListItemRecord>> {
        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT
                t.id AS thread_id,
                t.subject_norm,
                t.root_message_pk,
                t.created_at,
                t.last_activity_at,
                t.message_count,
                starter_msg.from_name AS starter_name,
                starter_msg.from_email AS starter_email,
                EXISTS (
                    SELECT 1
                    FROM thread_messages tm
                    JOIN messages m ON m.id = tm.message_pk
                    JOIN message_bodies mb ON mb.id = m.body_id
                    WHERE tm.mailing_list_id = t.mailing_list_id
                      AND tm.thread_id = t.id
                      AND mb.has_diff = true
                ) AS has_diff
            FROM threads t
            LEFT JOIN messages starter_msg ON starter_msg.id = t.root_message_pk
            WHERE t.mailing_list_id = "#,
        );
        qb.push_bind(mailing_list_id);

        let (cursor_column, descending) = match params.sort.as_str() {
            "date_desc" => ("t.created_at", true),
            "date_asc" => ("t.created_at", false),
            _ => ("t.last_activity_at", true),
        };

        if let Some(from_ts) = params.from_ts {
            qb.push(" AND ");
            qb.push(cursor_column);
            qb.push(" >= ");
            qb.push_bind(from_ts);
        }
        if let Some(to_ts) = params.to_ts {
            qb.push(" AND ");
            qb.push(cursor_column);
            qb.push(" < ");
            qb.push_bind(to_ts);
        }
        if let Some(author_email) = params.author_email.as_deref() {
            qb.push(
                r#" AND EXISTS (
                    SELECT 1
                    FROM thread_messages tm
                    JOIN messages m ON m.id = tm.message_pk
                    WHERE tm.mailing_list_id = t.mailing_list_id
                      AND tm.thread_id = t.id
                      AND lower(m.from_email) = lower("#,
            );
            qb.push_bind(author_email);
            qb.push("))");
        }
        if let Some(has_diff) = params.has_diff {
            if has_diff {
                qb.push(
                    r#" AND EXISTS (
                        SELECT 1
                        FROM thread_messages tm
                        JOIN messages m ON m.id = tm.message_pk
                        JOIN message_bodies mb ON mb.id = m.body_id
                        WHERE tm.mailing_list_id = t.mailing_list_id
                          AND tm.thread_id = t.id
                          AND mb.has_diff = true
                    )"#,
                );
            } else {
                qb.push(
                    r#" AND NOT EXISTS (
                        SELECT 1
                        FROM thread_messages tm
                        JOIN messages m ON m.id = tm.message_pk
                        JOIN message_bodies mb ON mb.id = m.body_id
                        WHERE tm.mailing_list_id = t.mailing_list_id
                          AND tm.thread_id = t.id
                          AND mb.has_diff = true
                    )"#,
                );
            }
        }

        if let (Some(cursor_ts), Some(cursor_id)) = (params.cursor_ts, params.cursor_id) {
            qb.push(" AND (");
            qb.push(cursor_column);
            qb.push(", t.id) ");
            if descending {
                qb.push("< (");
            } else {
                qb.push("> (");
            }
            qb.push_bind(cursor_ts);
            qb.push(", ");
            qb.push_bind(cursor_id);
            qb.push(")");
        }

        let limit = params.limit.clamp(1, 500);
        qb.push(" ORDER BY ");
        qb.push(cursor_column);
        if descending {
            qb.push(" DESC, t.id DESC LIMIT ");
        } else {
            qb.push(" ASC, t.id ASC LIMIT ");
        }
        qb.push_bind(limit);

        qb.build_query_as::<ThreadListItemRecord>()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn count_threads(
        &self,
        mailing_list_id: i64,
        params: &ListThreadsParams,
    ) -> Result<i64> {
        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT COUNT(*)::bigint
            FROM threads t
            WHERE t.mailing_list_id = "#,
        );
        qb.push_bind(mailing_list_id);

        let cursor_column = if params.sort == "date_desc" || params.sort == "date_asc" {
            "t.created_at"
        } else {
            "t.last_activity_at"
        };

        if let Some(from_ts) = params.from_ts {
            qb.push(" AND ");
            qb.push(cursor_column);
            qb.push(" >= ");
            qb.push_bind(from_ts);
        }
        if let Some(to_ts) = params.to_ts {
            qb.push(" AND ");
            qb.push(cursor_column);
            qb.push(" < ");
            qb.push_bind(to_ts);
        }
        if let Some(author_email) = params.author_email.as_deref() {
            qb.push(
                r#" AND EXISTS (
                    SELECT 1
                    FROM thread_messages tm
                    JOIN messages m ON m.id = tm.message_pk
                    WHERE tm.mailing_list_id = t.mailing_list_id
                      AND tm.thread_id = t.id
                      AND lower(m.from_email) = lower("#,
            );
            qb.push_bind(author_email);
            qb.push("))");
        }
        if let Some(has_diff) = params.has_diff {
            if has_diff {
                qb.push(
                    r#" AND EXISTS (
                        SELECT 1
                        FROM thread_messages tm
                        JOIN messages m ON m.id = tm.message_pk
                        JOIN message_bodies mb ON mb.id = m.body_id
                        WHERE tm.mailing_list_id = t.mailing_list_id
                          AND tm.thread_id = t.id
                          AND mb.has_diff = true
                    )"#,
                );
            } else {
                qb.push(
                    r#" AND NOT EXISTS (
                        SELECT 1
                        FROM thread_messages tm
                        JOIN messages m ON m.id = tm.message_pk
                        JOIN message_bodies mb ON mb.id = m.body_id
                        WHERE tm.mailing_list_id = t.mailing_list_id
                          AND tm.thread_id = t.id
                          AND mb.has_diff = true
                    )"#,
                );
            }
        }

        let row = qb.build_query_scalar::<i64>().fetch_one(&self.pool).await?;
        Ok(row)
    }

    pub async fn list_thread_participants(
        &self,
        mailing_list_id: i64,
        thread_ids: &[i64],
    ) -> Result<Vec<ThreadParticipantRecord>> {
        if thread_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, ThreadParticipantRecord>(
            r#"WITH ranked AS (
                SELECT
                    tm.thread_id,
                    m.from_name,
                    m.from_email,
                    row_number() OVER (
                        PARTITION BY tm.thread_id, lower(m.from_email)
                        ORDER BY m.date_utc DESC NULLS LAST, m.id DESC
                    ) AS rn
                FROM thread_messages tm
                JOIN messages m
                  ON m.id = tm.message_pk
                WHERE tm.mailing_list_id = $1
                  AND tm.thread_id = ANY($2)
            )
            SELECT
                thread_id,
                from_name,
                from_email
            FROM ranked
            WHERE rn = 1
            ORDER BY thread_id ASC, from_email ASC"#,
        )
        .bind(mailing_list_id)
        .bind(thread_ids)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_thread_summary(
        &self,
        mailing_list_id: i64,
        thread_id: i64,
    ) -> Result<Option<ThreadSummaryRecord>> {
        sqlx::query_as::<_, ThreadSummaryRecord>(
            r#"SELECT
                id AS thread_id,
                subject_norm,
                last_activity_at,
                membership_hash
            FROM threads
            WHERE mailing_list_id = $1
              AND id = $2
            LIMIT 1"#,
        )
        .bind(mailing_list_id)
        .bind(thread_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn list_thread_messages(
        &self,
        mailing_list_id: i64,
        thread_id: i64,
        limit: i64,
        cursor_sort_key: Option<&[u8]>,
        cursor_message_pk: Option<i64>,
    ) -> Result<Vec<ThreadMessageRecord>> {
        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT
                tm.message_pk,
                tm.parent_message_pk,
                tm.depth,
                tm.sort_key,
                m.from_name,
                m.from_email,
                m.date_utc,
                m.subject_raw,
                mb.has_diff,
                mb.body_text,
                patch_ref.patch_item_id
            FROM thread_messages tm
            JOIN messages m
              ON m.id = tm.message_pk
            JOIN message_bodies mb
              ON mb.id = m.body_id
            LEFT JOIN LATERAL (
                SELECT pi.id AS patch_item_id
                FROM patch_items pi
                WHERE pi.message_pk = tm.message_pk
                  AND pi.item_type = 'patch'
                ORDER BY pi.id DESC
                LIMIT 1
            ) patch_ref ON true
            WHERE tm.mailing_list_id = "#,
        );
        qb.push_bind(mailing_list_id);
        qb.push(" AND tm.thread_id = ");
        qb.push_bind(thread_id);

        if let (Some(cursor_sort_key), Some(cursor_message_pk)) =
            (cursor_sort_key, cursor_message_pk)
        {
            qb.push(" AND (tm.sort_key, tm.message_pk) > (");
            qb.push_bind(cursor_sort_key.to_vec());
            qb.push(", ");
            qb.push_bind(cursor_message_pk);
            qb.push(")");
        }

        qb.push(" ORDER BY tm.sort_key ASC, tm.message_pk ASC LIMIT ");
        qb.push_bind(limit.clamp(1, 500));

        qb.build_query_as::<ThreadMessageRecord>()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn count_thread_messages(&self, mailing_list_id: i64, thread_id: i64) -> Result<i64> {
        sqlx::query_scalar::<_, i64>(
            r#"SELECT COUNT(*)::bigint
            FROM thread_messages
            WHERE mailing_list_id = $1
              AND thread_id = $2"#,
        )
        .bind(mailing_list_id)
        .bind(thread_id)
        .fetch_one(&self.pool)
        .await
    }
}
