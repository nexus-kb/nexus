use super::*;

impl IngestStore {
    pub async fn ingest_messages_copy_batch(
        &self,
        repo: &MailingListRepo,
        rows: &[IngestCommitRow],
        relaxed_durability: bool,
    ) -> Result<BatchWriteOutcome> {
        if rows.is_empty() {
            return Ok(BatchWriteOutcome::default());
        }

        let mut tx = self.pool.begin().await?;
        if relaxed_durability {
            sqlx::query("SET LOCAL synchronous_commit = off")
                .execute(&mut *tx)
                .await?;
        }

        let mut unique_indexes = Vec::with_capacity(rows.len());
        let mut seen_commit_oids: HashSet<&str> = HashSet::new();
        for (idx, row) in rows.iter().enumerate() {
            if seen_commit_oids.insert(row.git_commit_oid.as_str()) {
                unique_indexes.push(idx);
            }
        }

        let commit_oids: Vec<String> = unique_indexes
            .iter()
            .map(|idx| rows[*idx].git_commit_oid.clone())
            .collect();

        #[derive(sqlx::FromRow)]
        struct ExistingInstanceRow {
            git_commit_oid: String,
            message_pk: i64,
        }

        let existing_instance_rows = sqlx::query_as::<_, ExistingInstanceRow>(
            r#"SELECT git_commit_oid, message_pk
            FROM list_message_instances
            WHERE mailing_list_id = $1
              AND git_commit_oid = ANY($2)"#,
        )
        .bind(repo.mailing_list_id)
        .bind(&commit_oids)
        .fetch_all(&mut *tx)
        .await?;

        let mut existing_instances = HashMap::with_capacity(existing_instance_rows.len());
        for row in existing_instance_rows {
            existing_instances.insert(row.git_commit_oid, row.message_pk);
        }

        let mut message_pk_by_commit: HashMap<String, i64> =
            HashMap::with_capacity(unique_indexes.len());
        let mut skipped_commits = Vec::new();
        let mut skipped_commit_set = HashSet::new();
        let mut pending_indexes = Vec::new();

        for idx in &unique_indexes {
            let row = &rows[*idx];
            if let Some(message_pk) = existing_instances.get(&row.git_commit_oid).copied() {
                message_pk_by_commit.insert(row.git_commit_oid.clone(), message_pk);
                skipped_commit_set.insert(row.git_commit_oid.clone());
                skipped_commits.push(row.git_commit_oid.clone());
            } else {
                pending_indexes.push(*idx);
            }
        }

        #[derive(sqlx::FromRow)]
        struct ExistingMessageRow {
            id: i64,
            content_hash_sha256: Vec<u8>,
            message_id_primary: String,
        }

        #[derive(Debug, Clone)]
        struct MessageIdentity {
            id: i64,
            message_id_primary: String,
        }

        let mut existing_messages_by_hash: HashMap<Vec<u8>, MessageIdentity> = HashMap::new();
        if !pending_indexes.is_empty() {
            let mut content_hashes = Vec::new();
            let mut seen_hashes: HashSet<Vec<u8>> = HashSet::new();
            for idx in &pending_indexes {
                let hash = &rows[*idx].parsed_message.content_hash_sha256;
                if seen_hashes.insert(hash.clone()) {
                    content_hashes.push(hash.clone());
                }
            }

            if !content_hashes.is_empty() {
                let mut qb = QueryBuilder::<Postgres>::new(
                    "SELECT id, content_hash_sha256, message_id_primary \
                     FROM messages WHERE content_hash_sha256 IN (",
                );
                let mut separated = qb.separated(", ");
                for hash in &content_hashes {
                    separated.push_bind(hash);
                }
                separated.push_unseparated(")");

                let existing_messages = qb
                    .build_query_as::<ExistingMessageRow>()
                    .fetch_all(&mut *tx)
                    .await?;

                for message in existing_messages {
                    existing_messages_by_hash.insert(
                        message.content_hash_sha256,
                        MessageIdentity {
                            id: message.id,
                            message_id_primary: message.message_id_primary,
                        },
                    );
                }
            }
        }

        let mut insert_indexes = Vec::new();
        let mut pending_insert_hashes: HashSet<Vec<u8>> = HashSet::new();
        for idx in &pending_indexes {
            let row = &rows[*idx];
            if let Some(message) =
                existing_messages_by_hash.get(&row.parsed_message.content_hash_sha256)
            {
                let message_pk = message.id;
                message_pk_by_commit.insert(row.git_commit_oid.clone(), message_pk);
            } else if pending_insert_hashes.insert(row.parsed_message.content_hash_sha256.clone()) {
                insert_indexes.push(*idx);
            }
        }

        let body_ids = reserve_ids(&mut tx, "message_bodies_id_seq", insert_indexes.len()).await?;
        let message_ids = reserve_ids(&mut tx, "messages_id_seq", insert_indexes.len()).await?;

        let mut body_rows = Vec::with_capacity(insert_indexes.len());
        let mut message_rows = Vec::with_capacity(insert_indexes.len());
        let mut inserted_messages_by_hash = HashMap::new();
        let mut inserted_primary_by_hash = HashMap::new();
        for (offset, idx) in insert_indexes.iter().enumerate() {
            let row = &rows[*idx];
            let body_id = body_ids[offset];
            let message_id = message_ids[offset];

            body_rows.push(CopyMessageBodyRow {
                id: body_id,
                body_text: row.parsed_message.body.body_text.clone(),
                diff_text: row.parsed_message.body.diff_text.clone(),
                search_text: row.parsed_message.body.search_text.clone(),
                has_diff: row.parsed_message.body.has_diff,
                has_attachments: row.parsed_message.body.has_attachments,
            });

            message_rows.push(CopyMessageRow {
                id: message_id,
                content_hash_sha256: row.parsed_message.content_hash_sha256.clone(),
                subject_raw: row.parsed_message.subject_raw.clone(),
                subject_norm: row.parsed_message.subject_norm.clone(),
                from_name: row.parsed_message.from_name.clone(),
                from_email: row.parsed_message.from_email.clone(),
                date_utc: row.parsed_message.date_utc,
                to_raw: row.parsed_message.to_raw.clone(),
                cc_raw: row.parsed_message.cc_raw.clone(),
                message_ids: row.parsed_message.message_ids.clone(),
                message_id_primary: row.parsed_message.message_id_primary.clone(),
                in_reply_to_ids: row.parsed_message.in_reply_to_ids.clone(),
                references_ids: row.parsed_message.references_ids.clone(),
                mime_type: row.parsed_message.mime_type.clone(),
                body_id,
            });

            inserted_messages_by_hash
                .insert(row.parsed_message.content_hash_sha256.clone(), message_id);
            inserted_primary_by_hash.insert(
                row.parsed_message.content_hash_sha256.clone(),
                row.parsed_message.message_id_primary.clone(),
            );
        }

        for idx in &pending_indexes {
            let row = &rows[*idx];
            if message_pk_by_commit.contains_key(&row.git_commit_oid) {
                continue;
            }

            if let Some(message_pk) = existing_messages_by_hash
                .get(&row.parsed_message.content_hash_sha256)
                .map(|message| message.id)
                .or_else(|| {
                    inserted_messages_by_hash
                        .get(&row.parsed_message.content_hash_sha256)
                        .copied()
                })
            {
                message_pk_by_commit.insert(row.git_commit_oid.clone(), message_pk);
            } else if skipped_commit_set.insert(row.git_commit_oid.clone()) {
                skipped_commits.push(row.git_commit_oid.clone());
            }
        }

        let mut message_id_rows = Vec::new();
        let mut instance_rows = Vec::new();
        for idx in &pending_indexes {
            let row = &rows[*idx];
            let Some(message_pk) = message_pk_by_commit.get(&row.git_commit_oid).copied() else {
                continue;
            };
            let canonical_primary_message_id = existing_messages_by_hash
                .get(&row.parsed_message.content_hash_sha256)
                .map(|message| message.message_id_primary.as_str())
                .or_else(|| {
                    inserted_primary_by_hash
                        .get(&row.parsed_message.content_hash_sha256)
                        .map(String::as_str)
                })
                .unwrap_or(row.parsed_message.message_id_primary.as_str());

            for message_id in &row.parsed_message.message_ids {
                message_id_rows.push((
                    message_id.clone(),
                    message_pk,
                    message_id == canonical_primary_message_id,
                ));
            }

            instance_rows.push(CopyInstanceRow {
                mailing_list_id: repo.mailing_list_id,
                message_pk,
                repo_id: repo.id,
                git_commit_oid: row.git_commit_oid.clone(),
            });
        }

        let message_id_rows = dedupe_message_id_rows(message_id_rows);
        let existing_message_id_rows =
            load_existing_message_id_rows(&mut tx, &message_id_rows).await?;

        let mut existing_primary_lookup = HashMap::with_capacity(existing_message_id_rows.len());
        for row in existing_message_id_rows {
            existing_primary_lookup.insert((row.message_id, row.message_pk), row.is_primary);
        }

        let mut message_id_insert_rows = Vec::new();
        let mut message_id_promote_rows = Vec::new();
        for row in message_id_rows {
            let key = (row.message_id.clone(), row.message_pk);
            if let Some(existing_primary) = existing_primary_lookup.get(&key).copied() {
                if row.is_primary && !existing_primary {
                    message_id_promote_rows.push(key);
                }
            } else {
                message_id_insert_rows.push(row);
            }
        }

        if !body_rows.is_empty() {
            let payload = build_message_bodies_copy_payload(&body_rows)?;
            copy_into_table(
                &mut tx,
                "COPY message_bodies (id, body_text, diff_text, search_text, has_diff, has_attachments) \
                 FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                payload,
            )
            .await?;
        }

        if !message_rows.is_empty() {
            let payload = build_messages_copy_payload(&message_rows)?;
            copy_into_table(
                &mut tx,
                "COPY messages (id, content_hash_sha256, subject_raw, subject_norm, from_name, from_email, \
                 date_utc, to_raw, cc_raw, message_ids, message_id_primary, in_reply_to_ids, references_ids, \
                 mime_type, body_id) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                payload,
            )
            .await?;
        }

        if !message_id_promote_rows.is_empty() {
            promote_message_id_rows(&mut tx, &message_id_promote_rows).await?;
        }

        if !message_id_insert_rows.is_empty() {
            let payload = build_message_id_copy_payload(&message_id_insert_rows)?;
            copy_into_table(
                &mut tx,
                "COPY message_id_map (message_id, message_pk, is_primary) \
                 FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                payload,
            )
            .await?;
        }

        let inserted_instances = if instance_rows.is_empty() {
            0
        } else {
            let payload = build_instances_copy_payload(&instance_rows)?;
            copy_into_table(
                &mut tx,
                "COPY list_message_instances (mailing_list_id, message_pk, repo_id, git_commit_oid) \
                 FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                payload,
            )
            .await?
        };

        let patch_facts_by_message_pk =
            collect_patch_facts_by_message_pk(rows, &pending_indexes, &message_pk_by_commit);
        upsert_message_patch_facts_batch(&mut tx, &patch_facts_by_message_pk).await?;

        let mut message_pks: Vec<i64> = message_pk_by_commit.values().copied().collect();
        message_pks.sort_unstable();
        message_pks.dedup();

        validate_text_integrity(&mut tx, &message_pks).await?;

        tx.commit().await?;

        Ok(BatchWriteOutcome {
            inserted_instances,
            message_pks,
            skipped_commits,
        })
    }
}
