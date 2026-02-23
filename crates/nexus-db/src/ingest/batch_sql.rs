use super::*;

impl IngestStore {
    pub async fn ingest_messages_batch(
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

        #[derive(sqlx::FromRow)]
        struct InsertedBodyRow {
            id: i64,
        }

        #[derive(sqlx::FromRow)]
        struct InsertedMessageRow {
            id: i64,
            content_hash_sha256: Vec<u8>,
        }

        let mut inserted_messages_by_hash = HashMap::new();
        let mut inserted_primary_by_hash = HashMap::new();
        if !insert_indexes.is_empty() {
            let mut insert_bodies_qb = QueryBuilder::<Postgres>::new(
                "INSERT INTO message_bodies (body_text, diff_text, search_text, has_diff, has_attachments) ",
            );
            insert_bodies_qb.push_values(insert_indexes.iter(), |mut b, idx| {
                let row = &rows[*idx];
                b.push_bind(row.parsed_message.body.body_text.as_deref())
                    .push_bind(row.parsed_message.body.diff_text.as_deref())
                    .push_bind(row.parsed_message.body.search_text.as_str())
                    .push_bind(row.parsed_message.body.has_diff)
                    .push_bind(row.parsed_message.body.has_attachments);
            });
            insert_bodies_qb.push(" RETURNING id");

            let inserted_bodies = insert_bodies_qb
                .build_query_as::<InsertedBodyRow>()
                .fetch_all(&mut *tx)
                .await?;

            let mut insert_messages_qb = QueryBuilder::<Postgres>::new(
                "INSERT INTO messages (content_hash_sha256, subject_raw, subject_norm, from_name, from_email, \
                 date_utc, to_raw, cc_raw, message_ids, message_id_primary, in_reply_to_ids, references_ids, \
                 mime_type, body_id) ",
            );

            insert_messages_qb.push_values(
                insert_indexes.iter().zip(inserted_bodies.iter()),
                |mut b, (idx, inserted_body)| {
                    let row = &rows[*idx];
                    b.push_bind(row.parsed_message.content_hash_sha256.as_slice())
                        .push_bind(row.parsed_message.subject_raw.as_str())
                        .push_bind(row.parsed_message.subject_norm.as_str())
                        .push_bind(row.parsed_message.from_name.as_deref())
                        .push_bind(row.parsed_message.from_email.as_str())
                        .push_bind(row.parsed_message.date_utc)
                        .push_bind(row.parsed_message.to_raw.as_deref())
                        .push_bind(row.parsed_message.cc_raw.as_deref())
                        .push_bind(&row.parsed_message.message_ids)
                        .push_bind(row.parsed_message.message_id_primary.as_str())
                        .push_bind(&row.parsed_message.in_reply_to_ids)
                        .push_bind(&row.parsed_message.references_ids)
                        .push_bind(row.parsed_message.mime_type.as_deref())
                        .push_bind(inserted_body.id);
                },
            );
            insert_messages_qb.push(" RETURNING id, content_hash_sha256");

            let inserted_messages = insert_messages_qb
                .build_query_as::<InsertedMessageRow>()
                .fetch_all(&mut *tx)
                .await?;

            for row in inserted_messages {
                inserted_messages_by_hash.insert(row.content_hash_sha256, row.id);
            }

            for idx in &insert_indexes {
                let row = &rows[*idx];
                inserted_primary_by_hash.insert(
                    row.parsed_message.content_hash_sha256.clone(),
                    row.parsed_message.message_id_primary.clone(),
                );
            }
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

            instance_rows.push((
                repo.mailing_list_id,
                message_pk,
                repo.id,
                row.git_commit_oid.clone(),
            ));
        }

        let message_id_rows = dedupe_message_id_rows(message_id_rows);
        if !message_id_rows.is_empty() {
            let mut insert_message_ids_qb = QueryBuilder::<Postgres>::new(
                "INSERT INTO message_id_map (message_id, message_pk, is_primary) ",
            );
            insert_message_ids_qb.push_values(message_id_rows.iter(), |mut b, row| {
                b.push_bind(&row.message_id)
                    .push_bind(row.message_pk)
                    .push_bind(row.is_primary);
            });
            insert_message_ids_qb.push(
                " ON CONFLICT (message_id, message_pk) \
                 DO UPDATE SET is_primary = message_id_map.is_primary OR EXCLUDED.is_primary",
            );
            insert_message_ids_qb.build().execute(&mut *tx).await?;
        }

        #[derive(sqlx::FromRow)]
        struct InsertedInstanceRow {
            git_commit_oid: String,
        }

        let mut inserted_instances = 0u64;
        if !instance_rows.is_empty() {
            let mut insert_instances_qb = QueryBuilder::<Postgres>::new(
                "INSERT INTO list_message_instances (mailing_list_id, message_pk, repo_id, git_commit_oid) ",
            );
            insert_instances_qb.push_values(instance_rows.iter(), |mut b, row| {
                b.push_bind(row.0)
                    .push_bind(row.1)
                    .push_bind(row.2)
                    .push_bind(&row.3);
            });
            insert_instances_qb
                .push(" ON CONFLICT (mailing_list_id, git_commit_oid) DO NOTHING RETURNING git_commit_oid");

            let inserted = insert_instances_qb
                .build_query_as::<InsertedInstanceRow>()
                .fetch_all(&mut *tx)
                .await?;

            inserted_instances = inserted.len() as u64;

            let inserted_commit_set: HashSet<String> =
                inserted.into_iter().map(|row| row.git_commit_oid).collect();
            for row in &instance_rows {
                if !inserted_commit_set.contains(&row.3) && skipped_commit_set.insert(row.3.clone())
                {
                    skipped_commits.push(row.3.clone());
                }
            }
        }

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
