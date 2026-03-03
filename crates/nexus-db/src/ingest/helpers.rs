use super::*;

fn usize_to_i64(value: usize, field_name: &str) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| protocol_error(format!("{field_name} exceeds i64::MAX: {value}")))
}

pub(super) async fn reserve_ids(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    sequence_name: &str,
    count: usize,
) -> Result<Vec<i64>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    sqlx::query_scalar::<_, i64>(
        "SELECT nextval($1::regclass)::bigint FROM generate_series(1, $2::bigint)",
    )
    .bind(sequence_name)
    .bind(usize_to_i64(count, "reserve_ids count")?)
    .fetch_all(&mut **tx)
    .await
}

pub(super) async fn copy_into_table(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    statement: &str,
    payload: Vec<u8>,
) -> Result<u64> {
    if payload.is_empty() {
        return Ok(0);
    }

    let mut copy = tx.copy_in_raw(statement).await?;
    copy.send(payload).await?;
    copy.finish().await
}

pub(super) fn build_message_bodies_copy_payload(rows: &[CopyMessageBodyRow]) -> Result<Vec<u8>> {
    let mut writer = make_copy_writer();
    for row in rows {
        let id = row.id.to_string();
        write_copy_record(
            &mut writer,
            [
                Some(Cow::Owned(id)),
                row.body_text.as_deref().map(Cow::Borrowed),
                row.diff_text.as_deref().map(Cow::Borrowed),
                Some(Cow::Borrowed(row.search_text.as_str())),
                Some(Cow::Borrowed(encode_bool(row.has_diff))),
                Some(Cow::Borrowed(encode_bool(row.has_attachments))),
            ],
        )?;
    }
    finish_copy_payload(writer)
}

pub(super) fn build_messages_copy_payload(rows: &[CopyMessageRow]) -> Result<Vec<u8>> {
    let mut writer = make_copy_writer();
    for row in rows {
        let id = row.id.to_string();
        let content_hash = encode_bytea_hex(&row.content_hash_sha256);
        let date_utc = encode_nullable_datetime(row.date_utc);
        let message_ids = encode_text_array(&row.message_ids);
        let in_reply_to_ids = encode_text_array(&row.in_reply_to_ids);
        let references_ids = encode_text_array(&row.references_ids);
        let body_id = row.body_id.to_string();
        write_copy_record(
            &mut writer,
            [
                Some(Cow::Owned(id)),
                Some(Cow::Owned(content_hash)),
                Some(Cow::Borrowed(row.subject_raw.as_str())),
                Some(Cow::Borrowed(row.subject_norm.as_str())),
                row.from_name.as_deref().map(Cow::Borrowed),
                Some(Cow::Borrowed(row.from_email.as_str())),
                date_utc.as_deref().map(Cow::Borrowed),
                row.to_raw.as_deref().map(Cow::Borrowed),
                row.cc_raw.as_deref().map(Cow::Borrowed),
                Some(Cow::Owned(message_ids)),
                Some(Cow::Borrowed(row.message_id_primary.as_str())),
                Some(Cow::Owned(in_reply_to_ids)),
                Some(Cow::Owned(references_ids)),
                row.mime_type.as_deref().map(Cow::Borrowed),
                Some(Cow::Owned(body_id)),
            ],
        )?;
    }
    finish_copy_payload(writer)
}

pub(super) fn build_message_id_copy_payload(rows: &[CopyMessageIdRow]) -> Result<Vec<u8>> {
    let mut writer = make_copy_writer();
    for row in rows {
        let message_pk = row.message_pk.to_string();
        write_copy_record(
            &mut writer,
            [
                Some(Cow::Borrowed(row.message_id.as_str())),
                Some(Cow::Owned(message_pk)),
                Some(Cow::Borrowed(encode_bool(row.is_primary))),
            ],
        )?;
    }
    finish_copy_payload(writer)
}

pub(super) fn build_instances_copy_payload(rows: &[CopyInstanceRow]) -> Result<Vec<u8>> {
    let mut writer = make_copy_writer();
    for row in rows {
        let mailing_list_id = row.mailing_list_id.to_string();
        let message_pk = row.message_pk.to_string();
        let repo_id = row.repo_id.to_string();
        write_copy_record(
            &mut writer,
            [
                Some(Cow::Owned(mailing_list_id)),
                Some(Cow::Owned(message_pk)),
                Some(Cow::Owned(repo_id)),
                Some(Cow::Borrowed(row.git_commit_oid.as_str())),
            ],
        )?;
    }
    finish_copy_payload(writer)
}

pub(super) fn make_copy_writer() -> csv::Writer<Vec<u8>> {
    WriterBuilder::new()
        .has_headers(false)
        .quote_style(QuoteStyle::Always)
        .terminator(Terminator::Any(b'\n'))
        .from_writer(Vec::new())
}

pub(super) fn write_copy_record<'a, const N: usize>(
    writer: &mut csv::Writer<Vec<u8>>,
    fields: [Option<Cow<'a, str>>; N],
) -> Result<()> {
    let encoded_fields: Vec<Cow<'a, str>> = fields
        .into_iter()
        .map(|field| field.unwrap_or(Cow::Borrowed(COPY_NULL_SENTINEL)))
        .collect();
    writer
        .write_record(encoded_fields.iter().map(|value| value.as_ref()))
        .map_err(|err| protocol_error(format!("failed to encode COPY row as CSV: {err}")))?;
    Ok(())
}

pub(super) fn finish_copy_payload(writer: csv::Writer<Vec<u8>>) -> Result<Vec<u8>> {
    let payload = writer.into_inner().map_err(|err| {
        protocol_error(format!("failed to finalize COPY payload: {}", err.error()))
    })?;
    let quoted_sentinel = format!("\"{COPY_NULL_SENTINEL}\"");
    let payload = String::from_utf8(payload)
        .map_err(|err| protocol_error(format!("COPY payload is not UTF-8: {err}")))?;
    Ok(payload.replace(&quoted_sentinel, "\\N").into_bytes())
}

pub(super) fn protocol_error(message: String) -> sqlx::Error {
    sqlx::Error::Protocol(message)
}

pub(super) fn encode_nullable_datetime(value: Option<DateTime<Utc>>) -> Option<String> {
    value.map(|v| v.to_rfc3339())
}

pub(super) fn encode_bool(value: bool) -> &'static str {
    if value { "t" } else { "f" }
}

pub(super) fn encode_bytea_hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2 + 2);
    text.push('\\');
    text.push('x');
    for byte in bytes {
        let _ = write!(&mut text, "{byte:02x}");
    }
    text
}

pub(super) fn encode_text_array(values: &[String]) -> String {
    if values.is_empty() {
        return "{}".to_string();
    }

    let mut out = String::from("{");
    for (idx, value) in values.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push('"');
        for ch in value.chars() {
            if ch == '\\' || ch == '"' {
                out.push('\\');
            }
            out.push(ch);
        }
        out.push('"');
    }
    out.push('}');
    out
}

pub(super) async fn validate_text_integrity(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    message_pks: &[i64],
) -> Result<()> {
    if message_pks.is_empty() {
        return Ok(());
    }

    let mut sample = message_pks.to_vec();
    sample.sort_unstable();
    sample.dedup();
    if sample.len() > UTF8_INTEGRITY_SAMPLE_LIMIT {
        sample.truncate(UTF8_INTEGRITY_SAMPLE_LIMIT);
    }

    let probe_error = match run_text_integrity_probe(tx, &sample).await {
        Ok(()) => return Ok(()),
        Err(err) => err,
    };

    let offenders =
        collect_text_integrity_offenders(tx, &sample, UTF8_INTEGRITY_OFFENDER_LIMIT).await?;
    let offenders_text = if offenders.is_empty() {
        "none".to_string()
    } else {
        offenders
            .iter()
            .map(|(message_pk, body_id)| format!("message_pk={message_pk} body_id={body_id:?}"))
            .collect::<Vec<_>>()
            .join(", ")
    };

    Err(protocol_error(format!(
        "utf8 integrity validation failed: stage=text_integrity_probe sampled_ids={} offenders=[{}] probe_error={}",
        sample.len(),
        offenders_text,
        probe_error
    )))
}

pub(super) async fn run_text_integrity_probe(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    message_pks: &[i64],
) -> Result<()> {
    sqlx::query("SAVEPOINT copy_validate_probe")
        .execute(&mut **tx)
        .await?;

    let probe = sqlx::query(
        r#"SELECT
            m.id,
            m.body_id,
            LENGTH(nexus_safe_prefix(mb.search_text, 256)),
            LENGTH(nexus_safe_prefix(m.from_email, 128)),
            LENGTH(nexus_safe_prefix(m.subject_norm, 256))
        FROM messages m
        JOIN message_bodies mb
          ON mb.id = m.body_id
        WHERE m.id = ANY($1)
        ORDER BY m.id
        LIMIT $2"#,
    )
    .bind(message_pks)
    .bind(usize_to_i64(
        message_pks.len(),
        "text_integrity_probe sample size",
    )?)
    .fetch_all(&mut **tx)
    .await;

    match probe {
        Ok(_) => {
            sqlx::query("RELEASE SAVEPOINT copy_validate_probe")
                .execute(&mut **tx)
                .await?;
            Ok(())
        }
        Err(err) => {
            sqlx::query("ROLLBACK TO SAVEPOINT copy_validate_probe")
                .execute(&mut **tx)
                .await?;
            sqlx::query("RELEASE SAVEPOINT copy_validate_probe")
                .execute(&mut **tx)
                .await?;
            Err(err)
        }
    }
}

pub(super) async fn collect_text_integrity_offenders(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    message_pks: &[i64],
    max_offenders: usize,
) -> Result<Vec<(i64, Option<i64>)>> {
    if max_offenders == 0 {
        return Ok(Vec::new());
    }

    let mut offenders = Vec::new();
    for message_pk in message_pks {
        if offenders.len() >= max_offenders {
            break;
        }

        sqlx::query("SAVEPOINT copy_validate_row")
            .execute(&mut **tx)
            .await?;
        let check_result = sqlx::query(
            r#"SELECT
                LENGTH(nexus_safe_prefix(mb.search_text, 256)),
                LENGTH(nexus_safe_prefix(m.from_email, 128)),
                LENGTH(nexus_safe_prefix(m.subject_norm, 256))
            FROM messages m
            JOIN message_bodies mb
              ON mb.id = m.body_id
            WHERE m.id = $1"#,
        )
        .bind(*message_pk)
        .fetch_optional(&mut **tx)
        .await;

        match check_result {
            Ok(_) => {
                sqlx::query("RELEASE SAVEPOINT copy_validate_row")
                    .execute(&mut **tx)
                    .await?;
            }
            Err(_) => {
                sqlx::query("ROLLBACK TO SAVEPOINT copy_validate_row")
                    .execute(&mut **tx)
                    .await?;
                sqlx::query("RELEASE SAVEPOINT copy_validate_row")
                    .execute(&mut **tx)
                    .await?;
                let body_id =
                    sqlx::query_scalar::<_, i64>("SELECT body_id FROM messages WHERE id = $1")
                        .bind(*message_pk)
                        .fetch_optional(&mut **tx)
                        .await?;
                offenders.push((*message_pk, body_id));
            }
        }
    }

    Ok(offenders)
}

pub(super) fn dedupe_message_id_rows(rows: Vec<(String, i64, bool)>) -> Vec<CopyMessageIdRow> {
    let mut deduped = HashMap::<(String, i64), bool>::new();
    for (message_id, message_pk, is_primary) in rows {
        let entry = deduped.entry((message_id, message_pk)).or_insert(false);
        *entry = *entry || is_primary;
    }

    deduped
        .into_iter()
        .map(|((message_id, message_pk), is_primary)| CopyMessageIdRow {
            message_id,
            message_pk,
            is_primary,
        })
        .collect()
}

pub(super) async fn load_existing_message_id_rows(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    rows: &[CopyMessageIdRow],
) -> Result<Vec<ExistingMessageIdRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut found = Vec::new();
    for chunk in rows.chunks(3000) {
        let mut qb = QueryBuilder::<Postgres>::new(
            "SELECT message_id, message_pk, is_primary FROM message_id_map WHERE (message_id, message_pk) IN (",
        );
        for (idx, row) in chunk.iter().enumerate() {
            if idx > 0 {
                qb.push(", ");
            }
            qb.push("(")
                .push_bind(&row.message_id)
                .push(", ")
                .push_bind(row.message_pk)
                .push(")");
        }
        qb.push(")");

        let mut existing = qb
            .build_query_as::<ExistingMessageIdRow>()
            .fetch_all(&mut **tx)
            .await?;
        found.append(&mut existing);
    }

    Ok(found)
}

pub(super) async fn promote_message_id_rows(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    rows: &[(String, i64)],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    for chunk in rows.chunks(3000) {
        let mut qb = QueryBuilder::<Postgres>::new(
            "UPDATE message_id_map SET is_primary = true WHERE is_primary = false AND (message_id, message_pk) IN (",
        );
        for (idx, (message_id, message_pk)) in chunk.iter().enumerate() {
            if idx > 0 {
                qb.push(", ");
            }
            qb.push("(")
                .push_bind(message_id)
                .push(", ")
                .push_bind(*message_pk)
                .push(")");
        }
        qb.push(")");
        qb.build().execute(&mut **tx).await?;
    }

    Ok(())
}

pub(super) fn collect_patch_facts_by_message_pk(
    rows: &[IngestCommitRow],
    pending_indexes: &[usize],
    message_pk_by_commit: &HashMap<String, i64>,
) -> HashMap<i64, ParsedPatchFactsInput> {
    let mut facts_by_message_pk = HashMap::new();
    for idx in pending_indexes {
        let row = &rows[*idx];
        let Some(message_pk) = message_pk_by_commit.get(&row.git_commit_oid).copied() else {
            continue;
        };
        let Some(facts) = row.parsed_message.patch_facts.as_ref() else {
            continue;
        };

        facts_by_message_pk
            .entry(message_pk)
            .or_insert_with(|| facts.clone());
    }
    facts_by_message_pk
}

pub(super) fn merge_patch_file_fact_row(
    existing: &mut ParsedPatchFileFactInput,
    incoming: &ParsedPatchFileFactInput,
) {
    if existing.old_path.is_none() {
        existing.old_path = incoming.old_path.clone();
    }
    existing.is_binary |= incoming.is_binary;
    existing.additions = existing.additions.saturating_add(incoming.additions);
    existing.deletions = existing.deletions.saturating_add(incoming.deletions);
    existing.hunk_count = existing.hunk_count.saturating_add(incoming.hunk_count);
    existing.diff_start = existing.diff_start.min(incoming.diff_start);
    existing.diff_end = existing.diff_end.max(incoming.diff_end);
    if existing.change_type == "M" {
        existing.change_type = incoming.change_type.clone();
    }
}

pub(super) async fn upsert_message_patch_facts(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    message_pk: i64,
    facts: &ParsedPatchFactsInput,
) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO message_patch_facts
           (message_pk, has_diff, patch_id_stable, base_commit, change_id, file_count, additions, deletions, hunk_count)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           ON CONFLICT (message_pk)
           DO UPDATE SET has_diff = EXCLUDED.has_diff,
                         patch_id_stable = COALESCE(EXCLUDED.patch_id_stable, message_patch_facts.patch_id_stable),
                         base_commit = COALESCE(EXCLUDED.base_commit, message_patch_facts.base_commit),
                         change_id = COALESCE(EXCLUDED.change_id, message_patch_facts.change_id),
                         file_count = EXCLUDED.file_count,
                         additions = EXCLUDED.additions,
                         deletions = EXCLUDED.deletions,
                         hunk_count = EXCLUDED.hunk_count,
                         updated_at = now()"#,
    )
    .bind(message_pk)
    .bind(facts.has_diff)
    .bind(&facts.patch_id_stable)
    .bind(&facts.base_commit)
    .bind(&facts.change_id)
    .bind(facts.file_count)
    .bind(facts.additions)
    .bind(facts.deletions)
    .bind(facts.hunk_count)
    .execute(&mut **tx)
    .await?;

    sqlx::query("DELETE FROM message_patch_file_facts WHERE message_pk = $1")
        .bind(message_pk)
        .execute(&mut **tx)
        .await?;

    if facts.files.is_empty() {
        return Ok(());
    }

    let mut qb: QueryBuilder<'_, Postgres> = QueryBuilder::new(
        "INSERT INTO message_patch_file_facts \
         (message_pk, old_path, new_path, change_type, is_binary, additions, deletions, hunk_count, diff_start, diff_end) ",
    );
    qb.push_values(facts.files.iter(), |mut b, file| {
        b.push_bind(message_pk)
            .push_bind(&file.old_path)
            .push_bind(&file.new_path)
            .push_bind(&file.change_type)
            .push_bind(file.is_binary)
            .push_bind(file.additions)
            .push_bind(file.deletions)
            .push_bind(file.hunk_count)
            .push_bind(file.diff_start)
            .push_bind(file.diff_end);
    });
    qb.push(
        " ON CONFLICT (message_pk, new_path) DO UPDATE SET \
          old_path = EXCLUDED.old_path, \
          change_type = EXCLUDED.change_type, \
          is_binary = EXCLUDED.is_binary, \
          additions = EXCLUDED.additions, \
          deletions = EXCLUDED.deletions, \
          hunk_count = EXCLUDED.hunk_count, \
          diff_start = EXCLUDED.diff_start, \
          diff_end = EXCLUDED.diff_end",
    );
    qb.build().execute(&mut **tx).await?;

    Ok(())
}

pub(super) async fn upsert_message_patch_facts_batch(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    facts_by_message_pk: &HashMap<i64, ParsedPatchFactsInput>,
) -> Result<()> {
    if facts_by_message_pk.is_empty() {
        return Ok(());
    }

    let mut message_fact_rows = facts_by_message_pk
        .iter()
        .map(|(message_pk, facts)| (*message_pk, facts))
        .collect::<Vec<_>>();
    message_fact_rows.sort_by_key(|(message_pk, _)| *message_pk);

    const MESSAGE_FACT_BINDS_PER_ROW: usize = 9;
    let rows_per_chunk = (MAX_QUERY_BIND_PARAMS / MESSAGE_FACT_BINDS_PER_ROW).max(1);

    for chunk in message_fact_rows.chunks(rows_per_chunk) {
        let mut qb: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "INSERT INTO message_patch_facts \
             (message_pk, has_diff, patch_id_stable, base_commit, change_id, file_count, additions, deletions, hunk_count) ",
        );
        qb.push_values(chunk.iter(), |mut b, (message_pk, facts)| {
            b.push_bind(*message_pk)
                .push_bind(facts.has_diff)
                .push_bind(&facts.patch_id_stable)
                .push_bind(&facts.base_commit)
                .push_bind(&facts.change_id)
                .push_bind(facts.file_count)
                .push_bind(facts.additions)
                .push_bind(facts.deletions)
                .push_bind(facts.hunk_count);
        });
        qb.push(
            " ON CONFLICT (message_pk) DO UPDATE SET \
              has_diff = EXCLUDED.has_diff, \
              patch_id_stable = COALESCE(EXCLUDED.patch_id_stable, message_patch_facts.patch_id_stable), \
              base_commit = COALESCE(EXCLUDED.base_commit, message_patch_facts.base_commit), \
              change_id = COALESCE(EXCLUDED.change_id, message_patch_facts.change_id), \
              file_count = EXCLUDED.file_count, \
              additions = EXCLUDED.additions, \
              deletions = EXCLUDED.deletions, \
              hunk_count = EXCLUDED.hunk_count, \
              updated_at = now()",
        );
        qb.build().execute(&mut **tx).await?;
    }

    let message_pks: Vec<i64> = message_fact_rows
        .iter()
        .map(|(message_pk, _)| *message_pk)
        .collect();
    sqlx::query("DELETE FROM message_patch_file_facts WHERE message_pk = ANY($1)")
        .bind(&message_pks)
        .execute(&mut **tx)
        .await?;

    let mut deduped_file_rows = HashMap::<(i64, String), ParsedPatchFileFactInput>::new();
    for (message_pk, facts) in message_fact_rows {
        for file in &facts.files {
            let key = (message_pk, file.new_path.clone());
            if let Some(existing) = deduped_file_rows.get_mut(&key) {
                merge_patch_file_fact_row(existing, file);
            } else {
                deduped_file_rows.insert(key, file.clone());
            }
        }
    }

    if deduped_file_rows.is_empty() {
        return Ok(());
    }

    let mut file_rows = deduped_file_rows
        .into_iter()
        .map(|((message_pk, new_path), file)| (message_pk, new_path, file))
        .collect::<Vec<_>>();
    file_rows.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));

    const FILE_FACT_BINDS_PER_ROW: usize = 10;
    let file_rows_per_chunk = (MAX_QUERY_BIND_PARAMS / FILE_FACT_BINDS_PER_ROW).max(1);

    for chunk in file_rows.chunks(file_rows_per_chunk) {
        let mut qb: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "INSERT INTO message_patch_file_facts \
             (message_pk, old_path, new_path, change_type, is_binary, additions, deletions, hunk_count, diff_start, diff_end) ",
        );
        qb.push_values(chunk.iter(), |mut b, row| {
            b.push_bind(row.0)
                .push_bind(&row.2.old_path)
                .push_bind(&row.2.new_path)
                .push_bind(&row.2.change_type)
                .push_bind(row.2.is_binary)
                .push_bind(row.2.additions)
                .push_bind(row.2.deletions)
                .push_bind(row.2.hunk_count)
                .push_bind(row.2.diff_start)
                .push_bind(row.2.diff_end);
        });
        qb.push(
            " ON CONFLICT (message_pk, new_path) DO UPDATE SET \
              old_path = EXCLUDED.old_path, \
              change_type = EXCLUDED.change_type, \
              is_binary = EXCLUDED.is_binary, \
              additions = EXCLUDED.additions, \
              deletions = EXCLUDED.deletions, \
              hunk_count = EXCLUDED.hunk_count, \
              diff_start = EXCLUDED.diff_start, \
              diff_end = EXCLUDED.diff_end",
        );
        qb.build().execute(&mut **tx).await?;
    }

    Ok(())
}
