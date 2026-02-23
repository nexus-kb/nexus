use chrono::{TimeZone, Utc};
use nexus_core::config::DatabaseConfig;
use nexus_db::{
    CatalogStore, Db, IngestStore, LineageSourceMessage, LineageStore, ParsedBodyInput,
    ParsedMessageInput,
};
use sqlx::Row;

use crate::mail::parse_email;
use crate::patch_subject::parse_patch_subject;

use super::core::build_candidates;
use super::{process_patch_enrichment_batch, process_patch_extract_threads};

#[test]
fn subject_examples_are_supported() {
    let a = parse_patch_subject("[PATCH v2 01/27] mm: reclaim");
    assert_eq!(a.version_num, 2);
    assert_eq!(a.ordinal, Some(1));
    assert_eq!(a.total, Some(27));

    let b = parse_patch_subject("[RFC PATCH 0/5] bpf: thing");
    assert!(b.is_rfc);
    assert_eq!(b.ordinal, Some(0));

    let c = parse_patch_subject("[PATCH RESEND v3 2/7] net: clean");
    assert!(c.is_resend);
    assert_eq!(c.version_num, 3);
    assert_eq!(c.ordinal, Some(2));

    let reply = parse_patch_subject("Re: [PATCH 1/1] net: clean");
    assert!(reply.had_reply_prefix);
}

#[test]
fn build_candidates_ignores_reply_subject_contamination() {
    let messages = vec![
        source_message(
            500,
            1,
            1,
            "[PATCH 0/2] bpf: series",
            "alice@example.com",
            "Mon, 01 Jan 2024 00:00:00 +0000",
            "Cover body",
            None,
        ),
        source_message(
            500,
            2,
            2,
            "[PATCH 1/2] bpf: patch one",
            "alice@example.com",
            "Mon, 01 Jan 2024 00:01:00 +0000",
            "Patch body",
            Some("diff --git a/a.c b/a.c\n--- a/a.c\n+++ b/a.c\n@@ -1 +1 @@\n-old\n+new\n"),
        ),
        source_message(
            500,
            3,
            3,
            "Re: [PATCH 1/2] bpf: patch one",
            "reviewer@example.com",
            "Mon, 01 Jan 2024 00:02:00 +0000",
            "Looks good",
            None,
        ),
        source_message(
            500,
            4,
            4,
            "Re: [PATCH 1/2] bpf: patch one",
            "reviewer2@example.com",
            "Mon, 01 Jan 2024 00:03:00 +0000",
            "Inline diff in reply",
            Some("diff --git a/a.c b/a.c\n--- a/a.c\n+++ b/a.c\n@@ -1 +1 @@\n-old\n+new-reply\n"),
        ),
        source_message(
            500,
            5,
            5,
            "[PATCH 2/2] bpf: patch two",
            "alice@example.com",
            "Mon, 01 Jan 2024 00:04:00 +0000",
            "Patch body",
            Some("diff --git a/b.c b/b.c\n--- a/b.c\n+++ b/b.c\n@@ -1 +1 @@\n-old\n+new\n"),
        ),
        source_message(
            500,
            6,
            6,
            "Re: [PATCH 2/2] bpf: patch two",
            "reviewer@example.com",
            "Mon, 01 Jan 2024 00:05:00 +0000",
            "Ack",
            None,
        ),
    ];

    let candidates = build_candidates(messages);
    assert_eq!(candidates.len(), 1);

    let candidate = &candidates[0];
    let cover_items = candidate
        .items
        .iter()
        .filter(|item| item.item_type == "cover")
        .collect::<Vec<_>>();
    assert_eq!(cover_items.len(), 1);
    assert_eq!(cover_items[0].ordinal, Some(0));
    assert_eq!(cover_items[0].message_pk, 1);

    let patch_items = candidate
        .items
        .iter()
        .filter(|item| item.item_type == "patch")
        .collect::<Vec<_>>();
    assert_eq!(patch_items.len(), 2);
    assert_eq!(patch_items[0].ordinal, Some(1));
    assert_eq!(patch_items[0].message_pk, 2);
    assert_eq!(patch_items[1].ordinal, Some(2));
    assert_eq!(patch_items[1].message_pk, 5);

    assert!(
        candidate
            .items
            .iter()
            .all(|item| !(item.item_type == "cover" && item.ordinal.unwrap_or(0) > 0))
    );
    assert!(
        candidate
            .items
            .iter()
            .all(|item| ![3, 4, 6].contains(&item.message_pk)),
        "reply messages must not be present in patch_items",
    );
}

#[tokio::test]
async fn local_db_e2e_partial_reroll_and_idempotency() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(database_url) = std::env::var("NEXUS_TEST_DATABASE_URL") else {
        return Ok(());
    };

    let db = Db::connect(&DatabaseConfig {
        url: database_url,
        max_connections: 4,
    })
    .await?;
    db.migrate().await?;

    let catalog = CatalogStore::new(db.pool().clone());
    let ingest = IngestStore::new(db.pool().clone());
    let lineage = LineageStore::new(db.pool().clone());

    let list_key = format!(
        "lineage-test-{}",
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000)
            .abs()
    );
    let list = catalog.ensure_mailing_list(&list_key).await?;
    let repo = catalog.ensure_repo(list.id, "test.git", "test.git").await?;

    let series_tag = format!("series-{}", list.id);
    let messages = fixture_messages(&series_tag);
    let mut message_pks = Vec::new();

    for (idx, raw) in messages.iter().enumerate() {
        let parsed = parse_email(raw.as_bytes())?;
        let input = ParsedMessageInput {
            content_hash_sha256: parsed.content_hash_sha256,
            subject_raw: parsed.subject_raw,
            subject_norm: parsed.subject_norm,
            from_name: parsed.from_name,
            from_email: parsed.from_email,
            date_utc: parsed.date_utc,
            to_raw: parsed.to_raw,
            cc_raw: parsed.cc_raw,
            message_ids: parsed.message_ids,
            message_id_primary: parsed.message_id_primary,
            in_reply_to_ids: parsed.in_reply_to_ids,
            references_ids: parsed.references_ids,
            mime_type: parsed.mime_type,
            body: ParsedBodyInput {
                raw_rfc822: raw.as_bytes().to_vec(),
                body_text: parsed.body_text,
                diff_text: parsed.diff_text,
                search_text: parsed.search_text,
                has_diff: parsed.has_diff,
                has_attachments: parsed.has_attachments,
            },
            patch_facts: None,
        };

        let outcome = ingest
            .ingest_message(&repo, &format!("{:040x}", idx + 1), &input)
            .await?;
        let message_pk = outcome.message_pk.expect("message pk");
        message_pks.push(message_pk);
    }

    let root_message_pk = *message_pks.first().expect("messages");
    let thread_id: i64 = sqlx::query_scalar(
        r#"INSERT INTO threads
        (mailing_list_id, root_node_key, root_message_pk, subject_norm, created_at, last_activity_at, message_count, membership_hash)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id"#,
    )
    .bind(list.id)
    .bind("lineage-test-root")
    .bind(root_message_pk)
    .bind("subsys: demo series")
    .bind(Utc.timestamp_opt(1_700_000_000, 0).single().expect("ts"))
    .bind(Utc.timestamp_opt(1_700_000_100, 0).single().expect("ts"))
    .bind(message_pks.len() as i32)
    .bind(vec![1u8; 32])
    .fetch_one(db.pool())
    .await?;

    for (idx, message_pk) in message_pks.iter().enumerate() {
        let depth = if idx == 0 { 0 } else { 1 };
        let parent = if idx == 0 {
            None
        } else {
            Some(root_message_pk)
        };
        let mut sort_key = Vec::new();
        sort_key.extend(((idx + 1) as u32).to_be_bytes());

        sqlx::query(
            r#"INSERT INTO thread_messages
            (mailing_list_id, thread_id, message_pk, parent_message_pk, depth, sort_key, is_dummy)
            VALUES ($1, $2, $3, $4, $5, $6, false)"#,
        )
        .bind(list.id)
        .bind(thread_id)
        .bind(*message_pk)
        .bind(parent)
        .bind(depth)
        .bind(sort_key)
        .execute(db.pool())
        .await?;
    }

    let first = process_patch_extract_threads(&lineage, list.id, &[thread_id]).await?;
    let series_subjects: Vec<String> = sqlx::query_scalar(
        "SELECT canonical_subject_norm FROM patch_series WHERE id = ANY($1) ORDER BY id ASC",
    )
    .bind(&first.series_ids)
    .fetch_all(db.pool())
    .await?;
    assert_eq!(
        first.series_ids.len(),
        1,
        "series ids: {:?}, subjects: {:?}",
        first.series_ids,
        series_subjects
    );
    assert!(!first.patch_item_ids.is_empty());

    let enrichment_outcome =
        process_patch_enrichment_batch(&lineage, &first.patch_item_ids).await?;
    assert!(enrichment_outcome.patch_items_hydrated == 0);
    assert!(enrichment_outcome.patch_items_fallback_patch_id >= 1);
    assert!(enrichment_outcome.patch_items_fallback_diff_parse >= 1);
    assert!(enrichment_outcome.patch_item_files_written >= 1);

    let series_id = first.series_ids[0];
    let series_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM patch_series WHERE id = $1")
        .bind(series_id)
        .fetch_one(db.pool())
        .await?;
    assert_eq!(series_count, 1);

    let version_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM patch_series_versions WHERE patch_series_id = $1")
            .bind(series_id)
            .fetch_one(db.pool())
            .await?;
    assert_eq!(version_count, 2);

    let partial_reroll: bool = sqlx::query_scalar(
        r#"SELECT is_partial_reroll
        FROM patch_series_versions
        WHERE patch_series_id = $1 AND version_num = 2
        ORDER BY id DESC
        LIMIT 1"#,
    )
    .bind(series_id)
    .fetch_one(db.pool())
    .await?;
    assert!(partial_reroll);

    let assembled_rows = sqlx::query(
        r#"SELECT psvai.ordinal, psvai.inherited_from_version_num
        FROM patch_series_versions psv
        JOIN patch_series_version_assembled_items psvai
          ON psvai.patch_series_version_id = psv.id
        WHERE psv.patch_series_id = $1
          AND psv.version_num = 2
        ORDER BY psvai.ordinal ASC"#,
    )
    .bind(series_id)
    .fetch_all(db.pool())
    .await?;

    let ordinals: Vec<i32> = assembled_rows
        .iter()
        .map(|row| row.get::<i32, _>(0))
        .collect();
    assert_eq!(ordinals, vec![0, 1, 2, 3]);
    let inherited_count = assembled_rows
        .iter()
        .filter(|row| row.get::<Option<i32>, _>(1).is_some())
        .count();
    assert!(inherited_count >= 2);

    let logical_versions_count: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*)
        FROM patch_logical_versions plv
        JOIN patch_logical pl
          ON pl.id = plv.patch_logical_id
        WHERE pl.patch_series_id = $1"#,
    )
    .bind(series_id)
    .fetch_one(db.pool())
    .await?;
    assert!(logical_versions_count >= 6);

    let patch_with_ids: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*)
        FROM patch_items pi
        JOIN patch_series_versions psv
          ON psv.id = pi.patch_series_version_id
        WHERE psv.patch_series_id = $1
          AND pi.item_type = 'patch'
          AND pi.patch_id_stable IS NOT NULL"#,
    )
    .bind(series_id)
    .fetch_one(db.pool())
    .await?;
    assert_eq!(patch_with_ids, 4);

    let bad_cover_ordinals: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*)
        FROM patch_items pi
        JOIN patch_series_versions psv
          ON psv.id = pi.patch_series_version_id
        WHERE psv.patch_series_id = $1
          AND pi.item_type = 'cover'
          AND pi.ordinal > 0"#,
    )
    .bind(series_id)
    .fetch_one(db.pool())
    .await?;
    assert_eq!(bad_cover_ordinals, 0);

    let patch_item_files_count: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*)
        FROM patch_item_files pif
        JOIN patch_items pi ON pi.id = pif.patch_item_id
        JOIN patch_series_versions psv ON psv.id = pi.patch_series_version_id
        WHERE psv.patch_series_id = $1"#,
    )
    .bind(series_id)
    .fetch_one(db.pool())
    .await?;
    assert!(patch_item_files_count >= 4);

    let file_slices = sqlx::query(
        r#"SELECT pif.diff_start, pif.diff_end, mb.diff_text
        FROM patch_item_files pif
        JOIN patch_items pi ON pi.id = pif.patch_item_id
        JOIN messages m ON m.id = pi.message_pk
        JOIN message_bodies mb ON mb.id = m.body_id
        JOIN patch_series_versions psv ON psv.id = pi.patch_series_version_id
        WHERE psv.patch_series_id = $1"#,
    )
    .bind(series_id)
    .fetch_all(db.pool())
    .await?;
    for row in &file_slices {
        let start = row.get::<i32, _>(0) as usize;
        let end = row.get::<i32, _>(1) as usize;
        let diff_text = row.get::<Option<String>, _>(2).unwrap_or_default();
        assert!(start < end);
        assert!(end <= diff_text.len());
        let slice = &diff_text[start..end];
        assert!(slice.starts_with("diff --git "));
    }

    let stats_sum: (i64, i64, i64, i64) = sqlx::query_as::<_, (i64, i64, i64, i64)>(
        r#"SELECT
            COALESCE(SUM(file_count)::bigint, 0) AS file_count_sum,
            COALESCE(SUM(additions)::bigint, 0) AS additions_sum,
            COALESCE(SUM(deletions)::bigint, 0) AS deletions_sum,
            COALESCE(SUM(hunk_count)::bigint, 0) AS hunk_count_sum
        FROM patch_items pi
        JOIN patch_series_versions psv
          ON psv.id = pi.patch_series_version_id
        WHERE psv.patch_series_id = $1
          AND pi.item_type = 'patch'"#,
    )
    .bind(series_id)
    .fetch_one(db.pool())
    .await?;
    assert!(stats_sum.0 >= 4);
    assert!(stats_sum.1 >= 4);
    assert!(stats_sum.2 >= 4);
    assert!(stats_sum.3 >= 4);

    let before_counts = snapshot_counts(db.pool(), series_id).await?;
    let _second = process_patch_extract_threads(&lineage, list.id, &[thread_id]).await?;
    let _second_enrichment =
        process_patch_enrichment_batch(&lineage, &first.patch_item_ids).await?;
    let after_counts = snapshot_counts(db.pool(), series_id).await?;
    assert_eq!(before_counts, after_counts);

    Ok(())
}

async fn snapshot_counts(
    pool: &sqlx::PgPool,
    series_id: i64,
) -> Result<(i64, i64, i64, i64, i64), sqlx::Error> {
    let versions =
        sqlx::query_scalar("SELECT COUNT(*) FROM patch_series_versions WHERE patch_series_id = $1")
            .bind(series_id)
            .fetch_one(pool)
            .await?;
    let items = sqlx::query_scalar(
        r#"SELECT COUNT(*)
        FROM patch_items pi
        JOIN patch_series_versions psv
          ON psv.id = pi.patch_series_version_id
        WHERE psv.patch_series_id = $1"#,
    )
    .bind(series_id)
    .fetch_one(pool)
    .await?;
    let assembled = sqlx::query_scalar(
        r#"SELECT COUNT(*)
        FROM patch_series_version_assembled_items psvai
        JOIN patch_series_versions psv
          ON psv.id = psvai.patch_series_version_id
        WHERE psv.patch_series_id = $1"#,
    )
    .bind(series_id)
    .fetch_one(pool)
    .await?;
    let logical_versions = sqlx::query_scalar(
        r#"SELECT COUNT(*)
        FROM patch_logical_versions plv
        JOIN patch_logical pl
          ON pl.id = plv.patch_logical_id
        WHERE pl.patch_series_id = $1"#,
    )
    .bind(series_id)
    .fetch_one(pool)
    .await?;
    let files = sqlx::query_scalar(
        r#"SELECT COUNT(*)
        FROM patch_item_files pif
        JOIN patch_items pi
          ON pi.id = pif.patch_item_id
        JOIN patch_series_versions psv
          ON psv.id = pi.patch_series_version_id
        WHERE psv.patch_series_id = $1"#,
    )
    .bind(series_id)
    .fetch_one(pool)
    .await?;

    Ok((versions, items, assembled, logical_versions, files))
}

fn fixture_messages(series_tag: &str) -> Vec<String> {
    let series_subject = format!("subsys: demo series {series_tag}");
    vec![
        cover_mail(
            "m1@example.com",
            "Mon, 01 Jan 2024 00:00:00 +0000",
            &format!("[PATCH 0/3] {series_subject}"),
            None,
        ),
        patch_mail(
            "m2@example.com",
            "Mon, 01 Jan 2024 00:01:00 +0000",
            "[PATCH 1/3] subsys: part one",
            "foo.c",
            "old1",
            "new1",
            None,
        ),
        reply_mail(
            "m2-reply@example.com",
            "Mon, 01 Jan 2024 00:01:30 +0000",
            "Re: [PATCH 1/3] subsys: part one",
            Some("<m2@example.com>"),
            None,
        ),
        reply_mail(
            "m2-reply-diff@example.com",
            "Mon, 01 Jan 2024 00:01:45 +0000",
            "Re: [PATCH 1/3] subsys: part one",
            Some("<m2@example.com>"),
            Some(
                "diff --git a/foo.c b/foo.c\r\n--- a/foo.c\r\n+++ b/foo.c\r\n@@ -1 +1 @@\r\n-old1\r\n+reply-change\r\n",
            ),
        ),
        patch_mail(
            "m3@example.com",
            "Mon, 01 Jan 2024 00:02:00 +0000",
            "[PATCH 2/3] subsys: part two",
            "bar.c",
            "old2",
            "new2",
            None,
        ),
        patch_mail(
            "m4@example.com",
            "Mon, 01 Jan 2024 00:03:00 +0000",
            "[PATCH 3/3] subsys: part three",
            "baz.c",
            "old3",
            "new3",
            None,
        ),
        cover_mail(
            "m5@example.com",
            "Tue, 02 Jan 2024 00:00:00 +0000",
            &format!("[PATCH v2 0/3] {series_subject}"),
            Some("<m1@example.com>"),
        ),
        patch_mail(
            "m6@example.com",
            "Tue, 02 Jan 2024 00:01:00 +0000",
            "[PATCH v2 2/3] subsys: part two",
            "bar.c",
            "old2",
            "new2-v2",
            Some("<m5@example.com>"),
        ),
    ]
}

#[allow(clippy::too_many_arguments)]
fn source_message(
    thread_id: i64,
    sort_ordinal: u32,
    message_pk: i64,
    subject_raw: &str,
    from_email: &str,
    date_raw: &str,
    body_text: &str,
    diff_text: Option<&str>,
) -> LineageSourceMessage {
    let date_utc = chrono::DateTime::parse_from_rfc2822(date_raw)
        .expect("valid rfc2822 date")
        .with_timezone(&Utc);
    LineageSourceMessage {
        thread_id,
        message_pk,
        sort_key: sort_ordinal.to_be_bytes().to_vec(),
        message_id_primary: format!("m{message_pk}@example.com"),
        subject_raw: subject_raw.to_string(),
        subject_norm: subject_raw.to_ascii_lowercase(),
        from_name: None,
        from_email: from_email.to_string(),
        date_utc: Some(date_utc),
        references_ids: Vec::new(),
        in_reply_to_ids: Vec::new(),
        base_commit: None,
        change_id: None,
        has_diff: diff_text.is_some(),
        patch_id_stable: None,
        body_text: Some(body_text.to_string()),
        diff_text: diff_text.map(str::to_string),
    }
}

fn cover_mail(message_id: &str, date: &str, subject: &str, references: Option<&str>) -> String {
    let mut headers = vec![
        "From: Alice <alice@example.com>".to_string(),
        format!("Message-ID: <{message_id}>"),
        format!("Date: {date}"),
        format!("Subject: {subject}"),
        "Content-Type: text/plain; charset=utf-8".to_string(),
    ];
    if let Some(reference) = references {
        headers.push(format!("References: {reference}"));
    }

    format!("{}\r\n\r\nCover letter body\r\n", headers.join("\r\n"))
}

fn patch_mail(
    message_id: &str,
    date: &str,
    subject: &str,
    file: &str,
    old: &str,
    new: &str,
    references: Option<&str>,
) -> String {
    let mut headers = vec![
        "From: Alice <alice@example.com>".to_string(),
        format!("Message-ID: <{message_id}>"),
        format!("Date: {date}"),
        format!("Subject: {subject}"),
        "Content-Type: text/plain; charset=utf-8".to_string(),
    ];
    if let Some(reference) = references {
        headers.push(format!("References: {reference}"));
        headers.push(format!("In-Reply-To: {reference}"));
    }

    let body = format!(
        concat!(
            "From deadbeef Mon Sep 17 00:00:00 2001\r\n",
            "Subject: {subject}\r\n",
            "\r\n",
            "Patch body\r\n",
            "\r\n",
            "---\r\n",
            " {file} | 2 +-\r\n",
            " 1 file changed, 1 insertion(+), 1 deletion(-)\r\n",
            "\r\n",
            "diff --git a/{file} b/{file}\r\n",
            "index 1111111..2222222 100644\r\n",
            "--- a/{file}\r\n",
            "+++ b/{file}\r\n",
            "@@ -1 +1 @@\r\n",
            "-{old}\r\n",
            "+{new}\r\n"
        ),
        subject = subject,
        file = file,
        old = old,
        new = new,
    );

    format!("{}\r\n\r\n{}", headers.join("\r\n"), body)
}

fn reply_mail(
    message_id: &str,
    date: &str,
    subject: &str,
    references: Option<&str>,
    inline_diff: Option<&str>,
) -> String {
    let mut headers = vec![
        "From: Reviewer <reviewer@example.com>".to_string(),
        format!("Message-ID: <{message_id}>"),
        format!("Date: {date}"),
        format!("Subject: {subject}"),
        "Content-Type: text/plain; charset=utf-8".to_string(),
    ];
    if let Some(reference) = references {
        headers.push(format!("References: {reference}"));
        headers.push(format!("In-Reply-To: {reference}"));
    }

    let body = match inline_diff {
        Some(diff) => format!("Review comments\r\n\r\n{diff}"),
        None => "Review comments only\r\n".to_string(),
    };

    format!("{}\r\n\r\n{}", headers.join("\r\n"), body)
}
