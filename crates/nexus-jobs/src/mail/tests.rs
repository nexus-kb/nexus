use super::{
    ContentHashInput, ParseEmailError, compute_content_hash, is_disallowed_char, limit_for_search,
    normalize_message_id_token, parse_email, sanitize_text,
};
use chrono::{Duration, Utc};

#[test]
fn hash_normalizes_line_endings_and_trailing_whitespace() {
    let h1 = compute_content_hash(ContentHashInput {
        subject: "[PATCH] Test",
        from: "Alice <alice@example.com>",
        date: "Mon, 01 Jan 2024 00:00:00 +0000",
        references_ids: &[],
        in_reply_to_ids: &[],
        to_raw: "list@example.com",
        cc_raw: "",
        body: "line one\r\nline two   \r\n",
    });

    let h2 = compute_content_hash(ContentHashInput {
        subject: "[PATCH] Test",
        from: "Alice <alice@example.com>",
        date: "Mon, 01 Jan 2024 00:00:00 +0000",
        references_ids: &[],
        in_reply_to_ids: &[],
        to_raw: "list@example.com",
        cc_raw: "",
        body: "line one\nline two\n",
    });

    assert_eq!(h1, h2);
}

#[test]
fn message_id_normalization_handles_quotes_and_brackets() {
    assert_eq!(
        normalize_message_id_token("\"<Foo@Example.COM>\""),
        Some("foo@example.com".to_string())
    );
    assert_eq!(
        normalize_message_id_token("  <bar@example.com>  "),
        Some("bar@example.com".to_string())
    );
}

#[test]
fn parser_detects_patch_attachment() {
    let raw = concat!(
        "From: Alice <alice@example.com>\r\n",
        "Message-ID: <abc@example.com>\r\n",
        "Date: Tue, 1 Jan 2024 00:00:00 +0000\r\n",
        "Subject: [PATCH] sample\r\n",
        "MIME-Version: 1.0\r\n",
        "Content-Type: multipart/mixed; boundary=\"x\"\r\n",
        "\r\n",
        "--x\r\n",
        "Content-Type: text/plain; charset=utf-8\r\n",
        "\r\n",
        "hello\r\n",
        "--x\r\n",
        "Content-Type: text/x-patch\r\n",
        "Content-Disposition: attachment; filename=\"0001.patch\"\r\n",
        "\r\n",
        "diff --git a/a b/a\r\n",
        "--- a/a\r\n",
        "+++ b/a\r\n",
        "+line\r\n",
        "--x--\r\n"
    )
    .as_bytes()
    .to_vec();

    let parsed = parse_email(&raw).expect("parse should succeed");
    assert!(parsed.has_attachments);
    assert!(parsed.has_diff);
    assert!(parsed.diff_text.unwrap_or_default().contains("diff --git"));
}

#[test]
fn parser_extracts_format_patch_diff_section() {
    let raw = concat!(
        "From: Alice <alice@example.com>\r\n",
        "Message-ID: <fmt@example.com>\r\n",
        "Date: Tue, 1 Jan 2024 00:00:00 +0000\r\n",
        "Subject: [PATCH] sample\r\n",
        "Content-Type: text/plain; charset=utf-8\r\n",
        "\r\n",
        "From abcdef Mon Sep 17 00:00:00 2001\r\n",
        "Subject: [PATCH] sample\r\n",
        "\r\n",
        "demo message\r\n",
        "\r\n",
        "---\r\n",
        " foo.c | 2 +-\r\n",
        " 1 file changed, 1 insertion(+), 1 deletion(-)\r\n",
        "\r\n",
        "diff --git a/foo.c b/foo.c\r\n",
        "index 1111111..2222222 100644\r\n",
        "--- a/foo.c\r\n",
        "+++ b/foo.c\r\n",
        "@@ -1 +1 @@\r\n",
        "-old\r\n",
        "+new\r\n"
    )
    .as_bytes()
    .to_vec();

    let parsed = parse_email(&raw).expect("parse should succeed");
    assert!(parsed.has_diff);
    let diff = parsed.diff_text.unwrap_or_default();
    assert!(diff.starts_with("diff --git a/foo.c b/foo.c"));
    assert!(parsed.search_text.contains("demo message"));
    assert!(!parsed.search_text.contains("diff --git"));
}

#[test]
fn parser_patch_only_message_has_empty_search_text() {
    let raw = concat!(
        "From: Alice <alice@example.com>\r\n",
        "Message-ID: <patch-only@example.com>\r\n",
        "Date: Tue, 1 Jan 2024 00:00:00 +0000\r\n",
        "Subject: [PATCH] patch only\r\n",
        "Content-Type: text/plain; charset=utf-8\r\n",
        "\r\n",
        "diff --git a/foo.c b/foo.c\r\n",
        "index 1111111..2222222 100644\r\n",
        "--- a/foo.c\r\n",
        "+++ b/foo.c\r\n",
        "@@ -1 +1 @@\r\n",
        "-old\r\n",
        "+new\r\n"
    )
    .as_bytes()
    .to_vec();

    let parsed = parse_email(&raw).expect("parse should succeed");
    assert!(parsed.has_diff);
    assert_eq!(parsed.search_text, "");
}

#[test]
fn parser_sanitizes_null_bytes_in_headers_and_message_ids() {
    let raw = concat!(
        "From: Al\0ice <ALICE@example.com>\r\n",
        "Message-ID: \"<ab\0c@example.com>\"\r\n",
        "References: <par\0ent@example.com>\r\n",
        "Date: Tue, 1 Jan 2024 00:00:00 +0000\r\n",
        "Subject: [PATCH] nu\0ll check\r\n",
        "Content-Type: text/plain; charset=utf-8\r\n",
        "\r\n",
        "body\r\n"
    )
    .as_bytes()
    .to_vec();

    let parsed = parse_email(&raw).expect("parse should succeed");
    assert_eq!(parsed.subject_raw, "[PATCH] null check");
    assert_eq!(parsed.from_email, "alice@example.com");
    assert_eq!(parsed.message_id_primary, "abc@example.com");
    assert_eq!(
        parsed.references_ids,
        vec!["parent@example.com".to_string()]
    );
}

#[test]
fn sanitize_text_removes_controls_and_normalizes_line_endings() {
    let input = "  a\0b\r\nc\rd\tz\u{0007}\u{0085}  ";
    assert_eq!(sanitize_text(input), "ab\nc\nd\tz");
}

#[test]
fn limit_for_search_truncates_on_char_boundaries() {
    let input = "🙂épatch";
    assert_eq!(limit_for_search(input, 1), "🙂");
    assert_eq!(limit_for_search(input, 2), "🙂é");
    assert_eq!(limit_for_search(input, 6), "🙂épatc");
}

#[test]
fn parser_keeps_unicode_body_text_while_dropping_controls() {
    let raw = concat!(
        "From: Alice <alice@example.com>\r\n",
        "Message-ID: <utf8@example.com>\r\n",
        "Date: Tue, 1 Jan 2024 00:00:00 +0000\r\n",
        "Subject: [PATCH] utf8 body\r\n",
        "Content-Type: text/plain; charset=utf-8\r\n",
        "\r\n",
        "hello Привет \u{0007}\r\n",
        "café\tline\r\n"
    )
    .as_bytes()
    .to_vec();

    let parsed = parse_email(&raw).expect("parse should succeed");
    assert_eq!(
        parsed.body_text.as_deref(),
        Some("hello Привет \ncafé\tline")
    );
    assert!(parsed.search_text.contains("Привет"));
}

#[test]
fn sanitization_invariant_rejects_disallowed_controls() {
    assert!(is_disallowed_char('\u{0001}'));
    assert!(is_disallowed_char('\0'));
    assert!(!is_disallowed_char('\n'));
    assert!(!is_disallowed_char('\t'));
    assert!(!is_disallowed_char('é'));
}

#[test]
fn parser_rejects_missing_date() {
    let raw = concat!(
        "From: Alice <alice@example.com>\r\n",
        "Message-ID: <missing-date@example.com>\r\n",
        "Subject: [PATCH] missing date\r\n",
        "Content-Type: text/plain; charset=utf-8\r\n",
        "\r\n",
        "body\r\n"
    )
    .as_bytes()
    .to_vec();

    let err = parse_email(&raw).expect_err("missing date should fail");
    assert!(matches!(err, ParseEmailError::MissingDate { .. }));
}

#[test]
fn parser_rejects_invalid_date() {
    let raw = concat!(
        "From: Alice <alice@example.com>\r\n",
        "Message-ID: <invalid-date@example.com>\r\n",
        "Date: definitely not a date\r\n",
        "Subject: [PATCH] invalid date\r\n",
        "Content-Type: text/plain; charset=utf-8\r\n",
        "\r\n",
        "body\r\n"
    )
    .as_bytes()
    .to_vec();

    let err = parse_email(&raw).expect_err("invalid date should fail");
    assert!(matches!(err, ParseEmailError::InvalidDate { .. }));
}

#[test]
fn parser_rejects_far_future_date() {
    let future_date = (Utc::now() + Duration::hours(72)).to_rfc2822();
    let raw = format!(
        "From: Alice <alice@example.com>\r\n\
         Message-ID: <future-date@example.com>\r\n\
         Date: {future_date}\r\n\
         Subject: [PATCH] future date\r\n\
         Content-Type: text/plain; charset=utf-8\r\n\
         \r\n\
         body\r\n"
    );

    let err = parse_email(raw.as_bytes()).expect_err("future date should fail");
    assert!(matches!(err, ParseEmailError::FutureDate { .. }));
}
