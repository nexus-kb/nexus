use std::collections::HashSet;

use chrono::{DateTime, TimeZone, Utc};
use mailparse::{MailAddr, MailHeaderMap, ParsedMail, addrparse, dateparse, parse_mail};
use once_cell::sync::Lazy;
use regex::Regex;
use sha2::{Digest, Sha256};

use crate::patch_detect::{extract_diff_text, has_diff_markers, normalize_line_endings};

static MESSAGE_ID_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"<([^<>\s]+)>").expect("valid message-id regex"));

#[derive(Debug, thiserror::Error)]
pub enum ParseEmailError {
    #[error("mailparse failure: {0}")]
    MailParse(#[from] mailparse::MailParseError),
    #[error("missing message-id")]
    MissingMessageId,
    #[error("missing author email")]
    MissingAuthorEmail,
}

#[derive(Debug, Clone)]
pub struct ParseOutcome {
    pub content_hash_sha256: Vec<u8>,
    pub subject_raw: String,
    pub subject_norm: String,
    pub from_name: Option<String>,
    pub from_email: String,
    pub date_utc: Option<DateTime<Utc>>,
    pub to_raw: Option<String>,
    pub cc_raw: Option<String>,
    pub message_ids: Vec<String>,
    pub message_id_primary: String,
    pub in_reply_to_ids: Vec<String>,
    pub references_ids: Vec<String>,
    pub mime_type: Option<String>,
    pub body_text: Option<String>,
    pub diff_text: Option<String>,
    pub search_text: String,
    pub has_diff: bool,
    pub has_attachments: bool,
}

pub fn parse_email(raw_rfc822: &[u8]) -> Result<ParseOutcome, ParseEmailError> {
    let parsed = parse_mail(raw_rfc822)?;

    let subject_raw = parsed
        .headers
        .get_first_value("Subject")
        .map(|v| sanitize_text(&v))
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "(No Subject)".to_string());

    let subject_norm = normalize_subject(&subject_raw);

    let from_header = parsed
        .headers
        .get_first_value("From")
        .map(|v| sanitize_text(&v))
        .unwrap_or_default();

    let (from_name, from_email) = parse_primary_author(&from_header);
    if from_email.is_empty() {
        return Err(ParseEmailError::MissingAuthorEmail);
    }

    let date_utc = parsed
        .headers
        .get_first_value("Date")
        .and_then(|v| parse_date(&v));

    let to_raw = parsed
        .headers
        .get_first_value("To")
        .map(|v| sanitize_text(&v))
        .filter(|v| !v.is_empty());

    let cc_raw = parsed
        .headers
        .get_first_value("Cc")
        .map(|v| sanitize_text(&v))
        .filter(|v| !v.is_empty());

    let mut message_ids = Vec::new();
    for header_value in parsed.headers.get_all_values("Message-ID") {
        message_ids.extend(extract_message_ids(&header_value));
    }
    message_ids = dedupe_preserve_order(message_ids);
    let message_id_primary = message_ids
        .first()
        .cloned()
        .ok_or(ParseEmailError::MissingMessageId)?;

    let mut in_reply_to_ids = Vec::new();
    for header_value in parsed.headers.get_all_values("In-Reply-To") {
        in_reply_to_ids.extend(extract_message_ids(&header_value));
    }
    in_reply_to_ids = dedupe_preserve_order(in_reply_to_ids);

    let mut references_ids = Vec::new();
    for header_value in parsed.headers.get_all_values("References") {
        references_ids.extend(extract_message_ids(&header_value));
    }
    references_ids = dedupe_preserve_order(references_ids);

    let mut body_text: Option<String> = None;
    let mut diff_parts: Vec<String> = Vec::new();
    let mut has_attachments = false;

    collect_mail_parts(&parsed, &mut |part| {
        let mime = part.ctype.mimetype.to_ascii_lowercase();
        let disposition = part
            .headers
            .get_first_value("Content-Disposition")
            .unwrap_or_default()
            .to_ascii_lowercase();

        let filename = extract_filename(&disposition);
        let is_attachment = disposition.contains("attachment") || filename.is_some();

        if is_attachment {
            has_attachments = true;
        }

        let body = extract_part_body(part);
        if body.trim().is_empty() {
            return;
        }

        if mime.starts_with("text/plain") && body_text.is_none() {
            body_text = Some(sanitize_text(&body));
        }

        let looks_like_patch_mime = mime.contains("x-patch") || mime.contains("x-diff");
        let looks_like_patch_name = filename
            .as_deref()
            .map(|name| name.ends_with(".patch") || name.ends_with(".diff"))
            .unwrap_or(false);

        let normalized_body = normalize_line_endings(&body);
        if looks_like_patch_mime || looks_like_patch_name {
            diff_parts.push(
                extract_diff_text(&normalized_body).unwrap_or_else(|| normalized_body.clone()),
            );
        } else if let Some(extracted) = extract_diff_text(&normalized_body) {
            diff_parts.push(extracted);
        }

        if body_text.is_none() && mime.starts_with("text/") {
            body_text = Some(sanitize_text(&body));
        }
    });

    let body_text = body_text.filter(|v| !v.is_empty());
    let diff_text = if diff_parts.is_empty() {
        None
    } else {
        Some(diff_parts.join("\n"))
    };

    let has_diff = diff_text
        .as_ref()
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
        || body_text
            .as_ref()
            .map(|v| has_diff_markers(v))
            .unwrap_or(false);

    let body_for_hash = body_text
        .as_deref()
        .map(normalize_text_for_hash)
        .unwrap_or_default();

    let content_hash_sha256 = compute_content_hash(ContentHashInput {
        subject: &subject_raw,
        from: &from_header,
        date: parsed
            .headers
            .get_first_value("Date")
            .as_deref()
            .unwrap_or(""),
        references_ids: &references_ids,
        in_reply_to_ids: &in_reply_to_ids,
        to_raw: to_raw.as_deref().unwrap_or(""),
        cc_raw: cc_raw.as_deref().unwrap_or(""),
        body: &body_for_hash,
    });

    let search_text = build_search_text(
        &subject_raw,
        &from_header,
        body_text.as_deref(),
        diff_text.as_deref(),
    );

    Ok(ParseOutcome {
        content_hash_sha256,
        subject_raw,
        subject_norm,
        from_name,
        from_email,
        date_utc,
        to_raw,
        cc_raw,
        message_ids,
        message_id_primary,
        in_reply_to_ids,
        references_ids,
        mime_type: Some(parsed.ctype.mimetype.to_ascii_lowercase()),
        body_text,
        diff_text,
        search_text,
        has_diff,
        has_attachments,
    })
}

fn build_search_text(subject: &str, from: &str, body: Option<&str>, diff: Option<&str>) -> String {
    let mut parts = vec![subject.to_string(), from.to_string()];
    if let Some(body) = body {
        parts.push(limit_for_search(&sanitize_text(body), 80_000));
    }
    if let Some(diff) = diff {
        parts.push(limit_for_search(&sanitize_text(diff), 20_000));
    }
    parts.join("\n")
}

fn limit_for_search(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        text.to_string()
    } else {
        text[..limit].to_string()
    }
}

fn collect_mail_parts<'a, F>(part: &'a ParsedMail<'a>, callback: &mut F)
where
    F: FnMut(&'a ParsedMail<'a>),
{
    callback(part);
    for sub in &part.subparts {
        collect_mail_parts(sub, callback);
    }
}

fn extract_part_body(part: &ParsedMail<'_>) -> String {
    match part.get_body() {
        Ok(body) => body,
        Err(_) => part
            .get_body_raw()
            .map(|raw| String::from_utf8_lossy(&raw).to_string())
            .unwrap_or_default(),
    }
}

fn parse_primary_author(from_header: &str) -> (Option<String>, String) {
    if let Ok(addrs) = addrparse(from_header)
        && let Some(MailAddr::Single(first)) = addrs.iter().next()
    {
        let name = first
            .display_name
            .as_ref()
            .map(|v| sanitize_text(v))
            .filter(|v| !v.is_empty());
        return (name, sanitize_email(&first.addr));
    }

    // Fallback for malformed headers.
    for token in from_header.split(|c: char| c.is_whitespace() || c == ',' || c == ';') {
        if token.contains('@') {
            return (None, sanitize_email(token));
        }
    }

    (None, String::new())
}

fn parse_date(raw: &str) -> Option<DateTime<Utc>> {
    let timestamp = dateparse(raw).ok()?;
    Utc.timestamp_opt(timestamp, 0).single()
}

fn sanitize_text(value: &str) -> String {
    value.replace('\0', "").trim().to_string()
}

fn sanitize_email(value: &str) -> String {
    sanitize_text(value)
        .trim_matches('<')
        .trim_matches('>')
        .trim_matches('"')
        .to_ascii_lowercase()
}

fn normalize_subject(subject: &str) -> String {
    let mut normalized = subject.trim().to_ascii_lowercase();

    loop {
        let before = normalized.clone();

        for prefix in ["re:", "fwd:", "fw:", "aw:"] {
            if normalized.starts_with(prefix) {
                normalized = normalized[prefix.len()..].trim_start().to_string();
            }
        }

        if normalized.starts_with('[')
            && let Some(idx) = normalized.find(']')
        {
            normalized = normalized[idx + 1..].trim_start().to_string();
        }

        if before == normalized {
            break;
        }
    }

    normalized.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn normalize_text_for_hash(text: &str) -> String {
    normalize_line_endings(text)
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
}

struct ContentHashInput<'a> {
    subject: &'a str,
    from: &'a str,
    date: &'a str,
    references_ids: &'a [String],
    in_reply_to_ids: &'a [String],
    to_raw: &'a str,
    cc_raw: &'a str,
    body: &'a str,
}

fn compute_content_hash(input: ContentHashInput<'_>) -> Vec<u8> {
    let mut hasher = Sha256::new();

    for (key, value) in [
        ("Subject", normalize_text_for_hash(input.subject)),
        ("From", normalize_text_for_hash(input.from)),
        ("Date", normalize_text_for_hash(input.date)),
        (
            "References",
            normalize_text_for_hash(&input.references_ids.join(" ")),
        ),
        (
            "In-Reply-To",
            normalize_text_for_hash(&input.in_reply_to_ids.join(" ")),
        ),
        ("To", normalize_text_for_hash(input.to_raw)),
        ("Cc", normalize_text_for_hash(input.cc_raw)),
    ] {
        hasher.update(key.as_bytes());
        hasher.update(b":");
        hasher.update(value.as_bytes());
        hasher.update(b"\n");
    }

    hasher.update(b"\n");
    hasher.update(normalize_text_for_hash(input.body).as_bytes());
    hasher.finalize().to_vec()
}

fn extract_filename(disposition: &str) -> Option<String> {
    for part in disposition.split(';') {
        let trimmed = part.trim();
        if let Some(rest) = trimmed.strip_prefix("filename=") {
            return Some(rest.trim_matches('"').to_ascii_lowercase());
        }
    }
    None
}

fn extract_message_ids(value: &str) -> Vec<String> {
    let mut ids = Vec::new();

    for caps in MESSAGE_ID_REGEX.captures_iter(value) {
        if let Some(normalized) = normalize_message_id_token(&caps[1]) {
            ids.push(normalized);
        }
    }

    if ids.is_empty() {
        for token in value
            .split(|c: char| c.is_whitespace() || c == ',' || c == ';')
            .filter(|v| !v.is_empty())
        {
            if let Some(normalized) = normalize_message_id_token(token) {
                ids.push(normalized);
            }
        }
    }

    ids
}

pub fn normalize_message_id_token(raw: &str) -> Option<String> {
    let mut value = raw.trim().to_string();

    loop {
        let before = value.clone();
        value = value
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();
        value = value.trim_matches('<').trim_matches('>').trim().to_string();
        if before == value {
            break;
        }
    }

    if value.is_empty() || !value.contains('@') {
        return None;
    }

    Some(value.to_ascii_lowercase())
}

fn dedupe_preserve_order(ids: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for id in ids {
        if seen.insert(id.clone()) {
            out.push(id);
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::{ContentHashInput, compute_content_hash, normalize_message_id_token, parse_email};

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
    }
}
