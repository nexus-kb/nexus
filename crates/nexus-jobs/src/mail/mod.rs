use std::collections::HashSet;

use chrono::{DateTime, Duration, TimeZone, Utc};
use mailparse::{MailAddr, MailHeaderMap, ParsedMail, addrparse, dateparse, parse_mail};
use once_cell::sync::Lazy;
use regex::Regex;
use sha2::{Digest, Sha256};

use crate::patch_detect::{extract_diff_text, has_diff_markers, normalize_line_endings};

static MESSAGE_ID_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"<([^<>\s]+)>").expect("valid message-id regex"));
const MAX_FUTURE_DATE_SKEW_HOURS: i64 = 24;

#[derive(Debug, thiserror::Error)]
pub enum ParseEmailError {
    #[error("mailparse failure: {0}")]
    MailParse(#[from] mailparse::MailParseError),
    #[error("missing message-id")]
    MissingMessageId,
    #[error("missing author email")]
    MissingAuthorEmail,
    #[error("missing date for message {message_id}")]
    MissingDate { message_id: String },
    #[error("invalid date `{raw}` for message {message_id}")]
    InvalidDate { message_id: String, raw: String },
    #[error("future date `{raw}` for message {message_id}")]
    FutureDate { message_id: String, raw: String },
    #[error("sanitization invariant violation: {0}")]
    SanitizationInvariantViolation(&'static str),
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

    let date_raw = parsed
        .headers
        .get_first_value("Date")
        .map(|v| sanitize_text(&v))
        .unwrap_or_default();

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
    let date_utc = parse_date(&date_raw, &message_id_primary)?;

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
    });

    let body_text = extract_preferred_body(&parsed)
        .map(|value| sanitize_text(&value))
        .filter(|v| !v.is_empty());
    let diff_text = if diff_parts.is_empty() {
        None
    } else {
        Some(sanitize_text(&diff_parts.join("\n")))
    }
    .filter(|v| !v.is_empty());

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
        date: &date_raw,
        references_ids: &references_ids,
        in_reply_to_ids: &in_reply_to_ids,
        to_raw: to_raw.as_deref().unwrap_or(""),
        cc_raw: cc_raw.as_deref().unwrap_or(""),
        body: &body_for_hash,
    });

    let search_text = build_search_text(body_text.as_deref());

    let outcome = ParseOutcome {
        content_hash_sha256,
        subject_raw,
        subject_norm,
        from_name,
        from_email,
        date_utc: Some(date_utc),
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
    };
    assert_parse_outcome_invariants(&outcome)?;
    Ok(outcome)
}

mod helpers;
#[cfg(test)]
mod tests;

use helpers::*;
