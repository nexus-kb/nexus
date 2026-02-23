use super::*;

pub(super) fn build_search_text(body: Option<&str>) -> String {
    let Some(body) = body else {
        return String::new();
    };
    let Some(prose_body) = extract_prose_for_search(body) else {
        return String::new();
    };
    limit_for_search(&sanitize_text(&prose_body), 80_000)
}

pub(super) fn extract_prose_for_search(body: &str) -> Option<String> {
    let normalized = normalize_line_endings(body);
    if normalized.trim().is_empty() {
        return None;
    }
    let Some(diff) = extract_diff_text(&normalized) else {
        return Some(normalized);
    };
    if let Some(start) = find_diff_start_index(&normalized, &diff) {
        let prose = normalized[..start].trim();
        if prose.is_empty() {
            return None;
        }
        return Some(prose.to_string());
    }
    if has_diff_markers(&normalized) {
        return None;
    }
    Some(normalized)
}

pub(super) fn find_diff_start_index(body: &str, diff: &str) -> Option<usize> {
    let direct = body.find(diff);
    if direct.is_some() {
        return direct;
    }
    body.find(format!("\n{diff}").as_str()).map(|idx| idx + 1)
}

pub(super) fn limit_for_search(text: &str, limit: usize) -> String {
    if limit == 0 {
        return String::new();
    }
    text.chars().take(limit).collect()
}

pub(super) fn collect_mail_parts<'a, F>(part: &'a ParsedMail<'a>, callback: &mut F)
where
    F: FnMut(&'a ParsedMail<'a>),
{
    callback(part);
    for sub in &part.subparts {
        collect_mail_parts(sub, callback);
    }
}

pub(super) fn extract_part_body(part: &ParsedMail<'_>) -> String {
    match part.get_body() {
        Ok(body) => body,
        Err(_) => part
            .get_body_raw()
            .map(|raw| String::from_utf8_lossy(&raw).to_string())
            .unwrap_or_default(),
    }
}

pub(super) fn extract_preferred_body(parsed: &ParsedMail<'_>) -> Option<String> {
    crate::b4_compat::extract_preferred_body(parsed)
}

pub(super) fn parse_primary_author(from_header: &str) -> (Option<String>, String) {
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

pub(super) fn parse_date(raw: &str, message_id: &str) -> Result<DateTime<Utc>, ParseEmailError> {
    let sanitized = sanitize_text(raw);
    if sanitized.is_empty() {
        return Err(ParseEmailError::MissingDate {
            message_id: message_id.to_string(),
        });
    }
    if !sanitized.chars().any(|ch| ch.is_ascii_digit()) {
        return Err(ParseEmailError::InvalidDate {
            message_id: message_id.to_string(),
            raw: sanitized,
        });
    }

    let timestamp = dateparse(&sanitized).map_err(|_| ParseEmailError::InvalidDate {
        message_id: message_id.to_string(),
        raw: sanitized.clone(),
    })?;

    let parsed =
        Utc.timestamp_opt(timestamp, 0)
            .single()
            .ok_or_else(|| ParseEmailError::InvalidDate {
                message_id: message_id.to_string(),
                raw: sanitized.clone(),
            })?;

    let now = Utc::now();
    if parsed > now + Duration::hours(MAX_FUTURE_DATE_SKEW_HOURS) {
        return Err(ParseEmailError::FutureDate {
            message_id: message_id.to_string(),
            raw: sanitized,
        });
    }

    Ok(parsed)
}

pub(super) fn sanitize_text(value: &str) -> String {
    let normalized = normalize_line_endings(value);
    let mut out = String::with_capacity(normalized.len());
    for ch in normalized.chars() {
        if ch == '\0' {
            continue;
        }
        if ch == '\n' || ch == '\t' || !ch.is_control() {
            out.push(ch);
        }
    }
    out.trim().to_string()
}

pub(super) fn sanitize_email(value: &str) -> String {
    sanitize_text(value)
        .trim_matches('<')
        .trim_matches('>')
        .trim_matches('"')
        .to_ascii_lowercase()
}

pub(super) fn assert_parse_outcome_invariants(
    outcome: &ParseOutcome,
) -> Result<(), ParseEmailError> {
    for (field, value) in [
        ("subject_raw", outcome.subject_raw.as_str()),
        ("subject_norm", outcome.subject_norm.as_str()),
        ("from_email", outcome.from_email.as_str()),
        ("message_id_primary", outcome.message_id_primary.as_str()),
        ("search_text", outcome.search_text.as_str()),
    ] {
        assert_text_invariant(field, value)?;
    }

    if let Some(value) = outcome.from_name.as_deref() {
        assert_text_invariant("from_name", value)?;
    }
    if let Some(value) = outcome.to_raw.as_deref() {
        assert_text_invariant("to_raw", value)?;
    }
    if let Some(value) = outcome.cc_raw.as_deref() {
        assert_text_invariant("cc_raw", value)?;
    }
    if let Some(value) = outcome.body_text.as_deref() {
        assert_text_invariant("body_text", value)?;
    }
    if let Some(value) = outcome.diff_text.as_deref() {
        assert_text_invariant("diff_text", value)?;
    }
    if let Some(value) = outcome.mime_type.as_deref() {
        assert_text_invariant("mime_type", value)?;
    }

    for value in &outcome.message_ids {
        assert_text_invariant("message_ids", value)?;
    }
    for value in &outcome.in_reply_to_ids {
        assert_text_invariant("in_reply_to_ids", value)?;
    }
    for value in &outcome.references_ids {
        assert_text_invariant("references_ids", value)?;
    }

    Ok(())
}

pub(super) fn assert_text_invariant(
    field: &'static str,
    value: &str,
) -> Result<(), ParseEmailError> {
    if value.chars().any(is_disallowed_char) {
        return Err(ParseEmailError::SanitizationInvariantViolation(field));
    }
    Ok(())
}

pub(super) fn is_disallowed_char(ch: char) -> bool {
    ch == '\0' || (ch.is_control() && ch != '\n' && ch != '\t')
}

pub(super) fn normalize_subject(subject: &str) -> String {
    let mut normalized = sanitize_text(subject).to_ascii_lowercase();

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

pub(super) fn normalize_text_for_hash(text: &str) -> String {
    normalize_line_endings(text)
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
}

pub(super) struct ContentHashInput<'a> {
    pub(super) subject: &'a str,
    pub(super) from: &'a str,
    pub(super) date: &'a str,
    pub(super) references_ids: &'a [String],
    pub(super) in_reply_to_ids: &'a [String],
    pub(super) to_raw: &'a str,
    pub(super) cc_raw: &'a str,
    pub(super) body: &'a str,
}

pub(super) fn compute_content_hash(input: ContentHashInput<'_>) -> Vec<u8> {
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

pub(super) fn extract_filename(disposition: &str) -> Option<String> {
    for part in disposition.split(';') {
        let trimmed = part.trim();
        if let Some(rest) = trimmed.strip_prefix("filename=") {
            return Some(rest.trim_matches('"').to_ascii_lowercase());
        }
    }
    None
}

pub(super) fn extract_message_ids(value: &str) -> Vec<String> {
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
    let mut value = sanitize_text(raw);

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

pub(super) fn dedupe_preserve_order(ids: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for id in ids {
        if seen.insert(id.clone()) {
            out.push(id);
        }
    }

    out
}
