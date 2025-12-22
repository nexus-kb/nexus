use std::collections::HashSet;

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use mailparse::{MailAddr, MailHeaderMap, ParsedMail, addrparse, dateparse, parse_mail};
use once_cell::sync::Lazy;
use regex::Regex;

use nexus_db::EmailRecipient;

static B4_DIFF_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?mi)^(---.*\n\+\+\+|GIT binary patch|diff --git \w/\S+ \w/\S+)")
        .expect("valid diff detection regex")
});

const MAX_FUTURE_SKEW: ChronoDuration = ChronoDuration::hours(24);

#[derive(Debug, thiserror::Error)]
pub enum ParseEmailError {
    #[error("failed to parse MIME structure: {0}")]
    MimeParse(#[from] mailparse::MailParseError),
    #[error("missing Message-ID header")]
    MissingMessageId,
    #[error("missing author email for message {message_id}")]
    MissingAuthorEmail { message_id: String },
    #[error("missing Date header for message {message_id}")]
    MissingDate { message_id: String },
    #[error("invalid Date header `{raw}` for message {message_id}: {error}")]
    InvalidDate {
        message_id: String,
        raw: String,
        error: String,
    },
    #[error("future Date header `{raw}` for message {message_id}")]
    FutureDate { message_id: String, raw: String },
}

/// Parsed email fields used for ingest and threading.
#[derive(Debug, Clone)]
pub struct ParsedEmail {
    pub message_id: String,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub subject: String,
    pub normalized_subject: String,
    pub date: DateTime<Utc>,
    pub from: EmailRecipient,
    pub to: Vec<EmailRecipient>,
    pub cc: Vec<EmailRecipient>,
    pub body: Option<String>,
}

/// Parse raw email bytes into a normalized, sanitized structure.
pub fn parse_email(raw: &[u8]) -> Result<ParsedEmail, ParseEmailError> {
    let parsed = parse_mail(raw)?;

    let message_id = normalize_message_id(parsed.headers.get_first_value("Message-ID"))
        .ok_or(ParseEmailError::MissingMessageId)?;

    let subject = parsed
        .headers
        .get_first_value("Subject")
        .map(|value| sanitize_text(&value))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "(No Subject)".to_string());

    let date = parse_email_date(parsed.headers.get_first_value("Date"), &message_id)?;

    let from_header = parsed.headers.get_first_value("From").unwrap_or_default();
    let (author_name, author_email) = if let Ok(addrs) = addrparse(&from_header) {
        if let Some(MailAddr::Single(info)) = addrs.iter().next() {
            let name = info.display_name.clone().unwrap_or_default();
            let email = info.addr.clone();
            (sanitize_text(&name), sanitize_email(&email))
        } else {
            (String::new(), String::new())
        }
    } else {
        (String::new(), String::new())
    };

    if author_email.is_empty() {
        return Err(ParseEmailError::MissingAuthorEmail { message_id });
    }

    let body = sanitize_text(&extract_preferred_body(&parsed));
    let body = if body.is_empty() { None } else { Some(body) };

    let to = parsed
        .headers
        .get_first_value("To")
        .map(|value| parse_email_addresses(&value))
        .unwrap_or_default();

    let cc = parsed
        .headers
        .get_first_value("Cc")
        .map(|value| parse_email_addresses(&value))
        .unwrap_or_default();

    let in_reply_to = normalize_message_id(parsed.headers.get_first_value("In-Reply-To"));

    let references = parsed
        .headers
        .get_first_value("References")
        .map(|value| extract_references(&value))
        .unwrap_or_default();

    let normalized_subject = normalize_subject(&subject);

    Ok(ParsedEmail {
        message_id,
        in_reply_to,
        references: dedupe_message_ids(references),
        subject,
        normalized_subject,
        date,
        from: EmailRecipient {
            email: author_email,
            name: if author_name.is_empty() {
                None
            } else {
                Some(author_name)
            },
        },
        to,
        cc,
        body,
    })
}

fn sanitize_text(text: &str) -> String {
    text.replace('\0', "").trim().to_string()
}

fn sanitize_email(text: &str) -> String {
    sanitize_text(text).to_lowercase()
}

fn normalize_message_id(raw: Option<String>) -> Option<String> {
    raw.as_deref()
        .and_then(normalize_message_id_value)
        .map(|value| value.to_lowercase())
}

fn normalize_message_id_value(raw: &str) -> Option<String> {
    let cleaned = raw.trim().trim_matches(&['<', '>'][..]).trim();
    if cleaned.is_empty() {
        None
    } else {
        Some(sanitize_text(cleaned))
    }
}

fn parse_email_addresses(header_value: &str) -> Vec<EmailRecipient> {
    let mut addresses = Vec::new();
    for addr_str in header_value.split(',') {
        if let Ok(addr) = addrparse(addr_str.trim()) {
            for single in addr.iter() {
                if let MailAddr::Single(info) = single {
                    let name = sanitize_text(info.display_name.as_deref().unwrap_or_default());
                    let email = sanitize_email(&info.addr);
                    if email.is_empty() {
                        continue;
                    }
                    addresses.push(EmailRecipient {
                        email,
                        name: if name.is_empty() { None } else { Some(name) },
                    });
                }
            }
        }
    }
    addresses
}

fn extract_references(header_value: &str) -> Vec<String> {
    header_value
        .split_whitespace()
        .filter_map(normalize_message_id_value)
        .map(|value| value.to_lowercase())
        .filter(|value| !value.is_empty())
        .collect()
}

fn parse_email_date(
    raw_date: Option<String>,
    message_id: &str,
) -> Result<DateTime<Utc>, ParseEmailError> {
    let raw = raw_date.unwrap_or_default();
    if raw.trim().is_empty() {
        return Err(ParseEmailError::MissingDate {
            message_id: message_id.to_string(),
        });
    }

    match dateparse(&raw) {
        Ok(ts) => {
            let parsed = Utc.timestamp_opt(ts, 0).single();
            let date = match parsed {
                Some(date) => date,
                None => {
                    return Err(ParseEmailError::InvalidDate {
                        message_id: message_id.to_string(),
                        raw,
                        error: "timestamp out of range".to_string(),
                    });
                }
            };
            if date > Utc::now() + MAX_FUTURE_SKEW {
                Err(ParseEmailError::FutureDate {
                    message_id: message_id.to_string(),
                    raw,
                })
            } else {
                Ok(date)
            }
        }
        Err(err) => Err(ParseEmailError::InvalidDate {
            message_id: message_id.to_string(),
            raw,
            error: err.to_string(),
        }),
    }
}

fn extract_preferred_body(parsed: &ParsedMail) -> String {
    let mut preferred: Option<String> = None;
    let mut stack = Vec::new();
    stack.push(parsed);

    while let Some(part) = stack.pop() {
        for sub in part.subparts.iter().rev() {
            stack.push(sub);
        }

        let mime = part.ctype.mimetype.to_ascii_lowercase();
        if !mime.contains("/plain") && !mime.contains("/x-patch") {
            continue;
        }

        let body = match part.get_body() {
            Ok(body) => body,
            Err(_) => match part.get_body_raw() {
                Ok(raw) => String::from_utf8_lossy(&raw).to_string(),
                Err(_) => continue,
            },
        };

        if body.is_empty() {
            continue;
        }

        if preferred.is_none() {
            preferred = Some(body.clone());
            continue;
        }

        if B4_DIFF_RE.is_match(&body) {
            preferred = Some(body);
        }
    }

    preferred
        .or_else(|| parsed.get_body().ok())
        .or_else(|| {
            parsed
                .get_body_raw()
                .ok()
                .map(|raw| String::from_utf8_lossy(&raw).to_string())
        })
        .unwrap_or_default()
}

fn normalize_subject(subject: &str) -> String {
    let mut normalized = subject.trim().to_lowercase();

    loop {
        let before = normalized.clone();

        for prefix in &["re:", "fwd:", "fw:", "aw:"] {
            if normalized.starts_with(prefix) {
                normalized = normalized[prefix.len()..].trim_start().to_string();
            }
        }

        if normalized.starts_with('[') {
            if let Some(end_bracket) = normalized.find(']') {
                normalized = normalized[end_bracket + 1..].trim_start().to_string();
            }
        }

        if before == normalized {
            break;
        }
    }

    normalized.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn dedupe_message_ids(ids: Vec<String>) -> Vec<String> {
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
    use super::*;

    #[test]
    fn normalize_message_id_strips_brackets() {
        assert_eq!(
            normalize_message_id(Some("<Test@Example.COM>".to_string())),
            Some("test@example.com".to_string())
        );
        assert_eq!(
            normalize_message_id(Some("  <id@host>  ".to_string())),
            Some("id@host".to_string())
        );
        assert_eq!(normalize_message_id(Some("".to_string())), None);
        assert_eq!(normalize_message_id(None), None);
    }

    #[test]
    fn extract_references_strips_brackets() {
        let ids = extract_references("<a@b> <c@d>");
        assert_eq!(ids, vec!["a@b".to_string(), "c@d".to_string()]);
    }

    #[test]
    fn sanitize_text_removes_nul_and_trims() {
        assert_eq!(sanitize_text("  hello\0world  "), "helloworld");
    }

    #[test]
    fn normalize_subject_strips_prefixes() {
        assert_eq!(
            normalize_subject("Re: Fwd:  [List]  Hello World"),
            "hello world".to_string()
        );
    }
}
