use once_cell::sync::Lazy;
use regex::Regex;

static REPLY_PREFIX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^(re|fwd|fw|aw):\s*").expect("valid reply prefix regex"));

static VERSION_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^v([0-9]+)$").expect("valid version regex"));

static ORDINAL_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(?P<ordinal>0*[0-9]+)/(?P<total>0*[0-9]+)$").expect("valid ordinal regex")
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedPatchSubject {
    pub subject_norm_base: String,
    pub has_patch_tag: bool,
    pub is_rfc: bool,
    pub version_num: i32,
    pub ordinal: Option<i32>,
    pub total: Option<i32>,
    pub is_resend: bool,
    pub extra_tags: Vec<String>,
}

pub fn parse_patch_subject(subject_raw: &str) -> ParsedPatchSubject {
    let stripped = strip_reply_prefixes(subject_raw.trim());
    let (tags, remainder) = consume_leading_bracket_groups(stripped);

    let mut has_patch_tag = false;
    let mut is_rfc = false;
    let mut version_num = 1i32;
    let mut ordinal = None;
    let mut total = None;
    let mut is_resend = false;
    let mut extra_tags = Vec::new();

    for tag in tags {
        for token in tag
            .split(|c: char| c.is_whitespace() || c == ',' || c == ';')
            .filter(|part| !part.trim().is_empty())
        {
            let normalized = token
                .trim_matches(|c: char| c == '[' || c == ']' || c == ':' || c == '-')
                .to_ascii_lowercase();

            if normalized.is_empty() {
                continue;
            }

            if normalized == "patch" || normalized.starts_with("patch-") {
                has_patch_tag = true;
                continue;
            }

            if normalized == "rfc" {
                is_rfc = true;
                continue;
            }

            if normalized == "resend" || normalized == "repost" {
                is_resend = true;
                continue;
            }

            if let Some(caps) = VERSION_RE.captures(&normalized) {
                if let Ok(parsed) = caps[1].parse::<i32>()
                    && parsed > 0
                {
                    version_num = parsed;
                }
                continue;
            }

            if let Some(caps) = ORDINAL_RE.captures(&normalized) {
                let parsed_ordinal = caps
                    .name("ordinal")
                    .and_then(|v| v.as_str().parse::<i32>().ok());
                let parsed_total = caps
                    .name("total")
                    .and_then(|v| v.as_str().parse::<i32>().ok());
                if let Some(value) = parsed_ordinal {
                    ordinal = Some(value);
                }
                if let Some(value) = parsed_total {
                    total = Some(value.max(0));
                }
                continue;
            }

            extra_tags.push(normalized);
        }
    }

    ParsedPatchSubject {
        subject_norm_base: normalize_subject_base(remainder),
        has_patch_tag,
        is_rfc,
        version_num,
        ordinal,
        total,
        is_resend,
        extra_tags,
    }
}

fn strip_reply_prefixes(mut value: &str) -> &str {
    loop {
        let trimmed = value.trim_start();
        if let Some(mat) = REPLY_PREFIX_RE.find(trimmed) {
            value = &trimmed[mat.end()..];
            continue;
        }
        return trimmed;
    }
}

fn consume_leading_bracket_groups(value: &str) -> (Vec<String>, &str) {
    let mut tags = Vec::new();
    let mut rest = value.trim_start();

    loop {
        if !rest.starts_with('[') {
            break;
        }
        let Some(close_idx) = rest.find(']') else {
            break;
        };

        let inside = rest[1..close_idx].trim();
        if !inside.is_empty() {
            tags.push(inside.to_string());
        }

        rest = rest[close_idx + 1..].trim_start();
    }

    (tags, rest)
}

fn normalize_subject_base(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    trimmed
        .to_ascii_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::parse_patch_subject;

    #[test]
    fn parses_patch_v2_ordinal() {
        let parsed = parse_patch_subject("[PATCH v2 01/27] mm: improve reclaim");
        assert!(parsed.has_patch_tag);
        assert_eq!(parsed.version_num, 2);
        assert_eq!(parsed.ordinal, Some(1));
        assert_eq!(parsed.total, Some(27));
        assert!(!parsed.is_rfc);
        assert_eq!(parsed.subject_norm_base, "mm: improve reclaim");
    }

    #[test]
    fn parses_rfc_cover() {
        let parsed = parse_patch_subject("[RFC PATCH 0/5] bpf: add feature");
        assert!(parsed.has_patch_tag);
        assert!(parsed.is_rfc);
        assert_eq!(parsed.version_num, 1);
        assert_eq!(parsed.ordinal, Some(0));
        assert_eq!(parsed.total, Some(5));
    }

    #[test]
    fn parses_resend_and_reply_prefixes() {
        let parsed = parse_patch_subject("Re: [PATCH RESEND v3 2/7] net: cleanup foo");
        assert!(parsed.has_patch_tag);
        assert!(parsed.is_resend);
        assert_eq!(parsed.version_num, 3);
        assert_eq!(parsed.ordinal, Some(2));
        assert_eq!(parsed.total, Some(7));
        assert_eq!(parsed.subject_norm_base, "net: cleanup foo");
    }
}
