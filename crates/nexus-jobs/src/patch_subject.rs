use once_cell::sync::Lazy;
use regex::Regex;

static REPLY_PREFIX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^(re|fwd|fw|aw):\s*").expect("valid reply prefix regex"));
static REPLY_BRACKET_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^\w{2,3}:\s*\[").expect("valid bracket reply regex"));

static VERSION_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^v([0-9]+)$").expect("valid version regex"));

static ORDINAL_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(?P<ordinal>0*[0-9]+)/(?P<total>0*[0-9]+)$").expect("valid ordinal regex")
});
static NESTED_LEFT_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\[([^]]*)\[([^\[\]]*)]").expect("valid nested-left regex"));
static NESTED_RIGHT_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\[([^]]*)]([^\[\]]*)]").expect("valid nested-right regex"));
static PATCHV_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(patch)(v\d+)\b").expect("valid patchv regex"));

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedPatchSubject {
    pub subject_norm_base: String,
    pub had_reply_prefix: bool,
    pub has_patch_tag: bool,
    pub is_rfc: bool,
    pub version_num: i32,
    pub revision_inferred: bool,
    pub ordinal: Option<i32>,
    pub total: Option<i32>,
    pub counters_inferred: bool,
    pub is_resend: bool,
    pub extra_tags: Vec<String>,
}

pub fn parse_patch_subject(subject_raw: &str) -> ParsedPatchSubject {
    let (stripped, had_reply_prefix) = strip_reply_prefixes(subject_raw.trim());
    let canonical = normalize_nested_brackets(stripped);
    let (tags, remainder) = consume_leading_bracket_groups(&canonical);

    let mut has_patch_tag = false;
    let mut is_rfc = false;
    let mut version_num = 1i32;
    let mut revision_inferred = true;
    let mut ordinal = None;
    let mut total = None;
    let mut counters_inferred = true;
    let mut is_resend = false;
    let mut extra_tags = Vec::new();

    for tag in tags {
        let patched_tag = PATCHV_RE.replace_all(&tag, "$1 $2").to_string();

        for token in patched_tag
            .split(|c: char| c.is_whitespace() || c == ',' || c == ';')
            .filter(|part| !part.trim().is_empty())
        {
            let normalized = token
                .trim_matches(|c: char| c == '[' || c == ']' || c == ':' || c == '-')
                .to_ascii_lowercase();

            if normalized.is_empty() {
                continue;
            }

            if let Some(raw_version) = normalized.strip_prefix("patchv") {
                has_patch_tag = true;
                if let Ok(parsed) = raw_version.parse::<i32>()
                    && parsed > 0
                {
                    version_num = parsed;
                    revision_inferred = false;
                }
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
                    revision_inferred = false;
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
                counters_inferred = false;
                continue;
            }

            extra_tags.push(normalized);
        }
    }

    if let (Some(ord), Some(tot)) = (ordinal, total)
        && ord > tot
    {
        total = Some(ord);
    }

    ParsedPatchSubject {
        subject_norm_base: normalize_subject_base(remainder),
        had_reply_prefix,
        has_patch_tag,
        is_rfc,
        version_num,
        revision_inferred,
        ordinal,
        total,
        counters_inferred,
        is_resend,
        extra_tags,
    }
}

fn strip_reply_prefixes(mut value: &str) -> (&str, bool) {
    let mut had_reply_prefix = false;
    loop {
        let trimmed = value.trim_start();
        if let Some(mat) = REPLY_PREFIX_RE.find(trimmed) {
            had_reply_prefix = true;
            value = &trimmed[mat.end()..];
            continue;
        }
        if let Some(mat) = REPLY_BRACKET_RE.find(trimmed) {
            had_reply_prefix = true;
            // Keep the opening bracket so bracket token parsing keeps working.
            let keep_from = mat.end().saturating_sub(1);
            value = &trimmed[keep_from..];
            continue;
        }
        return (trimmed, had_reply_prefix);
    }
}

fn normalize_nested_brackets(value: &str) -> String {
    let mut subject = value.to_string();
    loop {
        let before = subject.clone();
        subject = NESTED_LEFT_RE.replace_all(&subject, "[$1$2]").to_string();
        subject = NESTED_RIGHT_RE.replace_all(&subject, "[$1$2]").to_string();
        if subject == before {
            return subject;
        }
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
        assert!(!parsed.had_reply_prefix);
        assert!(parsed.has_patch_tag);
        assert_eq!(parsed.version_num, 2);
        assert!(!parsed.revision_inferred);
        assert_eq!(parsed.ordinal, Some(1));
        assert_eq!(parsed.total, Some(27));
        assert!(!parsed.counters_inferred);
        assert!(!parsed.is_rfc);
        assert_eq!(parsed.subject_norm_base, "mm: improve reclaim");
    }

    #[test]
    fn parses_rfc_cover() {
        let parsed = parse_patch_subject("[RFC PATCH 0/5] bpf: add feature");
        assert!(!parsed.had_reply_prefix);
        assert!(parsed.has_patch_tag);
        assert!(parsed.is_rfc);
        assert_eq!(parsed.version_num, 1);
        assert_eq!(parsed.ordinal, Some(0));
        assert_eq!(parsed.total, Some(5));
    }

    #[test]
    fn parses_resend_and_reply_prefixes() {
        let parsed = parse_patch_subject("Re: [PATCH RESEND v3 2/7] net: cleanup foo");
        assert!(parsed.had_reply_prefix);
        assert!(parsed.has_patch_tag);
        assert!(parsed.is_resend);
        assert_eq!(parsed.version_num, 3);
        assert_eq!(parsed.ordinal, Some(2));
        assert_eq!(parsed.total, Some(7));
        assert_eq!(parsed.subject_norm_base, "net: cleanup foo");
    }

    #[test]
    fn keeps_non_reply_subjects_marked_as_non_reply() {
        let parsed = parse_patch_subject("[PATCH 1/1] bpf: sample");
        assert!(!parsed.had_reply_prefix);
    }

    #[test]
    fn parses_compact_patchv_form() {
        let parsed = parse_patch_subject("[PATCHv4 6/5] test: compact");
        assert!(parsed.has_patch_tag);
        assert_eq!(parsed.version_num, 4);
        assert_eq!(parsed.ordinal, Some(6));
        assert_eq!(parsed.total, Some(6));
        assert!(!parsed.revision_inferred);
        assert!(!parsed.counters_inferred);
    }
}
