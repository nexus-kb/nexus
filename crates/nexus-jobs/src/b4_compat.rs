use mailparse::ParsedMail;
use once_cell::sync::Lazy;
use regex::Regex;

static B4_DIFF_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?mi)^(---.*\n\+\+\+|GIT binary patch|diff --git \w/\S+ \w/\S+)")
        .expect("valid b4 diff regex")
});

#[cfg(test)]
static B4_DIFFSTAT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?mi)^\s*\d+ file.*\d+ (insertion|deletion)").expect("valid b4 diffstat regex")
});

static FORMAT_PATCH_SEPARATOR_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?m)^---\s*$").expect("valid format-patch separator regex"));

pub fn normalize_line_endings(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

pub fn has_diff_markers(text: &str) -> bool {
    let normalized = normalize_line_endings(text);
    find_diff_start(&normalized).is_some()
}

#[cfg(test)]
pub fn has_diffstat_markers(text: &str) -> bool {
    B4_DIFFSTAT_RE.is_match(text)
}

pub fn extract_diff_text(text: &str) -> Option<String> {
    let normalized = normalize_line_endings(text);
    let start = find_diff_start(&normalized)?;
    let sliced = normalized[start..].trim_start_matches('\n').trim();
    if sliced.is_empty() {
        None
    } else {
        Some(sliced.to_string())
    }
}

pub fn extract_preferred_body(parsed: &ParsedMail<'_>) -> Option<String> {
    let mut preferred: Option<String> = None;
    let mut stack: Vec<&ParsedMail<'_>> = vec![parsed];

    while let Some(part) = stack.pop() {
        for sub in part.subparts.iter().rev() {
            stack.push(sub);
        }

        let mime = part.ctype.mimetype.to_ascii_lowercase();
        if !mime.contains("/plain") && !mime.contains("/x-patch") {
            continue;
        }

        let body = decode_part_body(part);
        if body.trim().is_empty() {
            continue;
        }

        if preferred.is_none() {
            preferred = Some(body.clone());
            continue;
        }

        if has_diff_markers(&body) {
            preferred = Some(body);
        }
    }

    preferred
        .or_else(|| Some(decode_part_body(parsed)))
        .filter(|body| !body.trim().is_empty())
}

fn find_diff_start(text: &str) -> Option<usize> {
    if let Some(found) = B4_DIFF_RE.find(text) {
        return Some(found.start());
    }

    for sep in FORMAT_PATCH_SEPARATOR_RE.find_iter(text) {
        let tail_start = sep.end();
        if let Some(found) = B4_DIFF_RE.find(&text[tail_start..]) {
            return Some(tail_start + found.start());
        }
    }

    None
}

fn decode_part_body(part: &ParsedMail<'_>) -> String {
    match part.get_body() {
        Ok(body) => body,
        Err(_) => part
            .get_body_raw()
            .map(|raw| String::from_utf8_lossy(&raw).to_string())
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use mailparse::parse_mail;

    use super::{
        extract_diff_text, extract_preferred_body, has_diff_markers, has_diffstat_markers,
        normalize_line_endings,
    };

    #[test]
    fn finds_format_patch_diff_after_separator() {
        let body = concat!(
            "Subject: [PATCH] demo\n\n",
            "commit text\n\n",
            "---\n",
            " 1 file changed, 1 insertion(+)\n\n",
            "diff --git a/a.txt b/a.txt\n",
            "index 1111111..2222222 100644\n",
            "--- a/a.txt\n",
            "+++ b/a.txt\n",
            "@@ -1 +1 @@\n",
            "-old\n",
            "+new\n"
        );
        let extracted = extract_diff_text(body).expect("diff expected");
        assert!(extracted.starts_with("diff --git a/a.txt b/a.txt"));
    }

    #[test]
    fn detects_diffstat() {
        assert!(has_diffstat_markers(
            " 2 files changed, 11 insertions(+), 3 deletions(-)"
        ));
    }

    #[test]
    fn normalizes_crlf() {
        assert_eq!(normalize_line_endings("a\r\nb\rc"), "a\nb\nc");
    }

    #[test]
    fn prefers_patch_carrying_mime_part() {
        let raw = concat!(
            "From: Alice <alice@example.com>\r\n",
            "Message-ID: <m1@example.com>\r\n",
            "Date: Mon, 01 Jan 2024 00:00:00 +0000\r\n",
            "Subject: [PATCH] demo\r\n",
            "MIME-Version: 1.0\r\n",
            "Content-Type: multipart/mixed; boundary=\"x\"\r\n",
            "\r\n",
            "--x\r\n",
            "Content-Type: text/plain; charset=utf-8\r\n",
            "\r\n",
            "cover text\r\n",
            "--x\r\n",
            "Content-Type: text/x-patch; charset=utf-8\r\n",
            "\r\n",
            "diff --git a/a b/a\r\n",
            "--- a/a\r\n",
            "+++ b/a\r\n",
            "@@ -1 +1 @@\r\n",
            "-old\r\n",
            "+new\r\n",
            "--x--\r\n"
        );
        let parsed = parse_mail(raw.as_bytes()).expect("mime parses");
        let body = extract_preferred_body(&parsed).expect("preferred body");
        assert!(has_diff_markers(&body));
    }
}
