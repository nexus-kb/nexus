use once_cell::sync::Lazy;
use regex::Regex;

static DIFF_LINE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(diff --git |---\s+\S|\+\+\+\s+\S|GIT binary patch|Index: \S)")
        .expect("valid diff line regex")
});

static FORMAT_PATCH_SEPARATOR_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?m)^---\s*$").expect("valid separator regex"));

pub fn has_diff_markers(text: &str) -> bool {
    find_diff_start(text).is_some()
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

pub fn normalize_line_endings(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

fn find_diff_start(text: &str) -> Option<usize> {
    let direct = find_first_diff_line_offset(text, 0);
    if direct.is_some() {
        return direct;
    }

    if let Some(separator) = FORMAT_PATCH_SEPARATOR_RE.find(text) {
        let start = separator.end();
        return find_first_diff_line_offset(text, start);
    }

    None
}

fn find_first_diff_line_offset(text: &str, from: usize) -> Option<usize> {
    let mut offset = from;
    let haystack = &text[from..];

    for line in haystack.split_inclusive('\n') {
        let line_no_newline = line.trim_end_matches('\n');
        if DIFF_LINE_RE.is_match(line_no_newline) {
            return Some(offset);
        }
        offset += line.len();
    }

    let trailing = haystack.trim_end_matches('\n');
    if !trailing.is_empty() {
        let final_line_start = text.len().saturating_sub(trailing.len());
        if DIFF_LINE_RE.is_match(trailing) {
            return Some(final_line_start);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{extract_diff_text, has_diff_markers};

    #[test]
    fn detects_inline_unified_diff() {
        let body = "hello\n\n--- a/foo.c\n+++ b/foo.c\n@@ -1 +1 @@\n-old\n+new\n";
        assert!(has_diff_markers(body));
        let diff = extract_diff_text(body).expect("diff text");
        assert!(diff.starts_with("--- a/foo.c"));
    }

    #[test]
    fn detects_format_patch_separator_and_diff() {
        let body = concat!(
            "From abcdef Mon Sep 17 00:00:00 2001\n",
            "Subject: [PATCH] demo\n",
            "\n",
            "commit message\n",
            "\n",
            "---\n",
            " foo.c | 2 +-\n",
            " 1 file changed, 1 insertion(+), 1 deletion(-)\n",
            "\n",
            "diff --git a/foo.c b/foo.c\n",
            "index 1111111..2222222 100644\n",
            "--- a/foo.c\n",
            "+++ b/foo.c\n",
            "@@ -1 +1 @@\n",
            "-old\n",
            "+new\n",
        );

        let diff = extract_diff_text(body).expect("diff text");
        assert!(diff.starts_with("diff --git a/foo.c b/foo.c"));
    }

    #[test]
    fn returns_none_for_non_patch_text() {
        let body = "this is a plain email body without diff markers";
        assert!(!has_diff_markers(body));
        assert!(extract_diff_text(body).is_none());
    }
}
