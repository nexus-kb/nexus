use crate::b4_compat;

pub fn has_diff_markers(text: &str) -> bool {
    b4_compat::has_diff_markers(text)
}

pub fn extract_diff_text(text: &str) -> Option<String> {
    b4_compat::extract_diff_text(text)
}

pub fn normalize_line_endings(text: &str) -> String {
    b4_compat::normalize_line_endings(text)
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
