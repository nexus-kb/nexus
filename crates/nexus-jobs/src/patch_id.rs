use std::collections::HashMap;

use once_cell::sync::Lazy;
use regex::Regex;
use sha1::{Digest, Sha1};
use sha2::Sha256;

use crate::b4_compat::normalize_line_endings;

// Mirrors b4's hunk normalization semantics used for patch hashing.
static HUNK_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^@@ -\d+(?:,(\d+))? \+\d+(?:,(\d+))? @@").expect("valid hunk re"));
static FILENAME_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(---|\+\+\+) (\S+)").expect("valid filename re"));

pub fn compute_patch_id_stable(diff_text: &str) -> Option<String> {
    let canonical = canonicalize_patch(diff_text);
    if canonical.is_empty() {
        return None;
    }

    let mut hasher = Sha1::new();
    hasher.update(canonical.as_bytes());
    let digest = hasher.finalize();
    Some(format!("{digest:x}"))
}

fn canonicalize_patch(diff_text: &str) -> String {
    let text = normalize_line_endings(diff_text);
    let mut out = String::new();

    for line in text.lines() {
        if line.is_empty() {
            continue;
        }

        if let Some(caps) = FILENAME_RE.captures(line) {
            let marker = caps.get(1).map(|v| v.as_str()).unwrap_or_default();
            let mut filename = caps
                .get(2)
                .map(|v| v.as_str())
                .unwrap_or_default()
                .trim_matches('"')
                .to_string();

            if filename != "/dev/null" {
                filename = normalize_path_for_hash(filename);
            }
            out.push_str(marker);
            out.push(' ');
            out.push_str(&filename);
            out.push('\n');
            continue;
        }

        if let Some(caps) = HUNK_RE.captures(line) {
            let old_count = caps
                .get(1)
                .and_then(|v| v.as_str().parse::<i32>().ok())
                .unwrap_or(1);
            let new_count = caps
                .get(2)
                .and_then(|v| v.as_str().parse::<i32>().ok())
                .unwrap_or(1);
            out.push_str(&format!("@@ -{old_count} +{new_count} @@\n"));
            continue;
        }

        if line.starts_with('+') || line.starts_with('-') || line.starts_with(' ') {
            if line.starts_with("+++") || line.starts_with("---") {
                continue;
            }
            // Keep semantic content, normalize whitespace noise away.
            let squashed = line
                .chars()
                .filter(|ch| !ch.is_whitespace())
                .collect::<String>();
            if !squashed.is_empty() {
                out.push_str(&squashed);
                out.push('\n');
            }
        }
    }

    out
}

fn normalize_path_for_hash(path: String) -> String {
    if let Some(stripped) = path.strip_prefix("a/") {
        return format!("a/{}", strip_first_dir(stripped));
    }
    if let Some(stripped) = path.strip_prefix("b/") {
        return format!("b/{}", strip_first_dir(stripped));
    }
    path
}

fn strip_first_dir(path: &str) -> String {
    let mut parts = path.splitn(2, '/');
    let _ = parts.next();
    match parts.next() {
        Some(rest) if !rest.is_empty() => rest.to_string(),
        _ => path.to_string(),
    }
}

#[derive(Debug, Default)]
pub struct PatchIdMemo {
    cache: HashMap<String, Option<String>>,
}

impl PatchIdMemo {
    pub fn compute(&mut self, diff_text: &str) -> Option<String> {
        let key = hash_key(diff_text);
        if let Some(existing) = self.cache.get(&key) {
            return existing.clone();
        }

        let computed = compute_patch_id_stable(diff_text);
        self.cache.insert(key, computed.clone());
        computed
    }
}

fn hash_key(diff_text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(diff_text.as_bytes());
    let digest = hasher.finalize();
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::{PatchIdMemo, compute_patch_id_stable};

    fn fixture_diff() -> &'static str {
        concat!(
            "diff --git a/foo.c b/foo.c\n",
            "index 1111111..2222222 100644\n",
            "--- a/foo.c\n",
            "+++ b/foo.c\n",
            "@@ -11,2 +21,2 @@\n",
            "-old value\n",
            "+new value\n",
            " unchanged\n"
        )
    }

    #[test]
    fn memoization_returns_stable_value() {
        let mut memo = PatchIdMemo::default();
        let first = memo.compute(fixture_diff());
        let second = memo.compute(fixture_diff());
        assert_eq!(first, second);
    }

    #[test]
    fn empty_diff_returns_none() {
        assert!(compute_patch_id_stable("\n\n").is_none());
    }

    #[test]
    fn normalizes_line_endings() {
        let a = compute_patch_id_stable(
            "diff --git a/a b/a\r\n--- a/a\r\n+++ b/a\r\n@@ -1 +1 @@\r\n-x\r\n+y\r\n",
        )
        .expect("hash");
        let b =
            compute_patch_id_stable("diff --git a/a b/a\n--- a/a\n+++ b/a\n@@ -1 +1 @@\n-x\n+y\n")
                .expect("hash");
        assert_eq!(a, b);
    }

    #[test]
    fn ignores_hunk_line_numbers() {
        let a = compute_patch_id_stable(
            "diff --git a/a b/a\n--- a/a\n+++ b/a\n@@ -1,2 +1,2 @@\n-x\n+y\n z\n",
        )
        .expect("hash");
        let b = compute_patch_id_stable(
            "diff --git a/a b/a\n--- a/a\n+++ b/a\n@@ -100,2 +200,2 @@\n-x\n+y\n z\n",
        )
        .expect("hash");
        assert_eq!(a, b);
    }
}
