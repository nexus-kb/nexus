use std::collections::HashMap;
use std::io::Write;
use std::process::{Command, Stdio};

use sha2::{Digest, Sha256};

pub fn compute_patch_id_stable(diff_text: &str) -> Option<String> {
    let trimmed = diff_text.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut child = Command::new("git")
        .arg("patch-id")
        .arg("--stable")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;

    if let Some(stdin) = child.stdin.as_mut()
        && stdin.write_all(trimmed.as_bytes()).is_err()
    {
        return None;
    }

    let output = child.wait_with_output().ok()?;
    if !output.status.success() {
        return None;
    }

    parse_patch_id_output(&String::from_utf8_lossy(&output.stdout))
}

fn parse_patch_id_output(output: &str) -> Option<String> {
    output
        .split_whitespace()
        .next()
        .map(str::trim)
        .filter(|token| token.len() >= 7)
        .map(ToString::to_string)
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
    use std::io::Write;
    use std::process::{Command, Stdio};

    use super::{PatchIdMemo, compute_patch_id_stable};

    fn fixture_diff() -> &'static str {
        concat!(
            "diff --git a/foo.c b/foo.c\n",
            "index 1111111..2222222 100644\n",
            "--- a/foo.c\n",
            "+++ b/foo.c\n",
            "@@ -1 +1 @@\n",
            "-old\n",
            "+new\n"
        )
    }

    #[test]
    fn patch_id_matches_git_cli_output() {
        let expected = git_patch_id(fixture_diff()).expect("patch-id from git");
        let actual = compute_patch_id_stable(fixture_diff()).expect("patch-id from helper");
        assert_eq!(actual, expected);
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

    fn git_patch_id(diff_text: &str) -> Option<String> {
        let mut child = Command::new("git")
            .arg("patch-id")
            .arg("--stable")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;

        if let Some(stdin) = child.stdin.as_mut()
            && stdin.write_all(diff_text.as_bytes()).is_err()
        {
            return None;
        }

        let output = child.wait_with_output().ok()?;
        if !output.status.success() {
            return None;
        }

        String::from_utf8_lossy(&output.stdout)
            .split_whitespace()
            .next()
            .map(|v| v.to_string())
    }
}
