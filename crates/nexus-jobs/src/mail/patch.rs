//! Patch detection for email bodies.
//!
//! Detects diff/patch content in email bodies and extracts metadata about
//! patch regions, enabling frontends to display patches with syntax highlighting.
//! Detection patterns are adapted from the b4 project.

use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};

/// Matches git diff headers: `diff --git a/file b/file`
static DIFF_GIT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^diff --git [ab]/\S+ [ab]/\S+").expect("valid diff git regex")
});

/// Matches unified diff file markers: `--- a/file` or `--- file`
static DIFF_MINUS_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^--- .+").expect("valid diff minus regex"));

/// Matches unified diff file markers: `+++ b/file` or `+++ file`
static DIFF_PLUS_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\+\+\+ .+").expect("valid diff plus regex"));

/// Matches hunk headers: `@@ -10,5 +10,7 @@`
static HUNK_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^@@ -\d+(?:,\d+)? \+\d+(?:,\d+)? @@").expect("valid hunk regex"));

/// Matches binary patch markers
static BINARY_PATCH_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^GIT binary patch").expect("valid binary patch regex"));

/// Matches diffstat summary lines: `3 files changed, 10 insertions(+), 5 deletions(-)`
static DIFFSTAT_SUMMARY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^\s*\d+ files? changed").expect("valid diffstat summary regex")
});

/// Matches diffstat file lines: ` file.c | 10 ++++------`
static DIFFSTAT_LINE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^\s*\S+.*\|\s*\d+\s*[+-]*\s*$").expect("valid diffstat line regex")
});

/// Extracts filename from `diff --git a/FILE b/FILE`
static DIFF_GIT_FILENAME_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^diff --git [ab]/(\S+) [ab]/\S+").expect("valid diff git filename regex")
});

/// Type of patch region detected in the email body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatchRegionType {
    /// Standard unified diff content
    Diff,
    /// Diffstat summary block
    DiffStat,
    /// Git binary patch content
    BinaryPatch,
}

/// A contiguous region of patch content within an email body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PatchRegion {
    /// 1-indexed start line number (inclusive)
    pub start_line: u32,
    /// 1-indexed end line number (inclusive)
    pub end_line: u32,
    /// Type of patch content in this region
    #[serde(rename = "type")]
    pub region_type: PatchRegionType,
}

/// Metadata about patches detected in an email body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PatchMetadata {
    /// Whether the email contains any diff content
    pub has_patch: bool,
    /// Whether the email contains a diffstat summary
    pub has_diffstat: bool,
    /// Contiguous regions of patch content
    pub regions: Vec<PatchRegion>,
    /// Files modified by the patch (extracted from diff headers)
    pub files: Vec<String>,
}

/// Detect patches in an email body and return metadata about patch regions.
///
/// Returns `None` if no patch content is detected, to save storage space.
pub fn detect_patches(body: &str) -> Option<PatchMetadata> {
    let lines: Vec<&str> = body.lines().collect();
    if lines.is_empty() {
        return None;
    }

    let mut regions = Vec::new();
    let mut files = Vec::new();
    let mut has_patch = false;
    let mut has_diffstat = false;

    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        let line_num = (i + 1) as u32; // 1-indexed

        // Check for git diff header
        if DIFF_GIT_RE.is_match(line) {
            // Extract filename
            if let Some(caps) = DIFF_GIT_FILENAME_RE.captures(line) {
                if let Some(filename) = caps.get(1) {
                    let fname = filename.as_str().to_string();
                    if !files.contains(&fname) {
                        files.push(fname);
                    }
                }
            }

            // Scan forward to find the end of this diff block
            let start = line_num;
            let end = scan_diff_region(&lines, i);
            regions.push(PatchRegion {
                start_line: start,
                end_line: end as u32,
                region_type: PatchRegionType::Diff,
            });
            has_patch = true;
            i = end;
            continue;
        }

        // Check for unified diff without git header (--- followed by +++)
        if DIFF_MINUS_RE.is_match(line) {
            if i + 1 < lines.len() && DIFF_PLUS_RE.is_match(lines[i + 1]) {
                let start = line_num;
                let end = scan_diff_region(&lines, i);
                regions.push(PatchRegion {
                    start_line: start,
                    end_line: end as u32,
                    region_type: PatchRegionType::Diff,
                });
                has_patch = true;
                i = end;
                continue;
            }
        }

        // Check for binary patch
        if BINARY_PATCH_RE.is_match(line) {
            let start = line_num;
            let end = scan_binary_patch_region(&lines, i);
            regions.push(PatchRegion {
                start_line: start,
                end_line: end as u32,
                region_type: PatchRegionType::BinaryPatch,
            });
            has_patch = true;
            i = end;
            continue;
        }

        // Check for diffstat block
        if is_diffstat_start(&lines, i) {
            let start = line_num;
            let end = scan_diffstat_region(&lines, i);
            if end > i {
                regions.push(PatchRegion {
                    start_line: start,
                    end_line: end as u32,
                    region_type: PatchRegionType::DiffStat,
                });
                has_diffstat = true;
                i = end;
                continue;
            }
        }

        i += 1;
    }

    if regions.is_empty() {
        return None;
    }

    // Merge adjacent regions of the same type
    let regions = merge_adjacent_regions(regions);

    Some(PatchMetadata {
        has_patch,
        has_diffstat,
        regions,
        files,
    })
}

/// Scan forward from a diff start to find where the diff region ends.
/// Returns the 1-indexed end line number.
fn scan_diff_region(lines: &[&str], start: usize) -> usize {
    let mut i = start + 1;

    while i < lines.len() {
        let line = lines[i];

        // Continue if we're in diff content
        if is_diff_content_line(line) {
            i += 1;
            continue;
        }

        // Check if this starts a new diff (end current region)
        if DIFF_GIT_RE.is_match(line) {
            break;
        }

        // Check for --- that might start a new unified diff
        if DIFF_MINUS_RE.is_match(line) && i + 1 < lines.len() && DIFF_PLUS_RE.is_match(lines[i + 1])
        {
            break;
        }

        // Empty lines within a diff are allowed
        if line.is_empty() || line.chars().all(|c| c.is_whitespace()) {
            // Look ahead - if the next non-empty line is diff content, continue
            let mut j = i + 1;
            while j < lines.len()
                && (lines[j].is_empty() || lines[j].chars().all(|c| c.is_whitespace()))
            {
                j += 1;
            }
            if j < lines.len() && is_diff_content_line(lines[j]) {
                i += 1;
                continue;
            }
            // Otherwise, end the diff region before the blank line
            break;
        }

        // Non-diff content - end the region
        break;
    }

    // Return 1-indexed end line (the last line that was part of the diff)
    i.max(start + 1)
}

/// Check if a line is part of diff content (but not a new diff header).
fn is_diff_content_line(line: &str) -> bool {
    // Note: We explicitly exclude `diff --git` headers here because they
    // indicate the start of a NEW diff, not content within the current one.
    // The caller checks for new diff headers separately.

    // Unified diff markers (--- and +++ are part of diff content)
    if DIFF_MINUS_RE.is_match(line) || DIFF_PLUS_RE.is_match(line) {
        return true;
    }
    // Hunk header
    if HUNK_RE.is_match(line) {
        return true;
    }
    // Context line, addition, or deletion
    if line.starts_with(' ') || line.starts_with('+') || line.starts_with('-') {
        return true;
    }
    // Index line
    if line.starts_with("index ") {
        return true;
    }
    // File mode lines
    if line.starts_with("old mode ")
        || line.starts_with("new mode ")
        || line.starts_with("new file mode ")
        || line.starts_with("deleted file mode ")
    {
        return true;
    }
    // Rename/copy detection
    if line.starts_with("similarity index ")
        || line.starts_with("rename from ")
        || line.starts_with("rename to ")
        || line.starts_with("copy from ")
        || line.starts_with("copy to ")
    {
        return true;
    }
    // No newline at end of file marker
    if line.starts_with("\\ No newline at end of file") {
        return true;
    }

    false
}

/// Scan forward from a binary patch start to find where it ends.
fn scan_binary_patch_region(lines: &[&str], start: usize) -> usize {
    let mut i = start + 1;

    while i < lines.len() {
        let line = lines[i];

        // Binary patch content is base85 encoded, typically alphanumeric lines
        // End when we hit a blank line or non-base85 content
        if line.is_empty() {
            // Check if next line starts a new diff
            if i + 1 < lines.len() && DIFF_GIT_RE.is_match(lines[i + 1]) {
                break;
            }
            // Allow one blank line within binary data
            i += 1;
            continue;
        }

        // Check for new diff header
        if DIFF_GIT_RE.is_match(line) {
            break;
        }

        // Base85 lines or "literal/delta" markers
        if line.starts_with("literal ")
            || line.starts_with("delta ")
            || line.chars().all(|c| c.is_alphanumeric() || c == '+' || c == '/')
        {
            i += 1;
            continue;
        }

        // End of binary patch
        break;
    }

    i.max(start + 1)
}

/// Check if the current position starts a diffstat block.
fn is_diffstat_start(lines: &[&str], pos: usize) -> bool {
    // Look for diffstat file lines followed by a summary
    if !DIFFSTAT_LINE_RE.is_match(lines[pos]) {
        return false;
    }

    // Scan ahead to see if there's a summary line
    for i in pos..lines.len().min(pos + 50) {
        if DIFFSTAT_SUMMARY_RE.is_match(lines[i]) {
            return true;
        }
        // Stop if we hit a diff header
        if DIFF_GIT_RE.is_match(lines[i]) || DIFF_MINUS_RE.is_match(lines[i]) {
            break;
        }
    }

    false
}

/// Scan forward from a diffstat start to find where it ends.
fn scan_diffstat_region(lines: &[&str], start: usize) -> usize {
    let mut i = start;
    let mut found_summary = false;

    while i < lines.len() {
        let line = lines[i];

        if DIFFSTAT_LINE_RE.is_match(line) {
            i += 1;
            continue;
        }

        if DIFFSTAT_SUMMARY_RE.is_match(line) {
            found_summary = true;
            i += 1;
            break;
        }

        // Allow empty lines within diffstat
        if line.is_empty() || line.chars().all(|c| c.is_whitespace()) {
            i += 1;
            continue;
        }

        // Non-diffstat line
        break;
    }

    if found_summary {
        i
    } else {
        start
    }
}

/// Merge adjacent regions of the same type.
fn merge_adjacent_regions(mut regions: Vec<PatchRegion>) -> Vec<PatchRegion> {
    if regions.len() <= 1 {
        return regions;
    }

    regions.sort_by_key(|r| r.start_line);

    let mut merged = Vec::new();
    let mut current = regions[0].clone();

    for region in regions.into_iter().skip(1) {
        // Merge if same type and adjacent (within 2 lines to allow for blank lines)
        if region.region_type == current.region_type && region.start_line <= current.end_line + 2 {
            current.end_line = current.end_line.max(region.end_line);
        } else {
            merged.push(current);
            current = region;
        }
    }
    merged.push(current);

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_simple_unified_diff() {
        let body = r#"Here is my patch:

--- a/file.c
+++ b/file.c
@@ -10,5 +10,7 @@
 context line
-old line
+new line
 more context

That's it.
"#;

        let meta = detect_patches(body).expect("should detect patch");
        assert!(meta.has_patch);
        assert!(!meta.has_diffstat);
        assert_eq!(meta.regions.len(), 1);
        assert_eq!(meta.regions[0].region_type, PatchRegionType::Diff);
        assert_eq!(meta.regions[0].start_line, 3);
    }

    #[test]
    fn detect_git_format_patch() {
        let body = r#"From: Author <author@example.com>
Subject: [PATCH] Fix bug

This fixes a memory leak.

---
 drivers/net/ice.c | 5 ++---
 1 file changed, 2 insertions(+), 3 deletions(-)

diff --git a/drivers/net/ice.c b/drivers/net/ice.c
index abc123..def456 100644
--- a/drivers/net/ice.c
+++ b/drivers/net/ice.c
@@ -100,7 +100,6 @@ static int ice_init(void)
 {
-    leaked_ptr = malloc(100);
+    ptr = malloc(100);
+    free(ptr);
 }
"#;

        let meta = detect_patches(body).expect("should detect patch");
        assert!(meta.has_patch);
        assert!(meta.has_diffstat);
        assert_eq!(meta.files, vec!["drivers/net/ice.c"]);

        // Should have both diffstat and diff regions
        let diffstat_regions: Vec<_> = meta
            .regions
            .iter()
            .filter(|r| r.region_type == PatchRegionType::DiffStat)
            .collect();
        let diff_regions: Vec<_> = meta
            .regions
            .iter()
            .filter(|r| r.region_type == PatchRegionType::Diff)
            .collect();

        assert!(!diffstat_regions.is_empty(), "should have diffstat region");
        assert!(!diff_regions.is_empty(), "should have diff region");
    }

    #[test]
    fn detect_no_patch() {
        let body = "This is just a regular email.\n\nNo patches here.\n";
        assert!(detect_patches(body).is_none());
    }

    #[test]
    fn detect_binary_patch() {
        let body = r#"Binary file change:

diff --git a/image.png b/image.png
GIT binary patch
literal 1234
zcmV;@1
"#;

        let meta = detect_patches(body).expect("should detect patch");
        assert!(meta.has_patch);
    }

    #[test]
    fn detect_multi_file_patch() {
        let body = r#"
diff --git a/file1.c b/file1.c
--- a/file1.c
+++ b/file1.c
@@ -1 +1 @@
-old
+new

diff --git a/file2.h b/file2.h
--- a/file2.h
+++ b/file2.h
@@ -1 +1 @@
-old
+new
"#;

        let meta = detect_patches(body).expect("should detect patch");
        assert!(meta.has_patch);
        assert_eq!(meta.files.len(), 2);
        assert!(meta.files.contains(&"file1.c".to_string()));
        assert!(meta.files.contains(&"file2.h".to_string()));
    }

    #[test]
    fn empty_body_returns_none() {
        assert!(detect_patches("").is_none());
    }
}
