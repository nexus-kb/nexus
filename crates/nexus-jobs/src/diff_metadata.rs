#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedFileDiff {
    pub old_path: Option<String>,
    pub new_path: String,
    pub change_type: String,
    pub is_binary: bool,
    pub additions: i32,
    pub deletions: i32,
    pub hunk_count: i32,
    pub diff_start: i32,
    pub diff_end: i32,
}

pub fn parse_diff_metadata(diff_text: &str) -> Vec<ParsedFileDiff> {
    if diff_text.trim().is_empty() {
        return Vec::new();
    }

    let starts = find_block_starts(diff_text);
    if starts.is_empty() {
        return parse_fallback_block(diff_text).into_iter().collect();
    }

    let mut out = Vec::new();
    for (idx, start) in starts.iter().enumerate() {
        let end = starts.get(idx + 1).copied().unwrap_or(diff_text.len());
        if let Some(parsed) = parse_block(diff_text, *start, end) {
            out.push(parsed);
        }
    }
    out
}

fn find_block_starts(text: &str) -> Vec<usize> {
    let mut starts = Vec::new();
    let mut offset = 0usize;
    for line in text.split_inclusive('\n') {
        if line.starts_with("diff --git ") {
            starts.push(offset);
        }
        offset += line.len();
    }

    if !text.ends_with('\n')
        && let Some(last_line) = text.lines().last()
        && last_line.starts_with("diff --git ")
    {
        let start = text.len().saturating_sub(last_line.len());
        if starts.last().copied() != Some(start) {
            starts.push(start);
        }
    }

    starts
}

fn parse_block(text: &str, start: usize, end: usize) -> Option<ParsedFileDiff> {
    if start >= end || end > text.len() {
        return None;
    }

    let block = &text[start..end];
    let mut lines = block.lines();
    let header = lines.next().unwrap_or_default();
    let (mut old_path, mut new_path) = parse_diff_git_header(header);

    let mut change_type = "M".to_string();
    let mut is_binary = false;
    let mut additions = 0i32;
    let mut deletions = 0i32;
    let mut hunk_count = 0i32;

    for line in lines {
        if let Some(value) = line.strip_prefix("rename from ") {
            old_path = Some(value.trim().to_string());
            change_type = "R".to_string();
            continue;
        }
        if let Some(value) = line.strip_prefix("rename to ") {
            new_path = Some(value.trim().to_string());
            change_type = "R".to_string();
            continue;
        }
        if let Some(value) = line.strip_prefix("copy from ") {
            old_path = Some(value.trim().to_string());
            change_type = "C".to_string();
            continue;
        }
        if let Some(value) = line.strip_prefix("copy to ") {
            new_path = Some(value.trim().to_string());
            change_type = "C".to_string();
            continue;
        }
        if line.starts_with("new file mode ") {
            change_type = "A".to_string();
            continue;
        }
        if line.starts_with("deleted file mode ") {
            change_type = "D".to_string();
            continue;
        }
        if line.starts_with("GIT binary patch") || line.starts_with("Binary files ") {
            is_binary = true;
            continue;
        }
        if let Some(value) = line.strip_prefix("--- ") {
            old_path = parse_marker_path(value);
            continue;
        }
        if let Some(value) = line.strip_prefix("+++ ") {
            new_path = parse_marker_path(value);
            continue;
        }
        if line.starts_with("@@") {
            hunk_count += 1;
            continue;
        }
        if line.starts_with('+') && !line.starts_with("+++") {
            additions += 1;
            continue;
        }
        if line.starts_with('-') && !line.starts_with("---") {
            deletions += 1;
        }
    }

    if change_type == "M" {
        if old_path.is_none() && new_path.is_some() {
            change_type = "A".to_string();
        } else if new_path.is_none() && old_path.is_some() {
            change_type = "D".to_string();
        } else if is_binary {
            change_type = "B".to_string();
        }
    }
    if is_binary && change_type == "M" {
        change_type = "B".to_string();
    }

    let old_path = old_path.map(normalize_path_value);
    let mut new_path = new_path.map(normalize_path_value);
    if new_path.is_none() && old_path.is_some() {
        new_path = old_path.clone();
    }
    let new_path = new_path?;

    Some(ParsedFileDiff {
        old_path,
        new_path,
        change_type,
        is_binary,
        additions,
        deletions,
        hunk_count,
        diff_start: start as i32,
        diff_end: end as i32,
    })
}

fn parse_fallback_block(text: &str) -> Option<ParsedFileDiff> {
    let mut old_path = None;
    let mut new_path = None;
    let mut change_type = "M".to_string();
    let mut is_binary = false;
    let mut additions = 0i32;
    let mut deletions = 0i32;
    let mut hunk_count = 0i32;

    for line in text.lines() {
        if let Some(value) = line.strip_prefix("Index: ") {
            let path = value.trim();
            if !path.is_empty() {
                let normalized = normalize_path_value(path.to_string());
                if old_path.is_none() {
                    old_path = Some(normalized.clone());
                }
                if new_path.is_none() {
                    new_path = Some(normalized);
                }
            }
            continue;
        }
        if let Some(value) = line.strip_prefix("--- ") {
            old_path = parse_marker_path(value).map(normalize_path_value);
            continue;
        }
        if let Some(value) = line.strip_prefix("+++ ") {
            new_path = parse_marker_path(value).map(normalize_path_value);
            continue;
        }
        if line.starts_with("GIT binary patch") || line.starts_with("Binary files ") {
            is_binary = true;
            continue;
        }
        if line.starts_with("@@") {
            hunk_count += 1;
            continue;
        }
        if line.starts_with('+') && !line.starts_with("+++") {
            additions += 1;
            continue;
        }
        if line.starts_with('-') && !line.starts_with("---") {
            deletions += 1;
        }
    }

    if old_path.is_none() && new_path.is_some() {
        change_type = "A".to_string();
    } else if new_path.is_none() && old_path.is_some() {
        change_type = "D".to_string();
    } else if is_binary {
        change_type = "B".to_string();
    }

    if new_path.is_none() && old_path.is_some() {
        new_path = old_path.clone();
    }
    let new_path = new_path?;

    Some(ParsedFileDiff {
        old_path,
        new_path,
        change_type,
        is_binary,
        additions,
        deletions,
        hunk_count,
        diff_start: 0,
        diff_end: text.len() as i32,
    })
}

fn parse_diff_git_header(line: &str) -> (Option<String>, Option<String>) {
    let rest = line.strip_prefix("diff --git ").unwrap_or_default().trim();
    let tokens = split_quoted_tokens(rest);
    if tokens.len() < 2 {
        return (None, None);
    }

    (
        normalize_git_header_path(&tokens[0], "a/"),
        normalize_git_header_path(&tokens[1], "b/"),
    )
}

fn split_quoted_tokens(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escaped = false;

    for ch in input.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        if ch == '\\' && in_quotes {
            escaped = true;
            continue;
        }

        if ch == '"' {
            in_quotes = !in_quotes;
            continue;
        }

        if ch.is_whitespace() && !in_quotes {
            if !current.is_empty() {
                out.push(std::mem::take(&mut current));
            }
            continue;
        }

        current.push(ch);
    }

    if !current.is_empty() {
        out.push(current);
    }

    out
}

fn normalize_git_header_path(token: &str, prefix: &str) -> Option<String> {
    let trimmed = token.trim().trim_matches('"');
    if trimmed == "/dev/null" {
        return None;
    }

    if let Some(path) = trimmed.strip_prefix(prefix) {
        return Some(path.to_string());
    }

    Some(trimmed.to_string())
}

fn parse_marker_path(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let token = if trimmed.starts_with('"') {
        let without_quote = trimmed.strip_prefix('"').unwrap_or(trimmed);
        let end = without_quote.find('"').unwrap_or(without_quote.len());
        &without_quote[..end]
    } else {
        trimmed.split_whitespace().next().unwrap_or_default()
    };

    if token.is_empty() || token == "/dev/null" {
        return None;
    }

    Some(token.to_string())
}

fn normalize_path_value(path: String) -> String {
    path.trim()
        .trim_matches('"')
        .strip_prefix("a/")
        .or_else(|| path.trim().trim_matches('"').strip_prefix("b/"))
        .unwrap_or_else(|| path.trim().trim_matches('"'))
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::parse_diff_metadata;

    #[test]
    fn parses_rename_with_edits() {
        let diff = concat!(
            "diff --git a/old.c b/new.c\n",
            "similarity index 88%\n",
            "rename from old.c\n",
            "rename to new.c\n",
            "index 1111111..2222222 100644\n",
            "--- a/old.c\n",
            "+++ b/new.c\n",
            "@@ -1 +1 @@\n",
            "-old\n",
            "+new\n",
        );

        let files = parse_diff_metadata(diff);
        assert_eq!(files.len(), 1);
        let file = &files[0];
        assert_eq!(file.change_type, "R");
        assert_eq!(file.old_path.as_deref(), Some("old.c"));
        assert_eq!(file.new_path, "new.c");
        assert_eq!(file.additions, 1);
        assert_eq!(file.deletions, 1);
        assert_eq!(file.hunk_count, 1);
    }

    #[test]
    fn parses_new_file_and_delete_file() {
        let diff = concat!(
            "diff --git a/dev/null b/foo.txt\n",
            "new file mode 100644\n",
            "index 0000000..1111111\n",
            "--- /dev/null\n",
            "+++ b/foo.txt\n",
            "@@ -0,0 +1 @@\n",
            "+hello\n",
            "diff --git a/bar.txt b/dev/null\n",
            "deleted file mode 100644\n",
            "index 1111111..0000000\n",
            "--- a/bar.txt\n",
            "+++ /dev/null\n",
            "@@ -1 +0,0 @@\n",
            "-bye\n",
        );

        let files = parse_diff_metadata(diff);
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].change_type, "A");
        assert_eq!(files[0].new_path, "foo.txt");
        assert_eq!(files[1].change_type, "D");
        assert_eq!(files[1].old_path.as_deref(), Some("bar.txt"));
        assert_eq!(files[1].new_path, "bar.txt");
    }

    #[test]
    fn parses_binary_marker() {
        let diff = concat!(
            "diff --git a/logo.png b/logo.png\n",
            "index 3f1f1f1..4a2a2a2 100644\n",
            "Binary files a/logo.png and b/logo.png differ\n",
        );

        let files = parse_diff_metadata(diff);
        assert_eq!(files.len(), 1);
        assert!(files[0].is_binary);
        assert_eq!(files[0].change_type, "B");
    }

    #[test]
    fn parses_multi_file_diff_and_offsets_match_slices() {
        let diff = concat!(
            "diff --git a/a.c b/a.c\n",
            "index 1111111..2222222 100644\n",
            "--- a/a.c\n",
            "+++ b/a.c\n",
            "@@ -1 +1 @@\n",
            "-one\n",
            "+two\n",
            "diff --git a/b.c b/b.c\n",
            "index 3333333..4444444 100644\n",
            "--- a/b.c\n",
            "+++ b/b.c\n",
            "@@ -1 +1 @@\n",
            "-left\n",
            "+right\n",
        );

        let files = parse_diff_metadata(diff);
        assert_eq!(files.len(), 2);
        for file in &files {
            assert!(file.diff_start >= 0);
            assert!(file.diff_end > file.diff_start);
            let slice = &diff[file.diff_start as usize..file.diff_end as usize];
            assert!(slice.starts_with("diff --git "));
            assert!(slice.contains(&format!(" b/{}", file.new_path)));
        }
    }

    #[test]
    fn parses_fallback_without_diff_git_header() {
        let diff = concat!(
            "--- a/legacy.txt\n",
            "+++ b/legacy.txt\n",
            "@@ -1 +1 @@\n",
            "-a\n",
            "+b\n",
        );

        let files = parse_diff_metadata(diff);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].new_path, "legacy.txt");
        assert_eq!(files[0].hunk_count, 1);
    }
}
