use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use axum::Json;
use chrono::{DateTime, Utc};
use nexus_db::{PatchItemDetailRecord, SeriesExportMessageRecord};
use serde::{Deserialize, Serialize};

use crate::state::ApiState;

#[derive(Debug, Serialize)]
pub struct PatchItemResponse {
    pub patch_item_id: i64,
    pub patch_series_id: i64,
    pub patch_series_version_id: i64,
    pub ordinal: i32,
    pub total: Option<i32>,
    pub item_type: String,
    pub subject_raw: String,
    pub subject_norm: String,
    pub commit_subject: Option<String>,
    pub commit_subject_norm: Option<String>,
    pub message_pk: i64,
    pub message_id_primary: String,
    pub patch_id_stable: Option<String>,
    pub has_diff: bool,
    pub file_count: i32,
    pub additions: i32,
    pub deletions: i32,
    pub hunk_count: i32,
}

#[derive(Debug, Serialize)]
pub struct PatchItemFileResponse {
    pub patch_item_id: i64,
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

#[derive(Debug, Serialize)]
pub struct PatchItemDiffResponse {
    pub patch_item_id: i64,
    pub diff_text: String,
}

#[derive(Debug, Serialize)]
pub struct PatchItemFileDiffResponse {
    pub patch_item_id: i64,
    pub path: String,
    pub diff_text: String,
}

#[derive(Debug, Deserialize)]
pub struct MessageBodyQuery {
    #[serde(default)]
    pub include_diff: Option<bool>,
    #[serde(default)]
    pub strip_quotes: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct MessageBodyResponse {
    pub message_pk: i64,
    pub subject_raw: String,
    pub body_text: Option<String>,
    pub diff_text: Option<String>,
    pub has_diff: bool,
    pub has_attachments: bool,
}

#[derive(Debug, Serialize)]
pub struct ThreadMessageParticipant {
    pub name: Option<String>,
    pub email: String,
}

#[derive(Debug, Serialize)]
pub struct ThreadMessageEntry {
    pub message_id: i64,
    pub parent_message_id: Option<i64>,
    pub depth: i32,
    pub sort_key: String,
    pub from: ThreadMessageParticipant,
    pub date_utc: Option<DateTime<Utc>>,
    pub subject: String,
    pub has_diff: bool,
    pub patch_item_id: Option<i64>,
    pub snippet: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ThreadDetailResponse {
    pub thread_id: i64,
    pub list_key: String,
    pub subject: String,
    pub membership_hash: String,
    pub last_activity_at: DateTime<Utc>,
    pub messages: Vec<ThreadMessageEntry>,
}

#[derive(Debug, Deserialize)]
pub struct ExportMboxQuery {
    #[serde(default)]
    pub assembled: Option<bool>,
    #[serde(default)]
    pub include_cover: Option<bool>,
}

pub async fn patch_item(
    State(state): State<ApiState>,
    Path(patch_item_id): Path<i64>,
) -> Result<Json<PatchItemResponse>, StatusCode> {
    let Some(item) = state
        .lineage
        .get_patch_item_detail(patch_item_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    Ok(Json(map_patch_item(item)))
}

pub async fn patch_item_files(
    State(state): State<ApiState>,
    Path(patch_item_id): Path<i64>,
) -> Result<Json<Vec<PatchItemFileResponse>>, StatusCode> {
    let exists = state
        .lineage
        .get_patch_item_detail(patch_item_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();
    if !exists {
        return Err(StatusCode::NOT_FOUND);
    }

    let files = state
        .lineage
        .list_patch_item_files(patch_item_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .map(|file| PatchItemFileResponse {
            patch_item_id: file.patch_item_id,
            old_path: file.old_path,
            new_path: file.new_path,
            change_type: file.change_type,
            is_binary: file.is_binary,
            additions: file.additions,
            deletions: file.deletions,
            hunk_count: file.hunk_count,
            diff_start: file.diff_start,
            diff_end: file.diff_end,
        })
        .collect();

    Ok(Json(files))
}

#[derive(Debug, Deserialize)]
pub struct PatchItemFileDiffPath {
    pub patch_item_id: i64,
    pub path: String,
}

pub async fn patch_item_file_diff(
    State(state): State<ApiState>,
    Path(path): Path<PatchItemFileDiffPath>,
) -> Result<Json<PatchItemFileDiffResponse>, StatusCode> {
    let Some(source) = state
        .lineage
        .get_patch_item_file_diff_source(path.patch_item_id, &path.path)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let diff_text = source.diff_text.ok_or(StatusCode::NOT_FOUND)?;
    let sliced = slice_by_offsets(&diff_text, source.diff_start, source.diff_end)
        .ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;

    Ok(Json(PatchItemFileDiffResponse {
        patch_item_id: path.patch_item_id,
        path: source.new_path,
        diff_text: sliced,
    }))
}

pub async fn patch_item_diff(
    State(state): State<ApiState>,
    Path(patch_item_id): Path<i64>,
) -> Result<Json<PatchItemDiffResponse>, StatusCode> {
    let Some(diff_text) = state
        .lineage
        .get_patch_item_full_diff(patch_item_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    Ok(Json(PatchItemDiffResponse {
        patch_item_id,
        diff_text,
    }))
}

pub async fn message_body(
    State(state): State<ApiState>,
    Path(message_id): Path<i64>,
    Query(query): Query<MessageBodyQuery>,
) -> Result<Json<MessageBodyResponse>, StatusCode> {
    let include_diff = query.include_diff.unwrap_or(false);
    let strip_quotes = query.strip_quotes.unwrap_or(false);

    let Some(record) = state
        .lineage
        .get_message_body(message_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let body_text = record
        .body_text
        .as_deref()
        .map(|body| strip_quoted_lines(body, strip_quotes));

    Ok(Json(MessageBodyResponse {
        message_pk: record.message_pk,
        subject_raw: record.subject_raw,
        body_text,
        diff_text: if include_diff { record.diff_text } else { None },
        has_diff: record.has_diff,
        has_attachments: record.has_attachments,
    }))
}

#[derive(Debug, Deserialize)]
pub struct ThreadPath {
    pub list_key: String,
    pub thread_id: i64,
}

pub async fn list_thread_detail(
    State(state): State<ApiState>,
    Path(path): Path<ThreadPath>,
) -> Result<Json<ThreadDetailResponse>, StatusCode> {
    let Some(list) = state
        .catalog
        .get_mailing_list(&path.list_key)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let Some(summary) = state
        .lineage
        .get_thread_summary(list.id, path.thread_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let messages = state
        .lineage
        .list_thread_messages(list.id, path.thread_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .map(|msg| ThreadMessageEntry {
            message_id: msg.message_pk,
            parent_message_id: msg.parent_message_pk,
            depth: msg.depth,
            sort_key: hex_encode(&msg.sort_key),
            from: ThreadMessageParticipant {
                name: msg.from_name,
                email: msg.from_email,
            },
            date_utc: msg.date_utc,
            subject: msg.subject_raw,
            has_diff: msg.has_diff,
            patch_item_id: msg.patch_item_id,
            snippet: build_snippet(msg.body_text.as_deref()),
        })
        .collect();

    Ok(Json(ThreadDetailResponse {
        thread_id: path.thread_id,
        list_key: path.list_key,
        subject: summary.subject_norm,
        membership_hash: hex_encode(&summary.membership_hash),
        last_activity_at: summary.last_activity_at,
        messages,
    }))
}

#[derive(Debug, Deserialize)]
pub struct SeriesVersionPath {
    pub series_id: i64,
    pub series_version_id: i64,
}

pub async fn series_version_export_mbox(
    State(state): State<ApiState>,
    Path(path): Path<SeriesVersionPath>,
    Query(query): Query<ExportMboxQuery>,
) -> Result<Response, StatusCode> {
    let assembled = query.assembled.unwrap_or(true);
    let include_cover = query.include_cover.unwrap_or(false);

    let Some(version) = state
        .lineage
        .get_series_version(path.series_id, path.series_version_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let messages = state
        .lineage
        .list_export_messages(path.series_id, version.id, assembled, include_cover)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if messages.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    let mbox = render_mbox(&messages);
    let filename = format!(
        "{}-v{}.mbox",
        slugify_for_filename(&version.subject_norm),
        version.version_num
    );

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/mbox")
        .header(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_str(&format!("attachment; filename=\"{filename}\""))
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        )
        .body(Body::from(mbox))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(response)
}

fn map_patch_item(item: PatchItemDetailRecord) -> PatchItemResponse {
    PatchItemResponse {
        patch_item_id: item.patch_item_id,
        patch_series_id: item.patch_series_id,
        patch_series_version_id: item.patch_series_version_id,
        ordinal: item.ordinal,
        total: item.total,
        item_type: item.item_type,
        subject_raw: item.subject_raw,
        subject_norm: item.subject_norm,
        commit_subject: item.commit_subject,
        commit_subject_norm: item.commit_subject_norm,
        message_pk: item.message_pk,
        message_id_primary: item.message_id_primary,
        patch_id_stable: item.patch_id_stable,
        has_diff: item.has_diff,
        file_count: item.file_count,
        additions: item.additions,
        deletions: item.deletions,
        hunk_count: item.hunk_count,
    }
}

fn slice_by_offsets(text: &str, start: i32, end: i32) -> Option<String> {
    if start < 0 || end < 0 || end < start {
        return None;
    }
    let start = start as usize;
    let end = end as usize;
    let bytes = text.as_bytes();
    if start > bytes.len() || end > bytes.len() {
        return None;
    }
    Some(String::from_utf8_lossy(&bytes[start..end]).to_string())
}

fn strip_quoted_lines(body: &str, strip_quotes: bool) -> String {
    if !strip_quotes {
        return body.to_string();
    }

    let mut out = String::new();
    for line in body.lines() {
        if line.trim_start().starts_with('>') {
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out.trim_end().to_string()
}

fn build_snippet(body: Option<&str>) -> Option<String> {
    let body = body?.trim();
    if body.is_empty() {
        return None;
    }

    let collapsed = body.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return None;
    }
    let char_count = collapsed.chars().count();
    if char_count <= 220 {
        return Some(collapsed);
    }
    Some(format!("{}...", collapsed.chars().take(220).collect::<String>()))
}

fn render_mbox(messages: &[SeriesExportMessageRecord]) -> Vec<u8> {
    let mut out = Vec::new();

    for msg in messages {
        let date_part = msg
            .date_utc
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).expect("epoch"))
            .format("%a %b %e %H:%M:%S %Y")
            .to_string();
        let separator = format!("From {} {}\n", msg.from_email, date_part);
        out.extend_from_slice(separator.as_bytes());

        let normalized = normalize_newlines(&String::from_utf8_lossy(&msg.raw_rfc822));
        for line in normalized.lines() {
            if line.starts_with("From ") {
                out.extend_from_slice(b">");
            }
            out.extend_from_slice(line.as_bytes());
            out.push(b'\n');
        }
        out.push(b'\n');
    }

    out
}

fn normalize_newlines(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

fn slugify_for_filename(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if (ch == '-' || ch == '_' || ch == '.') && !out.ends_with(ch) {
            out.push(ch);
        } else if !out.ends_with('-') {
            out.push('-');
        }
    }
    out.trim_matches('-')
        .chars()
        .take(80)
        .collect::<String>()
        .if_empty_then("series")
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

trait IfEmptyThen {
    fn if_empty_then(self, fallback: &str) -> String;
}

impl IfEmptyThen for String {
    fn if_empty_then(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::process::Command;

    use chrono::Utc;
    use nexus_db::SeriesExportMessageRecord;

    use super::{render_mbox, slice_by_offsets, strip_quoted_lines};

    #[test]
    fn slices_diff_using_offsets() {
        let diff = concat!(
            "diff --git a/foo.c b/foo.c\n",
            "--- a/foo.c\n",
            "+++ b/foo.c\n",
            "@@ -1 +1 @@\n",
            "-old\n",
            "+new\n",
        );
        let sliced = slice_by_offsets(diff, 0, diff.len() as i32).expect("slice");
        assert_eq!(sliced, diff);
    }

    #[test]
    fn strips_quoted_lines_when_requested() {
        let body = "hello\n> quote\nworld\n";
        assert_eq!(strip_quoted_lines(body, false), body);
        assert_eq!(strip_quoted_lines(body, true), "hello\nworld");
    }

    #[test]
    fn rendered_mbox_applies_with_git_am() {
        if Command::new("git").arg("--version").output().is_err() {
            return;
        }

        let temp_root = std::env::temp_dir().join(format!(
            "nexus-mbox-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default().abs()
        ));
        fs::create_dir_all(&temp_root).expect("create temp root");

        init_repo_with_patch(&temp_root);
        let raw_patch = run_git(
            &temp_root,
            &["format-patch", "-1", "--stdout"],
            "format patch",
        );

        run_git(&temp_root, &["reset", "--hard", "HEAD~1"], "reset to pre-patch");

        let rendered = render_mbox(&[SeriesExportMessageRecord {
            ordinal: 1,
            item_type: "patch".to_string(),
            message_pk: 1,
            from_email: "dev@example.com".to_string(),
            date_utc: Some(Utc::now()),
            subject_raw: "[PATCH] demo".to_string(),
            raw_rfc822: raw_patch.into_bytes(),
        }]);

        let mbox_path = temp_root.join("series.mbox");
        fs::write(&mbox_path, rendered).expect("write mbox");

        let status = Command::new("git")
            .current_dir(&temp_root)
            .arg("am")
            .arg(&mbox_path)
            .status()
            .expect("run git am");
        assert!(status.success());

        let content = fs::read_to_string(temp_root.join("foo.txt")).expect("read patched file");
        assert_eq!(content.trim(), "new");
    }

    fn init_repo_with_patch(repo: &Path) {
        run_git(repo, &["init"], "git init");
        run_git(repo, &["config", "user.name", "Nexus Test"], "git config user.name");
        run_git(
            repo,
            &["config", "user.email", "dev@example.com"],
            "git config user.email",
        );

        fs::write(repo.join("foo.txt"), "old\n").expect("write file");
        run_git(repo, &["add", "foo.txt"], "git add");
        run_git(repo, &["commit", "-m", "base"], "commit base");

        fs::write(repo.join("foo.txt"), "new\n").expect("write patch file");
        run_git(repo, &["add", "foo.txt"], "git add updated");
        run_git(repo, &["commit", "-m", "update foo"], "commit patch");
    }

    fn run_git(repo: &Path, args: &[&str], context: &str) -> String {
        let output = Command::new("git")
            .current_dir(repo)
            .args(args)
            .output()
            .expect("run git command");
        assert!(
            output.status.success(),
            "{context} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).to_string()
    }
}
