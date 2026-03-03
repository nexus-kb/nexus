use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VectorAttachmentMode {
    StoredVector,
    NullPlaceholder,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
pub(super) struct SearchFamilyOutcome {
    pub(super) artifact_kind: String,
    pub(super) index_uid: String,
    pub(super) chunks_done: u64,
    pub(super) ids_seen: u64,
    pub(super) docs_upserted: u64,
    pub(super) docs_bytes: u64,
    pub(super) tasks_submitted: u64,
}

#[derive(Default)]
pub(super) struct ThreadingWindowOutcome {
    pub(super) affected_threads: u64,
    pub(super) source_messages_read: u64,
    pub(super) anchors_scanned: u64,
    pub(super) anchors_deduped: u64,
    pub(super) apply_stats: ThreadingApplyStats,
    pub(super) impacted_thread_ids: Vec<i64>,
}

#[derive(Debug, Clone)]
pub(super) struct ThreadingEpochWindow {
    pub(super) epochs: Vec<i64>,
    pub(super) repo_ids: Vec<i64>,
}

pub(super) fn build_threading_epoch_windows(
    repos: &[MailingListRepo],
    start_epoch: Option<i64>,
) -> std::result::Result<Vec<ThreadingEpochWindow>, String> {
    if repos.is_empty() {
        return Err("no repositories available for epoch threading".to_string());
    }

    let mut epoch_groups: Vec<(i64, Vec<i64>)> = Vec::new();
    for repo in repos {
        let Some(epoch) = parse_epoch_repo_relpath(&repo.repo_relpath) else {
            return Err(format!(
                "repo {} is not epoch formatted (expected git/<n>.git)",
                repo.repo_relpath
            ));
        };

        if let Some((last_epoch, repo_ids)) = epoch_groups.last_mut()
            && *last_epoch == epoch
        {
            repo_ids.push(repo.id);
            continue;
        }
        epoch_groups.push((epoch, vec![repo.id]));
    }

    let start_idx = match start_epoch {
        Some(value) => epoch_groups
            .iter()
            .position(|(epoch, _)| *epoch >= value)
            .unwrap_or_else(|| epoch_groups.len().saturating_sub(1)),
        None => 0,
    };
    let scoped = &epoch_groups[start_idx..];
    if scoped.is_empty() {
        return Ok(Vec::new());
    }

    if scoped.len() == 1 {
        let (epoch, repo_ids) = &scoped[0];
        return Ok(vec![ThreadingEpochWindow {
            epochs: vec![*epoch],
            repo_ids: repo_ids.clone(),
        }]);
    }

    let mut windows = Vec::with_capacity(scoped.len() - 1);
    for idx in 0..(scoped.len() - 1) {
        let (left_epoch, left_ids) = &scoped[idx];
        let (right_epoch, right_ids) = &scoped[idx + 1];
        let mut repo_ids = Vec::with_capacity(left_ids.len() + right_ids.len());
        repo_ids.extend(left_ids.iter().copied());
        repo_ids.extend(right_ids.iter().copied());
        windows.push(ThreadingEpochWindow {
            epochs: vec![*left_epoch, *right_epoch],
            repo_ids,
        });
    }

    Ok(windows)
}

pub(super) struct RawCommitMail {
    index: usize,
    commit_oid: String,
    raw_rfc822: Vec<u8>,
}

pub(super) struct ParsedCommitRows {
    pub(super) rows: Vec<IngestCommitRow>,
    pub(super) parse_errors: u64,
    pub(super) non_mail_commits: u64,
    pub(super) bytes_read: u64,
    pub(super) header_rejections: u64,
    pub(super) date_rejections: u64,
    pub(super) sanitization_rejections: u64,
    pub(super) other_rejections: u64,
}

pub(super) struct IndexedParsedRow {
    index: usize,
    row: IngestCommitRow,
}

pub(super) struct IndexedCommitOid {
    index: usize,
    commit_oid: String,
}

#[derive(Default)]
pub(super) struct ParseChunkOutcome {
    parsed_rows: Vec<IndexedParsedRow>,
    parse_errors: u64,
    non_mail_commits: u64,
    bytes_read: u64,
    header_rejections: u64,
    date_rejections: u64,
    sanitization_rejections: u64,
    other_rejections: u64,
}

#[derive(Copy, Clone)]
pub(super) enum ParseSkipKind {
    MissingHeaders,
    InvalidDate,
    Sanitization,
    Other,
}

pub(super) enum ParseCommitResult {
    Parsed(Box<IndexedParsedRow>),
    Skipped {
        commit_oid: String,
        reason: String,
        kind: ParseSkipKind,
        warn: bool,
    },
}

pub(super) fn usize_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(super) fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(super) fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(super) fn i64_to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

pub(super) fn u128_to_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(super) async fn parse_commit_rows(
    repo_path: PathBuf,
    commit_oids: Vec<String>,
    worker_count: usize,
) -> anyhow::Result<ParsedCommitRows> {
    if commit_oids.is_empty() {
        return Ok(ParsedCommitRows {
            rows: Vec::new(),
            parse_errors: 0,
            non_mail_commits: 0,
            bytes_read: 0,
            header_rejections: 0,
            date_rejections: 0,
            sanitization_rejections: 0,
            other_rejections: 0,
        });
    }

    let partitions = partition_commit_oids(commit_oids, worker_count);
    let mut joinset = JoinSet::new();
    for partition in partitions {
        if partition.is_empty() {
            continue;
        }
        let worker_repo_path = repo_path.clone();
        joinset.spawn_blocking(move || parse_commit_partition(worker_repo_path, partition));
    }

    let mut parsed_rows = Vec::new();
    let mut parse_errors = 0u64;
    let mut non_mail_commits = 0u64;
    let mut bytes_read = 0u64;
    let mut header_rejections = 0u64;
    let mut date_rejections = 0u64;
    let mut sanitization_rejections = 0u64;
    let mut other_rejections = 0u64;

    while let Some(next) = joinset.join_next().await {
        match next {
            Ok(Ok(outcome)) => {
                parsed_rows.extend(outcome.parsed_rows);
                parse_errors += outcome.parse_errors;
                non_mail_commits += outcome.non_mail_commits;
                bytes_read += outcome.bytes_read;
                header_rejections += outcome.header_rejections;
                date_rejections += outcome.date_rejections;
                sanitization_rejections += outcome.sanitization_rejections;
                other_rejections += outcome.other_rejections;
            }
            Ok(Err(err)) => return Err(err),
            Err(err) => {
                return Err(anyhow::anyhow!("mail parse task failed: {err}"));
            }
        }
    }

    parsed_rows.sort_by_key(|row| row.index);

    Ok(ParsedCommitRows {
        rows: parsed_rows.into_iter().map(|row| row.row).collect(),
        parse_errors,
        non_mail_commits,
        bytes_read,
        header_rejections,
        date_rejections,
        sanitization_rejections,
        other_rejections,
    })
}

pub(super) fn parse_commit_partition(
    repo_path: PathBuf,
    partition: Vec<IndexedCommitOid>,
) -> anyhow::Result<ParseChunkOutcome> {
    let mut gix_repo = gix::open(&repo_path)?;
    gix_repo.object_cache_size_if_unset(64 * 1024 * 1024);

    let mut outcome = ParseChunkOutcome::default();

    for commit in partition {
        let raw_mail = match read_mail_blob(&gix_repo, &commit.commit_oid) {
            Ok(Some(value)) => value,
            Ok(None) => {
                outcome.non_mail_commits += 1;
                continue;
            }
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "failed to read commit blob {}: {err}",
                    commit.commit_oid
                ));
            }
        };

        outcome.bytes_read += usize_to_u64(raw_mail.len());
        let raw_commit = RawCommitMail {
            index: commit.index,
            commit_oid: commit.commit_oid,
            raw_rfc822: raw_mail,
        };

        match parse_one_commit(raw_commit) {
            ParseCommitResult::Parsed(parsed) => outcome.parsed_rows.push(*parsed),
            ParseCommitResult::Skipped {
                commit_oid,
                reason,
                kind,
                warn,
            } => {
                outcome.parse_errors += 1;
                match kind {
                    ParseSkipKind::MissingHeaders => outcome.header_rejections += 1,
                    ParseSkipKind::InvalidDate => outcome.date_rejections += 1,
                    ParseSkipKind::Sanitization => outcome.sanitization_rejections += 1,
                    ParseSkipKind::Other => outcome.other_rejections += 1,
                }
                if warn {
                    warn!(commit_oid = %commit_oid, error = %reason, "mail parse error");
                }
            }
        }
    }

    Ok(outcome)
}

pub(super) fn parse_one_commit(raw_commit: RawCommitMail) -> ParseCommitResult {
    let parsed = match parse_email(&raw_commit.raw_rfc822) {
        Ok(v) => v,
        Err(ParseEmailError::MissingMessageId | ParseEmailError::MissingAuthorEmail) => {
            return ParseCommitResult::Skipped {
                commit_oid: raw_commit.commit_oid,
                reason: "missing required message headers".to_string(),
                kind: ParseSkipKind::MissingHeaders,
                warn: false,
            };
        }
        Err(
            ParseEmailError::MissingDate { .. }
            | ParseEmailError::InvalidDate { .. }
            | ParseEmailError::FutureDate { .. },
        ) => {
            return ParseCommitResult::Skipped {
                commit_oid: raw_commit.commit_oid,
                reason: "invalid_or_missing_date".to_string(),
                kind: ParseSkipKind::InvalidDate,
                warn: false,
            };
        }
        Err(ParseEmailError::SanitizationInvariantViolation(field)) => {
            return ParseCommitResult::Skipped {
                commit_oid: raw_commit.commit_oid,
                reason: format!("sanitization_invariant_violation:{field}"),
                kind: ParseSkipKind::Sanitization,
                warn: true,
            };
        }
        Err(err) => {
            return ParseCommitResult::Skipped {
                commit_oid: raw_commit.commit_oid,
                reason: err.to_string(),
                kind: ParseSkipKind::Other,
                warn: true,
            };
        }
    };

    let patch_facts = extract_patch_facts(
        parsed.body_text.as_deref(),
        parsed.diff_text.as_deref(),
        parsed.has_diff,
    );

    let parsed_input = ParsedMessageInput {
        content_hash_sha256: parsed.content_hash_sha256,
        subject_raw: parsed.subject_raw,
        subject_norm: parsed.subject_norm,
        from_name: parsed.from_name,
        from_email: parsed.from_email,
        date_utc: parsed.date_utc,
        to_raw: parsed.to_raw,
        cc_raw: parsed.cc_raw,
        message_ids: parsed.message_ids,
        message_id_primary: parsed.message_id_primary,
        in_reply_to_ids: parsed.in_reply_to_ids,
        references_ids: parsed.references_ids,
        mime_type: parsed.mime_type,
        body: ParsedBodyInput {
            raw_rfc822: raw_commit.raw_rfc822,
            body_text: parsed.body_text,
            diff_text: parsed.diff_text,
            search_text: parsed.search_text,
            has_diff: parsed.has_diff,
            has_attachments: parsed.has_attachments,
        },
        patch_facts,
    };

    ParseCommitResult::Parsed(Box::new(IndexedParsedRow {
        index: raw_commit.index,
        row: IngestCommitRow {
            git_commit_oid: raw_commit.commit_oid,
            parsed_message: parsed_input,
        },
    }))
}

pub(super) fn extract_patch_facts(
    body_text: Option<&str>,
    diff_text: Option<&str>,
    has_diff: bool,
) -> Option<ParsedPatchFactsInput> {
    let extracted_diff = diff_text
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .or_else(|| body_text.and_then(extract_diff_text));

    let has_diff = has_diff
        || extracted_diff
            .as_deref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);

    let patch_id_stable = extracted_diff.as_deref().and_then(compute_patch_id_stable);

    let parsed_files = extracted_diff
        .as_deref()
        .map(parse_diff_metadata)
        .unwrap_or_default();

    let mut files = Vec::with_capacity(parsed_files.len());
    let mut additions = 0i32;
    let mut deletions = 0i32;
    let mut hunk_count = 0i32;

    for file in parsed_files {
        additions = additions.saturating_add(file.additions);
        deletions = deletions.saturating_add(file.deletions);
        hunk_count = hunk_count.saturating_add(file.hunk_count);
        files.push(ParsedPatchFileFactInput {
            old_path: file.old_path,
            new_path: file.new_path,
            change_type: file.change_type,
            is_binary: file.is_binary,
            additions: file.additions,
            deletions: file.deletions,
            hunk_count: file.hunk_count,
            diff_start: file.diff_start,
            diff_end: file.diff_end,
        });
    }

    let base_commit = body_text.and_then(extract_base_commit);
    let change_id = body_text.and_then(extract_change_id);

    if !has_diff
        && patch_id_stable.is_none()
        && base_commit.is_none()
        && change_id.is_none()
        && files.is_empty()
    {
        return None;
    }

    Some(ParsedPatchFactsInput {
        has_diff,
        patch_id_stable,
        base_commit,
        change_id,
        file_count: files.len() as i32,
        additions,
        deletions,
        hunk_count,
        files,
    })
}

pub(super) fn extract_change_id(body_text: &str) -> Option<String> {
    CHANGE_ID_RE
        .captures(body_text)
        .and_then(|captures| captures.get(1))
        .map(|matched| matched.as_str().trim().to_ascii_lowercase())
}

pub(super) fn extract_base_commit(body_text: &str) -> Option<String> {
    BASE_COMMIT_RE
        .captures(body_text)
        .and_then(|captures| captures.get(1))
        .map(|matched| matched.as_str().trim().to_ascii_lowercase())
}

pub(super) fn empty_metrics(duration_ms: u128) -> JobStoreMetrics {
    JobStoreMetrics {
        duration_ms,
        rows_written: 0,
        bytes_read: 0,
        commit_count: 0,
        parse_errors: 0,
    }
}

pub(super) fn retryable_error(
    reason: String,
    kind: &str,
    job: &Job,
    duration_ms: u128,
    settings: &Settings,
) -> JobExecutionOutcome {
    let backoff = nexus_db::JobStore::compute_backoff(
        settings.worker.base_backoff_ms,
        settings.worker.max_backoff_ms,
        job.attempt,
    );

    JobExecutionOutcome::Retryable {
        reason,
        kind: kind.to_string(),
        backoff_ms: i64_to_u64(backoff.num_milliseconds().max(1)),
        metrics: empty_metrics(duration_ms),
    }
}

pub(super) fn parse_embedding_scope(raw: &str) -> Option<EmbeddingScope> {
    match raw {
        "thread" => Some(EmbeddingScope::Thread),
        "series" => Some(EmbeddingScope::Series),
        _ => None,
    }
}

pub(super) fn embedding_doc_build_stage(scope: &str) -> &'static str {
    match scope {
        "thread" => "thread_snippet_aggregate",
        "series" => "series_doc_build",
        _ => "unknown_scope_doc_build",
    }
}

pub(super) fn hash_i64_list(values: &[i64]) -> String {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted.dedup();

    let mut hasher = Sha256::new();
    for value in &sorted {
        hasher.update(value.to_be_bytes());
    }

    let bytes = hasher.finalize();
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

pub(super) fn configured_parse_worker_count(chunk_size: usize, settings: &Settings) -> usize {
    settings
        .worker
        .ingest_parse_concurrency
        .max(1)
        .min(chunk_size.max(1))
}

pub(super) fn partition_commit_oids(
    commit_oids: Vec<String>,
    worker_count: usize,
) -> Vec<Vec<IndexedCommitOid>> {
    let lane_count = worker_count.max(1).min(commit_oids.len().max(1));
    let mut lanes: Vec<Vec<IndexedCommitOid>> = (0..lane_count).map(|_| Vec::new()).collect();

    for (idx, commit_oid) in commit_oids.into_iter().enumerate() {
        lanes[idx % lane_count].push(IndexedCommitOid {
            index: idx,
            commit_oid,
        });
    }

    lanes
}

pub(super) fn discover_repo_relpaths(list_root: &Path) -> Result<Vec<String>, std::io::Error> {
    if !list_root.exists() {
        return Ok(Vec::new());
    }

    let mut epoch_repos = BTreeSet::new();
    let git_dir = list_root.join("git");
    if git_dir.is_dir() {
        for entry in fs::read_dir(&git_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir()
                && path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("")
                    .ends_with(".git")
            {
                let repo_name = path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("");
                epoch_repos.insert(format!("git/{repo_name}"));
            }
        }
    }

    if !epoch_repos.is_empty() {
        return Ok(epoch_repos.into_iter().collect());
    }

    let all_git = list_root.join("all.git");
    if all_git.is_dir() {
        return Ok(vec!["all.git".to_string()]);
    }

    if looks_like_bare_repo(list_root) {
        return Ok(vec![".".to_string()]);
    }

    Ok(Vec::new())
}

pub(super) fn order_repos_for_ingest(mut repos: Vec<MailingListRepo>) -> Vec<MailingListRepo> {
    let has_epoch_repos = repos
        .iter()
        .any(|repo| parse_epoch_repo_relpath(&repo.repo_relpath).is_some());
    if has_epoch_repos {
        repos.retain(|repo| parse_epoch_repo_relpath(&repo.repo_relpath).is_some());
    }

    repos.sort_by(|left, right| compare_repo_relpaths(&left.repo_relpath, &right.repo_relpath));
    repos
}

pub(super) fn compare_repo_relpaths(left: &str, right: &str) -> std::cmp::Ordering {
    match (
        parse_epoch_repo_relpath(left),
        parse_epoch_repo_relpath(right),
    ) {
        (Some(l), Some(r)) => l.cmp(&r),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.cmp(right),
    }
}

pub(super) fn parse_epoch_repo_relpath(relpath: &str) -> Option<i64> {
    let trimmed = relpath.trim();
    let epoch_text = trimmed.strip_prefix("git/")?.strip_suffix(".git")?.trim();

    if epoch_text.is_empty() || !epoch_text.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    epoch_text.parse::<i64>().ok()
}

pub(super) fn looks_like_bare_repo(path: &Path) -> bool {
    path.join("objects").exists() && path.join("refs").exists()
}

pub(super) fn read_mail_blob(
    repo: &gix::Repository,
    commit_oid: &str,
) -> anyhow::Result<Option<Vec<u8>>> {
    let commit_id = ObjectId::from_hex(commit_oid.as_bytes())?;
    let commit = repo
        .find_object(commit_id)?
        .try_into_commit()
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let tree = commit.tree()?;
    let mut blob_oid = None;

    for entry in tree.iter() {
        let entry = entry?;
        if entry.filename() == "m" {
            blob_oid = Some(entry.id().detach());
            break;
        }
    }

    let Some(blob_oid) = blob_oid else {
        return Ok(None);
    };

    let blob = repo
        .find_object(blob_oid)?
        .try_into_blob()
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    Ok(Some(blob.data.to_vec()))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use nexus_db::MailingListRepo;

    use super::{build_threading_epoch_windows, parse_epoch_repo_relpath, partition_commit_oids};

    fn make_repo(id: i64, relpath: &str) -> MailingListRepo {
        MailingListRepo {
            id,
            mailing_list_id: 1,
            repo_key: relpath.to_string(),
            repo_relpath: relpath.to_string(),
            active: true,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn parse_epoch_repo_relpath_extracts_epoch() {
        assert_eq!(parse_epoch_repo_relpath("git/0.git"), Some(0));
        assert_eq!(parse_epoch_repo_relpath("git/12.git"), Some(12));
        assert_eq!(parse_epoch_repo_relpath("all.git"), None);
        assert_eq!(parse_epoch_repo_relpath("git/not-a-number.git"), None);
    }

    #[test]
    fn partition_commit_oids_distributes_evenly() {
        let commits = vec![
            "c1".to_string(),
            "c2".to_string(),
            "c3".to_string(),
            "c4".to_string(),
            "c5".to_string(),
            "c6".to_string(),
            "c7".to_string(),
            "c8".to_string(),
        ];
        let lanes = partition_commit_oids(commits, 4);
        assert_eq!(lanes.len(), 4);
        assert_eq!(
            lanes.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![2, 2, 2, 2]
        );
    }

    #[test]
    fn build_threading_epoch_windows_slides_by_one() {
        let repos = vec![
            make_repo(10, "git/0.git"),
            make_repo(11, "git/1.git"),
            make_repo(12, "git/2.git"),
        ];

        let windows = build_threading_epoch_windows(&repos, None).expect("build epoch windows");
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].epochs, vec![0, 1]);
        assert_eq!(windows[0].repo_ids, vec![10, 11]);
        assert_eq!(windows[1].epochs, vec![1, 2]);
        assert_eq!(windows[1].repo_ids, vec![11, 12]);
    }

    #[test]
    fn build_threading_epoch_windows_single_epoch_is_single_window() {
        let repos = vec![make_repo(42, "git/7.git")];

        let windows = build_threading_epoch_windows(&repos, None).expect("build epoch windows");
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].epochs, vec![7]);
        assert_eq!(windows[0].repo_ids, vec![42]);
    }

    #[test]
    fn build_threading_epoch_windows_can_start_from_prior_epoch() {
        let repos = vec![
            make_repo(10, "git/0.git"),
            make_repo(11, "git/1.git"),
            make_repo(12, "git/2.git"),
            make_repo(13, "git/3.git"),
        ];

        let windows = build_threading_epoch_windows(&repos, Some(2)).expect("build epoch windows");
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].epochs, vec![2, 3]);
        assert_eq!(windows[0].repo_ids, vec![12, 13]);
    }
}
