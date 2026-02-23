use super::*;

pub(super) fn to_queue_state_counts(rows: Vec<JobStateCount>) -> QueueStateCountsResponse {
    let mut counts = QueueStateCountsResponse::default();
    for row in rows {
        match row.state {
            JobState::Scheduled => counts.scheduled = row.count,
            JobState::Queued => counts.queued = row.count,
            JobState::Running => counts.running = row.count,
            JobState::Succeeded => counts.succeeded = row.count,
            JobState::FailedRetryable => counts.failed_retryable = row.count,
            JobState::FailedTerminal => counts.failed_terminal = row.count,
            JobState::Cancelled => counts.cancelled = row.count,
        }
    }
    counts
}

pub(super) fn map_db_totals(raw: DbStorageTotalsRecord) -> StorageDbTotalsResponse {
    StorageDbTotalsResponse {
        mailing_lists: raw.mailing_lists,
        messages: raw.messages,
        threads: raw.threads,
        patch_series: raw.patch_series,
        patch_items: raw.patch_items,
        jobs: raw.jobs,
        job_attempts: raw.job_attempts,
        embedding_vectors: raw.embedding_vectors,
    }
}

pub(super) fn map_db_list(raw: DbListStorageRecord) -> StorageDbListResponse {
    StorageDbListResponse {
        list_key: raw.list_key,
        repos: StorageDbListRepoCountsResponse {
            active: raw.active_repo_count,
            total: raw.total_repo_count,
        },
        counts: StorageDbListCountsResponse {
            messages: raw.message_count,
            threads: raw.thread_count,
            patch_series: raw.patch_series_count,
            patch_items: raw.patch_item_count,
        },
    }
}

pub(super) fn build_storage_drift(
    db_lists: &[DbListStorageRecord],
    meili_lists: &BTreeMap<String, StorageMeiliListResponse>,
) -> Vec<StorageDriftResponse> {
    db_lists
        .iter()
        .map(|row| {
            let meili = meili_lists.get(&row.list_key).cloned().unwrap_or_else(|| {
                StorageMeiliListResponse {
                    list_key: row.list_key.clone(),
                    ..StorageMeiliListResponse::default()
                }
            });
            StorageDriftResponse {
                list_key: row.list_key.clone(),
                threads_db: row.thread_count,
                threads_meili: meili.thread_docs,
                threads_delta: meili.thread_docs - row.thread_count,
                patch_series_db: row.patch_series_count,
                patch_series_meili: meili.patch_series_docs,
                patch_series_delta: meili.patch_series_docs - row.patch_series_count,
                patch_items_db: row.patch_item_count,
                patch_items_meili: meili.patch_item_docs,
                patch_items_delta: meili.patch_item_docs - row.patch_item_count,
            }
        })
        .collect()
}

pub(super) async fn fetch_meili_storage(state: &ApiState) -> MeiliStoragePayload {
    let mut response = StorageMeiliResponse {
        ok: true,
        error: None,
        totals: StorageMeiliTotalsResponse::default(),
        lists: Vec::new(),
    };
    let mut list_counts: BTreeMap<String, StorageMeiliListResponse> = BTreeMap::new();

    let client = reqwest::Client::new();
    let base_url = state.settings.meili.url.trim_end_matches('/');
    let stats_result = client
        .get(format!("{base_url}/stats"))
        .bearer_auth(&state.settings.meili.master_key)
        .send()
        .await;

    let stats_value = match stats_result {
        Ok(resp) => {
            if !resp.status().is_success() {
                response.ok = false;
                response.error = Some(format!("meili /stats returned http {}", resp.status()));
                return MeiliStoragePayload {
                    response,
                    list_counts,
                };
            }
            match resp.json::<Value>().await {
                Ok(value) => value,
                Err(err) => {
                    response.ok = false;
                    response.error = Some(format!("failed to parse meili /stats response: {err}"));
                    return MeiliStoragePayload {
                        response,
                        list_counts,
                    };
                }
            }
        }
        Err(err) => {
            response.ok = false;
            response.error = Some(format!("meili /stats request failed: {err}"));
            return MeiliStoragePayload {
                response,
                list_counts,
            };
        }
    };

    response.totals = parse_meili_totals(&stats_value);

    for index_kind in [
        MeiliIndexKind::ThreadDocs,
        MeiliIndexKind::PatchSeriesDocs,
        MeiliIndexKind::PatchItemDocs,
    ] {
        match fetch_meili_list_counts(&client, state, index_kind).await {
            Ok(counts) => merge_meili_list_counts(index_kind, counts, &mut list_counts),
            Err(err) => {
                response.ok = false;
                if response.error.is_none() {
                    response.error = Some(err);
                }
            }
        }
    }

    response.lists = list_counts.values().cloned().collect();
    MeiliStoragePayload {
        response,
        list_counts,
    }
}

pub(super) fn parse_meili_totals(raw: &Value) -> StorageMeiliTotalsResponse {
    let indexes = raw.get("indexes").and_then(Value::as_object);
    StorageMeiliTotalsResponse {
        database_size_bytes: raw.get("databaseSize").and_then(Value::as_u64),
        used_database_size_bytes: raw.get("usedDatabaseSize").and_then(Value::as_u64),
        last_update: raw
            .get("lastUpdate")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        indexes: StorageMeiliIndexesResponse {
            thread_docs: parse_meili_index(indexes.and_then(|m| m.get("thread_docs"))),
            patch_series_docs: parse_meili_index(indexes.and_then(|m| m.get("patch_series_docs"))),
            patch_item_docs: parse_meili_index(indexes.and_then(|m| m.get("patch_item_docs"))),
        },
    }
}

pub(super) fn parse_meili_index(raw: Option<&Value>) -> StorageMeiliIndexResponse {
    let raw = match raw {
        Some(value) => value,
        None => return StorageMeiliIndexResponse::default(),
    };
    StorageMeiliIndexResponse {
        documents: raw
            .get("numberOfDocuments")
            .and_then(Value::as_i64)
            .or_else(|| {
                raw.get("numberOfDocuments")
                    .and_then(Value::as_u64)
                    .map(|value| value as i64)
            })
            .unwrap_or(0),
        is_indexing: raw
            .get("isIndexing")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        embedded_documents: raw
            .get("numberOfEmbeddedDocuments")
            .and_then(Value::as_i64)
            .or_else(|| {
                raw.get("numberOfEmbeddedDocuments")
                    .and_then(Value::as_u64)
                    .map(|value| value as i64)
            })
            .or_else(|| {
                raw.get("numberOfEmbeddings")
                    .and_then(Value::as_u64)
                    .map(|value| value as i64)
            })
            .unwrap_or(0),
    }
}

pub(super) async fn fetch_meili_list_counts(
    client: &reqwest::Client,
    state: &ApiState,
    index_kind: MeiliIndexKind,
) -> Result<BTreeMap<String, i64>, String> {
    let base_url = state.settings.meili.url.trim_end_matches('/');
    let index_uid = index_kind.uid();
    let response = client
        .post(format!("{base_url}/indexes/{index_uid}/search"))
        .bearer_auth(&state.settings.meili.master_key)
        .json(&json!({
            "q": "",
            "limit": 0,
            "facets": ["list_keys"]
        }))
        .send()
        .await
        .map_err(|err| format!("meili facet query failed for {index_uid}: {err}"))?;

    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(BTreeMap::new());
    }
    if !response.status().is_success() {
        return Err(format!(
            "meili facet query for {index_uid} returned http {}",
            response.status()
        ));
    }
    let payload = response
        .json::<Value>()
        .await
        .map_err(|err| format!("failed to parse meili facet response for {index_uid}: {err}"))?;
    Ok(parse_meili_facet_counts(&payload))
}

pub(super) fn parse_meili_facet_counts(payload: &Value) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    let Some(by_list) = payload
        .get("facetDistribution")
        .and_then(|facets| facets.get("list_keys"))
        .and_then(Value::as_object)
    else {
        return out;
    };

    for (list_key, count_value) in by_list {
        if let Some(count) = count_value
            .as_i64()
            .or_else(|| count_value.as_u64().map(|value| value as i64))
        {
            out.insert(list_key.clone(), count);
        }
    }
    out
}

pub(super) fn merge_meili_list_counts(
    index_kind: MeiliIndexKind,
    counts: BTreeMap<String, i64>,
    by_list: &mut BTreeMap<String, StorageMeiliListResponse>,
) {
    for (list_key, count) in counts {
        let entry = by_list
            .entry(list_key.clone())
            .or_insert_with(|| StorageMeiliListResponse {
                list_key,
                ..StorageMeiliListResponse::default()
            });

        match index_kind {
            MeiliIndexKind::ThreadDocs => entry.thread_docs = count,
            MeiliIndexKind::PatchSeriesDocs => entry.patch_series_docs = count,
            MeiliIndexKind::PatchItemDocs => entry.patch_item_docs = count,
        }
    }
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
                    .and_then(|v| v.to_str())
                    .unwrap_or("")
                    .ends_with(".git")
            {
                let relpath = PathBuf::from("git")
                    .join(path.file_name().unwrap_or_default())
                    .display()
                    .to_string();
                epoch_repos.insert(relpath);
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

pub(super) fn discover_mirror_lists(
    mirror_root: &Path,
) -> Result<Vec<MirrorListCandidate>, std::io::Error> {
    let mut out = Vec::new();

    for entry in fs::read_dir(mirror_root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(list_key) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let repos = discover_repo_relpaths(&path)?;
        if repos.is_empty() {
            continue;
        }
        out.push(MirrorListCandidate {
            list_key: list_key.to_string(),
            repos,
        });
    }

    out.sort_by(|a, b| a.list_key.cmp(&b.list_key));
    Ok(out)
}

/// Ensure mailing list and repo catalog entries exist, returning the mailing list ID.
pub(super) async fn ensure_catalog_entries(
    state: &ApiState,
    list_key: &str,
    repos: &[String],
) -> Result<i64, IngestQueueError> {
    let list = state
        .catalog
        .ensure_mailing_list(list_key)
        .await
        .map_err(|err| {
            IngestQueueError::internal(format!("failed to ensure mailing list {list_key}: {err}"))
        })?;

    for repo_relpath in repos {
        state
            .catalog
            .ensure_repo(list.id, repo_relpath, repo_relpath)
            .await
            .map_err(|err| {
                IngestQueueError::internal(format!(
                    "failed to ensure repo metadata for list {list_key} repo {repo_relpath}: {err}"
                ))
            })?;
    }

    Ok(list.id)
}

/// Create a pipeline run for a single list and enqueue its ingest job.
pub(super) async fn queue_list_ingest(
    state: &ApiState,
    list_key: &str,
    repos: Vec<String>,
    source: &str,
) -> Result<IngestSyncResponse, IngestQueueError> {
    if let Some(active) = state
        .pipeline
        .get_open_run_for_list(list_key)
        .await
        .map_err(|err| {
            IngestQueueError::internal(format!(
                "failed to check open pipeline run for list {list_key}: {err}"
            ))
        })?
    {
        return Err(IngestQueueError::conflict(format!(
            "pipeline run {} is already {} for list {list_key}",
            active.id, active.state
        )));
    }

    let list_id = ensure_catalog_entries(state, list_key, &repos).await?;

    let run = state
        .pipeline
        .create_running_run(list_id, list_key, source)
        .await
        .map_err(|err| {
            IngestQueueError::internal(format!(
                "failed to create pipeline run for list {list_key}: {err}"
            ))
        })?;

    let payload = PipelineIngestPayload { run_id: run.id };
    let enqueue_result = state
        .jobs
        .enqueue(EnqueueJobParams {
            job_type: "pipeline_ingest".to_string(),
            payload_json: serde_json::to_value(payload).map_err(|err| {
                IngestQueueError::internal(format!(
                    "failed to serialize pipeline payload for list {list_key}: {err}"
                ))
            })?,
            priority: 20,
            dedupe_scope: Some(format!("pipeline:run:{}", run.id)),
            dedupe_key: Some("ingest".to_string()),
            run_after: None,
            max_attempts: Some(8),
        })
        .await;

    if let Err(err) = enqueue_result {
        let reason = format!("failed to enqueue pipeline_ingest for list {list_key}: {err}");
        if let Err(mark_err) = state
            .pipeline
            .mark_run_failed(run.id, &format!("initial enqueue error: {err}"))
            .await
        {
            tracing::error!(
                run_id = run.id,
                list_key,
                "failed to mark run failed after enqueue error: {mark_err}"
            );
        }
        return Err(IngestQueueError::internal(reason));
    }

    Ok(IngestSyncResponse {
        queued: 1,
        repos,
        mode: "pipeline".to_string(),
        pipeline_run_id: Some(run.id),
        current_stage: Some(run.current_stage),
    })
}

pub(super) fn looks_like_bare_repo(path: &Path) -> bool {
    path.join("objects").exists() && path.join("refs").exists()
}

pub(super) fn normalize_limit(limit: i64, default: i64, max: i64) -> i64 {
    let requested = if limit <= 0 { default } else { limit };
    requested.clamp(1, max)
}

pub(super) fn build_page_info(limit: i64, next_cursor: Option<String>) -> CursorPageInfoResponse {
    CursorPageInfoResponse {
        limit,
        next_cursor: next_cursor.clone(),
        prev_cursor: None,
        has_more: next_cursor.is_some(),
    }
}

pub(super) fn short_hash(value: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(value).unwrap_or_default());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(32);
    for byte in digest.iter().take(16) {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

pub(super) fn encode_cursor_token<T: Serialize>(token: &T) -> Option<String> {
    let bytes = serde_json::to_vec(token).ok()?;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    Some(out)
}

pub(super) fn decode_cursor_token<T: for<'de> Deserialize<'de>>(raw: &str) -> Option<T> {
    if !raw.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(raw.len() / 2);
    let data = raw.as_bytes();
    for idx in (0..raw.len()).step_by(2) {
        let hi = data[idx] as char;
        let lo = data[idx + 1] as char;
        let value = hi.to_digit(16)? * 16 + lo.to_digit(16)?;
        bytes.push(value as u8);
    }
    serde_json::from_slice(&bytes).ok()
}

pub(super) fn parse_embedding_scope(raw: &str) -> Option<EmbeddingScope> {
    match raw {
        "thread" => Some(EmbeddingScope::Thread),
        "series" => Some(EmbeddingScope::Series),
        _ => None,
    }
}

pub(super) fn parse_meili_bootstrap_scope(raw: &str) -> Option<MeiliBootstrapScope> {
    match raw {
        "embedding_indexes" => Some(MeiliBootstrapScope::EmbeddingIndexes),
        _ => None,
    }
}

pub(super) fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    if let Ok(ts) = DateTime::parse_from_rfc3339(raw) {
        return Some(ts.with_timezone(&Utc));
    }
    let date = chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok()?;
    let naive = date.and_hms_opt(0, 0, 0)?;
    Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

pub(super) fn parse_optional_timestamp_query(raw: Option<&str>) -> Option<Option<DateTime<Utc>>> {
    let Some(value) = raw else {
        return Some(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Some(None);
    }
    parse_timestamp(trimmed).map(Some)
}

pub(super) fn deserialize_optional_datetime_query<'de, D>(
    deserializer: D,
) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    parse_optional_timestamp_query(raw.as_deref()).ok_or_else(|| {
        de::Error::custom("invalid datetime: expected RFC3339 timestamp or YYYY-MM-DD")
    })
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::{env, fs};

    use nexus_core::search::MeiliIndexKind;
    use serde_json::json;

    use super::{
        StorageMeiliListResponse, discover_mirror_lists, discover_repo_relpaths,
        merge_meili_list_counts, parse_meili_facet_counts, parse_meili_totals,
        parse_optional_timestamp_query,
    };

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let now_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = env::temp_dir().join(format!(
                "nexus-api-admin-{prefix}-{}-{now_nanos}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn discover_repo_relpaths_covers_supported_layouts() {
        let temp = TempDir::new("repo-layouts");

        let all_git = temp.path().join("all-layout");
        fs::create_dir_all(all_git.join("all.git")).expect("all.git");
        assert_eq!(
            discover_repo_relpaths(&all_git).expect("discover all.git"),
            vec!["all.git".to_string()]
        );

        let git_epoch = temp.path().join("git-layout");
        fs::create_dir_all(git_epoch.join("git/0.git")).expect("git epoch");
        assert_eq!(
            discover_repo_relpaths(&git_epoch).expect("discover git epoch"),
            vec!["git/0.git".to_string()]
        );

        let bare = temp.path().join("bare-layout");
        fs::create_dir_all(bare.join("objects")).expect("objects");
        fs::create_dir_all(bare.join("refs")).expect("refs");
        assert_eq!(
            discover_repo_relpaths(&bare).expect("discover bare"),
            vec![".".to_string()]
        );
    }

    #[test]
    fn discover_mirror_lists_uses_immediate_repo_dirs_only() {
        let temp = TempDir::new("mirror-lists");

        fs::create_dir_all(temp.path().join("zzz/all.git")).expect("zzz");
        fs::create_dir_all(temp.path().join("alpha/git/1.git")).expect("alpha");
        fs::create_dir_all(temp.path().join("beta/objects")).expect("beta objects");
        fs::create_dir_all(temp.path().join("beta/refs")).expect("beta refs");
        fs::create_dir_all(temp.path().join("no-repo/subdir")).expect("no repo");
        fs::create_dir_all(temp.path().join("nested/child/all.git")).expect("nested");
        fs::write(temp.path().join("manifest.js.gz"), b"placeholder").expect("manifest");

        let discovered = discover_mirror_lists(temp.path()).expect("discover mirror lists");
        let keys: Vec<&str> = discovered
            .iter()
            .map(|item| item.list_key.as_str())
            .collect();

        assert_eq!(keys, vec!["alpha", "beta", "zzz"]);
        assert_eq!(discovered[0].repos, vec!["git/1.git".to_string()]);
        assert_eq!(discovered[1].repos, vec![".".to_string()]);
        assert_eq!(discovered[2].repos, vec!["all.git".to_string()]);
    }

    #[test]
    fn parse_meili_totals_extracts_expected_fields() {
        let payload = json!({
            "databaseSize": 10,
            "usedDatabaseSize": 8,
            "lastUpdate": "2026-02-15T18:22:01.592981161Z",
            "indexes": {
                "thread_docs": {
                    "numberOfDocuments": 11,
                    "isIndexing": false,
                    "numberOfEmbeddedDocuments": 9
                },
                "patch_series_docs": {
                    "numberOfDocuments": 22,
                    "isIndexing": true,
                    "numberOfEmbeddings": 7
                }
            }
        });

        let parsed = parse_meili_totals(&payload);
        assert_eq!(parsed.database_size_bytes, Some(10));
        assert_eq!(parsed.used_database_size_bytes, Some(8));
        assert_eq!(
            parsed.last_update.as_deref(),
            Some("2026-02-15T18:22:01.592981161Z")
        );
        assert_eq!(parsed.indexes.thread_docs.documents, 11);
        assert_eq!(parsed.indexes.thread_docs.embedded_documents, 9);
        assert_eq!(parsed.indexes.patch_series_docs.documents, 22);
        assert_eq!(parsed.indexes.patch_series_docs.embedded_documents, 7);
        assert_eq!(parsed.indexes.patch_item_docs.documents, 0);
    }

    #[test]
    fn parse_meili_facet_counts_reads_list_key_distribution() {
        let payload = json!({
            "facetDistribution": {
                "list_keys": {
                    "bpf": 10244,
                    "lkml": 77
                }
            }
        });
        let parsed = parse_meili_facet_counts(&payload);
        assert_eq!(parsed.get("bpf"), Some(&10244));
        assert_eq!(parsed.get("lkml"), Some(&77));
    }

    #[test]
    fn merge_meili_list_counts_tracks_index_families_per_list() {
        let mut out = std::collections::BTreeMap::<String, StorageMeiliListResponse>::new();
        merge_meili_list_counts(
            MeiliIndexKind::ThreadDocs,
            std::collections::BTreeMap::from([("bpf".to_string(), 10), ("lkml".to_string(), 4)]),
            &mut out,
        );
        merge_meili_list_counts(
            MeiliIndexKind::PatchSeriesDocs,
            std::collections::BTreeMap::from([("bpf".to_string(), 3)]),
            &mut out,
        );
        merge_meili_list_counts(
            MeiliIndexKind::PatchItemDocs,
            std::collections::BTreeMap::from([("bpf".to_string(), 20)]),
            &mut out,
        );

        let bpf = out.get("bpf").expect("bpf");
        assert_eq!(bpf.thread_docs, 10);
        assert_eq!(bpf.patch_series_docs, 3);
        assert_eq!(bpf.patch_item_docs, 20);

        let lkml = out.get("lkml").expect("lkml");
        assert_eq!(lkml.thread_docs, 4);
        assert_eq!(lkml.patch_series_docs, 0);
        assert_eq!(lkml.patch_item_docs, 0);
    }

    #[test]
    fn parse_optional_timestamp_query_accepts_rfc3339() {
        let parsed =
            parse_optional_timestamp_query(Some("2026-02-20T14:05:06Z")).expect("valid timestamp");
        assert!(parsed.is_some());
    }

    #[test]
    fn parse_optional_timestamp_query_accepts_date() {
        let parsed = parse_optional_timestamp_query(Some("2026-02-20")).expect("valid date");
        assert!(parsed.is_some());
    }

    #[test]
    fn parse_optional_timestamp_query_treats_empty_as_unbounded() {
        let parsed = parse_optional_timestamp_query(Some("   ")).expect("empty is allowed");
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_optional_timestamp_query_rejects_invalid_input() {
        let parsed = parse_optional_timestamp_query(Some("not-a-date"));
        assert!(parsed.is_none());
    }
}
