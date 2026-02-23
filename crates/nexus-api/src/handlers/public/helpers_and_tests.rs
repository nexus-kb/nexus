use super::*;

pub(super) fn map_patch_item(item: PatchItemDetailRecord) -> PatchItemResponse {
    PatchItemResponse {
        patch_item_id: item.patch_item_id,
        series_id: item.patch_series_id,
        series_version_id: item.patch_series_version_id,
        ordinal: item.ordinal,
        total: item.total,
        item_type: item.item_type,
        subject: item.subject_raw,
        subject_norm: item.subject_norm,
        commit_subject: item.commit_subject,
        commit_subject_norm: item.commit_subject_norm,
        message_id: item.message_pk,
        message_id_primary: item.message_id_primary,
        patch_id_stable: item.patch_id_stable,
        has_diff: item.has_diff,
        file_count: item.file_count,
        additions: item.additions,
        deletions: item.deletions,
        hunks: item.hunk_count,
    }
}

pub(super) fn to_patch_compare_rows(
    rows: Vec<SeriesLogicalCompareRow>,
) -> Vec<SeriesComparePatchRow> {
    rows.into_iter()
        .filter_map(|row| {
            let v1_is_patch = row.v1_item_type.as_deref() == Some("patch");
            let v2_is_patch = row.v2_item_type.as_deref() == Some("patch");
            if !v1_is_patch && !v2_is_patch {
                return None;
            }
            let status = match (row.v1_patch_item_id, row.v2_patch_item_id) {
                (Some(v1), Some(v2)) if v1 == v2 => "unchanged",
                (Some(_), Some(_)) => "changed",
                (None, Some(_)) => "added",
                (Some(_), None) => "removed",
                (None, None) => return None,
            };
            Some(SeriesComparePatchRow {
                slot: row.slot,
                title_norm: row.title_norm,
                status: status.to_string(),
                v1_patch_item_id: row.v1_patch_item_id,
                v1_patch_id_stable: row.v1_patch_id_stable,
                v1_subject: row.v1_subject_raw,
                v2_patch_item_id: row.v2_patch_item_id,
                v2_patch_id_stable: row.v2_patch_id_stable,
                v2_subject: row.v2_subject_raw,
            })
        })
        .collect()
}

pub(super) struct SearchRequestHashInput<'a> {
    pub(super) q: &'a str,
    pub(super) scope: SearchScope,
    pub(super) list_key: Option<&'a str>,
    pub(super) author: Option<&'a str>,
    pub(super) from_ts: Option<DateTime<Utc>>,
    pub(super) to_ts: Option<DateTime<Utc>>,
    pub(super) has_diff: Option<bool>,
    pub(super) sort: &'a str,
    pub(super) limit: usize,
    pub(super) hybrid: bool,
    pub(super) semantic_ratio: Option<f32>,
    pub(super) model_key: Option<&'a str>,
}

pub(super) fn search_request_hash(input: SearchRequestHashInput<'_>) -> String {
    let normalized = json!({
        "q": input.q,
        "scope": input.scope.as_str(),
        "list_key": input.list_key.unwrap_or(""),
        "author": input.author.unwrap_or(""),
        "from_ts": input.from_ts.map(|value| value.timestamp()),
        "to_ts": input.to_ts.map(|value| value.timestamp()),
        "has_diff": input.has_diff,
        "sort": input.sort,
        "limit": input.limit,
        "hybrid": input.hybrid,
        "semantic_ratio": input.semantic_ratio,
        "model_key": input.model_key.unwrap_or(""),
    });
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(&normalized).unwrap_or_default());
    let digest = hasher.finalize();
    hex_encode(&digest[..16])
}

pub(super) fn encode_search_cursor(offset: usize, request_hash: &str) -> String {
    format!("o{offset}-h{request_hash}")
}

pub(super) fn parse_search_cursor(raw: &str) -> Option<(usize, String)> {
    let mut parts = raw.split("-h");
    let offset_part = parts.next()?;
    let hash_part = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let offset = offset_part.strip_prefix('o')?.parse::<usize>().ok()?;
    if hash_part.is_empty() {
        return None;
    }
    Some((offset, hash_part.to_string()))
}

pub(super) fn escape_filter_value(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('\"', "\\\"")
}

pub(super) fn default_route_for_scope(scope: SearchScope, id: i64) -> String {
    match scope {
        SearchScope::Thread => "/search".to_string(),
        SearchScope::Series => format!("/series/{id}"),
        SearchScope::PatchItem => format!("/diff/{id}"),
    }
}

pub(super) fn extract_snippet(hit: &Value, crop_attributes: &[&str]) -> Option<String> {
    if let Some(formatted) = hit.get("_formatted") {
        if let Some(snippet) = formatted.get("snippet").and_then(Value::as_str) {
            return Some(snippet.to_string());
        }
        for field in crop_attributes {
            if let Some(value) = formatted.get(*field).and_then(Value::as_str) {
                return Some(value.to_string());
            }
        }
    }

    if let Some(snippet) = hit.get("snippet").and_then(Value::as_str) {
        return Some(snippet.to_string());
    }

    for field in crop_attributes {
        if let Some(value) = hit.get(*field).and_then(Value::as_str) {
            return build_snippet(Some(value));
        }
    }

    hit.get("subject")
        .and_then(Value::as_str)
        .and_then(|value| build_snippet(Some(value)))
}

pub(super) fn normalize_search_facets(raw: Value, author_field: &str) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(list_keys) = raw.get("list_keys") {
        out.insert("list_keys".to_string(), list_keys.clone());
    }
    if let Some(authors) = raw.get(author_field) {
        out.insert("author".to_string(), authors.clone());
    }
    if let Some(has_diff) = raw.get("has_diff") {
        out.insert("has_diff".to_string(), has_diff.clone());
    }
    Value::Object(out)
}

pub(super) fn hybrid_requested(hybrid: Option<bool>, semantic_ratio: Option<f32>) -> bool {
    hybrid.unwrap_or(false) || semantic_ratio.is_some()
}

pub(super) async fn compute_or_get_query_embedding(
    state: &ApiState,
    scope: SearchScope,
    query: &str,
) -> Result<Vec<f32>, String> {
    let normalized_query = normalize_embedding_query(query);
    let cache_key = format!(
        "{}:{}:{}:{}",
        state.settings.embeddings.model,
        state.settings.embeddings.dimensions,
        scope.as_str(),
        normalized_query
    );
    if let Some(vector) = state.query_embedding_cache.get(&cache_key).await {
        return Ok(vector);
    }

    let prompt = hybrid_query_prompt(scope, &normalized_query);
    let vector = state
        .embedding_client
        .embed_query(&prompt)
        .await
        .map_err(|err| format!("query embedding request failed: {err}"))?;
    if vector.len() != state.settings.embeddings.dimensions {
        return Err(format!(
            "query embedding dimensions mismatch: expected={} got={}",
            state.settings.embeddings.dimensions,
            vector.len()
        ));
    }
    state
        .query_embedding_cache
        .insert(cache_key, vector.clone())
        .await;
    Ok(vector)
}

pub(super) fn normalize_embedding_query(raw: &str) -> String {
    raw.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

pub(super) fn hybrid_query_prompt(scope: SearchScope, query: &str) -> String {
    match scope {
        SearchScope::Thread => {
            format!("Represent this mailing-list thread search query for retrieval: {query}")
        }
        SearchScope::Series => {
            format!("Represent this patch-series search query for retrieval: {query}")
        }
        SearchScope::PatchItem => {
            format!("Represent this patch search query for retrieval: {query}")
        }
    }
}

pub(super) fn json_error_response(
    status: StatusCode,
    payload: Value,
) -> Result<Response, StatusCode> {
    let body = serde_json::to_vec(&payload).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub(super) fn json_response_with_cache<T: Serialize>(
    headers: &HeaderMap,
    payload: &T,
    cache_control: &str,
    etag_override: Option<String>,
) -> Result<Response, StatusCode> {
    let body = serde_json::to_vec(payload).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let etag = etag_override.unwrap_or_else(|| strong_etag(&body));

    if if_none_match_matches(headers, &etag) {
        return not_modified_response(cache_control, &etag);
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::ETAG, etag)
        .body(Body::from(body))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub(super) fn not_modified_response(
    cache_control: &str,
    etag: &str,
) -> Result<Response, StatusCode> {
    Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::ETAG, etag)
        .body(Body::empty())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub(super) fn if_none_match_matches(headers: &HeaderMap, etag: &str) -> bool {
    let Some(value) = headers.get(header::IF_NONE_MATCH) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };

    value.split(',').any(|candidate| {
        let candidate = candidate.trim();
        if candidate == "*" {
            return true;
        }
        let candidate = candidate.strip_prefix("W/").unwrap_or(candidate);
        candidate == etag
    })
}

pub(super) fn strong_etag(body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    format!("\"{:x}\"", hasher.finalize())
}

pub(super) fn redirect_response(location: &str) -> Result<Response, StatusCode> {
    let location =
        HeaderValue::from_str(location).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, location)
        .body(Body::empty())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub(super) fn normalize_limit(limit: Option<i64>, default: i64, max: i64) -> i64 {
    limit.unwrap_or(default).clamp(1, max)
}

pub(super) fn build_page_info(limit: i64, next_cursor: Option<String>) -> PageInfoResponse {
    PageInfoResponse {
        limit,
        next_cursor: next_cursor.clone(),
        prev_cursor: None,
        has_more: next_cursor.is_some(),
    }
}

pub(super) fn parse_window_days(window: Option<&str>) -> Option<i64> {
    let raw = window.unwrap_or("30d");
    let raw = raw.trim().to_ascii_lowercase();
    let days = raw.strip_suffix('d')?.parse::<i64>().ok()?;
    if !(1..=3650).contains(&days) {
        return None;
    }
    Some(days)
}

pub(super) fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
        return Some(parsed.with_timezone(&Utc));
    }
    let date = NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok()?;
    date.and_hms_opt(0, 0, 0)
        .map(|naive| Utc.from_utc_datetime(&naive))
}

pub(super) fn slice_by_offsets(text: &str, start: i32, end: i32) -> Option<String> {
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

pub(super) fn strip_quoted_lines(body: &str, strip_quotes: bool) -> String {
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

pub(super) fn build_snippet(body: Option<&str>) -> Option<String> {
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
    Some(format!(
        "{}...",
        collapsed.chars().take(220).collect::<String>()
    ))
}

pub(super) fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

pub(super) fn hex_decode(raw: &str) -> Option<Vec<u8>> {
    if !raw.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(raw.len() / 2);
    let bytes = raw.as_bytes();
    for idx in (0..raw.len()).step_by(2) {
        let hi = bytes[idx] as char;
        let lo = bytes[idx + 1] as char;
        let value = hi.to_digit(16)? * 16 + lo.to_digit(16)?;
        out.push(value as u8);
    }
    Some(out)
}

pub(super) fn short_hash(value: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(value).unwrap_or_default());
    let digest = hasher.finalize();
    hex_encode(&digest[..16])
}

pub(super) fn encode_cursor_token<T: Serialize>(token: &T) -> Option<String> {
    let raw = serde_json::to_vec(token).ok()?;
    Some(hex_encode(&raw))
}

pub(super) fn decode_cursor_token<T: for<'de> Deserialize<'de>>(raw: &str) -> Option<T> {
    let bytes = hex_decode(raw)?;
    serde_json::from_slice(&bytes).ok()
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, header};
    use nexus_db::SeriesLogicalCompareRow;

    use super::{
        SearchRequestHashInput, build_page_info, encode_search_cursor, hybrid_requested,
        if_none_match_matches, normalize_limit, parse_search_cursor, parse_timestamp,
        parse_window_days, search_request_hash, slice_by_offsets, strip_quoted_lines,
        to_patch_compare_rows,
    };

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
    fn normalizes_limit_params() {
        assert_eq!(normalize_limit(None, 50, 200), 50);
        assert_eq!(normalize_limit(Some(0), 50, 200), 1);
        assert_eq!(normalize_limit(Some(1_000), 50, 200), 200);
        assert_eq!(normalize_limit(Some(25), 50, 200), 25);
    }

    #[test]
    fn builds_page_info_metadata() {
        let page = build_page_info(50, Some("next-token".to_string()));
        assert_eq!(page.limit, 50);
        assert_eq!(page.next_cursor.as_deref(), Some("next-token"));
        assert!(page.has_more);

        let empty = build_page_info(50, None);
        assert_eq!(empty.limit, 50);
        assert!(empty.next_cursor.is_none());
        assert!(!empty.has_more);
    }

    #[test]
    fn parses_window_days() {
        assert_eq!(parse_window_days(None), Some(30));
        assert_eq!(parse_window_days(Some("7d")), Some(7));
        assert_eq!(parse_window_days(Some("0d")), None);
        assert_eq!(parse_window_days(Some("bad")), None);
    }

    #[test]
    fn parses_rfc3339_and_date() {
        assert!(parse_timestamp("2026-02-13T12:30:00Z").is_some());
        assert!(parse_timestamp("2026-02-13").is_some());
        assert!(parse_timestamp("nope").is_none());
    }

    #[test]
    fn if_none_match_handles_weak_tags() {
        let mut headers = HeaderMap::new();
        headers.insert(header::IF_NONE_MATCH, "W/\"abc\"".parse().expect("header"));
        assert!(if_none_match_matches(&headers, "\"abc\""));
    }

    #[test]
    fn patch_compare_filters_non_patch_rows() {
        let rows = vec![
            SeriesLogicalCompareRow {
                slot: 1,
                title_norm: "cover".to_string(),
                v1_patch_item_id: None,
                v1_item_type: Some("cover".to_string()),
                v1_patch_id_stable: None,
                v1_subject_raw: None,
                v2_patch_item_id: Some(9),
                v2_item_type: Some("cover".to_string()),
                v2_patch_id_stable: None,
                v2_subject_raw: Some("[PATCH 0/3] demo".to_string()),
            },
            SeriesLogicalCompareRow {
                slot: 2,
                title_norm: "part two".to_string(),
                v1_patch_item_id: Some(7),
                v1_item_type: Some("patch".to_string()),
                v1_patch_id_stable: Some("old".to_string()),
                v1_subject_raw: Some("[PATCH 2/3] part two".to_string()),
                v2_patch_item_id: Some(10),
                v2_item_type: Some("patch".to_string()),
                v2_patch_id_stable: Some("new".to_string()),
                v2_subject_raw: Some("[PATCH v2 2/3] part two".to_string()),
            },
        ];

        let mapped = to_patch_compare_rows(rows);
        assert_eq!(mapped.len(), 1);
        assert_eq!(mapped[0].slot, 2);
        assert_eq!(mapped[0].status, "changed");
    }

    #[test]
    fn search_cursor_round_trip() {
        let encoded = encode_search_cursor(40, "abcd1234");
        let decoded = parse_search_cursor(&encoded).expect("decode");
        assert_eq!(decoded.0, 40);
        assert_eq!(decoded.1, "abcd1234");
    }

    #[test]
    fn search_request_hash_changes_with_query_shape() {
        let first = search_request_hash(SearchRequestHashInput {
            q: "mm reclaim",
            scope: nexus_core::search::SearchScope::Thread,
            list_key: Some("lkml"),
            author: None,
            from_ts: None,
            to_ts: None,
            has_diff: Some(true),
            sort: "relevance",
            limit: 20,
            hybrid: false,
            semantic_ratio: None,
            model_key: None,
        });
        let second = search_request_hash(SearchRequestHashInput {
            q: "mm reclaim",
            scope: nexus_core::search::SearchScope::Series,
            list_key: Some("lkml"),
            author: None,
            from_ts: None,
            to_ts: None,
            has_diff: Some(true),
            sort: "relevance",
            limit: 20,
            hybrid: false,
            semantic_ratio: None,
            model_key: None,
        });
        assert_ne!(first, second);
    }

    #[test]
    fn hybrid_flag_detection_matches_expected_contract() {
        assert!(!hybrid_requested(None, None));
        assert!(hybrid_requested(Some(true), None));
        assert!(hybrid_requested(None, Some(0.4)));
    }

    #[test]
    fn search_request_hash_changes_for_hybrid_shape() {
        let lexical = search_request_hash(SearchRequestHashInput {
            q: "mm reclaim",
            scope: nexus_core::search::SearchScope::Thread,
            list_key: Some("lkml"),
            author: None,
            from_ts: None,
            to_ts: None,
            has_diff: Some(true),
            sort: "relevance",
            limit: 20,
            hybrid: false,
            semantic_ratio: None,
            model_key: None,
        });
        let hybrid = search_request_hash(SearchRequestHashInput {
            q: "mm reclaim",
            scope: nexus_core::search::SearchScope::Thread,
            list_key: Some("lkml"),
            author: None,
            from_ts: None,
            to_ts: None,
            has_diff: Some(true),
            sort: "relevance",
            limit: 20,
            hybrid: true,
            semantic_ratio: Some(0.35),
            model_key: Some("qwen/qwen3-embedding-4b"),
        });
        assert_ne!(lexical, hybrid);
    }
}
