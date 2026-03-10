use super::*;

pub async fn series_list(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<SeriesListQuery>,
) -> HandlerResult<Response> {
    let limit = normalize_limit(query.limit, 30, 200);
    let fetch_limit = limit + 1;
    let sort = query.sort.as_deref().unwrap_or("last_seen_desc");
    if sort != "last_seen_desc" && sort != "last_seen_asc" {
        return Err(StatusCode::UNPROCESSABLE_ENTITY.into());
    }

    let cursor_hash = short_hash(&json!({
        "list_key": query.list_key.as_deref().unwrap_or(""),
        "merged": query.merged,
        "sort": sort,
    }));
    let (cursor_ts, cursor_id) = if let Some(raw_cursor) = query.cursor.as_deref() {
        let token: SeriesListCursorToken =
            decode_cursor_token(raw_cursor).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;
        if token.v != 1 || token.h != cursor_hash {
            return Err(StatusCode::UNPROCESSABLE_ENTITY.into());
        }
        let cursor_ts = Utc
            .timestamp_millis_opt(token.ts)
            .single()
            .ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;
        (Some(cursor_ts), Some(token.id))
    } else {
        (None, None)
    };

    ensure_mainline_filter_ready(&state, query.merged).await?;

    let mut items = state
        .lineage
        .list_series(
            query.list_key.as_deref(),
            query.merged,
            sort,
            fetch_limit,
            cursor_ts,
            cursor_id,
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let limit_usize = limit_to_usize(limit);
    let has_more = items.len() > limit_usize;
    if has_more {
        items.truncate(limit_usize);
    }
    let next_cursor = if has_more {
        items.last().and_then(|item| {
            encode_cursor_token(&SeriesListCursorToken {
                v: 1,
                h: cursor_hash.clone(),
                ts: item.last_seen_at.timestamp_millis(),
                id: item.series_id,
            })
        })
    } else {
        None
    };

    json_response_with_cache(
        &headers,
        &SeriesListResponse {
            items: items
                .into_iter()
                .map(|item| SeriesListItemResponse {
                    series_id: item.series_id,
                    canonical_subject: item.canonical_subject_norm,
                    author_email: item.author_email,
                    author_name: item.author_name,
                    first_seen_at: item.first_seen_at,
                    latest_patchset_at: item.latest_patchset_at,
                    last_seen_at: item.last_seen_at,
                    latest_version_num: item.latest_version_num,
                    is_rfc_latest: item.is_rfc_latest,
                    merge_summary: merge_summary_response(
                        item.mainline_merge_state,
                        item.mainline_matched_patch_count,
                        item.mainline_total_patch_count,
                        item.mainline_merged_in_tag,
                        item.mainline_merged_in_release,
                        item.mainline_merged_version_id,
                        item.mainline_single_patch_commit_oid,
                    ),
                })
                .collect(),
            page_info: build_page_info(limit, next_cursor),
        },
        CACHE_THREAD,
        None,
    )
}

pub async fn series_detail(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(series_id): Path<i64>,
) -> HandlerResult<Response> {
    let Some(series) = state
        .lineage
        .get_series_by_id(series_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    let lists = state
        .lineage
        .list_series_list_keys(series_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let versions = state
        .lineage
        .list_series_versions_with_counts(series_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let version_ids = versions
        .iter()
        .map(|version| version.id)
        .collect::<Vec<_>>();
    let thread_refs = state
        .lineage
        .list_thread_refs_for_series_versions(&version_ids)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut thread_refs_by_version =
        std::collections::BTreeMap::<i64, Vec<SeriesThreadRefResponse>>::new();
    for thread in thread_refs {
        thread_refs_by_version
            .entry(thread.patch_series_version_id)
            .or_default()
            .push(SeriesThreadRefResponse {
                list_key: thread.list_key,
                thread_id: thread.thread_id,
                message_count: thread.message_count,
                last_activity_at: thread.last_activity_at,
            });
    }

    let mut version_items = Vec::with_capacity(versions.len());
    for version in versions {
        version_items.push(SeriesVersionSummaryResponse {
            series_version_id: version.id,
            version_num: version.version_num,
            is_rfc: version.is_rfc,
            is_resend: version.is_resend,
            sent_at: version.sent_at,
            base_commit: version.base_commit,
            cover_message_id: version.cover_message_pk,
            thread_refs: thread_refs_by_version
                .remove(&version.id)
                .unwrap_or_default(),
            patch_count: version.patch_count,
            is_partial_reroll: version.is_partial_reroll,
            merge_summary: merge_summary_response(
                version.mainline_merge_state,
                version.mainline_matched_patch_count,
                version.mainline_total_patch_count,
                version.mainline_merged_in_tag,
                version.mainline_merged_in_release,
                None,
                version.mainline_single_patch_commit_oid,
            ),
        });
    }

    json_response_with_cache(
        &headers,
        &SeriesDetailResponse {
            series_id: series.id,
            canonical_subject: series.canonical_subject_norm,
            author: SeriesAuthorResponse {
                name: series.author_name,
                email: series.author_email,
            },
            first_seen_at: series.created_at,
            last_seen_at: series.last_seen_at,
            lists,
            versions: version_items,
            latest_version_id: series.latest_version_id,
            merge_summary: merge_summary_response(
                series.mainline_merge_state,
                series.mainline_matched_patch_count,
                series.mainline_total_patch_count,
                series.mainline_merged_in_tag,
                series.mainline_merged_in_release,
                series.mainline_merged_version_id,
                series.mainline_single_patch_commit_oid,
            ),
        },
        CACHE_THREAD,
        None,
    )
}

pub async fn series_version(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(path): Path<SeriesVersionPath>,
    Query(query): Query<SeriesVersionQuery>,
) -> HandlerResult<Response> {
    let assembled = query.assembled.unwrap_or(true);
    let Some(version) = state
        .lineage
        .get_series_version(path.series_id, path.series_version_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    let patch_items = state
        .lineage
        .list_series_version_patch_items(version.id, assembled)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .map(|item| SeriesVersionPatchItemResponse {
            patch_item_id: item.patch_item_id,
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
            inherited_from_version_num: item.inherited_from_version_num,
            mainline_commit: item
                .mainline_commit_oid
                .map(|commit_id| MainlineCommitResponse {
                    commit_id,
                    merged_in_tag: item.mainline_merged_in_tag,
                    merged_in_release: item.mainline_merged_in_release,
                    match_method: item
                        .mainline_match_method
                        .unwrap_or_else(|| "patch_id".to_string()),
                }),
        })
        .collect::<Vec<_>>();

    json_response_with_cache(
        &headers,
        &SeriesVersionResponse {
            series_id: version.patch_series_id,
            series_version_id: version.id,
            version_num: version.version_num,
            is_rfc: version.is_rfc,
            is_resend: version.is_resend,
            is_partial_reroll: version.is_partial_reroll,
            sent_at: version.sent_at,
            subject: version.subject_raw,
            subject_norm: version.subject_norm,
            base_commit: version.base_commit,
            cover_message_id: version.cover_message_pk,
            first_patch_message_id: version.first_patch_message_pk,
            assembled,
            merge_summary: merge_summary_response(
                version.mainline_merge_state,
                version.mainline_matched_patch_count,
                version.mainline_total_patch_count,
                version.mainline_merged_in_tag,
                version.mainline_merged_in_release,
                None,
                version.mainline_single_patch_commit_oid,
            ),
            patch_items,
        },
        CACHE_THREAD,
        None,
    )
}

async fn ensure_mainline_filter_ready(
    state: &ApiState,
    merged_filter: Option<bool>,
) -> Result<(), ApiError> {
    if merged_filter.is_none() {
        return Ok(());
    }

    let repo_key = format!("mainline:{}", state.settings.mainline.repo_path.trim());
    let scan_state = state
        .mainline
        .get_state(&repo_key)
        .await
        .map_err(|_| ApiError::internal("failed to load mainline filter readiness"))?;
    if scan_state
        .as_ref()
        .is_some_and(|scan_state| scan_state.public_filter_ready)
    {
        return Ok(());
    }

    Err(ApiError::from(StatusCode::CONFLICT)
        .with_detail("merged filter is not ready until the initial mainline bootstrap completes"))
}

pub async fn series_compare(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(series_id): Path<i64>,
    Query(query): Query<SeriesCompareQuery>,
) -> HandlerResult<Response> {
    let mode = query.mode.as_deref().unwrap_or("summary");
    if mode != "summary" && mode != "per_patch" && mode != "per_file" {
        return Err(StatusCode::UNPROCESSABLE_ENTITY.into());
    }

    let Some(v1) = state
        .lineage
        .get_series_version(series_id, query.v1)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    let Some(v2) = state
        .lineage
        .get_series_version(series_id, query.v2)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    let logical_rows = state
        .lineage
        .list_series_logical_compare(series_id, v1.version_num, v2.version_num)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let patch_rows = to_patch_compare_rows(logical_rows);

    let changed = patch_rows
        .iter()
        .filter(|row| row.status == "changed")
        .count();
    let added = patch_rows
        .iter()
        .filter(|row| row.status == "added")
        .count();
    let removed = patch_rows
        .iter()
        .filter(|row| row.status == "removed")
        .count();
    let v1_patch_count = patch_rows
        .iter()
        .filter(|row| row.v1_patch_item_id.is_some())
        .count();
    let v2_patch_count = patch_rows
        .iter()
        .filter(|row| row.v2_patch_item_id.is_some())
        .count();
    let changed = usize_to_i64(changed);
    let added = usize_to_i64(added);
    let removed = usize_to_i64(removed);
    let v1_patch_count = usize_to_i64(v1_patch_count);
    let v2_patch_count = usize_to_i64(v2_patch_count);

    let summary = SeriesCompareSummary {
        v1_patch_count,
        v2_patch_count,
        patch_count_delta: v2_patch_count - v1_patch_count,
        changed,
        added,
        removed,
    };

    let files = if mode == "per_file" {
        let v1_ids = patch_rows
            .iter()
            .filter_map(|row| row.v1_patch_item_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let v2_ids = patch_rows
            .iter()
            .filter_map(|row| row.v2_patch_item_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let v1_files = state
            .lineage
            .aggregate_patch_item_files(&v1_ids)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let v2_files = state
            .lineage
            .aggregate_patch_item_files(&v2_ids)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let mut v1_map = BTreeMap::new();
        for row in v1_files {
            let key = row.path.clone();
            v1_map.insert(key, row);
        }
        let mut v2_map = BTreeMap::new();
        for row in v2_files {
            let key = row.path.clone();
            v2_map.insert(key, row);
        }

        let mut paths = BTreeSet::new();
        for path in v1_map.keys() {
            paths.insert(path.clone());
        }
        for path in v2_map.keys() {
            paths.insert(path.clone());
        }

        let mut rows = Vec::with_capacity(paths.len());
        for path in paths {
            let left = v1_map.get(&path);
            let right = v2_map.get(&path);
            let (status, additions_delta, deletions_delta, hunks_delta) = match (left, right) {
                (None, Some(newer)) => (
                    "added".to_string(),
                    newer.additions,
                    newer.deletions,
                    newer.hunk_count,
                ),
                (Some(older), None) => (
                    "removed".to_string(),
                    -older.additions,
                    -older.deletions,
                    -older.hunk_count,
                ),
                (Some(older), Some(newer)) => {
                    let additions_delta = newer.additions - older.additions;
                    let deletions_delta = newer.deletions - older.deletions;
                    let hunks_delta = newer.hunk_count - older.hunk_count;
                    let status = if additions_delta == 0 && deletions_delta == 0 && hunks_delta == 0
                    {
                        "unchanged".to_string()
                    } else {
                        "changed".to_string()
                    };
                    (status, additions_delta, deletions_delta, hunks_delta)
                }
                (None, None) => continue,
            };

            rows.push(SeriesCompareFileRow {
                path,
                status,
                additions_delta,
                deletions_delta,
                hunks_delta,
            });
        }
        Some(rows)
    } else {
        None
    };

    json_response_with_cache(
        &headers,
        &SeriesCompareResponse {
            series_id,
            v1: query.v1,
            v2: query.v2,
            mode: mode.to_string(),
            summary,
            patches: if mode == "per_patch" {
                Some(patch_rows)
            } else {
                None
            },
            files,
        },
        CACHE_THREAD,
        None,
    )
}
