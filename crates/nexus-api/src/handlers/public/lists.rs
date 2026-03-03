use super::*;

pub async fn list_catalog(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<CursorQuery>,
) -> Result<Response, StatusCode> {
    let limit = normalize_limit(query.limit, 50, 200);
    let cursor_list_key = if let Some(raw) = query.cursor.as_deref() {
        let token: ListCatalogCursorToken =
            decode_cursor_token(raw).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;
        if token.v != 1 {
            return Err(StatusCode::UNPROCESSABLE_ENTITY);
        }
        Some(token.list_key)
    } else {
        None
    };

    let mut rows = state
        .catalog
        .list_mailing_lists(limit + 1, cursor_list_key.as_deref())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let limit_usize = limit_to_usize(limit);
    let has_more = rows.len() > limit_usize;
    if has_more {
        rows.truncate(limit_usize);
    }
    let next_cursor = if has_more {
        rows.last().and_then(|row| {
            encode_cursor_token(&ListCatalogCursorToken {
                v: 1,
                list_key: row.list_key.clone(),
            })
        })
    } else {
        None
    };

    let response = ListCatalogResponse {
        items: rows
            .into_iter()
            .map(|row| ListSummaryResponse {
                list_key: row.list_key,
                description: row.description,
                posting_address: row.posting_address,
                latest_activity_at: row.latest_activity_at,
                thread_count_30d: row.thread_count_30d,
                message_count_30d: row.message_count_30d,
            })
            .collect(),
        page_info: build_page_info(limit, next_cursor),
    };

    json_response_with_cache(&headers, &response, CACHE_THREAD, None)
}

pub async fn list_detail(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(list_key): Path<String>,
) -> Result<Response, StatusCode> {
    let Some(record) = state
        .catalog
        .get_mailing_list_detail(&list_key)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let response = ListDetailResponse {
        list_key: record.list_key,
        description: record.description,
        posting_address: record.posting_address,
        mirror_state: ListMirrorStateResponse {
            active_repos: record.active_repo_count,
            total_repos: record.repo_count,
            latest_repo_watermark_updated_at: record.latest_repo_watermark_updated_at,
        },
        counts: ListCountsResponse {
            messages: record.message_count,
            threads: record.thread_count,
            patch_series: record.patch_series_count,
        },
        facets_hint: ListFacetsHintResponse {
            default_scope: "thread".to_string(),
            available_scopes: vec!["thread".to_string(), "series".to_string()],
        },
    };

    json_response_with_cache(&headers, &response, CACHE_THREAD, None)
}

pub async fn list_stats(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(list_key): Path<String>,
    Query(query): Query<ListStatsQuery>,
) -> Result<Response, StatusCode> {
    let Some(list) = state
        .catalog
        .get_mailing_list(&list_key)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let window_days =
        parse_window_days(query.window.as_deref()).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;
    let since = Utc::now() - Duration::days(window_days);
    let totals = state
        .catalog
        .get_list_stats_totals(list.id, since)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let top_authors = state
        .catalog
        .get_list_top_authors(list.id, since, 10)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let activity = state
        .catalog
        .get_list_activity_by_day(list.id, since)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let response = ListStatsResponse {
        messages: totals.messages,
        threads: totals.threads,
        patch_series: totals.patch_series,
        top_authors: top_authors
            .into_iter()
            .map(|author| ListTopAuthorResponse {
                from_email: author.from_email,
                from_name: author.from_name,
                message_count: author.message_count,
            })
            .collect(),
        activity_by_day: activity
            .into_iter()
            .map(|day| ListActivityByDayResponse {
                day_utc: day.day_utc,
                messages: day.messages,
                threads: day.threads,
            })
            .collect(),
    };

    json_response_with_cache(&headers, &response, CACHE_THREAD, None)
}
