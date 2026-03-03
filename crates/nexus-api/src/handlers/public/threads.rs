use super::*;

pub async fn list_threads(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(list_key): Path<String>,
    Query(query): Query<ThreadListQuery>,
) -> HandlerResult<Response> {
    let Some(list) = state
        .catalog
        .get_mailing_list(&list_key)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    let sort = query.sort.as_deref().unwrap_or("activity_desc");
    if sort != "activity_desc" && sort != "date_desc" && sort != "date_asc" {
        return Err(StatusCode::UNPROCESSABLE_ENTITY.into());
    }

    let limit = normalize_limit(query.limit, 50, 200);
    let fetch_limit = limit + 1;
    let from_ts = match query.from.as_deref() {
        Some(raw) => Some(parse_timestamp(raw).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?),
        None => None,
    };
    let to_ts = match query.to.as_deref() {
        Some(raw) => Some(parse_timestamp(raw).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?),
        None => None,
    };

    let cursor_hash = short_hash(&json!({
        "list_id": list.id,
        "sort": sort,
        "from_ts": from_ts.map(|value| value.timestamp_millis()),
        "to_ts": to_ts.map(|value| value.timestamp_millis()),
        "author": query.author.as_deref().unwrap_or(""),
        "has_diff": query.has_diff,
    }));
    let (cursor_ts, cursor_id) = if let Some(raw_cursor) = query.cursor.as_deref() {
        let token: ThreadListCursorToken =
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

    let params = ListThreadsParams {
        sort: sort.to_string(),
        from_ts,
        to_ts,
        author_email: query.author.clone(),
        has_diff: query.has_diff,
        limit: fetch_limit,
        cursor_ts,
        cursor_id,
    };
    let mut items = state
        .lineage
        .list_threads(list.id, &params)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let limit_usize = limit_to_usize(limit);
    let has_more = items.len() > limit_usize;
    if has_more {
        items.truncate(limit_usize);
    }
    let next_cursor = if has_more {
        items.last().and_then(|item| {
            let ts = if sort == "date_desc" || sort == "date_asc" {
                item.created_at.timestamp_millis()
            } else {
                item.last_activity_at.timestamp_millis()
            };
            encode_cursor_token(&ThreadListCursorToken {
                v: 1,
                h: cursor_hash.clone(),
                ts,
                id: item.thread_id,
            })
        })
    } else {
        None
    };

    let thread_ids = items.iter().map(|item| item.thread_id).collect::<Vec<_>>();
    let mut participants_by_thread: HashMap<i64, Vec<ThreadMessageParticipant>> = HashMap::new();
    for participant in state
        .lineage
        .list_thread_participants(list.id, &thread_ids)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        let entries = participants_by_thread
            .entry(participant.thread_id)
            .or_default();
        if entries.len() < 8 {
            entries.push(ThreadMessageParticipant {
                name: participant.from_name,
                email: participant.from_email,
            });
        }
    }

    let response = ThreadListResponse {
        items: items
            .into_iter()
            .map(|item| {
                let starter = item.starter_email.map(|email| ThreadMessageParticipant {
                    name: item.starter_name,
                    email,
                });

                ThreadListItemResponse {
                    thread_id: item.thread_id,
                    subject: item.subject_norm,
                    root_message_id: item.root_message_pk,
                    created_at: item.created_at,
                    last_activity_at: item.last_activity_at,
                    message_count: item.message_count,
                    participants: participants_by_thread
                        .remove(&item.thread_id)
                        .unwrap_or_default(),
                    starter,
                    has_diff: item.has_diff,
                }
            })
            .collect(),
        page_info: build_page_info(limit, next_cursor),
    };

    json_response_with_cache(&headers, &response, CACHE_THREAD, None)
}

pub async fn list_thread_detail(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(path): Path<ThreadPath>,
) -> HandlerResult<Response> {
    let Some(list) = state
        .catalog
        .get_mailing_list(&path.list_key)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    let Some(summary) = state
        .lineage
        .get_thread_summary(list.id, path.thread_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    let messages = state
        .lineage
        .list_thread_messages(list.id, path.thread_id, 10_000, None, None)
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
            body_text: None,
        })
        .collect::<Vec<_>>();

    let etag = format!("\"{}\"", hex_encode(&summary.membership_hash));
    json_response_with_cache(
        &headers,
        &ThreadDetailResponse {
            thread_id: path.thread_id,
            list_key: path.list_key,
            subject: summary.subject_norm,
            membership_hash: hex_encode(&summary.membership_hash),
            last_activity_at: summary.last_activity_at,
            messages,
        },
        CACHE_THREAD,
        Some(etag),
    )
}

pub async fn thread_messages(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(path): Path<ThreadPath>,
    Query(query): Query<ThreadMessagesQuery>,
) -> HandlerResult<Response> {
    let Some(list) = state
        .catalog
        .get_mailing_list(&path.list_key)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    let view = query.view.as_deref().unwrap_or("snippets");
    let include_body = match view {
        "full" => true,
        "snippets" => false,
        _ => return Err(StatusCode::UNPROCESSABLE_ENTITY.into()),
    };
    let limit = normalize_limit(query.limit, 50, 200);
    let fetch_limit = limit + 1;

    let exists = state
        .lineage
        .get_thread_summary(list.id, path.thread_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();
    if !exists {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let cursor_hash = short_hash(&json!({
        "list_id": list.id,
        "thread_id": path.thread_id,
        "view": view,
    }));
    let (cursor_sort_key, cursor_message_id): (Option<Vec<u8>>, Option<i64>) =
        if let Some(raw_cursor) = query.cursor.as_deref() {
            let token: ThreadMessagesCursorToken =
                decode_cursor_token(raw_cursor).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;
            if token.v != 1 || token.h != cursor_hash {
                return Err(StatusCode::UNPROCESSABLE_ENTITY.into());
            }
            let sort_key = hex_decode(&token.sort_key).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;
            (Some(sort_key), Some(token.message_id))
        } else {
            (None, None)
        };

    let mut message_rows = state
        .lineage
        .list_thread_messages(
            list.id,
            path.thread_id,
            fetch_limit,
            cursor_sort_key.as_deref(),
            cursor_message_id,
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let limit_usize = limit_to_usize(limit);
    let has_more = message_rows.len() > limit_usize;
    if has_more {
        message_rows.truncate(limit_usize);
    }
    let next_cursor = if has_more {
        message_rows.last().and_then(|msg| {
            encode_cursor_token(&ThreadMessagesCursorToken {
                v: 1,
                h: cursor_hash.clone(),
                sort_key: hex_encode(&msg.sort_key),
                message_id: msg.message_pk,
            })
        })
    } else {
        None
    };

    let messages = message_rows
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
            snippet: if include_body {
                None
            } else {
                build_snippet(msg.body_text.as_deref())
            },
            body_text: if include_body { msg.body_text } else { None },
        })
        .collect::<Vec<_>>();

    json_response_with_cache(
        &headers,
        &ThreadMessagesResponse {
            thread_id: path.thread_id,
            list_key: path.list_key,
            view: view.to_string(),
            messages,
            page_info: build_page_info(limit, next_cursor),
        },
        CACHE_THREAD,
        None,
    )
}
