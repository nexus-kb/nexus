use super::*;

pub async fn search(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<SearchQuery>,
) -> HandlerResult<Response> {
    let q = query.q.trim();
    if q.is_empty() {
        return Err(ApiError::validation("q must not be empty")
            .with_invalid_param("q", "expected non-empty search query"));
    }

    let scope = match query.scope.as_deref() {
        Some("thread") => SearchScope::Thread,
        Some("series") => SearchScope::Series,
        Some(_) => {
            return Err(ApiError::validation("scope must be one of: thread, series")
                .with_invalid_param("scope", "unsupported search scope"));
        }
        None => SearchScope::Thread,
    };
    let spec = scope.index_kind().spec();

    let hybrid_enabled = hybrid_requested(query.hybrid, query.semantic_ratio);
    let semantic_ratio = if hybrid_enabled {
        let semantic_ratio = query.semantic_ratio.unwrap_or(0.35);
        if !(0.0..=1.0).contains(&semantic_ratio) {
            return Err(
                ApiError::validation("semantic_ratio must be between 0.0 and 1.0")
                    .with_invalid_param("semantic_ratio", "expected inclusive range [0.0, 1.0]"),
            );
        }
        Some(semantic_ratio)
    } else {
        None
    };

    let sort = query.sort.as_deref().unwrap_or("relevance");
    if sort != "relevance" && sort != "date_desc" {
        return Err(
            ApiError::validation("sort must be one of: relevance, date_desc")
                .with_invalid_param("sort", "unsupported sort value"),
        );
    }

    let from_ts = match query.from.as_deref() {
        Some(raw) => Some(parse_timestamp(raw).ok_or_else(|| {
            ApiError::validation("invalid from timestamp")
                .with_invalid_param("from", "expected RFC3339 or YYYY-MM-DD")
        })?),
        None => None,
    };
    let to_ts = match query.to.as_deref() {
        Some(raw) => Some(parse_timestamp(raw).ok_or_else(|| {
            ApiError::validation("invalid to timestamp")
                .with_invalid_param("to", "expected RFC3339 or YYYY-MM-DD")
        })?),
        None => None,
    };

    let limit = normalize_limit(query.limit, 20, 100);
    let limit_usize = limit_to_usize(limit);
    let request_hash = search_request_hash(SearchRequestHashInput {
        q,
        scope,
        list_key: query.list_key.as_deref(),
        author: query.author.as_deref(),
        from_ts,
        to_ts,
        has_diff: query.has_diff,
        sort,
        limit: limit_usize,
        hybrid: hybrid_enabled,
        semantic_ratio,
        model_key: if hybrid_enabled {
            Some(state.settings.embeddings.model.as_str())
        } else {
            None
        },
    });
    let mut offset = 0usize;
    let mut date_desc_cursor: Option<(i64, i64)> = None;
    if let Some(cursor) = query.cursor.as_deref() {
        match sort {
            "date_desc" => {
                let (cursor_ts, cursor_id, cursor_hash) = parse_date_desc_search_cursor(cursor)
                    .ok_or_else(|| {
                        ApiError::validation("invalid cursor format")
                            .with_invalid_param("cursor", "expected opaque search cursor token")
                    })?;
                if cursor_hash != request_hash {
                    return Err(
                        ApiError::validation("cursor does not match request filters")
                            .with_invalid_param(
                                "cursor",
                                "cursor must be used with unchanged query shape",
                            ),
                    );
                }
                date_desc_cursor = Some((cursor_ts, cursor_id));
            }
            _ => {
                let (parsed_offset, cursor_hash) =
                    parse_search_cursor(cursor).ok_or_else(|| {
                        ApiError::validation("invalid cursor format")
                            .with_invalid_param("cursor", "expected opaque search cursor token")
                    })?;
                if cursor_hash != request_hash {
                    return Err(
                        ApiError::validation("cursor does not match request filters")
                            .with_invalid_param(
                                "cursor",
                                "cursor must be used with unchanged query shape",
                            ),
                    );
                }
                offset = parsed_offset;
            }
        }
    }

    let mut filters: Vec<String> = Vec::new();
    if let Some(list_key) = query.list_key.as_deref() {
        filters.push(format!("list_keys = \"{}\"", escape_filter_value(list_key)));
    }
    if let Some(author) = query.author.as_deref() {
        filters.push(format!(
            "{} = \"{}\"",
            spec.author_filter_field,
            escape_filter_value(author)
        ));
    }
    if let Some(has_diff) = query.has_diff {
        filters.push(format!("has_diff = {has_diff}"));
    }
    if let Some(from_ts) = from_ts {
        filters.push(format!("date_ts >= {}", from_ts.timestamp()));
    }
    if let Some(to_ts) = to_ts {
        filters.push(format!("date_ts < {}", to_ts.timestamp()));
    }
    if let Some((cursor_ts, cursor_id)) = date_desc_cursor {
        filters.push(format!(
            "(date_ts < {cursor_ts} OR (date_ts = {cursor_ts} AND id < {cursor_id}))"
        ));
    }

    let fetch_limit = if sort == "date_desc" {
        limit.saturating_add(1)
    } else {
        limit
    };

    let mut request_body = json!({
        "q": q,
        "limit": fetch_limit,
        "facets": spec.default_facets,
        "attributesToHighlight": spec.highlight_attributes,
        "attributesToCrop": spec.crop_attributes,
        "cropLength": spec.crop_length
    });
    if sort != "date_desc" {
        request_body["offset"] = json!(offset);
    }
    if !filters.is_empty() {
        if filters.len() == 1 {
            request_body["filter"] = Value::String(filters[0].clone());
        } else {
            request_body["filter"] = json!(filters);
        }
    }
    if sort == "date_desc" {
        request_body["sort"] = json!(["date_ts:desc", "id:desc"]);
    }
    if hybrid_enabled {
        let vector = match compute_or_get_query_embedding(&state, scope, q).await {
            Ok(value) => value,
            Err(message) => return Err(ApiError::upstream(message)),
        };
        request_body["hybrid"] = json!({
            "embedder": state.settings.embeddings.embedder_name.clone(),
            "semanticRatio": semantic_ratio.unwrap_or(0.35),
        });
        request_body["vector"] = json!(vector);
    }

    let search_response = state
        .http_client
        .post(format!(
            "{}/indexes/{}/search",
            state.settings.meili.url.trim_end_matches('/'),
            spec.uid
        ))
        .bearer_auth(&state.settings.meili.master_key)
        .json(&request_body)
        .send()
        .await
        .map_err(|_| ApiError::upstream("search backend request failed"))?;

    if !search_response.status().is_success() {
        return match search_response.status().as_u16() {
            400 | 401 | 403 | 404 | 422 => Err(ApiError::validation(
                "search backend rejected request parameters",
            )),
            _ => Err(ApiError::upstream(
                "search backend returned an unexpected error",
            )),
        };
    }

    let payload: Value = search_response
        .json()
        .await
        .map_err(|_| ApiError::upstream("failed to parse search backend response"))?;
    let total_hits = payload
        .get("estimatedTotalHits")
        .and_then(Value::as_u64)
        .or_else(|| payload.get("totalHits").and_then(Value::as_u64))
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(0);

    let mut hits = payload
        .get("hits")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut has_more_date_desc = false;
    if sort == "date_desc" && hits.len() > limit_usize {
        hits.truncate(limit_usize);
        has_more_date_desc = true;
    }

    let mut fallback_ids: Vec<i64> = match scope {
        SearchScope::Thread => hits
            .iter()
            .filter_map(|hit| {
                let id = hit.get("id").and_then(Value::as_i64)?;
                let missing_metadata = hit.get("message_count").is_none()
                    || hit.get("created_at").is_none()
                    || hit.get("last_activity_at").is_none()
                    || hit.get("starter_email").is_none()
                    || hit.get("list_key").is_none();
                if missing_metadata { Some(id) } else { None }
            })
            .collect(),
        SearchScope::Series => hits
            .iter()
            .filter_map(|hit| {
                let id = hit.get("id").and_then(Value::as_i64)?;
                let missing_metadata =
                    hit.get("latest_version_num").is_none() || hit.get("is_rfc_latest").is_none();
                if missing_metadata { Some(id) } else { None }
            })
            .collect(),
        SearchScope::PatchItem => Vec::new(),
    };
    fallback_ids.sort_unstable();
    fallback_ids.dedup();

    let fallback_docs_by_id: HashMap<i64, Value> = if fallback_ids.is_empty() {
        HashMap::new()
    } else {
        let search_store = SearchStore::new(state.db.pool().clone());
        let docs = match scope {
            SearchScope::Thread => search_store.build_thread_metadata_docs(&fallback_ids).await,
            SearchScope::Series => {
                search_store
                    .build_patch_series_metadata_docs(&fallback_ids)
                    .await
            }
            SearchScope::PatchItem => Ok(Vec::new()),
        }
        .map_err(|_| ApiError::internal("failed to hydrate fallback search metadata"))?;

        docs.into_iter()
            .filter_map(|doc| doc.get("id").and_then(Value::as_i64).map(|id| (id, doc)))
            .collect()
    };

    let mut items = Vec::with_capacity(hits.len());
    let mut highlights = BTreeMap::new();
    for hit in &hits {
        let Some(id) = hit.get("id").and_then(Value::as_i64) else {
            continue;
        };
        let route = hit
            .get("route")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .unwrap_or_else(|| default_route_for_scope(scope, id));
        let title = hit
            .get("title")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .unwrap_or_else(|| format!("Result {id}"));
        let list_keys = hit
            .get("list_keys")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let snippet = extract_snippet(&hit, spec.crop_attributes);
        let author_email = hit
            .get("author_email")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .or_else(|| {
                hit.get("author_emails")
                    .and_then(Value::as_array)
                    .and_then(|values| values.first())
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            });

        let mut metadata = serde_json::Map::new();
        for key in [
            "message_id_primary",
            "patch_series_id",
            "series_version_id",
            "version_num",
            "ordinal",
            "is_rfc",
            "latest_version_num",
            "latest_version_id",
            "is_rfc_latest",
            "list_key",
            "created_at",
            "last_activity_at",
            "message_count",
            "starter_name",
            "starter_email",
            "participants",
        ] {
            if let Some(value) = hit.get(key) {
                metadata.insert(key.to_string(), value.clone());
            }
        }
        if let Some(doc) = fallback_docs_by_id.get(&id) {
            for key in [
                "message_id_primary",
                "patch_series_id",
                "series_version_id",
                "version_num",
                "ordinal",
                "is_rfc",
                "latest_version_num",
                "latest_version_id",
                "is_rfc_latest",
                "list_key",
                "created_at",
                "last_activity_at",
                "message_count",
                "starter_name",
                "starter_email",
                "participants",
            ] {
                if metadata.contains_key(key) {
                    continue;
                }
                if let Some(value) = doc.get(key) {
                    metadata.insert(key.to_string(), value.clone());
                }
            }
        }

        if let Some(formatted) = hit.get("_formatted") {
            highlights.insert(id.to_string(), formatted.clone());
        }

        items.push(SearchItemResponse {
            scope: scope.as_str().to_string(),
            id,
            title,
            snippet,
            route,
            date_utc: hit
                .get("date_utc")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            list_keys,
            has_diff: hit
                .get("has_diff")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            author_email,
            metadata: Value::Object(metadata),
        });
    }

    let facets = normalize_search_facets(
        payload
            .get("facetDistribution")
            .cloned()
            .unwrap_or_else(|| json!({})),
        spec.author_filter_field,
    );
    let next_cursor = if sort == "date_desc" {
        if has_more_date_desc {
            hits.last().and_then(|hit| {
                let date_ts = hit.get("date_ts").and_then(Value::as_i64)?;
                let id = hit.get("id").and_then(Value::as_i64)?;
                Some(encode_date_desc_search_cursor(date_ts, id, &request_hash))
            })
        } else {
            None
        }
    } else {
        let next_offset = offset.saturating_add(items.len());
        if !items.is_empty() && next_offset < total_hits {
            Some(encode_search_cursor(next_offset, &request_hash))
        } else {
            None
        }
    };

    json_response_with_cache(
        &headers,
        &SearchResponse {
            items,
            facets,
            highlights,
            page_info: build_page_info(limit, next_cursor),
        },
        CACHE_SEARCH,
        None,
    )
}
