use super::*;

pub async fn search(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<SearchQuery>,
) -> Result<Response, StatusCode> {
    let q = query.q.trim();
    if q.is_empty() {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }

    let scope = match query.scope.as_deref() {
        Some(raw) => SearchScope::parse(raw).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?,
        None => SearchScope::Thread,
    };
    if matches!(scope, SearchScope::PatchItem) {
        return json_error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({ "error": "patch_item search is temporarily disabled" }),
        );
    }
    let spec = scope.index_kind().spec();

    let hybrid_enabled = hybrid_requested(query.hybrid, query.semantic_ratio);
    let semantic_ratio = if hybrid_enabled {
        let semantic_ratio = query.semantic_ratio.unwrap_or(0.35);
        if !(0.0..=1.0).contains(&semantic_ratio) {
            return Err(StatusCode::UNPROCESSABLE_ENTITY);
        }
        if !state.settings.embeddings.enabled {
            return json_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({ "error": "hybrid requires embeddings to be enabled" }),
            );
        }
        if !matches!(scope, SearchScope::Thread | SearchScope::Series) {
            return json_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({ "error": "hybrid is only available for thread and series scopes" }),
            );
        }
        Some(semantic_ratio)
    } else {
        None
    };

    let sort = query.sort.as_deref().unwrap_or("relevance");
    if sort != "relevance" && sort != "date_desc" {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }

    let from_ts = match query.from.as_deref() {
        Some(raw) => Some(parse_timestamp(raw).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?),
        None => None,
    };
    let to_ts = match query.to.as_deref() {
        Some(raw) => Some(parse_timestamp(raw).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?),
        None => None,
    };

    let limit = query.limit.unwrap_or(20).clamp(1, 100) as usize;
    let request_hash = search_request_hash(SearchRequestHashInput {
        q,
        scope,
        list_key: query.list_key.as_deref(),
        author: query.author.as_deref(),
        from_ts,
        to_ts,
        has_diff: query.has_diff,
        sort,
        limit,
        hybrid: hybrid_enabled,
        semantic_ratio,
        model_key: if hybrid_enabled {
            Some(state.settings.embeddings.model.as_str())
        } else {
            None
        },
    });
    let offset = if let Some(cursor) = query.cursor.as_deref() {
        let (offset, cursor_hash) =
            parse_search_cursor(cursor).ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;
        if cursor_hash != request_hash {
            return Err(StatusCode::UNPROCESSABLE_ENTITY);
        }
        offset
    } else {
        0
    };

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

    let mut request_body = json!({
        "q": q,
        "offset": offset,
        "limit": limit,
        "facets": spec.default_facets,
        "attributesToHighlight": spec.highlight_attributes,
        "attributesToCrop": spec.crop_attributes,
        "cropLength": spec.crop_length
    });
    if !filters.is_empty() {
        if filters.len() == 1 {
            request_body["filter"] = Value::String(filters[0].clone());
        } else {
            request_body["filter"] = json!(filters);
        }
    }
    if sort == "date_desc" {
        request_body["sort"] = json!(["date_ts:desc"]);
    }
    if hybrid_enabled {
        let vector = match compute_or_get_query_embedding(&state, scope, q).await {
            Ok(value) => value,
            Err(message) => {
                return json_error_response(StatusCode::BAD_GATEWAY, json!({ "error": message }));
            }
        };
        request_body["hybrid"] = json!({
            "embedder": state.settings.embeddings.embedder_name.clone(),
            "semanticRatio": semantic_ratio.unwrap_or(0.35),
        });
        request_body["vector"] = json!(vector);
    }

    let search_response = reqwest::Client::new()
        .post(format!(
            "{}/indexes/{}/search",
            state.settings.meili.url.trim_end_matches('/'),
            spec.uid
        ))
        .bearer_auth(&state.settings.meili.master_key)
        .json(&request_body)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

    if !search_response.status().is_success() {
        return match search_response.status().as_u16() {
            400 | 401 | 403 | 404 | 422 => Err(StatusCode::UNPROCESSABLE_ENTITY),
            _ => Err(StatusCode::BAD_GATEWAY),
        };
    }

    let payload: Value = search_response
        .json()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let total_hits = payload
        .get("estimatedTotalHits")
        .and_then(Value::as_u64)
        .or_else(|| payload.get("totalHits").and_then(Value::as_u64))
        .unwrap_or(0) as usize;

    let hits = payload
        .get("hits")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let should_hydrate_thread_metadata = scope == SearchScope::Thread
        && hits.iter().any(|hit| {
            hit.get("message_count").is_none()
                || hit.get("created_at").is_none()
                || hit.get("last_activity_at").is_none()
                || hit.get("starter_email").is_none()
        });
    let should_hydrate_series_metadata = scope == SearchScope::Series
        && hits.iter().any(|hit| {
            hit.get("latest_version_num").is_none() || hit.get("is_rfc_latest").is_none()
        });

    let fallback_docs_by_id: HashMap<i64, Value> =
        if should_hydrate_thread_metadata || should_hydrate_series_metadata {
            let hit_ids: Vec<i64> = hits
                .iter()
                .filter_map(|hit| hit.get("id").and_then(Value::as_i64))
                .collect();
            if hit_ids.is_empty() {
                HashMap::new()
            } else {
                let search_store = SearchStore::new(state.db.pool().clone());
                let docs = if should_hydrate_thread_metadata {
                    search_store.build_thread_docs(&hit_ids).await
                } else {
                    search_store.build_patch_series_docs(&hit_ids).await
                }
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                docs.into_iter()
                    .filter_map(|doc| doc.get("id").and_then(Value::as_i64).map(|id| (id, doc)))
                    .collect()
            }
        } else {
            HashMap::new()
        };

    let mut items = Vec::with_capacity(hits.len());
    let mut highlights = BTreeMap::new();
    for hit in hits {
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
    let next_cursor = if !items.is_empty() && offset + items.len() < total_hits {
        Some(encode_search_cursor(offset + items.len(), &request_hash))
    } else {
        None
    };

    json_response_with_cache(
        &headers,
        &SearchResponse {
            items,
            facets,
            highlights,
            page_info: build_page_info(limit as i64, next_cursor),
        },
        CACHE_SEARCH,
        None,
    )
}
