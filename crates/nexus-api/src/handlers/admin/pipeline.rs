use super::*;

#[derive(Debug, Deserialize)]
pub struct PipelineRunsQuery {
    #[serde(default)]
    pub list_key: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ListPipelineRunsResponse {
    pub items: Vec<nexus_db::PipelineRun>,
    pub page_info: CursorPageInfoResponse,
}

pub async fn list_pipeline_runs(
    State(state): State<ApiState>,
    Query(query): Query<PipelineRunsQuery>,
) -> Result<Json<ListPipelineRunsResponse>, axum::http::StatusCode> {
    let limit = normalize_limit(query.limit, 100, 200);
    let cursor_hash = short_hash(&json!({
        "endpoint": "pipeline_runs",
        "list_key": query.list_key.as_deref().unwrap_or(""),
        "state": query.state.as_deref().unwrap_or(""),
    }));
    let cursor_id = if let Some(raw_cursor) = query.cursor.as_deref() {
        let token: IdCursorToken =
            decode_cursor_token(raw_cursor).ok_or(axum::http::StatusCode::UNPROCESSABLE_ENTITY)?;
        if token.v != 1 || token.h != cursor_hash {
            return Err(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
        }
        Some(token.id)
    } else {
        None
    };

    let mut runs = state
        .pipeline
        .list_runs(ListPipelineRunsParams {
            list_key: query.list_key.clone(),
            state: query.state.clone(),
            limit: limit + 1,
            cursor: cursor_id,
        })
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let limit_usize = limit_to_usize(limit);
    let has_more = runs.len() > limit_usize;
    if has_more {
        runs.truncate(limit_usize);
    }
    let next_cursor = if has_more {
        runs.last().and_then(|run| {
            encode_cursor_token(&IdCursorToken {
                v: 1,
                h: cursor_hash.clone(),
                id: run.id,
            })
        })
    } else {
        None
    };

    Ok(Json(ListPipelineRunsResponse {
        items: runs,
        page_info: build_page_info(limit, next_cursor),
    }))
}

pub async fn get_pipeline_run(
    State(state): State<ApiState>,
    AxumPath(run_id): AxumPath<i64>,
) -> Result<Json<nexus_db::PipelineRun>, axum::http::StatusCode> {
    let Some(run) = state
        .pipeline
        .get_run(run_id)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(axum::http::StatusCode::NOT_FOUND);
    };

    Ok(Json(run))
}
