use axum::Json;
use axum::extract::{Path, Query, State};
use nexus_db::{MailingList, MailingListStore};
use serde::Deserialize;

use crate::handlers::default_list_limit;
use crate::http::{ApiError, internal_error};
use crate::state::ApiState;

#[derive(Debug, Deserialize)]
pub struct MailingListParams {
    pub enabled: Option<bool>,
    #[serde(default = "default_list_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

#[derive(Debug, Deserialize)]
pub struct PagingParams {
    #[serde(default = "default_list_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

#[derive(Debug, Deserialize)]
pub struct UpdateMailingListRequest {
    pub enabled: bool,
}

/// List mailing lists with optional filtering.
pub async fn list_mailing_lists(
    State(state): State<ApiState>,
    Query(params): Query<MailingListParams>,
) -> Result<Json<Vec<MailingList>>, ApiError> {
    let limit = params.limit.clamp(1, 500);
    let offset = params.offset.max(0);
    let store = MailingListStore::new(state.db.pool().clone());
    let lists = store
        .list(params.enabled, limit, offset)
        .await
        .map_err(internal_error)?;
    Ok(Json(lists))
}

/// List enabled mailing lists.
pub async fn list_enabled_mailing_lists(
    State(state): State<ApiState>,
    Query(params): Query<PagingParams>,
) -> Result<Json<Vec<MailingList>>, ApiError> {
    let limit = params.limit.clamp(1, 500);
    let offset = params.offset.max(0);
    let store = MailingListStore::new(state.db.pool().clone());
    let lists = store
        .list(Some(true), limit, offset)
        .await
        .map_err(internal_error)?;
    Ok(Json(lists))
}

/// Enable/disable a mailing list by slug.
pub async fn update_mailing_list(
    State(state): State<ApiState>,
    Path(slug): Path<String>,
    Json(body): Json<UpdateMailingListRequest>,
) -> Result<Json<MailingList>, ApiError> {
    let store = MailingListStore::new(state.db.pool().clone());
    match store
        .set_enabled(&slug, body.enabled)
        .await
        .map_err(internal_error)?
    {
        Some(list) => Ok(Json(list)),
        None => Err((
            axum::http::StatusCode::NOT_FOUND,
            format!("mailing list '{}' not found", slug),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mailing_list_params_defaults() {
        let params: MailingListParams = serde_json::from_str("{}").expect("valid defaults");
        assert_eq!(params.enabled, None);
        assert_eq!(params.limit, 100);
        assert_eq!(params.offset, 0);
    }

    #[test]
    fn paging_params_defaults() {
        let params: PagingParams = serde_json::from_str("{}").expect("valid defaults");
        assert_eq!(params.limit, 100);
        assert_eq!(params.offset, 0);
    }
}
