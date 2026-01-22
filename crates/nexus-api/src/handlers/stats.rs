//! Aggregate statistics endpoints for mailing lists.

use axum::Json;
use axum::extract::{Path, State};

use crate::handlers::helpers::resolve_mailing_list_id;
use crate::http::{ApiError, internal_error};
use crate::models::{ListAggregateStats, MailingListStats};
use crate::response::{ApiResponse, ResponseMeta};
use crate::state::ApiState;

/// Aggregate counts across all mailing lists.
pub async fn aggregate_stats(
    State(state): State<ApiState>,
) -> Result<Json<ApiResponse<ListAggregateStats>>, ApiError> {
    let row: (i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
            (SELECT COUNT(*) FROM mailing_lists) AS total_lists,
            (SELECT COUNT(*) FROM emails) AS total_emails,
            (SELECT COUNT(*) FROM threads) AS total_threads,
            (SELECT COUNT(DISTINCT author_id) FROM emails) AS total_authors
        "#,
    )
    .fetch_one(state.db.pool())
    .await
    .map_err(internal_error)?;

    let stats = ListAggregateStats {
        total_lists: row.0,
        total_emails: row.1,
        total_threads: row.2,
        total_authors: row.3,
    };

    Ok(Json(ApiResponse::new(stats)))
}

/// Stats for a specific mailing list.
pub async fn list_stats(
    State(state): State<ApiState>,
    Path(slug): Path<String>,
) -> Result<Json<ApiResponse<MailingListStats>>, ApiError> {
    let mailing_list_id = resolve_mailing_list_id(state.db.pool(), &slug).await?;

    let stats = sqlx::query_as::<_, MailingListStats>(
        r#"
        SELECT
            CAST((SELECT COUNT(*) FROM emails WHERE mailing_list_id = $1) AS BIGINT) AS total_emails,
            CAST((SELECT COUNT(*) FROM threads WHERE mailing_list_id = $1) AS BIGINT) AS total_threads,
            CAST((SELECT COUNT(*) FROM author_mailing_list_activity WHERE mailing_list_id = $1) AS BIGINT) AS total_authors,
            (SELECT MIN(date) FROM emails WHERE mailing_list_id = $1) AS date_range_start,
            (SELECT MAX(date) FROM emails WHERE mailing_list_id = $1) AS date_range_end
        "#,
    )
    .bind(mailing_list_id)
    .fetch_one(state.db.pool())
    .await
    .map_err(internal_error)?;

    let meta = ResponseMeta::default().with_list_id(slug);
    Ok(Json(ApiResponse::with_meta(stats, meta)))
}
