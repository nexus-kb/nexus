use axum::middleware;
use axum::routing::{get, options, post};
use axum::{Router, http::StatusCode};

use crate::auth::require_admin;
use crate::handlers::{admin, health, public};
use crate::state::ApiState;

pub fn build_router(state: ApiState) -> Router {
    let public_routes = Router::new()
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .route("/version", get(health::version))
        .route("/openapi.json", get(public::openapi_json))
        .route("/lists", get(public::list_catalog))
        .route("/lists/{list_key}", get(public::list_detail))
        .route("/lists/{list_key}/stats", get(public::list_stats))
        .route("/lists/{list_key}/threads", get(public::list_threads))
        .route(
            "/lists/{list_key}/threads/{thread_id}",
            get(public::list_thread_detail),
        )
        .route(
            "/lists/{list_key}/threads/{thread_id}/messages",
            get(public::thread_messages),
        )
        .route("/messages/{message_id}", get(public::message_detail))
        .route("/messages/{message_id}/body", get(public::message_body))
        .route("/messages/{message_id}/raw", get(public::message_raw))
        .route("/r/{msgid}", get(public::message_id_redirect))
        .route("/series", get(public::series_list))
        .route("/series/{series_id}", get(public::series_detail))
        .route(
            "/series/{series_id}/versions/{series_version_id}",
            get(public::series_version),
        )
        .route("/series/{series_id}/compare", get(public::series_compare))
        .route("/patch-items/{patch_item_id}", get(public::patch_item))
        .route(
            "/patch-items/{patch_item_id}/files",
            get(public::patch_item_files),
        )
        .route(
            "/patch-items/{patch_item_id}/files/{path}/diff",
            get(public::patch_item_file_diff),
        )
        .route(
            "/patch-items/{patch_item_id}/diff",
            get(public::patch_item_diff),
        )
        .route(
            "/series/{series_id}/versions/{series_version_id}/export/mbox",
            get(public::series_version_export_mbox),
        )
        .route("/search", get(public::search))
        .route("/{*path}", options(preflight_options));

    let admin_routes = Router::new()
        .route("/jobs/enqueue", post(admin::enqueue_job))
        .route("/jobs", get(admin::list_jobs))
        .route("/jobs/{job_id}", get(admin::get_job))
        .route("/jobs/{job_id}/cancel", post(admin::cancel_job))
        .route("/jobs/{job_id}/retry", post(admin::retry_job))
        .route("/ingest/sync", post(admin::ingest_sync))
        .route("/ingest/reset-watermark", post(admin::reset_watermark))
        .route("/pipeline/runs", get(admin::list_pipeline_runs))
        .route("/pipeline/runs/{run_id}", get(admin::get_pipeline_run))
        .route("/threading/rebuild", post(admin::threading_rebuild))
        .route("/lineage/rebuild", post(admin::lineage_rebuild))
        .route(
            "/search/embeddings/backfill",
            post(admin::search_embeddings_backfill),
        )
        .route(
            "/search/embeddings/backfill/{run_id}",
            get(admin::get_search_embeddings_backfill),
        )
        .route_layer(middleware::from_fn_with_state(state.clone(), require_admin));

    Router::new()
        .nest("/api/v1", public_routes)
        .nest("/admin/v1", admin_routes)
        .with_state(state)
}

async fn preflight_options() -> StatusCode {
    StatusCode::NO_CONTENT
}
