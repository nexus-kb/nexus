use axum::Router;
use axum::middleware;
use axum::routing::{get, post};

use crate::auth::require_admin;
use crate::handlers::{admin, health};
use crate::state::ApiState;

pub fn build_router(state: ApiState) -> Router {
    let public_routes = Router::new()
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .route("/version", get(health::version));

    let admin_routes = Router::new()
        .route("/jobs/enqueue", post(admin::enqueue_job))
        .route("/jobs", get(admin::list_jobs))
        .route("/jobs/{job_id}", get(admin::get_job))
        .route("/jobs/{job_id}/cancel", post(admin::cancel_job))
        .route("/jobs/{job_id}/retry", post(admin::retry_job))
        .route("/ingest/sync", post(admin::ingest_sync))
        .route("/ingest/reset-watermark", post(admin::reset_watermark))
        .route("/threading/rebuild", post(admin::threading_rebuild))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin,
        ));

    Router::new()
        .nest("/api/v1", public_routes)
        .nest("/admin/v1", admin_routes)
        .with_state(state)
}
