use axum::Router;
use axum::routing::{get, patch, post};

use crate::handlers::{health, jobs, mailing_lists, webhooks};
use crate::state::ApiState;

/// Build the API router with all endpoints mounted.
pub fn build_router(state: ApiState) -> Router {
    Router::new()
        .route("/hello", get(health::hello))
        .route("/health", get(health::health))
        .route("/jobs", post(jobs::enqueue_job).get(jobs::list_jobs))
        .route("/jobs/{id}", get(jobs::job_status))
        .route("/jobs/stats", get(jobs::job_stats))
        .route("/mailing-lists", get(mailing_lists::list_mailing_lists))
        .route(
            "/mailing-lists/enabled",
            get(mailing_lists::list_enabled_mailing_lists),
        )
        .route(
            "/mailing-lists/{slug}",
            patch(mailing_lists::update_mailing_list),
        )
        .route("/webhooks/grokmirror", post(webhooks::grokmirror_webhook))
        .with_state(state)
}
