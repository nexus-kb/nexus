use axum::middleware;
use axum::routing::{get, patch, post};
use axum::Router;

use crate::handlers::{
    authors, docs, emails, health, jobs, mailing_lists, stats, threads, webhooks,
};
use crate::middleware::{add_security_headers, request_tracing_layer};
use crate::state::ApiState;

/// Build the API router with all endpoints and middleware.
pub fn build_router(state: ApiState) -> Router {
    Router::new()
        .route("/hello", get(health::hello))
        .route("/health", get(health::health))
        .route("/jobs", post(jobs::enqueue_job).get(jobs::list_jobs))
        .route("/jobs/{id}", get(jobs::job_status))
        .route("/jobs/stats", get(jobs::job_stats))
        .route("/openapi.json", get(docs::openapi_spec))
        .route("/docs", get(docs::rapidoc))
        .route("/mailing-lists", get(mailing_lists::list_mailing_lists))
        .route(
            "/mailing-lists/enabled",
            get(mailing_lists::list_enabled_mailing_lists),
        )
        .route(
            "/mailing-lists/{slug}",
            patch(mailing_lists::update_mailing_list),
        )
        .route("/lists/stats", get(stats::aggregate_stats))
        .route("/lists/{slug}/stats", get(stats::list_stats))
        .route("/lists/{slug}/emails", get(emails::list_emails))
        .route("/lists/{slug}/emails/{email_id}", get(emails::get_email))
        .route("/lists/{slug}/threads", get(threads::list_threads))
        .route(
            "/lists/{slug}/threads/{thread_id}",
            get(threads::get_thread),
        )
        .route("/authors", get(authors::list_authors))
        .route("/authors/{author_id}", get(authors::get_author))
        .route(
            "/authors/{author_id}/lists/{slug}/emails",
            get(authors::get_author_emails),
        )
        .route(
            "/authors/{author_id}/lists/{slug}/threads-started",
            get(authors::get_author_threads_started),
        )
        .route(
            "/authors/{author_id}/lists/{slug}/threads-participated",
            get(authors::get_author_threads_participated),
        )
        .route("/webhooks/grokmirror", post(webhooks::grokmirror_webhook))
        .layer(middleware::from_fn(add_security_headers))
        .layer(request_tracing_layer())
        .with_state(state)
}
