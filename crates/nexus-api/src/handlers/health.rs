use axum::response::IntoResponse;

/// Simple hello endpoint for smoke tests.
pub async fn hello() -> &'static str {
    "Hello, Nexus!"
}

/// Healthcheck endpoint.
pub async fn health() -> impl IntoResponse {
    axum::http::StatusCode::OK
}
