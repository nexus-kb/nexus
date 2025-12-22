use axum::http::StatusCode;

/// Shared API error type for handlers.
pub type ApiError = (StatusCode, String);

/// Convert an internal error into a 500 response payload.
pub fn internal_error<E: std::fmt::Display>(err: E) -> ApiError {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
