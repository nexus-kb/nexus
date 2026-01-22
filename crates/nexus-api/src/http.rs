//! HTTP utilities for API handlers.

pub use crate::error::ApiError;

/// Convert an internal error into an ApiError.
/// Logs the full error but returns a sanitized message to the client.
pub fn internal_error<E: std::fmt::Display>(err: E) -> ApiError {
    ApiError::internal(err)
}
