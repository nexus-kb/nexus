//! Production-safe error handling with logging and sanitized responses.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use tracing::error;
use uuid::Uuid;

/// Structured error response returned to clients.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    /// Human-readable error message (sanitized for production).
    pub message: String,
    /// Error code for programmatic handling.
    pub code: String,
    /// Unique identifier for this error instance (for log correlation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Application error type that implements IntoResponse.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: String,
    public_message: String,
    internal_message: Option<String>,
}

impl ApiError {
    /// Create a new API error.
    pub fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            public_message: message.into(),
            internal_message: None,
        }
    }

    /// Attach internal details for logging (not exposed to client).
    pub fn with_internal(mut self, msg: impl Into<String>) -> Self {
        self.internal_message = Some(msg.into());
        self
    }

    /// Create an internal server error with sanitized public message.
    pub fn internal<E: std::fmt::Display>(err: E) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "An internal error occurred",
        )
        .with_internal(err.to_string())
    }

    /// Create a not found error.
    pub fn not_found(resource: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            format!("{} not found", resource),
        )
    }

    /// Create an unauthorized error.
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "UNAUTHORIZED", message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let request_id = Uuid::new_v4().to_string();

        // Log the full error with internal details
        if let Some(ref internal) = self.internal_message {
            error!(
                request_id = %request_id,
                status = %self.status,
                code = %self.code,
                internal_error = %internal,
                "API error"
            );
        } else {
            error!(
                request_id = %request_id,
                status = %self.status,
                code = %self.code,
                "API error"
            );
        }

        let body = ErrorResponse {
            error: ErrorBody {
                message: self.public_message,
                code: self.code,
                request_id: Some(request_id),
            },
        };

        (self.status, Json(body)).into_response()
    }
}
