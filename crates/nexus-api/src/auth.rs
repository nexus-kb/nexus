use axum::extract::State;
use axum::http::{HeaderMap, Request};
use axum::middleware::Next;

use crate::error::ApiError;
use crate::state::ApiState;

pub async fn require_admin(
    State(state): State<ApiState>,
    headers: HeaderMap,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<axum::response::Response, ApiError> {
    let provided = headers
        .get("x-nexus-admin-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();

    if provided != state.settings.admin.token {
        return Err(ApiError::unauthorized(
            "missing or invalid x-nexus-admin-token header",
        ));
    }

    Ok(next.run(request).await)
}
