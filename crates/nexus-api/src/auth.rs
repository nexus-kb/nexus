use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::middleware::Next;

use crate::state::ApiState;

pub async fn require_admin(
    State(state): State<ApiState>,
    headers: HeaderMap,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<axum::response::Response, StatusCode> {
    let provided = headers
        .get("x-nexus-admin-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();

    if provided != state.settings.admin.token {
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(request).await)
}
