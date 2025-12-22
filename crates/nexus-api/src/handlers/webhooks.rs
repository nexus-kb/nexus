use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use nexus_db::{JobStore, MailingListStore};
use serde::Serialize;
use tracing::info;

use crate::http::{ApiError, internal_error};
use crate::state::ApiState;

#[derive(Debug, Serialize)]
pub struct WebhookEnqueueResponse {
    pub queued: usize,
}

/// Trigger a sync for all enabled mailing lists when grokmirror finishes.
pub async fn grokmirror_webhook(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<WebhookEnqueueResponse>, ApiError> {
    if let Some(secret) = state.webhook_secret.as_deref() {
        let provided = headers
            .get("x-nexus-webhook-secret")
            .and_then(|value| value.to_str().ok());
        if provided != Some(secret) {
            return Err((
                StatusCode::UNAUTHORIZED,
                "invalid webhook secret".to_string(),
            ));
        }
    }

    if !body.is_empty() {
        info!(payload = %String::from_utf8_lossy(&body), "grokmirror webhook received");
    } else {
        info!("grokmirror webhook received");
    }

    let store = MailingListStore::new(state.db.pool().clone());
    let lists = store.list_all_enabled().await.map_err(internal_error)?;
    let jobs = JobStore::new(state.db.pool().clone());
    let mut queued = 0usize;

    for list in lists {
        let payload = serde_json::json!({
            "type": "sync_mailing_list",
            "slug": list.slug,
        });
        jobs.enqueue("default", payload, None, None)
            .await
            .map_err(internal_error)?;
        queued += 1;
    }

    Ok(Json(WebhookEnqueueResponse { queued }))
}
