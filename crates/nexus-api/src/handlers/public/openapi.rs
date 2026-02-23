use super::*;

pub async fn openapi_json(headers: HeaderMap) -> Result<Response, StatusCode> {
    let doc = json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Nexus KB API",
            "version": "phase0"
        },
        "paths": {
            "/api/v1/healthz": { "get": { "summary": "Health probe" } },
            "/api/v1/readyz": { "get": { "summary": "Readiness probe" } },
            "/api/v1/version": { "get": { "summary": "Build metadata" } },
            "/api/v1/openapi.json": { "get": { "summary": "OpenAPI contract" } },
            "/api/v1/lists": { "get": { "summary": "List catalog" } },
            "/api/v1/lists/{list_key}": { "get": { "summary": "List detail" } },
            "/api/v1/lists/{list_key}/stats": { "get": { "summary": "List stats window" } },
            "/api/v1/lists/{list_key}/threads": { "get": { "summary": "List threads for list" } },
            "/api/v1/lists/{list_key}/threads/{thread_id}": { "get": { "summary": "Thread detail" } },
            "/api/v1/lists/{list_key}/threads/{thread_id}/messages": { "get": { "summary": "Thread messages (full|snippets)" } },
            "/api/v1/messages/{message_id}": { "get": { "summary": "Message metadata" } },
            "/api/v1/messages/{message_id}/body": { "get": { "summary": "Message body payload" } },
            "/api/v1/r/{msgid}": { "get": { "summary": "Message-ID redirector" } },
            "/api/v1/series": { "get": { "summary": "Series list" } },
            "/api/v1/series/{series_id}": { "get": { "summary": "Series detail timeline" } },
            "/api/v1/series/{series_id}/versions/{series_version_id}": { "get": { "summary": "Series version detail" } },
            "/api/v1/series/{series_id}/compare": { "get": { "summary": "Compare series versions" } },
            "/api/v1/search": { "get": { "summary": "Search across threads/series" } },
            "/api/v1/patch-items/{patch_item_id}": { "get": { "summary": "Patch item metadata" } },
            "/api/v1/patch-items/{patch_item_id}/files": { "get": { "summary": "Patch item files metadata" } },
            "/api/v1/patch-items/{patch_item_id}/files/{path}/diff": { "get": { "summary": "Patch file diff slice" } },
            "/api/v1/patch-items/{patch_item_id}/diff": { "get": { "summary": "Patch full diff text" } }
        }
    });

    json_response_with_cache(&headers, &doc, CACHE_THREAD, None)
}
