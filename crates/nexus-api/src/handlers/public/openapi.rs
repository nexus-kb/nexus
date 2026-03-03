use super::*;
use axum::response::Html;
use utoipa_rapidoc::RapiDoc;

pub async fn openapi_json(headers: HeaderMap) -> HandlerResult<Response> {
    let doc = serde_json::to_value(crate::api_docs::public_openapi())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    json_response_with_cache(&headers, &doc, CACHE_THREAD, None)
}

pub async fn openapi_docs() -> Html<String> {
    Html(RapiDoc::new("/api/v1/openapi.json").to_html())
}
