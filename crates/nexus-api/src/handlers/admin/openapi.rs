use super::*;
use axum::response::Html;
use utoipa_rapidoc::RapiDoc;

pub async fn openapi_json() -> HandlerResult<Json<Value>> {
    let doc = serde_json::to_value(crate::api_docs::admin_openapi())
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(doc))
}

pub async fn openapi_docs() -> Html<String> {
    Html(RapiDoc::new("/admin/v1/openapi.json").to_html())
}
