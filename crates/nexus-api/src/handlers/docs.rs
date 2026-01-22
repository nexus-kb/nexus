//! OpenAPI spec and documentation UI endpoints.

use axum::Json;
use axum::response::Html;

use crate::openapi::build_openapi;

const RAPIDOC_HTML: &str = r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Nexus API Docs</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      html, body { height: 100%; margin: 0; }
      rapi-doc { height: 100%; }
    </style>
  </head>
  <body>
    <rapi-doc spec-url="/openapi.json" show-header="false" theme="light"></rapi-doc>
    <script src="https://unpkg.com/rapidoc/dist/rapidoc-min.js"></script>
  </body>
</html>
"#;

/// Serve the OpenAPI specification as JSON.
pub async fn openapi_spec() -> Json<okapi::openapi3::OpenApi> {
    Json(build_openapi())
}

/// Serve the RapiDoc UI page.
pub async fn rapidoc() -> Html<&'static str> {
    Html(RAPIDOC_HTML)
}
