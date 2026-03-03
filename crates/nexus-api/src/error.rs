use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

const MAX_ERROR_BODY_BYTES: usize = 16 * 1024;
const MAX_DETAIL_BYTES: usize = 4 * 1024;

/// Machine-readable validation issue for one request parameter.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct InvalidParam {
    pub name: String,
    pub reason: String,
}

/// RFC7807 Problem Details payload used by all non-2xx API responses.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ProblemDetails {
    /// URI reference identifying the problem type.
    pub r#type: String,
    /// Short, human-readable summary of the problem.
    pub title: String,
    /// HTTP status code generated for this problem.
    pub status: u16,
    /// Human-readable explanation specific to this occurrence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// URI reference identifying the specific request instance.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
    /// Stable machine-readable problem code.
    pub code: String,
    /// Optional list of field/query/path-specific validation issues.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub invalid_params: Vec<InvalidParam>,
}

impl ProblemDetails {
    fn from_status(status: StatusCode) -> Self {
        let (code, title) = default_problem_code_and_title(status);
        Self {
            r#type: format!("urn:nexus:problem:{code}"),
            title: title.to_string(),
            status: status.as_u16(),
            detail: None,
            instance: None,
            code: code.to_string(),
            invalid_params: Vec::new(),
        }
    }

    fn truncate_detail(detail: String) -> String {
        let mut bytes = detail.into_bytes();
        if bytes.len() > MAX_DETAIL_BYTES {
            bytes.truncate(MAX_DETAIL_BYTES);
        }
        String::from_utf8_lossy(&bytes).to_string()
    }
}

/// API error wrapper that renders RFC7807 problem details.
#[derive(Debug, Clone)]
pub struct ApiError {
    status: StatusCode,
    problem: Box<ProblemDetails>,
}

impl ApiError {
    pub fn from_status(status: StatusCode) -> Self {
        Self {
            status,
            problem: Box::new(ProblemDetails::from_status(status)),
        }
    }

    pub fn bad_request(detail: impl Into<String>) -> Self {
        Self::from_status(StatusCode::BAD_REQUEST).with_detail(detail)
    }

    pub fn unauthorized(detail: impl Into<String>) -> Self {
        Self::from_status(StatusCode::UNAUTHORIZED).with_detail(detail)
    }

    pub fn validation(detail: impl Into<String>) -> Self {
        Self::from_status(StatusCode::UNPROCESSABLE_ENTITY).with_detail(detail)
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self::from_status(StatusCode::INTERNAL_SERVER_ERROR).with_detail(detail)
    }

    pub fn upstream(detail: impl Into<String>) -> Self {
        Self::from_status(StatusCode::BAD_GATEWAY).with_detail(detail)
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.problem.detail = Some(ProblemDetails::truncate_detail(detail.into()));
        self
    }

    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.problem.instance = Some(instance.into());
        self
    }

    pub fn with_invalid_param(
        mut self,
        name: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        self.problem.invalid_params.push(InvalidParam {
            name: name.into(),
            reason: reason.into(),
        });
        self
    }
}

impl From<StatusCode> for ApiError {
    fn from(status: StatusCode) -> Self {
        Self::from_status(status)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = serde_json::to_vec(&self.problem).unwrap_or_default();
        let mut response = Response::builder()
            .status(self.status)
            .header(header::CONTENT_TYPE, "application/problem+json")
            .body(Body::from(body))
            .unwrap_or_else(|_| Response::new(Body::empty()));
        *response.status_mut() = self.status;
        response
    }
}

/// Middleware that normalizes non-RFC7807 errors (extractor/plain-status) into problem+json.
pub async fn problem_details_middleware(request: Request<Body>, next: Next) -> Response {
    let instance = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());

    let response = next.run(request).await;
    let status = response.status();
    if !(status.is_client_error() || status.is_server_error()) {
        return response;
    }
    if matches!(status, StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED) {
        return response;
    }

    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    if content_type.contains("application/problem+json") {
        return response;
    }

    let (parts, body) = response.into_parts();
    let body_bytes = to_bytes(body, MAX_ERROR_BODY_BYTES)
        .await
        .unwrap_or_default();
    let detail = extract_detail_for_problem(&content_type, &body_bytes);

    let mut problem = ApiError::from_status(status).with_instance(instance);
    if let Some(detail) = detail {
        problem = problem.with_detail(detail);
    }
    let mut normalized = problem.into_response();
    *normalized.status_mut() = status;

    for (header_name, header_value) in &parts.headers {
        if *header_name == header::CONTENT_TYPE || *header_name == header::CONTENT_LENGTH {
            continue;
        }
        normalized
            .headers_mut()
            .insert(header_name.clone(), header_value.clone());
    }
    normalized
}

fn extract_detail_for_problem(content_type: &str, body: &[u8]) -> Option<String> {
    if body.is_empty() {
        return None;
    }

    if content_type.contains("application/json")
        && let Ok(value) = serde_json::from_slice::<Value>(body)
    {
        if let Some(detail) = value.get("detail").and_then(Value::as_str) {
            return Some(detail.to_string());
        }
        if let Some(message) = value.get("message").and_then(Value::as_str) {
            return Some(message.to_string());
        }
        if let Some(error) = value.get("error").and_then(Value::as_str) {
            return Some(error.to_string());
        }
        if let Ok(compact) = serde_json::to_string(&value) {
            return Some(compact);
        }
    }

    let text = String::from_utf8_lossy(body).trim().to_string();
    if text.is_empty() { None } else { Some(text) }
}

fn default_problem_code_and_title(status: StatusCode) -> (&'static str, &'static str) {
    match status {
        StatusCode::BAD_REQUEST => ("bad_request", "Bad Request"),
        StatusCode::UNAUTHORIZED => ("unauthorized", "Unauthorized"),
        StatusCode::FORBIDDEN => ("forbidden", "Forbidden"),
        StatusCode::NOT_FOUND => ("not_found", "Not Found"),
        StatusCode::METHOD_NOT_ALLOWED => ("method_not_allowed", "Method Not Allowed"),
        StatusCode::CONFLICT => ("conflict", "Conflict"),
        StatusCode::UNPROCESSABLE_ENTITY => ("validation_failed", "Validation Failed"),
        StatusCode::TOO_MANY_REQUESTS => ("rate_limited", "Too Many Requests"),
        StatusCode::BAD_GATEWAY => ("upstream_error", "Upstream Error"),
        StatusCode::SERVICE_UNAVAILABLE => ("service_unavailable", "Service Unavailable"),
        StatusCode::GATEWAY_TIMEOUT => ("gateway_timeout", "Gateway Timeout"),
        StatusCode::INTERNAL_SERVER_ERROR => ("internal_error", "Internal Server Error"),
        _ if status.is_client_error() => ("client_error", "Client Error"),
        _ if status.is_server_error() => ("server_error", "Server Error"),
        _ => ("http_error", "HTTP Error"),
    }
}
