use std::net::{IpAddr, SocketAddr};

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::header::{CONTENT_LENGTH, HeaderMap};
use axum::http::{Method, Request, StatusCode, Uri, Version};
use axum::middleware::Next;
use axum::response::Response;
use chrono::Utc;
use tracing::info;

const CLF_TIMESTAMP_FORMAT: &str = "%d/%b/%Y:%H:%M:%S %z";

pub async fn common_log_middleware(request: Request<Body>, next: Next) -> Response {
    let client_host = resolve_client_host(
        request.headers(),
        request
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|value| value.0),
    );
    let request_line = render_request_line(request.method(), request.uri(), request.version());
    let response = next.run(request).await;
    let response_bytes = render_response_bytes(response.headers());
    let timestamp = Utc::now().format(CLF_TIMESTAMP_FORMAT).to_string();
    let line = format_common_log_line(
        &client_host,
        &timestamp,
        &request_line,
        response.status(),
        &response_bytes,
    );
    info!(target: "access_clf", "{line}");
    response
}

fn resolve_client_host(headers: &HeaderMap, connect_info: Option<SocketAddr>) -> String {
    if let Some(host) = cloudflare_client_host(headers) {
        return host;
    }

    if let Some(host) = first_forwarded_for_value(headers) {
        return host;
    }

    if let Some(host) = header_single_value(headers, "x-real-ip") {
        return host;
    }

    connect_info
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn cloudflare_client_host(headers: &HeaderMap) -> Option<String> {
    let cf_connecting_ip = header_single_value(headers, "cf-connecting-ip");
    let cf_connecting_ipv6 = header_single_value(headers, "cf-connecting-ipv6");
    let true_client_ip = header_single_value(headers, "true-client-ip");

    if let Some(ip) = cf_connecting_ip {
        if is_cloudflare_pseudo_ipv4(&ip)
            && let Some(ipv6) = cf_connecting_ipv6.clone()
        {
            return Some(ipv6);
        }
        return Some(ip);
    }

    if let Some(ip) = true_client_ip {
        return Some(ip);
    }

    cf_connecting_ipv6
}

fn is_cloudflare_pseudo_ipv4(value: &str) -> bool {
    match value.parse::<IpAddr>() {
        Ok(IpAddr::V4(ipv4)) => ipv4.octets()[0] >= 240,
        _ => false,
    }
}

fn first_forwarded_for_value(headers: &HeaderMap) -> Option<String> {
    let raw = header_single_value(headers, "x-forwarded-for")?;
    let first = raw
        .split(',')
        .map(str::trim)
        .find(|segment| !segment.is_empty())?;
    Some(first.to_string())
}

fn header_single_value(headers: &HeaderMap, key: &str) -> Option<String> {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn render_request_line(method: &Method, uri: &Uri, version: Version) -> String {
    let path = uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/");
    let normalized_path = if path.is_empty() { "/" } else { path };
    format!(
        "{} {} {}",
        method.as_str(),
        normalized_path,
        render_http_version(version)
    )
}

fn render_http_version(version: Version) -> &'static str {
    match version {
        Version::HTTP_09 => "HTTP/0.9",
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_11 => "HTTP/1.1",
        Version::HTTP_2 => "HTTP/2.0",
        Version::HTTP_3 => "HTTP/3.0",
        _ => "HTTP/?",
    }
}

fn render_response_bytes(headers: &HeaderMap) -> String {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn format_common_log_line(
    client_host: &str,
    timestamp: &str,
    request_line: &str,
    status: StatusCode,
    bytes: &str,
) -> String {
    format!(
        "{client_host} - - [{timestamp}] \"{request_line}\" {} {bytes}",
        status.as_u16()
    )
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use axum::http::header::{CONTENT_LENGTH, HeaderMap, HeaderValue};
    use axum::http::{Method, StatusCode, Uri, Version};

    use super::{
        format_common_log_line, render_request_line, render_response_bytes, resolve_client_host,
    };

    #[test]
    fn request_line_renders_with_query_string() {
        let uri: Uri = "/api/v1/search?q=thread&scope=thread"
            .parse()
            .expect("valid uri");
        let line = render_request_line(&Method::GET, &uri, Version::HTTP_11);
        assert_eq!(line, "GET /api/v1/search?q=thread&scope=thread HTTP/1.1");
    }

    #[test]
    fn bytes_field_uses_dash_when_length_missing_or_zero() {
        let headers = HeaderMap::new();
        assert_eq!(render_response_bytes(&headers), "-");

        let mut zero_headers = HeaderMap::new();
        zero_headers.insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
        assert_eq!(render_response_bytes(&zero_headers), "-");
    }

    #[test]
    fn bytes_field_uses_content_length_when_present() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("321"));
        assert_eq!(render_response_bytes(&headers), "321");
    }

    #[test]
    fn client_host_precedence_uses_cloudflare_connecting_ip_first() {
        let mut headers = HeaderMap::new();
        headers.insert("cf-connecting-ip", HeaderValue::from_static("198.51.100.2"));
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("198.51.100.5, 203.0.113.9"),
        );
        headers.insert("x-real-ip", HeaderValue::from_static("203.0.113.99"));
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 3000);

        assert_eq!(resolve_client_host(&headers, Some(socket)), "198.51.100.2");
    }

    #[test]
    fn client_host_precedence_uses_true_client_ip_when_cf_connecting_missing() {
        let mut headers = HeaderMap::new();
        headers.insert("true-client-ip", HeaderValue::from_static("198.51.100.3"));
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("198.51.100.5, 203.0.113.9"),
        );
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 3000);

        assert_eq!(resolve_client_host(&headers, Some(socket)), "198.51.100.3");
    }

    #[test]
    fn client_host_precedence_uses_x_forwarded_for_when_cloudflare_headers_missing() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("198.51.100.5, 203.0.113.9"),
        );
        headers.insert("x-real-ip", HeaderValue::from_static("203.0.113.77"));
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 3000);

        assert_eq!(resolve_client_host(&headers, Some(socket)), "198.51.100.5");
    }

    #[test]
    fn client_host_precedence_prefers_cf_connecting_ipv6_for_pseudo_ipv4() {
        let mut headers = HeaderMap::new();
        headers.insert("cf-connecting-ip", HeaderValue::from_static("240.10.10.10"));
        headers.insert(
            "cf-connecting-ipv6",
            HeaderValue::from_static("2001:db8::1234"),
        );
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 3000);

        assert_eq!(
            resolve_client_host(&headers, Some(socket)),
            "2001:db8::1234"
        );
    }

    #[test]
    fn client_host_precedence_uses_x_real_ip_after_forwarded_for() {
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", HeaderValue::from_static("203.0.113.77"));
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 3000);

        assert_eq!(resolve_client_host(&headers, Some(socket)), "203.0.113.77");
    }

    #[test]
    fn client_host_precedence_uses_socket_when_headers_missing() {
        let headers = HeaderMap::new();
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 8, 0, 1)), 3000);

        assert_eq!(resolve_client_host(&headers, Some(socket)), "10.8.0.1");
    }

    #[test]
    fn client_host_precedence_falls_back_to_dash() {
        let headers = HeaderMap::new();
        assert_eq!(resolve_client_host(&headers, None), "-");
    }

    #[test]
    fn formats_common_log_line_exactly() {
        let line = format_common_log_line(
            "127.0.0.1",
            "15/Feb/2026:04:30:12 +0000",
            "GET /api/v1/healthz HTTP/1.1",
            StatusCode::OK,
            "11",
        );
        assert_eq!(
            line,
            "127.0.0.1 - - [15/Feb/2026:04:30:12 +0000] \"GET /api/v1/healthz HTTP/1.1\" 200 11"
        );
    }
}
