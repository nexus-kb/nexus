//! Query parameter helpers for explorer list endpoints.

use schemars::JsonSchema;
use serde::Deserialize;

const MAX_PAGE_SIZE: i64 = 100;

fn default_page() -> i64 {
    1
}

fn default_page_size() -> i64 {
    25
}

fn default_email_page_size() -> i64 {
    50
}

fn default_thread_page_size() -> i64 {
    50
}

/// Pagination-only parameters for simple list endpoints.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PaginationParams {
    #[serde(default = "default_page")]
    page: i64,
    #[serde(default = "default_page_size", rename = "pageSize")]
    page_size: i64,
}

impl Default for PaginationParams {
    fn default() -> Self {
        Self {
            page: default_page(),
            page_size: default_page_size(),
        }
    }
}

impl PaginationParams {
    pub fn page(&self) -> i64 {
        self.page.max(1)
    }

    pub fn page_size(&self) -> i64 {
        self.page_size.clamp(1, MAX_PAGE_SIZE)
    }
}

/// Query parameters supported by the email list endpoint.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EmailListParams {
    #[serde(default = "default_page")]
    page: i64,
    #[serde(default = "default_email_page_size", rename = "pageSize")]
    page_size: i64,
    #[serde(default)]
    sort: Vec<String>,
}

impl Default for EmailListParams {
    fn default() -> Self {
        Self {
            page: default_page(),
            page_size: default_email_page_size(),
            sort: vec!["date:desc".to_string()],
        }
    }
}

impl EmailListParams {
    pub fn page(&self) -> i64 {
        self.page.max(1)
    }

    pub fn page_size(&self) -> i64 {
        self.page_size.clamp(1, MAX_PAGE_SIZE)
    }

    pub fn normalized_sort(&self) -> Vec<String> {
        if self.sort.is_empty() {
            vec!["date:desc".to_string()]
        } else {
            self.sort.clone()
        }
    }
}

/// Query parameters supported by the thread list endpoint.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadListParams {
    #[serde(default = "default_page")]
    page: i64,
    #[serde(default = "default_thread_page_size", rename = "pageSize")]
    page_size: i64,
    #[serde(default)]
    sort: Vec<String>,
}

impl Default for ThreadListParams {
    fn default() -> Self {
        Self {
            page: default_page(),
            page_size: default_thread_page_size(),
            sort: vec!["lastActivity:desc".to_string()],
        }
    }
}

impl ThreadListParams {
    pub fn page(&self) -> i64 {
        self.page.max(1)
    }

    pub fn page_size(&self) -> i64 {
        self.page_size.clamp(1, MAX_PAGE_SIZE)
    }

    pub fn sort(&self) -> Vec<String> {
        if self.sort.is_empty() {
            vec!["lastActivity:desc".to_string()]
        } else {
            self.sort.clone()
        }
    }
}

/// Query parameters supported by the author list endpoint.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuthorListParams {
    #[serde(default = "default_page")]
    page: i64,
    #[serde(default = "default_page_size", rename = "pageSize")]
    page_size: i64,
    #[serde(default)]
    sort: Vec<String>,
    #[serde(default)]
    q: Option<String>,
    #[serde(default, rename = "listSlug")]
    list_slug: Option<String>,
}

impl Default for AuthorListParams {
    fn default() -> Self {
        Self {
            page: default_page(),
            page_size: default_page_size(),
            sort: vec!["lastSeen:desc".to_string()],
            q: None,
            list_slug: None,
        }
    }
}

impl AuthorListParams {
    pub fn page(&self) -> i64 {
        self.page.max(1)
    }

    pub fn page_size(&self) -> i64 {
        self.page_size.clamp(1, MAX_PAGE_SIZE)
    }

    pub fn sort(&self) -> Vec<String> {
        if self.sort.is_empty() {
            vec!["lastSeen:desc".to_string()]
        } else {
            self.sort.clone()
        }
    }

    pub fn normalized_query(&self) -> Option<String> {
        self.q
            .as_ref()
            .map(|q| q.trim())
            .filter(|q| !q.is_empty())
            .map(|q| q.to_lowercase())
    }

    pub fn raw_query(&self) -> Option<String> {
        self.q
            .as_ref()
            .map(|q| q.trim())
            .filter(|q| !q.is_empty())
            .map(|q| q.to_string())
    }

    pub fn list_slug(&self) -> Option<String> {
        self.list_slug
            .as_ref()
            .map(|slug| slug.trim())
            .filter(|slug| !slug.is_empty())
            .map(|slug| slug.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn email_list_params_defaults() {
        let params: EmailListParams = serde_json::from_str("{}").expect("valid defaults");
        assert_eq!(params.page(), 1);
        assert_eq!(params.page_size(), 50);
        assert_eq!(params.normalized_sort(), vec!["date:desc".to_string()]);
    }

    #[test]
    fn thread_list_params_defaults() {
        let params: ThreadListParams = serde_json::from_str("{}").expect("valid defaults");
        assert_eq!(params.page(), 1);
        assert_eq!(params.page_size(), 50);
        assert_eq!(params.sort(), vec!["lastActivity:desc".to_string()]);
    }

    #[test]
    fn author_list_params_defaults() {
        let params: AuthorListParams = serde_json::from_str("{}").expect("valid defaults");
        assert_eq!(params.page(), 1);
        assert_eq!(params.page_size(), 25);
        assert_eq!(params.sort(), vec!["lastSeen:desc".to_string()]);
        assert!(params.normalized_query().is_none());
        assert!(params.list_slug().is_none());
    }
}
