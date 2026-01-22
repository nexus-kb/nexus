//! Standard response envelope and metadata for explorer endpoints.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};

/// Sort direction for list responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Sort descriptor returned in response metadata.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SortDescriptor {
    pub field: String,
    pub direction: SortDirection,
}

/// Pagination metadata attached to list responses.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PaginationMeta {
    pub page: i64,
    #[serde(rename = "pageSize")]
    pub page_size: i64,
    #[serde(rename = "totalPages")]
    pub total_pages: i64,
    #[serde(rename = "totalItems")]
    pub total_items: i64,
}

impl PaginationMeta {
    pub fn new(page: i64, page_size: i64, total_items: i64) -> Self {
        let total_pages = if page_size > 0 {
            (total_items + page_size - 1) / page_size
        } else {
            0
        };

        Self {
            page,
            page_size,
            total_pages,
            total_items,
        }
    }
}

/// Metadata wrapper attached to explorer responses.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub struct ResponseMeta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationMeta>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort: Vec<SortDescriptor>,
    #[serde(rename = "listId", skip_serializing_if = "Option::is_none")]
    pub list_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<JsonMap<String, JsonValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<JsonMap<String, JsonValue>>,
}

impl ResponseMeta {
    pub fn with_pagination(mut self, pagination: PaginationMeta) -> Self {
        self.pagination = Some(pagination);
        self
    }

    pub fn with_sort(mut self, sort: Vec<SortDescriptor>) -> Self {
        self.sort = sort;
        self
    }

    pub fn with_list_id(mut self, list_id: impl Into<String>) -> Self {
        self.list_id = Some(list_id.into());
        self
    }

    pub fn with_filters(mut self, filters: JsonMap<String, JsonValue>) -> Self {
        self.filters = Some(filters);
        self
    }
}

/// Standard response envelope used by explorer endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(bound = "T: JsonSchema")]
pub struct ApiResponse<T> {
    pub data: T,
    #[serde(default)]
    pub meta: ResponseMeta,
}

impl<T> ApiResponse<T> {
    pub fn new(data: T) -> Self {
        Self::with_meta(data, ResponseMeta::default())
    }

    pub fn with_meta(data: T, meta: ResponseMeta) -> Self {
        Self { data, meta }
    }
}
