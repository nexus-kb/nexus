use std::collections::{BTreeMap, BTreeSet, HashMap};

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use nexus_core::search::SearchScope;
use nexus_db::{ListThreadsParams, PatchItemDetailRecord, SearchStore, SeriesLogicalCompareRow};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::error::ApiError;
use crate::state::ApiState;

mod helpers_and_tests;
mod lists;
mod messages;
mod openapi;
mod patch_items;
mod search;
mod series;
mod threads;
pub(crate) mod types;

pub use lists::{list_catalog, list_detail, list_stats};
pub use messages::{message_body, message_detail, message_id_redirect};
pub use openapi::{openapi_docs, openapi_json};
pub use patch_items::{patch_item, patch_item_diff, patch_item_file_diff, patch_item_files};
pub use search::search;
pub use series::{series_compare, series_detail, series_list, series_version};
pub use threads::{list_thread_detail, list_threads, thread_messages};

use helpers_and_tests::*;
use types::*;

type HandlerResult<T> = Result<T, ApiError>;

const CACHE_THREAD: &str = "public, max-age=300, stale-while-revalidate=86400";
const CACHE_LONG: &str = "public, max-age=86400, stale-while-revalidate=604800";
const CACHE_SEARCH: &str = "public, max-age=30, stale-while-revalidate=300";
