//! Response payload types for explorer endpoints.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Type of patch region detected in the email body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PatchRegionType {
    /// Standard unified diff content
    Diff,
    /// Diffstat summary block
    DiffStat,
    /// Git binary patch content
    BinaryPatch,
}

/// A contiguous region of patch content within an email body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PatchRegion {
    /// 1-indexed start line number (inclusive)
    pub start_line: u32,
    /// 1-indexed end line number (inclusive)
    pub end_line: u32,
    /// Type of patch content in this region
    #[serde(rename = "type")]
    pub region_type: PatchRegionType,
}

/// Metadata about patches detected in an email body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PatchMetadata {
    /// Whether the email contains any diff content
    pub has_patch: bool,
    /// Whether the email contains a diffstat summary
    pub has_diffstat: bool,
    /// Contiguous regions of patch content
    pub regions: Vec<PatchRegion>,
    /// Files modified by the patch (extracted from diff headers)
    pub files: Vec<String>,
}

/// Thread metadata stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sqlx::FromRow)]
pub struct Thread {
    pub id: i32,
    pub mailing_list_id: i32,
    pub root_message_id: String,
    pub subject: String,
    pub start_date: DateTime<Utc>,
    pub last_date: DateTime<Utc>,
    pub message_count: Option<i32>,
}

/// Thread metadata augmented with starter author details.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sqlx::FromRow)]
pub struct ThreadWithStarter {
    pub id: i32,
    pub mailing_list_id: i32,
    pub root_message_id: String,
    pub subject: String,
    pub start_date: DateTime<Utc>,
    pub last_date: DateTime<Utc>,
    pub message_count: Option<i32>,
    pub starter_id: i32,
    pub starter_name: Option<String>,
    pub starter_email: String,
}

/// Email row enriched with author metadata for API responses.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct EmailWithAuthor {
    pub id: i32,
    #[serde(rename = "mailingListId")]
    pub mailing_list_id: i32,
    #[serde(rename = "messageId")]
    pub message_id: String,
    #[serde(rename = "blobOid")]
    pub blob_oid: String,
    #[serde(rename = "authorId")]
    pub author_id: i32,
    pub subject: String,
    pub date: DateTime<Utc>,
    #[serde(rename = "inReplyTo")]
    pub in_reply_to: Option<String>,
    pub body: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(rename = "authorName")]
    pub author_name: Option<String>,
    #[serde(rename = "authorEmail")]
    pub author_email: String,
    /// Metadata about patches detected in the email body
    #[serde(rename = "patchMetadata")]
    #[schemars(with = "Option<PatchMetadata>")]
    pub patch_metadata: Option<sqlx::types::Json<PatchMetadata>>,
}

/// Email node enriched with depth information for thread rendering.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct EmailHierarchy {
    pub id: i32,
    #[serde(rename = "mailingListId")]
    pub mailing_list_id: i32,
    #[serde(rename = "messageId")]
    pub message_id: String,
    #[serde(rename = "blobOid")]
    pub blob_oid: String,
    #[serde(rename = "authorId")]
    pub author_id: i32,
    pub subject: String,
    pub date: DateTime<Utc>,
    #[serde(rename = "inReplyTo")]
    pub in_reply_to: Option<String>,
    pub body: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(rename = "authorName")]
    pub author_name: Option<String>,
    #[serde(rename = "authorEmail")]
    pub author_email: String,
    /// Metadata about patches detected in the email body
    #[serde(rename = "patchMetadata")]
    #[schemars(with = "Option<PatchMetadata>")]
    pub patch_metadata: Option<sqlx::types::Json<PatchMetadata>>,
    pub depth: i32,
}

/// Thread details including the threaded list of emails.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ThreadDetail {
    pub thread: Thread,
    pub emails: Vec<EmailHierarchy>,
}

/// Aggregated author statistics used in list and detail endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sqlx::FromRow)]
pub struct AuthorWithStats {
    pub id: i32,
    pub email: String,
    pub canonical_name: Option<String>,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
    pub email_count: i64,
    pub thread_count: i64,
    pub first_email_date: Option<DateTime<Utc>>,
    pub last_email_date: Option<DateTime<Utc>>,
    pub mailing_lists: Vec<String>,
    pub name_variations: Vec<String>,
}

/// Summary statistics for a single mailing list.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sqlx::FromRow)]
pub struct MailingListStats {
    #[serde(rename = "emailCount")]
    pub total_emails: i64,
    #[serde(rename = "threadCount")]
    pub total_threads: i64,
    #[serde(rename = "authorCount")]
    pub total_authors: i64,
    #[serde(rename = "dateRangeStart")]
    pub date_range_start: Option<DateTime<Utc>>,
    #[serde(rename = "dateRangeEnd")]
    pub date_range_end: Option<DateTime<Utc>>,
}

/// Aggregate mailing list statistics across the deployment.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ListAggregateStats {
    #[serde(rename = "totalLists")]
    pub total_lists: i64,
    #[serde(rename = "totalEmails")]
    pub total_emails: i64,
    #[serde(rename = "totalThreads")]
    pub total_threads: i64,
    #[serde(rename = "totalAuthors")]
    pub total_authors: i64,
}
