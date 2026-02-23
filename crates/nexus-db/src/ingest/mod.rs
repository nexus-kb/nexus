use chrono::{DateTime, Utc};
use csv::{QuoteStyle, Terminator, WriterBuilder};
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

use crate::{MailingListRepo, Result};

const COPY_NULL_SENTINEL: &str = "\u{001f}NEXUS_NULL\u{001f}";
const MAX_QUERY_BIND_PARAMS: usize = 60_000;
const UTF8_INTEGRITY_SAMPLE_LIMIT: usize = 4_000;
const UTF8_INTEGRITY_OFFENDER_LIMIT: usize = 5;

#[derive(Debug, Clone)]
pub struct ParsedBodyInput {
    pub raw_rfc822: Vec<u8>,
    pub body_text: Option<String>,
    pub diff_text: Option<String>,
    pub search_text: String,
    pub has_diff: bool,
    pub has_attachments: bool,
}

#[derive(Debug, Clone)]
pub struct ParsedPatchFileFactInput {
    pub old_path: Option<String>,
    pub new_path: String,
    pub change_type: String,
    pub is_binary: bool,
    pub additions: i32,
    pub deletions: i32,
    pub hunk_count: i32,
    pub diff_start: i32,
    pub diff_end: i32,
}

#[derive(Debug, Clone)]
pub struct ParsedPatchFactsInput {
    pub has_diff: bool,
    pub patch_id_stable: Option<String>,
    pub base_commit: Option<String>,
    pub change_id: Option<String>,
    pub file_count: i32,
    pub additions: i32,
    pub deletions: i32,
    pub hunk_count: i32,
    pub files: Vec<ParsedPatchFileFactInput>,
}

#[derive(Debug, Clone)]
pub struct ParsedMessageInput {
    pub content_hash_sha256: Vec<u8>,
    pub subject_raw: String,
    pub subject_norm: String,
    pub from_name: Option<String>,
    pub from_email: String,
    pub date_utc: Option<DateTime<Utc>>,
    pub to_raw: Option<String>,
    pub cc_raw: Option<String>,
    pub message_ids: Vec<String>,
    pub message_id_primary: String,
    pub in_reply_to_ids: Vec<String>,
    pub references_ids: Vec<String>,
    pub mime_type: Option<String>,
    pub body: ParsedBodyInput,
    pub patch_facts: Option<ParsedPatchFactsInput>,
}

#[derive(Debug, Clone)]
pub struct WriteOutcome {
    pub message_inserted: bool,
    pub instance_inserted: bool,
    pub message_pk: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct IngestCommitRow {
    pub git_commit_oid: String,
    pub parsed_message: ParsedMessageInput,
}

#[derive(Debug, Clone, Default)]
pub struct BatchWriteOutcome {
    pub inserted_instances: u64,
    pub message_pks: Vec<i64>,
    pub skipped_commits: Vec<String>,
}

#[derive(Clone)]
pub struct IngestStore {
    pool: PgPool,
}

impl IngestStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(Debug, Clone)]
pub(super) struct CopyMessageBodyRow {
    pub(super) id: i64,
    pub(super) body_text: Option<String>,
    pub(super) diff_text: Option<String>,
    pub(super) search_text: String,
    pub(super) has_diff: bool,
    pub(super) has_attachments: bool,
}

#[derive(Debug, Clone)]
pub(super) struct CopyMessageRow {
    pub(super) id: i64,
    pub(super) content_hash_sha256: Vec<u8>,
    pub(super) subject_raw: String,
    pub(super) subject_norm: String,
    pub(super) from_name: Option<String>,
    pub(super) from_email: String,
    pub(super) date_utc: Option<DateTime<Utc>>,
    pub(super) to_raw: Option<String>,
    pub(super) cc_raw: Option<String>,
    pub(super) message_ids: Vec<String>,
    pub(super) message_id_primary: String,
    pub(super) in_reply_to_ids: Vec<String>,
    pub(super) references_ids: Vec<String>,
    pub(super) mime_type: Option<String>,
    pub(super) body_id: i64,
}

#[derive(Debug, Clone)]
pub(super) struct CopyMessageIdRow {
    pub(super) message_id: String,
    pub(super) message_pk: i64,
    pub(super) is_primary: bool,
}

#[derive(Debug, Clone)]
pub(super) struct CopyInstanceRow {
    pub(super) mailing_list_id: i64,
    pub(super) message_pk: i64,
    pub(super) repo_id: i64,
    pub(super) git_commit_oid: String,
}

#[derive(Debug, sqlx::FromRow)]
pub(super) struct ExistingMessageIdRow {
    pub(super) message_id: String,
    pub(super) message_pk: i64,
    pub(super) is_primary: bool,
}

mod batch_copy;
mod helpers;
mod message;
#[cfg(test)]
mod tests;

use helpers::*;
