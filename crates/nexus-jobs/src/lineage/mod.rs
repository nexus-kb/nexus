use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use sha2::{Digest, Sha256};

use crate::diff_metadata::parse_diff_metadata;
use crate::patch_id::PatchIdMemo;
use crate::patch_subject::parse_patch_subject;
use nexus_db::{
    AssembledItemRecord, LineageSourceMessage, LineageStore, PatchItemFileBatchInput,
    UpsertPatchItemFileInput, UpsertPatchItemInput, UpsertPatchSeriesInput,
    UpsertPatchSeriesVersionInput,
};

#[derive(Debug, Clone, Default)]
pub struct PatchExtractOutcome {
    pub source_threads_scanned: u64,
    pub source_messages_read: u64,
    pub series_versions_written: u64,
    pub patch_items_written: u64,
    pub patch_item_ids: Vec<i64>,
    pub series_ids: Vec<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct PatchIdComputeOutcome {
    pub patch_items_updated: u64,
}

#[derive(Debug, Clone, Default)]
pub struct DiffParseOutcome {
    pub patch_items_updated: u64,
    pub patch_item_files_written: u64,
}

#[derive(Debug, Clone, Default)]
pub struct PatchEnrichmentOutcome {
    pub patch_items_hydrated: u64,
    pub patch_items_fallback_patch_id: u64,
    pub patch_items_fallback_diff_parse: u64,
    pub patch_item_files_written: u64,
}

#[derive(Debug, Clone)]
struct CandidateVersion {
    thread_id: i64,
    canonical_subject_norm: String,
    author_email: String,
    author_name: Option<String>,
    version_num: i32,
    is_rfc: bool,
    is_resend: bool,
    sent_at: DateTime<Utc>,
    subject_raw: String,
    subject_norm: String,
    cover_message_pk: Option<i64>,
    first_patch_message_pk: Option<i64>,
    base_commit: Option<String>,
    change_id: Option<String>,
    reference_message_ids: Vec<String>,
    items: Vec<CandidateItem>,
}

#[derive(Debug, Clone)]
struct CandidateItem {
    message_pk: i64,
    thread_order: usize,
    ordinal: Option<i32>,
    total: Option<i32>,
    subject_raw: String,
    subject_norm: String,
    commit_subject: Option<String>,
    commit_subject_norm: Option<String>,
    commit_author_name: Option<String>,
    commit_author_email: Option<String>,
    item_type: String,
    has_diff: bool,
    patch_id_stable: Option<String>,
}

#[derive(Debug, Clone)]
struct SimilarityChoice {
    series_id: i64,
    score: f64,
}

mod core;
mod similarity;
#[cfg(test)]
mod tests;

use similarity::*;

pub async fn process_patch_extract_window(
    store: &LineageStore,
    mailing_list_id: i64,
    anchor_message_pks: &[i64],
) -> anyhow::Result<PatchExtractOutcome> {
    core::process_patch_extract_window(store, mailing_list_id, anchor_message_pks).await
}

pub async fn process_patch_extract_threads(
    store: &LineageStore,
    mailing_list_id: i64,
    thread_ids: &[i64],
) -> anyhow::Result<PatchExtractOutcome> {
    core::process_patch_extract_threads(store, mailing_list_id, thread_ids).await
}

pub async fn process_patch_enrichment_batch(
    store: &LineageStore,
    patch_item_ids: &[i64],
) -> anyhow::Result<PatchEnrichmentOutcome> {
    core::process_patch_enrichment_batch(store, patch_item_ids).await
}
