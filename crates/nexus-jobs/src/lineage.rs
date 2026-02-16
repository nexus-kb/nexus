use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use once_cell::sync::Lazy;
use regex::Regex;
use sha2::{Digest, Sha256};

use crate::diff_metadata::parse_diff_metadata;
use crate::patch_detect::extract_diff_text;
use crate::patch_id::PatchIdMemo;
use crate::patch_subject::parse_patch_subject;
use nexus_db::{
    AssembledItemRecord, LineageSourceMessage, LineageStore, PatchItemFileBatchInput,
    UpsertPatchItemFileInput, UpsertPatchItemInput, UpsertPatchSeriesInput,
    UpsertPatchSeriesVersionInput,
};

static CHANGE_ID_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?im)^change-id:\s*(\S+)").expect("valid change-id regex"));
static BASE_COMMIT_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?im)^base-commit:\s*(\S+)").expect("valid base-commit regex"));

#[derive(Debug, Clone, Default)]
pub struct PatchExtractOutcome {
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

pub async fn process_patch_extract_window(
    store: &LineageStore,
    mailing_list_id: i64,
    anchor_message_pks: &[i64],
) -> anyhow::Result<PatchExtractOutcome> {
    let source_messages = store
        .load_messages_for_anchors(mailing_list_id, anchor_message_pks)
        .await
        .context("load lineage source messages")?;

    if source_messages.is_empty() {
        return Ok(PatchExtractOutcome::default());
    }

    let mut memo = PatchIdMemo::default();
    let mut candidates = build_candidates(source_messages, &mut memo);

    if candidates.is_empty() {
        return Ok(PatchExtractOutcome::default());
    }

    candidates.sort_by_key(|v| (v.sent_at, v.thread_id, v.version_num));

    let mut output = PatchExtractOutcome::default();
    let mut seen_series_ids = BTreeSet::new();

    for candidate in candidates {
        if candidate.items.iter().all(|item| item.item_type != "patch") {
            continue;
        }

        let resolved_series_id = resolve_series_id(store, mailing_list_id, &candidate)
            .await
            .context("resolve series id")?;

        let series_id = if resolved_series_id != 0 {
            resolved_series_id
        } else {
            let fallback_candidates = store
                .find_similarity_candidates(
                    &candidate.canonical_subject_norm,
                    candidate.sent_at - Duration::days(3650),
                    candidate.sent_at + Duration::days(3650),
                )
                .await
                .context("load fallback series candidates")?;
            let fallback_existing = fallback_candidates
                .iter()
                .find(|row| row.author_email == candidate.author_email)
                .map(|row| row.id);

            if let Some(existing_id) = fallback_existing {
                existing_id
            } else {
                store
                    .upsert_series(&UpsertPatchSeriesInput {
                        canonical_subject_norm: candidate.canonical_subject_norm.clone(),
                        author_email: candidate.author_email.clone(),
                        author_name: candidate.author_name.clone(),
                        change_id: candidate.change_id.clone(),
                        created_at: candidate.sent_at,
                        last_seen_at: candidate.sent_at,
                    })
                    .await
                    .context("upsert patch_series")?
                    .id
            }
        };

        store
            .upsert_series_list_presence(series_id, mailing_list_id, candidate.sent_at)
            .await
            .context("upsert patch_series_lists")?;

        let version_fingerprint = compute_version_fingerprint(&candidate.items);
        let version = store
            .upsert_series_version(&UpsertPatchSeriesVersionInput {
                patch_series_id: series_id,
                version_num: candidate.version_num,
                is_rfc: candidate.is_rfc,
                is_resend: candidate.is_resend,
                thread_id: Some(candidate.thread_id),
                cover_message_pk: candidate.cover_message_pk,
                first_patch_message_pk: candidate.first_patch_message_pk,
                sent_at: candidate.sent_at,
                subject_raw: candidate.subject_raw.clone(),
                subject_norm: candidate.subject_norm.clone(),
                base_commit: candidate.base_commit.clone(),
                version_fingerprint,
            })
            .await
            .context("upsert patch_series_versions")?;

        let upsert_inputs = candidate
            .items
            .iter()
            .map(|item| UpsertPatchItemInput {
                ordinal: item.ordinal.unwrap_or(0),
                total: item.total,
                message_pk: item.message_pk,
                subject_raw: item.subject_raw.clone(),
                subject_norm: item.subject_norm.clone(),
                commit_subject: item.commit_subject.clone(),
                commit_subject_norm: item.commit_subject_norm.clone(),
                commit_author_name: item.commit_author_name.clone(),
                commit_author_email: item.commit_author_email.clone(),
                item_type: item.item_type.clone(),
                has_diff: item.has_diff,
                patch_id_stable: item.patch_id_stable.clone(),
                file_count: 0,
                additions: 0,
                deletions: 0,
                hunk_count: 0,
            })
            .collect::<Vec<_>>();
        let mut written_items = store
            .upsert_patch_items_batch(version.id, &upsert_inputs)
            .await
            .context("upsert patch_items batch")?;
        written_items.sort_by_key(|item| (item.ordinal, item.id));

        let written_patch_count = written_items
            .iter()
            .filter(|record| record.item_type == "patch")
            .count() as u64;
        output.patch_item_ids.extend(
            written_items
                .iter()
                .filter(|record| record.item_type == "patch")
                .map(|record| record.id),
        );

        apply_assembled_view(
            store,
            series_id,
            version.id,
            version.version_num,
            &written_items,
            &candidate,
        )
        .await
        .context("apply assembled view")?;

        recompute_logical_mapping(store, series_id)
            .await
            .context("recompute logical mapping")?;

        store
            .touch_series_seen(series_id, candidate.sent_at, Some(version.id))
            .await
            .context("touch patch_series")?;

        seen_series_ids.insert(series_id);
        output.series_versions_written += 1;
        output.patch_items_written += written_patch_count;
    }

    output.patch_item_ids.sort_unstable();
    output.patch_item_ids.dedup();
    output.series_ids = seen_series_ids.into_iter().collect();

    Ok(output)
}

pub async fn process_patch_id_compute_batch(
    store: &LineageStore,
    patch_item_ids: &[i64],
) -> anyhow::Result<PatchIdComputeOutcome> {
    let mut ids = patch_item_ids.to_vec();
    ids.sort_unstable();
    ids.dedup();

    if ids.is_empty() {
        return Ok(PatchIdComputeOutcome::default());
    }

    let diffs = store
        .load_patch_item_diffs(&ids)
        .await
        .context("load patch item diffs")?;

    let mut memo = PatchIdMemo::default();
    let mut updates = Vec::with_capacity(diffs.len());
    for row in diffs {
        let patch_id = row.diff_text.as_deref().and_then(|diff| memo.compute(diff));
        updates.push((row.patch_item_id, patch_id));
    }
    store
        .set_patch_item_patch_ids_batch(&updates)
        .await
        .context("batch update patch item patch-id values")?;

    Ok(PatchIdComputeOutcome {
        patch_items_updated: updates.len() as u64,
    })
}

pub async fn process_diff_parse_patch_items(
    store: &LineageStore,
    patch_item_ids: &[i64],
) -> anyhow::Result<DiffParseOutcome> {
    let mut ids = patch_item_ids.to_vec();
    ids.sort_unstable();
    ids.dedup();
    ids.retain(|v| *v > 0);

    if ids.is_empty() {
        return Ok(DiffParseOutcome::default());
    }

    let diffs = store
        .load_patch_item_diffs(&ids)
        .await
        .context("load patch item diffs for metadata parse")?;

    let mut updates = Vec::with_capacity(diffs.len());
    let mut files_written = 0u64;

    for row in diffs {
        let parsed_files = row
            .diff_text
            .as_deref()
            .map(parse_diff_metadata)
            .unwrap_or_default();

        let mut file_rows = Vec::with_capacity(parsed_files.len());
        let mut additions = 0i32;
        let mut deletions = 0i32;
        let mut hunk_count = 0i32;

        for file in parsed_files {
            additions += file.additions;
            deletions += file.deletions;
            hunk_count += file.hunk_count;

            file_rows.push(UpsertPatchItemFileInput {
                old_path: file.old_path,
                new_path: file.new_path,
                change_type: file.change_type,
                is_binary: file.is_binary,
                additions: file.additions,
                deletions: file.deletions,
                hunk_count: file.hunk_count,
                diff_start: file.diff_start,
                diff_end: file.diff_end,
            });
        }

        let file_count = file_rows.len() as i32;
        updates.push(PatchItemFileBatchInput {
            patch_item_id: row.patch_item_id,
            file_count,
            additions,
            deletions,
            hunk_count,
            files: file_rows,
        });
        files_written += file_count as u64;
    }

    store
        .replace_patch_item_files_batch(&updates)
        .await
        .context("replace patch item files and stats in batch")?;

    Ok(DiffParseOutcome {
        patch_items_updated: updates.len() as u64,
        patch_item_files_written: files_written,
    })
}

fn build_candidates(
    messages: Vec<LineageSourceMessage>,
    patch_id_memo: &mut PatchIdMemo,
) -> Vec<CandidateVersion> {
    let mut by_thread: BTreeMap<i64, Vec<LineageSourceMessage>> = BTreeMap::new();
    for msg in messages {
        by_thread.entry(msg.thread_id).or_default().push(msg);
    }

    let mut out = Vec::new();
    for (thread_id, thread_messages) in by_thread {
        let mut by_subject_and_version: BTreeMap<(String, i32, bool), CandidateVersion> =
            BTreeMap::new();
        let mut cover_subject_by_version: HashMap<(i32, bool), String> = HashMap::new();
        let mut explicit_revision_by_message_id: HashMap<String, i32> = HashMap::new();

        for message in &thread_messages {
            let parsed = parse_patch_subject(&message.subject_raw);
            if parsed.has_patch_tag && !parsed.revision_inferred {
                explicit_revision_by_message_id
                    .insert(message.message_id_primary.clone(), parsed.version_num);
            }
            if parsed.has_patch_tag
                && !parsed.had_reply_prefix
                && parsed.ordinal == Some(0)
                && !parsed.subject_norm_base.is_empty()
            {
                cover_subject_by_version
                    .entry((parsed.version_num, parsed.is_rfc))
                    .or_insert(parsed.subject_norm_base);
            }
        }

        for (thread_order, message) in thread_messages.into_iter().enumerate() {
            let parsed = parse_patch_subject(&message.subject_raw);
            let effective_version_num = if parsed.revision_inferred {
                message
                    .in_reply_to_ids
                    .iter()
                    .chain(message.references_ids.iter())
                    .find_map(|message_id| explicit_revision_by_message_id.get(message_id))
                    .copied()
                    .unwrap_or(parsed.version_num)
            } else {
                parsed.version_num
            };
            let extracted_diff = message
                .diff_text
                .as_deref()
                .and_then(extract_diff_text)
                .or_else(|| message.body_text.as_deref().and_then(extract_diff_text));
            let has_diff = extracted_diff
                .as_deref()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false);

            if !parsed.has_patch_tag {
                continue;
            }

            if parsed.subject_norm_base.is_empty() {
                continue;
            }

            let is_cover = !parsed.had_reply_prefix && parsed.ordinal == Some(0);
            let is_patch = !parsed.had_reply_prefix
                && parsed.ordinal.is_some_and(|ordinal| ordinal > 0)
                && has_diff;

            if !is_cover && !is_patch {
                continue;
            }

            let mut canonical_subject_norm = cover_subject_by_version
                .get(&(effective_version_num, parsed.is_rfc))
                .cloned()
                .unwrap_or_else(|| parsed.subject_norm_base.clone());

            let is_synthetic_group = !cover_subject_by_version
                .contains_key(&(effective_version_num, parsed.is_rfc))
                && parsed.total.unwrap_or(0) > 1;

            let group_key_subject = if is_synthetic_group {
                format!(
                    "__thread:{}:v{}:r{}:t{}",
                    thread_id,
                    effective_version_num,
                    parsed.is_rfc,
                    parsed.total.unwrap_or(0)
                )
            } else {
                canonical_subject_norm.clone()
            };

            let key = (group_key_subject, effective_version_num, parsed.is_rfc);

            let entry = by_subject_and_version
                .entry(key)
                .or_insert_with(|| CandidateVersion {
                    thread_id,
                    canonical_subject_norm: canonical_subject_norm.clone(),
                    author_email: message.from_email.clone(),
                    author_name: message.from_name.clone(),
                    version_num: effective_version_num,
                    is_rfc: parsed.is_rfc,
                    is_resend: parsed.is_resend,
                    sent_at: message.date_utc.unwrap_or_else(epoch_utc),
                    subject_raw: message.subject_raw.clone(),
                    subject_norm: message.subject_norm.clone(),
                    cover_message_pk: None,
                    first_patch_message_pk: None,
                    base_commit: message.body_text.as_deref().and_then(extract_base_commit),
                    change_id: message.body_text.as_deref().and_then(extract_change_id),
                    reference_message_ids: lineage_reference_ids(&message),
                    items: Vec::new(),
                });

            if entry.canonical_subject_norm.starts_with("__thread:") {
                entry.canonical_subject_norm = canonical_subject_norm.clone();
            }
            if entry.canonical_subject_norm.is_empty() {
                entry.canonical_subject_norm = std::mem::take(&mut canonical_subject_norm);
            }

            if message.date_utc.unwrap_or_else(epoch_utc) < entry.sent_at {
                entry.sent_at = message.date_utc.unwrap_or_else(epoch_utc);
                entry.subject_raw = message.subject_raw.clone();
                entry.subject_norm = message.subject_norm.clone();
            }

            if entry.change_id.is_none() {
                entry.change_id = message.body_text.as_deref().and_then(extract_change_id);
            }
            if entry.base_commit.is_none() {
                entry.base_commit = message.body_text.as_deref().and_then(extract_base_commit);
            }
            if entry.reference_message_ids.is_empty() {
                entry.reference_message_ids = lineage_reference_ids(&message);
            }

            let item_type = if is_cover { "cover" } else { "patch" }.to_string();
            let ordinal = parsed.ordinal;

            if is_cover {
                entry.cover_message_pk.get_or_insert(message.message_pk);
            } else if entry.first_patch_message_pk.is_none() {
                entry.first_patch_message_pk = Some(message.message_pk);
            }

            let patch_id_stable = extracted_diff
                .as_deref()
                .and_then(|diff| patch_id_memo.compute(diff));

            entry.items.push(CandidateItem {
                message_pk: message.message_pk,
                thread_order,
                ordinal,
                total: parsed.total,
                subject_raw: message.subject_raw,
                subject_norm: message.subject_norm,
                commit_subject: if item_type == "patch" {
                    Some(parsed.subject_norm_base.clone())
                } else {
                    None
                },
                commit_subject_norm: if item_type == "patch" {
                    Some(parsed.subject_norm_base)
                } else {
                    None
                },
                commit_author_name: message.from_name,
                commit_author_email: Some(message.from_email),
                item_type,
                has_diff,
                patch_id_stable,
            });
        }

        for mut candidate in by_subject_and_version.into_values() {
            candidate.items = dedupe_items_by_ordinal(candidate.items);
            normalize_item_ordinals(&mut candidate.items);
            candidate.items.sort_by_key(|item| {
                (
                    item.ordinal.unwrap_or(0),
                    item.thread_order,
                    item.message_pk,
                )
            });
            candidate.cover_message_pk = candidate
                .items
                .iter()
                .find(|item| item.item_type == "cover" && item.ordinal == Some(0))
                .map(|item| item.message_pk);
            candidate.first_patch_message_pk = candidate
                .items
                .iter()
                .find(|item| item.item_type == "patch")
                .map(|item| item.message_pk);
            out.push(candidate);
        }
    }

    out
}

fn dedupe_items_by_ordinal(items: Vec<CandidateItem>) -> Vec<CandidateItem> {
    let mut by_ordinal: BTreeMap<i32, CandidateItem> = BTreeMap::new();
    for item in items {
        let Some(ordinal) = item.ordinal else {
            continue;
        };

        let replace = by_ordinal
            .get(&ordinal)
            .map(|existing| is_better_candidate_item(&item, existing))
            .unwrap_or(true);
        if replace {
            by_ordinal.insert(ordinal, item);
        }
    }
    by_ordinal.into_values().collect()
}

fn is_better_candidate_item(candidate: &CandidateItem, existing: &CandidateItem) -> bool {
    let candidate_rank = (
        if candidate.item_type == "patch" { 0 } else { 1 },
        if candidate.has_diff { 0 } else { 1 },
        candidate.thread_order,
        candidate.message_pk,
    );
    let existing_rank = (
        if existing.item_type == "patch" { 0 } else { 1 },
        if existing.has_diff { 0 } else { 1 },
        existing.thread_order,
        existing.message_pk,
    );
    candidate_rank < existing_rank
}

fn normalize_item_ordinals(items: &mut [CandidateItem]) {
    let mut used = HashSet::new();
    for item in items.iter() {
        if let Some(ordinal) = item.ordinal
            && ordinal > 0
        {
            used.insert(ordinal);
        }
    }

    let mut next_ordinal = 1i32;
    for item in items.iter_mut() {
        if item.item_type != "patch" {
            if item.item_type == "cover" && item.ordinal.is_none() {
                item.ordinal = Some(0);
            }
            continue;
        }

        let needs_assignment = item.ordinal.is_none() || item.ordinal == Some(0);
        if !needs_assignment {
            continue;
        }

        while used.contains(&next_ordinal) {
            next_ordinal += 1;
        }
        item.ordinal = Some(next_ordinal);
        used.insert(next_ordinal);
        next_ordinal += 1;
    }

    let inferred_total = items
        .iter()
        .filter(|item| item.item_type == "patch")
        .filter_map(|item| item.ordinal)
        .max()
        .unwrap_or(0);

    for item in items.iter_mut() {
        if item.item_type == "patch" {
            item.total = Some(item.total.unwrap_or(inferred_total).max(inferred_total));
        }
    }
}

async fn resolve_series_id(
    store: &LineageStore,
    mailing_list_id: i64,
    candidate: &CandidateVersion,
) -> anyhow::Result<i64> {
    if let Some(change_id) = candidate.change_id.as_deref()
        && let Some(series) = store.get_series_by_change_id(change_id).await?
    {
        return Ok(series.id);
    }

    if !candidate.reference_message_ids.is_empty() {
        let linked_series = store
            .find_series_ids_by_message_ids(mailing_list_id, &candidate.reference_message_ids)
            .await?;
        if let Some(series_id) = linked_series.first().copied() {
            return Ok(series_id);
        }
    }

    let patch_ids = candidate_patch_ids(candidate);
    let from_ts = candidate.sent_at - Duration::days(180);
    let to_ts = candidate.sent_at + Duration::days(1);
    let candidates = store
        .find_similarity_candidates(&candidate.canonical_subject_norm, from_ts, to_ts)
        .await?;
    let similarity_series_ids = candidates.iter().map(|row| row.id).collect::<Vec<_>>();
    let latest_patch_id_rows = store
        .list_latest_patch_ids_for_series_bulk(&similarity_series_ids)
        .await?;
    let mut latest_patch_ids_by_series: HashMap<i64, HashSet<String>> = HashMap::new();
    for (series_id, patch_id) in latest_patch_id_rows {
        latest_patch_ids_by_series
            .entry(series_id)
            .or_default()
            .insert(patch_id);
    }

    let mut best: Option<SimilarityChoice> = None;
    let empty_ids = HashSet::new();
    for row in &candidates {
        let latest_ids = latest_patch_ids_by_series
            .get(&row.id)
            .unwrap_or(&empty_ids);
        let score = jaccard_similarity(&patch_ids, latest_ids);

        let author_matches = row.author_email == candidate.author_email;
        let allowed = if author_matches {
            score >= 0.40
        } else {
            score >= 0.85 && candidate.is_resend
        };

        if !allowed {
            continue;
        }

        match &best {
            Some(existing) if existing.score >= score => {}
            _ => {
                best = Some(SimilarityChoice {
                    series_id: row.id,
                    score,
                });
            }
        }
    }

    if let Some(choice) = best {
        return Ok(choice.series_id);
    }

    if candidate.version_num > 1 || candidate.is_resend {
        let mut same_author = candidates
            .iter()
            .filter(|row| row.author_email == candidate.author_email);
        if let Some(first) = same_author.next() {
            return Ok(first.id);
        }
    }

    Ok(0)
}

fn candidate_patch_ids(candidate: &CandidateVersion) -> HashSet<String> {
    candidate
        .items
        .iter()
        .filter(|item| item.item_type == "patch")
        .filter_map(|item| item.patch_id_stable.clone())
        .collect()
}

fn jaccard_similarity(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
    if a.is_empty() && b.is_empty() {
        return 0.0;
    }

    let intersection = a.intersection(b).count() as f64;
    let union = a.union(b).count() as f64;
    if union == 0.0 {
        0.0
    } else {
        intersection / union
    }
}

fn compute_version_fingerprint(items: &[CandidateItem]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    let mut ordered = items.to_vec();
    ordered.sort_by_key(|item| (item.ordinal.unwrap_or(0), item.message_pk));

    for item in ordered {
        if item.item_type != "patch" {
            continue;
        }
        hasher.update(item.ordinal.unwrap_or(0).to_be_bytes());
        if let Some(patch_id) = item.patch_id_stable {
            hasher.update(patch_id.as_bytes());
        }
        if let Some(commit_subject_norm) = item.commit_subject_norm {
            hasher.update(commit_subject_norm.as_bytes());
        }
        hasher.update(b"\n");
    }

    hasher.finalize().to_vec()
}

async fn apply_assembled_view(
    store: &LineageStore,
    series_id: i64,
    version_id: i64,
    version_num: i32,
    current_patch_items: &[nexus_db::PatchItemRecord],
    candidate: &CandidateVersion,
) -> anyhow::Result<()> {
    let max_total = current_patch_items
        .iter()
        .filter(|item| item.item_type == "patch")
        .filter_map(|item| item.total)
        .max()
        .unwrap_or_else(|| {
            current_patch_items
                .iter()
                .filter(|item| item.item_type == "patch")
                .map(|item| item.ordinal)
                .max()
                .unwrap_or(0)
        });

    let mut assembled = Vec::new();

    if let Some(cover) = current_patch_items
        .iter()
        .find(|item| item.item_type == "cover" && item.ordinal == 0)
    {
        assembled.push((0, cover.id, None));
    }

    let mut inherited_found = false;
    let mut current_by_ordinal: HashMap<i32, i64> = HashMap::new();
    for item in current_patch_items {
        if item.item_type == "patch" {
            current_by_ordinal.insert(item.ordinal, item.id);
        }
    }

    for ordinal in 1..=max_total {
        if let Some(patch_item_id) = current_by_ordinal.get(&ordinal).copied() {
            assembled.push((ordinal, patch_item_id, None));
            continue;
        }

        if let Some((inherited_patch_item_id, inherited_from_version_num)) = store
            .find_inherited_patch_item(series_id, version_num, ordinal)
            .await?
        {
            assembled.push((
                ordinal,
                inherited_patch_item_id,
                Some(inherited_from_version_num),
            ));
            inherited_found = true;
        }
    }

    store
        .replace_assembled_items(version_id, &assembled)
        .await?;
    store
        .set_partial_reroll_flag(version_id, inherited_found)
        .await?;

    // Keep subject-based metadata for logical mapping stable.
    if candidate.items.is_empty() {
        return Ok(());
    }

    Ok(())
}

async fn recompute_logical_mapping(store: &LineageStore, series_id: i64) -> anyhow::Result<()> {
    let assembled = store.load_assembled_items_for_series(series_id).await?;
    if assembled.is_empty() {
        return Ok(());
    }

    let mut by_version: BTreeMap<i32, Vec<AssembledItemRecord>> = BTreeMap::new();
    for item in assembled {
        by_version.entry(item.version_num).or_default().push(item);
    }

    let existing = store.list_patch_logicals_for_series(series_id).await?;
    let mut next_slot = existing.iter().map(|row| row.slot).max().unwrap_or(0) + 1;

    let mut slot_by_patch_id: HashMap<String, i32> = HashMap::new();
    let mut slot_by_title: HashMap<String, i32> = HashMap::new();
    let mut slot_titles: HashMap<i32, String> = HashMap::new();
    let mut mappings: Vec<(i32, i32, i64)> = Vec::new();

    for (version_num, mut items) in by_version {
        items.sort_by_key(|item| item.ordinal);
        let mut used_slots = HashSet::new();

        for item in items {
            let title = item.title_norm.trim().to_string();
            let patch_id = item.patch_id_stable.clone().unwrap_or_default();

            let mut chosen_slot = None;
            if !patch_id.is_empty()
                && let Some(slot) = slot_by_patch_id.get(&patch_id).copied()
                && !used_slots.contains(&slot)
            {
                chosen_slot = Some(slot);
            }

            if chosen_slot.is_none()
                && !title.is_empty()
                && let Some(slot) = slot_by_title.get(&title).copied()
                && !used_slots.contains(&slot)
            {
                chosen_slot = Some(slot);
            }

            if chosen_slot.is_none() && item.ordinal > 0 && !used_slots.contains(&item.ordinal) {
                chosen_slot = Some(item.ordinal);
            }

            let slot = chosen_slot.unwrap_or_else(|| {
                let assigned = next_slot;
                next_slot += 1;
                assigned
            });

            used_slots.insert(slot);
            if !patch_id.is_empty() {
                slot_by_patch_id.insert(patch_id, slot);
            }
            if !title.is_empty() {
                slot_by_title.insert(title.clone(), slot);
                slot_titles.entry(slot).or_insert(title);
            }

            mappings.push((slot, version_num, item.patch_item_id));
        }
    }

    let mut used_slots = BTreeSet::new();
    for (slot, _, _) in &mappings {
        used_slots.insert(*slot);
    }

    for slot in used_slots {
        let title = slot_titles
            .get(&slot)
            .cloned()
            .unwrap_or_else(|| format!("slot-{slot}"));
        store.upsert_patch_logical(series_id, slot, &title).await?;
    }

    store
        .replace_patch_logical_versions(series_id, &mappings)
        .await?;

    Ok(())
}

fn lineage_reference_ids(message: &LineageSourceMessage) -> Vec<String> {
    nexus_db::LineageStore::unique_message_ids(&message.references_ids, &message.in_reply_to_ids)
}

fn extract_change_id(body_text: &str) -> Option<String> {
    CHANGE_ID_RE
        .captures(body_text)
        .and_then(|caps| caps.get(1))
        .map(|value| value.as_str().trim().to_string())
        .filter(|value| !value.is_empty())
}

fn extract_base_commit(body_text: &str) -> Option<String> {
    BASE_COMMIT_RE
        .captures(body_text)
        .and_then(|caps| caps.get(1))
        .map(|value| value.as_str().trim().to_string())
        .filter(|value| !value.is_empty())
}

fn epoch_utc() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch valid")
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use nexus_core::config::DatabaseConfig;
    use nexus_db::{
        CatalogStore, Db, IngestStore, LineageSourceMessage, LineageStore, ParsedBodyInput,
        ParsedMessageInput,
    };
    use sqlx::Row;

    use crate::mail::parse_email;
    use crate::patch_id::PatchIdMemo;
    use crate::patch_subject::parse_patch_subject;

    use super::{
        build_candidates, process_diff_parse_patch_items, process_patch_extract_window,
        process_patch_id_compute_batch,
    };

    #[test]
    fn subject_examples_are_supported() {
        let a = parse_patch_subject("[PATCH v2 01/27] mm: reclaim");
        assert_eq!(a.version_num, 2);
        assert_eq!(a.ordinal, Some(1));
        assert_eq!(a.total, Some(27));

        let b = parse_patch_subject("[RFC PATCH 0/5] bpf: thing");
        assert!(b.is_rfc);
        assert_eq!(b.ordinal, Some(0));

        let c = parse_patch_subject("[PATCH RESEND v3 2/7] net: clean");
        assert!(c.is_resend);
        assert_eq!(c.version_num, 3);
        assert_eq!(c.ordinal, Some(2));

        let reply = parse_patch_subject("Re: [PATCH 1/1] net: clean");
        assert!(reply.had_reply_prefix);
    }

    #[test]
    fn build_candidates_ignores_reply_subject_contamination() {
        let messages = vec![
            source_message(
                500,
                1,
                1,
                "[PATCH 0/2] bpf: series",
                "alice@example.com",
                "Mon, 01 Jan 2024 00:00:00 +0000",
                "Cover body",
                None,
            ),
            source_message(
                500,
                2,
                2,
                "[PATCH 1/2] bpf: patch one",
                "alice@example.com",
                "Mon, 01 Jan 2024 00:01:00 +0000",
                "Patch body",
                Some("diff --git a/a.c b/a.c\n--- a/a.c\n+++ b/a.c\n@@ -1 +1 @@\n-old\n+new\n"),
            ),
            source_message(
                500,
                3,
                3,
                "Re: [PATCH 1/2] bpf: patch one",
                "reviewer@example.com",
                "Mon, 01 Jan 2024 00:02:00 +0000",
                "Looks good",
                None,
            ),
            source_message(
                500,
                4,
                4,
                "Re: [PATCH 1/2] bpf: patch one",
                "reviewer2@example.com",
                "Mon, 01 Jan 2024 00:03:00 +0000",
                "Inline diff in reply",
                Some(
                    "diff --git a/a.c b/a.c\n--- a/a.c\n+++ b/a.c\n@@ -1 +1 @@\n-old\n+new-reply\n",
                ),
            ),
            source_message(
                500,
                5,
                5,
                "[PATCH 2/2] bpf: patch two",
                "alice@example.com",
                "Mon, 01 Jan 2024 00:04:00 +0000",
                "Patch body",
                Some("diff --git a/b.c b/b.c\n--- a/b.c\n+++ b/b.c\n@@ -1 +1 @@\n-old\n+new\n"),
            ),
            source_message(
                500,
                6,
                6,
                "Re: [PATCH 2/2] bpf: patch two",
                "reviewer@example.com",
                "Mon, 01 Jan 2024 00:05:00 +0000",
                "Ack",
                None,
            ),
        ];

        let mut memo = PatchIdMemo::default();
        let candidates = build_candidates(messages, &mut memo);
        assert_eq!(candidates.len(), 1);

        let candidate = &candidates[0];
        let cover_items = candidate
            .items
            .iter()
            .filter(|item| item.item_type == "cover")
            .collect::<Vec<_>>();
        assert_eq!(cover_items.len(), 1);
        assert_eq!(cover_items[0].ordinal, Some(0));
        assert_eq!(cover_items[0].message_pk, 1);

        let patch_items = candidate
            .items
            .iter()
            .filter(|item| item.item_type == "patch")
            .collect::<Vec<_>>();
        assert_eq!(patch_items.len(), 2);
        assert_eq!(patch_items[0].ordinal, Some(1));
        assert_eq!(patch_items[0].message_pk, 2);
        assert_eq!(patch_items[1].ordinal, Some(2));
        assert_eq!(patch_items[1].message_pk, 5);

        assert!(
            candidate
                .items
                .iter()
                .all(|item| !(item.item_type == "cover" && item.ordinal.unwrap_or(0) > 0))
        );
        assert!(
            candidate
                .items
                .iter()
                .all(|item| ![3, 4, 6].contains(&item.message_pk)),
            "reply messages must not be present in patch_items",
        );
    }

    #[tokio::test]
    async fn local_db_e2e_partial_reroll_and_idempotency() -> Result<(), Box<dyn std::error::Error>>
    {
        let Ok(database_url) = std::env::var("NEXUS_TEST_DATABASE_URL") else {
            return Ok(());
        };

        let db = Db::connect(&DatabaseConfig {
            url: database_url,
            max_connections: 4,
        })
        .await?;
        db.migrate().await?;

        let catalog = CatalogStore::new(db.pool().clone());
        let ingest = IngestStore::new(db.pool().clone());
        let lineage = LineageStore::new(db.pool().clone());

        let list_key = format!(
            "lineage-test-{}",
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000)
                .abs()
        );
        let list = catalog.ensure_mailing_list(&list_key).await?;
        let repo = catalog.ensure_repo(list.id, "test.git", "test.git").await?;

        let series_tag = format!("series-{}", list.id);
        let messages = fixture_messages(&series_tag);
        let mut message_pks = Vec::new();

        for (idx, raw) in messages.iter().enumerate() {
            let parsed = parse_email(raw.as_bytes())?;
            let input = ParsedMessageInput {
                content_hash_sha256: parsed.content_hash_sha256,
                subject_raw: parsed.subject_raw,
                subject_norm: parsed.subject_norm,
                from_name: parsed.from_name,
                from_email: parsed.from_email,
                date_utc: parsed.date_utc,
                to_raw: parsed.to_raw,
                cc_raw: parsed.cc_raw,
                message_ids: parsed.message_ids,
                message_id_primary: parsed.message_id_primary,
                in_reply_to_ids: parsed.in_reply_to_ids,
                references_ids: parsed.references_ids,
                mime_type: parsed.mime_type,
                body: ParsedBodyInput {
                    raw_rfc822: raw.as_bytes().to_vec(),
                    body_text: parsed.body_text,
                    diff_text: parsed.diff_text,
                    search_text: parsed.search_text,
                    has_diff: parsed.has_diff,
                    has_attachments: parsed.has_attachments,
                },
            };

            let outcome = ingest
                .ingest_message(&repo, &format!("{:040x}", idx + 1), &input)
                .await?;
            let message_pk = outcome.message_pk.expect("message pk");
            message_pks.push(message_pk);
        }

        let root_message_pk = *message_pks.first().expect("messages");
        let thread_id: i64 = sqlx::query_scalar(
            r#"INSERT INTO threads
            (mailing_list_id, root_node_key, root_message_pk, subject_norm, created_at, last_activity_at, message_count, membership_hash)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id"#,
        )
        .bind(list.id)
        .bind("lineage-test-root")
        .bind(root_message_pk)
        .bind("subsys: demo series")
        .bind(Utc.timestamp_opt(1_700_000_000, 0).single().expect("ts"))
        .bind(Utc.timestamp_opt(1_700_000_100, 0).single().expect("ts"))
        .bind(message_pks.len() as i32)
        .bind(vec![1u8; 32])
        .fetch_one(db.pool())
        .await?;

        for (idx, message_pk) in message_pks.iter().enumerate() {
            let depth = if idx == 0 { 0 } else { 1 };
            let parent = if idx == 0 {
                None
            } else {
                Some(root_message_pk)
            };
            let mut sort_key = Vec::new();
            sort_key.extend(((idx + 1) as u32).to_be_bytes());

            sqlx::query(
                r#"INSERT INTO thread_messages
                (mailing_list_id, thread_id, message_pk, parent_message_pk, depth, sort_key, is_dummy)
                VALUES ($1, $2, $3, $4, $5, $6, false)"#,
            )
            .bind(list.id)
            .bind(thread_id)
            .bind(*message_pk)
            .bind(parent)
            .bind(depth)
            .bind(sort_key)
            .execute(db.pool())
            .await?;
        }

        let anchors = vec![*message_pks.last().expect("anchor")];
        let first = process_patch_extract_window(&lineage, list.id, &anchors).await?;
        let series_subjects: Vec<String> = sqlx::query_scalar(
            "SELECT canonical_subject_norm FROM patch_series WHERE id = ANY($1) ORDER BY id ASC",
        )
        .bind(&first.series_ids)
        .fetch_all(db.pool())
        .await?;
        assert_eq!(
            first.series_ids.len(),
            1,
            "series ids: {:?}, subjects: {:?}",
            first.series_ids,
            series_subjects
        );
        assert!(!first.patch_item_ids.is_empty());

        let patch_id_outcome =
            process_patch_id_compute_batch(&lineage, &first.patch_item_ids).await?;
        assert!(patch_id_outcome.patch_items_updated >= 1);
        let diff_parse_outcome =
            process_diff_parse_patch_items(&lineage, &first.patch_item_ids).await?;
        assert!(diff_parse_outcome.patch_items_updated >= 1);
        assert!(diff_parse_outcome.patch_item_files_written >= 1);

        let series_id = first.series_ids[0];
        let series_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM patch_series WHERE id = $1")
                .bind(series_id)
                .fetch_one(db.pool())
                .await?;
        assert_eq!(series_count, 1);

        let version_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM patch_series_versions WHERE patch_series_id = $1",
        )
        .bind(series_id)
        .fetch_one(db.pool())
        .await?;
        assert_eq!(version_count, 2);

        let partial_reroll: bool = sqlx::query_scalar(
            r#"SELECT is_partial_reroll
            FROM patch_series_versions
            WHERE patch_series_id = $1 AND version_num = 2
            ORDER BY id DESC
            LIMIT 1"#,
        )
        .bind(series_id)
        .fetch_one(db.pool())
        .await?;
        assert!(partial_reroll);

        let assembled_rows = sqlx::query(
            r#"SELECT psvai.ordinal, psvai.inherited_from_version_num
            FROM patch_series_versions psv
            JOIN patch_series_version_assembled_items psvai
              ON psvai.patch_series_version_id = psv.id
            WHERE psv.patch_series_id = $1
              AND psv.version_num = 2
            ORDER BY psvai.ordinal ASC"#,
        )
        .bind(series_id)
        .fetch_all(db.pool())
        .await?;

        let ordinals: Vec<i32> = assembled_rows
            .iter()
            .map(|row| row.get::<i32, _>(0))
            .collect();
        assert_eq!(ordinals, vec![0, 1, 2, 3]);
        let inherited_count = assembled_rows
            .iter()
            .filter(|row| row.get::<Option<i32>, _>(1).is_some())
            .count();
        assert!(inherited_count >= 2);

        let logical_versions_count: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*)
            FROM patch_logical_versions plv
            JOIN patch_logical pl
              ON pl.id = plv.patch_logical_id
            WHERE pl.patch_series_id = $1"#,
        )
        .bind(series_id)
        .fetch_one(db.pool())
        .await?;
        assert!(logical_versions_count >= 6);

        let patch_with_ids: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*)
            FROM patch_items pi
            JOIN patch_series_versions psv
              ON psv.id = pi.patch_series_version_id
            WHERE psv.patch_series_id = $1
              AND pi.item_type = 'patch'
              AND pi.patch_id_stable IS NOT NULL"#,
        )
        .bind(series_id)
        .fetch_one(db.pool())
        .await?;
        assert_eq!(patch_with_ids, 4);

        let bad_cover_ordinals: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*)
            FROM patch_items pi
            JOIN patch_series_versions psv
              ON psv.id = pi.patch_series_version_id
            WHERE psv.patch_series_id = $1
              AND pi.item_type = 'cover'
              AND pi.ordinal > 0"#,
        )
        .bind(series_id)
        .fetch_one(db.pool())
        .await?;
        assert_eq!(bad_cover_ordinals, 0);

        let patch_item_files_count: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*)
            FROM patch_item_files pif
            JOIN patch_items pi ON pi.id = pif.patch_item_id
            JOIN patch_series_versions psv ON psv.id = pi.patch_series_version_id
            WHERE psv.patch_series_id = $1"#,
        )
        .bind(series_id)
        .fetch_one(db.pool())
        .await?;
        assert!(patch_item_files_count >= 4);

        let file_slices = sqlx::query(
            r#"SELECT pif.diff_start, pif.diff_end, mb.diff_text
            FROM patch_item_files pif
            JOIN patch_items pi ON pi.id = pif.patch_item_id
            JOIN messages m ON m.id = pi.message_pk
            JOIN message_bodies mb ON mb.id = m.body_id
            JOIN patch_series_versions psv ON psv.id = pi.patch_series_version_id
            WHERE psv.patch_series_id = $1"#,
        )
        .bind(series_id)
        .fetch_all(db.pool())
        .await?;
        for row in &file_slices {
            let start = row.get::<i32, _>(0) as usize;
            let end = row.get::<i32, _>(1) as usize;
            let diff_text = row.get::<Option<String>, _>(2).unwrap_or_default();
            assert!(start < end);
            assert!(end <= diff_text.len());
            let slice = &diff_text[start..end];
            assert!(slice.starts_with("diff --git "));
        }

        let stats_sum: (i64, i64, i64, i64) = sqlx::query_as::<_, (i64, i64, i64, i64)>(
            r#"SELECT
                COALESCE(SUM(file_count)::bigint, 0) AS file_count_sum,
                COALESCE(SUM(additions)::bigint, 0) AS additions_sum,
                COALESCE(SUM(deletions)::bigint, 0) AS deletions_sum,
                COALESCE(SUM(hunk_count)::bigint, 0) AS hunk_count_sum
            FROM patch_items pi
            JOIN patch_series_versions psv
              ON psv.id = pi.patch_series_version_id
            WHERE psv.patch_series_id = $1
              AND pi.item_type = 'patch'"#,
        )
        .bind(series_id)
        .fetch_one(db.pool())
        .await?;
        assert!(stats_sum.0 >= 4);
        assert!(stats_sum.1 >= 4);
        assert!(stats_sum.2 >= 4);
        assert!(stats_sum.3 >= 4);

        let before_counts = snapshot_counts(db.pool(), series_id).await?;
        let _second = process_patch_extract_window(&lineage, list.id, &anchors).await?;
        let _second_diff_parse =
            process_diff_parse_patch_items(&lineage, &first.patch_item_ids).await?;
        let after_counts = snapshot_counts(db.pool(), series_id).await?;
        assert_eq!(before_counts, after_counts);

        Ok(())
    }

    async fn snapshot_counts(
        pool: &sqlx::PgPool,
        series_id: i64,
    ) -> Result<(i64, i64, i64, i64, i64), sqlx::Error> {
        let versions = sqlx::query_scalar(
            "SELECT COUNT(*) FROM patch_series_versions WHERE patch_series_id = $1",
        )
        .bind(series_id)
        .fetch_one(pool)
        .await?;
        let items = sqlx::query_scalar(
            r#"SELECT COUNT(*)
            FROM patch_items pi
            JOIN patch_series_versions psv
              ON psv.id = pi.patch_series_version_id
            WHERE psv.patch_series_id = $1"#,
        )
        .bind(series_id)
        .fetch_one(pool)
        .await?;
        let assembled = sqlx::query_scalar(
            r#"SELECT COUNT(*)
            FROM patch_series_version_assembled_items psvai
            JOIN patch_series_versions psv
              ON psv.id = psvai.patch_series_version_id
            WHERE psv.patch_series_id = $1"#,
        )
        .bind(series_id)
        .fetch_one(pool)
        .await?;
        let logical_versions = sqlx::query_scalar(
            r#"SELECT COUNT(*)
            FROM patch_logical_versions plv
            JOIN patch_logical pl
              ON pl.id = plv.patch_logical_id
            WHERE pl.patch_series_id = $1"#,
        )
        .bind(series_id)
        .fetch_one(pool)
        .await?;
        let files = sqlx::query_scalar(
            r#"SELECT COUNT(*)
            FROM patch_item_files pif
            JOIN patch_items pi
              ON pi.id = pif.patch_item_id
            JOIN patch_series_versions psv
              ON psv.id = pi.patch_series_version_id
            WHERE psv.patch_series_id = $1"#,
        )
        .bind(series_id)
        .fetch_one(pool)
        .await?;

        Ok((versions, items, assembled, logical_versions, files))
    }

    fn fixture_messages(series_tag: &str) -> Vec<String> {
        let series_subject = format!("subsys: demo series {series_tag}");
        vec![
            cover_mail(
                "m1@example.com",
                "Mon, 01 Jan 2024 00:00:00 +0000",
                &format!("[PATCH 0/3] {series_subject}"),
                None,
            ),
            patch_mail(
                "m2@example.com",
                "Mon, 01 Jan 2024 00:01:00 +0000",
                "[PATCH 1/3] subsys: part one",
                "foo.c",
                "old1",
                "new1",
                None,
            ),
            reply_mail(
                "m2-reply@example.com",
                "Mon, 01 Jan 2024 00:01:30 +0000",
                "Re: [PATCH 1/3] subsys: part one",
                Some("<m2@example.com>"),
                None,
            ),
            reply_mail(
                "m2-reply-diff@example.com",
                "Mon, 01 Jan 2024 00:01:45 +0000",
                "Re: [PATCH 1/3] subsys: part one",
                Some("<m2@example.com>"),
                Some(
                    "diff --git a/foo.c b/foo.c\r\n--- a/foo.c\r\n+++ b/foo.c\r\n@@ -1 +1 @@\r\n-old1\r\n+reply-change\r\n",
                ),
            ),
            patch_mail(
                "m3@example.com",
                "Mon, 01 Jan 2024 00:02:00 +0000",
                "[PATCH 2/3] subsys: part two",
                "bar.c",
                "old2",
                "new2",
                None,
            ),
            patch_mail(
                "m4@example.com",
                "Mon, 01 Jan 2024 00:03:00 +0000",
                "[PATCH 3/3] subsys: part three",
                "baz.c",
                "old3",
                "new3",
                None,
            ),
            cover_mail(
                "m5@example.com",
                "Tue, 02 Jan 2024 00:00:00 +0000",
                &format!("[PATCH v2 0/3] {series_subject}"),
                Some("<m1@example.com>"),
            ),
            patch_mail(
                "m6@example.com",
                "Tue, 02 Jan 2024 00:01:00 +0000",
                "[PATCH v2 2/3] subsys: part two",
                "bar.c",
                "old2",
                "new2-v2",
                Some("<m5@example.com>"),
            ),
        ]
    }

    fn source_message(
        thread_id: i64,
        sort_ordinal: u32,
        message_pk: i64,
        subject_raw: &str,
        from_email: &str,
        date_raw: &str,
        body_text: &str,
        diff_text: Option<&str>,
    ) -> LineageSourceMessage {
        let date_utc = chrono::DateTime::parse_from_rfc2822(date_raw)
            .expect("valid rfc2822 date")
            .with_timezone(&Utc);
        LineageSourceMessage {
            thread_id,
            message_pk,
            sort_key: sort_ordinal.to_be_bytes().to_vec(),
            message_id_primary: format!("m{message_pk}@example.com"),
            subject_raw: subject_raw.to_string(),
            subject_norm: subject_raw.to_ascii_lowercase(),
            from_name: None,
            from_email: from_email.to_string(),
            date_utc: Some(date_utc),
            references_ids: Vec::new(),
            in_reply_to_ids: Vec::new(),
            body_text: Some(body_text.to_string()),
            diff_text: diff_text.map(str::to_string),
        }
    }

    fn cover_mail(message_id: &str, date: &str, subject: &str, references: Option<&str>) -> String {
        let mut headers = vec![
            "From: Alice <alice@example.com>".to_string(),
            format!("Message-ID: <{message_id}>"),
            format!("Date: {date}"),
            format!("Subject: {subject}"),
            "Content-Type: text/plain; charset=utf-8".to_string(),
        ];
        if let Some(reference) = references {
            headers.push(format!("References: {reference}"));
        }

        format!("{}\r\n\r\nCover letter body\r\n", headers.join("\r\n"))
    }

    fn patch_mail(
        message_id: &str,
        date: &str,
        subject: &str,
        file: &str,
        old: &str,
        new: &str,
        references: Option<&str>,
    ) -> String {
        let mut headers = vec![
            "From: Alice <alice@example.com>".to_string(),
            format!("Message-ID: <{message_id}>"),
            format!("Date: {date}"),
            format!("Subject: {subject}"),
            "Content-Type: text/plain; charset=utf-8".to_string(),
        ];
        if let Some(reference) = references {
            headers.push(format!("References: {reference}"));
            headers.push(format!("In-Reply-To: {reference}"));
        }

        let body = format!(
            concat!(
                "From deadbeef Mon Sep 17 00:00:00 2001\r\n",
                "Subject: {subject}\r\n",
                "\r\n",
                "Patch body\r\n",
                "\r\n",
                "---\r\n",
                " {file} | 2 +-\r\n",
                " 1 file changed, 1 insertion(+), 1 deletion(-)\r\n",
                "\r\n",
                "diff --git a/{file} b/{file}\r\n",
                "index 1111111..2222222 100644\r\n",
                "--- a/{file}\r\n",
                "+++ b/{file}\r\n",
                "@@ -1 +1 @@\r\n",
                "-{old}\r\n",
                "+{new}\r\n"
            ),
            subject = subject,
            file = file,
            old = old,
            new = new,
        );

        format!("{}\r\n\r\n{}", headers.join("\r\n"), body)
    }

    fn reply_mail(
        message_id: &str,
        date: &str,
        subject: &str,
        references: Option<&str>,
        inline_diff: Option<&str>,
    ) -> String {
        let mut headers = vec![
            "From: Reviewer <reviewer@example.com>".to_string(),
            format!("Message-ID: <{message_id}>"),
            format!("Date: {date}"),
            format!("Subject: {subject}"),
            "Content-Type: text/plain; charset=utf-8".to_string(),
        ];
        if let Some(reference) = references {
            headers.push(format!("References: {reference}"));
            headers.push(format!("In-Reply-To: {reference}"));
        }

        let body = match inline_diff {
            Some(diff) => format!("Review comments\r\n\r\n{diff}"),
            None => "Review comments only\r\n".to_string(),
        };

        format!("{}\r\n\r\n{}", headers.join("\r\n"), body)
    }
}
