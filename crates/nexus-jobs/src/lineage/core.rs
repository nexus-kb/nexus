use super::*;

pub async fn process_patch_extract_threads(
    store: &LineageStore,
    mailing_list_id: i64,
    thread_ids: &[i64],
) -> anyhow::Result<PatchExtractOutcome> {
    let source_messages = store
        .load_messages_for_threads(mailing_list_id, thread_ids)
        .await
        .context("load lineage source messages for threads")?;

    process_patch_extract_source_messages(store, mailing_list_id, source_messages)
        .await
        .context("process lineage source messages")
}

async fn process_patch_extract_source_messages(
    store: &LineageStore,
    mailing_list_id: i64,
    source_messages: Vec<LineageSourceMessage>,
) -> anyhow::Result<PatchExtractOutcome> {
    let mut output = PatchExtractOutcome {
        source_threads_scanned: source_messages
            .iter()
            .map(|message| message.thread_id)
            .collect::<BTreeSet<_>>()
            .len() as u64,
        source_messages_read: source_messages.len() as u64,
        ..PatchExtractOutcome::default()
    };

    if source_messages.is_empty() {
        return Ok(output);
    }

    let mut candidates = build_candidates(source_messages);

    if candidates.is_empty() {
        return Ok(output);
    }

    candidates.sort_by_key(|v| (v.sent_at, v.thread_id, v.version_num));

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

async fn process_patch_id_compute_batch(
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

async fn process_diff_parse_patch_items(
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

pub async fn process_patch_enrichment_batch(
    store: &LineageStore,
    patch_item_ids: &[i64],
) -> anyhow::Result<PatchEnrichmentOutcome> {
    let mut ids = patch_item_ids.to_vec();
    ids.sort_unstable();
    ids.dedup();
    ids.retain(|v| *v > 0);

    if ids.is_empty() {
        return Ok(PatchEnrichmentOutcome::default());
    }

    let hydration = store
        .hydrate_patch_items_from_message_facts(&ids)
        .await
        .context("hydrate patch items from message patch facts")?;

    let mut output = PatchEnrichmentOutcome {
        patch_items_hydrated: hydration.hydrated_patch_items,
        patch_item_files_written: hydration.patch_item_files_written,
        ..PatchEnrichmentOutcome::default()
    };

    if hydration.missing_patch_item_ids.is_empty() {
        return Ok(output);
    }

    let patch_id_outcome = process_patch_id_compute_batch(store, &hydration.missing_patch_item_ids)
        .await
        .context("fallback patch-id compute for missing patch facts")?;
    let diff_outcome = process_diff_parse_patch_items(store, &hydration.missing_patch_item_ids)
        .await
        .context("fallback diff parse for missing patch facts")?;

    output.patch_items_fallback_patch_id = patch_id_outcome.patch_items_updated;
    output.patch_items_fallback_diff_parse = diff_outcome.patch_items_updated;
    output.patch_item_files_written += diff_outcome.patch_item_files_written;

    Ok(output)
}

pub(super) fn build_candidates(messages: Vec<LineageSourceMessage>) -> Vec<CandidateVersion> {
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
            let has_diff = message.has_diff;

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
                    base_commit: message.base_commit.clone(),
                    change_id: message.change_id.clone(),
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
                entry.change_id = message.change_id.clone();
            }
            if entry.base_commit.is_none() {
                entry.base_commit = message.base_commit.clone();
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

            let patch_id_stable = if item_type == "patch" {
                message.patch_id_stable.clone()
            } else {
                None
            };

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
