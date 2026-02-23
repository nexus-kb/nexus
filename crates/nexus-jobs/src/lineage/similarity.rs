use super::*;

pub(super) fn candidate_patch_ids(candidate: &CandidateVersion) -> HashSet<String> {
    candidate
        .items
        .iter()
        .filter(|item| item.item_type == "patch")
        .filter_map(|item| item.patch_id_stable.clone())
        .collect()
}

pub(super) fn jaccard_similarity(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
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

pub(super) fn compute_version_fingerprint(items: &[CandidateItem]) -> Vec<u8> {
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

pub(super) async fn apply_assembled_view(
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

pub(super) async fn recompute_logical_mapping(
    store: &LineageStore,
    series_id: i64,
) -> anyhow::Result<()> {
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

pub(super) fn lineage_reference_ids(message: &LineageSourceMessage) -> Vec<String> {
    nexus_db::LineageStore::unique_message_ids(&message.references_ids, &message.in_reply_to_ids)
}

pub(super) fn epoch_utc() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch valid")
}
