-- Lineage throughput indexes for set-based matching and patch-id lookups.

CREATE INDEX IF NOT EXISTS idx_patch_series_subject_seen_author
    ON patch_series (canonical_subject_norm, last_seen_at DESC, author_email);

CREATE INDEX IF NOT EXISTS idx_patch_items_version_type_patch_id
    ON patch_items (patch_series_version_id, item_type, patch_id_stable)
    WHERE item_type = 'patch';

CREATE INDEX IF NOT EXISTS idx_message_id_map_lookup_primary
    ON message_id_map (message_id, is_primary DESC, message_pk);
