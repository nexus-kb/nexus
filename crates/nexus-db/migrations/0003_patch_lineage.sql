-- Nexus KB Phase 0 patch parsing + lineage schema: tasks 9-15.

CREATE TABLE IF NOT EXISTS patch_series (
    id BIGSERIAL PRIMARY KEY,
    canonical_subject_norm TEXT NOT NULL,
    author_email TEXT NOT NULL,
    author_name TEXT,
    change_id TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    latest_version_id BIGINT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_patch_series_change_id_unique
    ON patch_series (change_id)
    WHERE change_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_patch_series_author_subject
    ON patch_series (author_email, canonical_subject_norm);

CREATE INDEX IF NOT EXISTS idx_patch_series_last_seen
    ON patch_series (last_seen_at DESC);

CREATE TABLE IF NOT EXISTS patch_series_lists (
    patch_series_id BIGINT NOT NULL REFERENCES patch_series(id) ON DELETE CASCADE,
    mailing_list_id BIGINT NOT NULL REFERENCES mailing_lists(id) ON DELETE CASCADE,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (patch_series_id, mailing_list_id)
);

CREATE INDEX IF NOT EXISTS idx_patch_series_lists_list
    ON patch_series_lists (mailing_list_id, last_seen_at DESC);

CREATE TABLE IF NOT EXISTS patch_series_versions (
    id BIGSERIAL PRIMARY KEY,
    patch_series_id BIGINT NOT NULL REFERENCES patch_series(id) ON DELETE CASCADE,
    version_num INTEGER NOT NULL DEFAULT 1,
    is_rfc BOOLEAN NOT NULL DEFAULT false,
    is_resend BOOLEAN NOT NULL DEFAULT false,
    is_partial_reroll BOOLEAN NOT NULL DEFAULT false,
    thread_id BIGINT,
    cover_message_pk BIGINT REFERENCES messages(id) ON DELETE SET NULL,
    first_patch_message_pk BIGINT REFERENCES messages(id) ON DELETE SET NULL,
    sent_at TIMESTAMPTZ NOT NULL,
    subject_raw TEXT NOT NULL,
    subject_norm TEXT NOT NULL,
    base_commit TEXT,
    version_fingerprint BYTEA NOT NULL,
    UNIQUE (patch_series_id, version_num, version_fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_patch_series_versions_by_series
    ON patch_series_versions (patch_series_id, version_num DESC, sent_at DESC);

CREATE INDEX IF NOT EXISTS idx_patch_series_versions_sent
    ON patch_series_versions (sent_at DESC);

CREATE TABLE IF NOT EXISTS patch_items (
    id BIGSERIAL PRIMARY KEY,
    patch_series_version_id BIGINT NOT NULL REFERENCES patch_series_versions(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    total INTEGER,
    message_pk BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    subject_raw TEXT NOT NULL,
    subject_norm TEXT NOT NULL,
    commit_subject TEXT,
    commit_subject_norm TEXT,
    commit_author_name TEXT,
    commit_author_email TEXT,
    item_type TEXT NOT NULL,
    has_diff BOOLEAN NOT NULL DEFAULT false,
    patch_id_stable TEXT,
    file_count INTEGER NOT NULL DEFAULT 0,
    additions INTEGER NOT NULL DEFAULT 0,
    deletions INTEGER NOT NULL DEFAULT 0,
    hunk_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE (patch_series_version_id, ordinal)
);

CREATE INDEX IF NOT EXISTS idx_patch_items_series_version
    ON patch_items (patch_series_version_id, ordinal);

CREATE INDEX IF NOT EXISTS idx_patch_items_patch_id
    ON patch_items (patch_id_stable);

CREATE INDEX IF NOT EXISTS idx_patch_items_message
    ON patch_items (message_pk);

CREATE TABLE IF NOT EXISTS patch_series_version_assembled_items (
    patch_series_version_id BIGINT NOT NULL REFERENCES patch_series_versions(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    patch_item_id BIGINT NOT NULL REFERENCES patch_items(id) ON DELETE CASCADE,
    inherited_from_version_num INTEGER,
    PRIMARY KEY (patch_series_version_id, ordinal)
);

CREATE INDEX IF NOT EXISTS idx_patch_series_assembled_items_patch_item
    ON patch_series_version_assembled_items (patch_item_id);

CREATE TABLE IF NOT EXISTS patch_logical (
    id BIGSERIAL PRIMARY KEY,
    patch_series_id BIGINT NOT NULL REFERENCES patch_series(id) ON DELETE CASCADE,
    slot INTEGER NOT NULL,
    title_norm TEXT NOT NULL,
    UNIQUE (patch_series_id, slot)
);

CREATE INDEX IF NOT EXISTS idx_patch_logical_series
    ON patch_logical (patch_series_id, slot);

CREATE TABLE IF NOT EXISTS patch_logical_versions (
    patch_logical_id BIGINT NOT NULL REFERENCES patch_logical(id) ON DELETE CASCADE,
    patch_item_id BIGINT NOT NULL REFERENCES patch_items(id) ON DELETE CASCADE,
    version_num INTEGER NOT NULL,
    PRIMARY KEY (patch_logical_id, version_num)
);

CREATE INDEX IF NOT EXISTS idx_patch_logical_versions_patch_item
    ON patch_logical_versions (patch_item_id);
