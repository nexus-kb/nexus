-- Mainline merge detection state, matches, and series rollups.

CREATE TABLE IF NOT EXISTS mainline_scan_state (
    repo_key TEXT PRIMARY KEY,
    bootstrap_completed_at TIMESTAMPTZ,
    last_scanned_commit_oid TEXT,
    last_scanned_head_oid TEXT,
    public_filter_ready BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mainline_scan_runs (
    id BIGSERIAL PRIMARY KEY,
    repo_key TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('bootstrap', 'incremental')),
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    job_id BIGINT REFERENCES jobs(id) ON DELETE SET NULL,
    head_commit_oid TEXT,
    cursor_commit_oid TEXT,
    scanned_commits BIGINT NOT NULL DEFAULT 0,
    matched_commits BIGINT NOT NULL DEFAULT 0,
    matched_patch_items BIGINT NOT NULL DEFAULT 0,
    updated_series BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    progress_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_mainline_scan_runs_state_created
    ON mainline_scan_runs (state, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_mainline_scan_runs_repo_created
    ON mainline_scan_runs (repo_key, created_at DESC, id DESC);

DROP TRIGGER IF EXISTS mainline_scan_runs_touch_updated_at ON mainline_scan_runs;
CREATE TRIGGER mainline_scan_runs_touch_updated_at
BEFORE UPDATE ON mainline_scan_runs
FOR EACH ROW EXECUTE FUNCTION jobs_set_updated_at();

CREATE TABLE IF NOT EXISTS mainline_commits (
    commit_oid TEXT PRIMARY KEY,
    subject TEXT NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL,
    first_containing_tag TEXT,
    first_final_release TEXT,
    first_tag_sort_key BIGINT,
    first_release_sort_key BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_mainline_commits_release_sort
    ON mainline_commits (first_release_sort_key DESC, first_tag_sort_key DESC, commit_oid);

CREATE TABLE IF NOT EXISTS patch_item_mainline_matches (
    patch_item_id BIGINT NOT NULL REFERENCES patch_items(id) ON DELETE CASCADE,
    commit_oid TEXT NOT NULL REFERENCES mainline_commits(commit_oid) ON DELETE CASCADE,
    match_method TEXT NOT NULL CHECK (match_method IN ('link', 'patch_id')),
    matched_message_id TEXT,
    is_canonical BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (patch_item_id, commit_oid)
);

CREATE INDEX IF NOT EXISTS idx_patch_item_mainline_matches_commit
    ON patch_item_mainline_matches (commit_oid, patch_item_id);

CREATE INDEX IF NOT EXISTS idx_patch_item_mainline_matches_patch_item
    ON patch_item_mainline_matches (patch_item_id, is_canonical DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_patch_item_mainline_matches_one_canonical
    ON patch_item_mainline_matches (patch_item_id)
    WHERE is_canonical;

ALTER TABLE patch_series_versions
ADD COLUMN IF NOT EXISTS mainline_merge_state TEXT NOT NULL DEFAULT 'unknown',
ADD COLUMN IF NOT EXISTS mainline_matched_patch_count INTEGER NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS mainline_total_patch_count INTEGER NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS mainline_merged_in_tag TEXT,
ADD COLUMN IF NOT EXISTS mainline_merged_in_release TEXT,
ADD COLUMN IF NOT EXISTS mainline_single_patch_commit_oid TEXT;

ALTER TABLE patch_series_versions
DROP CONSTRAINT IF EXISTS patch_series_versions_mainline_merge_state_check;

ALTER TABLE patch_series_versions
ADD CONSTRAINT patch_series_versions_mainline_merge_state_check
CHECK (mainline_merge_state IN ('unknown', 'unmerged', 'partial', 'merged'));

ALTER TABLE patch_series
ADD COLUMN IF NOT EXISTS mainline_merge_state TEXT NOT NULL DEFAULT 'unknown',
ADD COLUMN IF NOT EXISTS mainline_matched_patch_count INTEGER NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS mainline_total_patch_count INTEGER NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS mainline_merged_in_tag TEXT,
ADD COLUMN IF NOT EXISTS mainline_merged_in_release TEXT,
ADD COLUMN IF NOT EXISTS mainline_single_patch_commit_oid TEXT,
ADD COLUMN IF NOT EXISTS mainline_merged_version_id BIGINT;

ALTER TABLE patch_series
DROP CONSTRAINT IF EXISTS patch_series_mainline_merge_state_check;

ALTER TABLE patch_series
ADD CONSTRAINT patch_series_mainline_merge_state_check
CHECK (mainline_merge_state IN ('unknown', 'unmerged', 'partial', 'merged'));

ALTER TABLE patch_series
DROP CONSTRAINT IF EXISTS patch_series_mainline_merged_version_fk;

ALTER TABLE patch_series
ADD CONSTRAINT patch_series_mainline_merged_version_fk
FOREIGN KEY (mainline_merged_version_id, id)
REFERENCES patch_series_versions (id, patch_series_id)
ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_patch_series_mainline_merge_state_last_seen
    ON patch_series (mainline_merge_state, last_seen_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_patch_series_versions_mainline_merge_state_sent
    ON patch_series_versions (mainline_merge_state, sent_at DESC, id DESC);
