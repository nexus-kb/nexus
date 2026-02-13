-- Nexus KB Phase 0 diff metadata schema: tasks 16-20.

CREATE TABLE IF NOT EXISTS patch_item_files (
    patch_item_id BIGINT NOT NULL REFERENCES patch_items(id) ON DELETE CASCADE,
    old_path TEXT,
    new_path TEXT NOT NULL,
    change_type TEXT NOT NULL,
    is_binary BOOLEAN NOT NULL DEFAULT false,
    additions INTEGER NOT NULL DEFAULT 0,
    deletions INTEGER NOT NULL DEFAULT 0,
    hunk_count INTEGER NOT NULL DEFAULT 0,
    diff_start INTEGER NOT NULL,
    diff_end INTEGER NOT NULL,
    PRIMARY KEY (patch_item_id, new_path),
    CHECK (change_type IN ('A', 'M', 'D', 'R', 'C', 'B')),
    CHECK (diff_start >= 0),
    CHECK (diff_end >= diff_start)
);

CREATE INDEX IF NOT EXISTS idx_patch_item_files_path
    ON patch_item_files (new_path);

CREATE INDEX IF NOT EXISTS idx_patch_item_files_patch_item
    ON patch_item_files (patch_item_id, new_path);
