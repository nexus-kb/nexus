CREATE TABLE IF NOT EXISTS message_patch_facts (
    message_pk BIGINT PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
    has_diff BOOLEAN NOT NULL DEFAULT FALSE,
    patch_id_stable TEXT NULL,
    base_commit TEXT NULL,
    change_id TEXT NULL,
    file_count INT NOT NULL DEFAULT 0,
    additions INT NOT NULL DEFAULT 0,
    deletions INT NOT NULL DEFAULT 0,
    hunk_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_message_patch_facts_patch_id
    ON message_patch_facts (patch_id_stable)
    WHERE patch_id_stable IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_message_patch_facts_change_id
    ON message_patch_facts (change_id)
    WHERE change_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS message_patch_file_facts (
    message_pk BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    old_path TEXT NULL,
    new_path TEXT NOT NULL,
    change_type TEXT NOT NULL,
    is_binary BOOLEAN NOT NULL DEFAULT FALSE,
    additions INT NOT NULL DEFAULT 0,
    deletions INT NOT NULL DEFAULT 0,
    hunk_count INT NOT NULL DEFAULT 0,
    diff_start INT NOT NULL DEFAULT 0,
    diff_end INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (message_pk, new_path)
);

CREATE INDEX IF NOT EXISTS idx_message_patch_file_facts_message_pk
    ON message_patch_file_facts (message_pk);
