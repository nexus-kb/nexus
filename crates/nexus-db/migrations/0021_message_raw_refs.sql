-- Store canonical raw-message location in mirrored git instead of DB blob payload.

CREATE TABLE IF NOT EXISTS message_raw_refs (
    message_pk BIGINT PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
    repo_id BIGINT NOT NULL REFERENCES mailing_list_repos(id) ON DELETE RESTRICT,
    git_commit_oid TEXT NOT NULL,
    blob_path TEXT NOT NULL DEFAULT 'm',
    raw_sha256 BYTEA,
    raw_size_bytes INTEGER,
    last_verified_at TIMESTAMPTZ,
    last_verify_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (blob_path = 'm')
);

CREATE INDEX IF NOT EXISTS idx_message_raw_refs_repo_commit
    ON message_raw_refs (repo_id, git_commit_oid);

DROP TRIGGER IF EXISTS message_raw_refs_touch_updated_at ON message_raw_refs;
CREATE TRIGGER message_raw_refs_touch_updated_at
BEFORE UPDATE ON message_raw_refs
FOR EACH ROW EXECUTE FUNCTION jobs_set_updated_at();
