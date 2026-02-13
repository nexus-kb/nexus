-- Nexus KB Phase 0 baseline schema: ingestion + job engine.

CREATE TABLE IF NOT EXISTS mailing_lists (
    id BIGSERIAL PRIMARY KEY,
    list_key TEXT NOT NULL UNIQUE,
    posting_address TEXT,
    description TEXT,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mailing_list_repos (
    id BIGSERIAL PRIMARY KEY,
    mailing_list_id BIGINT NOT NULL REFERENCES mailing_lists(id) ON DELETE CASCADE,
    repo_key TEXT NOT NULL,
    repo_relpath TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (mailing_list_id, repo_key)
);

CREATE TABLE IF NOT EXISTS repo_watermarks (
    repo_id BIGINT PRIMARY KEY REFERENCES mailing_list_repos(id) ON DELETE CASCADE,
    last_indexed_commit_oid TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS message_bodies (
    id BIGSERIAL PRIMARY KEY,
    raw_rfc822 BYTEA NOT NULL,
    body_text TEXT,
    diff_text TEXT,
    search_text TEXT NOT NULL,
    has_diff BOOLEAN NOT NULL DEFAULT false,
    has_attachments BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS messages (
    id BIGSERIAL PRIMARY KEY,
    content_hash_sha256 BYTEA NOT NULL UNIQUE,
    subject_raw TEXT NOT NULL,
    subject_norm TEXT NOT NULL,
    from_name TEXT,
    from_email TEXT NOT NULL,
    date_utc TIMESTAMPTZ,
    to_raw TEXT,
    cc_raw TEXT,
    message_ids TEXT[] NOT NULL,
    message_id_primary TEXT NOT NULL,
    in_reply_to_ids TEXT[] NOT NULL DEFAULT '{}',
    references_ids TEXT[] NOT NULL DEFAULT '{}',
    mime_type TEXT,
    body_id BIGINT NOT NULL REFERENCES message_bodies(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_messages_from_email ON messages(from_email);
CREATE INDEX IF NOT EXISTS idx_messages_date_utc ON messages(date_utc DESC);
CREATE INDEX IF NOT EXISTS idx_messages_subject_norm ON messages(subject_norm);

CREATE TABLE IF NOT EXISTS message_id_map (
    message_id TEXT NOT NULL,
    message_pk BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    is_primary BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (message_id, message_pk)
);

CREATE INDEX IF NOT EXISTS idx_message_id_map_message_id ON message_id_map(message_id);

CREATE TABLE IF NOT EXISTS list_message_instances (
    mailing_list_id BIGINT NOT NULL REFERENCES mailing_lists(id) ON DELETE CASCADE,
    message_pk BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    repo_id BIGINT NOT NULL REFERENCES mailing_list_repos(id) ON DELETE CASCADE,
    git_commit_oid TEXT NOT NULL,
    seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (mailing_list_id, repo_id, git_commit_oid, message_pk),
    UNIQUE (mailing_list_id, git_commit_oid)
) PARTITION BY LIST (mailing_list_id);

CREATE TABLE IF NOT EXISTS list_message_instances_default PARTITION OF list_message_instances DEFAULT;
CREATE INDEX IF NOT EXISTS idx_lmi_default_seen_at ON list_message_instances_default (seen_at DESC);

CREATE OR REPLACE FUNCTION ensure_list_message_instances_partition(target_list_id BIGINT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    partition_name TEXT := format('list_message_instances_%s', target_list_id);
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF list_message_instances FOR VALUES IN (%s)',
        partition_name,
        target_list_id
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %I (seen_at DESC)',
        partition_name || '_seen_at_idx',
        partition_name
    );
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_state') THEN
        CREATE TYPE job_state AS ENUM (
          'queued',
          'scheduled',
          'running',
          'succeeded',
          'failed_retryable',
          'failed_terminal',
          'cancelled'
        );
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS jobs (
    id BIGSERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,
    state job_state NOT NULL DEFAULT 'queued',
    priority INT NOT NULL DEFAULT 0,
    run_after TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_by TEXT,
    lease_until TIMESTAMPTZ,
    dedupe_key TEXT,
    dedupe_scope TEXT NOT NULL DEFAULT 'global',
    attempt INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 8,
    last_error TEXT,
    last_error_kind TEXT,
    cancel_requested BOOLEAN NOT NULL DEFAULT false,
    payload_json JSONB NOT NULL,
    result_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_type, dedupe_scope, dedupe_key)
);

CREATE TABLE IF NOT EXISTS job_attempts (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt INT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    error TEXT,
    metrics_json JSONB
);

CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(state, priority DESC, run_after ASC, id ASC);
CREATE INDEX IF NOT EXISTS idx_jobs_type_state ON jobs(job_type, state);
CREATE INDEX IF NOT EXISTS idx_job_attempts_job_attempt ON job_attempts(job_id, attempt);

CREATE OR REPLACE FUNCTION jobs_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS jobs_touch_updated_at ON jobs;
CREATE TRIGGER jobs_touch_updated_at
BEFORE UPDATE ON jobs
FOR EACH ROW EXECUTE FUNCTION jobs_set_updated_at();
