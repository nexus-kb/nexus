-- Refine mailing list schema for public-inbox shards and lean email rows
-- Assumes previous migrations created base tables with no data yet.

-- Per-epoch metadata from manifest.js
CREATE TABLE mailing_list_epochs (
    id SERIAL PRIMARY KEY,
    mailing_list_id INTEGER NOT NULL REFERENCES mailing_lists(id) ON DELETE CASCADE,
    epoch SMALLINT NOT NULL,
    repo_relpath TEXT NOT NULL,        -- e.g., git/0.git
    fingerprint TEXT,
    modified_at TIMESTAMPTZ,
    reference_epoch SMALLINT,
    UNIQUE (mailing_list_id, epoch)
);

-- Emails: add epoch + blob_oid, drop git_commit_hash/body, adjust uniqueness
ALTER TABLE emails
    ADD COLUMN IF NOT EXISTS epoch SMALLINT NOT NULL DEFAULT 0;

ALTER TABLE emails
    ADD COLUMN IF NOT EXISTS blob_oid TEXT NOT NULL DEFAULT '';

-- Drop old unique on git_commit_hash if it exists
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'emails_mailing_list_id_git_commit_hash_key'
    ) THEN
        ALTER TABLE emails DROP CONSTRAINT emails_mailing_list_id_git_commit_hash_key;
    END IF;
END$$;

ALTER TABLE emails DROP COLUMN IF EXISTS git_commit_hash;
ALTER TABLE emails DROP COLUMN IF EXISTS body;

-- New uniqueness on blob_oid within a mailing list
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'emails_mailing_list_id_blob_oid_key'
    ) THEN
        ALTER TABLE emails
            ADD CONSTRAINT emails_mailing_list_id_blob_oid_key UNIQUE (mailing_list_id, blob_oid);
    END IF;
END$$;

-- Remove temporary defaults so future inserts must supply values
ALTER TABLE emails ALTER COLUMN blob_oid DROP DEFAULT;
ALTER TABLE emails ALTER COLUMN epoch DROP DEFAULT;

-- Thread table: remove unused membership hash
ALTER TABLE threads DROP COLUMN IF EXISTS membership_hash;

-- Externalized bodies per mailing list partition
CREATE TABLE email_bodies (
    email_id INTEGER NOT NULL,
    mailing_list_id INTEGER NOT NULL,
    body TEXT,
    PRIMARY KEY (email_id, mailing_list_id)
) PARTITION BY LIST (mailing_list_id);

CREATE TABLE email_bodies_default PARTITION OF email_bodies DEFAULT;

-- FK to partitioned emails; order must match referenced PK (id, mailing_list_id)
ALTER TABLE email_bodies
    ADD CONSTRAINT email_bodies_email_fk
    FOREIGN KEY (email_id, mailing_list_id)
    REFERENCES emails(id, mailing_list_id)
    ON DELETE CASCADE;
