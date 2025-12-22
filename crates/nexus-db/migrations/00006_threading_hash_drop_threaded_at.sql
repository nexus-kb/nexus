-- Reintroduce thread membership hashing and drop per-email threaded marker.

ALTER TABLE threads
    ADD COLUMN IF NOT EXISTS membership_hash BYTEA;

DROP INDEX IF EXISTS idx_emails_threaded_at;
DROP INDEX IF EXISTS idx_emails_unthreaded;

ALTER TABLE emails
    DROP COLUMN IF EXISTS threaded_at;
