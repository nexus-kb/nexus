-- Track sync progress per mailing list epoch
ALTER TABLE mailing_list_epochs
    ADD COLUMN IF NOT EXISTS last_indexed_commit TEXT,
    ADD COLUMN IF NOT EXISTS last_indexed_at TIMESTAMPTZ;
