-- Improve ingest-window -> thread join scans by making seen_at filtering and message_pk
-- join key available from one index path.
CREATE INDEX IF NOT EXISTS idx_lmi_seen_at_message_pk
    ON list_message_instances (mailing_list_id, seen_at DESC, message_pk);
