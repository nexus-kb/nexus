-- Staging tables for bulk mail ingestion
CREATE UNLOGGED TABLE IF NOT EXISTS staging_emails (
    mailing_list_id INTEGER NOT NULL,
    message_id TEXT NOT NULL,
    blob_oid TEXT NOT NULL,
    epoch SMALLINT NOT NULL,
    author_email TEXT NOT NULL,
    author_name TEXT,
    subject TEXT NOT NULL,
    normalized_subject TEXT,
    date TIMESTAMPTZ NOT NULL,
    in_reply_to TEXT,
    body TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_emails_list_message
    ON staging_emails (mailing_list_id, message_id);

CREATE UNLOGGED TABLE IF NOT EXISTS staging_recipients (
    mailing_list_id INTEGER NOT NULL,
    message_id TEXT NOT NULL,
    recipient_type TEXT NOT NULL,
    email TEXT NOT NULL,
    name TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_recipients_list_message
    ON staging_recipients (mailing_list_id, message_id);

CREATE UNLOGGED TABLE IF NOT EXISTS staging_references (
    mailing_list_id INTEGER NOT NULL,
    message_id TEXT NOT NULL,
    referenced_message_id TEXT NOT NULL,
    position INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_staging_references_list_message
    ON staging_references (mailing_list_id, message_id);
