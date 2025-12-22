-- Mailing list core tables and partitioned email storage
-- Assumptions: targeting Postgres 15+ (supports partitioned PKs, LIST partition default)

-- Core mailing lists
CREATE TABLE mailing_lists (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    description TEXT,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_synced_at TIMESTAMPTZ,
    last_threaded_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_mailing_lists_slug ON mailing_lists(slug);
CREATE INDEX IF NOT EXISTS idx_mailing_lists_enabled ON mailing_lists(enabled);

-- Repositories per list
CREATE TABLE mailing_list_repositories (
    id SERIAL PRIMARY KEY,
    mailing_list_id INTEGER REFERENCES mailing_lists(id) ON DELETE CASCADE,
    repo_url TEXT NOT NULL,
    repo_order INTEGER NOT NULL DEFAULT 0,
    last_indexed_commit TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (mailing_list_id, repo_order)
);

-- Authors
CREATE TABLE authors (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    name TEXT,
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_seen TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE author_mailing_list_activity (
    author_id INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    mailing_list_id INTEGER NOT NULL REFERENCES mailing_lists(id) ON DELETE CASCADE,
    first_email_date TIMESTAMPTZ,
    last_email_date TIMESTAMPTZ,
    created_count BIGINT DEFAULT 0,
    participated_count BIGINT DEFAULT 0,
    PRIMARY KEY (author_id, mailing_list_id)
);

-- Partitioned emails table (LIST partitioned on mailing_list_id)
CREATE TABLE emails (
    id SERIAL,
    mailing_list_id INTEGER NOT NULL,
    message_id TEXT NOT NULL,
    git_commit_hash TEXT NOT NULL,
    author_id INTEGER NOT NULL,
    subject TEXT NOT NULL,
    normalized_subject TEXT,
    series_id TEXT,
    date TIMESTAMPTZ NOT NULL,
    in_reply_to TEXT,
    body TEXT,
    epoch INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    patch_metadata JSONB,
    threaded_at TIMESTAMPTZ,
    PRIMARY KEY (id, mailing_list_id),
    UNIQUE (mailing_list_id, message_id),
    UNIQUE (mailing_list_id, git_commit_hash)
) PARTITION BY LIST (mailing_list_id);

CREATE INDEX idx_emails_author_id ON emails(author_id);
CREATE INDEX idx_emails_date ON emails(date);
CREATE INDEX idx_emails_in_reply_to ON emails(in_reply_to);
CREATE INDEX idx_emails_normalized_subject ON emails(normalized_subject);
CREATE INDEX idx_emails_series_id ON emails(series_id);
CREATE INDEX idx_emails_threaded_at ON emails(threaded_at);
CREATE INDEX idx_emails_unthreaded ON emails(id) WHERE threaded_at IS NULL;

CREATE TABLE emails_default PARTITION OF emails DEFAULT;

-- Partitioned threads table
CREATE TABLE threads (
    id SERIAL,
    mailing_list_id INTEGER NOT NULL,
    root_message_id TEXT NOT NULL,
    subject TEXT NOT NULL,
    start_date TIMESTAMPTZ NOT NULL,
    last_date TIMESTAMPTZ NOT NULL,
    message_count INTEGER DEFAULT 0,
    membership_hash BYTEA,
    PRIMARY KEY (id, mailing_list_id),
    UNIQUE (mailing_list_id, root_message_id)
) PARTITION BY LIST (mailing_list_id);

CREATE INDEX idx_threads_start_date ON threads(start_date);
CREATE INDEX idx_threads_last_date ON threads(last_date);
CREATE INDEX idx_threads_message_count ON threads(message_count);

CREATE TABLE threads_default PARTITION OF threads DEFAULT;

-- Partitioned email recipients
CREATE TABLE email_recipients (
    id SERIAL,
    mailing_list_id INTEGER NOT NULL,
    email_id INTEGER NOT NULL,
    author_id INTEGER NOT NULL,
    recipient_type TEXT CHECK (recipient_type IN ('to', 'cc')),
    PRIMARY KEY (id, mailing_list_id)
) PARTITION BY LIST (mailing_list_id);

CREATE INDEX idx_email_recipients_email_id ON email_recipients(email_id);
CREATE INDEX idx_email_recipients_author_id ON email_recipients(author_id);

CREATE TABLE email_recipients_default PARTITION OF email_recipients DEFAULT;

-- Partitioned email references
CREATE TABLE email_references (
    mailing_list_id INTEGER NOT NULL,
    email_id INTEGER NOT NULL,
    referenced_message_id TEXT NOT NULL,
    position INTEGER NOT NULL,
    PRIMARY KEY (mailing_list_id, email_id, referenced_message_id)
) PARTITION BY LIST (mailing_list_id);

CREATE INDEX idx_email_references_email_id ON email_references(email_id);
CREATE INDEX idx_email_references_ref_msg_id ON email_references(referenced_message_id);

CREATE TABLE email_references_default PARTITION OF email_references DEFAULT;

-- Partitioned thread memberships
CREATE TABLE thread_memberships (
    mailing_list_id INTEGER NOT NULL,
    thread_id INTEGER NOT NULL,
    email_id INTEGER NOT NULL,
    depth INTEGER DEFAULT 0,
    PRIMARY KEY (mailing_list_id, thread_id, email_id)
) PARTITION BY LIST (mailing_list_id);

CREATE INDEX idx_thread_memberships_thread_id ON thread_memberships(thread_id);
CREATE INDEX idx_thread_memberships_email_id ON thread_memberships(email_id);

CREATE TABLE thread_memberships_default PARTITION OF thread_memberships DEFAULT;
