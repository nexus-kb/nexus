-- Nexus KB Phase 0 threading schema: tasks 6-8.

CREATE TABLE IF NOT EXISTS threads (
    id BIGSERIAL NOT NULL,
    mailing_list_id BIGINT NOT NULL REFERENCES mailing_lists(id) ON DELETE CASCADE,
    root_node_key TEXT NOT NULL,
    root_message_pk BIGINT REFERENCES messages(id) ON DELETE SET NULL,
    subject_norm TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    last_activity_at TIMESTAMPTZ NOT NULL,
    message_count INT NOT NULL,
    membership_hash BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (mailing_list_id, id),
    UNIQUE (mailing_list_id, root_node_key)
) PARTITION BY LIST (mailing_list_id);

CREATE TABLE IF NOT EXISTS thread_nodes (
    mailing_list_id BIGINT NOT NULL REFERENCES mailing_lists(id) ON DELETE CASCADE,
    thread_id BIGINT NOT NULL,
    node_key TEXT NOT NULL,
    message_pk BIGINT REFERENCES messages(id) ON DELETE SET NULL,
    parent_node_key TEXT,
    depth INT NOT NULL,
    sort_key BYTEA NOT NULL,
    is_dummy BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (mailing_list_id, thread_id, node_key),
    FOREIGN KEY (mailing_list_id, thread_id)
        REFERENCES threads(mailing_list_id, id)
        ON DELETE CASCADE
) PARTITION BY LIST (mailing_list_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_thread_nodes_unique_message
    ON thread_nodes (mailing_list_id, thread_id, message_pk)
    WHERE message_pk IS NOT NULL;

CREATE TABLE IF NOT EXISTS thread_messages (
    mailing_list_id BIGINT NOT NULL REFERENCES mailing_lists(id) ON DELETE CASCADE,
    thread_id BIGINT NOT NULL,
    message_pk BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    parent_message_pk BIGINT REFERENCES messages(id) ON DELETE SET NULL,
    depth INT NOT NULL,
    sort_key BYTEA NOT NULL,
    is_dummy BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (mailing_list_id, thread_id, message_pk),
    FOREIGN KEY (mailing_list_id, thread_id)
        REFERENCES threads(mailing_list_id, id)
        ON DELETE CASCADE
) PARTITION BY LIST (mailing_list_id);

CREATE TABLE IF NOT EXISTS threads_default PARTITION OF threads DEFAULT;
CREATE TABLE IF NOT EXISTS thread_nodes_default PARTITION OF thread_nodes DEFAULT;
CREATE TABLE IF NOT EXISTS thread_messages_default PARTITION OF thread_messages DEFAULT;

CREATE INDEX IF NOT EXISTS idx_threads_default_activity
    ON threads_default (mailing_list_id, last_activity_at DESC);
CREATE INDEX IF NOT EXISTS idx_thread_nodes_default_sort
    ON thread_nodes_default (mailing_list_id, thread_id, sort_key);
CREATE INDEX IF NOT EXISTS idx_thread_nodes_default_message
    ON thread_nodes_default (mailing_list_id, message_pk);
CREATE INDEX IF NOT EXISTS idx_thread_messages_default_sort
    ON thread_messages_default (mailing_list_id, thread_id, sort_key);
CREATE INDEX IF NOT EXISTS idx_thread_messages_default_message
    ON thread_messages_default (mailing_list_id, message_pk);

CREATE INDEX IF NOT EXISTS idx_threads_activity
    ON threads (mailing_list_id, last_activity_at DESC);
CREATE INDEX IF NOT EXISTS idx_thread_nodes_sort
    ON thread_nodes (mailing_list_id, thread_id, sort_key);
CREATE INDEX IF NOT EXISTS idx_thread_nodes_message
    ON thread_nodes (mailing_list_id, message_pk);
CREATE INDEX IF NOT EXISTS idx_thread_messages_sort
    ON thread_messages (mailing_list_id, thread_id, sort_key);
CREATE INDEX IF NOT EXISTS idx_thread_messages_message
    ON thread_messages (mailing_list_id, message_pk);

CREATE INDEX IF NOT EXISTS idx_messages_references_ids_gin
    ON messages USING GIN (references_ids);
CREATE INDEX IF NOT EXISTS idx_messages_in_reply_to_ids_gin
    ON messages USING GIN (in_reply_to_ids);

CREATE INDEX IF NOT EXISTS idx_lmi_default_message_pk
    ON list_message_instances_default (message_pk);

CREATE OR REPLACE FUNCTION ensure_threads_partition(target_list_id BIGINT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    partition_name TEXT := format('threads_%s', target_list_id);
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF threads FOR VALUES IN (%s)',
        partition_name,
        target_list_id
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %I (mailing_list_id, last_activity_at DESC)',
        partition_name || '_activity_idx',
        partition_name
    );
END;
$$;

CREATE OR REPLACE FUNCTION ensure_thread_nodes_partition(target_list_id BIGINT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    partition_name TEXT := format('thread_nodes_%s', target_list_id);
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF thread_nodes FOR VALUES IN (%s)',
        partition_name,
        target_list_id
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %I (mailing_list_id, thread_id, sort_key)',
        partition_name || '_sort_idx',
        partition_name
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %I (mailing_list_id, message_pk)',
        partition_name || '_message_idx',
        partition_name
    );

    EXECUTE format(
        'CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (mailing_list_id, thread_id, message_pk) WHERE message_pk IS NOT NULL',
        partition_name || '_unique_message_idx',
        partition_name
    );
END;
$$;

CREATE OR REPLACE FUNCTION ensure_thread_messages_partition(target_list_id BIGINT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    partition_name TEXT := format('thread_messages_%s', target_list_id);
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF thread_messages FOR VALUES IN (%s)',
        partition_name,
        target_list_id
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %I (mailing_list_id, thread_id, sort_key)',
        partition_name || '_sort_idx',
        partition_name
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %I (mailing_list_id, message_pk)',
        partition_name || '_message_idx',
        partition_name
    );
END;
$$;

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

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %I (message_pk)',
        partition_name || '_message_pk_idx',
        partition_name
    );
END;
$$;
