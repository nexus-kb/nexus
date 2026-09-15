-- One BM25 document per thread: root plus two reply levels.
CREATE EXTENSION IF NOT EXISTS pg_search CASCADE;

CREATE TABLE thread_search_documents (
    thread_id       bigint PRIMARY KEY
                        REFERENCES threads(id) ON DELETE CASCADE,
    subject         text NOT NULL,
    content         text NOT NULL
);

CREATE FUNCTION search_thread_message_text(
    input_body text
)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    source_lines text[];
    source_line text;
    next_line text;
    output_lines text[] := ARRAY[]::text[];
    line_number integer;
BEGIN
    source_lines := regexp_split_to_array(
        replace(input_body, E'\r\n', E'\n'),
        E'\n'
    );

    FOR line_number IN
        1..COALESCE(array_length(source_lines, 1), 0)
    LOOP
        source_line := source_lines[line_number];
        next_line := source_lines[line_number + 1];

        -- Replies retain ancestor text as lines prefixed with one or more
        -- '>' characters. That text is already represented by its source
        -- message and must not affect this thread document again.
        IF source_line ~ '^[[:space:]]*>' THEN
            CONTINUE;
        END IF;

        -- Drop a reply attribution only when the next line proves that it
        -- introduces a quoted block.
        IF next_line ~ '^[[:space:]]*>'
           AND source_line ~*
                '(wrote|writes|schrieb|écrit|escribió|ha scritto|pisze)[[:space:]]*:[[:space:]]*$'
        THEN
            CONTINUE;
        END IF;

        -- These clients quote the complete original message without adding
        -- a prefix to each line. Everything after the marker is old text.
        IF source_line ~*
            '^[[:space:]]*[-_]{2,}[[:space:]]*(Original Message|Forwarded message)[[:space:]]*[-_]{2,}[[:space:]]*$'
        THEN
            EXIT;
        END IF;

        -- Keep cover-letter prose, patch commit messages, and discussion,
        -- but stop before the patch payload. Paired old/new file markers
        -- cover traditional unified diffs without treating a mail separator
        -- line containing only "---" as a diff.
        IF source_line ~ '^diff --git[[:space:]]'
           OR source_line ~ '^Index:[[:space:]]'
           OR source_line ~ '^GIT binary patch[[:space:]]*$'
           OR (
                source_line ~ '^---[[:space:]]'
                AND next_line ~ '^\+\+\+[[:space:]]'
           )
           OR (
                source_line ~ '^\*\*\*[[:space:]]'
                AND next_line ~ '^---[[:space:]]'
           )
        THEN
            EXIT;
        END IF;

        output_lines := array_append(
            output_lines,
            source_line
        );
    END LOOP;

    RETURN btrim(
        array_to_string(output_lines, E'\n'),
        E'\n\r'
    );
END;
$$;

CREATE FUNCTION refresh_thread_search_documents(
    target_thread_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count bigint;
BEGIN
    IF COALESCE(array_length(target_thread_ids, 1), 0) = 0 THEN
        RETURN 0;
    END IF;

    DELETE FROM thread_search_documents
    WHERE thread_id = ANY(target_thread_ids);

    INSERT INTO thread_search_documents (
        thread_id,
        subject,
        content
    )
    WITH RECURSIVE selected_messages AS (
        SELECT
            thread.id AS thread_id,
            root.id AS message_database_id,
            root.message_id,
            root.subject AS root_subject,
            root.sent_at,
            root.body,
            0 AS reply_depth
        FROM threads AS thread
        JOIN messages AS root
          ON root.message_id = thread.root_message_id
        WHERE thread.id = ANY(target_thread_ids)
          AND NOT root.is_placeholder

        UNION ALL

        SELECT
            parent.thread_id,
            child.id,
            child.message_id,
            parent.root_subject,
            child.sent_at,
            child.body,
            parent.reply_depth + 1
        FROM selected_messages AS parent
        JOIN messages AS child
          ON child.thread_id = parent.thread_id
         AND child.in_reply_to = parent.message_id
        WHERE parent.reply_depth < 2
          AND NOT child.is_placeholder
    )
    SELECT
        selected.thread_id,
        COALESCE(
            max(selected.root_subject)
                FILTER (WHERE selected.reply_depth = 0),
            ''
        ),
        COALESCE(
            string_agg(
                NULLIF(
                    search_thread_message_text(
                        selected.body
                    ),
                    ''
                ),
                E'\n\n'
                ORDER BY
                    selected.reply_depth,
                    selected.sent_at NULLS LAST,
                    selected.message_database_id
            ),
            ''
        )
    FROM selected_messages AS selected
    GROUP BY selected.thread_id
    ORDER BY selected.thread_id;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$$;

CREATE FUNCTION sync_thread_search_documents_after_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_thread_ids bigint[];
BEGIN
    SELECT array_agg(DISTINCT thread_id)
    INTO target_thread_ids
    FROM inserted_messages;

    PERFORM refresh_thread_search_documents(
        target_thread_ids
    );
    RETURN NULL;
END;
$$;

CREATE FUNCTION sync_thread_search_documents_after_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_thread_ids bigint[];
BEGIN
    SELECT array_agg(thread_id)
    INTO target_thread_ids
    FROM (
        SELECT thread_id
        FROM old_messages
        UNION
        SELECT thread_id
        FROM updated_messages
    ) AS affected_threads;

    PERFORM refresh_thread_search_documents(
        target_thread_ids
    );
    RETURN NULL;
END;
$$;

CREATE FUNCTION sync_thread_search_documents_after_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_thread_ids bigint[];
BEGIN
    SELECT array_agg(DISTINCT thread_id)
    INTO target_thread_ids
    FROM deleted_messages;

    PERFORM refresh_thread_search_documents(
        target_thread_ids
    );
    RETURN NULL;
END;
$$;

CREATE FUNCTION sync_thread_search_document_for_thread()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM refresh_thread_search_documents(
        ARRAY[NEW.id]
    );
    RETURN NULL;
END;
$$;

CREATE TRIGGER messages_sync_thread_search_after_insert
AFTER INSERT ON messages
REFERENCING NEW TABLE AS inserted_messages
FOR EACH STATEMENT
EXECUTE FUNCTION sync_thread_search_documents_after_insert();

CREATE TRIGGER messages_sync_thread_search_after_update
AFTER UPDATE ON messages
REFERENCING
    OLD TABLE AS old_messages
    NEW TABLE AS updated_messages
FOR EACH STATEMENT
EXECUTE FUNCTION sync_thread_search_documents_after_update();

CREATE TRIGGER messages_sync_thread_search_after_delete
AFTER DELETE ON messages
REFERENCING OLD TABLE AS deleted_messages
FOR EACH STATEMENT
EXECUTE FUNCTION sync_thread_search_documents_after_delete();

CREATE TRIGGER threads_sync_thread_search_after_root_update
AFTER UPDATE OF root_message_id ON threads
FOR EACH ROW
EXECUTE FUNCTION sync_thread_search_document_for_thread();
