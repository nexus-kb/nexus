-- Backfill one canonical raw reference per message from earliest seen commit instance.

INSERT INTO message_raw_refs (message_pk, repo_id, git_commit_oid, blob_path)
SELECT selected.message_pk, selected.repo_id, selected.git_commit_oid, 'm'
FROM (
    SELECT DISTINCT ON (lmi.message_pk)
        lmi.message_pk,
        lmi.repo_id,
        lmi.git_commit_oid
    FROM list_message_instances lmi
    ORDER BY lmi.message_pk, lmi.seen_at ASC, lmi.git_commit_oid ASC
) AS selected
ON CONFLICT (message_pk) DO NOTHING;

DO $$
DECLARE
    total_messages BIGINT;
    total_refs BIGINT;
BEGIN
    SELECT COUNT(*) INTO total_messages FROM messages;
    SELECT COUNT(*) INTO total_refs FROM message_raw_refs;

    IF total_messages <> total_refs THEN
        RAISE EXCEPTION 'message_raw_refs coverage mismatch: messages=% refs=%', total_messages, total_refs;
    END IF;
END $$;
