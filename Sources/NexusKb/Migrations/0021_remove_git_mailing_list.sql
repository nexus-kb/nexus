-- Stop mirror/maintenance writers before applying. Git development is outside
-- Nexus's scope. Preserve cross-posts and placeholder parents of Linux replies.
BEGIN;
LOCK TABLE maintenance_runs IN SHARE ROW EXCLUSIVE MODE;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM maintenance_runs WHERE state IN ('queued', 'running')) THEN
        RAISE EXCEPTION 'Drain maintenance before removing the Git mailing list';
    END IF;
END $$;

CREATE TEMP TABLE removed_git_messages ON COMMIT DROP AS
SELECT m.id, m.message_id, m.thread_id,
       NOT EXISTS (
           SELECT 1 FROM messages_mailing_lists other
           WHERE other.message_id = m.id AND other.mailing_list_id <> list.id
       ) AS exclusive
FROM mailing_lists list
JOIN messages_mailing_lists link ON link.mailing_list_id = list.id
JOIN messages m ON m.id = link.message_id
WHERE list.archive_group = 'git';
CREATE INDEX ON removed_git_messages (id);
CREATE INDEX ON removed_git_messages (message_id);
CREATE INDEX ON removed_git_messages (thread_id);
ANALYZE removed_git_messages;

CREATE TEMP TABLE removed_git_threads ON COMMIT DROP AS
SELECT DISTINCT thread_id FROM removed_git_messages;
CREATE TEMP TABLE removed_git_patchsets ON COMMIT DROP AS
SELECT p.id FROM patchsets p
WHERE p.thread_id IN (SELECT thread_id FROM removed_git_threads);
CREATE TEMP TABLE removed_git_lineages ON COMMIT DROP AS
SELECT DISTINCT lineage_id FROM patchset_lineage_state
WHERE patchset_id IN (SELECT id FROM removed_git_patchsets);

-- Stage targets cascade; other lists' stages and run history remain intact.
DELETE FROM maintenance_run_stages
WHERE mailing_list_id IN (SELECT id FROM mailing_lists WHERE archive_group = 'git');
DELETE FROM mailing_lists WHERE archive_group = 'git';

-- Bound search-trigger transition tables and cascading deletes by thread batch.
DO $$ DECLARE batch bigint[]; last_id bigint := 0; BEGIN
    LOOP
        SELECT array_agg(thread_id ORDER BY thread_id) INTO batch FROM (
            SELECT thread_id FROM removed_git_threads WHERE thread_id > last_id
            ORDER BY thread_id LIMIT 500
        ) next_batch;
        EXIT WHEN batch IS NULL;
        last_id := batch[array_length(batch, 1)];
        DELETE FROM threads t
        WHERE t.id = ANY(batch) AND NOT EXISTS (
            SELECT 1 FROM messages m JOIN messages_mailing_lists link ON link.message_id = m.id
            WHERE m.thread_id = t.id
        );
    END LOOP;
END $$;

-- The remaining candidates belong to mixed-list threads. Keep their topology,
-- but remove Git-only content and derived patches just as ingest deletion does.
DELETE FROM patches p USING removed_git_messages r
WHERE r.exclusive AND p.message_id = r.message_id;
UPDATE patchsets p SET cover_letter_message_id = NULL
FROM removed_git_messages r
WHERE r.exclusive AND p.cover_letter_message_id = r.message_id;
DELETE FROM messages_recipients link USING removed_git_messages r
WHERE r.exclusive AND link.message_id = r.id;
DELETE FROM messages_subsystems link USING removed_git_messages r
WHERE r.exclusive AND link.message_id = r.id;
UPDATE messages m SET in_reply_to = NULL, references_ids = ARRAY[]::text[],
    author = NULL, subject = '(placeholder)', sent_at = NULL, body = '',
    to_recipients = '', cc_recipients = '', is_placeholder = true, updated_at = now()
FROM removed_git_messages r WHERE r.exclusive AND m.id = r.id;

DELETE FROM patchsets p WHERE p.id IN (SELECT id FROM removed_git_patchsets)
AND p.cover_letter_message_id IS NULL
AND NOT EXISTS (SELECT 1 FROM patches part WHERE part.patchset_id = p.id);
UPDATE patchsets p SET received_parts = counts.parts,
    status = CASE WHEN p.status = 'Malformed' THEN p.status
                  WHEN counts.parts >= p.total_parts THEN 'Complete' ELSE 'Incomplete' END,
    updated_at = now()
FROM (
    SELECT p.id, count(part.id)::integer AS parts FROM patchsets p
    LEFT JOIN patches part ON part.patchset_id = p.id
    WHERE p.id IN (SELECT id FROM removed_git_patchsets) GROUP BY p.id
) counts WHERE p.id = counts.id;

-- Drop the removed archive's timestamp evidence, and refresh retained members
-- of affected lineages even if their removed sibling was in a different thread.
SELECT refresh_message_timestamps(ARRAY(
    SELECT m.id FROM messages m JOIN removed_git_messages r ON r.id = m.id
    WHERE NOT r.exclusive
));
SELECT refresh_patchset_timestamps(ARRAY(
    SELECT id FROM patchsets WHERE id IN (SELECT id FROM removed_git_patchsets)
    UNION
    SELECT patchset_id FROM patchset_lineage_state
    WHERE lineage_id IN (SELECT lineage_id FROM removed_git_lineages)
));

-- Apply PostgresThreadRootService's normal reconciliation/promotion contract
-- to affected threads. Root updates rebuild search documents via the trigger.
WITH RECURSIVE promoted AS MATERIALIZED (
    SELECT t.id, t.root_message_id, t.promoted_from_message_id FROM threads t
    WHERE t.id IN (SELECT thread_id FROM removed_git_threads)
      AND t.promoted_from_message_id IS NOT NULL
), reachable AS (
    SELECT id, root_message_id AS message_id FROM promoted
    UNION
    SELECT r.id, child.message_id FROM reachable r
    JOIN messages child ON child.thread_id = r.id AND child.in_reply_to = r.message_id
), invalid AS (
    SELECT t.* FROM promoted t
    LEFT JOIN messages missing ON missing.thread_id = t.id AND missing.message_id = t.promoted_from_message_id
    LEFT JOIN messages root ON root.thread_id = t.id AND root.message_id = t.root_message_id
    WHERE missing.id IS NULL OR NOT missing.is_placeholder OR root.id IS NULL OR root.is_placeholder
       OR root.in_reply_to IS DISTINCT FROM t.promoted_from_message_id
       OR (SELECT count(*) FROM messages m WHERE m.thread_id = t.id AND m.in_reply_to = t.promoted_from_message_id) <> 1
       OR (SELECT count(*) FROM messages m WHERE m.thread_id = t.id) <>
          1 + (SELECT count(*) FROM reachable r WHERE r.id = t.id)
)
UPDATE threads t SET root_message_id = i.promoted_from_message_id, promoted_from_message_id = NULL
FROM invalid i WHERE t.id = i.id;

WITH RECURSIVE candidates AS MATERIALIZED (
    SELECT t.id, root.message_id AS missing_id, child.message_id AS child_id
    FROM threads t
    JOIN messages root ON root.thread_id = t.id AND root.message_id = t.root_message_id AND root.is_placeholder
    JOIN messages child ON child.thread_id = t.id AND child.in_reply_to = root.message_id AND NOT child.is_placeholder
    WHERE t.id IN (SELECT thread_id FROM removed_git_threads)
      AND t.promoted_from_message_id IS NULL
      AND (SELECT count(*) FROM messages m WHERE m.thread_id = t.id AND m.in_reply_to = root.message_id) = 1
), reachable AS (
    SELECT id, child_id AS message_id FROM candidates
    UNION
    SELECT r.id, child.message_id FROM reachable r
    JOIN messages child ON child.thread_id = r.id AND child.in_reply_to = r.message_id
), eligible AS (
    SELECT c.* FROM candidates c
    WHERE (SELECT count(*) FROM messages m WHERE m.thread_id = c.id) =
          1 + (SELECT count(*) FROM reachable r WHERE r.id = c.id)
)
UPDATE threads t SET root_message_id = e.child_id, promoted_from_message_id = e.missing_id
FROM eligible e WHERE t.id = e.id;

UPDATE threads t SET subject = COALESCE((
    SELECT m.subject FROM messages m WHERE m.thread_id = t.id AND NOT m.is_placeholder
    ORDER BY (m.message_id = t.root_message_id) DESC,
             COALESCE(m.sent_at, m.created_at), m.id LIMIT 1
), '(placeholder)'), last_updated_at = COALESCE((
    SELECT max(m.sent_at) FROM messages m WHERE m.thread_id = t.id AND NOT m.is_placeholder
), 'epoch'::timestamptz)
WHERE t.id IN (SELECT thread_id FROM removed_git_threads);
DELETE FROM threads_subsystems WHERE thread_id IN (SELECT thread_id FROM removed_git_threads);
INSERT INTO threads_subsystems (thread_id, subsystem_id)
SELECT DISTINCT m.thread_id, s.subsystem_id FROM messages m
JOIN messages_subsystems s ON s.message_id = m.id
WHERE m.thread_id IN (SELECT thread_id FROM removed_git_threads);

COMMIT;
