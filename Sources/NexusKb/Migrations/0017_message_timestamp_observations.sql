-- A NULL observed date is meaningful (both source clocks were unusable).
-- Distinguish it from legacy links whose archive evidence has not been read.
-- Backfill legacy links with deploy/fix-malformed-message-timestamps.sql while
-- maintenance writers are paused, before resuming ingestion with this schema.
ALTER TABLE messages_mailing_lists
    ADD COLUMN effective_sent_at timestamptz,
    ADD COLUMN timestamp_recorded boolean NOT NULL DEFAULT false;

CREATE FUNCTION refresh_message_timestamps(target_ids bigint[])
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    -- Take locks before the aggregate statements so a waiter reads a fresh
    -- READ COMMITTED snapshot after a concurrent ingest/deletion commits.
    PERFORM id FROM messages WHERE id = ANY(target_ids) ORDER BY id FOR UPDATE;
    PERFORM id FROM threads WHERE id IN (
        SELECT thread_id FROM messages WHERE id = ANY(target_ids)
    ) ORDER BY id FOR UPDATE;

    IF EXISTS (
        SELECT 1 FROM messages_mailing_lists
        WHERE message_id = ANY(target_ids) AND NOT timestamp_recorded
    ) THEN
        RAISE EXCEPTION 'Timestamp observations need backfill before ingestion resumes';
    END IF;

    -- A replay replaces one observation, not the other lists' evidence.
    -- Never fold a legacy messages.sent_at into this aggregate: it may be bad.
    UPDATE messages AS message
    SET sent_at = observation.sent_at, updated_at = now()
    FROM (
        SELECT link.message_id, min(link.effective_sent_at) AS sent_at
        FROM messages_mailing_lists AS link
        WHERE link.message_id = ANY(target_ids) AND link.timestamp_recorded
        GROUP BY link.message_id
    ) AS observation
    WHERE message.id = observation.message_id AND NOT message.is_placeholder
      AND message.sent_at IS DISTINCT FROM observation.sent_at;

    UPDATE threads AS thread
    SET last_updated_at = metadata.sent_at
    FROM (
        SELECT member.thread_id,
               COALESCE(max(member.sent_at) FILTER (WHERE NOT member.is_placeholder),
                        'epoch'::timestamptz) AS sent_at
        FROM messages AS member
        JOIN (SELECT DISTINCT thread_id FROM messages WHERE id = ANY(target_ids)) AS affected
          ON affected.thread_id = member.thread_id
        GROUP BY member.thread_id
    ) AS metadata
    WHERE thread.id = metadata.thread_id
      AND thread.last_updated_at IS DISTINCT FROM metadata.sent_at;
END
$$;

CREATE FUNCTION refresh_patchset_timestamps(target_ids bigint[])
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM id FROM patchsets WHERE id = ANY(target_ids) ORDER BY id FOR UPDATE;

    UPDATE patchsets AS patchset
    SET sent_at = metadata.sent_at, updated_at = now()
    FROM (
        SELECT target.id, COALESCE(cover.sent_at, min(part.sent_at)) AS sent_at
        FROM patchsets AS target
        LEFT JOIN messages AS cover
          ON cover.message_id = target.cover_letter_message_id AND NOT cover.is_placeholder
        LEFT JOIN patches AS patch ON patch.patchset_id = target.id
        LEFT JOIN messages AS part ON part.message_id = patch.message_id AND NOT part.is_placeholder
        WHERE target.id = ANY(target_ids)
        GROUP BY target.id, cover.sent_at
    ) AS metadata
    WHERE patchset.id = metadata.id AND patchset.sent_at IS DISTINCT FROM metadata.sent_at;

    -- Refresh summaries synchronously without changing lineage membership.
    -- Include ALL members of an affected lineage, not only corrected patchsets.
    PERFORM id FROM patch_lineages WHERE id IN (
        SELECT lineage_id FROM patchset_lineage_state WHERE patchset_id = ANY(target_ids)
    ) ORDER BY id FOR UPDATE;

    UPDATE patch_lineages AS lineage
    SET first_sent_at = summary.first_sent_at,
        latest_sent_at = summary.latest_sent_at,
        canonical_subject = summary.canonical_subject,
        updated_at = now()
    FROM (
        SELECT state.lineage_id, min(patchset.sent_at) AS first_sent_at,
               max(patchset.sent_at) AS latest_sent_at,
               (array_agg(state.display_subject ORDER BY
                    (state.phase = 'PATCH') DESC, patchset.sent_at DESC NULLS LAST,
                    state.patchset_id DESC))[1] AS canonical_subject
        FROM patchset_lineage_state AS state
        JOIN patchsets AS patchset ON patchset.id = state.patchset_id
        WHERE state.lineage_id IN (
            SELECT lineage_id FROM patchset_lineage_state WHERE patchset_id = ANY(target_ids)
        )
        GROUP BY state.lineage_id
    ) AS summary
    WHERE lineage.id = summary.lineage_id
      AND (lineage.first_sent_at, lineage.latest_sent_at, lineage.canonical_subject)
          IS DISTINCT FROM (summary.first_sent_at, summary.latest_sent_at, summary.canonical_subject);
END
$$;
