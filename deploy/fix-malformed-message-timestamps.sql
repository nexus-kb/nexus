\set ON_ERROR_STOP on
\if :{?apply}
\else
\set apply false
\endif

-- Preview by default; no persistent updates without -v apply=true.
-- Requires migration 0017. Pause ALL maintenance writers before export/preview,
-- backfill before resuming ingestion, and preserve the review CSV before reruns.
-- From the repository root, with PG* connection variables set:
--   python3 deploy/export-archive-timestamps.py /opt/nexus/lore > archive-timestamps.csv
--   psql -X -f deploy/fix-malformed-message-timestamps.sql
-- After reviewing the preview, backing up, and pausing ingestion:
--   psql -X -v apply=true -f deploy/fix-malformed-message-timestamps.sql
-- Regenerate the CSV after ingestion/mirror changes. Do not use a failed export.
-- CSV paths are relative to the psql client's working directory.
--
-- Policy matches IngestMessageParser.effectiveDate: keep Date unless it is
-- absent, predates this Linux KB (1991), or exceeds the archive timestamp.
-- PublicInbox::Import::extract_cmt_info uses Smsg's -ts for Git committer time;
-- MsgTime::msg_timestamp prefers Received, then Date, then import time:
-- https://github.com/nojb/public-inbox/blob/master/lib/PublicInbox/MsgTime.pm
-- https://github.com/nojb/public-inbox/blob/master/lib/PublicInbox/Import.pm
-- It is NOT necessarily the time public-inbox itself received the message.
-- Bad Received clocks and Date-only fallbacks still require manual review.
-- Never use Nexus created_at/first_seen_at as historical delivery dates.
-- Original mail blobs remain unchanged. Spam is repaired, not deleted.

BEGIN;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL lock_timeout = '10s';

CREATE TEMP TABLE archive_timestamps (
    archive_group text NOT NULL,
    blob_oid text NOT NULL,
    committed_at bigint NOT NULL
) ON COMMIT DROP;
\copy archive_timestamps FROM 'archive-timestamps.csv' WITH (FORMAT csv)
ANALYZE archive_timestamps;

\if :apply
-- Prevent ingestion from changing memberships or overwriting repaired dates.
-- Ordinary API SELECTs can continue. Pause workers before running this script.
LOCK TABLE mailing_lists, messages_mailing_lists, messages, threads, patchsets, patches,
    patch_lineages, patchset_lineage_state
    IN SHARE ROW EXCLUSIVE MODE;
\endif

-- Retain each list's evidence, not just the earliest observation per message.
CREATE TEMP TABLE archive_link_times ON COMMIT DROP AS
SELECT link.message_id AS id, link.mailing_list_id,
       COALESCE(
           min(to_timestamp(source.committed_at)) FILTER (
               WHERE to_timestamp(source.committed_at) BETWEEN timestamptz '1991-01-01' AND now()
           ),
           min(to_timestamp(source.committed_at))
       ) AS archive_at
FROM archive_timestamps AS source
JOIN mailing_lists AS list ON list.archive_group = source.archive_group
JOIN messages_mailing_lists AS link
  ON link.mailing_list_id = list.id AND link.archive_blob_oid = source.blob_oid
GROUP BY link.message_id, link.mailing_list_id;
CREATE UNIQUE INDEX ON archive_link_times (id, mailing_list_id);
ANALYZE archive_link_times;

-- Report unusable archive clocks. When both clocks are broken, leave the date
-- unknown. In particular, Date-only imports can have the same bad time twice.
SELECT message.message_id, link.mailing_list_id, message.sent_at, source.archive_at
FROM messages AS message
LEFT JOIN messages_mailing_lists AS link ON link.message_id = message.id
LEFT JOIN archive_link_times AS source
  ON source.id = message.id AND source.mailing_list_id = link.mailing_list_id
WHERE NOT message.is_placeholder
  AND (source.archive_at IS NULL OR source.archive_at < timestamptz '1991-01-01'
       OR source.archive_at > now());

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM messages AS message
        LEFT JOIN messages_mailing_lists AS link ON link.message_id = message.id
        LEFT JOIN archive_link_times AS source
          ON source.id = message.id AND source.mailing_list_id = link.mailing_list_id
        WHERE NOT message.is_placeholder
          AND source.id IS NULL
    ) THEN
        RAISE EXCEPTION 'Incomplete archive export; review the rows above';
    END IF;
END
$$;

-- Legacy sent_at is the parsed sender date. Seed each link before modifying it.
-- Recorded observations (including known NULLs) must survive repeated repairs:
-- a canonical date is NOT the original sender date of every cross-post copy.
CREATE TEMP TABLE timestamp_observations ON COMMIT DROP AS
SELECT message.id, link.mailing_list_id, NOT link.timestamp_recorded AS needs_backfill,
       CASE WHEN link.timestamp_recorded THEN link.effective_sent_at
            ELSE correction.sent_at END AS sent_at
FROM messages AS message
JOIN messages_mailing_lists AS link ON link.message_id = message.id
JOIN archive_link_times AS source
  ON source.id = message.id AND source.mailing_list_id = link.mailing_list_id
CROSS JOIN LATERAL (
    SELECT CASE
        WHEN source.archive_at < timestamptz '1991-01-01' OR source.archive_at > now()
        THEN CASE WHEN message.sent_at BETWEEN timestamptz '1991-01-01' AND now()
                  THEN message.sent_at ELSE NULL END
        WHEN message.sent_at IS NULL OR message.sent_at < timestamptz '1991-01-01'
        THEN source.archive_at
        ELSE least(message.sent_at, source.archive_at)
    END AS sent_at
) AS correction
WHERE NOT message.is_placeholder;
CREATE UNIQUE INDEX ON timestamp_observations (id, mailing_list_id);
ANALYZE timestamp_observations;

CREATE TEMP TABLE message_timestamp_repairs ON COMMIT DROP AS
SELECT message.id, message.message_id, message.thread_id,
       message.sent_at AS old_sent_at, observation.sent_at AS corrected_sent_at
FROM messages AS message
JOIN (
    SELECT id, min(sent_at) AS sent_at FROM timestamp_observations GROUP BY id
) AS observation USING (id)
WHERE message.sent_at IS DISTINCT FROM observation.sent_at;
CREATE UNIQUE INDEX ON message_timestamp_repairs (id);
CREATE UNIQUE INDEX ON message_timestamp_repairs (message_id);
ANALYZE message_timestamp_repairs;

SELECT count(*) AS messages_to_repair,
       count(DISTINCT thread_id) AS affected_threads,
       count(*) FILTER (WHERE old_sent_at IS NULL) AS missing_dates,
       count(*) FILTER (WHERE old_sent_at < timestamptz '1991-01-01') AS ancient_dates,
       count(*) FILTER (WHERE corrected_sent_at IS NULL) AS unknown_dates,
       count(*) FILTER (WHERE old_sent_at > corrected_sent_at) AS ahead_of_archive,
       count(*) FILTER (WHERE old_sent_at > corrected_sent_at + interval '1 day') AS ahead_over_one_day
FROM message_timestamp_repairs;
SELECT count(*) AS archive_observations_to_backfill
FROM timestamp_observations WHERE needs_backfill;
-- Persist the complete reviewable before/after manifest on the client.
\copy (SELECT * FROM message_timestamp_repairs ORDER BY id) TO 'message-timestamp-repairs.csv' WITH (FORMAT csv, HEADER true)

\if :apply
UPDATE messages_mailing_lists AS link
SET effective_sent_at = observation.sent_at, timestamp_recorded = true
FROM timestamp_observations AS observation
WHERE observation.needs_backfill AND link.message_id = observation.id
  AND link.mailing_list_id = observation.mailing_list_id;

UPDATE messages AS message
SET sent_at = repair.corrected_sent_at, updated_at = now()
FROM message_timestamp_repairs AS repair
WHERE message.id = repair.id;

-- Missing parents have no sender date. Remove only obviously broken inherited
-- dates; placeholders never contribute to the thread's activity aggregate.
UPDATE messages SET sent_at = NULL, updated_at = now()
WHERE is_placeholder AND (sent_at < timestamptz '1991-01-01' OR sent_at > now());

-- Use ingestion's derived-date logic. Include newly backfilled observations
-- even if the message date was unchanged (e.g. a previous partial repair).
SELECT refresh_patchset_timestamps(ARRAY(
    SELECT patchset.id FROM patchsets AS patchset
    WHERE patchset.thread_id IN (
        SELECT thread_id FROM message_timestamp_repairs
        UNION
        SELECT message.thread_id FROM messages AS message
        JOIN timestamp_observations AS observation USING (id)
        WHERE observation.needs_backfill
    )
));

-- startedAt is projected from the root message's sent_at, not threads.created_at.
-- lastActivityAt must be rebuilt, not GREATEST'ed with its corrupted old value.
-- Match ingestion's epoch sorting sentinel for entirely undated threads.
WITH metadata AS (
    SELECT message.thread_id, COALESCE(max(message.sent_at), 'epoch'::timestamptz) AS last_updated_at
    FROM messages AS message
    JOIN (SELECT DISTINCT thread_id FROM message_timestamp_repairs) AS affected
      ON affected.thread_id = message.thread_id
    WHERE NOT message.is_placeholder
    GROUP BY message.thread_id
)
UPDATE threads AS thread SET last_updated_at = metadata.last_updated_at
FROM metadata WHERE thread.id = metadata.thread_id
  AND thread.last_updated_at IS DISTINCT FROM metadata.last_updated_at;

-- Existing message UPDATE triggers refresh thread_search_documents. Do not
-- disable triggers: their chronological snippet selection can change too.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM messages AS message
        JOIN message_timestamp_repairs AS repair USING (id)
        WHERE message.sent_at IS DISTINCT FROM repair.corrected_sent_at
    ) THEN
        RAISE EXCEPTION 'One or more timestamp repairs did not apply';
    END IF;
END
$$;
COMMIT;
\else
ROLLBACK;
\echo 'Preview only. Review message-timestamp-repairs.csv; use -v apply=true to commit.'
\endif
