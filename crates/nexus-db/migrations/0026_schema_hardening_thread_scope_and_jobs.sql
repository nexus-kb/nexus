-- Schema hardening pass:
-- 1) make patch_series_versions thread references list-scoped,
-- 2) enforce job_attempt lifecycle invariants,
-- 3) simplify list_message_instances partition helper and drop duplicate partition-local indexes,
-- 4) remove currently-unused lineage/threading indexes that add write overhead.

ALTER TABLE patch_series_versions
ADD COLUMN IF NOT EXISTS thread_mailing_list_id BIGINT;

UPDATE patch_series_versions psv
SET thread_mailing_list_id = t.mailing_list_id
FROM threads t
WHERE psv.thread_id IS NOT NULL
  AND psv.thread_mailing_list_id IS NULL
  AND t.id = psv.thread_id;

UPDATE patch_series_versions
SET thread_id = NULL,
    thread_mailing_list_id = NULL
WHERE thread_id IS NOT NULL
  AND thread_mailing_list_id IS NULL;

ALTER TABLE patch_series_versions
DROP CONSTRAINT IF EXISTS patch_series_versions_thread_ref_pair_check;

ALTER TABLE patch_series_versions
ADD CONSTRAINT patch_series_versions_thread_ref_pair_check
CHECK (
    (thread_id IS NULL AND thread_mailing_list_id IS NULL)
    OR (thread_id IS NOT NULL AND thread_mailing_list_id IS NOT NULL)
)
NOT VALID;

ALTER TABLE patch_series_versions
VALIDATE CONSTRAINT patch_series_versions_thread_ref_pair_check;

ALTER TABLE patch_series_versions
DROP CONSTRAINT IF EXISTS patch_series_versions_thread_ref_fk;

ALTER TABLE patch_series_versions
ADD CONSTRAINT patch_series_versions_thread_ref_fk
FOREIGN KEY (thread_mailing_list_id, thread_id)
REFERENCES threads(mailing_list_id, id)
ON DELETE SET NULL
NOT VALID;

ALTER TABLE patch_series_versions
VALIDATE CONSTRAINT patch_series_versions_thread_ref_fk;

CREATE INDEX IF NOT EXISTS idx_patch_series_versions_thread_ref
    ON patch_series_versions (thread_mailing_list_id, thread_id)
    WHERE thread_id IS NOT NULL;

WITH ranked_running AS (
    SELECT
        ja.id,
        ja.job_id,
        row_number() OVER (
            PARTITION BY ja.job_id
            ORDER BY ja.attempt DESC, ja.id DESC
        ) AS rn
    FROM job_attempts ja
    WHERE ja.status = 'running'
)
UPDATE job_attempts ja
SET status = 'failed',
    finished_at = COALESCE(ja.finished_at, now()),
    error = COALESCE(ja.error, 'superseded running attempt')
FROM ranked_running rr
WHERE ja.id = rr.id
  AND rr.rn > 1;

UPDATE job_attempts ja
SET status = CASE
        WHEN j.state = 'succeeded' THEN 'succeeded'
        WHEN j.state = 'cancelled' THEN 'cancelled'
        ELSE 'failed'
    END,
    finished_at = COALESCE(ja.finished_at, now()),
    error = CASE
        WHEN j.state = 'succeeded' THEN ja.error
        ELSE COALESCE(ja.error, j.last_error, 'finalized by schema reconciliation')
    END
FROM jobs j
WHERE ja.job_id = j.id
  AND ja.status = 'running'
  AND j.state IN ('succeeded', 'failed_retryable', 'failed_terminal', 'cancelled');

UPDATE job_attempts
SET finished_at = NULL
WHERE status = 'running'
  AND finished_at IS NOT NULL;

UPDATE job_attempts
SET finished_at = now()
WHERE status <> 'running'
  AND finished_at IS NULL;

ALTER TABLE job_attempts
DROP CONSTRAINT IF EXISTS job_attempts_status_check;

ALTER TABLE job_attempts
ADD CONSTRAINT job_attempts_status_check
CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled'))
NOT VALID;

ALTER TABLE job_attempts
VALIDATE CONSTRAINT job_attempts_status_check;

ALTER TABLE job_attempts
DROP CONSTRAINT IF EXISTS job_attempts_status_finished_at_check;

ALTER TABLE job_attempts
ADD CONSTRAINT job_attempts_status_finished_at_check
CHECK (
    (status = 'running' AND finished_at IS NULL)
    OR (status <> 'running' AND finished_at IS NOT NULL)
)
NOT VALID;

ALTER TABLE job_attempts
VALIDATE CONSTRAINT job_attempts_status_finished_at_check;

CREATE UNIQUE INDEX IF NOT EXISTS idx_job_attempts_one_running_per_job
    ON job_attempts (job_id)
    WHERE status = 'running';

CREATE OR REPLACE FUNCTION jobs_finalize_running_attempts_on_state_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.state = OLD.state THEN
        RETURN NEW;
    END IF;

    IF NEW.state IN ('succeeded', 'failed_retryable', 'failed_terminal', 'cancelled') THEN
        UPDATE job_attempts
        SET status = CASE
                WHEN NEW.state = 'succeeded' THEN 'succeeded'
                WHEN NEW.state = 'cancelled' THEN 'cancelled'
                ELSE 'failed'
            END,
            finished_at = COALESCE(finished_at, now()),
            error = CASE
                WHEN NEW.state = 'succeeded' THEN error
                ELSE COALESCE(error, NEW.last_error, 'finalized by job state transition')
            END
        WHERE job_id = NEW.id
          AND status = 'running';
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS jobs_finalize_running_attempts_trg ON jobs;

CREATE TRIGGER jobs_finalize_running_attempts_trg
AFTER UPDATE OF state ON jobs
FOR EACH ROW
WHEN (OLD.state IS DISTINCT FROM NEW.state)
EXECUTE FUNCTION jobs_finalize_running_attempts_on_state_change();

DO $$
DECLARE
    idx RECORD;
BEGIN
    FOR idx IN
        SELECT n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'i'
          AND n.nspname = 'public'
          AND c.relname ~ '^list_message_instances_[0-9]+_(seen_at|message_pk)_idx$'
    LOOP
        EXECUTE format('DROP INDEX IF EXISTS %I.%I', idx.nspname, idx.relname);
    END LOOP;
END
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
END;
$$;

DROP INDEX IF EXISTS idx_messages_references_ids_gin;
DROP INDEX IF EXISTS idx_messages_in_reply_to_ids_gin;
DROP INDEX IF EXISTS idx_patch_series_author_subject;
DROP INDEX IF EXISTS idx_patch_items_patch_id;
DROP INDEX IF EXISTS idx_patch_item_files_path;
DROP INDEX IF EXISTS idx_message_patch_facts_patch_id;
