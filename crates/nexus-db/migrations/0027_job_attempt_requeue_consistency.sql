-- Ensure running job attempts are finalized whenever a job leaves running state.

UPDATE job_attempts ja
SET status = CASE
        WHEN j.state = 'succeeded' THEN 'succeeded'
        WHEN j.state = 'cancelled' THEN 'cancelled'
        ELSE 'failed'
    END,
    finished_at = COALESCE(ja.finished_at, now()),
    error = CASE
        WHEN j.state = 'succeeded' THEN ja.error
        ELSE COALESCE(ja.error, j.last_error, 'finalized by non-running job state reconciliation')
    END
FROM jobs j
WHERE ja.job_id = j.id
  AND ja.status = 'running'
  AND j.state <> 'running';

CREATE OR REPLACE FUNCTION jobs_finalize_running_attempts_on_state_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.state = OLD.state THEN
        RETURN NEW;
    END IF;

    IF OLD.state = 'running' AND NEW.state <> 'running' THEN
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
