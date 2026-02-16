-- Pipeline run safety guards:
-- 1) dedicated sequence for batch ids (decoupled from pipeline_runs.id),
-- 2) enforce one running run per mailing list.

CREATE SEQUENCE IF NOT EXISTS pipeline_run_batches_seq
    AS BIGINT
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1;

SELECT setval(
    'pipeline_run_batches_seq',
    GREATEST(COALESCE((SELECT MAX(batch_id) FROM pipeline_runs), 0), 1),
    true
);

-- Clean up any duplicate active runs before adding the uniqueness guard.
WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (PARTITION BY mailing_list_id ORDER BY id DESC) AS rn
    FROM pipeline_runs
    WHERE state = 'running'
)
UPDATE pipeline_runs pr
SET state = 'failed',
    completed_at = COALESCE(pr.completed_at, now()),
    last_error = COALESCE(pr.last_error, 'superseded duplicate running pipeline run'),
    updated_at = now()
FROM ranked r
WHERE pr.id = r.id
  AND r.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_runs_active_per_list
    ON pipeline_runs (mailing_list_id)
    WHERE state = 'running';
