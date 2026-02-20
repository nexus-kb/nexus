-- Enforce one open pipeline run (pending|running) per mailing list.

-- Keep one open run per list, preferring a running row and then newest id.
WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (
               PARTITION BY mailing_list_id
               ORDER BY CASE state WHEN 'running' THEN 0 ELSE 1 END, id DESC
           ) AS rn
    FROM pipeline_runs
    WHERE state IN ('pending', 'running')
)
UPDATE pipeline_runs pr
SET state = 'cancelled',
    completed_at = COALESCE(pr.completed_at, now()),
    last_error = COALESCE(pr.last_error, 'superseded duplicate open pipeline run'),
    updated_at = now()
FROM ranked r
WHERE pr.id = r.id
  AND r.rn > 1;

DROP INDEX IF EXISTS idx_pipeline_runs_active_per_list;

CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_runs_open_per_list
    ON pipeline_runs (mailing_list_id)
    WHERE state IN ('pending', 'running');
