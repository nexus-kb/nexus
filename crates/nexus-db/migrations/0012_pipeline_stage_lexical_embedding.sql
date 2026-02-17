-- Expand pipeline stage enum/check for lexical-first flow while keeping
-- legacy names for queued/old jobs during rollout.

ALTER TABLE pipeline_runs
DROP CONSTRAINT IF EXISTS pipeline_runs_current_stage_check;

ALTER TABLE pipeline_runs
ADD CONSTRAINT pipeline_runs_current_stage_check
CHECK (current_stage IN (
  'ingest',
  'threading',
  'lineage',
  'lexical',
  'embedding',
  'search',
  'lineage_diff'
));
