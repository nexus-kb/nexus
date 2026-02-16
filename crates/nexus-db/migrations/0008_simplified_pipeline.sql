-- Simplified ingest pipeline: serial 4-stage pipeline per list with batch ordering.

-- 1. Add 'pending' state to pipeline_runs
ALTER TABLE pipeline_runs DROP CONSTRAINT IF EXISTS pipeline_runs_state_check;
ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_state_check
  CHECK (state IN ('pending', 'running', 'succeeded', 'failed', 'cancelled'));

-- 2. Widen current_stage to include 'lineage' (renamed from 'lineage_diff')
ALTER TABLE pipeline_runs DROP CONSTRAINT IF EXISTS pipeline_runs_current_stage_check;
ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_current_stage_check
  CHECK (current_stage IN ('ingest', 'threading', 'lineage', 'lineage_diff', 'search'));

-- 3. Add progress tracking and batch ordering columns
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS progress_json JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS batch_id BIGINT;
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS batch_position INTEGER;

-- 4. Index for finding next pending run in a batch
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pending_batch
  ON pipeline_runs (batch_id, batch_position ASC) WHERE state = 'pending';

-- 5. Drop the unique-active-per-list index (pending runs coexist with running)
DROP INDEX IF EXISTS idx_pipeline_runs_active_per_list;

-- 6. Drop tables no longer needed
DROP TABLE IF EXISTS pipeline_stage_artifacts;
DROP TABLE IF EXISTS pipeline_stage_runs;
