-- Align mainline scan state/run schema with the finalized operator and worker contract.

ALTER TABLE mainline_scan_state
ADD COLUMN IF NOT EXISTS ref_name TEXT;

UPDATE mainline_scan_state
SET ref_name = 'refs/heads/master'
WHERE ref_name IS NULL;

ALTER TABLE mainline_scan_state
ALTER COLUMN ref_name SET NOT NULL;

ALTER TABLE mainline_scan_state
ADD COLUMN IF NOT EXISTS last_successful_scan_at TIMESTAMPTZ;

ALTER TABLE mainline_scan_state
ADD COLUMN IF NOT EXISTS last_error TEXT;

ALTER TABLE mainline_scan_state
ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

DROP TRIGGER IF EXISTS mainline_scan_state_touch_updated_at ON mainline_scan_state;
CREATE TRIGGER mainline_scan_state_touch_updated_at
BEFORE UPDATE ON mainline_scan_state
FOR EACH ROW EXECUTE FUNCTION jobs_set_updated_at();

ALTER TABLE mainline_scan_runs
ADD COLUMN IF NOT EXISTS ref_name TEXT;

UPDATE mainline_scan_runs
SET ref_name = 'refs/heads/master'
WHERE ref_name IS NULL;

ALTER TABLE mainline_scan_runs
ALTER COLUMN ref_name SET NOT NULL;
