-- Nexus KB staged per-list ingest pipeline schema.

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id BIGSERIAL PRIMARY KEY,
    mailing_list_id BIGINT NOT NULL REFERENCES mailing_lists(id) ON DELETE CASCADE,
    list_key TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('running', 'succeeded', 'failed', 'cancelled')),
    current_stage TEXT NOT NULL CHECK (current_stage IN ('ingest', 'threading', 'lineage_diff', 'search')),
    source TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    ingest_window_from TIMESTAMPTZ,
    ingest_window_to TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_runs_active_per_list
    ON pipeline_runs (mailing_list_id)
    WHERE state = 'running';

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_list_created
    ON pipeline_runs (list_key, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_state_created
    ON pipeline_runs (state, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS pipeline_stage_runs (
    run_id BIGINT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    stage TEXT NOT NULL CHECK (stage IN ('ingest', 'threading', 'lineage_diff', 'search')),
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'failed')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    progress_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, stage)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_stage_runs_run_state
    ON pipeline_stage_runs (run_id, state, stage);

CREATE TABLE IF NOT EXISTS pipeline_stage_artifacts (
    run_id BIGINT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    artifact_kind TEXT NOT NULL CHECK (artifact_kind IN ('message_pk', 'thread_id', 'patch_item_id', 'patch_series_id')),
    artifact_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, artifact_kind, artifact_id)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_stage_artifacts_run_kind_id
    ON pipeline_stage_artifacts (run_id, artifact_kind, artifact_id);

DROP TRIGGER IF EXISTS pipeline_runs_touch_updated_at ON pipeline_runs;
CREATE TRIGGER pipeline_runs_touch_updated_at
BEFORE UPDATE ON pipeline_runs
FOR EACH ROW EXECUTE FUNCTION jobs_set_updated_at();

DROP TRIGGER IF EXISTS pipeline_stage_runs_touch_updated_at ON pipeline_stage_runs;
CREATE TRIGGER pipeline_stage_runs_touch_updated_at
BEFORE UPDATE ON pipeline_stage_runs
FOR EACH ROW EXECUTE FUNCTION jobs_set_updated_at();
