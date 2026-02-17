-- Diagnostics Meili bootstrap runs for prod migration workflows.

CREATE TABLE IF NOT EXISTS meili_bootstrap_runs (
    id BIGSERIAL PRIMARY KEY,
    scope TEXT NOT NULL CHECK (scope IN ('embedding_indexes')),
    list_key TEXT,
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    embedder_name TEXT NOT NULL,
    model_key TEXT NOT NULL,
    job_id BIGINT,
    thread_cursor_id BIGINT NOT NULL DEFAULT 0,
    series_cursor_id BIGINT NOT NULL DEFAULT 0,
    total_candidates_thread BIGINT NOT NULL DEFAULT 0,
    total_candidates_series BIGINT NOT NULL DEFAULT 0,
    processed_thread BIGINT NOT NULL DEFAULT 0,
    processed_series BIGINT NOT NULL DEFAULT 0,
    docs_upserted BIGINT NOT NULL DEFAULT 0,
    vectors_attached BIGINT NOT NULL DEFAULT 0,
    placeholders_written BIGINT NOT NULL DEFAULT 0,
    failed_batches BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    progress_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_meili_bootstrap_runs_state_created
    ON meili_bootstrap_runs (state, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_meili_bootstrap_runs_scope_list_state
    ON meili_bootstrap_runs (scope, list_key, state, id DESC);

DROP TRIGGER IF EXISTS meili_bootstrap_runs_touch_updated_at ON meili_bootstrap_runs;
CREATE TRIGGER meili_bootstrap_runs_touch_updated_at
BEFORE UPDATE ON meili_bootstrap_runs
FOR EACH ROW EXECUTE FUNCTION jobs_set_updated_at();
