-- Nexus KB embeddings + hybrid search persistence.

CREATE TABLE IF NOT EXISTS search_doc_embeddings (
    scope TEXT NOT NULL CHECK (scope IN ('thread', 'series')),
    doc_id BIGINT NOT NULL,
    model_key TEXT NOT NULL,
    dimensions INT NOT NULL CHECK (dimensions > 0),
    vector REAL[] NOT NULL,
    source_hash BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, doc_id, model_key)
);

CREATE INDEX IF NOT EXISTS idx_search_doc_embeddings_scope_model
    ON search_doc_embeddings (scope, model_key, doc_id);

CREATE TABLE IF NOT EXISTS embedding_backfill_runs (
    id BIGSERIAL PRIMARY KEY,
    scope TEXT NOT NULL CHECK (scope IN ('thread', 'series')),
    list_key TEXT,
    from_seen_at TIMESTAMPTZ,
    to_seen_at TIMESTAMPTZ,
    state TEXT NOT NULL CHECK (state IN ('running', 'succeeded', 'failed', 'cancelled')),
    model_key TEXT NOT NULL,
    cursor_id BIGINT NOT NULL DEFAULT 0,
    total_candidates BIGINT NOT NULL DEFAULT 0,
    processed_count BIGINT NOT NULL DEFAULT 0,
    embedded_count BIGINT NOT NULL DEFAULT 0,
    failed_count BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    progress_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_embedding_backfill_runs_state_created
    ON embedding_backfill_runs (state, created_at DESC, id DESC);

DROP TRIGGER IF EXISTS search_doc_embeddings_touch_updated_at ON search_doc_embeddings;
CREATE TRIGGER search_doc_embeddings_touch_updated_at
BEFORE UPDATE ON search_doc_embeddings
FOR EACH ROW EXECUTE FUNCTION jobs_set_updated_at();

DROP TRIGGER IF EXISTS embedding_backfill_runs_touch_updated_at ON embedding_backfill_runs;
CREATE TRIGGER embedding_backfill_runs_touch_updated_at
BEFORE UPDATE ON embedding_backfill_runs
FOR EACH ROW EXECUTE FUNCTION jobs_set_updated_at();
