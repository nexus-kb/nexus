CREATE TABLE maintenance_stage_thread_targets (
    stage_id            uuid NOT NULL
                            REFERENCES maintenance_run_stages(id)
                            ON DELETE CASCADE,
    -- Deliberately not a threads FK: merges and deletions can remove a target
    -- before end-of-run finalization, where a missing ID is harmless.
    thread_id           bigint NOT NULL,

    PRIMARY KEY (stage_id, thread_id)
);

ALTER TABLE maintenance_run_stages
    ADD COLUMN thread_targets_complete boolean NOT NULL DEFAULT true;

-- A workflow that committed ingest batches before this migration has no way
-- to reconstruct their exact thread set. It gets one legacy global pass.
UPDATE maintenance_run_stages
SET thread_targets_complete = false
WHERE operation = 'ingest'
  AND processed_items > 0;
