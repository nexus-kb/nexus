-- Keep patch_series.latest_version_id aligned with its owning series.

CREATE UNIQUE INDEX IF NOT EXISTS idx_patch_series_versions_id_series_unique
    ON patch_series_versions (id, patch_series_id);

ALTER TABLE patch_series
DROP CONSTRAINT IF EXISTS patch_series_latest_version_fk;

ALTER TABLE patch_series
ADD CONSTRAINT patch_series_latest_version_fk
FOREIGN KEY (latest_version_id, id)
REFERENCES patch_series_versions (id, patch_series_id)
DEFERRABLE INITIALLY DEFERRED;
