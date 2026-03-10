-- Preserve all list-scoped thread associations for cross-posted series versions.

CREATE TABLE IF NOT EXISTS patch_series_version_threads (
    patch_series_version_id BIGINT NOT NULL REFERENCES patch_series_versions(id) ON DELETE CASCADE,
    mailing_list_id BIGINT NOT NULL REFERENCES mailing_lists(id) ON DELETE CASCADE,
    thread_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (patch_series_version_id, mailing_list_id),
    FOREIGN KEY (mailing_list_id, thread_id)
        REFERENCES threads(mailing_list_id, id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_patch_series_version_threads_thread
    ON patch_series_version_threads (mailing_list_id, thread_id);

CREATE INDEX IF NOT EXISTS idx_patch_series_version_threads_version
    ON patch_series_version_threads (patch_series_version_id);

INSERT INTO patch_series_version_threads (patch_series_version_id, mailing_list_id, thread_id)
SELECT psv.id, psv.thread_mailing_list_id, psv.thread_id
FROM patch_series_versions psv
WHERE psv.thread_mailing_list_id IS NOT NULL
  AND psv.thread_id IS NOT NULL
ON CONFLICT (patch_series_version_id, mailing_list_id)
DO UPDATE SET thread_id = EXCLUDED.thread_id;
