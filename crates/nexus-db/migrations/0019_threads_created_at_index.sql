-- Support date_desc thread listing without seq scan + top-N sort.
CREATE INDEX IF NOT EXISTS idx_threads_created_at
    ON threads (mailing_list_id, created_at DESC, id DESC);
