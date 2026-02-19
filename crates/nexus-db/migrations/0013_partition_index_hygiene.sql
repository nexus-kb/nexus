-- Remove duplicate per-partition indexes created by legacy ensure_* functions
-- and make partition creation rely on parent partitioned indexes for threading tables.

DO $$
DECLARE
    idx RECORD;
BEGIN
    FOR idx IN
        SELECT n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'i'
          AND n.nspname = 'public'
          AND (
              c.relname ~ '^threads_[0-9]+_activity_idx$'
              OR c.relname ~ '^thread_nodes_[0-9]+_(sort|message|unique_message)_idx$'
              OR c.relname ~ '^thread_messages_[0-9]+_(sort|message)_idx$'
          )
    LOOP
        EXECUTE format('DROP INDEX IF EXISTS %I.%I', idx.nspname, idx.relname);
    END LOOP;
END
$$;

-- Global duplicate indexes shadowing existing PK/UNIQUE coverage.
DROP INDEX IF EXISTS public.idx_patch_item_files_patch_item;
DROP INDEX IF EXISTS public.idx_patch_items_series_version;
DROP INDEX IF EXISTS public.idx_patch_logical_series;

CREATE OR REPLACE FUNCTION ensure_threads_partition(target_list_id BIGINT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    partition_name TEXT := format('threads_%s', target_list_id);
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF threads FOR VALUES IN (%s)',
        partition_name,
        target_list_id
    );
END;
$$;

CREATE OR REPLACE FUNCTION ensure_thread_nodes_partition(target_list_id BIGINT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    partition_name TEXT := format('thread_nodes_%s', target_list_id);
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF thread_nodes FOR VALUES IN (%s)',
        partition_name,
        target_list_id
    );
END;
$$;

CREATE OR REPLACE FUNCTION ensure_thread_messages_partition(target_list_id BIGINT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    partition_name TEXT := format('thread_messages_%s', target_list_id);
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF thread_messages FOR VALUES IN (%s)',
        partition_name,
        target_list_id
    );
END;
$$;
