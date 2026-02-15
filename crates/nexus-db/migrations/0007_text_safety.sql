-- UTF-8/operator safety helpers for mail-derived text access paths.

CREATE OR REPLACE FUNCTION nexus_safe_prefix(value TEXT, max_chars INT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT LEFT((COALESCE(value, '') || ''), GREATEST(COALESCE(max_chars, 0), 0));
$$;
