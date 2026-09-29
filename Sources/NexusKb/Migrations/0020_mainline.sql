-- A bounded, reproducible index of commits reachable from the configured
-- mainline tip.  Git remains the source of truth; these tables are a cache.
CREATE TABLE mainline_index_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    base_ref text NOT NULL,
    coverage_start text,
    indexed_tip text,
    target_tip text,
    target_base text,
    checked_at timestamptz,
    completed boolean NOT NULL DEFAULT false,
    last_error text,
    matcher_version integer NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE mainline_commits (
    oid text PRIMARY KEY CHECK (oid ~ '^[0-9a-f]{40,64}$'),
    subject text NOT NULL,
    patch_id text,
    patch_id_no_renames text,
    first_release text,
    published boolean NOT NULL DEFAULT false,
    indexed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX mainline_commits_patch_id_idx ON mainline_commits(patch_id)
    WHERE patch_id IS NOT NULL;
CREATE INDEX mainline_commits_no_renames_idx ON mainline_commits(patch_id_no_renames)
    WHERE patch_id_no_renames IS NOT NULL;

CREATE TABLE mainline_releases (
    name text PRIMARY KEY,
    oid text NOT NULL
);

-- References are deliberately retained even when lore has not imported the
-- referenced message yet, enabling reverse lookup when it arrives later.
CREATE TABLE mainline_commit_references (
    commit_oid text NOT NULL REFERENCES mainline_commits(oid) ON DELETE CASCADE,
    message_id text NOT NULL,
    PRIMARY KEY (commit_oid, message_id)
);
CREATE INDEX mainline_references_message_idx
    ON mainline_commit_references(message_id);

CREATE TABLE mainline_patch_fingerprints (
    patch_id bigint PRIMARY KEY REFERENCES patches(id) ON DELETE CASCADE,
    content_fingerprint text NOT NULL,
    stable_patch_id text,
    matcher_version integer NOT NULL,
    checked_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE mainline_patch_matches (
    patch_id bigint NOT NULL REFERENCES mainline_patch_fingerprints(patch_id) ON DELETE CASCADE,
    commit_oid text NOT NULL REFERENCES mainline_commits(oid) ON DELETE CASCADE,
    match_kind text NOT NULL CHECK (match_kind IN ('submission', 'equivalent')),
    matcher_version integer NOT NULL,
    matched_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (patch_id, commit_oid)
);
CREATE INDEX mainline_matches_commit_idx ON mainline_patch_matches(commit_oid);

-- Mail ingestion can replace a diff without changing its patch ID. Invalidate
-- both evidence and checked state immediately, before the next index job.
CREATE FUNCTION invalidate_mainline_patch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.diff IS DISTINCT FROM OLD.diff THEN
        DELETE FROM mainline_patch_matches WHERE patch_id = NEW.id;
        DELETE FROM mainline_patch_fingerprints WHERE patch_id = NEW.id;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER patches_invalidate_mainline
AFTER UPDATE OF diff ON patches
FOR EACH ROW EXECUTE FUNCTION invalidate_mainline_patch();
