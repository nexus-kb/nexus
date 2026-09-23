ALTER TABLE patchset_lineage_state
    DROP CONSTRAINT patchset_lineage_source_check;
ALTER TABLE patchset_lineage_state
    ADD CONSTRAINT patchset_lineage_source_check CHECK (
        match_source IN (
            'singleton', 'change-id', 'reply-chain', 'revision-link',
            'subject-author', 'manual'
        )
    );

-- Keep unresolved targets: an older revision may be imported later.
CREATE TABLE patch_revision_links (
    patchset_id bigint NOT NULL REFERENCES patchsets(id) ON DELETE CASCADE,
    message_id text NOT NULL,
    revision integer NOT NULL CHECK (revision > 0),
    PRIMARY KEY (patchset_id, message_id, revision)
);
CREATE INDEX patch_revision_links_message_idx ON patch_revision_links(message_id);
