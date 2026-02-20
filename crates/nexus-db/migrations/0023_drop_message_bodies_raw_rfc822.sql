-- Hard switch to reference-only raw storage.
ALTER TABLE message_bodies
    DROP COLUMN IF EXISTS raw_rfc822;
