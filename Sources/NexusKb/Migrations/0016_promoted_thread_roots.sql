ALTER TABLE threads
ADD COLUMN promoted_from_message_id text;

CREATE INDEX threads_promoted_from_message_idx
ON threads (promoted_from_message_id)
WHERE promoted_from_message_id IS NOT NULL;
