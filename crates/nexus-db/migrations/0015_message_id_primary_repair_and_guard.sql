-- Enforce a single canonical primary Message-ID mapping per message.

-- Demote stale primary flags that do not match messages.message_id_primary.
UPDATE message_id_map mim
SET is_primary = false
FROM messages m
WHERE mim.message_pk = m.id
  AND mim.is_primary = true
  AND mim.message_id <> m.message_id_primary;

-- Ensure canonical primary rows are marked primary.
UPDATE message_id_map mim
SET is_primary = true
FROM messages m
WHERE mim.message_pk = m.id
  AND mim.message_id = m.message_id_primary
  AND mim.is_primary = false;

-- Backfill missing canonical primary rows when absent.
INSERT INTO message_id_map (message_id, message_pk, is_primary)
SELECT m.message_id_primary, m.id, true
FROM messages m
LEFT JOIN message_id_map mim
  ON mim.message_pk = m.id
 AND mim.message_id = m.message_id_primary
WHERE mim.message_pk IS NULL
ON CONFLICT (message_id, message_pk)
DO UPDATE SET is_primary = true;

-- Exactly one primary row is allowed per message.
CREATE UNIQUE INDEX IF NOT EXISTS idx_message_id_map_primary_per_message
    ON message_id_map (message_pk)
    WHERE is_primary;
