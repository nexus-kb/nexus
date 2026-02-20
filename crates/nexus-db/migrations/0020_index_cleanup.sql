-- Remove redundant index shadowed by idx_message_id_map_lookup_primary.
DROP INDEX IF EXISTS idx_message_id_map_message_id;
