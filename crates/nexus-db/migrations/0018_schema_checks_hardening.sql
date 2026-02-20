-- Tighten schema-level invariants for lineage and embeddings.

ALTER TABLE patch_items
DROP CONSTRAINT IF EXISTS patch_items_item_type_check;

ALTER TABLE patch_items
ADD CONSTRAINT patch_items_item_type_check
CHECK (item_type IN ('patch', 'cover'))
NOT VALID;

ALTER TABLE patch_items
VALIDATE CONSTRAINT patch_items_item_type_check;

ALTER TABLE patch_series_versions
DROP CONSTRAINT IF EXISTS patch_series_versions_version_num_positive;

ALTER TABLE patch_series_versions
ADD CONSTRAINT patch_series_versions_version_num_positive
CHECK (version_num >= 1)
NOT VALID;

ALTER TABLE patch_series_versions
VALIDATE CONSTRAINT patch_series_versions_version_num_positive;

ALTER TABLE search_doc_embeddings
DROP CONSTRAINT IF EXISTS search_doc_embeddings_vector_dimensions_check;

ALTER TABLE search_doc_embeddings
ADD CONSTRAINT search_doc_embeddings_vector_dimensions_check
CHECK (COALESCE(array_length(vector, 1), 0) = dimensions)
NOT VALID;

ALTER TABLE search_doc_embeddings
VALIDATE CONSTRAINT search_doc_embeddings_vector_dimensions_check;
