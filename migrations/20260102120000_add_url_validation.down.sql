-- Remove URL validation columns from storage_providers
DROP INDEX IF EXISTS idx_sp_consistent_bms;
ALTER TABLE storage_providers
    DROP COLUMN IF EXISTS is_consistent,
    DROP COLUMN IF EXISTS url_metadata;
