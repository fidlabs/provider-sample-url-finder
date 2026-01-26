DROP INDEX IF EXISTS idx_storage_providers_peer_id_stale;
DROP INDEX IF EXISTS idx_storage_providers_peer_id_null;
ALTER TABLE storage_providers DROP COLUMN IF EXISTS peer_id_fetched_at;
ALTER TABLE storage_providers DROP COLUMN IF EXISTS peer_id;
