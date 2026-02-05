ALTER TABLE storage_providers ADD COLUMN cached_http_endpoints TEXT[];
ALTER TABLE storage_providers ADD COLUMN endpoints_fetched_at TIMESTAMPTZ;

DROP INDEX IF EXISTS idx_storage_providers_peer_id_stale;
ALTER TABLE storage_providers DROP COLUMN IF EXISTS peer_id_fetched_at;
