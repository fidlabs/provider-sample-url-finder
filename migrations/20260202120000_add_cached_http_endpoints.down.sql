ALTER TABLE storage_providers DROP COLUMN IF EXISTS cached_http_endpoints;
ALTER TABLE storage_providers DROP COLUMN IF EXISTS endpoints_fetched_at;

ALTER TABLE storage_providers ADD COLUMN peer_id_fetched_at TIMESTAMPTZ;
CREATE INDEX idx_storage_providers_peer_id_stale
ON storage_providers (peer_id_fetched_at)
WHERE peer_id IS NOT NULL;
