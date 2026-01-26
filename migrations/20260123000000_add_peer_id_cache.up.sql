ALTER TABLE storage_providers
ADD COLUMN peer_id TEXT,
ADD COLUMN peer_id_fetched_at TIMESTAMPTZ;

CREATE INDEX idx_storage_providers_peer_id_null
ON storage_providers (created_at)
WHERE peer_id IS NULL;

CREATE INDEX idx_storage_providers_peer_id_stale
ON storage_providers (peer_id_fetched_at)
WHERE peer_id IS NOT NULL;
