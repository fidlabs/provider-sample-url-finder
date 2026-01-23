-- Add URL validation columns to storage_providers
ALTER TABLE storage_providers
    ADD COLUMN is_consistent BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN url_metadata JSONB;

-- Force re-validation for existing providers with URLs
-- These were discovered before Content-Length validation was added
UPDATE storage_providers
SET is_consistent = false
WHERE last_working_url IS NOT NULL;

CREATE INDEX idx_sp_consistent_bms
    ON storage_providers(next_bms_test_at)
    WHERE is_consistent = true AND last_working_url IS NOT NULL;
