-- Add is_reliable column for tracking connection stability
ALTER TABLE storage_providers
ADD COLUMN is_reliable BOOLEAN NOT NULL DEFAULT true;

-- Update BMS eligibility index to require both consistent AND reliable
DROP INDEX IF EXISTS idx_sp_consistent_bms;

CREATE INDEX idx_sp_bms_eligible ON storage_providers (
    next_bms_test_at,
    bms_test_status
) WHERE
    is_consistent = true
    AND is_reliable = true
    AND last_working_url IS NOT NULL;

COMMENT ON COLUMN storage_providers.is_reliable IS
    'False if timeout rate exceeds 30% during URL discovery';
