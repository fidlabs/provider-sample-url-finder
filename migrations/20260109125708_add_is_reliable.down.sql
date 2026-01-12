DROP INDEX IF EXISTS idx_sp_bms_eligible;

CREATE INDEX idx_sp_consistent_bms ON storage_providers (
    next_bms_test_at,
    bms_test_status
) WHERE
    is_consistent = true
    AND last_working_url IS NOT NULL;

ALTER TABLE storage_providers DROP COLUMN is_reliable;
