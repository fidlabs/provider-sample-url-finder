UPDATE url_results
SET retrievability_percent = 0.0
WHERE retrievability_percent IS NULL;

ALTER TABLE url_results
    ALTER COLUMN retrievability_percent SET NOT NULL,
    ALTER COLUMN retrievability_percent SET DEFAULT 0.0;

ALTER TABLE url_results
    DROP CONSTRAINT IF EXISTS url_results_retrievability_percent_check;

ALTER TABLE url_results
    ADD CONSTRAINT url_results_retrievability_percent_check
    CHECK (retrievability_percent >= 0.0 AND retrievability_percent <= 100.0);

UPDATE storage_providers SET is_consistent = false WHERE is_consistent IS NULL;
ALTER TABLE storage_providers
    ALTER COLUMN is_consistent SET NOT NULL,
    ALTER COLUMN is_consistent SET DEFAULT true;

UPDATE storage_providers SET is_reliable = false WHERE is_reliable IS NULL;
ALTER TABLE storage_providers
    ALTER COLUMN is_reliable SET NOT NULL,
    ALTER COLUMN is_reliable SET DEFAULT true;
