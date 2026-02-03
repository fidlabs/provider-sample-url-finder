ALTER TABLE url_results
    ALTER COLUMN retrievability_percent DROP NOT NULL,
    ALTER COLUMN retrievability_percent DROP DEFAULT;

ALTER TABLE url_results
    DROP CONSTRAINT IF EXISTS url_results_retrievability_percent_check;

ALTER TABLE url_results
    ADD CONSTRAINT url_results_retrievability_percent_check
    CHECK (retrievability_percent IS NULL OR (retrievability_percent >= 0.0 AND retrievability_percent <= 100.0));

ALTER TABLE storage_providers
    ALTER COLUMN is_consistent DROP NOT NULL,
    ALTER COLUMN is_consistent DROP DEFAULT;

ALTER TABLE storage_providers
    ALTER COLUMN is_reliable DROP NOT NULL,
    ALTER COLUMN is_reliable DROP DEFAULT;

UPDATE url_results
SET retrievability_percent = NULL
WHERE result_code IN (
    'NoCidContactData',
    'MissingAddrFromCidContact',
    'MissingHttpAddrFromCidContact',
    'NoDealsFound',
    'Error'
);
