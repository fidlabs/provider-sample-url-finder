ALTER TABLE url_results
    DROP COLUMN IF EXISTS is_consistent,
    DROP COLUMN IF EXISTS is_reliable,
    DROP COLUMN IF EXISTS url_metadata;
