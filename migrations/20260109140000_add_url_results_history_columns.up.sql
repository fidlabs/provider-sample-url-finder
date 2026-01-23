-- Add columns to support historical data retrieval with extended details
ALTER TABLE url_results
    ADD COLUMN is_consistent BOOLEAN,
    ADD COLUMN is_reliable BOOLEAN,
    ADD COLUMN url_metadata JSONB;
