-- Add sector utilization tracking to url_results
ALTER TABLE url_results
    ADD COLUMN sector_utilization_percent NUMERIC(5, 2);
