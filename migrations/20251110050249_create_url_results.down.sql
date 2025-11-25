DROP INDEX IF EXISTS idx_url_results_result_type;
DROP INDEX IF EXISTS idx_url_results_tested_at;
DROP INDEX IF EXISTS idx_url_results_client;
DROP INDEX IF EXISTS idx_url_results_pair;
DROP INDEX IF EXISTS idx_url_results_provider;

DROP TABLE IF EXISTS url_results;

DROP TYPE IF EXISTS error_code;
DROP TYPE IF EXISTS result_code;
DROP TYPE IF EXISTS discovery_type;