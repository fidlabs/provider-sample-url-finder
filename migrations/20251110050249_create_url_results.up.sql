CREATE TABLE url_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_id VARCHAR(255) NOT NULL,
    client_id VARCHAR(255),
    result_type VARCHAR(50) NOT NULL,

    working_url TEXT,
    retrievability_percent DOUBLE PRECISION NOT NULL DEFAULT 0.0,

    result_code VARCHAR(100) NOT NULL,
    error_code VARCHAR(100),

    tested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_url_results_provider ON url_results(provider_id, tested_at DESC);
CREATE INDEX idx_url_results_pair ON url_results(provider_id, client_id, tested_at DESC);
CREATE INDEX idx_url_results_client ON url_results(client_id, tested_at DESC)
    WHERE client_id IS NOT NULL;
CREATE INDEX idx_url_results_tested_at ON url_results(tested_at DESC);
CREATE INDEX idx_url_results_result_type ON url_results(result_type, tested_at DESC);
