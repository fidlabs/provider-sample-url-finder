CREATE TABLE bms_bandwidth_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_id VARCHAR(255) NOT NULL,
    bms_job_id UUID NOT NULL UNIQUE,
    url_tested TEXT NOT NULL,
    routing_key VARCHAR(50) NOT NULL,
    worker_count INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL,
    ping_avg_ms NUMERIC(10, 3),
    head_avg_ms NUMERIC(10, 3),
    ttfb_ms NUMERIC(10, 3),
    download_speed_mbps NUMERIC(10, 2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_bms_results_provider ON bms_bandwidth_results(provider_id, created_at DESC);
CREATE INDEX idx_bms_results_job_id ON bms_bandwidth_results(bms_job_id);
