CREATE TABLE deal_sli_targets (
    deal_id TEXT PRIMARY KEY,
    deal_version TEXT NOT NULL DEFAULT 'v2',
    provider_id TEXT NOT NULL,
    client_id TEXT,
    manifest_hash TEXT,
    manifest_location TEXT,
    retrievability_bps INTEGER,
    bandwidth_mbps INTEGER,
    latency_ms INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT deal_sli_targets_deal_version_check CHECK (deal_version = 'v2'),
    CONSTRAINT deal_sli_targets_deal_id_decimal_check CHECK (deal_id ~ '^[0-9]+$'),
    CONSTRAINT deal_sli_targets_retri_bps_check CHECK (
        retrievability_bps IS NULL
        OR (retrievability_bps >= 0 AND retrievability_bps <= 10000)
    ),
    CONSTRAINT deal_sli_targets_bandwidth_check CHECK (
        bandwidth_mbps IS NULL OR bandwidth_mbps >= 0
    ),
    CONSTRAINT deal_sli_targets_latency_check CHECK (
        latency_ms IS NULL OR latency_ms >= 0
    )
);

CREATE TABLE deal_sli_pieces (
    deal_id TEXT NOT NULL REFERENCES deal_sli_targets(deal_id) ON DELETE CASCADE,
    piece_index INTEGER NOT NULL,
    piece_cid TEXT NOT NULL,
    piece_size_bytes NUMERIC(78, 0),
    allocation_id TEXT,
    claim_id TEXT,
    PRIMARY KEY (deal_id, piece_index),
    CONSTRAINT deal_sli_pieces_index_check CHECK (piece_index >= 0),
    CONSTRAINT deal_sli_pieces_size_check CHECK (
        piece_size_bytes IS NULL OR piece_size_bytes >= 0
    )
);

CREATE TABLE deal_sli_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id TEXT NOT NULL REFERENCES deal_sli_targets(deal_id) ON DELETE CASCADE,
    state TEXT NOT NULL,
    measurement_state TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    tested_at TIMESTAMPTZ,
    provider_id TEXT NOT NULL,
    client_id TEXT,
    working_url TEXT,
    retrievability_percent NUMERIC(5, 2),
    large_files_percent NUMERIC(5, 2),
    car_files_percent NUMERIC(5, 2),
    sector_utilization_percent NUMERIC(5, 2),
    is_consistent BOOLEAN,
    is_reliable BOOLEAN,
    result_code result_code,
    error_code error_code,
    piece_count INTEGER NOT NULL,
    success_count INTEGER NOT NULL,
    failed_count INTEGER NOT NULL,
    url_metadata JSONB,
    CONSTRAINT deal_sli_runs_state_check CHECK (state IN ('running', 'completed')),
    CONSTRAINT deal_sli_runs_measurement_state_check CHECK (
        measurement_state IN ('missing', 'fresh', 'stale', 'failed', 'skipped')
    ),
    CONSTRAINT deal_sli_runs_retri_check CHECK (
        retrievability_percent IS NULL
        OR (retrievability_percent >= 0.0 AND retrievability_percent <= 100.0)
    ),
    CONSTRAINT deal_sli_runs_large_check CHECK (
        large_files_percent IS NULL
        OR (large_files_percent >= 0.0 AND large_files_percent <= 100.0)
    ),
    CONSTRAINT deal_sli_runs_car_check CHECK (
        car_files_percent IS NULL
        OR (car_files_percent >= 0.0 AND car_files_percent <= 100.0)
    ),
    CONSTRAINT deal_sli_runs_sector_check CHECK (
        sector_utilization_percent IS NULL OR sector_utilization_percent >= 0.0
    ),
    CONSTRAINT deal_sli_runs_counts_check CHECK (
        piece_count >= 0
        AND success_count >= 0
        AND failed_count >= 0
        AND success_count + failed_count <= piece_count
    ),
    CONSTRAINT deal_sli_runs_id_deal_unique UNIQUE (id, deal_id)
);

CREATE TABLE deal_sli_piece_results (
    run_id UUID NOT NULL,
    deal_id TEXT NOT NULL,
    piece_index INTEGER NOT NULL,
    piece_cid TEXT NOT NULL,
    url_tested TEXT NOT NULL,
    success BOOLEAN NOT NULL,
    content_length BIGINT,
    is_valid_car BOOLEAN,
    result_code result_code,
    tested_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, piece_index, url_tested),
    FOREIGN KEY (run_id, deal_id)
        REFERENCES deal_sli_runs(id, deal_id) ON DELETE CASCADE,
    FOREIGN KEY (deal_id, piece_index)
        REFERENCES deal_sli_pieces(deal_id, piece_index),
    CONSTRAINT deal_sli_piece_results_content_length_check CHECK (
        content_length IS NULL OR content_length >= 0
    )
);

CREATE INDEX idx_deal_sli_runs_deal_started
    ON deal_sli_runs (deal_id, started_at DESC);

CREATE INDEX idx_deal_sli_runs_latest_completed
    ON deal_sli_runs (deal_id, completed_at DESC)
    WHERE state = 'completed';

CREATE INDEX idx_deal_sli_targets_provider
    ON deal_sli_targets (provider_id, updated_at DESC);

CREATE INDEX idx_deal_sli_piece_results_run
    ON deal_sli_piece_results (run_id);

CREATE INDEX idx_deal_sli_piece_results_deal
    ON deal_sli_piece_results (deal_id, piece_index);
