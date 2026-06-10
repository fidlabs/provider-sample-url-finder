CREATE TABLE deal_sli_target_schedules (
    deal_id TEXT PRIMARY KEY REFERENCES deal_sli_targets(deal_id) ON DELETE CASCADE,
    next_run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO
    deal_sli_target_schedules (deal_id)
SELECT
    deal_id
FROM
    deal_sli_targets
ON CONFLICT (deal_id) DO NOTHING;

CREATE INDEX idx_deal_sli_target_schedules_next_run
    ON deal_sli_target_schedules (next_run_at, deal_id);

CREATE TABLE deal_sli_bms_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id TEXT NOT NULL,
    run_id UUID NOT NULL,
    piece_index INTEGER NOT NULL,
    piece_cid TEXT NOT NULL,
    bms_job_id UUID NOT NULL UNIQUE,
    url_tested TEXT NOT NULL,
    routing_key VARCHAR(50) NOT NULL,
    worker_count INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL,
    ping_avg_ms NUMERIC(10, 3),
    head_avg_ms NUMERIC(10, 3),
    ttfb_ms NUMERIC(10, 3),
    download_speed_mbps NUMERIC(10, 2),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    FOREIGN KEY (run_id, deal_id)
        REFERENCES deal_sli_runs(id, deal_id) ON DELETE CASCADE,
    FOREIGN KEY (deal_id, piece_index)
        REFERENCES deal_sli_pieces(deal_id, piece_index),
    CONSTRAINT deal_sli_bms_jobs_piece_index_check CHECK (piece_index >= 0),
    CONSTRAINT deal_sli_bms_jobs_worker_count_check CHECK (worker_count >= 0)
);

CREATE INDEX idx_deal_sli_bms_jobs_deal_run
    ON deal_sli_bms_jobs (deal_id, run_id);

CREATE INDEX idx_deal_sli_bms_jobs_status
    ON deal_sli_bms_jobs (status, created_at);
