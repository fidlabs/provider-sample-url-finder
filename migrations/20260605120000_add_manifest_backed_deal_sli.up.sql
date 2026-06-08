CREATE TABLE deal_sli_manifest_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id TEXT NOT NULL REFERENCES deal_sli_targets(deal_id) ON DELETE CASCADE,
    manifest_hash TEXT NOT NULL,
    manifest_location TEXT NOT NULL,
    raw_content TEXT NOT NULL,
    parsed_content JSONB NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content_byte_length BIGINT NOT NULL,
    computed_hash TEXT NOT NULL,
    CONSTRAINT deal_sli_manifest_snapshots_deal_id_id_unique UNIQUE (deal_id, id)
);

ALTER TABLE deal_sli_targets
    ADD COLUMN deal_size_bytes NUMERIC(78, 0),
    ADD COLUMN active_manifest_snapshot_id UUID;

ALTER TABLE deal_sli_targets
    ADD CONSTRAINT deal_sli_targets_active_manifest_snapshot_fk
    FOREIGN KEY (deal_id, active_manifest_snapshot_id)
    REFERENCES deal_sli_manifest_snapshots(deal_id, id)
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE deal_sli_pieces
    ADD COLUMN manifest_snapshot_id UUID,
    ADD COLUMN file_size_bytes NUMERIC(78, 0),
    ADD COLUMN root_cid TEXT,
    ADD COLUMN storage_path TEXT,
    ADD COLUMN piece_type TEXT;

ALTER TABLE deal_sli_pieces
    ADD CONSTRAINT deal_sli_pieces_manifest_snapshot_fk
    FOREIGN KEY (deal_id, manifest_snapshot_id)
    REFERENCES deal_sli_manifest_snapshots(deal_id, id)
    ON DELETE CASCADE;

ALTER TABLE deal_sli_runs
    ADD COLUMN manifest_snapshot_id UUID,
    ADD COLUMN deal_size_bytes NUMERIC(78, 0),
    ADD COLUMN manifest_size_bytes NUMERIC(78, 0),
    ADD COLUMN content_matches_deal BOOLEAN,
    ADD COLUMN sampled_piece_count INTEGER,
    ADD COLUMN size_matched_percent NUMERIC(5, 2),
    ADD COLUMN avg_response_time_ms NUMERIC(12, 2);

ALTER TABLE deal_sli_runs
    ADD CONSTRAINT deal_sli_runs_manifest_snapshot_fk
    FOREIGN KEY (deal_id, manifest_snapshot_id)
    REFERENCES deal_sli_manifest_snapshots(deal_id, id);

ALTER TABLE deal_sli_piece_results
    ADD COLUMN manifest_snapshot_id UUID,
    ADD COLUMN file_size_bytes NUMERIC(78, 0),
    ADD COLUMN observed_size_bytes BIGINT,
    ADD COLUMN size_matched BOOLEAN,
    ADD COLUMN manifest_response_time_ms BIGINT;

ALTER TABLE deal_sli_piece_results
    ADD CONSTRAINT deal_sli_piece_results_manifest_snapshot_fk
    FOREIGN KEY (deal_id, manifest_snapshot_id)
    REFERENCES deal_sli_manifest_snapshots(deal_id, id);

CREATE INDEX idx_deal_sli_manifest_snapshots_deal_id
    ON deal_sli_manifest_snapshots(deal_id);

CREATE INDEX idx_deal_sli_pieces_manifest_snapshot_id
    ON deal_sli_pieces(manifest_snapshot_id);
