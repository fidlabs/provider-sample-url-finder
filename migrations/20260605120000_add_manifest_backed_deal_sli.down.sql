ALTER TABLE deal_sli_piece_results
    DROP CONSTRAINT deal_sli_piece_results_manifest_snapshot_fk,
    DROP COLUMN manifest_response_time_ms,
    DROP COLUMN size_matched,
    DROP COLUMN observed_size_bytes,
    DROP COLUMN file_size_bytes,
    DROP COLUMN manifest_snapshot_id;

ALTER TABLE deal_sli_runs
    DROP CONSTRAINT deal_sli_runs_manifest_snapshot_fk,
    DROP COLUMN avg_response_time_ms,
    DROP COLUMN size_matched_percent,
    DROP COLUMN sampled_piece_count,
    DROP COLUMN content_matches_deal,
    DROP COLUMN manifest_size_bytes,
    DROP COLUMN deal_size_bytes,
    DROP COLUMN manifest_snapshot_id;

ALTER TABLE deal_sli_pieces
    DROP CONSTRAINT deal_sli_pieces_manifest_snapshot_fk,
    DROP COLUMN piece_type,
    DROP COLUMN storage_path,
    DROP COLUMN root_cid,
    DROP COLUMN file_size_bytes,
    DROP COLUMN manifest_snapshot_id;

ALTER TABLE deal_sli_targets
    DROP CONSTRAINT deal_sli_targets_active_manifest_snapshot_fk,
    DROP COLUMN active_manifest_snapshot_id,
    DROP COLUMN deal_size_bytes;

DROP TABLE deal_sli_manifest_snapshots;
