-- Deal label cache for CAR header verification
-- Stores Label (payload CID) fetched from Lotus RPC
-- Data is immutable once deal is made, so cache forever

CREATE TABLE deal_labels (
    deal_id         INTEGER PRIMARY KEY,
    piece_cid       TEXT NOT NULL,
    label_raw       TEXT,
    payload_cid     TEXT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_deal_labels_piece_cid ON deal_labels(piece_cid);

COMMENT ON TABLE deal_labels IS 'Cache of deal Labels fetched from Lotus RPC for CAR header verification';
COMMENT ON COLUMN deal_labels.label_raw IS 'Raw Label value from DealProposal';
COMMENT ON COLUMN deal_labels.payload_cid IS 'Parsed CID if label_raw is valid CID format (bafy/bafk/Qm prefix)';
