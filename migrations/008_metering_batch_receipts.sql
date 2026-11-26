-- AIR Control Plane Metadata Schema (Version 8)
-- Additive migration for idempotent Gateway metering batch receipts.

CREATE TABLE IF NOT EXISTS metering_batch_receipts (
    batch_id TEXT PRIMARY KEY,
    gateway_id TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    entry_count BIGINT NOT NULL,
    tenant_count INT NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_metering_batch_receipts_gateway
    ON metering_batch_receipts(gateway_id, committed_at DESC);
