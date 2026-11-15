-- AIR v1.1 Control Plane Metadata Schema (Version 2: Bearer API Keys)
-- Applied transactionally by internal/control/control.go#runMigrations

CREATE TABLE IF NOT EXISTS api_keys (
    key_id TEXT PRIMARY KEY,
    key_sha256 TEXT UNIQUE NOT NULL,
    account_id TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_api_keys_account_id ON api_keys(account_id);
