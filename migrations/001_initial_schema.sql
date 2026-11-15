-- AIR v1 Control Plane Metadata Schema (Version 1)
-- Applied transactionally by internal/control/control.go#runMigrations

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS clusters (
    tenant_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    state TEXT NOT NULL,
    direct_port INT NOT NULL DEFAULT 0,
    system_identifier TEXT NOT NULL DEFAULT '',
    fencing_epoch BIGINT NOT NULL DEFAULT 1,
    app_password_enc TEXT NOT NULL DEFAULT '',
    wrapped_dek TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS idempotency_records (
    account_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    response_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS provisioning_ops (
    op_id BIGSERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    state TEXT NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
