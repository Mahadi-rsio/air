CREATE TABLE IF NOT EXISTS tenant_events (
    version BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    state TEXT NOT NULL,
    host TEXT NOT NULL DEFAULT '',
    direct_port INT NOT NULL DEFAULT 0,
    fencing_epoch BIGINT NOT NULL DEFAULT 1,
    emitted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tenant_events_tenant_id ON tenant_events(tenant_id, version);
