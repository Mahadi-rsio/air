package control

import (
	"context"
	"fmt"
)

const currentSchemaVersion = 6

func (cp *ControlPlane) runMigrations(ctx context.Context) error {
	if _, err := cp.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return err
	}
	var current int
	_ = cp.db.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current)
	if current < 1 {
		tx, err := cp.db.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS clusters (
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
			)`,
			`CREATE TABLE IF NOT EXISTS idempotency_records (
				account_id TEXT NOT NULL,
				idempotency_key TEXT NOT NULL,
				tenant_id TEXT NOT NULL,
				payload_hash TEXT NOT NULL,
				response_json JSONB NOT NULL,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				PRIMARY KEY (account_id, idempotency_key)
			)`,
			`CREATE TABLE IF NOT EXISTS provisioning_ops (
				op_id BIGSERIAL PRIMARY KEY,
				account_id TEXT NOT NULL,
				tenant_id TEXT NOT NULL,
				idempotency_key TEXT NOT NULL,
				state TEXT NOT NULL,
				last_error TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`INSERT INTO schema_migrations(version) VALUES (1) ON CONFLICT DO NOTHING`,
		}
		for _, s := range stmts {
			if _, err := tx.Exec(ctx, s); err != nil {
				return fmt.Errorf("migration v1 failed: %w", err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	if current < 2 {
		tx, err := cp.db.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS api_keys (
				key_id TEXT PRIMARY KEY,
				key_sha256 TEXT UNIQUE NOT NULL,
				account_id TEXT NOT NULL,
				description TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				revoked_at TIMESTAMPTZ
			)`,
			`CREATE INDEX IF NOT EXISTS idx_api_keys_account_id ON api_keys(account_id)`,
			`INSERT INTO schema_migrations(version) VALUES (2) ON CONFLICT DO NOTHING`,
		}
		for _, s := range stmts {
			if _, err := tx.Exec(ctx, s); err != nil {
				return fmt.Errorf("migration v2 failed: %w", err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	if current < 3 {
		tx, err := cp.db.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS tenant_events (
				version BIGSERIAL PRIMARY KEY,
				tenant_id TEXT NOT NULL,
				event_type TEXT NOT NULL,
				state TEXT NOT NULL,
				host TEXT NOT NULL DEFAULT '',
				direct_port INT NOT NULL DEFAULT 0,
				fencing_epoch BIGINT NOT NULL DEFAULT 1,
				emitted_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`CREATE INDEX IF NOT EXISTS idx_tenant_events_tenant_id ON tenant_events(tenant_id, version)`,
			`INSERT INTO schema_migrations(version) VALUES (3) ON CONFLICT DO NOTHING`,
		}
		for _, s := range stmts {
			if _, err := tx.Exec(ctx, s); err != nil {
				return fmt.Errorf("migration v3 failed: %w", err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	if current < 4 {
		tx, err := cp.db.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		stmts := []string{
			`ALTER TABLE clusters ADD COLUMN IF NOT EXISTS generation TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE clusters ADD COLUMN IF NOT EXISTS lifecycle_seq BIGINT NOT NULL DEFAULT 1`,
			`CREATE TABLE IF NOT EXISTS offload_ops (
				op_id TEXT PRIMARY KEY,
				tenant_id TEXT NOT NULL,
				generation TEXT NOT NULL DEFAULT '',
				state TEXT NOT NULL,
				started_at TIMESTAMPTZ,
				completed_at TIMESTAMPTZ,
				uploaded_objects INT NOT NULL DEFAULT 0,
				failed_objects INT NOT NULL DEFAULT 0,
				error_details TEXT NOT NULL DEFAULT '',
				retry_count INT NOT NULL DEFAULT 0,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`CREATE INDEX IF NOT EXISTS idx_offload_ops_tenant ON offload_ops(tenant_id, created_at DESC)`,
			`INSERT INTO schema_migrations(version) VALUES (4) ON CONFLICT DO NOTHING`,
		}
		for _, s := range stmts {
			if _, err := tx.Exec(ctx, s); err != nil {
				return fmt.Errorf("migration v4 failed: %w", err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	if current < 5 {
		tx, err := cp.db.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS backup_ops (
				backup_id TEXT PRIMARY KEY,
				tenant_id TEXT NOT NULL,
				generation TEXT NOT NULL DEFAULT '',
				parent_id TEXT NOT NULL DEFAULT '',
				backup_type TEXT NOT NULL,
				state TEXT NOT NULL,
				manifest_key TEXT NOT NULL DEFAULT '',
				base_object_key TEXT NOT NULL DEFAULT '',
				wal_start_lsn TEXT NOT NULL DEFAULT '',
				wal_end_lsn TEXT NOT NULL DEFAULT '',
				consistency_lsn TEXT NOT NULL DEFAULT '',
				cipher_bytes BIGINT NOT NULL DEFAULT 0,
				wal_segments_count INT NOT NULL DEFAULT 0,
				verified BOOLEAN NOT NULL DEFAULT FALSE,
				error_details TEXT NOT NULL DEFAULT '',
				started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				completed_at TIMESTAMPTZ,
				updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`CREATE INDEX IF NOT EXISTS idx_backup_ops_tenant ON backup_ops(tenant_id, started_at DESC)`,
			`CREATE TABLE IF NOT EXISTS restore_ops (
				restore_id TEXT PRIMARY KEY,
				source_tenant_id TEXT NOT NULL,
				source_generation TEXT NOT NULL DEFAULT '',
				target_tenant_id TEXT NOT NULL DEFAULT '',
				target_generation TEXT NOT NULL DEFAULT '',
				backup_id TEXT NOT NULL DEFAULT '',
				recovery_target TEXT NOT NULL DEFAULT '',
				state TEXT NOT NULL,
				error_details TEXT NOT NULL DEFAULT '',
				started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				completed_at TIMESTAMPTZ,
				updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`CREATE INDEX IF NOT EXISTS idx_restore_ops_source ON restore_ops(source_tenant_id, started_at DESC)`,
			`INSERT INTO schema_migrations(version) VALUES (5) ON CONFLICT DO NOTHING`,
		}
		for _, s := range stmts {
			if _, err := tx.Exec(ctx, s); err != nil {
				return fmt.Errorf("migration v5 failed: %w", err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	if current < 6 {
		tx, err := cp.db.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS query_metering (
				id BIGSERIAL PRIMARY KEY,
				recorded_at TIMESTAMPTZ NOT NULL,
				db_id TEXT NOT NULL,
				bandwidth_bytes BIGINT NOT NULL,
				query_time_us BIGINT NOT NULL,
				query_type TEXT NOT NULL,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`CREATE INDEX IF NOT EXISTS idx_query_metering_db_recorded ON query_metering(db_id, recorded_at DESC)`,
			`CREATE INDEX IF NOT EXISTS idx_query_metering_recorded ON query_metering(recorded_at DESC)`,
			`INSERT INTO schema_migrations(version) VALUES (6) ON CONFLICT DO NOTHING`,
		}
		for _, s := range stmts {
			if _, err := tx.Exec(ctx, s); err != nil {
				return fmt.Errorf("migration v6 failed: %w", err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	cp.reconcileInterruptedBackupAndRestoreOps(ctx)
	return nil
}

// reconcileInterruptedBackupAndRestoreOps marks any non-terminal backup or restore operation
// left over from a prior control-plane crash as failed on startup so unverified backups are
// never reported as restorable.
func (cp *ControlPlane) reconcileInterruptedBackupAndRestoreOps(ctx context.Context) {
	if cp.db == nil {
		return
	}
	_, _ = cp.db.Exec(ctx,
		`UPDATE backup_ops SET state = 'failed', completed_at = COALESCE(completed_at, now()),
		 error_details = CASE WHEN error_details = '' THEN 'interrupted by control-plane restart before manifest publication' ELSE error_details END,
		 updated_at = now()
		 WHERE state IN ('pending', 'running', 'verifying')`,
	)
	_, _ = cp.db.Exec(ctx,
		`UPDATE restore_ops SET state = 'failed', completed_at = COALESCE(completed_at, now()),
		 error_details = CASE WHEN error_details = '' THEN 'interrupted by control-plane restart' ELSE error_details END,
		 updated_at = now()
		 WHERE state IN ('pending', 'running')`,
	)
}

// SchemaVersion returns the highest applied schema migration version.
func (cp *ControlPlane) SchemaVersion(ctx context.Context) (int, error) {
	var v int
	err := cp.db.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&v)
	return v, err
}
