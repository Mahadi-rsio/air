package readadapter

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPhase31_RejectUnknownCastsAndRoleChanges(t *testing.T) {
	// 1. Direct rewriter strictness: unknown, array, and schema-qualified casts must error,
	// never silently convert to TEXT.
	badCasts := []string{
		"SELECT 'active'::status_enum",
		"SELECT '123'::public.pos_int",
		"SELECT '{1,2}'::int[]",
		"SELECT CAST(1 AS my_domain)",
		"SELECT CAST(1 AS pg_catalog.int4)",
	}
	for _, q := range badCasts {
		if _, _, err := translatePGToSQLiteStrict(q); err == nil {
			t.Fatalf("expected translatePGToSQLiteStrict(%q) to fail, got nil", q)
		}
	}

	// Known casts still succeed.
	goodCasts := []string{
		"SELECT '42'::int4",
		"SELECT 'true'::boolean",
		"SELECT CAST(12.5 AS numeric(10,2))",
		"SELECT 'hello'::text",
	}
	for _, q := range goodCasts {
		if _, _, err := translatePGToSQLiteStrict(q); err != nil {
			t.Fatalf("expected translatePGToSQLiteStrict(%q) to succeed, got %v", q, err)
		}
	}

	// 2. End-to-end SQLiteReadServer rejects unknown casts, SET ROLE, and unknown SHOW GUCs.
	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost:   "127.0.0.1",
		PGPort:       0,
		DataDir:      t.TempDir(),
		QueryTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("srv.Start: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/tenant_p31?sslmode=disable", srv.Port())
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, "SELECT 'abc'::unknown_custom_type"); err == nil {
		t.Fatalf("expected error on unknown cast, got nil")
	}
	if _, err := conn.Exec(ctx, "SET ROLE some_role"); err == nil || !strings.Contains(err.Error(), "ROLE") {
		t.Fatalf("expected SET ROLE rejection, got %v", err)
	}
	if _, err := conn.Exec(ctx, "SHOW nonexistent_guc_param"); err == nil {
		t.Fatalf("expected error on unknown SHOW GUC, got nil")
	}
}
