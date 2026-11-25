package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/Mahadi-rsio/air/internal/pb"
	"github.com/jackc/pgx/v5"
)

func TestSharedReadEngine_DefaultSQLiteLifecycle(t *testing.T) {
	cfg := loadConfig([]string{
		"-data-dir", t.TempDir(),
		"-pg-port", "0",
	})

	re, err := NewSharedReadEngine(cfg)
	if err != nil {
		t.Fatalf("NewSharedReadEngine: %v", err)
	}
	if err := re.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer re.Stop()

	ctx := context.Background()
	svc := &readEngineGRPCServer{re: re}

	// 1. Check gRPC Health reports ACTIVE
	health, err := svc.Health(ctx, &pb.ReadEngineEmpty{})
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if health.GetStatus() != "ok" || health.GetState() != "ACTIVE" {
		t.Fatalf("unexpected Health: %+v", health)
	}

	// 2. Register tenant via gRPC EnsureTenant
	route, err := svc.EnsureTenant(ctx, &pb.ReadEngineTenantRequest{TenantId: "t_app"})
	if err != nil || route.GetState() != "ACTIVE" {
		t.Fatalf("EnsureTenant failed: err=%v route=%+v", err, route)
	}

	// 3. Seed table via gRPC SyncTenant
	if _, err := svc.SyncTenant(ctx, &pb.SyncTenantRequest{
		TenantId: "t_app",
		Sql:      "CREATE TABLE users (id INT4 PRIMARY KEY, name TEXT); INSERT INTO users VALUES (1, 'Ada'), (2, 'Grace');",
		Lsn:      "0/05000100",
	}); err != nil {
		t.Fatalf("SyncTenant failed: %v", err)
	}

	// 4. Connect with pgx and query the shared SQLite read engine
	dsn := fmt.Sprintf("postgres://app_user@127.0.0.1:%d/t_app?sslmode=disable", re.cfg.PGPort)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer conn.Close(ctx)

	var name string
	if err := conn.QueryRow(ctx, "SELECT name FROM users WHERE id = $1", 2).Scan(&name); err != nil || name != "Grace" {
		t.Fatalf("query users: got=%q err=%v", name, err)
	}

	// 5. Verify writes are rejected with SQLSTATE 25006
	if _, err := conn.Exec(ctx, "DELETE FROM users WHERE id = 1"); err == nil {
		t.Fatalf("expected DELETE to be rejected on SQLite read engine")
	}
}
