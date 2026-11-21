package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

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

	ts := httptest.NewServer(re.HTTPHandler())
	defer ts.Close()

	// 1. Check /healthz reports engine: "sqlite"
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	var health map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&health)
	_ = resp.Body.Close()
	if health["engine"] != "sqlite" || health["state"] != "ACTIVE" {
		t.Fatalf("unexpected /healthz: %+v", health)
	}

	// 2. Register tenant via POST /v1/tenants/t_app/register
	regResp, err := http.Post(ts.URL+"/v1/tenants/t_app/register", "application/json", nil)
	if err != nil || regResp.StatusCode != http.StatusOK {
		t.Fatalf("POST register failed: err=%v status=%v", err, regResp)
	}
	_ = regResp.Body.Close()

	// 3. Seed table via POST /v1/tenants/t_app/sync
	syncBody, _ := json.Marshal(map[string]string{
		"sql": "CREATE TABLE users (id INT4 PRIMARY KEY, name TEXT); INSERT INTO users VALUES (1, 'Ada'), (2, 'Grace');",
		"lsn": "0/05000100",
	})
	syncResp, err := http.Post(ts.URL+"/v1/tenants/t_app/sync", "application/json", bytes.NewReader(syncBody))
	if err != nil || syncResp.StatusCode != http.StatusOK {
		t.Fatalf("POST sync failed: err=%v status=%v", err, syncResp)
	}
	_ = syncResp.Body.Close()

	// 4. Connect with pgx and query the shared SQLite read engine
	ctx := context.Background()
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
