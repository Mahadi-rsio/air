package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func waitForHTTP200(t *testing.T, url string, logPath string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	logBytes, _ := os.ReadFile(logPath)
	t.Fatalf("service at %s did not become healthy within %s;\nlog:\n%s", url, timeout, string(logBytes))
}

func buildBinary(t *testing.T, pkg, outPath string) {
	t.Helper()
	cmd := exec.Command("go", "build", "-o", outPath, pkg)
	cmd.Dir = "../.."
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build %s failed: %v\n%s", pkg, err, string(out))
	}
}

func stopCmd(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}

type readEngineHealth struct {
	Status         string `json:"status"`
	Engine         string `json:"engine"`
	Mode           string `json:"mode"`
	State          string `json:"state"`
	PGPort         int    `json:"pg_port"`
	SQLiteVersion  string `json:"sqlite_version"`
	QueriesAllowed uint64 `json:"queries_allowed"`
	WritesBlocked  uint64 `json:"writes_blocked"`
}

func getReadEngineHealth(t *testing.T, baseURL string) readEngineHealth {
	t.Helper()
	resp, err := http.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("GET %s/healthz: %v", baseURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s/healthz status %d: %s", baseURL, resp.StatusCode, string(body))
	}
	var h readEngineHealth
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		t.Fatalf("decode read-engine health: %v", err)
	}
	return h
}

func TestE2E_AirServer_Gateway_SQLiteReadEngine_FullStack(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/initdb"); err != nil {
		t.Fatalf("PostgreSQL 17 is required: %v", err)
	}

	rootDir, err := os.MkdirTemp("/tmp", "air_full_sqlite_e2e_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	serverLogPath := filepath.Join(rootDir, "air-server.log")
	gwLogPath := filepath.Join(rootDir, "air-gateway.log")
	defer func() {
		if t.Failed() {
			if b, err := os.ReadFile(serverLogPath); err == nil {
				t.Logf("=== air-server.log ===\n%s", string(b))
			}
			if b, err := os.ReadFile(gwLogPath); err == nil {
				t.Logf("=== air-gateway.log ===\n%s", string(b))
			}
		}
	}()
	defer func() {
		if t.Failed() {
			if b, err := os.ReadFile(serverLogPath); err == nil {
				t.Logf("=== air-server.log ===\n%s", string(b))
			}
			if b, err := os.ReadFile(gwLogPath); err == nil {
				t.Logf("=== air-gateway.log ===\n%s", string(b))
			}
		}
	}()

	binDir := filepath.Join(rootDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}

	airServerBin := filepath.Join(binDir, "air-server")
	airReadEngineBin := filepath.Join(binDir, "air-read-engine")
	airGatewayBin := filepath.Join(binDir, "air-gateway")

	buildBinary(t, "./cmd/air-server", airServerBin)
	buildBinary(t, "./cmd/air-read-engine", airReadEngineBin)
	buildBinary(t, "./cmd/air-gateway", airGatewayBin)

	serverHTTPAddr := freeTCPAddr(t)
	serverGRPCAddr := freeTCPAddr(t)
	metaPort := freeTCPPort(t)
	readEngineHTTPAddr := freeTCPAddr(t)
	readEnginePGPort := freeTCPPort(t)
	gwPGAddr := freeTCPAddr(t)
	gwAdminAddr := freeTCPAddr(t)
	gwAdminGRPCAddr := freeTCPAddr(t)

	const (
		bootstrapAccount = "acct_e2e_full"
		bootstrapAPIKey  = "air_e2e_secret_key_123456"
		internalToken    = "air_e2e_internal_token"
		tenantID         = "e2e_sqlite_db"
	)

	// 1. Start air-server
	serverDataDir := filepath.Join(rootDir, "air-server-data")
	serverLog, err := os.Create(serverLogPath)
	if err != nil {
		t.Fatal(err)
	}
	defer serverLog.Close()

	serverCmd := exec.Command(airServerBin,
		"-data-dir", serverDataDir,
		"-meta-dir", filepath.Join(serverDataDir, "meta"),
		"-master-key-path", filepath.Join(serverDataDir, "master.key"),
		"-http-addr", serverHTTPAddr,
		"-grpc-addr", serverGRPCAddr,
		"-meta-port", fmt.Sprintf("%d", metaPort),
		"-bootstrap-account-id", bootstrapAccount,
		"-bootstrap-api-key", bootstrapAPIKey,
		"-internal-token", internalToken,
		"-gateway-admin-grpc-addr", gwAdminGRPCAddr,
	)
	serverCmd.Stdout = serverLog
	serverCmd.Stderr = serverLog
	if err := serverCmd.Start(); err != nil {
		t.Fatalf("start air-server: %v", err)
	}
	defer stopCmd(serverCmd)
	t.Cleanup(func() {
		if t.Failed() {
			if b, err := os.ReadFile(serverLogPath); err == nil {
				t.Logf("=== air-server.log ===\n%s", string(b))
			}
		}
	})

	serverURL := "http://" + serverHTTPAddr
	waitForHTTP200(t, serverURL+"/healthz", serverLogPath, 30*time.Second)

	// 2. Start air-read-engine (shared SQLite read-only engine)
	readDataDir := filepath.Join(rootDir, "air-read-data")
	readLogPath := filepath.Join(rootDir, "air-read-engine.log")
	readLog, err := os.Create(readLogPath)
	if err != nil {
		t.Fatal(err)
	}
	defer readLog.Close()

	readCmd := exec.Command(airReadEngineBin,
		"-data-dir", readDataDir,
		"-http-addr", readEngineHTTPAddr,
		"-listen-host", "127.0.0.1",
		"-pg-port", fmt.Sprintf("%d", readEnginePGPort),
	)
	readCmd.Stdout = readLog
	readCmd.Stderr = readLog
	if err := readCmd.Start(); err != nil {
		t.Fatalf("start air-read-engine: %v", err)
	}
	defer stopCmd(readCmd)

	readEngineURL := "http://" + readEngineHTTPAddr
	waitForHTTP200(t, readEngineURL+"/healthz", readLogPath, 15*time.Second)
	rh0 := getReadEngineHealth(t, readEngineURL)
	t.Logf("air-read-engine started: sqlite=%s pg_port=%d state=%s", rh0.SQLiteVersion, rh0.PGPort, rh0.State)

	// 3. Start Go air-gateway wired to air-server gRPC and air-read-engine
	gwLog, err := os.Create(gwLogPath)
	if err != nil {
		t.Fatal(err)
	}
	defer gwLog.Close()

	gwCmd := exec.Command(airGatewayBin,
		"--gateway-addr", gwPGAddr,
		"--gateway-admin-addr", gwAdminAddr,
		"--gateway-admin-grpc-addr", gwAdminGRPCAddr,
		"--db-grpc-addr", serverGRPCAddr,
		"--control-plane-url", serverURL,
		"--internal-token", internalToken,
		"--backend-host", "127.0.0.1",
		"--pool-mode",
		"--pool-max-backends", "4",
		"--read-engine-host", "127.0.0.1",
		"--read-engine-port", fmt.Sprintf("%d", readEnginePGPort),
	)
	gwCmd.Stdout = gwLog
	gwCmd.Stderr = gwLog
	if err := gwCmd.Start(); err != nil {
		t.Fatalf("start air-gateway: %v", err)
	}
	defer stopCmd(gwCmd)

	gwAdminURL := "http://" + gwAdminAddr
	waitForHTTP200(t, gwAdminURL+"/healthz", gwLogPath, 15*time.Second)

	// 4. Create a new tenant database via air-server Control Plane
	prov := httpCreateDatabase(t, serverURL, bootstrapAPIKey, "idem-e2e-sqlite-1", tenantID)
	t.Logf("Created database %q: state=%s direct_port=%d user=%s", prov.TenantID, prov.State, prov.DirectPort, prov.Username)

	// Give gateway gRPC WatchTenantChanges a moment to discover the new tenant
	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gwDSN := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", prov.Username, prov.Password, gwPGAddr, tenantID)

	// 5. Connect through air-gateway and perform DDL + DML writes (routed to PG17 Write Engine)
	writeConn, err := pgx.Connect(ctx, gwDSN)
	if err != nil {
		gwLogBytes, _ := os.ReadFile(gwLogPath)
		t.Fatalf("pgx.Connect to gateway failed: %v\ngateway log:\n%s", err, string(gwLogBytes))
	}
	defer writeConn.Close(ctx)

	if _, err := writeConn.Exec(ctx, `
		CREATE TABLE orders (
			id INT PRIMARY KEY,
			item TEXT NOT NULL,
			amount INT NOT NULL
		);
		INSERT INTO orders (id, item, amount) VALUES (1, 'keyboard', 120), (2, 'mouse', 45);
	`, pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("CREATE/INSERT via gateway failed: %v", err)
	}

	// 5a. Causal fallback check: reading on the same write session before SQLite sync
	// must fall back to PG17 Write Engine and still see the newly inserted row.
	var item1 string
	var amt1 int
	if err := writeConn.QueryRow(ctx, "SELECT item, amount FROM orders WHERE id = 1").Scan(&item1, &amt1); err != nil {
		t.Fatalf("causal fallback SELECT on write session failed: %v", err)
	}
	if item1 != "keyboard" || amt1 != 120 {
		t.Fatalf("unexpected row from causal fallback: got (%q, %d)", item1, amt1)
	}

	// 6. Sync air-read-engine from the PG17 primary
	primaryConnInfo := fmt.Sprintf("host=127.0.0.1 port=%d user=%s password=%s dbname=%s sslmode=disable",
		prov.DirectPort, prov.Username, prov.Password, tenantID)
	syncBody, _ := json.Marshal(map[string]string{
		"primary_conninfo": primaryConnInfo,
	})
	syncResp, err := http.Post(readEngineURL+"/v1/tenants/"+tenantID+"/sync", "application/json", bytes.NewReader(syncBody))
	if err != nil {
		t.Fatalf("POST /v1/tenants/%s/sync failed: %v", tenantID, err)
	}
	syncRespBytes, _ := io.ReadAll(syncResp.Body)
	syncResp.Body.Close()
	if syncResp.StatusCode != http.StatusOK {
		t.Fatalf("sync failed (%d): %s", syncResp.StatusCode, string(syncRespBytes))
	}
	t.Logf("Synced SQLite read engine: %s", strings.TrimSpace(string(syncRespBytes)))

	// 7. Open a fresh read connection through air-gateway (sessionWriteLSN == 0)
	// and verify reads are routed to SQLiteReadServer!
	readConn, err := pgx.Connect(ctx, gwDSN)
	if err != nil {
		t.Fatalf("pgx.Connect readConn failed: %v", err)
	}
	defer readConn.Close(ctx)

	beforeHealth := getReadEngineHealth(t, readEngineURL)

	var item2 string
	var amt2 int
	if err := readConn.QueryRow(ctx, "SELECT item, amount FROM orders WHERE id = 2", pgx.QueryExecModeSimpleProtocol).Scan(&item2, &amt2); err != nil {
		t.Fatalf("simple SELECT via gateway -> SQLite failed: %v", err)
	}
	if item2 != "mouse" || amt2 != 45 {
		t.Fatalf("expected ('mouse', 45), got (%q, %d)", item2, amt2)
	}

	// Extended / parameterized query
	var extItem string
	if err := readConn.QueryRow(ctx, "SELECT item FROM orders WHERE amount > $1 ORDER BY id LIMIT 1", 100).Scan(&extItem); err != nil {
		t.Fatalf("extended SELECT via gateway -> SQLite failed: %v", err)
	}
	if extItem != "keyboard" {
		t.Fatalf("expected 'keyboard', got %q", extItem)
	}

	// Read-only CTE query
	var cnt int64
	if err := readConn.QueryRow(ctx, "WITH all_orders AS (SELECT id FROM orders) SELECT count(*) FROM all_orders", pgx.QueryExecModeSimpleProtocol).Scan(&cnt); err != nil {
		t.Fatalf("CTE SELECT via gateway -> SQLite failed: %v", err)
	}
	if cnt != 2 {
		t.Fatalf("expected count=2, got %d", cnt)
	}

	afterHealth := getReadEngineHealth(t, readEngineURL)
	if afterHealth.QueriesAllowed <= beforeHealth.QueriesAllowed {
		t.Fatalf("expected SQLite queries_allowed to increment (before=%d, after=%d)",
			beforeHealth.QueriesAllowed, afterHealth.QueriesAllowed)
	}
	t.Logf("SQLite read engine queries_allowed: %d -> %d", beforeHealth.QueriesAllowed, afterHealth.QueriesAllowed)

	// 8. Verify direct write-blocking enforcement on air-read-engine
	directSQLiteDSN := fmt.Sprintf("postgres://app_user@127.0.0.1:%d/%s?sslmode=disable", readEnginePGPort, tenantID)
	directSQLiteConn, err := pgx.Connect(ctx, directSQLiteDSN)
	if err != nil {
		t.Fatalf("pgx.Connect directly to air-read-engine failed: %v", err)
	}
	defer directSQLiteConn.Close(ctx)

	if _, err := directSQLiteConn.Exec(ctx, "INSERT INTO orders (id, item, amount) VALUES (99, 'forbidden', 1)", pgx.QueryExecModeSimpleProtocol); err == nil {
		t.Fatalf("expected INSERT directly on air-read-engine to be rejected")
	} else {
		t.Logf("Read-only enforcement rejected write as expected: %v", err)
	}
	blockedHealth := getReadEngineHealth(t, readEngineURL)
	if blockedHealth.WritesBlocked <= afterHealth.WritesBlocked {
		t.Fatalf("expected writes_blocked to increment (before=%d, after=%d)",
			afterHealth.WritesBlocked, blockedHealth.WritesBlocked)
	}

	// 9. Perform another write through air-gateway, verify causal fallback on writeConn,
	// then re-sync SQLite and verify readConn sees the new row from SQLite.
	if _, err := writeConn.Exec(ctx, "INSERT INTO orders (id, item, amount) VALUES (3, 'monitor', 350)", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("second INSERT via gateway failed: %v", err)
	}
	var item3 string
	if err := writeConn.QueryRow(ctx, "SELECT item FROM orders WHERE id = 3").Scan(&item3); err != nil {
		t.Fatalf("causal fallback after second INSERT failed: %v", err)
	}
	if item3 != "monitor" {
		t.Fatalf("expected 'monitor' from causal fallback, got %q", item3)
	}

	// Re-sync SQLite
	syncResp2, err := http.Post(readEngineURL+"/v1/tenants/"+tenantID+"/sync", "application/json", bytes.NewReader(syncBody))
	if err != nil {
		t.Fatalf("second sync failed: %v", err)
	}
	_ = syncResp2.Body.Close()
	if syncResp2.StatusCode != http.StatusOK {
		t.Fatalf("second sync status: %d", syncResp2.StatusCode)
	}

	readConn2, err := pgx.Connect(ctx, gwDSN)
	if err != nil {
		t.Fatalf("pgx.Connect readConn2 failed: %v", err)
	}
	defer readConn2.Close(ctx)

	var item3Read string
	if err := readConn2.QueryRow(ctx, "SELECT item FROM orders WHERE id = 3", pgx.QueryExecModeSimpleProtocol).Scan(&item3Read); err != nil {
		t.Fatalf("SELECT id=3 via gateway -> SQLite failed: %v", err)
	}
	if item3Read != "monitor" {
		t.Fatalf("expected 'monitor' from SQLite after re-sync, got %q", item3Read)
	}

	finalHealth := getReadEngineHealth(t, readEngineURL)
	if finalHealth.QueriesAllowed <= blockedHealth.QueriesAllowed {
		t.Fatalf("expected queries_allowed to increment after re-sync (%d -> %d)",
			blockedHealth.QueriesAllowed, finalHealth.QueriesAllowed)
	}

	// 10. Check gateway metrics endpoint
	mReq, _ := http.NewRequest(http.MethodGet, gwAdminURL+"/internal/v1/metrics", nil)
	mReq.Header.Set("Authorization", "Bearer "+internalToken)
	mResp, err := http.DefaultClient.Do(mReq)
	if err != nil {
		t.Fatalf("GET gateway metrics failed: %v", err)
	}
	mBody, _ := io.ReadAll(mResp.Body)
	mResp.Body.Close()
	if mResp.StatusCode != http.StatusOK {
		t.Fatalf("gateway metrics status %d: %s", mResp.StatusCode, string(mBody))
	}
	if !strings.Contains(string(mBody), tenantID) {
		t.Fatalf("expected gateway metrics to include tenant %q, got: %s", tenantID, string(mBody))
	}

	// 11. Delete/purge database via Control Plane
	delReq, _ := http.NewRequest(http.MethodDelete, serverURL+"/v1/databases/"+tenantID, nil)
	delReq.Header.Set("Authorization", "Bearer "+bootstrapAPIKey)
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE database failed: %v", err)
	}
	_ = delResp.Body.Close()
	if delResp.StatusCode != http.StatusOK && delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE database unexpected status: %d", delResp.StatusCode)
	}

	t.Logf("Full-stack E2E passed! SQLite queries_allowed=%d writes_blocked=%d",
		finalHealth.QueriesAllowed, finalHealth.WritesBlocked)
}

// TestE2E_NativePageserver_SQLiteReadEngine_ZeroSync verifies that with air-server
// running in -pageserver mode and air-read-engine connected to pageserver.sock,
// air-gateway routes SELECT queries to SQLiteReadServer which reads PG17 8 KiB pages
// natively via the Cgo air_pg virtual table with ZERO /sync API calls.
func TestE2E_NativePageserver_SQLiteReadEngine_ZeroSync(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/initdb"); err != nil {
		t.Skip("PostgreSQL 17 not installed")
	}

	rootDir, err := os.MkdirTemp("/tmp", "air_nat_e2e_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	binDir := filepath.Join(rootDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}

	airServerBin := filepath.Join(binDir, "air-server")
	airReadEngineBin := filepath.Join(binDir, "air-read-engine")
	airGatewayBin := filepath.Join(binDir, "air-gateway")

	buildBinary(t, "./cmd/air-server", airServerBin)
	buildBinary(t, "./cmd/air-read-engine", airReadEngineBin)
	buildBinary(t, "./cmd/air-gateway", airGatewayBin)

	serverHTTPAddr := freeTCPAddr(t)
	serverGRPCAddr := freeTCPAddr(t)
	metaPort := freeTCPPort(t)
	readEngineHTTPAddr := freeTCPAddr(t)
	readEnginePGPort := freeTCPPort(t)
	gwPGAddr := freeTCPAddr(t)
	gwAdminAddr := freeTCPAddr(t)
	gwAdminGRPCAddr := freeTCPAddr(t)

	const (
		bootstrapAccount = "acct_e2e_native"
		bootstrapAPIKey  = "air_e2e_native_key_123456"
		internalToken    = "air_e2e_native_internal_token"
		tenantID         = "e2e_native_db"
	)

	dataDir := filepath.Join(rootDir, "air-data")
	metaDir := filepath.Join(rootDir, "air-meta")
	masterKey := filepath.Join(rootDir, "master.key")
	psSock := filepath.Join(rootDir, "pageserver.sock")
	serverLogPath := filepath.Join(rootDir, "air-server.log")
	gwLogPath := filepath.Join(rootDir, "air-gateway.log")
	defer func() {
		if t.Failed() {
			if b, err := os.ReadFile(serverLogPath); err == nil {
				t.Logf("=== air-server.log ===\n%s", string(b))
			}
			if b, err := os.ReadFile(gwLogPath); err == nil {
				t.Logf("=== air-gateway.log ===\n%s", string(b))
			}
		}
	}()

	serverLog, err := os.Create(serverLogPath)
	if err != nil {
		t.Fatal(err)
	}
	defer serverLog.Close()

	// 1. Start air-server WITH Pageserver enabled
	serverCmd := exec.Command(airServerBin,
		"-http-addr", serverHTTPAddr,
		"-grpc-addr", serverGRPCAddr,
		"-meta-port", fmt.Sprintf("%d", metaPort),
		"-data-dir", dataDir,
		"-meta-dir", metaDir,
		"-master-key-path", masterKey,
		"-bootstrap-account-id", bootstrapAccount,
		"-bootstrap-api-key", bootstrapAPIKey,
		"-internal-token", internalToken,
		"-gateway-admin-grpc-addr", gwAdminGRPCAddr,
		"-pageserver",
		"-pageserver-socket", psSock,
		"-pageserver-cache-mb", "32",
	)
	serverCmd.Stdout = serverLog
	serverCmd.Stderr = serverLog
	if err := serverCmd.Start(); err != nil {
		t.Fatalf("start air-server: %v", err)
	}
	defer stopCmd(serverCmd)

	serverURL := "http://" + serverHTTPAddr
	waitForHTTP200(t, serverURL+"/healthz", serverLogPath, 30*time.Second)

	// 2. Start air-read-engine in NATIVE Pageserver mode (ZERO /sync!)
	readDataDir := filepath.Join(rootDir, "air-read-data")
	readCacheDir := filepath.Join(rootDir, "air-read-l2cache")
	readLogPath := filepath.Join(rootDir, "air-read-engine.log")
	readLog, err := os.Create(readLogPath)
	if err != nil {
		t.Fatal(err)
	}
	defer readLog.Close()

	readCmd := exec.Command(airReadEngineBin,
		"-data-dir", readDataDir,
		"-http-addr", readEngineHTTPAddr,
		"-listen-host", "127.0.0.1",
		"-pg-port", fmt.Sprintf("%d", readEnginePGPort),
		"-pageserver-socket", psSock,
		"-cache-dir", readCacheDir,
	)
	readCmd.Stdout = readLog
	readCmd.Stderr = readLog
	if err := readCmd.Start(); err != nil {
		t.Fatalf("start air-read-engine: %v", err)
	}
	defer stopCmd(readCmd)

	readEngineURL := "http://" + readEngineHTTPAddr
	waitForHTTP200(t, readEngineURL+"/healthz", readLogPath, 15*time.Second)

	// 3. Start air-gateway wired to air-server and native air-read-engine
	gwLog, err := os.Create(gwLogPath)
	if err != nil {
		t.Fatal(err)
	}
	defer gwLog.Close()

	gwCmd := exec.Command(airGatewayBin,
		"--gateway-addr", gwPGAddr,
		"--gateway-admin-addr", gwAdminAddr,
		"--gateway-admin-grpc-addr", gwAdminGRPCAddr,
		"--db-grpc-addr", serverGRPCAddr,
		"--control-plane-url", serverURL,
		"--internal-token", internalToken,
		"--backend-host", "127.0.0.1",
		"--pool-mode",
		"--pool-max-backends", "4",
		"--read-engine-host", "127.0.0.1",
		"--read-engine-port", fmt.Sprintf("%d", readEnginePGPort),
	)
	gwCmd.Stdout = gwLog
	gwCmd.Stderr = gwLog
	if err := gwCmd.Start(); err != nil {
		t.Fatalf("start air-gateway: %v", err)
	}
	defer stopCmd(gwCmd)

	gwAdminURL := "http://" + gwAdminAddr
	waitForHTTP200(t, gwAdminURL+"/healthz", gwLogPath, 15*time.Second)

	// 4. Create a new tenant database via air-server
	prov := httpCreateDatabase(t, serverURL, bootstrapAPIKey, "idem-e2e-native-1", tenantID)
	t.Logf("Created native tenant %q: state=%s direct_port=%d", prov.TenantID, prov.State, prov.DirectPort)
	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	gwDSN := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", prov.Username, prov.Password, gwPGAddr, tenantID)

	// 5. Write data via air-gateway (routed to PG17 Write Engine)
	writeConn, err := pgx.Connect(ctx, gwDSN)
	if err != nil {
		t.Fatalf("pgx.Connect writeConn: %v", err)
	}
	defer writeConn.Close(ctx)

	if _, err := writeConn.Exec(ctx, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			label TEXT NOT NULL,
			qty INT NOT NULL
		);
		INSERT INTO widgets (id, label, qty) VALUES (1, 'bolt', 50), (2, 'nut', 125);
	`, pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("CREATE/INSERT via gateway failed: %v", err)
	}

	// Give WALReceiver time to stream WAL into Pageserver
	time.Sleep(1500 * time.Millisecond)

	// 6. Read via air-gateway with ZERO /sync calls!
	readConn, err := pgx.Connect(ctx, gwDSN)
	if err != nil {
		t.Fatalf("pgx.Connect readConn: %v", err)
	}
	defer readConn.Close(ctx)

	beforeHealth := getReadEngineHealth(t, readEngineURL)

	var label string
	var qty int
	if err := readConn.QueryRow(ctx, "SELECT label, qty FROM widgets WHERE id = 2", pgx.QueryExecModeSimpleProtocol).Scan(&label, &qty); err != nil {
		srvLogs, _ := os.ReadFile(serverLogPath)
		reLogs, _ := os.ReadFile(readLogPath)
		gwLogs, _ := os.ReadFile(gwLogPath)
		t.Fatalf("native zero-sync SELECT failed: %v\n--- air-server ---\n%s\n--- air-read-engine ---\n%s\n--- air-gateway ---\n%s",
			err, string(srvLogs), string(reLogs), string(gwLogs))
	}
	if label != "nut" || qty != 125 {
		t.Fatalf("expected ('nut', 125), got (%q, %d)", label, qty)
	}

	var total int64
	if err := readConn.QueryRow(ctx, "SELECT SUM(qty) FROM widgets", pgx.QueryExecModeSimpleProtocol).Scan(&total); err != nil {
		t.Fatalf("native zero-sync SUM failed: %v", err)
	}
	if total != 175 {
		t.Fatalf("expected SUM(qty)=175, got %d", total)
	}

	afterHealth := getReadEngineHealth(t, readEngineURL)
	if afterHealth.QueriesAllowed <= beforeHealth.QueriesAllowed {
		srvLogs, _ := os.ReadFile(serverLogPath)
		reLogs, _ := os.ReadFile(readLogPath)
		gwLogs, _ := os.ReadFile(gwLogPath)
		t.Fatalf("expected SQLite queries_allowed to increment with ZERO /sync (before=%d, after=%d)\n--- air-server ---\n%s\n--- air-read-engine ---\n%s\n--- air-gateway ---\n%s",
			beforeHealth.QueriesAllowed, afterHealth.QueriesAllowed, string(srvLogs), string(reLogs), string(gwLogs))
	}
	t.Logf("SUCCESS: Zero-sync native Pageserver -> SQLite air_pg vtab -> air-gateway E2E passed! (queries_allowed: %d -> %d)",
		beforeHealth.QueriesAllowed, afterHealth.QueriesAllowed)
}
