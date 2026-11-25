package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func doAdminJSON(t *testing.T, method, url, token string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, url, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("X-Air-Internal-Token", token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s failed: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func waitForTCP(t *testing.T, addr, logPath string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	raw, _ := os.ReadFile(logPath)
	t.Fatalf("timed out waiting for TCP %s; log:\n%s", addr, string(raw))
}

func TestE2E_FourServiceGRPCMesh_NamespaceProvisioningAndIsolation(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/initdb"); err != nil {
		t.Skipf("PostgreSQL 17 is required: %v", err)
	}

	rootDir, err := os.MkdirTemp("/tmp", "air_4svc_mesh_e2e_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	binDir := filepath.Join(rootDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	psBin := filepath.Join(binDir, "air-pageserver")
	airBin := filepath.Join(binDir, "air-server")
	reBin := filepath.Join(binDir, "air-read-engine")
	gwBin := filepath.Join(binDir, "air-gateway")

	buildBinary(t, "./cmd/air-pageserver", psBin)
	buildBinary(t, "./cmd/air-server", airBin)
	buildBinary(t, "./cmd/air-read-engine", reBin)
	buildBinary(t, "./cmd/air-gateway", gwBin)

	internalToken := "mesh-secret-token-2026"
	masterKeyHex := "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	apiKey := "air_mesh_bootstrap_key"

	psGRPCAddr := freeTCPAddr(t)
	airGRPCAddr := freeTCPAddr(t)
	airMetaPort := strings.Split(freeTCPAddr(t), ":")[1]
	reGRPCAddr := freeTCPAddr(t)
	rePGPort := strings.Split(freeTCPAddr(t), ":")[1]
	gwPGAddr := freeTCPAddr(t)
	gwAdminHTTPAddr := freeTCPAddr(t)
	gwAdminGRPCAddr := freeTCPAddr(t)

	// 1. Start air-pageserver (gRPC only)
	psLogPath := filepath.Join(rootDir, "pageserver.log")
	psLog, _ := os.Create(psLogPath)
	defer psLog.Close()
	psCmd := exec.Command(psBin)
	psCmd.Env = append(os.Environ(),
		"AIR_DATA_DIR="+filepath.Join(rootDir, "ps-data"),
		"AIR_PAGESERVER_GRPC_ADDR="+psGRPCAddr,
		"AIR_INTERNAL_TOKEN="+internalToken,
		"AIR_MASTER_KEY_HEX="+masterKeyHex,
	)
	psCmd.Stdout = psLog
	psCmd.Stderr = psLog
	if err := psCmd.Start(); err != nil {
		t.Fatalf("start air-pageserver: %v", err)
	}
	defer stopCmd(psCmd)
	waitForTCP(t, psGRPCAddr, psLogPath, 15*time.Second)

	// 2. Start air-read-engine (gRPC + PG wire only, connected to air-pageserver via gRPC)
	reLogPath := filepath.Join(rootDir, "read-engine.log")
	reLog, _ := os.Create(reLogPath)
	defer reLog.Close()
	reCmd := exec.Command(reBin)
	reCmd.Env = append(os.Environ(),
		"AIR_READ_ENGINE_DATA_DIR="+filepath.Join(rootDir, "re-data"),
		"AIR_READ_ENGINE_LISTEN_HOST=127.0.0.1",
		"AIR_READ_ENGINE_PG_PORT="+rePGPort,
		"AIR_READ_ENGINE_GRPC_ADDR="+reGRPCAddr,
		"AIR_PAGESERVER_GRPC_ADDR="+psGRPCAddr,
		"AIR_INTERNAL_TOKEN="+internalToken,
	)
	reCmd.Stdout = reLog
	reCmd.Stderr = reLog
	if err := reCmd.Start(); err != nil {
		t.Fatalf("start air-read-engine: %v", err)
	}
	defer stopCmd(reCmd)
	waitForTCP(t, reGRPCAddr, reLogPath, 15*time.Second)

	// 3. Start air-server (gRPC only, connected to pageserver, read-engine, and gateway admin via gRPC)
	airLogPath := filepath.Join(rootDir, "air-server.log")
	airLog, _ := os.Create(airLogPath)
	defer airLog.Close()
	airCmd := exec.Command(airBin)
	airCmd.Env = append(os.Environ(),
		"AIR_DATA_DIR="+filepath.Join(rootDir, "air-data"),
		"AIR_PG_BIN_DIR=/usr/lib/postgresql/17/bin",
		"AIR_META_PORT="+airMetaPort,
		"AIR_GRPC_ADDR="+airGRPCAddr,
		"AIR_PAGESERVER_GRPC_ADDR="+psGRPCAddr,
		"AIR_READ_ENGINE_GRPC_ADDR="+reGRPCAddr,
		"AIR_GATEWAY_ADMIN_GRPC_ADDR="+gwAdminGRPCAddr,
		"AIR_BACKEND_HOST=127.0.0.1",
		"AIR_INTERNAL_TOKEN="+internalToken,
		"AIR_MASTER_KEY_HEX="+masterKeyHex,
		"AIR_BOOTSTRAP_API_KEY="+apiKey,
		"AIR_BOOTSTRAP_ACCOUNT_ID=acct_mesh",
	)
	airCmd.Stdout = airLog
	airCmd.Stderr = airLog
	if err := airCmd.Start(); err != nil {
		t.Fatalf("start air-server: %v", err)
	}
	defer stopCmd(airCmd)
	waitForTCP(t, airGRPCAddr, airLogPath, 20*time.Second)

	// 4. Start air-gateway (single HTTP Control Plane + PG wire, connected to all 3 backends via gRPC)
	gwLogPath := filepath.Join(rootDir, "gateway.log")
	gwLog, _ := os.Create(gwLogPath)
	defer gwLog.Close()
	gwCmd := exec.Command(gwBin)
	gwCmd.Env = append(os.Environ(),
		"AIR_GATEWAY_ADDR="+gwPGAddr,
		"AIR_GATEWAY_ADMIN_ADDR="+gwAdminHTTPAddr,
		"AIR_GATEWAY_ADMIN_GRPC_ADDR="+gwAdminGRPCAddr,
		"AIR_DB_GRPC_ADDR="+airGRPCAddr,
		"AIR_PAGESERVER_GRPC_ADDR="+psGRPCAddr,
		"AIR_READ_ENGINE_HOST=127.0.0.1",
		"AIR_READ_ENGINE_PORT="+rePGPort,
		"AIR_READ_ENGINE_GRPC_ADDR="+reGRPCAddr,
		"AIR_BACKEND_HOST=127.0.0.1",
		"AIR_INTERNAL_TOKEN="+internalToken,
		"AIR_POOL_MODE=true",
	)
	gwCmd.Stdout = gwLog
	gwCmd.Stderr = gwLog
	if err := gwCmd.Start(); err != nil {
		t.Fatalf("start air-gateway: %v", err)
	}
	defer stopCmd(gwCmd)
	waitForHTTP200(t, "http://"+gwAdminHTTPAddr+"/healthz", gwLogPath, 15*time.Second)

	// 5. Verify internal token auth is enforced on air-gateway Control Plane APIs
	for _, path := range []string{"/v1/namespaces", "/v1/cluster/health", "/v1/pageserver/tenants", "/v1/read-engine/tenants"} {
		code, _ := doAdminJSON(t, http.MethodGet, "http://"+gwAdminHTTPAddr+path, "", nil)
		if code != http.StatusUnauthorized {
			t.Fatalf("gateway %s without token: expected 401, got %d", path, code)
		}
	}

	// 6. Provision DB via air-gateway -> orchestrates air-server -> pageserver -> read-engine via gRPC
	tenantID := "mesh_ns_alpha"
	code, prov := doAdminJSON(t, http.MethodPost, "http://"+gwAdminHTTPAddr+"/v1/databases", internalToken, map[string]any{
		"tenant_id":  tenantID,
		"account_id": "acct_mesh",
	})
	if code != http.StatusCreated || prov["state"] != "ACTIVE" || prov["password"] == "" {
		t.Fatalf("unexpected provision response: code=%d body=%+v", code, prov)
	}
	username, _ := prov["user"].(string)
	password, _ := prov["password"].(string)
	if username != tenantID {
		t.Fatalf("expected tenant-scoped username %q, got %q", tenantID, username)
	}

	// Verify namespace exists on pageserver & read-engine via gateway control plane
	code, psNS := doAdminJSON(t, http.MethodGet, "http://"+gwAdminHTTPAddr+"/v1/pageserver/tenants/"+tenantID, internalToken, nil)
	if code != http.StatusOK || psNS["exists"] != true {
		t.Fatalf("pageserver namespace %s missing: status=%d body=%+v", tenantID, code, psNS)
	}
	code, reNS := doAdminJSON(t, http.MethodGet, "http://"+gwAdminHTTPAddr+"/v1/read-engine/tenants/"+tenantID, internalToken, nil)
	if code != http.StatusOK || reNS["tenant_id"] != tenantID {
		t.Fatalf("read-engine namespace %s missing: status=%d body=%+v", tenantID, code, reNS)
	}
	code, gwNS := doAdminJSON(t, http.MethodGet, "http://"+gwAdminHTTPAddr+"/v1/namespaces/"+tenantID, internalToken, nil)
	if code != http.StatusOK || gwNS["air_connected"] != true || gwNS["read_engine_connected"] != true || gwNS["pageserver_connected"] != true {
		t.Fatalf("gateway namespace %s not connected to all services: status=%d body=%+v", tenantID, code, gwNS)
	}

	// 7. Connect through air-gateway using [tenant_db]:password URL (without /db path) and execute SQL end-to-end
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dsn := fmt.Sprintf("postgres://%s:%s@%s?sslmode=disable", username, password, gwPGAddr)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect through air-gateway failed: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE mesh_items (id INT PRIMARY KEY, note TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO mesh_items (id, note) VALUES (1, 'hello-4-service-mesh')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var note string
	if err := conn.QueryRow(ctx, `SELECT note FROM mesh_items WHERE id = 1`).Scan(&note); err != nil {
		t.Fatalf("select: %v", err)
	}
	if note != "hello-4-service-mesh" {
		t.Fatalf("unexpected row value: %q", note)
	}
	_ = conn.Close(ctx)

	// 8. Verify gateway namespace creation orchestrates read-engine + pageserver over gRPC
	manualNS := "manual_ns_beta"
	code, gwManual := doAdminJSON(t, http.MethodPost, "http://"+gwAdminHTTPAddr+"/v1/namespaces", internalToken, map[string]any{"namespace": manualNS})
	if code != http.StatusCreated || gwManual["read_engine_connected"] != true || gwManual["pageserver_connected"] != true {
		t.Fatalf("gateway POST /v1/namespaces: status=%d body=%+v", code, gwManual)
	}

	// 9. Delete DB via air-gateway and verify cascade across gateway, read-engine, and pageserver
	code, _ = doAdminJSON(t, http.MethodDelete, "http://"+gwAdminHTTPAddr+"/v1/databases/"+tenantID, internalToken, nil)
	if code != http.StatusOK {
		t.Fatalf("DELETE /v1/databases/%s: expected 200, got %d", tenantID, code)
	}
	if code, _ := doAdminJSON(t, http.MethodGet, "http://"+gwAdminHTTPAddr+"/v1/pageserver/tenants/"+tenantID, internalToken, nil); code != http.StatusNotFound {
		t.Fatalf("expected pageserver namespace %s to be deleted (404), got %d", tenantID, code)
	}
}
