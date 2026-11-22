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
	psHTTPAddr := freeTCPAddr(t)
	airHTTPAddr := freeTCPAddr(t)
	airGRPCAddr := freeTCPAddr(t)
	airMetaPort := strings.Split(freeTCPAddr(t), ":")[1]
	reHTTPAddr := freeTCPAddr(t)
	reGRPCAddr := freeTCPAddr(t)
	rePGPort := strings.Split(freeTCPAddr(t), ":")[1]
	gwPGAddr := freeTCPAddr(t)
	gwAdminHTTPAddr := freeTCPAddr(t)
	gwAdminGRPCAddr := freeTCPAddr(t)

	// 1. Start air-pageserver
	psLogPath := filepath.Join(rootDir, "pageserver.log")
	psLog, _ := os.Create(psLogPath)
	defer psLog.Close()
	psCmd := exec.Command(psBin)
	psCmd.Env = append(os.Environ(),
		"AIR_DATA_DIR="+filepath.Join(rootDir, "ps-data"),
		"AIR_PAGESERVER_GRPC_ADDR="+psGRPCAddr,
		"AIR_PAGESERVER_HTTP_ADDR="+psHTTPAddr,
		"AIR_INTERNAL_TOKEN="+internalToken,
		"AIR_MASTER_KEY_HEX="+masterKeyHex,
	)
	psCmd.Stdout = psLog
	psCmd.Stderr = psLog
	if err := psCmd.Start(); err != nil {
		t.Fatalf("start air-pageserver: %v", err)
	}
	defer stopCmd(psCmd)
	waitForHTTP200(t, "http://"+psHTTPAddr+"/healthz", psLogPath, 15*time.Second)

	// 2. Start air-read-engine (connected to air-pageserver via gRPC)
	reLogPath := filepath.Join(rootDir, "read-engine.log")
	reLog, _ := os.Create(reLogPath)
	defer reLog.Close()
	reCmd := exec.Command(reBin)
	reCmd.Env = append(os.Environ(),
		"AIR_READ_ENGINE_DATA_DIR="+filepath.Join(rootDir, "re-data"),
		"AIR_READ_ENGINE_LISTEN_HOST=127.0.0.1",
		"AIR_READ_ENGINE_PG_PORT="+rePGPort,
		"AIR_READ_ENGINE_HTTP_ADDR="+reHTTPAddr,
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
	waitForHTTP200(t, "http://"+reHTTPAddr+"/healthz", reLogPath, 15*time.Second)

	// 3. Start air-server (connected to pageserver, read-engine, and gateway admin via gRPC)
	airLogPath := filepath.Join(rootDir, "air-server.log")
	airLog, _ := os.Create(airLogPath)
	defer airLog.Close()
	airCmd := exec.Command(airBin)
	airCmd.Env = append(os.Environ(),
		"AIR_DATA_DIR="+filepath.Join(rootDir, "air-data"),
		"AIR_PG_BIN_DIR=/usr/lib/postgresql/17/bin",
		"AIR_META_PORT="+airMetaPort,
		"AIR_HTTP_ADDR="+airHTTPAddr,
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
	waitForHTTP200(t, "http://"+airHTTPAddr+"/healthz", airLogPath, 20*time.Second)

	// 4. Start air-gateway (connected to air-server, air-read-engine, and air-pageserver via gRPC)
	gwLogPath := filepath.Join(rootDir, "gateway.log")
	gwLog, _ := os.Create(gwLogPath)
	defer gwLog.Close()
	gwCmd := exec.Command(gwBin)
	gwCmd.Env = append(os.Environ(),
		"AIR_GATEWAY_ADDR="+gwPGAddr,
		"AIR_GATEWAY_ADMIN_ADDR="+gwAdminHTTPAddr,
		"AIR_GATEWAY_ADMIN_GRPC_ADDR="+gwAdminGRPCAddr,
		"AIR_CONTROL_PLANE_URL=http://"+airHTTPAddr,
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

	// 5. Verify internal token auth is enforced on all Admin namespace APIs
	for name, url := range map[string]string{
		"pageserver":  "http://" + psHTTPAddr + "/v1/namespaces",
		"read-engine": "http://" + reHTTPAddr + "/v1/namespaces",
		"gateway":     "http://" + gwAdminHTTPAddr + "/v1/namespaces",
	} {
		code, _ := doAdminJSON(t, http.MethodGet, url, "", nil)
		if code != http.StatusUnauthorized {
			t.Fatalf("%s /v1/namespaces without token: expected 401, got %d", name, code)
		}
	}

	// 6. Provision DB via air-server -> orchestrates air-server -> pageserver -> read-engine -> gateway
	tenantID := "mesh_ns_alpha"
	prov := httpCreateDatabase(t, "http://"+airHTTPAddr, apiKey, "idem-"+tenantID, tenantID)
	if prov.State != "ACTIVE" || prov.Password == "" {
		t.Fatalf("unexpected provision response: %+v", prov)
	}

	// Verify namespace exists on pageserver
	code, psNS := doAdminJSON(t, http.MethodGet, "http://"+psHTTPAddr+"/v1/namespaces/"+tenantID, internalToken, nil)
	if code != http.StatusOK || psNS["exists"] != true {
		t.Fatalf("pageserver namespace %s missing: status=%d body=%+v", tenantID, code, psNS)
	}

	// Verify namespace exists on read-engine
	code, reNS := doAdminJSON(t, http.MethodGet, "http://"+reHTTPAddr+"/v1/namespaces/"+tenantID, internalToken, nil)
	if code != http.StatusOK || reNS["tenant_id"] != tenantID {
		t.Fatalf("read-engine namespace %s missing: status=%d body=%+v", tenantID, code, reNS)
	}

	// Verify namespace is registered on gateway and connected to all 3 services
	code, gwNS := doAdminJSON(t, http.MethodGet, "http://"+gwAdminHTTPAddr+"/v1/namespaces/"+tenantID, internalToken, nil)
	if code != http.StatusOK || gwNS["air_connected"] != true || gwNS["read_engine_connected"] != true || gwNS["pageserver_connected"] != true {
		t.Fatalf("gateway namespace %s not connected to all services: status=%d body=%+v", tenantID, code, gwNS)
	}

	// 7. Connect through air-gateway using the user namespace and execute SQL end-to-end
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", prov.Username, prov.Password, gwPGAddr, prov.Database)
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

	// 8. Verify standalone Admin API namespace creation works on each service
	manualNS := "manual_ns_beta"
	code, _ = doAdminJSON(t, http.MethodPost, "http://"+psHTTPAddr+"/v1/namespaces", internalToken, map[string]any{"namespace": manualNS})
	if code != http.StatusCreated {
		t.Fatalf("pageserver POST /v1/namespaces: expected 201, got %d", code)
	}
	code, _ = doAdminJSON(t, http.MethodPost, "http://"+reHTTPAddr+"/v1/namespaces", internalToken, map[string]any{"namespace": manualNS})
	if code != http.StatusCreated {
		t.Fatalf("read-engine POST /v1/namespaces: expected 201, got %d", code)
	}
	code, gwManual := doAdminJSON(t, http.MethodPost, "http://"+gwAdminHTTPAddr+"/v1/namespaces", internalToken, map[string]any{"namespace": manualNS})
	if code != http.StatusCreated || gwManual["read_engine_connected"] != true || gwManual["pageserver_connected"] != true {
		t.Fatalf("gateway POST /v1/namespaces: status=%d body=%+v", code, gwManual)
	}

	// 9. Delete DB via air-server and verify cascade across gateway, read-engine, and pageserver
	delReq, _ := http.NewRequest(http.MethodDelete, "http://"+airHTTPAddr+"/v1/databases/"+tenantID, nil)
	delReq.Header.Set("Authorization", "Bearer "+apiKey)
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE /v1/databases/%s: %v", tenantID, err)
	}
	_ = delResp.Body.Close()
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE /v1/databases/%s: expected 200, got %d", tenantID, delResp.StatusCode)
	}

	if code, _ := doAdminJSON(t, http.MethodGet, "http://"+reHTTPAddr+"/v1/namespaces/"+tenantID, internalToken, nil); code != http.StatusNotFound {
		t.Fatalf("expected read-engine namespace %s to be deleted (404), got %d", tenantID, code)
	}
	if code, _ := doAdminJSON(t, http.MethodGet, "http://"+psHTTPAddr+"/v1/namespaces/"+tenantID, internalToken, nil); code != http.StatusNotFound {
		t.Fatalf("expected pageserver namespace %s to be deleted (404), got %d", tenantID, code)
	}
}
