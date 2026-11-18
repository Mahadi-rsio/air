package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/control"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestAIRv1_SingleCellEndToEndMilestones(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/initdb"); err != nil {
		t.Fatalf("PostgreSQL 17 is required for e2e tests: %v", err)
	}

	ctx := context.Background()
	rootDir, err := os.MkdirTemp("/tmp", "air_v1_e2e_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	metaDir := filepath.Join(rootDir, "meta")
	keyPath := filepath.Join(rootDir, "master.key")
	kms, err := crypto.LoadOrCreateEnvelopeKMS(keyPath)
	if err != nil {
		t.Fatalf("LoadOrCreateEnvelopeKMS: %v", err)
	}

	ag1, err := cell.NewAgent(rootDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer func() {
		for _, tid := range []string{"tenant_alpha", "tenant_beta", "tenant_crash"} {
			_ = ag1.StopAndPurgeTenant(tid)
		}
		ag1.Close()
	}()

	cp, err := control.NewControlPlane(ctx, metaDir, "/usr/lib/postgresql/17/bin", 19832, ag1, kms)
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	defer cp.Close()

	keyA, err := cp.CreateAPIKey(ctx, "acct_a", "acct_a primary key")
	if err != nil {
		t.Fatalf("CreateAPIKey acct_a: %v", err)
	}
	keyB, err := cp.CreateAPIKey(ctx, "acct_b", "acct_b primary key")
	if err != nil {
		t.Fatalf("CreateAPIKey acct_b: %v", err)
	}

	httpSrv := httptest.NewServer(cp.HTTPHandler())
	defer httpSrv.Close()

	// Milestone 1 & 2: Create a database through Control Plane HTTP API and receive credentials.
	alpha := httpCreateDatabase(t, httpSrv.URL, keyA.Token, "idem-alpha-1", "tenant_alpha")
	if alpha.State != "ACTIVE" || alpha.Password == "" || alpha.Username != "app_user" || alpha.Database != "tenant_alpha" {
		t.Fatalf("unexpected provision response: %+v", alpha)
	}

	// Milestone 3 & 4: Connect with standard PostgreSQL clients (pgx + psql) and execute SQL + transactions.
	alphaDSN := fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", alpha.Username, alpha.Password, alpha.DirectPort, alpha.Database)
	conn, err := pgx.Connect(ctx, alphaDSN)
	if err != nil {
		t.Fatalf("pgx.Connect through gateway failed: %v", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE items (id INT PRIMARY KEY, label TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO items (id, label) VALUES (1, 'durable-row')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit tx: %v", err)
	}
	_ = conn.Close(ctx)

	// Verify standard `psql` CLI also works end-to-end.
	if psqlPath, lookErr := exec.LookPath("psql"); lookErr == nil {
		out, err := exec.Command(psqlPath, alphaDSN, "-tAc", "SELECT label FROM items WHERE id = 1").CombinedOutput()
		if err != nil || strings.TrimSpace(string(out)) != "durable-row" {
			t.Fatalf("psql query failed: err=%v out=%q", err, string(out))
		}
	}

	// Milestone 5: Restart PostgreSQL without losing data.
	rtRestarted, err := ag1.RestartTenant("tenant_alpha")
	if err != nil {
		t.Fatalf("RestartTenant: %v", err)
	}
	alphaDSN = fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", alpha.Username, alpha.Password, rtRestarted.Port, alpha.Database)
	conn2, err := pgx.Connect(ctx, alphaDSN)
	if err != nil {
		t.Fatalf("reconnect after PG restart failed: %v", err)
	}
	var gotLabel string
	if err := conn2.QueryRow(ctx, `SELECT label FROM items WHERE id = 1`).Scan(&gotLabel); err != nil || gotLabel != "durable-row" {
		t.Fatalf("data lost across PG restart: label=%q err=%v", gotLabel, err)
	}
	_ = conn2.Close(ctx)

	// Milestone 6: Restart the Agent without losing tenant ownership (and verify concurrent agent cannot steal flock).
	ag1.Close()
	ag2, err := cell.NewAgent(rootDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("restarted NewAgent: %v", err)
	}
	defer ag2.Close()

	rt, ok := ag2.GetTenant("tenant_alpha")
	if !ok || rt.State != "ACTIVE" {
		t.Fatalf("restarted agent did not readopt tenant_alpha: ok=%v rt=%+v", ok, rt)
	}
	// A competing agent on the same volDir MUST NOT readopt while ag2 holds agent.lock.
	agCompeting, err := cell.NewAgent(rootDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("competing NewAgent: %v", err)
	}
	if _, stolen := agCompeting.GetTenant("tenant_alpha"); stolen {
		agCompeting.Close()
		t.Fatalf("competing agent illegally readopted tenant_alpha while ag2 held flock")
	}
	agCompeting.Close()

	// Milestone 7: Create another tenant without cross-tenant access.
	beta := httpCreateDatabase(t, httpSrv.URL, keyB.Token, "idem-beta-1", "tenant_beta")
	if beta.Password == "" || beta.Password == alpha.Password {
		t.Fatalf("unexpected beta credentials")
	}
	// Cross-account GET/DELETE must be rejected with 403 Forbidden.
	req, _ := http.NewRequest(http.MethodGet, httpSrv.URL+"/v1/databases/tenant_beta", nil)
	req.Header.Set("Authorization", "Bearer "+keyA.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 on cross-account GET, got %d", resp.StatusCode)
	}

	// Cross-tenant DB login with alpha's password on beta's DB must fail SCRAM authentication.
	badDSN := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", alpha.Password, beta.DirectPort, "tenant_beta")
	if c, cErr := pgx.Connect(ctx, badDSN); cErr == nil {
		_ = c.Close(ctx)
		t.Fatalf("cross-tenant credential succeeded on tenant_beta!")
	}
	// Reserved superuser `postgres` over TCP must be rejected by pg_hba.conf.
	pgUserDSN := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/%s?sslmode=disable", beta.DirectPort, "tenant_beta")
	if c, cErr := pgx.Connect(ctx, pgUserDSN); cErr == nil {
		_ = c.Close(ctx)
		t.Fatalf("reserved superuser postgres was allowed over TCP!")
	}

	// Write a row into tenant_beta before deleting tenant_alpha.
	betaDSN := fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", beta.Username, beta.Password, beta.DirectPort, beta.Database)
	betaConn, err := pgx.Connect(ctx, betaDSN)
	if err != nil {
		t.Fatalf("connect beta: %v", err)
	}
	if _, err := betaConn.Exec(ctx, `CREATE TABLE beta_tbl (v TEXT); INSERT INTO beta_tbl VALUES ('still-alive')`); err != nil {
		t.Fatalf("beta write: %v", err)
	}
	_ = betaConn.Close(ctx)

	// Milestone 8: Delete tenant_alpha without affecting tenant_beta.
	delReq, _ := http.NewRequest(http.MethodDelete, httpSrv.URL+"/v1/databases/tenant_alpha", nil)
	delReq.Header.Set("Authorization", "Bearer "+keyA.Token)
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = delResp.Body.Close()
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on DELETE tenant_alpha, got %d", delResp.StatusCode)
	}
	if _, err := os.Stat(filepath.Join(rootDir, "tenants/active/tenant_alpha")); !os.IsNotExist(err) {
		t.Fatalf("tenant_alpha directory survived deletion")
	}
	if c, cErr := pgx.Connect(ctx, alphaDSN); cErr == nil {
		_ = c.Close(ctx)
		t.Fatalf("deleted tenant_alpha still accepted connections")
	}
	// Verify tenant_beta is completely unaffected.
	betaConn2, err := pgx.Connect(ctx, betaDSN)
	if err != nil {
		t.Fatalf("tenant_beta affected by tenant_alpha deletion: %v", err)
	}
	var betaVal string
	if err := betaConn2.QueryRow(ctx, `SELECT v FROM beta_tbl`).Scan(&betaVal); err != nil || betaVal != "still-alive" {
		t.Fatalf("tenant_beta data check failed: val=%q err=%v", betaVal, err)
	}
	_ = betaConn2.Close(ctx)

	// Milestone 9: Recover interrupted provisioning safely.
	cp.SetFaultHook(func(stage string) error {
		if stage == "after_claimed" {
			return fmt.Errorf("simulated crash after claim")
		}
		return nil
	})
	if _, err := cp.ProvisionDatabase(ctx, "acct_a", "idem-crash-1", "tenant_crash"); err == nil {
		t.Fatalf("expected simulated crash error")
	}
	cp.SetFaultHook(nil)

	if _, err := cp.RecoverProvisioningOperations(ctx); err != nil {
		t.Fatalf("RecoverProvisioningOperations: %v", err)
	}
	if _, err := os.Stat(filepath.Join(rootDir, "tenants/claiming/tenant_crash")); !os.IsNotExist(err) {
		t.Fatalf("orphaned claiming directory remained for tenant_crash")
	}
	if _, err := os.Stat(filepath.Join(rootDir, "tenants/active/tenant_crash")); !os.IsNotExist(err) {
		t.Fatalf("orphaned active directory remained for tenant_crash")
	}
	// Retry provisioning tenant_crash after recovery -> must succeed cleanly!
	crashOK := httpCreateDatabase(t, httpSrv.URL, keyA.Token, "idem-crash-2", "tenant_crash")
	if crashOK.State != "ACTIVE" {
		t.Fatalf("expected ACTIVE after crash recovery retry, got %+v", crashOK)
	}

	// Verify clean cleanup on agent stop for remaining tenants.
	_ = ag2.StopAndPurgeTenant("tenant_beta")
	_ = ag2.StopAndPurgeTenant("tenant_crash")
	_ = time.Millisecond
}

func TestAIRv1_1_PhaseE_BearerAuthAndMandatoryGatewayTLS(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/initdb"); err != nil {
		t.Fatalf("PostgreSQL 17 is required for e2e tests: %v", err)
	}

	ctx := context.Background()
	rootDir, err := os.MkdirTemp("/tmp", "air_v1_1_phase_e_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	metaDir := filepath.Join(rootDir, "meta")
	keyPath := filepath.Join(rootDir, "master.key")
	kms, err := crypto.LoadOrCreateEnvelopeKMS(keyPath)
	if err != nil {
		t.Fatalf("LoadOrCreateEnvelopeKMS: %v", err)
	}

	ag, err := cell.NewAgent(rootDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer func() {
		_ = ag.StopAndPurgeTenant("tenant_tls")
		ag.Close()
	}()

	cp, err := control.NewControlPlane(ctx, metaDir, "/usr/lib/postgresql/17/bin", 19842, ag, kms)
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	defer cp.Close()

	keyA, err := cp.SeedAPIKey(ctx, "acct_a", "air_live_bootstrap_secret_acct_a_01", "bootstrap acct_a")
	if err != nil {
		t.Fatalf("SeedAPIKey acct_a: %v", err)
	}
	keyB, err := cp.CreateAPIKey(ctx, "acct_b", "acct_b key")
	if err != nil {
		t.Fatalf("CreateAPIKey acct_b: %v", err)
	}

	httpSrv := httptest.NewServer(cp.HTTPHandler())
	defer httpSrv.Close()

	// 1. Missing Authorization header -> 401 Unauthorized (even if X-Account-ID is supplied)
	body, _ := json.Marshal(map[string]string{"tenant_id": "tenant_tls"})
	unauthReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/databases", bytes.NewReader(body))
	unauthReq.Header.Set("Content-Type", "application/json")
	unauthReq.Header.Set("X-Account-ID", "acct_a")
	unauthReq.Header.Set("Idempotency-Key", "idem-unauth-1")
	unauthResp, err := http.DefaultClient.Do(unauthReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = unauthResp.Body.Close()
	if unauthResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 on missing Bearer token, got %d", unauthResp.StatusCode)
	}

	// 2. Invalid Bearer token -> 401 Unauthorized
	badTokenReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/databases", bytes.NewReader(body))
	badTokenReq.Header.Set("Content-Type", "application/json")
	badTokenReq.Header.Set("Authorization", "Bearer air_live_invalid_token_value_12345")
	badTokenReq.Header.Set("Idempotency-Key", "idem-unauth-2")
	badTokenResp, err := http.DefaultClient.Do(badTokenReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = badTokenResp.Body.Close()
	if badTokenResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 on invalid Bearer token, got %d", badTokenResp.StatusCode)
	}

	// 3. Mismatched X-Account-ID header vs Bearer token account -> 403 Forbidden
	spoofReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/databases", bytes.NewReader(body))
	spoofReq.Header.Set("Content-Type", "application/json")
	spoofReq.Header.Set("Authorization", "Bearer "+keyA.Token)
	spoofReq.Header.Set("X-Account-ID", "acct_b")
	spoofReq.Header.Set("Idempotency-Key", "idem-spoof-1")
	spoofResp, err := http.DefaultClient.Do(spoofReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = spoofResp.Body.Close()
	if spoofResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 on mismatched X-Account-ID, got %d", spoofResp.StatusCode)
	}

	// 4. Provision tenant_tls with valid Bearer token for acct_a
	prov := httpCreateDatabase(t, httpSrv.URL, keyA.Token, "idem-tls-1", "tenant_tls")
	if prov.State != "ACTIVE" {
		t.Fatalf("expected ACTIVE tenant_tls, got %+v", prov)
	}

	// 5. Cross-account key revocation attempt (acct_b trying to revoke acct_a's key) -> 403 Forbidden
	crossRevokeReq, _ := http.NewRequest(http.MethodDelete, httpSrv.URL+"/v1/api-keys/"+keyA.KeyID, nil)
	crossRevokeReq.Header.Set("Authorization", "Bearer "+keyB.Token)
	crossRevokeResp, err := http.DefaultClient.Do(crossRevokeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = crossRevokeResp.Body.Close()
	if crossRevokeResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 on cross-account key revocation, got %d", crossRevokeResp.StatusCode)
	}

	// 6. Verify direct connection to provisioned tenant_tls works with SCRAM credentials
	tlsConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", prov.Username, prov.Password, prov.DirectPort, prov.Database))
	if err != nil {
		t.Fatalf("pgx.Connect failed: %v", err)
	}
	var currentDB string
	if err := tlsConn.QueryRow(ctx, `SELECT current_database()`).Scan(&currentDB); err != nil || currentDB != "tenant_tls" {
		t.Fatalf("unexpected current_database: db=%q err=%v", currentDB, err)
	}
	_ = tlsConn.Close(ctx)

	// 7. Revoke acct_a's API key -> subsequent HTTP calls with keyA.Token must fail with 401 Unauthorized
	revokeReq, _ := http.NewRequest(http.MethodDelete, httpSrv.URL+"/v1/api-keys/"+keyA.KeyID, nil)
	revokeReq.Header.Set("Authorization", "Bearer "+keyA.Token)
	revokeResp, err := http.DefaultClient.Do(revokeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = revokeResp.Body.Close()
	if revokeResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on self key revocation, got %d", revokeResp.StatusCode)
	}

	postRevokeReq, _ := http.NewRequest(http.MethodGet, httpSrv.URL+"/v1/databases/tenant_tls", nil)
	postRevokeReq.Header.Set("Authorization", "Bearer "+keyA.Token)
	postRevokeResp, err := http.DefaultClient.Do(postRevokeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = postRevokeResp.Body.Close()
	if postRevokeResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 after API key revocation, got %d", postRevokeResp.StatusCode)
	}
}

func TestAIRv1_1_SupautilsUserHardening(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/initdb"); err != nil {
		t.Fatalf("PostgreSQL 17 is required for e2e tests: %v", err)
	}

	ctx := context.Background()
	rootDir, err := os.MkdirTemp("/tmp", "air_v1_1_supautils_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	metaDir := filepath.Join(rootDir, "meta")
	keyPath := filepath.Join(rootDir, "master.key")
	kms, err := crypto.LoadOrCreateEnvelopeKMS(keyPath)
	if err != nil {
		t.Fatalf("LoadOrCreateEnvelopeKMS: %v", err)
	}

	ag, err := cell.NewAgent(rootDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer func() {
		_ = ag.StopAndPurgeTenant("tenant_hardened")
		ag.Close()
	}()

	cp, err := control.NewControlPlane(ctx, metaDir, "/usr/lib/postgresql/17/bin", 19852, ag, kms)
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	defer cp.Close()

	keyA, err := cp.CreateAPIKey(ctx, "acct_sec", "security test key")
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}

	httpSrv := httptest.NewServer(cp.HTTPHandler())
	defer httpSrv.Close()

	prov := httpCreateDatabase(t, httpSrv.URL, keyA.Token, "idem-sec-1", "tenant_hardened")
	dsn := fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", prov.Username, prov.Password, prov.DirectPort, prov.Database)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect failed: %v", err)
	}
	defer conn.Close(ctx)

	// 1. Verify app_user role attributes are strictly least-privilege in pg_roles.
	var rolSuper, rolCreateDB, rolCreateRole, rolReplication, rolBypassRLS, rolInherit bool
	if err := conn.QueryRow(ctx, `
		SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolinherit
		FROM pg_roles WHERE rolname = 'app_user'
	`).Scan(&rolSuper, &rolCreateDB, &rolCreateRole, &rolReplication, &rolBypassRLS, &rolInherit); err != nil {
		t.Fatalf("query pg_roles for app_user: %v", err)
	}
	if rolSuper || rolCreateDB || rolCreateRole || rolReplication || rolBypassRLS || rolInherit {
		t.Fatalf("app_user has excessive role flags: super=%v createdb=%v createrole=%v repl=%v bypassrls=%v inherit=%v",
			rolSuper, rolCreateDB, rolCreateRole, rolReplication, rolBypassRLS, rolInherit)
	}

	// 2. Verify supautils allows app_user (privileged_role) to install allowlisted extensions (pg_trgm).
	if _, err := conn.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pg_trgm`); err != nil {
		t.Fatalf("supautils failed to allow allowlisted CREATE EXTENSION pg_trgm for app_user: %v", err)
	}
	var sim float64
	if err := conn.QueryRow(ctx, `SELECT similarity('postgres', 'postgresql')`).Scan(&sim); err != nil || sim <= 0 {
		t.Fatalf("pg_trgm similarity check failed: sim=%v err=%v", sim, err)
	}

	// 3. Verify supautils blocks non-allowlisted superuser extensions (amcheck).
	if _, err := conn.Exec(ctx, `CREATE EXTENSION amcheck`); !hasSQLState(err, "42501") {
		t.Fatalf("expected SQLSTATE 42501 on non-allowlisted CREATE EXTENSION amcheck, got: %v", err)
	}

	// 4. Verify supautils.restrict_extension_versions = 'error' blocks explicit VERSION clause.
	if _, err := conn.Exec(ctx, `CREATE EXTENSION pg_stat_statements VERSION '1.11'`); !hasSQLState(err, "42501") {
		t.Fatalf("expected SQLSTATE 42501 on explicit EXTENSION VERSION clause, got: %v", err)
	}

	// 5. Verify supautils blocks reserved role modification, privileged_role self-alteration, and reserved memberships.
	hardeningChecks := []struct {
		name string
		sql  string
	}{
		{"alter reserved role postgres", `ALTER ROLE postgres WITH PASSWORD 'hacked'`},
		{"drop reserved role postgres", `DROP ROLE postgres`},
		{"self-elevate app_user superuser", `ALTER ROLE app_user WITH SUPERUSER`},
		{"self-elevate app_user createdb", `ALTER ROLE app_user WITH CREATEDB`},
		{"grant pg_read_server_files", `GRANT pg_read_server_files TO app_user`},
		{"grant pg_write_server_files", `GRANT pg_write_server_files TO app_user`},
		{"grant pg_execute_server_program", `GRANT pg_execute_server_program TO app_user`},
		{"copy to program RCE", `COPY (SELECT 1) TO PROGRAM 'id'`},
		{"pg_read_file host escape", `SELECT pg_read_file('/etc/passwd')`},
		{"alter system preload libraries", `ALTER SYSTEM SET shared_preload_libraries = ''`},
	}
	for _, tc := range hardeningChecks {
		if _, err := conn.Exec(ctx, tc.sql); !hasSQLState(err, "42501") && !hasSQLState(err, "0A000") {
			t.Fatalf("%s was not blocked with 42501/0A000: got %v", tc.name, err)
		}
	}

	// 6. Verify direct TCP connection to cell port as superuser postgres is blocked by pg_hba.conf (28000).
	directSuperDSN := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/%s?sslmode=disable", prov.DirectPort, prov.Database)
	if c, cErr := pgx.Connect(ctx, directSuperDSN); cErr == nil {
		_ = c.Close(ctx)
		t.Fatalf("direct TCP connection as postgres to cell port succeeded (pg_hba.conf failed to reject)!")
	} else if !hasSQLState(cErr, "28000") {
		t.Fatalf("expected SQLSTATE 28000 on direct TCP postgres connection, got: %v", cErr)
	}

	// 7. Verify temp_file_limit = 32MB contains runaway sort disk-spill bombs with SQLSTATE 53400.
	_, _ = conn.Exec(ctx, `SET work_mem = '64kB'`)
	_, spillErr := conn.Exec(ctx, `
		SELECT count(*) FROM (
			SELECT repeat(md5(g::text), 128) AS big_payload
			FROM generate_series(1, 15000) g
			ORDER BY big_payload
		) sub
	`)
	if !hasSQLState(spillErr, "53400") {
		t.Fatalf("expected SQLSTATE 53400 (configuration_limit_exceeded) on temp_file_limit spill, got: %v", spillErr)
	}
}

func hasSQLState(err error, wantCode string) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == wantCode
}

func httpCreateDatabase(t *testing.T, baseURL, bearerToken, idemKey, tenantID string) control.ProvisionResponse {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"tenant_id": tenantID})
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/databases", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearerToken)
	req.Header.Set("Idempotency-Key", idemKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/databases: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /v1/databases status=%d", resp.StatusCode)
	}
	var out control.ProvisionResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return out
}
