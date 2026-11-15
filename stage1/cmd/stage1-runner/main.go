package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"air-stage1/pkg/cell"
	"air-stage1/pkg/control"
	"air-stage1/pkg/crypto"
	"air-stage1/pkg/gateway"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Stage1Report struct {
	Timestamp string         `json:"timestamp"`
	ExpA2     map[string]any `json:"exp_a2_concurrent_idempotent_provisioning"`
	ExpA3     map[string]any `json:"exp_a3_crash_readoption_and_orphan_reconcile"`
	Stage1E2E map[string]any `json:"stage1_e2e_gateway_pgbouncer_bbolt_r2_cryptoshred"`
	AllPassed bool           `json:"all_passed"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("=== STARTING STAGE 1 CORE RELIABILITY MVP VERIFICATION SUITE ===")

	ctx := context.Background()
	report := Stage1Report{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		log.Fatalf("kms init: %v", err)
	}

	volBase := "/vol/stage1_main"
	_ = os.RemoveAll(volBase)
	agent, err := cell.NewAgent(volBase, 5, kms)
	if err != nil {
		log.Fatalf("agent init: %v", err)
	}
	defer agent.Close()

	log.Println("[Stage 1] Pre-warming 5 two-phase initdb slots (--data-checksums)...")
	if err := agent.PrewarmPool(5); err != nil {
		log.Fatalf("prewarm pool: %v", err)
	}

	cp, err := control.NewControlPlane(ctx, filepath.Join(volBase, "control_meta"), 15400, agent, kms)
	if err != nil {
		log.Fatalf("control plane init: %v", err)
	}
	defer cp.Close()

	// 1. Run EXP-A2: Concurrent & Idempotent Provisioning (100 parallel requests across 12 unique tenants, 5-slot initial pool)
	log.Println("--- Running EXP-A2: 100 Concurrent & Idempotent Provisioning Requests (Pool + Fallback + UNIQUE(system_identifier)) ---")
	report.ExpA2 = runExpA2(ctx, cp, agent)

	// 2. Run EXP-A3: Interrupted Provisioning, Orphan Reconciler & Live pg-cell-agent SIGKILL Re-Adoption
	log.Println("--- Running EXP-A3: Interrupted initdb/Claiming Reconciler & Live pg-cell-agent SIGKILL Under Active SQL Load ---")
	report.ExpA3 = runExpA3(ctx, cp)

	// 3. Run STAGE1-E2E: Wire Gateway (SSLRequest + SNI==Tenant + 8KB Bomb Fence) + PgBouncer 1.21+ + bbolt RU Ledger + Encrypted R2 Backup/Restore + Crypto-Shredding
	log.Println("--- Running STAGE1-E2E: Wire Gateway + PgBouncer 1.21+ + bbolt RU Ledger + Envelope-Encrypted R2 PITR + Crypto-Shred ---")
	report.Stage1E2E = runStage1E2E(ctx, volBase, cp, agent, kms)

	pA2, _ := report.ExpA2["pass"].(bool)
	pA3, _ := report.ExpA3["pass"].(bool)
	pE2E, _ := report.Stage1E2E["pass"].(bool)
	report.AllPassed = pA2 && pA3 && pE2E

	outBytes, _ := json.MarshalIndent(report, "", "  ")
	_ = os.WriteFile("/vol/stage1-results.json", outBytes, 0o644)
	fmt.Println(string(outBytes))

	if !report.AllPassed {
		log.Fatalf("STAGE 1 FAILED ONE OR MORE GATES (A2=%v, A3=%v, E2E=%v)", pA2, pA3, pE2E)
	}
	log.Println("=== ALL STAGE 1 CORE RELIABILITY MVP GATES PASSED (100%) ===")
}

func runExpA2(ctx context.Context, cp *control.ControlPlane, agent *cell.Agent) map[string]any {
	res := map[string]any{}
	const (
		totalRequests = 100
		uniqueTenants = 12 // Exceeds initial 5-slot pool, exercising both pool-hit (<15ms) and on-demand initdb fallback
	)

	type callOutcome struct {
		idx  int
		key  string
		resp *control.ProvisionResponse
		err  error
	}

	outcomes := make([]callOutcome, totalRequests)
	var wg sync.WaitGroup
	t0 := time.Now()

	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tIdx := i % uniqueTenants
			tenantID := fmt.Sprintf("tn_a2_%02d", tIdx)
			idempKey := fmt.Sprintf("idem_key_a2_%02d", tIdx)
			r, err := cp.ProvisionDatabase(ctx, idempKey, tenantID)
			outcomes[i] = callOutcome{idx: i, key: idempKey, resp: r, err: err}
		}(i)
	}
	wg.Wait()
	burstWallMs := time.Since(t0).Milliseconds()

	errCount := 0
	byKey := make(map[string]*control.ProvisionResponse)
	idempotentConsistent := true
	var poolClaimLatencies []float64
	var fallbackClaimLatencies []float64
	replaysCount := 0

	for _, o := range outcomes {
		if o.err != nil || o.resp == nil {
			errCount++
			log.Printf("[EXP-A2] error on req %d: %v", o.idx, o.err)
			continue
		}
		if o.resp.IdempotentReplay {
			replaysCount++
		} else {
			if o.resp.ClaimSource == "pool" {
				poolClaimLatencies = append(poolClaimLatencies, o.resp.ClaimLatencyMs)
			} else {
				fallbackClaimLatencies = append(fallbackClaimLatencies, o.resp.ClaimLatencyMs)
			}
		}
		if first, exists := byKey[o.key]; exists {
			if first.SystemIdentifier != o.resp.SystemIdentifier || first.Port != o.resp.Port || first.AppPassword != o.resp.AppPassword {
				idempotentConsistent = false
			}
		} else {
			byKey[o.key] = o.resp
		}
	}

	sort.Float64s(poolClaimLatencies)
	sort.Float64s(fallbackClaimLatencies)

	totalDBClusters, distinctSysIDs, dbErr := cp.CountDistinctSystemIdentifiers(ctx)

	// Verify SCRAM-SHA-256 login + SELECT 1 on all 12 provisioned clusters
	scramVerified := 0
	for _, r := range byKey {
		dsn := fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/postgres?sslmode=disable", r.AppUser, r.AppPassword, r.Port)
		conn, err := pgx.Connect(ctx, dsn)
		if err == nil {
			var one int
			if err := conn.QueryRow(ctx, "SELECT 1;").Scan(&one); err == nil && one == 1 {
				scramVerified++
			}
			_ = conn.Close(ctx)
		}
		_ = agent.StopTenantOnly(r.TenantID)
	}

	poolP50 := percentile(poolClaimLatencies, 50)
	poolP99 := percentile(poolClaimLatencies, 99)

	res["total_concurrent_requests"] = totalRequests
	res["unique_idempotency_keys"] = uniqueTenants
	res["failed_requests"] = errCount
	res["idempotent_replays_served"] = replaysCount
	res["idempotent_responses_identical"] = idempotentConsistent
	res["clusters_created_in_db"] = totalDBClusters
	res["distinct_system_identifiers"] = distinctSysIDs
	res["pool_hits_count"] = len(poolClaimLatencies)
	res["on_demand_fallbacks_count"] = len(fallbackClaimLatencies)
	res["pool_claim_p50_ms"] = poolP50
	res["pool_claim_p99_ms"] = poolP99
	res["on_demand_claim_p50_ms"] = percentile(fallbackClaimLatencies, 50)
	res["scram_logins_verified"] = scramVerified
	res["burst_wall_ms"] = burstWallMs

	pass := errCount == 0 &&
		dbErr == nil &&
		idempotentConsistent &&
		totalDBClusters == uniqueTenants &&
		distinctSysIDs == uniqueTenants &&
		scramVerified == uniqueTenants &&
		len(poolClaimLatencies) >= 5 &&
		poolP99 < 15.0

	res["pass"] = pass
	return res
}

func runExpA3(ctx context.Context, cp *control.ControlPlane) map[string]any {
	res := map[string]any{}

	// Part 1: Kill initdb mid-staging + inject uncommitted claiming directories -> run ReconcileOnce
	daemonVol := "/vol/exp_a3_daemon"
	_ = os.RemoveAll(daemonVol)
	_ = os.MkdirAll(filepath.Join(daemonVol, "pool/staging/interrupted_initdb_1"), 0o700)
	_ = exec.Command("chown", "-R", "postgres:postgres", daemonVol).Run()

	// Start a real initdb in staging and SIGKILL it mid-execution
	killStageDir := filepath.Join(daemonVol, "pool/staging/sigkilled_initdb")
	_ = os.MkdirAll(killStageDir, 0o700)
	_ = exec.Command("chown", "-R", "postgres:postgres", killStageDir).Run()
	initCmd := exec.Command("runuser", "-u", "postgres", "--", "/usr/lib/postgresql/17/bin/initdb", "-D", killStageDir, "-U", "postgres", "--data-checksums")
	_ = initCmd.Start()
	time.Sleep(40 * time.Millisecond)
	if initCmd.Process != nil {
		_ = initCmd.Process.Kill()
		_, _ = initCmd.Process.Wait()
	}

	// Also inject uncommitted claiming orphan into main Control Plane's volume
	mainClaimOrphan := "/vol/stage1_main/tenants/claiming/orphan_mid_claim_99"
	_ = os.MkdirAll(mainClaimOrphan, 0o700)
	purgedMain, recErr := cp.ReconcileOnce(ctx)
	_, orphanStillExistsErr := os.Stat(mainClaimOrphan)
	orphanCleaned := os.IsNotExist(orphanStillExistsErr)

	// Part 2: Start standalone pg-cell-agent daemon, provision 3 active tenants, run continuous OLTP queries,
	// send SIGKILL (kill -9) to pg-cell-agent, restart pg-cell-agent, and verify 0 dropped queries + 100% PID re-adoption!
	agentAddr := "127.0.0.1:18081"
	cmd1 := exec.Command("/usr/local/bin/pg-cell-agent", "-vol", daemonVol, "-addr", agentAddr, "-pool", "3")
	if err := cmd1.Start(); err != nil {
		res["error"] = "start daemon 1: " + err.Error()
		res["pass"] = false
		return res
	}
	agent1PID := cmd1.Process.Pid

	if err := waitHTTPReady("http://"+agentAddr+"/health", 15*time.Second); err != nil {
		res["error"] = "wait daemon 1 ready: " + err.Error()
		res["pass"] = false
		return res
	}

	// Verify interrupted staging dirs (interrupted_initdb_1 & sigkilled_initdb) in daemonVol were purged on daemon boot
	_, stErr1 := os.Stat(filepath.Join(daemonVol, "pool/staging/interrupted_initdb_1"))
	_, stErr2 := os.Stat(filepath.Join(daemonVol, "pool/staging/sigkilled_initdb"))
	bootPurgedStaging := os.IsNotExist(stErr1) && os.IsNotExist(stErr2)

	// Provision 3 active tenants on daemon 1
	tenantIDs := []string{"a3_live_1", "a3_live_2", "a3_live_3"}
	initialPIDs := make(map[string]int)
	tenantPorts := make(map[string]int)

	for _, id := range tenantIDs {
		resp, err := http.Get(fmt.Sprintf("http://%s/claim?tenant_id=%s", agentAddr, id))
		if err != nil {
			res["error"] = err.Error()
			res["pass"] = false
			return res
		}
		_ = resp.Body.Close()

		actBody, _ := json.Marshal(map[string]any{"tenant_id": id, "password": "pw_a3_test", "epoch": 1})
		actResp, err := http.Post(fmt.Sprintf("http://%s/activate", agentAddr), "application/json", bytes.NewReader(actBody))
		if err != nil {
			res["error"] = err.Error()
			res["pass"] = false
			return res
		}
		var rt cell.TenantRuntime
		_ = json.NewDecoder(actResp.Body).Decode(&rt)
		_ = actResp.Body.Close()
		initialPIDs[id] = rt.PID
		tenantPorts[id] = rt.Port
	}

	// Launch continuous OLTP query workers against all 3 tenant PostgreSQL clusters
	var activeQueriesSucceeded atomic.Int64
	var activeQueriesDropped atomic.Int64
	stopOLTP := make(chan struct{})
	var oltpWG sync.WaitGroup

	for _, id := range tenantIDs {
		port := tenantPorts[id]
		oltpWG.Add(1)
		go func(id string, port int) {
			defer oltpWG.Done()
			dsn := fmt.Sprintf("postgres://app_user:pw_a3_test@127.0.0.1:%d/postgres?sslmode=disable", port)
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				activeQueriesDropped.Add(1)
				return
			}
			defer func() { _ = conn.Close(ctx) }()
			_, _ = conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS heartbeat (id serial primary key, ts timestamptz default now());")

			for {
				select {
				case <-stopOLTP:
					return
				default:
					if _, err := conn.Exec(ctx, "INSERT INTO heartbeat DEFAULT VALUES;"); err != nil {
						activeQueriesDropped.Add(1)
					} else {
						activeQueriesSucceeded.Add(1)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(id, port)
	}

	// Let OLTP run for 150ms, then SIGKILL pg-cell-agent #1!
	time.Sleep(150 * time.Millisecond)
	_ = cmd1.Process.Kill()
	_, _ = cmd1.Process.Wait()

	// Keep running OLTP while pg-cell-agent is dead for 150ms
	time.Sleep(150 * time.Millisecond)

	// Start pg-cell-agent #2 on the same volume -> must re-adopt all 3 running postgres PIDs without dropping queries!
	tReadopt := time.Now()
	cmd2 := exec.Command("/usr/local/bin/pg-cell-agent", "-vol", daemonVol, "-addr", agentAddr, "-pool", "0")
	if err := cmd2.Start(); err != nil {
		close(stopOLTP)
		oltpWG.Wait()
		res["error"] = "start daemon 2: " + err.Error()
		res["pass"] = false
		return res
	}
	defer func() {
		if cmd2.Process != nil {
			_ = cmd2.Process.Signal(syscall.SIGTERM)
			_, _ = cmd2.Process.Wait()
		}
	}()

	if err := waitHTTPReady("http://"+agentAddr+"/health", 10*time.Second); err != nil {
		close(stopOLTP)
		oltpWG.Wait()
		res["error"] = "wait daemon 2 ready: " + err.Error()
		res["pass"] = false
		return res
	}
	readoptMs := time.Since(tReadopt).Milliseconds()

	// Let OLTP continue for another 150ms after re-adoption
	time.Sleep(150 * time.Millisecond)
	close(stopOLTP)
	oltpWG.Wait()

	// Verify PIDs unchanged and dual-primary start is blocked
	pidsUnchanged := true
	for _, id := range tenantIDs {
		sResp, err := http.Get(fmt.Sprintf("http://%s/start?tenant_id=%s", agentAddr, id))
		if err != nil {
			pidsUnchanged = false
			continue
		}
		var rt cell.TenantRuntime
		_ = json.NewDecoder(sResp.Body).Decode(&rt)
		_ = sResp.Body.Close()
		if rt.PID != initialPIDs[id] || rt.PID == 0 {
			pidsUnchanged = false
		}
	}

	// Also verify external process flock on agent.lock is blocked (LOCK_EX | LOCK_NB returns EWOULDBLOCK)
	lockFile, err := os.OpenFile(filepath.Join(daemonVol, "tenants/active/a3_live_1/agent.lock"), os.O_RDWR, 0o600)
	flockBlocked := false
	if err == nil {
		if flockErr := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); flockErr != nil {
			flockBlocked = true
		} else {
			_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		}
		_ = lockFile.Close()
	}

	hResp, _ := http.Get("http://" + agentAddr + "/health")
	var hData map[string]any
	if hResp != nil {
		_ = json.NewDecoder(hResp.Body).Decode(&hData)
		_ = hResp.Body.Close()
	}

	dropped := activeQueriesDropped.Load()
	succeeded := activeQueriesSucceeded.Load()

	res["agent1_killed_pid"] = agent1PID
	res["agent2_pid"] = cmd2.Process.Pid
	res["boot_purged_interrupted_staging"] = bootPurgedStaging
	res["reconciler_purged_orphans_count"] = purgedMain
	res["orphan_dirs_after_reconcile"] = btoi(!orphanCleaned)
	res["active_queries_succeeded_across_sigkill"] = succeeded
	res["dropped_queries_during_agent_sigkill"] = dropped
	res["readopt_recovery_ms"] = readoptMs
	res["postgres_pids_unchanged"] = pidsUnchanged
	res["external_flock_dual_primary_blocked"] = flockBlocked
	res["daemon2_health"] = hData

	pass := recErr == nil &&
		bootPurgedStaging &&
		orphanCleaned &&
		dropped == 0 &&
		succeeded >= 30 &&
		pidsUnchanged &&
		flockBlocked

	res["pass"] = pass
	return res
}

func runStage1E2E(ctx context.Context, volBase string, cp *control.ControlPlane, agent *cell.Agent, kms *crypto.EnvelopeKMS) map[string]any {
	res := map[string]any{}

	boltPath := filepath.Join(volBase, "gateway_ru.bolt")
	gw, err := gateway.NewGateway("gw-stage1-1", "127.0.0.1:15432", boltPath, cp)
	if err != nil {
		res["error"] = "new gateway: " + err.Error()
		res["pass"] = false
		return res
	}
	defer gw.Close()

	// 1. Provision E2E tenant via Control Plane
	prov, err := cp.ProvisionDatabase(ctx, "idem_e2e_main", "e2e_tenant")
	if err != nil {
		res["error"] = "provision e2e_tenant: " + err.Error()
		res["pass"] = false
		return res
	}

	// 2. Test Gateway Security Fence #1: >8KB StartupMessage Length Bomb (FMEA-16)
	lengthBombBlocked := testLengthBomb(gw.Addr())

	// 3. Test Gateway Security Fence #2: SNI != StartupMessage Tenant Spoof (FMEA-17)
	sniSpoofSQLState := testSNISpoof(ctx, gw.Addr(), "victim.db.air.local", "e2e_tenant", prov.AppUser, prov.AppPassword)
	sniSpoofRejected := sniSpoofSQLState == "28000"

	// 4. Connect via Gateway Direct Mode (TLS + SNI == e2e_tenant.db.air.local + SCRAM-SHA-256)
	directCfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s/e2e_tenant?sslmode=require", prov.AppUser, prov.AppPassword, gw.Addr()))
	if err != nil {
		res["error"] = err.Error()
		res["pass"] = false
		return res
	}
	directCfg.TLSConfig = &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "e2e_tenant.db.air.local",
	}
	directConn, err := pgx.ConnectConfig(ctx, directCfg)
	if err != nil {
		res["error"] = "gateway direct connect failed: " + err.Error()
		res["pass"] = false
		return res
	}
	// Install allowlisted extension pg_trgm as non-superuser app_user via supautils and create table
	_, err = directConn.Exec(ctx, `
		CREATE EXTENSION IF NOT EXISTS pg_trgm;
		CREATE TABLE orders (id INT PRIMARY KEY, item TEXT NOT NULL);
	`)
	_ = directConn.Close(ctx)
	if err != nil {
		res["error"] = "direct SQL setup failed: " + err.Error()
		res["pass"] = false
		return res
	}

	// 5. Connect via Gateway Pooled Mode -> PgBouncer 1.21+ (transaction pooling + protocol prepared statements, ADR-009)
	poolCfg, _ := pgx.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s/e2e_tenant?sslmode=require", prov.AppUser, prov.AppPassword, gw.Addr()))
	poolCfg.TLSConfig = &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "e2e_tenant-pooler.db.air.local",
	}
	poolConn, err := pgx.ConnectConfig(ctx, poolCfg)
	if err != nil {
		res["error"] = "gateway pooled pgbouncer connect failed: " + err.Error()
		res["pass"] = false
		return res
	}
	_, prepErr := poolConn.Prepare(ctx, "ins_order", "INSERT INTO orders (id, item) VALUES ($1, $2)")
	preparedStmtsOK := prepErr == nil
	for i := 1; i <= 10; i++ {
		if _, err := poolConn.Exec(ctx, "ins_order", i, fmt.Sprintf("item_%d", i)); err != nil {
			preparedStmtsOK = false
			res["prep_exec_err"] = err.Error()
			break
		}
	}
	_ = poolConn.Close(ctx)

	// Give Gateway goroutine 50ms to flush connection usage into local bbolt WAL
	time.Sleep(50 * time.Millisecond)

	// 6. Verify local bbolt WAL records & 5x Queue Redelivery Idempotency (ADR-004, ADR-012, FMEA-22, FMEA-23)
	pendingWAL, _ := gw.ReadPendingBoltWAL()
	totalInsertedAcross5Replays, batchCount, flushErr := gw.FlushBoltWALToControlPlane(ctx, 5)
	ledgerBatches, ledgerStmts, ledgerRUs, _ := cp.GetTenantUsageSummary(ctx, "e2e_tenant")
	duplicateBillingRows := totalInsertedAcross5Replays - batchCount

	// 7. Full + Incremental PG17 Backup with ZSTD + AES-256-GCM Envelope Encryption (AAD) -> Cloudflare R2 + Combine Restore
	s3c, bucket, r2Err := cell.NewR2ClientFromEnv(ctx)
	if r2Err != nil {
		res["error"] = "r2 client: " + r2Err.Error()
		res["pass"] = false
		return res
	}

	fullArt, fullManifestPath, err := agent.RunEncryptedBackup(ctx, s3c, bucket, "e2e_tenant", "FULL", "")
	if err != nil {
		res["error"] = "full backup failed: " + err.Error()
		res["pass"] = false
		return res
	}

	// Insert 10 more rows (11..20) before Incremental backup
	directConn2, err := pgx.ConnectConfig(ctx, directCfg)
	if err != nil {
		res["error"] = "direct reconnect failed: " + err.Error()
		res["pass"] = false
		return res
	}
	for i := 11; i <= 20; i++ {
		_, _ = directConn2.Exec(ctx, "INSERT INTO orders (id, item) VALUES ($1, $2)", i, fmt.Sprintf("item_%d", i))
	}
	_ = directConn2.Close(ctx)

	incrArt, _, err := agent.RunEncryptedBackup(ctx, s3c, bucket, "e2e_tenant", "INCR", fullManifestPath)
	if err != nil {
		res["error"] = "incr backup failed: " + err.Error()
		res["pass"] = false
		return res
	}

	// Test Cross-Tenant AAD Tamper Rejection (SI-4): try decrypting with another tenant's DEK/AAD or wrong epoch
	_, _ = kms.EnsureTenantDEK("other_tenant")
	sampleCipher, _ := kms.EncryptObject("e2e_tenant", 1, fullArt.R2Key, []byte("secret-pg-page"))
	_, wrongTenantErr := kms.DecryptObject("other_tenant", 1, fullArt.R2Key, sampleCipher)
	_, wrongEpochErr := kms.DecryptObject("e2e_tenant", 2, fullArt.R2Key, sampleCipher)
	aadTamperRejected := errors.Is(wrongTenantErr, crypto.ErrAADMismatch) && errors.Is(wrongEpochErr, crypto.ErrAADMismatch)

	// Combine & restore FULL + INCR from Cloudflare R2 and verify all 20 rows + pg_amcheck
	restoredCountStr, amcheckOK, restoreErr := agent.DownloadAndCombineRestore(
		ctx, s3c, bucket, "e2e_tenant",
		[]*cell.BackupArtifact{fullArt, incrArt},
		"SELECT count(*) FROM orders;",
	)

	// 8. Test Soft Delete (SQLSTATE 57P03 login gate while DEK remains intact for 7-day grace)
	_ = cp.DeleteDatabase(ctx, "e2e_tenant", false)
	softDeleteSQLState := testSNISpoof(ctx, gw.Addr(), "e2e_tenant.db.air.local", "e2e_tenant", prov.AppUser, prov.AppPassword)
	_, dekBeforeShredErr := kms.UnwrapDEK("e2e_tenant")
	softDeleteGraceDEKIntact := dekBeforeShredErr == nil

	// 9. Test Hard Purge Crypto-Shredding (DEK destroyed -> ciphertext permanently unrecoverable, Flow 12)
	_ = cp.DeleteDatabase(ctx, "e2e_tenant", true)
	_, dekAfterShredErr := kms.DecryptObject("e2e_tenant", 1, fullArt.R2Key, sampleCipher)
	cryptoShredUnrecoverable := errors.Is(dekAfterShredErr, crypto.ErrDEKShredded)

	// Cleanup Stage 1 R2 test objects
	_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(fullArt.R2Key)})
	_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(incrArt.R2Key)})

	res["length_bomb_rejected"] = lengthBombBlocked
	res["sni_spoof_sqlstate"] = sniSpoofSQLState
	res["sni_spoof_rejected"] = sniSpoofRejected
	res["pgbouncer_prepared_stmts_ok"] = preparedStmtsOK
	res["bbolt_wal_pending_batches"] = len(pendingWAL)
	res["billing_5x_replay_inserted_total"] = totalInsertedAcross5Replays
	res["billing_duplicate_rows_on_5x_replay"] = duplicateBillingRows
	res["ledger_batches_recorded"] = ledgerBatches
	res["ledger_statements_recorded"] = ledgerStmts
	res["ledger_total_ru_charged"] = ledgerRUs
	res["full_backup_r2_key"] = fullArt.R2Key
	res["full_backup_cipher_bytes"] = fullArt.CipherBytes
	res["incr_backup_r2_key"] = incrArt.R2Key
	res["incr_backup_cipher_bytes"] = incrArt.CipherBytes
	res["aad_tamper_rejected"] = aadTamperRejected
	res["restored_orders_count"] = restoredCountStr
	res["restored_pg_amcheck_ok"] = amcheckOK
	res["restore_err"] = errStr(restoreErr)
	res["soft_delete_sqlstate"] = softDeleteSQLState
	res["soft_delete_grace_dek_intact"] = softDeleteGraceDEKIntact
	res["crypto_shred_unrecoverable"] = cryptoShredUnrecoverable

	pass := lengthBombBlocked &&
		sniSpoofRejected &&
		preparedStmtsOK &&
		flushErr == nil &&
		batchCount > 0 &&
		duplicateBillingRows == 0 &&
		ledgerRUs > 0 &&
		aadTamperRejected &&
		restoreErr == nil &&
		restoredCountStr == "20" &&
		amcheckOK &&
		softDeleteSQLState == "57P03" &&
		softDeleteGraceDEKIntact &&
		cryptoShredUnrecoverable

	res["pass"] = pass
	return res
}

func testLengthBomb(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	defer func() { _ = conn.Close() }()
	// Send 100 MB StartupMessage length header (>8KB limit)
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], 100*1024*1024)
	binary.BigEndian.PutUint32(hdr[4:8], 196608)
	_, _ = conn.Write(hdr[:])
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	reply, _ := io.ReadAll(conn)
	return bytes.Contains(reply, []byte("08P01"))
}

func testSNISpoof(ctx context.Context, addr, sniServerName, dbName, user, pass string) string {
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=require", user, pass, addr, dbName))
	if err != nil {
		return ""
	}
	cfg.TLSConfig = &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         sniServerName,
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err == nil {
		_ = conn.Close(ctx)
		return "CONNECTED_UNEXPECTEDLY"
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return err.Error()
}

func waitHTTPReady(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", url)
}

func percentile(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := (len(sorted) * p) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}
