package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	mode := flag.String("mode", "storage-path", "Mode: storage-path|crash-recovery|soak|multitenant")
	duration := flag.Duration("duration", 300*time.Second, "Duration for soak/multitenant tests")
	concurrency := flag.Int("concurrency", 16, "Client concurrency for soak test")
	tenantPrefix := flag.String("tenant-prefix", "p21", "Tenant ID prefix")
	flag.Parse()

	ctx := context.Background()
	switch *mode {
	case "storage-path":
		if err := runStoragePathValidation(ctx, *tenantPrefix+"_sp"); err != nil {
			log.Fatalf("storage-path failed: %v", err)
		}
	case "crash-recovery":
		if err := runCrashRecoveryValidation(ctx, *tenantPrefix+"_cr"); err != nil {
			log.Fatalf("crash-recovery failed: %v", err)
		}
	case "soak":
		if err := runSoakTest(ctx, *tenantPrefix+"_soak", *duration, *concurrency); err != nil {
			log.Fatalf("soak failed: %v", err)
		}
	case "multitenant":
		if err := runMultiTenantTest(ctx, *tenantPrefix+"_mt", *duration); err != nil {
			log.Fatalf("multitenant failed: %v", err)
		}
	default:
		log.Fatalf("unknown mode: %s", *mode)
	}
}

func runStoragePathValidation(ctx context.Context, tenantID string) error {
	log.Printf("=== Phase 21.3: Full AIR Storage Path Integration (%s) ===", tenantID)
	t, err := provisionTenant(ctx, tenantID)
	if err != nil {
		return err
	}
	log.Printf("Provisioned tenant=%s gen=%s direct_port=%d", t.TenantID, t.Generation, t.DirectPort)

	pool, err := connectGatewayPool(ctx, t, 8)
	if err != nil {
		return err
	}
	defer pool.Close()

	// 1. Relation + Index creation + INSERT + UPDATE + DELETE + SELECT
	_, err = pool.Exec(ctx, `
		CREATE TABLE p21_orders (
			id bigserial PRIMARY KEY,
			acct text NOT NULL,
			amount int NOT NULL,
			payload text NOT NULL
		);
		CREATE INDEX idx_p21_orders_acct ON p21_orders(acct);
		INSERT INTO p21_orders (acct, amount, payload)
		SELECT 'acct_' || (g % 10), g * 10, repeat('a', 120)
		FROM generate_series(1, 1000) g;
		UPDATE p21_orders SET amount = amount + 5 WHERE id <= 200;
		DELETE FROM p21_orders WHERE id > 900;
	`)
	if err != nil {
		return fmt.Errorf("ddl/dml: %w", err)
	}

	// 2. Explicit TX commit and rollback
	txCommit, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	if _, err := txCommit.Exec(ctx, `INSERT INTO p21_orders (acct, amount, payload) VALUES ('tx_ok', 777, 'committed')`); err != nil {
		return err
	}
	if err := txCommit.Commit(ctx); err != nil {
		return err
	}

	txRollback, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	if _, err := txRollback.Exec(ctx, `INSERT INTO p21_orders (acct, amount, payload) VALUES ('tx_rb', 999, 'rolled_back')`); err != nil {
		return err
	}
	if err := txRollback.Rollback(ctx); err != nil {
		return err
	}

	var cnt int64
	var sumAmt int64
	var rbCnt int64
	if err := pool.QueryRow(ctx, `SELECT count(*), coalesce(sum(amount),0) FROM p21_orders`).Scan(&cnt, &sumAmt); err != nil {
		return err
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM p21_orders WHERE acct = 'tx_rb'`).Scan(&rbCnt); err != nil {
		return err
	}
	if cnt != 901 || rbCnt != 0 {
		return fmt.Errorf("unexpected counts after commit/rollback: cnt=%d rbCnt=%d", cnt, rbCnt)
	}
	log.Printf("DML + Index + Commit/Rollback verified: count=%d sum=%d rbCnt=%d", cnt, sumAmt, rbCnt)

	// 3. Prove air_smgr bypassed local base/ and stored on Pageserver
	var dbOID, relNode uint32
	catTx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	err = catTx.QueryRow(ctx, `
		SELECT (SELECT oid FROM pg_database WHERE datname = current_database())::int4,
		       pg_relation_filenode('p21_orders')::int4
	`).Scan(&dbOID, &relNode)
	_ = catTx.Rollback(ctx)
	if err != nil {
		return err
	}
	localRelPath := fmt.Sprintf("/var/lib/air/tenants/active/%s/pgdata/base/%d/%d", tenantID, dbOID, relNode)
	if _, err := dockerCmd("exec", "air-p21-server", "test", "!", "-e", localRelPath); err != nil {
		return fmt.Errorf("local relation file %s exists on air-server disk; air_smgr bypassed", localRelPath)
	}
	psLayers, err := dockerCmd("exec", "air-p21-pageserver", "ls", "-la", fmt.Sprintf("/var/lib/air-pageserver/ps_layers/%s", tenantID))
	if err != nil {
		return fmt.Errorf("pageserver layers missing for %s: %v", tenantID, err)
	}
	log.Printf("CONFIRMED: local %s absent on air-server; Pageserver layers present:\n%s", localRelPath, psLayers)

	// 4. PG Restart via Control Plane suspend/wake
	if err := suspendTenant(ctx, tenantID); err != nil {
		return err
	}
	if err := wakeTenant(ctx, tenantID); err != nil {
		return err
	}
	var cntAfterRestart int64
	var sumAfterRestart int64
	if err := pool.QueryRow(ctx, `SELECT count(*), coalesce(sum(amount),0) FROM p21_orders`).Scan(&cntAfterRestart, &sumAfterRestart); err != nil {
		return err
	}
	if cntAfterRestart != cnt || sumAfterRestart != sumAmt {
		return fmt.Errorf("restart mismatch: got (%d,%d) want (%d,%d)", cntAfterRestart, sumAfterRestart, cnt, sumAmt)
	}
	log.Printf("PG restart (suspend/wake) verified: count=%d sum=%d", cntAfterRestart, sumAfterRestart)

	// 5. Pageserver restart
	if _, err := dockerCmd("restart", "air-p21-pageserver"); err != nil {
		return err
	}
	if err := waitHealthy(ctx, pageserverURL+"/healthz", 20*time.Second); err != nil {
		return err
	}
	// Trigger read/write after Pageserver restart (retry briefly if PG is completing automatic recovery)
	var insErr error
	for i := 0; i < 20; i++ {
		_, insErr = pool.Exec(ctx, `INSERT INTO p21_orders (acct, amount, payload) VALUES ('after_ps_restart', 111, 'ok')`)
		if insErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if insErr != nil {
		return fmt.Errorf("insert after pageserver restart: %w", insErr)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM p21_orders`).Scan(&cntAfterRestart); err != nil {
		return fmt.Errorf("select after pageserver restart: %w", err)
	}
	if cntAfterRestart != 902 {
		return fmt.Errorf("expected 902 rows after pageserver restart, got %d", cntAfterRestart)
	}
	log.Printf("Pageserver restart verified: count=%d", cntAfterRestart)
	log.Printf("=== Phase 21.3 PASSED ===")
	return nil
}

func runCrashRecoveryValidation(ctx context.Context, tenantID string) error {
	log.Printf("=== Phase 21.4: Durability & Crash Recovery (%s) ===", tenantID)
	t, err := provisionTenant(ctx, tenantID)
	if err != nil {
		return err
	}
	pool, err := connectGatewayPool(ctx, t, 16)
	if err != nil {
		return err
	}
	defer pool.Close()

	// Check durability GUCs (fsync, full_page_writes, synchronous_commit)
	var fsyncVal, fpwVal, syncCommitVal string
	if err := pool.QueryRow(ctx, `SHOW fsync`).Scan(&fsyncVal); err != nil {
		return err
	}
	if err := pool.QueryRow(ctx, `SHOW full_page_writes`).Scan(&fpwVal); err != nil {
		return err
	}
	if err := pool.QueryRow(ctx, `SHOW synchronous_commit`).Scan(&syncCommitVal); err != nil {
		return err
	}
	log.Printf("Durability GUCs: fsync=%s full_page_writes=%s synchronous_commit=%s", fsyncVal, fpwVal, syncCommitVal)
	if fsyncVal != "on" || fpwVal != "on" || syncCommitVal != "on" {
		return fmt.Errorf("durability GUCs weakened: fsync=%s fpw=%s sync=%s", fsyncVal, fpwVal, syncCommitVal)
	}

	if _, err := pool.Exec(ctx, `CREATE TABLE cr_ledger (id int PRIMARY KEY, phase text NOT NULL, val int NOT NULL)`); err != nil {
		return err
	}

	// Track committed IDs deterministically
	committed := int64(0)
	insertCommitted := func(id int, phase string) error {
		_, err := pool.Exec(ctx, `INSERT INTO cr_ledger (id, phase, val) VALUES ($1, $2, $3)`, id, phase, id*10)
		if err == nil {
			atomic.AddInt64(&committed, 1)
		}
		return err
	}

	for i := 1; i <= 100; i++ {
		if err := insertCommitted(i, "initial"); err != nil {
			return err
		}
	}

	// Start an uncommitted transaction that will be interrupted by SIGKILL
	rawDSN := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", t.Username, t.Password, gatewayPGHost, gatewayPGPort, t.Database)
	uncommittedConn, err := pgx.Connect(ctx, rawDSN)
	if err != nil {
		return err
	}
	txUncomm, err := uncommittedConn.Begin(ctx)
	if err != nil {
		_ = uncommittedConn.Close(ctx)
		return err
	}
	if _, err := txUncomm.Exec(ctx, `INSERT INTO cr_ledger (id, phase, val) VALUES (999001, 'uncommitted', 1)`); err != nil {
		_ = uncommittedConn.Close(ctx)
		return err
	}

	// 1. PG SIGKILL during active uncommitted TX
	log.Printf("1. Sending SIGKILL to tenant postgres postmaster...")
	pidOut, err := dockerCmd("exec", "air-p21-server", "head", "-n", "1", fmt.Sprintf("/var/lib/air/tenants/active/%s/pgdata/postmaster.pid", tenantID))
	if err != nil {
		_ = uncommittedConn.Close(ctx)
		return err
	}
	if _, err := dockerCmd("exec", "air-p21-server", "sh", "-c", "kill -9 "+pidOut); err != nil {
		_ = uncommittedConn.Close(ctx)
		return err
	}
	_ = uncommittedConn.Close(ctx)
	time.Sleep(1 * time.Second)

	// Wake/adopt via Control Plane or restart air-server
	if _, err := dockerCmd("exec", "air-p21-server", "rm", "-f", fmt.Sprintf("/var/lib/air/tenants/active/%s/pgdata/postmaster.pid", tenantID)); err != nil {
		return err
	}
	log.Printf("1b. Waking tenant %s after SIGKILL...", tenantID)
	wakeCtx, wakeCancel := context.WithTimeout(ctx, 10*time.Second)
	err = wakeTenant(wakeCtx, tenantID)
	wakeCancel()
	if err != nil {
		// If still marked ACTIVE in meta, restart air-server to trigger ReadoptRunningTenants / recovery
		log.Printf("wake returned %v; restarting air-p21-server to test Cell Agent recovery...", err)
	}

	// 2. Cell Agent (air-p21-server) restart
	log.Printf("2. Restarting air-p21-server (Cell Agent)...")
	if _, err := dockerCmd("restart", "air-p21-server"); err != nil {
		return err
	}
	if err := waitHealthy(ctx, controlPlaneURL+"/healthz", 30*time.Second); err != nil {
		return err
	}
	// Ensure tenant is active via wake (in case it was suspended/crashed)
	_ = wakeTenant(ctx, tenantID)

	// 3. Gateway restart
	log.Printf("3. Restarting air-p21-gateway...")
	if _, err := dockerCmd("restart", "air-p21-gateway"); err != nil {
		return err
	}
	if err := waitHealthy(ctx, gatewayAdminURL+"/healthz", 20*time.Second); err != nil {
		return err
	}

	// Verify 100 committed rows intact and uncommitted row 999001 rolled back
	var cnt, uncommCnt int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cr_ledger`).Scan(&cnt); err != nil {
		return fmt.Errorf("post-cell-gateway-restart query: %w", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cr_ledger WHERE id = 999001`).Scan(&uncommCnt); err != nil {
		return err
	}
	if cnt != 100 || uncommCnt != 0 {
		return fmt.Errorf("integrity mismatch after SIGKILL+Agent+Gateway restart: cnt=%d uncommCnt=%d", cnt, uncommCnt)
	}
	log.Printf("Verified after SIGKILL + Cell Agent restart + Gateway restart: committed=%d uncommitted=%d", cnt, uncommCnt)

	// 4. Pageserver restart during writes
	for i := 101; i <= 150; i++ {
		if err := insertCommitted(i, "pre_ps_restart"); err != nil {
			return err
		}
	}
	log.Printf("4. Restarting air-p21-pageserver...")
	if _, err := dockerCmd("restart", "air-p21-pageserver"); err != nil {
		return err
	}
	if err := waitHealthy(ctx, pageserverURL+"/healthz", 20*time.Second); err != nil {
		return err
	}
	for i := 151; i <= 200; i++ {
		if err := insertCommitted(i, "post_ps_restart"); err != nil {
			return err
		}
	}

	// 5. Temporary network interruption (disconnect/reconnect air-p21-pageserver from air-p21-net)
	log.Printf("5. Simulating temporary network interruption (disconnecting air-p21-pageserver for 2s)...")
	if _, err := dockerCmd("network", "disconnect", "air-p21-net", "air-p21-pageserver"); err != nil {
		return err
	}
	time.Sleep(2 * time.Second)
	if _, err := dockerCmd("network", "connect", "--alias", "air-pageserver", "air-p21-net", "air-p21-pageserver"); err != nil {
		return err
	}
	if err := waitHealthy(ctx, pageserverURL+"/healthz", 15*time.Second); err != nil {
		return err
	}
	for i := 201; i <= 250; i++ {
		if err := insertCommitted(i, "post_net_heal"); err != nil {
			return err
		}
	}

	// Final integrity check: index scan + seq scan + sum
	var finalCnt, finalSum int64
	if err := pool.QueryRow(ctx, `SELECT count(*), sum(val) FROM cr_ledger`).Scan(&finalCnt, &finalSum); err != nil {
		return err
	}
	expectedSum := int64(250 * 251 / 2 * 10)
	if finalCnt != 250 || finalSum != expectedSum {
		return fmt.Errorf("final integrity mismatch: cnt=%d sum=%d want (250,%d)", finalCnt, finalSum, expectedSum)
	}
	log.Printf("Final integrity check PASSED: count=%d sum=%d (0 data loss, 0 corruption)", finalCnt, finalSum)
	log.Printf("=== Phase 21.4 PASSED ===")
	return nil
}

func runSoakTest(ctx context.Context, tenantID string, dur time.Duration, concurrency int) error {
	log.Printf("=== Phase 21.5: Production Stability Soak Test (%s, dur=%s, c=%d) ===", tenantID, dur, concurrency)
	t, err := provisionTenant(ctx, tenantID)
	if err != nil {
		return err
	}
	pool, err := connectGatewayPool(ctx, t, int32(concurrency+4))
	if err != nil {
		return err
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS soak_kv (
			k int PRIMARY KEY,
			v text NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT now()
		);
		INSERT INTO soak_kv (k, v)
		SELECT g, repeat('s', 100) FROM generate_series(1, 2000) g
		ON CONFLICT (k) DO NOTHING;
		CREATE TABLE IF NOT EXISTS soak_log (
			id bigserial PRIMARY KEY,
			worker_id int NOT NULL,
			payload text NOT NULL
		);
	`); err != nil {
		return err
	}

	stopCtx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()

	var totalOps, totalErrs atomic.Int64
	var latMu sync.Mutex
	var intervalLats []float64
	var allLats []float64

	// Telemetry sampler every 5s
	type sampleRow struct {
		ElapsedSec int
		IntervalTPS float64
		P50Ms      float64
		P95Ms      float64
		P99Ms      float64
		Errs       int64
		Cgroup     CgroupSample
		ActiveSlots float64
		QueueDepth  float64
	}
	var samples []sampleRow
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		start := time.Now()
		prevOps := int64(0)
		for {
			select {
			case <-stopCtx.Done():
				return
			case <-ticker.C:
				curOps := totalOps.Load()
				curErrs := totalErrs.Load()
				tps := float64(curOps-prevOps) / 5.0
				prevOps = curOps

				latMu.Lock()
				lats := intervalLats
				intervalLats = nil
				latMu.Unlock()
				p50, p95, p99 := calcPercentiles(lats)
				cg := sampleContainerCgroup("air-p21-server")
				gov := getGatewayGovStats(ctx)
				var activeSlots, qDepth float64
				if gov != nil {
					if v, ok := gov["active_writes"].(float64); ok {
						activeSlots = v
					}
					if v, ok := gov["queued_writes"].(float64); ok {
						qDepth = v
					}
				}
				row := sampleRow{
					ElapsedSec:  int(time.Since(start).Seconds()),
					IntervalTPS: tps,
					P50Ms:       p50,
					P95Ms:       p95,
					P99Ms:       p99,
					Errs:        curErrs,
					Cgroup:      cg,
					ActiveSlots: activeSlots,
					QueueDepth:  qDepth,
				}
				samples = append(samples, row)
				log.Printf("[soak t=%ds] tps=%.1f p50=%.2fms p95=%.2fms p99=%.2fms errs=%d cell_mem=%.1fMB(peak=%.1fMB anon=%.1fMB file=%.1fMB high_ev=%d oom=%d) pg_rss=%.1fMB gw=%.1fMB ps=%.1fMB memPSI=%.2f cpuPSI=%.2f ioPSI=%.2f",
					row.ElapsedSec, tps, p50, p95, p99, curErrs,
					cg.MemCurrentMB, cg.MemPeakMB, cg.AnonMB, cg.FileMB, cg.HighEvents, cg.OOMKillEvents,
					cg.PGRSSMB, cg.GatewayRSSMB, cg.PageserverMB, cg.MemPSISome10, cg.CPUPSISome10, cg.IOPSIMax10)
			}
		}
	}()

	// Mixed workload workers: INSERT, UPDATE, UPSERT, DELETE, SELECT, COPY FROM STDIN, Explicit TX
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(wid)*997))
			for stopCtx.Err() == nil {
				t0 := time.Now()
				var opErr error
				choice := rng.Intn(100)
				switch {
				case choice < 30: // INSERT
					_, opErr = pool.Exec(stopCtx, `INSERT INTO soak_log (worker_id, payload) VALUES ($1, repeat('w', 100))`, wid)
				case choice < 50: // UPDATE
					k := rng.Intn(2000) + 1
					_, opErr = pool.Exec(stopCtx, `UPDATE soak_kv SET v = repeat('u', 100), updated_at = now() WHERE k = $1`, k)
				case choice < 65: // UPSERT
					k := rng.Intn(2000) + 1
					_, opErr = pool.Exec(stopCtx, `INSERT INTO soak_kv (k, v) VALUES ($1, repeat('p', 100)) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, updated_at = now()`, k)
				case choice < 75: // SELECT
					k := rng.Intn(2000) + 1
					var v string
					opErr = pool.QueryRow(stopCtx, `SELECT v FROM soak_kv WHERE k = $1`, k).Scan(&v)
				case choice < 85: // Explicit TX (UPDATE + INSERT)
					tx, err := pool.Begin(stopCtx)
					if err != nil {
						opErr = err
					} else {
						k := rng.Intn(2000) + 1
						_, opErr = tx.Exec(stopCtx, `UPDATE soak_kv SET updated_at = now() WHERE k = $1`, k)
						if opErr == nil {
							_, opErr = tx.Exec(stopCtx, `INSERT INTO soak_log (worker_id, payload) VALUES ($1, 'tx')`, wid)
						}
						if opErr == nil {
							opErr = tx.Commit(stopCtx)
						} else {
							_ = tx.Rollback(stopCtx)
						}
					}
				case choice < 95: // DELETE (bounded)
					_, opErr = pool.Exec(stopCtx, `DELETE FROM soak_log WHERE id IN (SELECT id FROM soak_log WHERE worker_id = $1 LIMIT 1)`, wid)
				default: // Small streaming COPY (50 rows)
					rows := make([][]any, 50)
					for i := 0; i < 50; i++ {
						rows[i] = []any{wid, "copy_row"}
					}
					_, opErr = pool.CopyFrom(stopCtx, pgx.Identifier{"soak_log"}, []string{"worker_id", "payload"}, pgx.CopyFromRows(rows))
				}

				if stopCtx.Err() != nil {
					break
				}
				latUs := float64(time.Since(t0).Microseconds())
				if opErr != nil {
					n := totalErrs.Add(1)
					if n <= 10 {
						log.Printf("[soak err #%d] choice=%d err=%v", n, choice, opErr)
					}
				} else {
					totalOps.Add(1)
					latMu.Lock()
					intervalLats = append(intervalLats, latUs)
					if len(allLats) < 500000 {
						allLats = append(allLats, latUs)
					}
					latMu.Unlock()
				}
			}
		}(w)
	}

	wg.Wait()
	p50, p95, p99 := calcPercentiles(allLats)
	ops := totalOps.Load()
	errs := totalErrs.Load()
	avgTPS := float64(ops) / dur.Seconds()
	finalCG := sampleContainerCgroup("air-p21-server")

	// Save raw CSV
	outDir := filepath.Join("benchmarks", "phase21", "results")
	_ = os.MkdirAll(outDir, 0o755)
	csvPath := filepath.Join(outDir, fmt.Sprintf("soak_%s_%ds.csv", tenantID, int(dur.Seconds())))
	f, _ := os.Create(csvPath)
	if f != nil {
		fmt.Fprintln(f, "elapsed_s,tps,p50_ms,p95_ms,p99_ms,errs,cell_cur_mb,cell_peak_mb,anon_mb,file_mb,high_events,oom_kills,pg_rss_mb,gw_mb,ps_mb,mem_psi,cpu_psi,io_psi")
		for _, r := range samples {
			fmt.Fprintf(f, "%d,%.2f,%.2f,%.2f,%.2f,%d,%.2f,%.2f,%.2f,%.2f,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n",
				r.ElapsedSec, r.IntervalTPS, r.P50Ms, r.P95Ms, r.P99Ms, r.Errs,
				r.Cgroup.MemCurrentMB, r.Cgroup.MemPeakMB, r.Cgroup.AnonMB, r.Cgroup.FileMB,
				r.Cgroup.HighEvents, r.Cgroup.OOMKillEvents, r.Cgroup.PGRSSMB, r.Cgroup.GatewayRSSMB,
				r.Cgroup.PageserverMB, r.Cgroup.MemPSISome10, r.Cgroup.CPUPSISome10, r.Cgroup.IOPSIMax10)
		}
		_ = f.Close()
	}

	log.Printf("=== Soak Summary (%s) ===", dur)
	log.Printf("Total Ops: %d | Avg TPS: %.1f | Errors: %d | p50=%.2fms p95=%.2fms p99=%.2fms", ops, avgTPS, errs, p50, p95, p99)
	log.Printf("Final Cell Cgroup: current=%.1fMB peak=%.1fMB anon=%.1fMB file=%.1fMB high_ev=%d oom_kills=%d",
		finalCG.MemCurrentMB, finalCG.MemPeakMB, finalCG.AnonMB, finalCG.FileMB, finalCG.HighEvents, finalCG.OOMKillEvents)
	log.Printf("Raw CSV: %s", csvPath)

	if errs > 0 || finalCG.OOMKillEvents > 0 {
		return fmt.Errorf("soak acceptance failed: errs=%d oom_kills=%d", errs, finalCG.OOMKillEvents)
	}
	return nil
}

func runMultiTenantTest(ctx context.Context, prefix string, dur time.Duration) error {
	log.Printf("=== Phase 21.6: 4-Tenant Stability Test (prefix=%s, dur=%s) ===", prefix, dur)
	tenants := make([]ProvisionResp, 4)
	pools := make([]*pgxpool.Pool, 4)
	names := []string{"a_bulk", "b_interactive", "c_mixed", "d_intermittent"}
	for i := 0; i < 4; i++ {
		t, err := provisionTenant(ctx, fmt.Sprintf("%s_%s", prefix, names[i]))
		if err != nil {
			return err
		}
		tenants[i] = t
		p, err := connectGatewayPool(ctx, t, 20)
		if err != nil {
			return err
		}
		pools[i] = p
		defer p.Close()
		if _, err := p.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS mt_data (id bigserial PRIMARY KEY, k int, v text);
			INSERT INTO mt_data (k, v) SELECT g, repeat('m', 80) FROM generate_series(1, 500) g;
		`); err != nil {
			return err
		}
	}

	stopCtx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()

	type tStats struct {
		ops  atomic.Int64
		errs atomic.Int64
		mu   sync.Mutex
		lats []float64
	}
	stats := make([]tStats, 4)
	var wg sync.WaitGroup

	record := func(idx int, latUs float64, err error) {
		if err != nil {
			stats[idx].errs.Add(1)
			return
		}
		stats[idx].ops.Add(1)
		stats[idx].mu.Lock()
		if len(stats[idx].lats) < 200000 {
			stats[idx].lats = append(stats[idx].lats, latUs)
		}
		stats[idx].mu.Unlock()
	}

	// Tenant A: 8 workers doing heavy bulk INSERT + COPY
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for stopCtx.Err() == nil {
				t0 := time.Now()
				var err error
				if wid%2 == 0 {
					rows := make([][]any, 100)
					for i := 0; i < 100; i++ {
						rows[i] = []any{i, "bulk_copy_payload"}
					}
					_, err = pools[0].CopyFrom(stopCtx, pgx.Identifier{"mt_data"}, []string{"k", "v"}, pgx.CopyFromRows(rows))
				} else {
					_, err = pools[0].Exec(stopCtx, `INSERT INTO mt_data (k, v) VALUES ($1, repeat('b', 120))`, wid)
				}
				if stopCtx.Err() != nil {
					break
				}
				record(0, float64(time.Since(t0).Microseconds()), err)
			}
		}(w)
	}

	// Tenant B: 2 workers doing small interactive INSERT/UPDATE
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(wid) + 11))
			for stopCtx.Err() == nil {
				t0 := time.Now()
				k := rng.Intn(500) + 1
				_, err := pools[1].Exec(stopCtx, `UPDATE mt_data SET v = 'upd' WHERE id = $1`, k)
				if stopCtx.Err() != nil {
					break
				}
				record(1, float64(time.Since(t0).Microseconds()), err)
			}
		}(w)
	}

	// Tenant C: 4 workers doing mixed R/W
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(wid) + 33))
			for stopCtx.Err() == nil {
				t0 := time.Now()
				var err error
				if rng.Intn(2) == 0 {
					var v string
					err = pools[2].QueryRow(stopCtx, `SELECT v FROM mt_data WHERE id = $1`, rng.Intn(500)+1).Scan(&v)
				} else {
					_, err = pools[2].Exec(stopCtx, `INSERT INTO mt_data (k, v) VALUES ($1, 'mix')`, wid)
				}
				if stopCtx.Err() != nil {
					break
				}
				record(2, float64(time.Since(t0).Microseconds()), err)
			}
		}(w)
	}

	// Tenant D: 1 worker doing intermittent traffic (burst then sleep 500ms)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for stopCtx.Err() == nil {
			t0 := time.Now()
			_, err := pools[3].Exec(stopCtx, `INSERT INTO mt_data (k, v) VALUES (1, 'intermittent')`)
			if stopCtx.Err() != nil {
				break
			}
			record(3, float64(time.Since(t0).Microseconds()), err)
			select {
			case <-stopCtx.Done():
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()

	wg.Wait()
	for i := 0; i < 4; i++ {
		ops := stats[i].ops.Load()
		errs := stats[i].errs.Load()
		p50, p95, p99 := calcPercentiles(stats[i].lats)
		tps := float64(ops) / dur.Seconds()
		log.Printf("Tenant %s: ops=%d tps=%.1f errs=%d p50=%.2fms p95=%.2fms p99=%.2fms",
			names[i], ops, tps, errs, p50, p95, p99)
	}
	return nil
}

func init() {
	_ = http.MethodGet
}
