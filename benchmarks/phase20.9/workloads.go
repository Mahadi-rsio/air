package main

import (
	"context"
	"fmt"
	"math/rand"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

type WorkloadResult struct {
	Name        string          `json:"name"`
	Category    string          `json:"category"`
	Clients     int             `json:"clients"`
	DurationSec float64         `json:"duration_sec"`
	Summary     LatencySummary  `json:"summary"`
	RowsPerSec  float64         `json:"rows_per_sec"`
	WALDeltaB   int64           `json:"wal_delta_bytes"`
	CgroupPeakB uint64          `json:"cgroup_peak_bytes"`
	PGRSSBytes  uint64          `json:"pg_rss_bytes"`
	GWRSSBytes  uint64          `json:"gateway_rss_bytes"`
	Gov         GovSnapshot     `json:"governor"`
}

func runConcurrentPGX(ctx context.Context, dsn string, clients int, dur time.Duration, rowsPerOp int, fn func(ctx context.Context, conn *pgx.Conn, workerID int, iter int64) error) (LatencySummary, float64) {
	runCtx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()

	var mu sync.Mutex
	var allLats []int64
	var errs atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < clients; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			cfg, err := pgx.ParseConfig(dsn)
			if err != nil {
				errs.Add(1)
				return
			}
			cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				errs.Add(1)
				return
			}
			defer conn.Close(context.Background())

			var localLats []int64
			var iter int64
			for runCtx.Err() == nil {
				iter++
				t0 := time.Now()
				err := fn(runCtx, conn, wid, iter)
				dt := time.Since(t0).Microseconds()
				if err != nil {
					if runCtx.Err() != nil {
						break
					}
					errs.Add(1)
					continue
				}
				localLats = append(localLats, dt)
			}
			mu.Lock()
			allLats = append(allLats, localLats...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	sum := summarizeLatencies(allLats, errs.Load(), elapsed)
	return sum, sum.TPS * float64(rowsPerOp)
}

func setupWorkloadSchema(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	stmts := []string{
		`DROP TABLE IF EXISTS bench_ins_noindex, bench_ins_indexed, bench_upd, bench_upsert, bench_del, bench_copy, bench_tx CASCADE`,
		`CREATE TABLE bench_ins_noindex (id BIGSERIAL, k INT, payload TEXT)`,
		`CREATE TABLE bench_ins_indexed (id BIGSERIAL PRIMARY KEY, k INT, tag TEXT, payload TEXT)`,
		`CREATE INDEX idx_bench_ins_k ON bench_ins_indexed(k)`,
		`CREATE INDEX idx_bench_ins_tag ON bench_ins_indexed(tag)`,
		`CREATE TABLE bench_upd (id INT PRIMARY KEY, idx_col INT, nonidx_col INT, payload TEXT)`,
		`CREATE INDEX idx_bench_upd_col ON bench_upd(idx_col)`,
		`INSERT INTO bench_upd SELECT g, g % 1000, g, repeat('a', 64) FROM generate_series(1, 20000) g`,
		`CREATE TABLE bench_upsert (k INT PRIMARY KEY, v INT, payload TEXT)`,
		`INSERT INTO bench_upsert SELECT g, 0, 'init' FROM generate_series(1, 5000) g`,
		`CREATE TABLE bench_del (id BIGSERIAL PRIMARY KEY, grp INT, payload TEXT)`,
		`INSERT INTO bench_del (grp, payload) SELECT g % 100, repeat('d', 64) FROM generate_series(1, 50000) g`,
		`CREATE TABLE bench_copy (id INT, tenant TEXT, val INT, payload TEXT)`,
		`CREATE TABLE bench_tx (id INT PRIMARY KEY, bal BIGINT NOT NULL)`,
		`INSERT INTO bench_tx SELECT g, 10000 FROM generate_series(1, 2000) g`,
		`ANALYZE`,
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("schema stmt %q: %w", s[:min(40, len(s))], err)
		}
	}
	return nil
}

func runWorkloadSuite(ctx context.Context, env *BenchEnv, tenantID string, dur time.Duration) ([]WorkloadResult, error) {
	dsn := env.GatewayDSN(tenantID)
	if err := setupWorkloadSchema(ctx, dsn); err != nil {
		return nil, err
	}

	var results []WorkloadResult
	largePayload := strings.Repeat("X", 3800)

	runOne := func(cat, name string, clients int, rowsPerOp int, fn func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error) {
		pgBefore, _ := env.SamplePGStats(ctx, tenantID)
		sum, rps := runConcurrentPGX(ctx, dsn, clients, dur, rowsPerOp, fn)
		pgAfter, _ := env.SamplePGStats(ctx, tenantID)
		cg := env.SampleCgroup(tenantID)
		gov, _ := env.FetchGovernorSnapshot()
		env.CleanWALSpool(tenantID)
		res := WorkloadResult{
			Name:        name,
			Category:    cat,
			Clients:     clients,
			DurationSec: dur.Seconds(),
			Summary:     sum,
			RowsPerSec:  rps,
			WALDeltaB:   pgAfter.WALBytes - pgBefore.WALBytes,
			CgroupPeakB: cg.MemoryPeakBytes,
			PGRSSBytes:  cg.PGRSSBytes,
			GWRSSBytes:  cg.GatewayRSSBytes,
			Gov:         gov,
		}
		results = append(results, res)
		fmt.Printf("[workload] %-28s c=%-2d TPS=%7.1f rows/s=%9.1f p50=%5.2fms p95=%6.2fms p99=%6.2fms err=%d WAL=%.1fMB cgPeak=%.1fMB gwRSS=%.1fMB\n",
			name, clients, sum.TPS, rps, sum.P50Ms, sum.P95Ms, sum.P99Ms, sum.Errs,
			float64(res.WALDeltaB)/(1024*1024), float64(cg.MemoryPeakBytes)/(1024*1024), float64(cg.GatewayRSSBytes)/(1024*1024))
	}

	// A. Basic INSERT
	runOne("INSERT", "insert_single_noindex", 8, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO bench_ins_noindex (k, payload) VALUES (%d, 'row_%d_%d')", iter%10000, wid, iter))
		return err
	})
	runOne("INSERT", "insert_single_indexed", 8, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO bench_ins_indexed (k, tag, payload) VALUES (%d, 't%d', 'row_%d_%d')", iter%10000, wid, wid, iter))
		return err
	})
	// Multi-row INSERT (50 rows per statement)
	var multiVals []string
	for i := 0; i < 50; i++ {
		multiVals = append(multiVals, fmt.Sprintf("(%d, 'batch_payload_%d')", i, i))
	}
	multiSQL := "INSERT INTO bench_ins_noindex (k, payload) VALUES " + strings.Join(multiVals, ",")
	runOne("INSERT", "insert_multirow_50", 8, 50, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		_, err := conn.Exec(ctx, multiSQL)
		return err
	})

	// B. UPDATE
	runOne("UPDATE", "update_nonindexed_small", 8, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		id := 1 + int((int64(wid)*2500+iter)%20000)
		_, err := conn.Exec(ctx, fmt.Sprintf("UPDATE bench_upd SET nonidx_col = nonidx_col + 1 WHERE id = %d", id))
		return err
	})
	runOne("UPDATE", "update_indexed_col", 8, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		id := 1 + int((int64(wid)*2500+iter)%20000)
		_, err := conn.Exec(ctx, fmt.Sprintf("UPDATE bench_upd SET idx_col = (idx_col + 1) %% 1000 WHERE id = %d", id))
		return err
	})
	runOne("UPDATE", "update_large_row_4kb", 8, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		id := 1 + int((int64(wid)*2500+iter)%20000)
		_, err := conn.Exec(ctx, fmt.Sprintf("UPDATE bench_upd SET payload = '%s' WHERE id = %d", largePayload, id))
		return err
	})

	// C. UPSERT
	var noConfKey atomic.Int64
	noConfKey.Store(100000)
	runOne("UPSERT", "upsert_no_conflict", 8, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		k := noConfKey.Add(1)
		_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO bench_upsert (k, v, payload) VALUES (%d, 1, 'new') ON CONFLICT (k) DO UPDATE SET v = bench_upsert.v + 1", k))
		return err
	})
	runOne("UPSERT", "upsert_on_conflict_low", 8, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		k := 1 + int((int64(wid)*600+iter)%5000)
		_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO bench_upsert (k, v, payload) VALUES (%d, 1, 'upd') ON CONFLICT (k) DO UPDATE SET v = bench_upsert.v + 1", k))
		return err
	})
	runOne("UPSERT", "upsert_high_conflict_hot", 8, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		k := 1 + int(iter%4) // 4 hot keys across 8 clients
		_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO bench_upsert (k, v, payload) VALUES (%d, 1, 'hot') ON CONFLICT (k) DO UPDATE SET v = bench_upsert.v + 1", k))
		return err
	})

	// D. DELETE
	var delID atomic.Int64
	runOne("DELETE", "delete_small_single", 8, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		id := delID.Add(1)
		_, err := conn.Exec(ctx, fmt.Sprintf("DELETE FROM bench_del WHERE id = %d", id))
		return err
	})
	// Repopulate for large/concurrent delete
	if c, err := pgx.Connect(ctx, dsn); err == nil {
		_, _ = c.Exec(ctx, "TRUNCATE bench_del RESTART IDENTITY")
		_, _ = c.Exec(ctx, "INSERT INTO bench_del (grp, payload) SELECT g % 500, repeat('d', 64) FROM generate_series(1, 50000) g")
		c.Close(ctx)
	}
	var delGrp atomic.Int64
	runOne("DELETE", "delete_large_batch_100rows", 4, 100, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		g := delGrp.Add(1) % 500
		_, err := conn.Exec(ctx, fmt.Sprintf("DELETE FROM bench_del WHERE grp = %d", g))
		return err
	})

	// E. COPY FROM STDIN (streaming!)
	runCopyBatch := func(name string, clients int, rowsPerBatch int) {
		pgBefore, _ := env.SamplePGStats(ctx, tenantID)
		var gwRSSPeak atomic.Uint64
		stopMon := make(chan struct{})
		go func() {
			t := time.NewTicker(100 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-stopMon:
					return
				case <-t.C:
					cg := env.SampleCgroup(tenantID)
					for {
						old := gwRSSPeak.Load()
						if cg.GatewayRSSBytes <= old || gwRSSPeak.CompareAndSwap(old, cg.GatewayRSSBytes) {
							break
						}
					}
				}
			}
		}()

		sum, rps := runConcurrentPGX(ctx, dsn, clients, dur, rowsPerBatch, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
			var sb strings.Builder
			sb.Grow(rowsPerBatch * 48)
			for r := 0; r < rowsPerBatch; r++ {
				fmt.Fprintf(&sb, "%d\t%s\t%d\tcopy_payload_%d\n", r, tenantID, wid, r)
			}
			_, err := conn.PgConn().CopyFrom(ctx, strings.NewReader(sb.String()), "COPY bench_copy (id, tenant, val, payload) FROM STDIN")
			return err
		})
		close(stopMon)
		pgAfter, _ := env.SamplePGStats(ctx, tenantID)
		cg := env.SampleCgroup(tenantID)
		gov, _ := env.FetchGovernorSnapshot()
		env.CleanWALSpool(tenantID)
		peakGW := gwRSSPeak.Load()
		if cg.GatewayRSSBytes > peakGW {
			peakGW = cg.GatewayRSSBytes
		}
		res := WorkloadResult{
			Name:        name,
			Category:    "COPY",
			Clients:     clients,
			DurationSec: dur.Seconds(),
			Summary:     sum,
			RowsPerSec:  rps,
			WALDeltaB:   pgAfter.WALBytes - pgBefore.WALBytes,
			CgroupPeakB: cg.MemoryPeakBytes,
			PGRSSBytes:  cg.PGRSSBytes,
			GWRSSBytes:  peakGW,
			Gov:         gov,
		}
		results = append(results, res)
		fmt.Printf("[workload] %-28s c=%-2d TPS=%7.1f rows/s=%9.1f p50=%5.2fms p95=%6.2fms p99=%6.2fms err=%d WAL=%.1fMB cgPeak=%.1fMB gwRSSPeak=%.1fMB\n",
			name, clients, sum.TPS, rps, sum.P50Ms, sum.P95Ms, sum.P99Ms, sum.Errs,
			float64(res.WALDeltaB)/(1024*1024), float64(cg.MemoryPeakBytes)/(1024*1024), float64(peakGW)/(1024*1024))
	}
	runCopyBatch("copy_small_1k_rows", 4, 1000)
	runCopyBatch("copy_large_25k_rows", 4, 25000)

	// F. Transaction Workloads
	runOne("TX", "tx_explicit_begin_commit", 8, 2, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		a := 1 + int((int64(wid)*250+iter)%2000)
		b := 1 + ((a + 1) % 2000)
		if a > b {
			a, b = b, a
		}
		if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("UPDATE bench_tx SET bal = bal - 1 WHERE id = %d", a)); err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
			return err
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("UPDATE bench_tx SET bal = bal + 1 WHERE id = %d", b)); err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
			return err
		}
		_, err := conn.Exec(ctx, "COMMIT")
		return err
	})
	runOne("TX", "tx_explicit_rollback", 8, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		a := 1 + int((int64(wid)*250+iter)%2000)
		if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("UPDATE bench_tx SET bal = bal - 10 WHERE id = %d", a)); err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
			return err
		}
		_, err := conn.Exec(ctx, "ROLLBACK")
		return err
	})
	runOne("TX", "tx_savepoint_rollback_to", 8, 2, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		a := 1 + int((int64(wid)*250+iter)%2000)
		if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("UPDATE bench_tx SET bal = bal + 1 WHERE id = %d", a)); err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
			return err
		}
		if _, err := conn.Exec(ctx, "SAVEPOINT sp1"); err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
			return err
		}
		_, _ = conn.Exec(ctx, fmt.Sprintf("UPDATE bench_tx SET bal = bal + 999 WHERE id = %d", a))
		if _, err := conn.Exec(ctx, "ROLLBACK TO SAVEPOINT sp1"); err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
			return err
		}
		_, err := conn.Exec(ctx, "COMMIT")
		return err
	})

	return results, nil
}

type SweepPoint struct {
	Profile     string         `json:"profile"`
	Workload    string         `json:"workload"`
	Clients     int            `json:"clients"`
	Summary     LatencySummary `json:"summary"`
	PGBenchTPS  float64        `json:"pgbench_tps,omitempty"`
	PGBenchAvg  float64        `json:"pgbench_avg_ms,omitempty"`
	WALDeltaB   int64          `json:"wal_delta_bytes"`
	CgroupCurB  uint64         `json:"cgroup_current_bytes"`
	CgroupPeakB uint64         `json:"cgroup_peak_bytes"`
	PGRSSBytes  uint64         `json:"pg_rss_bytes"`
	GWRSSBytes  uint64         `json:"gateway_rss_bytes"`
	Gov         GovSnapshot    `json:"governor"`
}

var (
	rePGBenchTPS = regexp.MustCompile(`tps = ([0-9.]+) \(without initial connection time\)`)
	rePGBenchLat = regexp.MustCompile(`latency average = ([0-9.]+) ms`)
)

func runPGBench(ctx context.Context, host string, port int, db string, clients int, durSec int) (tps float64, avgMs float64, raw string, err error) {
	threads := clients
	if threads > 4 {
		threads = 4
	}
	cmd := exec.CommandContext(ctx, defaultPGBinDir+"/pgbench",
		"-h", host, "-p", strconv.Itoa(port), "-U", "app_user",
		"-c", strconv.Itoa(clients), "-j", strconv.Itoa(threads),
		"-T", strconv.Itoa(durSec), "-M", "simple", "-n", db,
	)
	cmd.Env = append(cmd.Environ(), "PGPASSWORD="+defaultAppPassword)
	out, err := cmd.CombinedOutput()
	raw = string(out)
	if m := rePGBenchTPS.FindStringSubmatch(raw); len(m) == 2 {
		tps, _ = strconv.ParseFloat(m[1], 64)
	}
	if m := rePGBenchLat.FindStringSubmatch(raw); len(m) == 2 {
		avgMs, _ = strconv.ParseFloat(m[1], 64)
	}
	return tps, avgMs, raw, err
}

func initPGBench(ctx context.Context, host string, port int, db string, scale int) error {
	cmd := exec.CommandContext(ctx, defaultPGBinDir+"/pgbench",
		"-h", host, "-p", strconv.Itoa(port), "-U", "app_user",
		"-i", "-s", strconv.Itoa(scale), "-q", db,
	)
	cmd.Env = append(cmd.Environ(), "PGPASSWORD="+defaultAppPassword)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pgbench -i failed: %w (%s)", err, string(out))
	}
	return nil
}

func runConcurrencySweep(ctx context.Context, env *BenchEnv, tenantID string, dur time.Duration) ([]SweepPoint, error) {
	profiles := []GatewayProfile{
		ProfileNoGovernor(),
		ProfileStaticGovernor(),
		ProfileAdaptiveGovernor(),
		ProfileGovBufLazyLSN(),
		ProfileFullPhase20(),
	}
	clientLevels := []int{1, 2, 4, 8, 16, 32}

	// Initialize pgbench scale 10 + autocommit table directly on PG
	rt := env.Tenants[tenantID]
	if err := initPGBench(ctx, "127.0.0.1", rt.Port, tenantID, 10); err != nil {
		return nil, err
	}
	if c, err := pgx.Connect(ctx, env.DirectPGDSN(tenantID)); err == nil {
		_, _ = c.Exec(ctx, `CREATE TABLE IF NOT EXISTS sweep_writes (id BIGSERIAL PRIMARY KEY, worker INT, val INT, payload TEXT)`)
		c.Close(ctx)
	}

	var points []SweepPoint
	for _, prof := range profiles {
		if err := env.StartGateway(prof, tenantID); err != nil {
			return points, fmt.Errorf("StartGateway(%s): %w", prof.Name, err)
		}
		// Give adaptive controller 1s to receive fresh telemetry
		time.Sleep(1100 * time.Millisecond)

		for _, c := range clientLevels {
			// 1. Autocommit INSERT workload (directly exercises WriteGovernor admission per op)
			pgB, _ := env.SamplePGStats(ctx, tenantID)
			sum, _ := runConcurrentPGX(ctx, env.GatewayDSN(tenantID), c, dur, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
				_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO sweep_writes (worker, val, payload) VALUES (%d, %d, 'p209_sweep_row_payload_%d')", wid, rand.Intn(100000), iter))
				return err
			})
			pgA, _ := env.SamplePGStats(ctx, tenantID)
			cg := env.SampleCgroup(tenantID)
			gov, _ := env.FetchGovernorSnapshot()
			env.CleanWALSpool(tenantID)
			pt := SweepPoint{
				Profile:     prof.Name,
				Workload:    "autocommit_insert",
				Clients:     c,
				Summary:     sum,
				WALDeltaB:   pgA.WALBytes - pgB.WALBytes,
				CgroupCurB:  cg.MemoryCurrentBytes,
				CgroupPeakB: cg.MemoryPeakBytes,
				PGRSSBytes:  cg.PGRSSBytes,
				GWRSSBytes:  cg.GatewayRSSBytes,
				Gov:         gov,
			}
			points = append(points, pt)
			fmt.Printf("[sweep] %-18s autocommit_insert c=%-2d TPS=%7.1f p50=%5.2fms p95=%6.2fms p99=%6.2fms err=%d effLim=%d rej=%d cgCur=%.1fMB\n",
				prof.Name, c, sum.TPS, sum.P50Ms, sum.P95Ms, sum.P99Ms, sum.Errs, gov.EffectiveGlobalLimit, gov.RejectedTotal, float64(cg.MemoryCurrentBytes)/(1024*1024))

			// 2. Real pgbench TPC-B (explicit BEGIN/UPDATE/SELECT/UPDATE/UPDATE/INSERT/END)
			pgB2, _ := env.SamplePGStats(ctx, tenantID)
			tps, avgMs, _, _ := runPGBench(ctx, "127.0.0.1", env.GatewayPort, tenantID, c, max(5, int(dur.Seconds())))
			pgA2, _ := env.SamplePGStats(ctx, tenantID)
			cg2 := env.SampleCgroup(tenantID)
			gov2, _ := env.FetchGovernorSnapshot()
			env.CleanWALSpool(tenantID)
			pt2 := SweepPoint{
				Profile:     prof.Name,
				Workload:    "pgbench_tpcb",
				Clients:     c,
				PGBenchTPS:  tps,
				PGBenchAvg:  avgMs,
				WALDeltaB:   pgA2.WALBytes - pgB2.WALBytes,
				CgroupCurB:  cg2.MemoryCurrentBytes,
				CgroupPeakB: cg2.MemoryPeakBytes,
				PGRSSBytes:  cg2.PGRSSBytes,
				GWRSSBytes:  cg2.GatewayRSSBytes,
				Gov:         gov2,
			}
			points = append(points, pt2)
			fmt.Printf("[sweep] %-18s pgbench_tpcb      c=%-2d TPS=%7.1f avg=%6.2fms effLim=%d cgCur=%.1fMB cgPeak=%.1fMB\n",
				prof.Name, c, tps, avgMs, gov2.EffectiveGlobalLimit, float64(cg2.MemoryCurrentBytes)/(1024*1024), float64(cg2.MemoryPeakBytes)/(1024*1024))
		}
		// Truncate sweep_writes between profiles to keep disk usage flat
		if c, err := pgx.Connect(ctx, env.DirectPGDSN(tenantID)); err == nil {
			_, _ = c.Exec(ctx, "TRUNCATE sweep_writes")
			c.Close(ctx)
		}
		env.StopGateway()
	}
	return points, nil
}
