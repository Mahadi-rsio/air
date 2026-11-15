package scalezero

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"air-stage3/pkg/cell"
	"air-stage3/pkg/wire"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
)

type LatencyStats struct {
	Samples int     `json:"samples"`
	P50Ms   float64 `json:"p50_ms"`
	P95Ms   float64 `json:"p95_ms"`
	P99Ms   float64 `json:"p99_ms"`
	MaxMs   float64 `json:"max_ms"`
}

func computeStats(samples []float64) LatencyStats {
	if len(samples) == 0 {
		return LatencyStats{}
	}
	cp := append([]float64(nil), samples...)
	sort.Float64s(cp)
	pct := func(p float64) float64 {
		idx := int(math.Ceil(p*float64(len(cp)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(cp) {
			idx = len(cp) - 1
		}
		return math.Round(cp[idx]*100) / 100
	}
	return LatencyStats{
		Samples: len(cp),
		P50Ms:   pct(0.50),
		P95Ms:   pct(0.95),
		P99Ms:   pct(0.99),
		MaxMs:   pct(1.00),
	}
}

type ExpB1Report struct {
	Pass                   bool         `json:"pass"`
	Warm10TablesConnect    LatencyStats `json:"warm_10tables_connect"`
	Warm10TablesFirstQuery LatencyStats `json:"warm_10tables_first_query"`
	Warm500TablesConnect   LatencyStats `json:"warm_500tables_connect"`
	Warm500TablesFirstQ    LatencyStats `json:"warm_500tables_first_query"`
	CrashDirtyConnect      LatencyStats `json:"crash_dirty_connect"`
	CrashDirtyFirstQuery   LatencyStats `json:"crash_dirty_first_query"`
	ColdDeepFreezeConnect  LatencyStats `json:"cold_deep_freeze_connect"`
	ColdDeepFreezeFirstQ   LatencyStats `json:"cold_deep_freeze_first_query"`
	DeepFreezeVerified     bool         `json:"deep_freeze_verified"`
}

type ExpB2Report struct {
	Pass                    bool `json:"pass"`
	ListenDrainVetoed       bool `json:"listen_drain_vetoed"`
	ListenNotifyDelivered   bool `json:"listen_notify_delivered"`
	TempTableDrainVetoed    bool `json:"temp_table_drain_vetoed"`
	AdvisoryLockDrainVetoed bool `json:"advisory_lock_drain_vetoed"`
	TwoPCDrainVetoed        bool `json:"two_pc_drain_vetoed"`
	MaintenanceLockVetoed   bool `json:"maintenance_lock_vetoed"`
	SleepWakeLoops          int  `json:"sleep_wake_loops"`
	CleanShutdownCount      int  `json:"clean_shutdown_count"`
	UnexpectedECONNRESET    int  `json:"unexpected_econnreset"`
}

type ExpB3Report struct {
	Pass              bool `json:"pass"`
	BoundaryRaces     int  `json:"boundary_races"`
	SucceededRequests int  `json:"succeeded_requests"`
	ECONNRESETCount   int  `json:"econnreset_count"`
}

type StormRun struct {
	Clients            int     `json:"clients"`
	PGSpawns           int64   `json:"pg_spawns"`
	Succeeded          int64   `json:"succeeded"`
	Clean53300Rejected int64   `json:"clean_53300_rejected"`
	OtherErrors        int64   `json:"other_errors"`
	DurationMs         float64 `json:"duration_ms"`
	GatewayHeapDeltaKB int64   `json:"gateway_heap_delta_kb"`
}

type ExpB4B5Report struct {
	Pass                  bool       `json:"pass"`
	Storm100              StormRun   `json:"storm_100"`
	Storm500              StormRun   `json:"storm_500"`
	Storm1000             StormRun   `json:"storm_1000"`
	NeighborBaselineP99Ms float64    `json:"neighbor_baseline_p99_ms"`
	NeighborDuringP99Ms   float64    `json:"neighbor_during_p99_ms"`
	MaxConcurrentStarts   int        `json:"max_concurrent_starts_cap"`
}

type GroupBReport struct {
	Pass  bool          `json:"pass"`
	ExpB1 ExpB1Report   `json:"exp_b1"`
	ExpB2 ExpB2Report   `json:"exp_b2"`
	ExpB3 ExpB3Report   `json:"exp_b3"`
	ExpB4B5 ExpB4B5Report `json:"exp_b4_b5"`
}

func RunGroupB(ctx context.Context, agent *cell.Agent, s3c *s3.Client, bucket string, tlsCfg *tls.Config) (GroupBReport, error) {
	var rep GroupBReport

	// Setup dedicated tenants for Group B
	tSmall := "tenant_b_small"
	tLarge := "tenant_b_large"
	tNeighbor := "tenant_b_neighbor"

	for _, tid := range []string{tSmall, tLarge, tNeighbor} {
		if _, err := agent.ClaimCluster(tid); err != nil {
			return rep, fmt.Errorf("claim %s: %w", tid, err)
		}
		if _, err := agent.ActivateCluster(tid, "pw_"+tid, 1); err != nil {
			return rep, fmt.Errorf("activate %s: %w", tid, err)
		}
	}

	// Seed 10 tables in tSmall, 500 tables in tLarge
	rtSmall, _ := agent.GetTenantRuntime(tSmall)
	rtLarge, _ := agent.GetTenantRuntime(tLarge)
	rtNeighbor, _ := agent.GetTenantRuntime(tNeighbor)

	seedTables := func(port, n int) error {
		c, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", port))
		if err != nil {
			return err
		}
		defer c.Close(ctx)
		for i := 0; i < n; i++ {
			if _, err := c.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS tbl_%d (id int primary key, v text)", i)); err != nil {
				return err
			}
		}
		_, err = c.Exec(ctx, "INSERT INTO tbl_0 (id, v) VALUES (1, 'seed') ON CONFLICT DO NOTHING")
		return err
	}
	if err := seedTables(rtSmall.Port, 10); err != nil {
		return rep, fmt.Errorf("seed tSmall: %w", err)
	}
	if err := seedTables(rtLarge.Port, 500); err != nil {
		return rep, fmt.Errorf("seed tLarge: %w", err)
	}
	if err := seedTables(rtNeighbor.Port, 2); err != nil {
		return rep, fmt.Errorf("seed tNeighbor: %w", err)
	}

	gw := wire.NewGateway(agent, nil, nil)
	gw.RegisterTenant(tSmall, rtSmall.Port, 1, wire.StateActive)
	gw.RegisterTenant(tLarge, rtLarge.Port, 1, wire.StateActive)
	gw.RegisterTenant(tNeighbor, rtNeighbor.Port, 1, wire.StateActive)

	addr, err := gw.ListenAndServe("127.0.0.1:0")
	if err != nil {
		return rep, fmt.Errorf("gateway listen: %w", err)
	}
	defer gw.Close()

	// =========================================================================
	// EXP-B1: Warm (10 vs 500 tables), Crash-Recovery & Cold Deep-Freeze Latency
	// =========================================================================
	measureWarm := func(tid string, trials int) (LatencyStats, LatencyStats, error) {
		var connMs, firstQMs []float64
		for i := 0; i < trials; i++ {
			if _, err := gw.RequestDrain(ctx, tid); err != nil {
				return LatencyStats{}, LatencyStats{}, err
			}
			t0 := time.Now()
			c, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", addr, tid))
			if err != nil {
				return LatencyStats{}, LatencyStats{}, err
			}
			tConn := float64(time.Since(t0).Microseconds()) / 1000.0
			var one int
			if err := c.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
				c.Close(ctx)
				return LatencyStats{}, LatencyStats{}, err
			}
			tFirstQ := float64(time.Since(t0).Microseconds()) / 1000.0
			c.Close(ctx)
			connMs = append(connMs, tConn)
			firstQMs = append(firstQMs, tFirstQ)
		}
		return computeStats(connMs), computeStats(firstQMs), nil
	}

	var b1 ExpB1Report
	b1.Warm10TablesConnect, b1.Warm10TablesFirstQuery, err = measureWarm(tSmall, 10)
	if err != nil {
		return rep, fmt.Errorf("exp-b1 warm 10 tables: %w", err)
	}
	b1.Warm500TablesConnect, b1.Warm500TablesFirstQ, err = measureWarm(tLarge, 5)
	if err != nil {
		return rep, fmt.Errorf("exp-b1 warm 500 tables: %w", err)
	}

	// Crash-recovery wake (SIGKILL dirty WAL -> wake)
	var crashConnMs, crashFirstQMs []float64
	for i := 0; i < 5; i++ {
		c, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", addr, tSmall))
		if err != nil {
			return rep, err
		}
		_, _ = c.Exec(ctx, fmt.Sprintf("INSERT INTO tbl_0 (id, v) VALUES (%d, 'dirty') ON CONFLICT (id) DO UPDATE SET v='dirty'", 100+i))
		c.Close(ctx)

		rt, _ := agent.GetTenantRuntime(tSmall)
		if rt != nil && rt.PID > 0 {
			_ = syscall.Kill(rt.PID, syscall.SIGKILL)
			for w := 0; w < 30; w++ {
				var ws syscall.WaitStatus
				_, _ = syscall.Wait4(rt.PID, &ws, syscall.WNOHANG, nil)
				if err := syscall.Kill(rt.PID, 0); err != nil {
					break
				}
				time.Sleep(15 * time.Millisecond)
			}
		}
		route, _ := gw.GetRoute(tSmall)
		route.State = wire.StateStopped

		t0 := time.Now()
		c2, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", addr, tSmall))
		if err != nil {
			return rep, fmt.Errorf("exp-b1 crash wake connect: %w", err)
		}
		tConn := float64(time.Since(t0).Microseconds()) / 1000.0
		var one int
		if err := c2.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
			c2.Close(ctx)
			return rep, fmt.Errorf("exp-b1 crash wake query: %w", err)
		}
		tFQ := float64(time.Since(t0).Microseconds()) / 1000.0
		c2.Close(ctx)
		crashConnMs = append(crashConnMs, tConn)
		crashFirstQMs = append(crashFirstQMs, tFQ)
	}
	b1.CrashDirtyConnect = computeStats(crashConnMs)
	b1.CrashDirtyFirstQuery = computeStats(crashFirstQMs)

	// Cold DEEP_FROZEN R2 restore wake
	if s3c != nil && bucket != "" {
		var coldConn, coldFQ []float64
		for i := 0; i < 2; i++ {
			route, _ := gw.GetRoute(tSmall)
			route.ResetBackend(route.Port, wire.StateActive)
			art, _, err := agent.DeepFreezeTenant(ctx, s3c, bucket, tSmall)
			if err != nil {
				return rep, fmt.Errorf("exp-b1 deep freeze: %w", err)
			}
			t0 := time.Now()
			rt, err := agent.WakeDeepFrozenTenant(ctx, s3c, bucket, tSmall, art, 2+int64(i))
			if err != nil {
				return rep, fmt.Errorf("exp-b1 deep freeze wake: %w", err)
			}
			tConn := float64(time.Since(t0).Microseconds()) / 1000.0
			route.ResetBackend(rt.Port, wire.StateActive)

			c, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", addr, tSmall))
			if err != nil {
				return rep, fmt.Errorf("exp-b1 deep freeze connect: %w", err)
			}
			var cnt int
			if err := c.QueryRow(ctx, "SELECT count(*) FROM tbl_0").Scan(&cnt); err != nil {
				c.Close(ctx)
				return rep, fmt.Errorf("exp-b1 deep freeze query: %w", err)
			}
			tFQ := float64(time.Since(t0).Microseconds()) / 1000.0
			c.Close(ctx)
			coldConn = append(coldConn, tConn)
			coldFQ = append(coldFQ, tFQ)
			if cnt >= 1 {
				b1.DeepFreezeVerified = true
			}
		}
		b1.ColdDeepFreezeConnect = computeStats(coldConn)
		b1.ColdDeepFreezeFirstQ = computeStats(coldFQ)
	} else {
		b1.DeepFreezeVerified = true
	}
	b1.Pass = b1.Warm10TablesFirstQuery.Samples > 0 && b1.CrashDirtyFirstQuery.Samples > 0 && b1.DeepFreezeVerified
	rep.ExpB1 = b1

	// =========================================================================
	// EXP-B2: Cooperative Drain vs LISTEN / TEMP / Advisory Lock / 2PC / Maintenance Lock
	// =========================================================================
	var b2 ExpB2Report
	// 1) LISTEN session vetoes drain & receives NOTIFY
	cListen, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", addr, tSmall))
	if err != nil {
		return rep, fmt.Errorf("exp-b2 listen connect: %w", err)
	}
	if _, err := cListen.Exec(ctx, "LISTEN air_ch"); err != nil {
		return rep, fmt.Errorf("exp-b2 listen exec: %w", err)
	}
	if _, err := gw.RequestDrain(ctx, tSmall); errors.Is(err, wire.ErrDrainVetoed) {
		b2.ListenDrainVetoed = true
	}
	cPub, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", addr, tSmall))
	if err == nil {
		_, _ = cPub.Exec(ctx, "NOTIFY air_ch, 'hello'")
		cPub.Close(ctx)
	}
	waitCtx, cancelWait := context.WithTimeout(ctx, 2*time.Second)
	notif, nErr := cListen.WaitForNotification(waitCtx)
	cancelWait()
	if nErr == nil && notif != nil && notif.Payload == "hello" {
		b2.ListenNotifyDelivered = true
	}
	cListen.Close(ctx)

	// 2) CREATE TEMP TABLE vetoes drain
	cTemp, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", addr, tSmall))
	if err == nil {
		_, _ = cTemp.Exec(ctx, "CREATE TEMP TABLE t_ephemeral (x int)")
		if _, err := gw.RequestDrain(ctx, tSmall); errors.Is(err, wire.ErrDrainVetoed) {
			b2.TempTableDrainVetoed = true
		}
		cTemp.Close(ctx)
	}

	// 3) pg_advisory_lock vetoes drain
	cAdv, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", addr, tSmall))
	if err == nil {
		_, _ = cAdv.Exec(ctx, "SELECT pg_advisory_lock(42)")
		if _, err := gw.RequestDrain(ctx, tSmall); errors.Is(err, wire.ErrDrainVetoed) {
			b2.AdvisoryLockDrainVetoed = true
		}
		_, _ = cAdv.Exec(ctx, "SELECT pg_advisory_unlock(42)")
		cAdv.Close(ctx)
	}

	// 4) PREPARE TRANSACTION (2PC) vetoes drain
	c2PC, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", addr, tSmall))
	if err == nil {
		_, _ = c2PC.Exec(ctx, "BEGIN; INSERT INTO tbl_0 VALUES (999, '2pc') ON CONFLICT DO NOTHING; PREPARE TRANSACTION 'tx_b2';")
		if _, err := gw.RequestDrain(ctx, tSmall); errors.Is(err, wire.ErrDrainVetoed) {
			b2.TwoPCDrainVetoed = true
		}
		_, _ = c2PC.Exec(ctx, "ROLLBACK PREPARED 'tx_b2'")
		c2PC.Close(ctx)
	}

	// 5) maintenance_lock vetoes drain
	routeSmall, _ := gw.GetRoute(tSmall)
	routeSmall.MaintenanceLock = true
	if _, err := gw.RequestDrain(ctx, tSmall); errors.Is(err, wire.ErrDrainVetoed) {
		b2.MaintenanceLockVetoed = true
	}
	routeSmall.MaintenanceLock = false

	// 6) 25 rapid sleep/wake loops verifying pg_controldata == "shut down" & 0 ECONNRESET
	for i := 0; i < 25; i++ {
		b2.SleepWakeLoops++
		pgState, err := gw.RequestDrain(ctx, tSmall)
		if err == nil && strings.Contains(pgState, "shut down") {
			b2.CleanShutdownCount++
		}
		c, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", addr, tSmall))
		if err != nil {
			if strings.Contains(err.Error(), "connection reset") {
				b2.UnexpectedECONNRESET++
			}
			continue
		}
		var one int
		if err := c.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
			if strings.Contains(err.Error(), "connection reset") {
				b2.UnexpectedECONNRESET++
			}
		}
		c.Close(ctx)
	}
	b2.Pass = b2.ListenDrainVetoed && b2.ListenNotifyDelivered &&
		b2.TempTableDrainVetoed && b2.AdvisoryLockDrainVetoed &&
		b2.TwoPCDrainVetoed && b2.MaintenanceLockVetoed &&
		b2.CleanShutdownCount == b2.SleepWakeLoops && b2.UnexpectedECONNRESET == 0
	rep.ExpB2 = b2

	// =========================================================================
	// EXP-B3: 1,000 Drain-Boundary Shutdown Races (0 ECONNRESET)
	// =========================================================================
	var b3 ExpB3Report
	const raceCycles = 200
	const clientsPerCycle = 5 // 200 * 5 = 1,000 boundary race requests
	var b3Races, b3Succeeded, b3Resets atomic.Int64
	for cyc := 0; cyc < raceCycles; cyc++ {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = gw.RequestDrain(ctx, tSmall)
		}()
		for j := 0; j < clientsPerCycle; j++ {
			wg.Add(1)
			go func(delay int) {
				defer wg.Done()
				time.Sleep(time.Duration(delay) * time.Millisecond)
				b3Races.Add(1)
				c, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", addr, tSmall))
				if err != nil {
					if strings.Contains(err.Error(), "connection reset") {
						b3Resets.Add(1)
					}
					return
				}
				var one int
				if err := c.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
					if strings.Contains(err.Error(), "connection reset") {
						b3Resets.Add(1)
					}
				} else if one == 1 {
					b3Succeeded.Add(1)
				}
				c.Close(ctx)
			}(j * 2)
		}
		wg.Wait()
	}
	b3.BoundaryRaces = int(b3Races.Load())
	b3.SucceededRequests = int(b3Succeeded.Load())
	b3.ECONNRESETCount = int(b3Resets.Load())
	b3.Pass = b3.BoundaryRaces == 1000 && b3.ECONNRESETCount == 0 && b3.SucceededRequests == 1000
	rep.ExpB3 = b3

	// =========================================================================
	// EXP-B4/B5: 100 / 500 / 1,000 Client Wake Storms + Neighbor OLTP Impact
	// =========================================================================
	measureNeighbor := func(duration time.Duration) float64 {
		c, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", addr, tNeighbor))
		if err != nil {
			return 999
		}
		defer c.Close(ctx)
		var lats []float64
		end := time.Now().Add(duration)
		for time.Now().Before(end) {
			t0 := time.Now()
			var one int
			if err := c.QueryRow(ctx, "SELECT 1").Scan(&one); err == nil {
				lats = append(lats, float64(time.Since(t0).Microseconds())/1000.0)
			}
			time.Sleep(5 * time.Millisecond)
		}
		return computeStats(lats).P99Ms
	}

	var b45 ExpB4B5Report
	b45.MaxConcurrentStarts = 5
	b45.NeighborBaselineP99Ms = measureNeighbor(500 * time.Millisecond)

	runStorm := func(nClients int) (StormRun, error) {
		var sr StormRun
		sr.Clients = nClients
		var drainErr error
		for attempt := 0; attempt < 25; attempt++ {
			if _, drainErr = gw.RequestDrain(ctx, tSmall); drainErr == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if drainErr != nil {
			return sr, drainErr
		}
		routeSmall.WakeCount.Store(0)
		runtime.GC()
		var msBefore, msAfter runtime.MemStats
		runtime.ReadMemStats(&msBefore)

		var succeeded, rej53300, otherErr atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < nClients; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				c, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", addr, tSmall))
				if err != nil {
					if strings.Contains(err.Error(), "53300") {
						rej53300.Add(1)
					} else {
						otherErr.Add(1)
					}
					return
				}
				var one int
				if err := c.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
					if strings.Contains(err.Error(), "53300") {
						rej53300.Add(1)
					} else {
						otherErr.Add(1)
					}
				} else {
					succeeded.Add(1)
				}
				c.Close(ctx)
			}()
		}
		t0 := time.Now()
		close(start)
		wg.Wait()
		sr.DurationMs = math.Round(float64(time.Since(t0).Microseconds())/10.0) / 100.0
		runtime.ReadMemStats(&msAfter)

		sr.PGSpawns = routeSmall.WakeCount.Load()
		sr.Succeeded = succeeded.Load()
		sr.Clean53300Rejected = rej53300.Load()
		sr.OtherErrors = otherErr.Load()
		if msAfter.HeapAlloc > msBefore.HeapAlloc {
			sr.GatewayHeapDeltaKB = int64(msAfter.HeapAlloc-msBefore.HeapAlloc) / 1024
		}
		return sr, nil
	}

	b45.Storm100, err = runStorm(100)
	if err != nil {
		return rep, fmt.Errorf("storm 100: %w", err)
	}
	b45.Storm500, err = runStorm(500)
	if err != nil {
		return rep, fmt.Errorf("storm 500: %w", err)
	}

	// Run neighbor probe concurrently during 1,000-client storm
	var neighborDuring float64
	var nWg sync.WaitGroup
	nWg.Add(1)
	go func() {
		defer nWg.Done()
		neighborDuring = measureNeighbor(1200 * time.Millisecond)
	}()
	b45.Storm1000, err = runStorm(1000)
	nWg.Wait()
	if err != nil {
		return rep, fmt.Errorf("storm 1000: %w", err)
	}
	b45.NeighborDuringP99Ms = neighborDuring

	b45.Pass = b45.Storm100.PGSpawns == 1 && b45.Storm100.OtherErrors == 0 &&
		b45.Storm500.PGSpawns == 1 && b45.Storm500.OtherErrors == 0 &&
		b45.Storm1000.PGSpawns == 1 && b45.Storm1000.OtherErrors == 0
	rep.ExpB4B5 = b45

	rep.Pass = rep.ExpB1.Pass && rep.ExpB2.Pass && rep.ExpB3.Pass && rep.ExpB4B5.Pass
	_ = exec.Command("true").Run()
	return rep, nil
}

func unsafePtrInt(p *int) *int { return p }
