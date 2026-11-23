package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/jackc/pgx/v5"
)

type MemoryProfilePoint struct {
	MemProfile     string       `json:"mem_profile"`
	GovMode        string       `json:"gov_mode"`
	Clients        int          `json:"clients"`
	Summary        LatencySummary `json:"summary"`
	PeakCgroupMB   float64      `json:"peak_cgroup_mb"`
	CurCgroupMB    float64      `json:"cur_cgroup_mb"`
	AnonMB         float64      `json:"anon_mb"`
	FileMB         float64      `json:"file_mb"`
	PGRSSMB        float64      `json:"pg_rss_mb"`
	GatewayRSSMB   float64      `json:"gateway_rss_mb"`
	HighEvents     uint64       `json:"high_events"`
	MaxEvents      uint64       `json:"max_events"`
	OOMEvents      uint64       `json:"oom_events"`
	OOMKills       uint64       `json:"oom_kills"`
	MemPSISomeMax  float64      `json:"mem_psi_some_max"`
	MemPSIFullMax  float64      `json:"mem_psi_full_max"`
	CPUPSISomeMax  float64      `json:"cpu_psi_some_max"`
	IOPSISomeMax   float64      `json:"io_psi_max"`
	MinEffLimit    int          `json:"min_eff_limit"`
	MaxEffLimit    int          `json:"max_eff_limit"`
	EndEffLimit    int          `json:"end_eff_limit"`
	EndCtrlState   int          `json:"end_ctrl_state"`
	GovRejected    uint64       `json:"gov_rejected"`
	GovTimeouts    uint64       `json:"gov_timeouts"`
}

type AdaptiveTransition struct {
	Phase        string  `json:"phase"`
	EffLimit     int     `json:"eff_limit"`
	CtrlState    int     `json:"ctrl_state"`
	MemCurMB     float64 `json:"mem_cur_mb"`
	MemHighMB    float64 `json:"mem_high_mb"`
	MemPSISome   float64 `json:"mem_psi_some"`
	HighEvents   uint64  `json:"high_events"`
	Passed       bool    `json:"passed"`
	Notes        string  `json:"notes"`
}

func runMemory512Suite(ctx context.Context, gatewayBin, baseDir string, dur time.Duration) ([]MemoryProfilePoint, []AdaptiveTransition, error) {
	type memSpec struct {
		name    string
		highB   string
		maxB    string
		clients []int
	}
	specs := []memSpec{
		{name: "512MB", highB: strconv.Itoa(435 * 1024 * 1024), maxB: strconv.Itoa(512 * 1024 * 1024), clients: []int{4, 8, 16, 32, 64}},
		{name: "256MB", highB: strconv.Itoa(218 * 1024 * 1024), maxB: strconv.Itoa(256 * 1024 * 1024), clients: []int{4, 8, 16, 32}},
		{name: "1GB", highB: strconv.Itoa(870 * 1024 * 1024), maxB: strconv.Itoa(1024 * 1024 * 1024), clients: []int{8, 16, 32}},
	}

	var allPoints []MemoryProfilePoint
	var transitions []AdaptiveTransition

	for _, sp := range specs {
		limits := cell.CgroupLimits{
			MemoryHigh: sp.highB,
			MemoryMax:  sp.maxB,
			SwapMax:    "0",
			CPUMax:     "max 100000",
			PIDsMax:    "256",
		}
		env, err := NewBenchEnv(filepath.Join(baseDir, "mem_"+sp.name), gatewayBin, defaultCgroupRoot, limits)
		if err != nil {
			return allPoints, transitions, err
		}
		tid := "t_mem_" + strings.ToLower(sp.name)
		if _, err := env.ProvisionTenant(tid); err != nil {
			env.Cleanup()
			return allPoints, transitions, err
		}
		if c, err := pgx.Connect(ctx, env.DirectPGDSN(tid)); err == nil {
			_, _ = c.Exec(ctx, `CREATE TABLE mem_writes (id BIGSERIAL PRIMARY KEY, k INT, payload TEXT)`)
			c.Close(ctx)
		}

		modes := []GatewayProfile{
			ProfileNoGovernor(),
			ProfileStaticGovernor(),
			ProfileAdaptiveGovernor(),
		}
		for _, prof := range modes {
			prof.PoolMaxBackends = 64
			prof.WriteMaxGlobal = 16
			prof.WriteMinGlobal = 2
			prof.WriteMaxPerTenant = 16
			if err := env.StartGateway(prof, tid); err != nil {
				env.Cleanup()
				return allPoints, transitions, err
			}
			time.Sleep(1200 * time.Millisecond)

			for _, c := range sp.clients {
				var peakMem, peakPGRSS, peakGWRSS uint64
				var maxMemPSISome, maxMemPSIFull, maxCPUPSI, maxIOPSI float64
				minEff := 999
				maxEff := 0
				stopMon := make(chan struct{})
				var monMu sync.Mutex
				go func() {
					tk := time.NewTicker(250 * time.Millisecond)
					defer tk.Stop()
					for {
						select {
						case <-stopMon:
							return
						case <-tk.C:
							cg := env.SampleCgroup(tid)
							gv, _ := env.FetchGovernorSnapshot()
							monMu.Lock()
							if cg.MemoryCurrentBytes > peakMem {
								peakMem = cg.MemoryCurrentBytes
							}
							if cg.MemoryPeakBytes > peakMem {
								peakMem = cg.MemoryPeakBytes
							}
							if cg.PGRSSBytes > peakPGRSS {
								peakPGRSS = cg.PGRSSBytes
							}
							if cg.GatewayRSSBytes > peakGWRSS {
								peakGWRSS = cg.GatewayRSSBytes
							}
							if cg.MemoryPSISomeAvg10 > maxMemPSISome {
								maxMemPSISome = cg.MemoryPSISomeAvg10
							}
							if cg.MemoryPSIFullAvg10 > maxMemPSIFull {
								maxMemPSIFull = cg.MemoryPSIFullAvg10
							}
							if cg.CPUPSISomeAvg10 > maxCPUPSI {
								maxCPUPSI = cg.CPUPSISomeAvg10
							}
							if cg.IOPSIMaxAvg10 > maxIOPSI {
								maxIOPSI = cg.IOPSIMaxAvg10
							}
							if gv.EffectiveGlobalLimit > 0 {
								if gv.EffectiveGlobalLimit < minEff {
									minEff = gv.EffectiveGlobalLimit
								}
								if gv.EffectiveGlobalLimit > maxEff {
									maxEff = gv.EffectiveGlobalLimit
								}
							}
							monMu.Unlock()
						}
					}
				}()

				payload := strings.Repeat("M", 512)
				sum, _ := runConcurrentPGX(ctx, env.GatewayDSN(tid), c, dur, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
					_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO mem_writes (k, payload) VALUES (%d, '%s')", iter%10000, payload))
					return err
				})
				close(stopMon)

				cgEnd := env.SampleCgroup(tid)
				gvEnd, _ := env.FetchGovernorSnapshot()
				env.CleanWALSpool(tid)
				if minEff == 999 {
					minEff = gvEnd.EffectiveGlobalLimit
				}
				pt := MemoryProfilePoint{
					MemProfile:    sp.name,
					GovMode:       prof.Name,
					Clients:       c,
					Summary:       sum,
					PeakCgroupMB:  float64(max(peakMem, cgEnd.MemoryPeakBytes)) / (1024 * 1024),
					CurCgroupMB:   float64(cgEnd.MemoryCurrentBytes) / (1024 * 1024),
					AnonMB:        float64(cgEnd.MemoryAnonBytes) / (1024 * 1024),
					FileMB:        float64(cgEnd.MemoryFileBytes) / (1024 * 1024),
					PGRSSMB:       float64(max(peakPGRSS, cgEnd.PGRSSBytes)) / (1024 * 1024),
					GatewayRSSMB:  float64(max(peakGWRSS, cgEnd.GatewayRSSBytes)) / (1024 * 1024),
					HighEvents:    cgEnd.MemoryHighEvents,
					MaxEvents:     cgEnd.MemoryMaxEvents,
					OOMEvents:     cgEnd.MemoryOOMEvents,
					OOMKills:      cgEnd.MemoryOOMKill,
					MemPSISomeMax: maxMemPSISome,
					MemPSIFullMax: maxMemPSIFull,
					CPUPSISomeMax: maxCPUPSI,
					IOPSISomeMax:  maxIOPSI,
					MinEffLimit:   minEff,
					MaxEffLimit:   maxEff,
					EndEffLimit:   gvEnd.EffectiveGlobalLimit,
					EndCtrlState:  gvEnd.ControllerState,
					GovRejected:   gvEnd.RejectedTotal,
					GovTimeouts:   gvEnd.TimeoutTotal,
				}
				allPoints = append(allPoints, pt)
				fmt.Printf("[mem-%-5s] %-19s c=%-2d TPS=%7.1f p95=%6.2fms err=%d cgPeak=%6.1fMB pgRSS=%5.1fMB gwRSS=%4.1fMB hiEv=%d oom=%d psiM=%.2f eff=%d..%d\n",
					sp.name, prof.Name, c, sum.TPS, sum.P95Ms, sum.Errs,
					pt.PeakCgroupMB, pt.PGRSSMB, pt.GatewayRSSMB, pt.HighEvents, pt.OOMKills, pt.MemPSISomeMax, pt.MinEffLimit, pt.MaxEffLimit)
			}

			// Controlled AIMD & Telemetry Validation on 512MB profile with Adaptive Governor
			if sp.name == "512MB" && prof.WriteAdaptiveEnabled {
				transitions = runAdaptiveValidation(ctx, env, tid)
			}
			env.StopGateway()
			if c, err := pgx.Connect(ctx, env.DirectPGDSN(tid)); err == nil {
				_, _ = c.Exec(ctx, "TRUNCATE mem_writes")
				c.Close(ctx)
			}
		}
		env.Cleanup()
	}
	return allPoints, transitions, nil
}

func pushTelemetryDirect(adminPort int, payload map[string]any) error {
	b, _ := json.Marshal(payload)
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/internal/v1/telemetry", adminPort), bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Air-Internal-Token", defaultInternalToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func runAdaptiveValidation(ctx context.Context, env *BenchEnv, tid string) []AdaptiveTransition {
	var out []AdaptiveTransition
	// 1. Real cgroup pressure test: temporarily lower memory.high to trigger real kernel reclaim + high events
	cgDir := filepath.Join(env.CgroupRoot, "air_tenant_"+tid)
	cgBefore := env.SampleCgroup(tid)
	gvBefore, _ := env.FetchGovernorSnapshot()
	out = append(out, AdaptiveTransition{
		Phase:      "1_steady_healthy",
		EffLimit:   gvBefore.EffectiveGlobalLimit,
		CtrlState:  gvBefore.ControllerState,
		MemCurMB:   float64(cgBefore.MemoryCurrentBytes) / (1024 * 1024),
		MemHighMB:  float64(cgBefore.MemoryHighBytes) / (1024 * 1024),
		MemPSISome: cgBefore.MemoryPSISomeAvg10,
		HighEvents: cgBefore.MemoryHighEvents,
		Passed:     gvBefore.ControllerState == 1 || gvBefore.EffectiveGlobalLimit >= 8,
		Notes:      fmt.Sprintf("Real cgroup telemetry active; effLimit=%d", gvBefore.EffectiveGlobalLimit),
	})

	// Lower memory.high to 70% of current usage (or 64MB) to induce real cgroup v2 memory.high pressure without OOM
	tightHigh := uint64(64 * 1024 * 1024)
	if cgBefore.MemoryCurrentBytes > 96*1024*1024 {
		tightHigh = cgBefore.MemoryCurrentBytes * 75 / 100
	}
	_ = os.WriteFile(filepath.Join(cgDir, "memory.high"), []byte(strconv.FormatUint(tightHigh, 10)), 0o644)
	// Run 4s of real writes to push memory over tightHigh and trigger real high events
	_, _ = runConcurrentPGX(ctx, env.GatewayDSN(tid), 8, 4*time.Second, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO mem_writes (k, payload) VALUES (%d, '%s')", iter, strings.Repeat("P", 2048)))
		return err
	})
	cgPress := env.SampleCgroup(tid)
	gvPress, _ := env.FetchGovernorSnapshot()
	out = append(out, AdaptiveTransition{
		Phase:      "2_real_cgroup_memory_high_pressure",
		EffLimit:   gvPress.EffectiveGlobalLimit,
		CtrlState:  gvPress.ControllerState,
		MemCurMB:   float64(cgPress.MemoryCurrentBytes) / (1024 * 1024),
		MemHighMB:  float64(cgPress.MemoryHighBytes) / (1024 * 1024),
		MemPSISome: cgPress.MemoryPSISomeAvg10,
		HighEvents: cgPress.MemoryHighEvents,
		Passed:     gvPress.EffectiveGlobalLimit < gvBefore.EffectiveGlobalLimit || gvPress.ControllerState == 2 || gvPress.ControllerState == 3,
		Notes:      fmt.Sprintf("Lowered memory.high to %.1fMB; effLimit %d->%d, highEvents %d->%d", float64(tightHigh)/(1024*1024), gvBefore.EffectiveGlobalLimit, gvPress.EffectiveGlobalLimit, cgBefore.MemoryHighEvents, cgPress.MemoryHighEvents),
	})

	// Restore memory.high to 435MB and wait 4s for AIMD additive recovery
	_ = os.WriteFile(filepath.Join(cgDir, "memory.high"), []byte(strconv.Itoa(435*1024*1024)), 0o644)
	time.Sleep(4 * time.Second)
	cgRec := env.SampleCgroup(tid)
	gvRec, _ := env.FetchGovernorSnapshot()
	out = append(out, AdaptiveTransition{
		Phase:      "3_recovery_additive_increase",
		EffLimit:   gvRec.EffectiveGlobalLimit,
		CtrlState:  gvRec.ControllerState,
		MemCurMB:   float64(cgRec.MemoryCurrentBytes) / (1024 * 1024),
		MemHighMB:  float64(cgRec.MemoryHighBytes) / (1024 * 1024),
		MemPSISome: cgRec.MemoryPSISomeAvg10,
		HighEvents: cgRec.MemoryHighEvents,
		Passed:     gvRec.EffectiveGlobalLimit > gvPress.EffectiveGlobalLimit,
		Notes:      fmt.Sprintf("Restored memory.high=435MB; effLimit recovered %d->%d", gvPress.EffectiveGlobalLimit, gvRec.EffectiveGlobalLimit),
	})

	// 4. Stale telemetry fallback test: stop publisher and wait 16s (>15s TTL)
	env.StopTelemetryPublisher()
	// Push one old timestamped snapshot to verify stale detection immediately without waiting 16s
	_ = pushTelemetryDirect(env.AdminPort, map[string]any{
		"timestamp":            time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339),
		"memory_current_bytes": 100 * 1024 * 1024,
		"memory_high_bytes":    435 * 1024 * 1024,
		"memory_max_bytes":     512 * 1024 * 1024,
	})
	time.Sleep(1200 * time.Millisecond)
	gvStale, _ := env.FetchGovernorSnapshot()
	out = append(out, AdaptiveTransition{
		Phase:     "4_stale_telemetry_fallback",
		EffLimit:  gvStale.EffectiveGlobalLimit,
		CtrlState: gvStale.ControllerState,
		Passed:    gvStale.ControllerState == 4 && gvStale.EffectiveGlobalLimit <= gvStale.MaxGlobal/2,
		Notes:     fmt.Sprintf("Stale telemetry state=%d (4=stale_fallback), effLimit=%d (max/2=%d)", gvStale.ControllerState, gvStale.EffectiveGlobalLimit, gvStale.MaxGlobal/2),
	})

	for _, tr := range out {
		fmt.Printf("[adaptive] %-36s pass=%-5v effLim=%2d state=%d mem=%.1f/%.1fMB hiEv=%d | %s\n",
			tr.Phase, tr.Passed, tr.EffLimit, tr.CtrlState, tr.MemCurMB, tr.MemHighMB, tr.HighEvents, tr.Notes)
	}
	return out
}

type TenantFairnessRow struct {
	Mode     string         `json:"mode"`
	Tenant   string         `json:"tenant"`
	Role     string         `json:"role"`
	Clients  int            `json:"clients"`
	Summary  LatencySummary `json:"summary"`
}

func runMultiTenantFairnessSuite(ctx context.Context, gatewayBin, baseDir string, dur time.Duration) ([]TenantFairnessRow, error) {
	limits := cell.CgroupLimits{
		MemoryHigh: strconv.Itoa(435 * 1024 * 1024),
		MemoryMax:  strconv.Itoa(512 * 1024 * 1024),
		SwapMax:    "0",
		CPUMax:     "max 100000",
		PIDsMax:    "256",
	}
	env, err := NewBenchEnv(filepath.Join(baseDir, "multitenant"), gatewayBin, defaultCgroupRoot, limits)
	if err != nil {
		return nil, err
	}
	defer env.Cleanup()

	tenants := []string{"tenant_a", "tenant_b", "tenant_c", "tenant_d"}
	for _, tid := range tenants {
		if _, err := env.ProvisionTenant(tid); err != nil {
			return nil, err
		}
		if c, err := pgx.Connect(ctx, env.DirectPGDSN(tid)); err == nil {
			_, _ = c.Exec(ctx, `CREATE TABLE mt_data (id BIGSERIAL PRIMARY KEY, k INT, payload TEXT)`)
			_, _ = c.Exec(ctx, `INSERT INTO mt_data (k, payload) SELECT g, repeat('r', 64) FROM generate_series(1, 5000) g`)
			c.Close(ctx)
		}
	}

	modes := []GatewayProfile{
		ProfileNoGovernor(),
		ProfileStaticGovernor(),
		ProfileAdaptiveGovernor(),
	}
	var rows []TenantFairnessRow

	for _, prof := range modes {
		prof.WriteMaxGlobal = 12
		prof.WriteMinGlobal = 4
		prof.WriteMaxPerTenant = 4
		prof.PoolMaxBackends = 16
		if err := env.StartGateway(prof, ""); err != nil {
			return rows, err
		}
		time.Sleep(1100 * time.Millisecond)

		var wg sync.WaitGroup
		var mu sync.Mutex
		modeRows := make([]TenantFairnessRow, 4)

		// Tenant A: Heavy bulk writes (16 concurrent clients writing 2KB rows)
		wg.Add(1)
		go func() {
			defer wg.Done()
			bulkPayload := strings.Repeat("A", 2048)
			sum, _ := runConcurrentPGX(ctx, env.GatewayDSN("tenant_a"), 16, dur, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
				_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO mt_data (k, payload) VALUES (%d, '%s')", iter%10000, bulkPayload))
				return err
			})
			mu.Lock()
			modeRows[0] = TenantFairnessRow{Mode: prof.Name, Tenant: "tenant_a", Role: "heavy_bulk_16c", Clients: 16, Summary: sum}
			mu.Unlock()
		}()

		// Tenant B: Interactive small writes (2 clients)
		wg.Add(1)
		go func() {
			defer wg.Done()
			sum, _ := runConcurrentPGX(ctx, env.GatewayDSN("tenant_b"), 2, dur, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
				_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO mt_data (k, payload) VALUES (%d, 'small_b')", iter%1000))
				return err
			})
			mu.Lock()
			modeRows[1] = TenantFairnessRow{Mode: prof.Name, Tenant: "tenant_b", Role: "interactive_small_2c", Clients: 2, Summary: sum}
			mu.Unlock()
		}()

		// Tenant C: Mixed read/write (4 clients, 70% SELECT / 30% UPDATE)
		wg.Add(1)
		go func() {
			defer wg.Done()
			sum, _ := runConcurrentPGX(ctx, env.GatewayDSN("tenant_c"), 4, dur, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
				if iter%10 < 7 {
					var cnt int
					return conn.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM mt_data WHERE id = %d", 1+(iter%5000))).Scan(&cnt)
				}
				_, err := conn.Exec(ctx, fmt.Sprintf("UPDATE mt_data SET k = k + 1 WHERE id = %d", 1+(iter%5000)))
				return err
			})
			mu.Lock()
			modeRows[2] = TenantFairnessRow{Mode: prof.Name, Tenant: "tenant_c", Role: "mixed_rw_4c", Clients: 4, Summary: sum}
			mu.Unlock()
		}()

		// Tenant D: Intermittent burst writes (2 clients, 50ms sleep between writes)
		wg.Add(1)
		go func() {
			defer wg.Done()
			sum, _ := runConcurrentPGX(ctx, env.GatewayDSN("tenant_d"), 2, dur, 1, func(ctx context.Context, conn *pgx.Conn, wid int, iter int64) error {
				time.Sleep(50 * time.Millisecond)
				_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO mt_data (k, payload) VALUES (%d, 'intermittent_d')", iter%1000))
				return err
			})
			mu.Lock()
			modeRows[3] = TenantFairnessRow{Mode: prof.Name, Tenant: "tenant_d", Role: "intermittent_2c", Clients: 2, Summary: sum}
			mu.Unlock()
		}()

		wg.Wait()
		for _, r := range modeRows {
			rows = append(rows, r)
			fmt.Printf("[fairness] %-19s %-10s %-20s TPS=%7.1f p50=%5.2fms p95=%6.2fms p99=%6.2fms err=%d\n",
				r.Mode, r.Tenant, r.Role, r.Summary.TPS, r.Summary.P50Ms, r.Summary.P95Ms, r.Summary.P99Ms, r.Summary.Errs)
		}
		for _, tid := range tenants {
			env.CleanWALSpool(tid)
		}
		env.StopGateway()
	}
	return rows, nil
}

type CorrectnessResult struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail"`
}

// startLSNCountingProxy proxies TCP to real PG17 while counting exact occurrences of primary "SELECT pg_current_wal_lsn()" queries.
func startLSNCountingProxy(targetAddr string) (listenPort int, lsnCount *atomic.Int64, readLSNCount *atomic.Int64, closeFn func(), err error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, nil, nil, nil, err
	}
	lsnCount = &atomic.Int64{}
	readLSNCount = &atomic.Int64{}
	var wg sync.WaitGroup
	var mu sync.Mutex
	conns := make(map[net.Conn]struct{})

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			b, err := net.DialTimeout("tcp", targetAddr, 3*time.Second)
			if err != nil {
				c.Close()
				continue
			}
			mu.Lock()
			conns[c] = struct{}{}
			conns[b] = struct{}{}
			mu.Unlock()
			wg.Add(2)
			go func() {
				defer wg.Done()
				buf := make([]byte, 32*1024)
				for {
					n, rerr := c.Read(buf)
					if n > 0 {
						low := bytes.ToLower(buf[:n])
						if bytes.Contains(low, []byte("pg_last_wal_replay_lsn")) {
							readLSNCount.Add(1)
						} else if bytes.Contains(low, []byte("pg_current_wal_lsn")) {
							lsnCount.Add(1)
						}
						if _, werr := b.Write(buf[:n]); werr != nil {
							break
						}
					}
					if rerr != nil {
						break
					}
				}
				b.Close()
				c.Close()
			}()
			go func() {
				defer wg.Done()
				_, _ = io.Copy(c, b)
				c.Close()
				b.Close()
			}()
		}
	}()

	return ln.Addr().(*net.TCPAddr).Port, lsnCount, readLSNCount, func() {
		ln.Close()
		mu.Lock()
		for c := range conns {
			c.Close()
		}
		mu.Unlock()
		wg.Wait()
	}, nil
}

func runCorrectnessSuite(ctx context.Context, gatewayBin, baseDir string) ([]CorrectnessResult, error) {
	limits := cell.CgroupLimits{
		MemoryHigh: strconv.Itoa(435 * 1024 * 1024),
		MemoryMax:  strconv.Itoa(512 * 1024 * 1024),
		SwapMax:    "0",
		CPUMax:     "max 100000",
		PIDsMax:    "256",
	}
	env, err := NewBenchEnv(filepath.Join(baseDir, "correctness"), gatewayBin, defaultCgroupRoot, limits)
	if err != nil {
		return nil, err
	}
	defer env.Cleanup()

	tid := "t_corr"
	rt, err := env.ProvisionTenant(tid)
	if err != nil {
		return nil, err
	}

	prof := ProfileFullPhase20()
	if err := env.StartGateway(prof, tid); err != nil {
		return nil, err
	}
	dsn := env.GatewayDSN(tid)

	var results []CorrectnessResult
	record := func(name string, ok bool, detail string) {
		results = append(results, CorrectnessResult{Name: name, Passed: ok, Detail: detail})
		status := "PASS"
		if !ok {
			status = "FAIL"
		}
		fmt.Printf("[correctness] %-32s [%s] %s\n", name, status, detail)
	}

	conn1, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer conn1.Close(ctx)
	conn2, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer conn2.Close(ctx)

	// 1. Durability settings check (fsync, full_page_writes, synchronous_commit)
	var fsyncOn, fpwOn, syncCommit string
	_ = conn1.QueryRow(ctx, "SHOW fsync").Scan(&fsyncOn)
	_ = conn1.QueryRow(ctx, "SHOW full_page_writes").Scan(&fpwOn)
	_ = conn1.QueryRow(ctx, "SHOW synchronous_commit").Scan(&syncCommit)
	record("durability_gucs", fsyncOn == "on" && fpwOn == "on" && syncCommit == "on",
		fmt.Sprintf("fsync=%s full_page_writes=%s synchronous_commit=%s", fsyncOn, fpwOn, syncCommit))

	// 2. Schema with PK, UNIQUE, FK, SEQUENCE, TRIGGER
	_, err = conn1.Exec(ctx, `
		CREATE TABLE parent_t (id SERIAL PRIMARY KEY, code TEXT UNIQUE NOT NULL);
		CREATE TABLE child_t (id SERIAL PRIMARY KEY, parent_id INT NOT NULL REFERENCES parent_t(id), val INT NOT NULL DEFAULT 0);
		CREATE TABLE audit_t (id SERIAL PRIMARY KEY, msg TEXT);
		CREATE OR REPLACE FUNCTION child_audit_fn() RETURNS TRIGGER AS $$
		BEGIN
			INSERT INTO audit_t (msg) VALUES ('ins:' || NEW.parent_id::text || ':' || NEW.val::text);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER trg_child_audit AFTER INSERT ON child_t FOR EACH ROW EXECUTE FUNCTION child_audit_fn();
	`)
	record("ddl_constraints_triggers_seq", err == nil, fmt.Sprintf("err=%v", err))

	// 3. RETURNING + ON CONFLICT + UNIQUE + FK + TRIGGER
	var pid int
	err = conn1.QueryRow(ctx, "INSERT INTO parent_t (code) VALUES ('p1') RETURNING id").Scan(&pid)
	_, dupErr := conn1.Exec(ctx, "INSERT INTO parent_t (code) VALUES ('p1')")
	var upsertID int
	errUp := conn1.QueryRow(ctx, "INSERT INTO parent_t (code) VALUES ('p1') ON CONFLICT (code) DO UPDATE SET code = EXCLUDED.code RETURNING id").Scan(&upsertID)
	_, fkErr := conn1.Exec(ctx, "INSERT INTO child_t (parent_id, val) VALUES (999999, 1)")
	_, chErr := conn1.Exec(ctx, "INSERT INTO child_t (parent_id, val) VALUES ($1, 42)", pid)
	var auditCnt int
	_ = conn1.QueryRow(ctx, "SELECT count(*) FROM audit_t WHERE msg = $1", fmt.Sprintf("ins:%d:42", pid)).Scan(&auditCnt)
	record("constraints_fk_trigger_upsert", err == nil && dupErr != nil && errUp == nil && upsertID == pid && fkErr != nil && chErr == nil && auditCnt == 1,
		fmt.Sprintf("pid=%d dupErr=%v fkErr=%v auditCnt=%d", pid, dupErr != nil, fkErr != nil, auditCnt))

	// 4. MVCC isolation & transaction affinity (pg_backend_pid constant inside explicit tx)
	_, _ = conn1.Exec(ctx, "BEGIN")
	var txPID1, txPID2 int
	_ = conn1.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&txPID1)
	_, _ = conn1.Exec(ctx, "INSERT INTO parent_t (code) VALUES ('mvcc_uncommitted')")
	var visibleInConn2 int
	_ = conn2.QueryRow(ctx, "SELECT count(*) FROM parent_t WHERE code = 'mvcc_uncommitted'").Scan(&visibleInConn2)
	_ = conn1.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&txPID2)
	_, _ = conn1.Exec(ctx, "ROLLBACK")
	var visibleAfterRollback int
	_ = conn1.QueryRow(ctx, "SELECT count(*) FROM parent_t WHERE code = 'mvcc_uncommitted'").Scan(&visibleAfterRollback)
	record("mvcc_isolation_and_tx_affinity", txPID1 > 0 && txPID1 == txPID2 && visibleInConn2 == 0 && visibleAfterRollback == 0,
		fmt.Sprintf("txPID1=%d txPID2=%d unseenInConn2=%d afterRollback=%d", txPID1, txPID2, visibleInConn2, visibleAfterRollback))

	// 5. SAVEPOINT + ROLLBACK TO + COMMIT
	_, _ = conn1.Exec(ctx, "BEGIN")
	_, _ = conn1.Exec(ctx, "INSERT INTO parent_t (code) VALUES ('sp_keep')")
	_, _ = conn1.Exec(ctx, "SAVEPOINT s1")
	_, _ = conn1.Exec(ctx, "INSERT INTO parent_t (code) VALUES ('sp_drop')")
	_, _ = conn1.Exec(ctx, "ROLLBACK TO SAVEPOINT s1")
	_, _ = conn1.Exec(ctx, "COMMIT")
	var keepCnt, dropCnt int
	_ = conn1.QueryRow(ctx, "SELECT count(*) FROM parent_t WHERE code = 'sp_keep'").Scan(&keepCnt)
	_ = conn1.QueryRow(ctx, "SELECT count(*) FROM parent_t WHERE code = 'sp_drop'").Scan(&dropCnt)
	record("savepoint_semantics", keepCnt == 1 && dropCnt == 0, fmt.Sprintf("keep=%d drop=%d", keepCnt, dropCnt))

	// 6. Concurrent updates (lost-update / row-locking correctness)
	_, _ = conn1.Exec(ctx, "CREATE TABLE conc_ctr (id INT PRIMARY KEY, v INT NOT NULL); INSERT INTO conc_ctr VALUES (1, 0);")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := pgx.Connect(ctx, dsn)
			if err != nil {
				return
			}
			defer c.Close(ctx)
			for j := 0; j < 25; j++ {
				_, _ = c.Exec(ctx, "UPDATE conc_ctr SET v = v + 1 WHERE id = 1")
			}
		}()
	}
	wg.Wait()
	var finalCtr int
	_ = conn1.QueryRow(ctx, "SELECT v FROM conc_ctr WHERE id = 1").Scan(&finalCtr)
	record("concurrent_updates_atomic", finalCtr == 200, fmt.Sprintf("expected=200 got=%d", finalCtr))

	// 7. Streaming COPY FROM STDIN
	_, _ = conn1.Exec(ctx, "CREATE TABLE copy_verify (id INT, txt TEXT)")
	var copyBuf strings.Builder
	for i := 1; i <= 10000; i++ {
		fmt.Fprintf(&copyBuf, "%d\trow_%d\n", i, i)
	}
	tag, copyErr := conn1.PgConn().CopyFrom(ctx, strings.NewReader(copyBuf.String()), "COPY copy_verify (id, txt) FROM STDIN")
	var copyCnt int
	_ = conn1.QueryRow(ctx, "SELECT count(*) FROM copy_verify").Scan(&copyCnt)
	record("copy_from_stdin_streaming", copyErr == nil && tag.RowsAffected() == 10000 && copyCnt == 10000,
		fmt.Sprintf("rowsAffected=%d count=%d err=%v", tag.RowsAffected(), copyCnt, copyErr))

	// 8. Prepared statements (SQL PREPARE + Extended protocol)
	_, prepErr := conn1.Exec(ctx, "PREPARE ins_p (TEXT) AS INSERT INTO parent_t (code) VALUES ($1)")
	_, execErr := conn1.Exec(ctx, "EXECUTE ins_p('prep_sql_1')")
	_, deallocErr := conn1.Exec(ctx, "DEALLOCATE ins_p")
	var extVal string
	extErr := conn1.QueryRow(ctx, "SELECT code FROM parent_t WHERE code = $1", "prep_sql_1").Scan(&extVal)
	record("prepared_statements", prepErr == nil && execErr == nil && deallocErr == nil && extErr == nil && extVal == "prep_sql_1",
		fmt.Sprintf("val=%q errs=%v/%v/%v/%v", extVal, prepErr, execErr, deallocErr, extErr))

	// 9. WAL Durability + Crash Recovery (SIGKILL real PostgreSQL 17 postmaster!)
	_, _ = conn1.Exec(ctx, "INSERT INTO parent_t (code) VALUES ('crash_committed')")
	_, _ = conn2.Exec(ctx, "BEGIN")
	_, _ = conn2.Exec(ctx, "INSERT INTO parent_t (code) VALUES ('crash_uncommitted')")
	// Hard SIGKILL PostgreSQL postmaster!
	_ = syscall.Kill(rt.PID, syscall.SIGKILL)
	time.Sleep(500 * time.Millisecond)
	conn1.Close(ctx)
	conn2.Close(ctx)
	// Restart PG17 via Cell Agent (real WAL crash recovery!)
	rtNew, rErr := env.Agent.StartOrAdoptTenant(tid, 2)
	if rErr == nil {
		env.Tenants[tid] = rtNew
		_ = env.RegisterTenant(tid, rtNew, "", 0)
	}
	var recCommitted, recUncommitted int
	var recErr error
	if rErr == nil {
		cRec, err := pgx.Connect(ctx, env.GatewayDSN(tid))
		if err != nil {
			recErr = err
		} else {
			_ = cRec.QueryRow(ctx, "SELECT count(*) FROM parent_t WHERE code = 'crash_committed'").Scan(&recCommitted)
			_ = cRec.QueryRow(ctx, "SELECT count(*) FROM parent_t WHERE code = 'crash_uncommitted'").Scan(&recUncommitted)
			cRec.Close(ctx)
		}
	}
	record("wal_crash_recovery_sigkill", rErr == nil && recErr == nil && recCommitted == 1 && recUncommitted == 0,
		fmt.Sprintf("committed=%d uncommitted=%d restartErr=%v queryErr=%v", recCommitted, recUncommitted, rErr, recErr))

	// 10. Lazy LSN Validation with REAL PostgreSQL 17 + Counting Proxy (measures actual pg_current_wal_lsn() queries!)
	rtCur := env.Tenants[tid]
	proxyPort, lsnCount, readLSNCount, closeProxy, pErr := startLSNCountingProxy(fmt.Sprintf("127.0.0.1:%d", rtCur.Port))
	if pErr == nil {
		defer closeProxy()
		lazyProf := ProfileFullPhase20()
		lazyProf.WriteLazyLSNEnabled = true
		lazyProf.ReadEngineHost = "127.0.0.1"
		lazyProf.ReadEnginePort = proxyPort
		if err := env.StartGateway(lazyProf, tid); err == nil {
			_ = env.RegisterTenant(tid, &cell.TenantRuntime{Port: proxyPort, SockDir: ""}, "127.0.0.1", proxyPort)
			cfgLazy, _ := pgx.ParseConfig(env.GatewayDSN(tid))
			cfgLazy.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
			cLazy, err := pgx.ConnectConfig(ctx, cfgLazy)
			if err == nil {
				// 10a. Pure write workload (20 writes) -> MUST execute 0 pg_current_wal_lsn() queries
				lsnCount.Store(0)
				readLSNCount.Store(0)
				for i := 0; i < 20; i++ {
					_, _ = cLazy.Exec(ctx, fmt.Sprintf("INSERT INTO parent_t (code) VALUES ('lazy_w_%d')", i))
				}
				pureWriteLSNs := lsnCount.Load()

				// 10b. Write followed by Read -> MUST preserve RYOW AND issue 1 pg_current_wal_lsn() on the read
				var ryowCnt int
				_ = cLazy.QueryRow(ctx, "SELECT count(*) FROM parent_t WHERE code = 'lazy_w_19'").Scan(&ryowCnt)
				time.Sleep(50 * time.Millisecond)
				afterReadLSNs := lsnCount.Load()

				// 10c. Explicit tx followed by read
				lsnCount.Store(0)
				readLSNCount.Store(0)
				_, _ = cLazy.Exec(ctx, "BEGIN")
				_, _ = cLazy.Exec(ctx, "INSERT INTO parent_t (code) VALUES ('lazy_tx_1')")
				_, _ = cLazy.Exec(ctx, "COMMIT")
				time.Sleep(50 * time.Millisecond)
				txWriteLSNs := lsnCount.Load()
				txReadLSNs := readLSNCount.Load()
				var txRyow int
				_ = cLazy.QueryRow(ctx, "SELECT count(*) FROM parent_t WHERE code = 'lazy_tx_1'").Scan(&txRyow)
				time.Sleep(50 * time.Millisecond)
				txAfterReadLSNs := lsnCount.Load()
				cLazy.Close(ctx)

				record("lazy_lsn_ryow_and_count", pureWriteLSNs == 0 && ryowCnt == 1 && afterReadLSNs == 1 && txWriteLSNs == 0 && txReadLSNs == 0 && txRyow == 1 && txAfterReadLSNs == 1,
					fmt.Sprintf("pure20WritesLSNs=%d afterReadLSNs=%d ryow=%d txCommitLSNs=%d txReadCheckLSNs=%d txAfterReadLSNs=%d txRyow=%d",
						pureWriteLSNs, afterReadLSNs, ryowCnt, txWriteLSNs, txReadLSNs, txAfterReadLSNs, txRyow))
			}

			// Compare with LazyLSN=false (eager LSN)
			eagerProf := ProfileFullPhase20()
			eagerProf.WriteLazyLSNEnabled = false
			eagerProf.ReadEngineHost = "127.0.0.1"
			eagerProf.ReadEnginePort = proxyPort
			if err := env.StartGateway(eagerProf, tid); err == nil {
				_ = env.RegisterTenant(tid, &cell.TenantRuntime{Port: proxyPort, SockDir: ""}, "127.0.0.1", proxyPort)
				cfgEager, _ := pgx.ParseConfig(env.GatewayDSN(tid))
				cfgEager.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
				cEager, err := pgx.ConnectConfig(ctx, cfgEager)
				if err == nil {
					lsnCount.Store(0)
					for i := 0; i < 20; i++ {
						_, _ = cEager.Exec(ctx, fmt.Sprintf("INSERT INTO parent_t (code) VALUES ('eager_w_%d')", i))
					}
					time.Sleep(50 * time.Millisecond)
					eagerLSNs := lsnCount.Load()
					cEager.Close(ctx)
					record("eager_lsn_baseline_count", eagerLSNs == 20, fmt.Sprintf("eager20WritesLSNs=%d (expected 20)", eagerLSNs))
				}
			}
		}
	}

	return results, nil
}

type SustainedSample struct {
	ElapsedSec   int     `json:"elapsed_sec"`
	WindowTPS    float64 `json:"window_tps"`
	WindowP95Ms  float64 `json:"window_p95_ms"`
	WindowP99Ms  float64 `json:"window_p99_ms"`
	WindowErrs   int64   `json:"window_errs"`
	CgroupCurMB  float64 `json:"cgroup_cur_mb"`
	CgroupPeakMB float64 `json:"cgroup_peak_mb"`
	PGRSSMB      float64 `json:"pg_rss_mb"`
	GatewayRSSMB float64 `json:"gateway_rss_mb"`
	HighEvents   uint64  `json:"high_events"`
	OOMKills     uint64  `json:"oom_kills"`
	EffLimit     int     `json:"eff_limit"`
	QueueDepth   int64   `json:"queue_depth"`
	WALTotalMB   float64 `json:"wal_total_mb"`
	CkptTotal    int64   `json:"ckpt_total"`
}

func runSustainedSuite(ctx context.Context, gatewayBin, baseDir string, totalDur time.Duration, clients int) ([]SustainedSample, error) {
	limits := cell.CgroupLimits{
		MemoryHigh: strconv.Itoa(435 * 1024 * 1024),
		MemoryMax:  strconv.Itoa(512 * 1024 * 1024),
		SwapMax:    "0",
		CPUMax:     "max 100000",
		PIDsMax:    "256",
	}
	env, err := NewBenchEnv(filepath.Join(baseDir, "sustained"), gatewayBin, defaultCgroupRoot, limits)
	if err != nil {
		return nil, err
	}
	defer env.Cleanup()

	tid := "t_sustained"
	if _, err := env.ProvisionTenant(tid); err != nil {
		return nil, err
	}
	if c, err := pgx.Connect(ctx, env.DirectPGDSN(tid)); err == nil {
		_, _ = c.Exec(ctx, `CREATE TABLE sus_kv (id INT PRIMARY KEY, v BIGINT, payload TEXT)`)
		_, _ = c.Exec(ctx, `INSERT INTO sus_kv SELECT g, 0, repeat('s', 128) FROM generate_series(1, 20000) g`)
		c.Close(ctx)
	}

	prof := ProfileFullPhase20()
	prof.WriteMaxGlobal = 16
	prof.WriteMaxPerTenant = 8
	if err := env.StartGateway(prof, tid); err != nil {
		return nil, err
	}
	time.Sleep(1100 * time.Millisecond)

	runCtx, cancel := context.WithTimeout(ctx, totalDur)
	defer cancel()

	var latMu sync.Mutex
	var winLats []int64
	var winErrs atomic.Int64
	var wg sync.WaitGroup

	for w := 0; w < clients; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			cfg, _ := pgx.ParseConfig(env.GatewayDSN(tid))
			cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				winErrs.Add(1)
				return
			}
			defer conn.Close(context.Background())
			var iter int64
			for runCtx.Err() == nil {
				iter++
				id := 1 + int((int64(wid)*2500+iter)%20000)
				t0 := time.Now()
				_, err := conn.Exec(runCtx, fmt.Sprintf("UPDATE sus_kv SET v = v + 1 WHERE id = %d", id))
				dt := time.Since(t0).Microseconds()
				if err != nil {
					if runCtx.Err() != nil {
						break
					}
					winErrs.Add(1)
					continue
				}
				latMu.Lock()
				winLats = append(winLats, dt)
				latMu.Unlock()
			}
		}(w)
	}

	var samples []SustainedSample
	start := time.Now()
	pgStart, _ := env.SamplePGStats(ctx, tid)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-runCtx.Done():
			wg.Wait()
			return samples, nil
		case <-ticker.C:
			latMu.Lock()
			batch := winLats
			winLats = nil
			latMu.Unlock()
			errs := winErrs.Swap(0)
			sum := summarizeLatencies(batch, errs, 10*time.Second)
			cg := env.SampleCgroup(tid)
			gv, _ := env.FetchGovernorSnapshot()
			pgNow, _ := env.SamplePGStats(ctx, tid)
			env.CleanWALSpool(tid)

			s := SustainedSample{
				ElapsedSec:   int(time.Since(start).Seconds()),
				WindowTPS:    sum.TPS,
				WindowP95Ms:  sum.P95Ms,
				WindowP99Ms:  sum.P99Ms,
				WindowErrs:   errs,
				CgroupCurMB:  float64(cg.MemoryCurrentBytes) / (1024 * 1024),
				CgroupPeakMB: float64(cg.MemoryPeakBytes) / (1024 * 1024),
				PGRSSMB:      float64(cg.PGRSSBytes) / (1024 * 1024),
				GatewayRSSMB: float64(cg.GatewayRSSBytes) / (1024 * 1024),
				HighEvents:   cg.MemoryHighEvents,
				OOMKills:     cg.MemoryOOMKill,
				EffLimit:     gv.EffectiveGlobalLimit,
				QueueDepth:   gv.QueueDepth,
				WALTotalMB:   float64(pgNow.WALBytes-pgStart.WALBytes) / (1024 * 1024),
				CkptTotal:    (pgNow.CheckpointsTimed + pgNow.CheckpointsReq) - (pgStart.CheckpointsTimed + pgStart.CheckpointsReq),
			}
			samples = append(samples, s)
			fmt.Printf("[sustained t=%3ds] TPS=%7.1f p95=%6.2fms p99=%6.2fms err=%d cgCur=%5.1fMB cgPeak=%5.1fMB pgRSS=%5.1fMB gwRSS=%4.1fMB hiEv=%d oom=%d effLim=%d WAL=%.1fMB ckpt=%d\n",
				s.ElapsedSec, s.WindowTPS, s.WindowP95Ms, s.WindowP99Ms, s.WindowErrs,
				s.CgroupCurMB, s.CgroupPeakMB, s.PGRSSMB, s.GatewayRSSMB, s.HighEvents, s.OOMKills, s.EffLimit, s.WALTotalMB, s.CkptTotal)
		}
	}
}
