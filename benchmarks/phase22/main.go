package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	controlPlaneURL = "http://127.0.0.1:18080"
	psHTTPURL       = "http://127.0.0.1:18083"
	gatewayPGHost   = "127.0.0.1"
	gatewayPGPort   = 15432
	bootstrapAPIKey = "air_p21_bootstrap_key"
	internalToken   = "air-p21-internal-token"
	psContainerName = "air-p21-pageserver"
)

type ProvisionResp struct {
	TenantID   string `json:"tenant_id"`
	Generation string `json:"generation"`
	Database   string `json:"database"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	DirectPort int    `json:"direct_port"`
	State      string `json:"state"`
}

type PSMemoryStats struct {
	MemoryLimitBytes        int64            `json:"memory_limit_bytes"`
	GoMemoryLimitBytes      int64            `json:"go_memory_limit_bytes"`
	GlobalInMemBytes        int64            `json:"global_inmem_bytes"`
	TotalDirtyPages         int64            `json:"total_dirty_pages"`
	TenantInMemBytes        map[string]int64 `json:"tenant_inmem_bytes"`
	DecodedChunkCacheBytes  int64            `json:"decoded_chunk_cache_bytes"`
	IndexCacheBytes         int64            `json:"index_cache_bytes"`
	HeapAllocBytes          uint64           `json:"heap_alloc_bytes"`
	HeapInuseBytes          uint64           `json:"heap_inuse_bytes"`
	HeapIdleBytes           uint64           `json:"heap_idle_bytes"`
	HeapReleasedBytes       uint64           `json:"heap_released_bytes"`
	TotalAllocBytes         uint64           `json:"total_alloc_bytes"`
	NumGC                   uint32           `json:"num_gc"`
	GCPauseTotalNs          uint64           `json:"gc_pause_total_ns"`
	FlushesCompleted        int64            `json:"flushes_completed"`
	FlushedBytesTotal       int64            `json:"flushed_bytes_total"`
	BackpressureWaits       int64            `json:"backpressure_waits"`
	BackpressureRejects     int64            `json:"backpressure_rejects"`
	PressureEvictions       int64            `json:"pressure_evictions"`
}

type MetricSample struct {
	ElapsedSec              float64 `json:"elapsed_sec"`
	Running                 bool    `json:"running"`
	OOMKilled               bool    `json:"oom_killed"`
	CgroupCurrentMB         float64 `json:"cgroup_current_mb"`
	CgroupPeakMB            float64 `json:"cgroup_peak_mb"`
	CgroupAnonMB            float64 `json:"cgroup_anon_mb"`
	CgroupFileMB            float64 `json:"cgroup_file_mb"`
	OOMEvents               int64   `json:"oom_events"`
	OOMKillEvents           int64   `json:"oom_kill_events"`
	VmRSSMB                 float64 `json:"vm_rss_mb"`
	VmHWMMB                 float64 `json:"vm_hwm_mb"`
	RssAnonMB               float64 `json:"rss_anon_mb"`
	RssFileMB               float64 `json:"rss_file_mb"`
	HeapAllocMB             float64 `json:"heap_alloc_mb"`
	HeapInuseMB             float64 `json:"heap_inuse_mb"`
	HeapIdleMB              float64 `json:"heap_idle_mb"`
	HeapReleasedMB          float64 `json:"heap_released_mb"`
	AllocRateMBPerSec       float64 `json:"alloc_rate_mb_s"`
	GCFreqPerSec            float64 `json:"gc_freq_s"`
	GCPauseMsDelta          float64 `json:"gc_pause_ms_delta"`
	GlobalInMemMB           float64 `json:"global_inmem_mb"`
	MaxTenantInMemMB        float64 `json:"max_tenant_inmem_mb"`
	DirtyPageCount          int64   `json:"dirty_page_count"`
	FlushesCompleted        int64   `json:"flushes_completed"`
	FlushThroughputMBPerSec float64 `json:"flush_throughput_mb_s"`
	BackpressureWaits       int64   `json:"backpressure_waits"`
	BackpressureRejects     int64   `json:"backpressure_rejects"`
	PressureEvictions       int64   `json:"pressure_evictions"`
}

type BenchSummary struct {
	Mode               string  `json:"mode"`
	Tenants            int     `json:"tenants"`
	PSMemoryMB         int     `json:"ps_memory_mb"`
	DurationSec        float64 `json:"duration_sec"`
	ConcBootstrap      bool    `json:"conc_bootstrap"`
	BootstrapSec       float64 `json:"bootstrap_sec"`
	PeakCgroupMB       float64 `json:"peak_cgroup_mb"`
	PeakVmRSSMB        float64 `json:"peak_vm_rss_mb"`
	PeakVmHWMMB        float64 `json:"peak_vm_hwm_mb"`
	PeakHeapAllocMB    float64 `json:"peak_heap_alloc_mb"`
	PeakGlobalInMemMB  float64 `json:"peak_global_inmem_mb"`
	PeakTenantInMemMB  float64 `json:"peak_tenant_inmem_mb"`
	PeakDirtyPages     int64   `json:"peak_dirty_pages"`
	AvgAllocRateMBs    float64 `json:"avg_alloc_rate_mb_s"`
	TotalNumGC         uint32  `json:"total_num_gc"`
	TotalGCPauseMs     float64 `json:"total_gc_pause_ms"`
	TotalFlushes       int64   `json:"total_flushes"`
	AvgFlushMBs        float64 `json:"avg_flush_mb_s"`
	OOMEvents          int64   `json:"oom_events"`
	OOMKillEvents      int64   `json:"oom_kill_events"`
	TxCommitted        int64   `json:"tx_committed"`
	QueryErrors        int64   `json:"query_errors"`
	DataIntegrityOK    bool    `json:"data_integrity_ok"`
	FinalInMemMB       float64 `json:"final_inmem_mb"`
	FinalVmRSSMB       float64 `json:"final_vm_rss_mb"`
}

func provisionTenant(ctx context.Context, tenantID string) (ProvisionResp, error) {
	body := fmt.Sprintf(`{"tenant_id":%q}`, tenantID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, controlPlaneURL+"/v1/databases", strings.NewReader(body))
	if err != nil {
		return ProvisionResp{}, err
	}
	req.Header.Set("Authorization", "Bearer "+bootstrapAPIKey)
	req.Header.Set("Idempotency-Key", "idem-"+tenantID+"-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ProvisionResp{}, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return ProvisionResp{}, fmt.Errorf("provision %s status=%d body=%s", tenantID, resp.StatusCode, string(raw))
	}
	var out ProvisionResp
	if err := json.Unmarshal(raw, &out); err != nil {
		return ProvisionResp{}, err
	}
	return out, nil
}

func suspendTenant(ctx context.Context, tenantID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, controlPlaneURL+"/internal/v1/databases/"+tenantID+"/suspend", nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Air-Internal-Token", internalToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("suspend %s status=%d body=%s", tenantID, resp.StatusCode, string(raw))
	}
	return nil
}

func wakeTenant(ctx context.Context, tenantID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, controlPlaneURL+"/internal/v1/tenants/"+tenantID+"/wake", nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Air-Internal-Token", internalToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("wake %s status=%d body=%s", tenantID, resp.StatusCode, string(raw))
	}
	return nil
}

func connectPool(ctx context.Context, t ProvisionResp, maxConns int32) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", t.Username, t.Password, gatewayPGHost, gatewayPGPort, t.Database)
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = 1
	return pgxpool.NewWithConfig(ctx, cfg)
}

func fetchPSStats() (PSMemoryStats, bool) {
	var st PSMemoryStats
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(psHTTPURL + "/internal/v1/memory-stats")
	if err != nil {
		return st, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return st, false
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return st, false
	}
	return st, true
}

func sampleOnce(start time.Time, prevStats *PSMemoryStats, prevTime *time.Time) MetricSample {
	now := time.Now()
	s := MetricSample{ElapsedSec: now.Sub(start).Seconds()}

	inspOut, err := exec.Command("docker", "inspect", "--format", "{{.State.Running}} {{.State.OOMKilled}}", psContainerName).Output()
	if err == nil {
		parts := strings.Fields(strings.TrimSpace(string(inspOut)))
		if len(parts) == 2 {
			s.Running = parts[0] == "true"
			s.OOMKilled = parts[1] == "true"
		}
	}
	if s.Running {
		script := `
cat /sys/fs/cgroup/memory.current 2>/dev/null || echo 0
cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo 0
awk '/^anon /{a=$2} /^file /{f=$2} END{printf "%d %d\n", a+0, f+0}' /sys/fs/cgroup/memory.stat 2>/dev/null || echo "0 0"
awk '/^oom /{o=$2} /^oom_kill /{k=$2} END{printf "%d %d\n", o+0, k+0}' /sys/fs/cgroup/memory.events 2>/dev/null || echo "0 0"
for p in /proc/[0-9]*/comm; do
  [ -r "$p" ] || continue
  dir=${p%/comm}
  comm=$(cat "$p" 2>/dev/null) || continue
  case "$comm" in
    *air-pageserver*)
      awk '/^VmRSS:/{rss=$2} /^VmHWM:/{hwm=$2} /^RssAnon:/{ra=$2} /^RssFile:/{rf=$2} END{printf "%d %d %d %d\n", rss+0, hwm+0, ra+0, rf+0}' "$dir/status"
      break
      ;;
  esac
done
`
		out, err := exec.Command("docker", "exec", psContainerName, "sh", "-c", script).Output()
		if err == nil {
			lines := strings.Split(strings.TrimSpace(string(out)), "\n")
			if len(lines) >= 1 {
				v, _ := strconv.ParseFloat(strings.TrimSpace(lines[0]), 64)
				s.CgroupCurrentMB = v / (1024 * 1024)
			}
			if len(lines) >= 2 {
				v, _ := strconv.ParseFloat(strings.TrimSpace(lines[1]), 64)
				s.CgroupPeakMB = v / (1024 * 1024)
			}
			if len(lines) >= 3 {
				p := strings.Fields(lines[2])
				if len(p) == 2 {
					a, _ := strconv.ParseFloat(p[0], 64)
					f, _ := strconv.ParseFloat(p[1], 64)
					s.CgroupAnonMB = a / (1024 * 1024)
					s.CgroupFileMB = f / (1024 * 1024)
				}
			}
			if len(lines) >= 4 {
				p := strings.Fields(lines[3])
				if len(p) == 2 {
					s.OOMEvents, _ = strconv.ParseInt(p[0], 10, 64)
					s.OOMKillEvents, _ = strconv.ParseInt(p[1], 10, 64)
				}
			}
			if len(lines) >= 5 {
				p := strings.Fields(lines[4])
				if len(p) == 4 {
					rss, _ := strconv.ParseFloat(p[0], 64)
					hwm, _ := strconv.ParseFloat(p[1], 64)
					ra, _ := strconv.ParseFloat(p[2], 64)
					rf, _ := strconv.ParseFloat(p[3], 64)
					s.VmRSSMB = rss / 1024.0
					s.VmHWMMB = hwm / 1024.0
					s.RssAnonMB = ra / 1024.0
					s.RssFileMB = rf / 1024.0
				}
			}
		}
	}

	if st, ok := fetchPSStats(); ok {
		s.HeapAllocMB = float64(st.HeapAllocBytes) / (1024 * 1024)
		s.HeapInuseMB = float64(st.HeapInuseBytes) / (1024 * 1024)
		s.HeapIdleMB = float64(st.HeapIdleBytes) / (1024 * 1024)
		s.HeapReleasedMB = float64(st.HeapReleasedBytes) / (1024 * 1024)
		s.GlobalInMemMB = float64(st.GlobalInMemBytes) / (1024 * 1024)
		s.DirtyPageCount = st.TotalDirtyPages
		s.FlushesCompleted = st.FlushesCompleted
		s.BackpressureWaits = st.BackpressureWaits
		s.BackpressureRejects = st.BackpressureRejects
		s.PressureEvictions = st.PressureEvictions
		var maxT int64
		for _, v := range st.TenantInMemBytes {
			if v > maxT {
				maxT = v
			}
		}
		s.MaxTenantInMemMB = float64(maxT) / (1024 * 1024)

		if prevStats != nil && !prevTime.IsZero() {
			dt := now.Sub(*prevTime).Seconds()
			if dt > 0 {
				if st.TotalAllocBytes >= prevStats.TotalAllocBytes {
					s.AllocRateMBPerSec = float64(st.TotalAllocBytes-prevStats.TotalAllocBytes) / (1024 * 1024) / dt
				}
				if st.NumGC >= prevStats.NumGC {
					s.GCFreqPerSec = float64(st.NumGC-prevStats.NumGC) / dt
				}
				if st.GCPauseTotalNs >= prevStats.GCPauseTotalNs {
					s.GCPauseMsDelta = float64(st.GCPauseTotalNs-prevStats.GCPauseTotalNs) / 1e6
				}
				if st.FlushedBytesTotal >= prevStats.FlushedBytesTotal {
					s.FlushThroughputMBPerSec = float64(st.FlushedBytesTotal-prevStats.FlushedBytesTotal) / (1024 * 1024) / dt
				}
			}
		}
		*prevStats = st
		*prevTime = now
	}

	return s
}

func writeCSV(path string, samples []MetricSample) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{
		"elapsed_sec", "running", "oom_killed",
		"cgroup_current_mb", "cgroup_peak_mb", "cgroup_anon_mb", "cgroup_file_mb", "oom_events", "oom_kill_events",
		"vm_rss_mb", "vm_hwm_mb", "rss_anon_mb", "rss_file_mb",
		"heap_alloc_mb", "heap_inuse_mb", "heap_idle_mb", "heap_released_mb",
		"alloc_rate_mb_s", "gc_freq_s", "gc_pause_ms_delta",
		"global_inmem_mb", "max_tenant_inmem_mb", "dirty_page_count",
		"flushes_completed", "flush_throughput_mb_s",
		"backpressure_waits", "backpressure_rejects", "pressure_evictions",
	})
	for _, s := range samples {
		_ = w.Write([]string{
			fmt.Sprintf("%.2f", s.ElapsedSec), fmt.Sprintf("%v", s.Running), fmt.Sprintf("%v", s.OOMKilled),
			fmt.Sprintf("%.2f", s.CgroupCurrentMB), fmt.Sprintf("%.2f", s.CgroupPeakMB), fmt.Sprintf("%.2f", s.CgroupAnonMB), fmt.Sprintf("%.2f", s.CgroupFileMB),
			fmt.Sprintf("%d", s.OOMEvents), fmt.Sprintf("%d", s.OOMKillEvents),
			fmt.Sprintf("%.2f", s.VmRSSMB), fmt.Sprintf("%.2f", s.VmHWMMB), fmt.Sprintf("%.2f", s.RssAnonMB), fmt.Sprintf("%.2f", s.RssFileMB),
			fmt.Sprintf("%.2f", s.HeapAllocMB), fmt.Sprintf("%.2f", s.HeapInuseMB), fmt.Sprintf("%.2f", s.HeapIdleMB), fmt.Sprintf("%.2f", s.HeapReleasedMB),
			fmt.Sprintf("%.2f", s.AllocRateMBPerSec), fmt.Sprintf("%.2f", s.GCFreqPerSec), fmt.Sprintf("%.2f", s.GCPauseMsDelta),
			fmt.Sprintf("%.2f", s.GlobalInMemMB), fmt.Sprintf("%.2f", s.MaxTenantInMemMB), fmt.Sprintf("%d", s.DirtyPageCount),
			fmt.Sprintf("%d", s.FlushesCompleted), fmt.Sprintf("%.2f", s.FlushThroughputMBPerSec),
			fmt.Sprintf("%d", s.BackpressureWaits), fmt.Sprintf("%d", s.BackpressureRejects), fmt.Sprintf("%d", s.PressureEvictions),
		})
	}
	return nil
}

func main() {
	mode := flag.String("mode", "scale", "Benchmark mode: scale|pressure-recovery|suspend-wake|restart-recovery|soak")
	tenants := flag.Int("tenants", 8, "Number of tenants")
	psMemMB := flag.Int("ps-mem-mb", 256, "Pageserver memory profile label (256|512)")
	dur := flag.Duration("duration", 45*time.Second, "Workload duration")
	concBootstrap := flag.Bool("conc-bootstrap", true, "Bootstrap tenants concurrently")
	sampleInterval := flag.Duration("sample-interval", time.Second, "Metrics sample interval")
	prefix := flag.String("prefix", "p22", "Tenant ID prefix")
	outDir := flag.String("out", "/tmp/air_p22_bench", "Output directory for CSV/JSON")
	flag.Parse()

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", *outDir, err)
	}

	ctx := context.Background()
	start := time.Now()
	var prevStats PSMemoryStats
	var prevTime time.Time
	var mu sync.Mutex
	var samples []MetricSample
	stopSampler := make(chan struct{})
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		t := time.NewTicker(*sampleInterval)
		defer t.Stop()
		for {
			s := sampleOnce(start, &prevStats, &prevTime)
			mu.Lock()
			samples = append(samples, s)
			mu.Unlock()
			select {
			case <-stopSampler:
				return
			case <-t.C:
			}
		}
	}()

	var txCommitted, queryErrors atomic.Int64
	var bootDur time.Duration
	integrityOK := true

	bootstrapAll := func(n int, conc bool) ([]ProvisionResp, []*pgxpool.Pool) {
		bStart := time.Now()
		resps := make([]ProvisionResp, n)
		pools := make([]*pgxpool.Pool, n)
		if conc {
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					tid := fmt.Sprintf("%s_t%02d", *prefix, idx)
					r, err := provisionTenant(ctx, tid)
					if err != nil {
						log.Fatalf("provision %s: %v", tid, err)
					}
					p, err := connectPool(ctx, r, 2)
					if err != nil {
						log.Fatalf("connect %s: %v", tid, err)
					}
					if _, err := p.Exec(ctx, `CREATE TABLE IF NOT EXISTS kv (id bigserial PRIMARY KEY, val int NOT NULL, pad text NOT NULL)`); err != nil {
						log.Fatalf("ddl %s: %v", tid, err)
					}
					resps[idx] = r
					pools[idx] = p
				}(i)
			}
			wg.Wait()
		} else {
			for i := 0; i < n; i++ {
				tid := fmt.Sprintf("%s_t%02d", *prefix, i)
				r, err := provisionTenant(ctx, tid)
				if err != nil {
					log.Fatalf("provision %s: %v", tid, err)
				}
				p, err := connectPool(ctx, r, 2)
				if err != nil {
					log.Fatalf("connect %s: %v", tid, err)
				}
				if _, err := p.Exec(ctx, `CREATE TABLE IF NOT EXISTS kv (id bigserial PRIMARY KEY, val int NOT NULL, pad text NOT NULL)`); err != nil {
					log.Fatalf("ddl %s: %v", tid, err)
				}
				resps[i] = r
				pools[i] = p
			}
		}
		bootDur = time.Since(bStart)
		return resps, pools
	}

	verifyTenants := func(pools []*pgxpool.Pool, expected []int64) bool {
		ok := true
		for i, p := range pools {
			var cnt, sum int64
			var err error
			for attempt := 0; attempt < 4; attempt++ {
				err = p.QueryRow(ctx, `SELECT count(*), coalesce(sum(val), 0) FROM kv`).Scan(&cnt, &sum)
				if err == nil {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			if err != nil {
				log.Printf("[VERIFY] tenant %d query error: %v", i, err)
				queryErrors.Add(1)
				ok = false
				continue
			}
			if cnt != expected[i] {
				log.Printf("[VERIFY] tenant %d count mismatch: got=%d want=%d", i, cnt, expected[i])
				ok = false
			}
		}
		return ok
	}

	runWorkload := func(pools []*pgxpool.Pool, expected []int64, d time.Duration, padBytes int) {
		deadline := time.Now().Add(d)
		var wg sync.WaitGroup
		for i, p := range pools {
			wg.Add(1)
			go func(idx int, pool *pgxpool.Pool) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(idx + 1)))
				pad := string(bytes.Repeat([]byte{'x'}, padBytes))
				for time.Now().Before(deadline) {
					v := rng.Intn(1000) + 1
					if _, err := pool.Exec(ctx, `INSERT INTO kv (val, pad) VALUES ($1, $2)`, v, pad); err != nil {
						queryErrors.Add(1)
						time.Sleep(20 * time.Millisecond)
						continue
					}
					atomic.AddInt64(&expected[idx], 1)
					txCommitted.Add(1)

					var c int64
					if err := pool.QueryRow(ctx, `SELECT count(*) FROM kv`).Scan(&c); err != nil {
						queryErrors.Add(1)
					}
					time.Sleep(15 * time.Millisecond)
				}
			}(i, p)
		}
		wg.Wait()
	}

	switch *mode {
	case "scale", "soak":
		resps, pools := bootstrapAll(*tenants, *concBootstrap)
		_ = resps
		expected := make([]int64, len(pools))
		log.Printf("[%s] Bootstrapped %d tenants in %s (conc=%v). Running workload for %s...", *mode, *tenants, bootDur.Round(time.Millisecond), *concBootstrap, *dur)
		runWorkload(pools, expected, *dur, 256)
		integrityOK = verifyTenants(pools, expected)
		for _, p := range pools {
			p.Close()
		}

	case "pressure-recovery":
		_, pools := bootstrapAll(*tenants, true)
		expected := make([]int64, len(pools))
		log.Printf("[pressure-recovery] Bursting writes across %d tenants for %s...", *tenants, *dur)
		runWorkload(pools, expected, *dur, 1024)
		log.Printf("[pressure-recovery] Going idle for 5s to verify flush drain and memory recovery...")
		time.Sleep(5 * time.Second)
		st, ok := fetchPSStats()
		if !ok || st.GlobalInMemBytes > 128*1024 {
			log.Printf("[pressure-recovery] warning: GlobalInMemBytes=%d after 5s idle", st.GlobalInMemBytes)
		}
		integrityOK = verifyTenants(pools, expected)
		for _, p := range pools {
			p.Close()
		}

	case "suspend-wake":
		resps, pools := bootstrapAll(*tenants, true)
		expected := make([]int64, len(pools))
		runWorkload(pools, expected, 15*time.Second, 256)
		for _, p := range pools {
			p.Close()
		}
		for _, r := range resps {
			if err := suspendTenant(ctx, r.TenantID); err != nil {
				log.Fatalf("suspend %s: %v", r.TenantID, err)
			}
		}
		time.Sleep(2 * time.Second)
		if st, ok := fetchPSStats(); ok {
			log.Printf("[suspend-wake] after suspend: GlobalInMemBytes=%d DirtyPages=%d VmRSS?", st.GlobalInMemBytes, st.TotalDirtyPages)
		}
		for i, r := range resps {
			if err := wakeTenant(ctx, r.TenantID); err != nil {
				log.Fatalf("wake %s: %v", r.TenantID, err)
			}
			p, err := connectPool(ctx, r, 2)
			if err != nil {
				log.Fatalf("reconnect %s: %v", r.TenantID, err)
			}
			pools[i] = p
		}
		integrityOK = verifyTenants(pools, expected)
		for _, p := range pools {
			p.Close()
		}

	case "restart-recovery":
		resps, pools := bootstrapAll(*tenants, true)
		expected := make([]int64, len(pools))
		runWorkload(pools, expected, 15*time.Second, 256)
		for _, p := range pools {
			p.Close()
		}
		log.Printf("[restart-recovery] Graceful restart of %s...", psContainerName)
		if out, err := exec.Command("docker", "restart", psContainerName).CombinedOutput(); err != nil {
			log.Fatalf("docker restart failed: %v %s", err, string(out))
		}
		for i := 0; i < 40; i++ {
			if _, ok := fetchPSStats(); ok {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		for i, r := range resps {
			p, err := connectPool(ctx, r, 2)
			if err != nil {
				log.Fatalf("reconnect post-restart %s: %v", r.TenantID, err)
			}
			pools[i] = p
		}
		if !verifyTenants(pools, expected) {
			integrityOK = false
		}
		for _, p := range pools {
			p.Close()
		}

		log.Printf("[restart-recovery] Crash kill (-SIGKILL) + restart of %s...", psContainerName)
		if out, err := exec.Command("docker", "kill", "-s", "SIGKILL", psContainerName).CombinedOutput(); err != nil {
			log.Fatalf("docker kill failed: %v %s", err, string(out))
		}
		if out, err := exec.Command("docker", "start", psContainerName).CombinedOutput(); err != nil {
			log.Fatalf("docker start failed: %v %s", err, string(out))
		}
		for i := 0; i < 40; i++ {
			if _, ok := fetchPSStats(); ok {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		for i, r := range resps {
			p, err := connectPool(ctx, r, 2)
			if err != nil {
				log.Fatalf("reconnect post-crash %s: %v", r.TenantID, err)
			}
			pools[i] = p
		}
		if !verifyTenants(pools, expected) {
			integrityOK = false
		}
		for _, p := range pools {
			p.Close()
		}

	default:
		log.Fatalf("unknown mode: %s", *mode)
	}

	close(stopSampler)
	samplerWG.Wait()

	finalPS, _ := fetchPSStats()
	mu.Lock()
	snapSamples := append([]MetricSample(nil), samples...)
	mu.Unlock()

	var sum BenchSummary
	sum.Mode = *mode
	sum.Tenants = *tenants
	sum.PSMemoryMB = *psMemMB
	sum.DurationSec = time.Since(start).Seconds()
	sum.ConcBootstrap = *concBootstrap
	sum.BootstrapSec = bootDur.Seconds()
	sum.TotalNumGC = finalPS.NumGC
	sum.TotalGCPauseMs = float64(finalPS.GCPauseTotalNs) / 1e6
	sum.TotalFlushes = finalPS.FlushesCompleted
	sum.TxCommitted = txCommitted.Load()
	sum.QueryErrors = queryErrors.Load()
	sum.DataIntegrityOK = integrityOK && sum.QueryErrors == 0
	sum.FinalInMemMB = float64(finalPS.GlobalInMemBytes) / (1024 * 1024)

	var allocSum, flushSum float64
	var rateCount int
	for _, s := range snapSamples {
		if s.CgroupPeakMB > sum.PeakCgroupMB {
			sum.PeakCgroupMB = s.CgroupPeakMB
		}
		if s.VmRSSMB > sum.PeakVmRSSMB {
			sum.PeakVmRSSMB = s.VmRSSMB
		}
		if s.VmHWMMB > sum.PeakVmHWMMB {
			sum.PeakVmHWMMB = s.VmHWMMB
		}
		if s.HeapAllocMB > sum.PeakHeapAllocMB {
			sum.PeakHeapAllocMB = s.HeapAllocMB
		}
		if s.GlobalInMemMB > sum.PeakGlobalInMemMB {
			sum.PeakGlobalInMemMB = s.GlobalInMemMB
		}
		if s.MaxTenantInMemMB > sum.PeakTenantInMemMB {
			sum.PeakTenantInMemMB = s.MaxTenantInMemMB
		}
		if s.DirtyPageCount > sum.PeakDirtyPages {
			sum.PeakDirtyPages = s.DirtyPageCount
		}
		if s.OOMEvents > sum.OOMEvents {
			sum.OOMEvents = s.OOMEvents
		}
		if s.OOMKillEvents > sum.OOMKillEvents {
			sum.OOMKillEvents = s.OOMKillEvents
		}
		if s.AllocRateMBPerSec > 0 {
			allocSum += s.AllocRateMBPerSec
			flushSum += s.FlushThroughputMBPerSec
			rateCount++
		}
		sum.FinalVmRSSMB = s.VmRSSMB
	}
	if rateCount > 0 {
		sum.AvgAllocRateMBs = allocSum / float64(rateCount)
		sum.AvgFlushMBs = flushSum / float64(rateCount)
	}

	tag := fmt.Sprintf("%s_%dmb_t%d", *mode, *psMemMB, *tenants)
	csvPath := filepath.Join(*outDir, tag+".csv")
	jsonPath := filepath.Join(*outDir, tag+"_summary.json")
	_ = writeCSV(csvPath, snapSamples)
	rawJSON, _ := json.MarshalIndent(sum, "", "  ")
	_ = os.WriteFile(jsonPath, rawJSON, 0o644)

	log.Printf("=== SUMMARY [%s] ===\n%s", tag, string(rawJSON))
	if !sum.DataIntegrityOK || sum.OOMKillEvents > 0 {
		log.Fatalf("benchmark failed integrity=%v oom_kills=%d query_errors=%d", sum.DataIntegrityOK, sum.OOMKillEvents, sum.QueryErrors)
	}
}
