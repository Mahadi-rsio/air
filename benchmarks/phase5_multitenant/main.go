package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	apiURL = "http://127.0.0.1:8080"
	token  = "air_bench_token_local_only"
	outDir = "/workspaces/air/benchmarks/results/phase5"
)

type TenantInfo struct {
	ID   string
	Port int
	Pass string
}

type TierResult struct {
	Tenants             int     `json:"tenants"`
	ProvisionP50Ms      float64 `json:"provision_p50_ms"`
	ProvisionP95Ms      float64 `json:"provision_p95_ms"`
	ProvisionFail       int     `json:"provision_fail"`
	ReadAggregateTPS    float64 `json:"read_agg_tps"`
	ReadP95Ms           float64 `json:"read_p95_ms"`
	ReadP99Ms           float64 `json:"read_p99_ms"`
	ReadFail            int64   `json:"read_fail"`
	MixedAggregateTPS   float64 `json:"mixed_agg_tps"`
	MixedP95Ms          float64 `json:"mixed_p95_ms"`
	MixedP99Ms          float64 `json:"mixed_p99_ms"`
	MixedFail           int64   `json:"mixed_fail"`
	IsolationPassed     bool    `json:"isolation_passed"`
	IsolationDetail     string  `json:"isolation_detail"`
	ActiveMemMiB        float64 `json:"active_mem_mib"`
	IdleMemMiB          float64 `json:"idle_mem_mib"`
	ConcurrentWakeP50Ms float64 `json:"concurrent_wake_p50_ms"`
	ConcurrentWakeP95Ms float64 `json:"concurrent_wake_p95_ms"`
	ConcurrentWakeFail  int     `json:"concurrent_wake_fail"`
}

func httpJSON(method, path string, body any, auth bool) ([]byte, int, error) {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, apiURL+path, r)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if auth {
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Idempotency-Key", fmt.Sprintf("p5-%d-%s", time.Now().UnixNano(), path))
	}
	resp, err := (&http.Client{Timeout: 120 * time.Second}).Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b, resp.StatusCode, nil
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	k := float64(len(sorted)-1) * (p / 100.0)
	f := int(math.Floor(k))
	c := int(math.Ceil(k))
	if f == c {
		return sorted[f]
	}
	return sorted[f]*(float64(c)-k) + sorted[c]*(k-float64(f))
}

func containerMemMiB() float64 {
	out, err := exec.Command("docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", "air-bench").Output()
	if err != nil {
		return 0
	}
	parts := strings.Fields(string(out))
	if len(parts) == 0 {
		return 0
	}
	s := parts[0]
	if strings.HasSuffix(s, "GiB") {
		v, _ := strconv.ParseFloat(strings.TrimSuffix(s, "GiB"), 64)
		return v * 1024
	}
	if strings.HasSuffix(s, "MiB") {
		v, _ := strconv.ParseFloat(strings.TrimSuffix(s, "MiB"), 64)
		return v
	}
	return 0
}

func runTier(ctx context.Context, n int) TierResult {
	log.Printf("=== Multi-Tenant Tier: %d tenants ===", n)
	res := TierResult{Tenants: n}
	tenants := make([]TenantInfo, 0, n)
	var provLats []float64

	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("mt_%d_%d", n, i)
		t0 := time.Now()
		b, code, err := httpJSON("POST", "/v1/databases", map[string]string{"tenant_id": id}, true)
		lat := float64(time.Since(t0).Milliseconds())
		if err != nil || code >= 300 {
			res.ProvisionFail++
			log.Printf("  provision %s failed: code=%d err=%v body=%s", id, code, err, string(b))
			continue
		}
		provLats = append(provLats, lat)
		rb, rcode, _ := httpJSON("GET", "/internal/v1/tenants/"+id+"/route", nil, false)
		if rcode == 200 {
			var rt struct {
				Port        int    `json:"port"`
				AppPassword string `json:"app_password"`
			}
			_ = json.Unmarshal(rb, &rt)
			tenants = append(tenants, TenantInfo{ID: id, Port: rt.Port, Pass: rt.AppPassword})
		}
	}
	sort.Float64s(provLats)
	res.ProvisionP50Ms = math.Round(pct(provLats, 50)*100) / 100
	res.ProvisionP95Ms = math.Round(pct(provLats, 95)*100) / 100

	// Seed each tenant with unique marker & 1000 rows
	pools := make(map[string]*pgxpool.Pool)
	for _, t := range tenants {
		dsn := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable&pool_max_conns=4", t.Pass, t.Port, t.ID)
		p, err := pgxpool.New(ctx, dsn)
		if err != nil {
			continue
		}
		_, err = p.Exec(ctx, `CREATE TABLE IF NOT EXISTS mt_data (id INT PRIMARY KEY, secret TEXT NOT NULL, val INT NOT NULL)`)
		if err == nil {
			_, err = p.Exec(ctx, `INSERT INTO mt_data (id, secret, val) SELECT g, $1, g FROM generate_series(1, 1000) AS g ON CONFLICT (id) DO NOTHING`, "secret_"+t.ID)
		}
		if err != nil {
			log.Printf("  seed %s err: %v", t.ID, err)
		}
		pools[t.ID] = p
	}

	res.ActiveMemMiB = containerMemMiB()

	// Independent Read Workload (10s across all tenants)
	runWorkload := func(mixed bool) (float64, float64, float64, int64) {
		var okCnt, failCnt atomic.Int64
		var latMu sync.Mutex
		var lats []float64
		var wg sync.WaitGroup
		stopAt := time.Now().Add(6 * time.Second)
		for _, t := range tenants {
			p := pools[t.ID]
			if p == nil {
				continue
			}
			wg.Add(1)
			go func(tid string, pool *pgxpool.Pool) {
				defer wg.Done()
				var localLats []float64
				i := 1
				for time.Now().Before(stopAt) {
					id := (i % 1000) + 1
					i++
					t0 := time.Now()
					var err error
					if mixed && i%4 == 0 {
						_, err = pool.Exec(ctx, `UPDATE mt_data SET val = val + 1 WHERE id = $1`, id)
					} else {
						var v int
						err = pool.QueryRow(ctx, `SELECT val FROM mt_data WHERE id = $1`, id).Scan(&v)
					}
					ms := float64(time.Since(t0).Microseconds()) / 1000.0
					if err != nil {
						failCnt.Add(1)
					} else {
						okCnt.Add(1)
						localLats = append(localLats, ms)
					}
				}
				latMu.Lock()
				lats = append(lats, localLats...)
				latMu.Unlock()
			}(t.ID, p)
		}
		wg.Wait()
		sort.Float64s(lats)
		return math.Round(float64(okCnt.Load())/10.0*100) / 100, math.Round(pct(lats, 95)*1000) / 1000, math.Round(pct(lats, 99)*1000) / 1000, failCnt.Load()
	}

	res.ReadAggregateTPS, res.ReadP95Ms, res.ReadP99Ms, res.ReadFail = runWorkload(false)
	res.MixedAggregateTPS, res.MixedP95Ms, res.MixedP99Ms, res.MixedFail = runWorkload(true)

	// Verify tenant isolation
	isoOK := true
	var isoErrs []string
	for idx, t := range tenants {
		p := pools[t.ID]
		if p == nil {
			continue
		}
		var sec string
		err := p.QueryRow(ctx, `SELECT DISTINCT secret FROM mt_data LIMIT 1`).Scan(&sec)
		if err != nil || sec != "secret_"+t.ID {
			isoOK = false
			isoErrs = append(isoErrs, fmt.Sprintf("%s got secret %q err=%v", t.ID, sec, err))
		}
		if idx > 0 {
			other := tenants[idx-1]
			crossDSN := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", other.Pass, t.Port, other.ID)
			cp, cerr := pgxpool.New(ctx, crossDSN)
			if cerr == nil {
				var dummy int
				qerr := cp.QueryRow(ctx, `SELECT 1`).Scan(&dummy)
				cp.Close()
				if qerr == nil {
					isoOK = false
					isoErrs = append(isoErrs, fmt.Sprintf("cross-tenant connect succeeded: %s -> %s", other.ID, t.ID))
				}
			}
		}
	}
	res.IsolationPassed = isoOK
	if isoOK {
		res.IsolationDetail = fmt.Sprintf("verified %d tenants: unique data + cross-tenant auth/db rejected", len(tenants))
	} else {
		res.IsolationDetail = strings.Join(isoErrs, "; ")
	}

	for _, p := range pools {
		p.Close()
	}

	// Suspend all tenants
	for _, t := range tenants {
		_, _, _ = httpJSON("POST", "/internal/v1/databases/"+t.ID+"/suspend", nil, false)
	}
	time.Sleep(2 * time.Second)
	res.IdleMemMiB = containerMemMiB()

	// Wake a subset simultaneously (up to 10)
	wakeCount := len(tenants)
	if wakeCount > 10 {
		wakeCount = 10
	}
	var wakeMu sync.Mutex
	var wakeLats []float64
	var wakeFail atomic.Int32
	var wwg sync.WaitGroup
	for i := 0; i < wakeCount; i++ {
		wwg.Add(1)
		go func(tid string) {
			defer wwg.Done()
			t0 := time.Now()
			_, code, err := httpJSON("POST", "/internal/v1/databases/"+tid+"/start", nil, false)
			ms := float64(time.Since(t0).Milliseconds())
			if err != nil || code >= 300 {
				wakeFail.Add(1)
				return
			}
			wakeMu.Lock()
			wakeLats = append(wakeLats, ms)
			wakeMu.Unlock()
		}(tenants[i].ID)
	}
	wwg.Wait()
	sort.Float64s(wakeLats)
	res.ConcurrentWakeP50Ms = math.Round(pct(wakeLats, 50)*100) / 100
	res.ConcurrentWakeP95Ms = math.Round(pct(wakeLats, 95)*100) / 100
	res.ConcurrentWakeFail = int(wakeFail.Load())

	// Cleanup tier tenants
	for _, t := range tenants {
		_, _, _ = httpJSON("DELETE", "/v1/databases/"+t.ID, nil, true)
	}
	return res
}

func main() {
	tiersFlag := flag.String("tiers", "1,10,25,50", "Comma-separated tenant counts")
	flag.Parse()

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	var results []TierResult
	for _, s := range strings.Split(*tiersFlag, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n <= 0 {
			continue
		}
		r := runTier(ctx, n)
		results = append(results, r)
		b, _ := json.Marshal(r)
		log.Printf("TIER %d RESULT: %s", n, string(b))
	}
	outBytes, _ := json.MarshalIndent(results, "", "  ")
	_ = os.WriteFile(filepath.Join(outDir, "multitenant_results.json"), outBytes, 0o644)
	fmt.Println(string(outBytes))
}
