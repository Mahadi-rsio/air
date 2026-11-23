package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	controlPlaneURL = "http://127.0.0.1:18080"
	gatewayPGHost   = "127.0.0.1"
	gatewayPGPort   = 15432
	bootstrapAPIKey = "air_p21_bootstrap_key"
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

func connectPool(ctx context.Context, t ProvisionResp) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", t.Username, t.Password, gatewayPGHost, gatewayPGPort, t.Database)
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 4
	return pgxpool.NewWithConfig(ctx, cfg)
}

type PSContainerMetrics struct {
	Stage         string  `json:"stage"`
	Running       bool    `json:"running"`
	OOMKilled     bool    `json:"oom_killed"`
	ExitCode      int     `json:"exit_code"`
	MemCurrentMB  float64 `json:"mem_current_mb"`
	MemPeakMB     float64 `json:"mem_peak_mb"`
	MemMaxMB      float64 `json:"mem_max_mb"`
	AnonMB        float64 `json:"anon_mb"`
	FileMB        float64 `json:"file_mb"`
	HighEvents    int64   `json:"high_events"`
	OOMEvents     int64   `json:"oom_events"`
	OOMKillEvents int64   `json:"oom_kill_events"`
	VmRSSMB       float64 `json:"vm_rss_mb"`
	VmHWMMB       float64 `json:"vm_hwm_mb"`
	RssAnonMB     float64 `json:"rss_anon_mb"`
	RssFileMB     float64 `json:"rss_file_mb"`
	Threads       int64   `json:"threads"`
}

func samplePSContainer(stage string) PSContainerMetrics {
	m := PSContainerMetrics{Stage: stage}
	inspOut, err := exec.Command("docker", "inspect", "--format", "{{.State.Running}} {{.State.OOMKilled}} {{.State.ExitCode}}", "air-p21-pageserver").Output()
	if err == nil {
		parts := strings.Fields(strings.TrimSpace(string(inspOut)))
		if len(parts) == 3 {
			m.Running = parts[0] == "true"
			m.OOMKilled = parts[1] == "true"
			m.ExitCode, _ = strconv.Atoi(parts[2])
		}
	}
	if !m.Running {
		log.Printf("[REPRO][%s] air-p21-pageserver STOPPED! OOMKilled=%v ExitCode=%d", stage, m.OOMKilled, m.ExitCode)
		return m
	}
	script := `
cat /sys/fs/cgroup/memory.current 2>/dev/null || echo 0
cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo 0
cat /sys/fs/cgroup/memory.max 2>/dev/null || echo 0
awk '/^anon /{a=$2} /^file /{f=$2} END{printf "%d %d\n", a+0, f+0}' /sys/fs/cgroup/memory.stat 2>/dev/null || echo "0 0"
awk '/^high /{h=$2} /^oom /{o=$2} /^oom_kill /{k=$2} END{printf "%d %d %d\n", h+0, o+0, k+0}' /sys/fs/cgroup/memory.events 2>/dev/null || echo "0 0 0"
for p in /proc/[0-9]*/comm; do
  [ -r "$p" ] || continue
  dir=${p%/comm}
  comm=$(cat "$p" 2>/dev/null) || continue
  case "$comm" in
    *air-pageserver*)
      awk '/^VmRSS:/{rss=$2} /^VmHWM:/{hwm=$2} /^RssAnon:/{ra=$2} /^RssFile:/{rf=$2} /^Threads:/{th=$2} END{printf "%d %d %d %d %d\n", rss+0, hwm+0, ra+0, rf+0, th+0}' "$dir/status"
      break
      ;;
  esac
done
`
	var out bytes.Buffer
	cmd := exec.Command("docker", "exec", "air-p21-pageserver", "sh", "-c", script)
	cmd.Stdout = &out
	if err := cmd.Run(); err == nil {
		lines := strings.Split(strings.TrimSpace(out.String()), "\n")
		if len(lines) >= 6 {
			cur, _ := strconv.ParseFloat(strings.TrimSpace(lines[0]), 64)
			peak, _ := strconv.ParseFloat(strings.TrimSpace(lines[1]), 64)
			mx, _ := strconv.ParseFloat(strings.TrimSpace(lines[2]), 64)
			m.MemCurrentMB = cur / (1024 * 1024)
			m.MemPeakMB = peak / (1024 * 1024)
			m.MemMaxMB = mx / (1024 * 1024)
			if f := strings.Fields(lines[3]); len(f) == 2 {
				a, _ := strconv.ParseFloat(f[0], 64)
				fl, _ := strconv.ParseFloat(f[1], 64)
				m.AnonMB = a / (1024 * 1024)
				m.FileMB = fl / (1024 * 1024)
			}
			if f := strings.Fields(lines[4]); len(f) == 3 {
				m.HighEvents, _ = strconv.ParseInt(f[0], 10, 64)
				m.OOMEvents, _ = strconv.ParseInt(f[1], 10, 64)
				m.OOMKillEvents, _ = strconv.ParseInt(f[2], 10, 64)
			}
			if f := strings.Fields(lines[5]); len(f) == 5 {
				rss, _ := strconv.ParseFloat(f[0], 64)
				hwm, _ := strconv.ParseFloat(f[1], 64)
				ra, _ := strconv.ParseFloat(f[2], 64)
				rf, _ := strconv.ParseFloat(f[3], 64)
				m.VmRSSMB = rss / 1024.0
				m.VmHWMMB = hwm / 1024.0
				m.RssAnonMB = ra / 1024.0
				m.RssFileMB = rf / 1024.0
				m.Threads, _ = strconv.ParseInt(f[4], 10, 64)
			}
		}
	}
	log.Printf("[REPRO][%s] Running=%v OOMKilled=%v Exit=%d | CgroupCur=%.2fMB Peak=%.2fMB Max=%.0fMB Anon=%.2fMB File=%.2fMB OOM=%d OOMKill=%d | VmRSS=%.2fMB(Anon=%.2fMB File=%.2fMB HWM=%.2fMB) Threads=%d",
		stage, m.Running, m.OOMKilled, m.ExitCode,
		m.MemCurrentMB, m.MemPeakMB, m.MemMaxMB, m.AnonMB, m.FileMB, m.OOMEvents, m.OOMKillEvents,
		m.VmRSSMB, m.RssAnonMB, m.RssFileMB, m.VmHWMMB, m.Threads)
	return m
}

func main() {
	ctx := context.Background()
	var history []PSContainerMetrics
	history = append(history, samplePSContainer("00_initial"))

	// Step 1: Provision 4 prior active tenants and run heavy writes on them (matching Phase 21.10 mt-overload 4-tenant test)
	log.Printf("=== Step 1: Provisioning 4 prior active tenants and running initial writes ===")
	priorPools := make([]*pgxpool.Pool, 4)
	for i := 0; i < 4; i++ {
		tid := fmt.Sprintf("repro_prior_t%d", i)
		t, err := provisionTenant(ctx, tid)
		if err != nil {
			log.Fatalf("provision %s: %v", tid, err)
		}
		p, err := connectPool(ctx, t)
		if err != nil {
			log.Fatalf("connect %s: %v", tid, err)
		}
		defer p.Close()
		priorPools[i] = p
		if _, err := p.Exec(ctx, `
			CREATE TABLE mto_data (id bigserial PRIMARY KEY, k int, v text);
			INSERT INTO mto_data (k, v) SELECT g, repeat('x', 500) FROM generate_series(1, 3000) g;
		`); err != nil {
			log.Fatalf("init writes %s: %v", tid, err)
		}
		history = append(history, samplePSContainer(fmt.Sprintf("01_prior_t%d_loaded", i)))
	}

	// Step 2: Concurrently bootstrap 8 new active tenants while the 4 prior active tenants continue receiving writes
	log.Printf("=== Step 2: Bootstrapping 8 additional active tenants + sustained writes ===")
	var wg sync.WaitGroup
	stopWriters := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for {
				select {
				case <-stopWriters:
					return
				default:
				}
				_, _ = priorPools[idx].Exec(ctx, `INSERT INTO mto_data (k, v) SELECT g, repeat('w', 400) FROM generate_series(1, 200) g`)
				time.Sleep(50 * time.Millisecond)
			}
		}(i)
	}

	for i := 0; i < 8; i++ {
		tid := fmt.Sprintf("repro_active_t%d", i)
		t, err := provisionTenant(ctx, tid)
		m := samplePSContainer(fmt.Sprintf("02_active_t%d_provision", i))
		history = append(history, m)
		if err != nil || !m.Running || m.OOMKilled {
			log.Printf("OOM / failure reproduced at active tenant %d (%s): err=%v OOMKilled=%v ExitCode=%d", i, tid, err, m.OOMKilled, m.ExitCode)
			break
		}
		p, err := connectPool(ctx, t)
		if err == nil {
			_, werr := p.Exec(ctx, `
				CREATE TABLE mto_data (id bigserial PRIMARY KEY, k int, v text);
				INSERT INTO mto_data (k, v) SELECT g, repeat('y', 500) FROM generate_series(1, 3000) g;
			`)
			p.Close()
			m2 := samplePSContainer(fmt.Sprintf("02_active_t%d_writes", i))
			history = append(history, m2)
			if werr != nil || !m2.Running || m2.OOMKilled {
				log.Printf("OOM / failure reproduced during writes on active tenant %d (%s): err=%v OOMKilled=%v ExitCode=%d", i, tid, werr, m2.OOMKilled, m2.ExitCode)
				break
			}
		}
	}
	close(stopWriters)
	wg.Wait()

	history = append(history, samplePSContainer("03_final"))
	raw, _ := json.MarshalIndent(history, "", "  ")
	_ = os.MkdirAll("/tmp/air_p22_repro", 0o755)
	_ = os.WriteFile("/tmp/air_p22_repro/oom_repro_metrics.json", raw, 0o644)
	log.Printf("Saved OOM reproduction metrics to /tmp/air_p22_repro/oom_repro_metrics.json")
}
