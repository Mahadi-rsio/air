package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	controlPlaneURL = "http://127.0.0.1:18080"
	gatewayAdminURL = "http://127.0.0.1:18081"
	pageserverURL   = "http://127.0.0.1:18083"
	gatewayPGHost   = "127.0.0.1"
	gatewayPGPort   = 15432
	bootstrapAPIKey = "air_p21_bootstrap_key"
	internalToken   = "air-p21-internal-token"
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
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
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
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("wake %s status=%d body=%s", tenantID, resp.StatusCode, string(raw))
	}
	return nil
}

func connectGatewayPool(ctx context.Context, tenant ProvisionResp, maxConns int32) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		tenant.Username, tenant.Password, gatewayPGHost, gatewayPGPort, tenant.Database)
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = 1
	return pgxpool.NewWithConfig(ctx, cfg)
}

func waitHealthy(ctx context.Context, url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", url)
}

func dockerCmd(args ...string) (string, error) {
	cmd := exec.Command("docker", args...)
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	err := cmd.Run()
	if err != nil {
		return out.String(), fmt.Errorf("%v: %s", err, errBuf.String())
	}
	return strings.TrimSpace(out.String()), nil
}

type CgroupSample struct {
	Timestamp     time.Time `json:"timestamp"`
	MemCurrentMB  float64   `json:"mem_current_mb"`
	MemPeakMB     float64   `json:"mem_peak_mb"`
	MemMaxMB      float64   `json:"mem_max_mb"`
	AnonMB        float64   `json:"anon_mb"`
	FileMB        float64   `json:"file_mb"`
	HighEvents    uint64    `json:"high_events"`
	OOMEvents     uint64    `json:"oom_events"`
	OOMKillEvents uint64    `json:"oom_kill_events"`
	MemPSISome10  float64   `json:"mem_psi_some_10"`
	CPUPSISome10  float64   `json:"cpu_psi_some_10"`
	IOPSIMax10    float64   `json:"io_psi_max_10"`
	PGRSSMB       float64   `json:"pg_rss_mb"`
	GatewayRSSMB  float64   `json:"gateway_rss_mb"`
	PageserverMB  float64   `json:"pageserver_mb"`
}

func sampleContainerCgroup(container string) CgroupSample {
	s := CgroupSample{Timestamp: time.Now()}
	script := `
cat /sys/fs/cgroup/memory.current 2>/dev/null || echo 0
cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo 0
cat /sys/fs/cgroup/memory.max 2>/dev/null || echo 0
awk '/^anon /{a=$2} /^file /{f=$2} END{printf "%d %d\n", a+0, f+0}' /sys/fs/cgroup/memory.stat 2>/dev/null || echo "0 0"
awk '/^high /{h=$2} /^oom /{o=$2} /^oom_kill /{k=$2} END{printf "%d %d %d\n", h+0, o+0, k+0}' /sys/fs/cgroup/memory.events 2>/dev/null || echo "0 0 0"
awk '/^some /{for(i=1;i<=NF;i++) if($i~/^avg10=/) {split($i,x,"="); print x[2]}}' /sys/fs/cgroup/memory.pressure 2>/dev/null || echo 0
awk '/^some /{for(i=1;i<=NF;i++) if($i~/^avg10=/) {split($i,x,"="); print x[2]}}' /sys/fs/cgroup/cpu.pressure 2>/dev/null || echo 0
awk '/^some /{for(i=1;i<=NF;i++) if($i~/^avg10=/) {split($i,x,"="); print x[2]}}' /sys/fs/cgroup/io.pressure 2>/dev/null || echo 0
ps -eo comm,rss --no-headers | awk '/postgres/{sum+=$2} END{printf "%.2f\n", sum/1024}'
`
	out, err := dockerCmd("exec", container, "sh", "-c", script)
	if err != nil {
		return s
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	parseF := func(idx int) float64 {
		if idx >= len(lines) {
			return 0
		}
		v, _ := strconv.ParseFloat(strings.TrimSpace(lines[idx]), 64)
		return v
	}
	s.MemCurrentMB = parseF(0) / (1024 * 1024)
	s.MemPeakMB = parseF(1) / (1024 * 1024)
	s.MemMaxMB = parseF(2) / (1024 * 1024)
	if len(lines) > 3 {
		parts := strings.Fields(lines[3])
		if len(parts) == 2 {
			a, _ := strconv.ParseFloat(parts[0], 64)
			f, _ := strconv.ParseFloat(parts[1], 64)
			s.AnonMB = a / (1024 * 1024)
			s.FileMB = f / (1024 * 1024)
		}
	}
	if len(lines) > 4 {
		parts := strings.Fields(lines[4])
		if len(parts) == 3 {
			s.HighEvents, _ = strconv.ParseUint(parts[0], 10, 64)
			s.OOMEvents, _ = strconv.ParseUint(parts[1], 10, 64)
			s.OOMKillEvents, _ = strconv.ParseUint(parts[2], 10, 64)
		}
	}
	s.MemPSISome10 = parseF(5)
	s.CPUPSISome10 = parseF(6)
	s.IOPSIMax10 = parseF(7)
	s.PGRSSMB = parseF(8)

	if gwOut, err := dockerCmd("exec", "air-p21-gateway", "sh", "-c", "cat /sys/fs/cgroup/memory.current 2>/dev/null || echo 0"); err == nil {
		v, _ := strconv.ParseFloat(strings.TrimSpace(gwOut), 64)
		s.GatewayRSSMB = v / (1024 * 1024)
	}
	if psOut, err := dockerCmd("exec", "air-p21-pageserver", "sh", "-c", "cat /sys/fs/cgroup/memory.current 2>/dev/null || echo 0"); err == nil {
		v, _ := strconv.ParseFloat(strings.TrimSpace(psOut), 64)
		s.PageserverMB = v / (1024 * 1024)
	}
	return s
}

func getGatewayGovStats(ctx context.Context) map[string]any {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, gatewayAdminURL+"/internal/v1/write-governor", nil)
	req.Header.Set("X-Air-Internal-Token", internalToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return m
}

func calcPercentiles(latUs []float64) (p50, p95, p99 float64) {
	if len(latUs) == 0 {
		return 0, 0, 0
	}
	cp := make([]float64, len(latUs))
	copy(cp, latUs)
	sort.Float64s(cp)
	at := func(q float64) float64 {
		idx := int(float64(len(cp)-1) * q)
		return cp[idx] / 1000.0 // ms
	}
	return at(0.50), at(0.95), at(0.99)
}
