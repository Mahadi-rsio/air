package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
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
	Timestamp       time.Time `json:"timestamp"`
	MemCurrentMB    float64   `json:"mem_current_mb"`
	MemPeakMB       float64   `json:"mem_peak_mb"`
	MemMaxMB        float64   `json:"mem_max_mb"`
	AnonMB          float64   `json:"anon_mb"`
	FileMB          float64   `json:"file_mb"`
	HighEvents      uint64    `json:"high_events"`
	OOMEvents       uint64    `json:"oom_events"`
	OOMKillEvents   uint64    `json:"oom_kill_events"`
	EventsAvail     bool      `json:"events_avail"`
	CPUUsageUsec    uint64    `json:"cpu_usage_usec"`
	CPUUsageAvail   bool      `json:"cpu_usage_avail"`
	MemPSISome10    float64   `json:"mem_psi_some_10"`
	CPUPSISome10    float64   `json:"cpu_psi_some_10"`
	IOPSIMax10      float64   `json:"io_psi_max_10"`
	PGRSSMB         float64   `json:"pg_rss_mb"`
	GatewayRSSMB    float64   `json:"gateway_rss_mb"`
	PageserverMB    float64   `json:"pageserver_mb"`
	ReadEngineRSSMB float64   `json:"read_engine_rss_mb"`
}

func newUnavailableSample() CgroupSample {
	nan := math.NaN()
	return CgroupSample{
		Timestamp:       time.Now(),
		MemCurrentMB:    nan,
		MemPeakMB:       nan,
		MemMaxMB:        nan,
		AnonMB:          nan,
		FileMB:          nan,
		MemPSISome10:    nan,
		CPUPSISome10:    nan,
		IOPSIMax10:      nan,
		PGRSSMB:         nan,
		GatewayRSSMB:    nan,
		PageserverMB:    nan,
		ReadEngineRSSMB: nan,
	}
}

func fmtMetric(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "NA"
	}
	return fmt.Sprintf("%.2f", v)
}

func parseFloatOrNaN(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" || s == "NA" || s == "max" {
		return math.NaN()
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return math.NaN()
	}
	return v
}

// readProcRSSMBFromRoot sums VmRSS (in MB) for all processes under procRoot whose
// comm or cmdline matches targetSubstr. Returns math.NaN() and an error if none match.
func readProcRSSMBFromRoot(procRoot, targetSubstr string) (float64, error) {
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return math.NaN(), err
	}
	var totalKB uint64
	matched := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, err := strconv.Atoi(e.Name()); err != nil {
			continue
		}
		pdir := filepath.Join(procRoot, e.Name())
		commRaw, _ := os.ReadFile(filepath.Join(pdir, "comm"))
		comm := strings.TrimSpace(string(commRaw))
		match := strings.Contains(comm, targetSubstr)
		if !match {
			cmdRaw, _ := os.ReadFile(filepath.Join(pdir, "cmdline"))
			cmdline := strings.ReplaceAll(string(cmdRaw), "\x00", " ")
			match = strings.Contains(cmdline, targetSubstr)
		}
		if !match {
			continue
		}
		statusRaw, err := os.ReadFile(filepath.Join(pdir, "status"))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(statusRaw), "\n") {
			if strings.HasPrefix(line, "VmRSS:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					if kb, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
						totalKB += kb
						matched++
					}
				}
				break
			}
		}
	}
	if matched == 0 {
		return math.NaN(), fmt.Errorf("no process matching %q in %s", targetSubstr, procRoot)
	}
	return float64(totalKB) / 1024.0, nil
}

func sampleContainerProcRSSMB(container, procMatch string) float64 {
	script := fmt.Sprintf(`
rss_kb=0
found=0
for p in /proc/[0-9]*/comm; do
  [ -r "$p" ] || continue
  dir=${p%%/comm}
  comm=$(cat "$p" 2>/dev/null) || continue
  matched=0
  case "$comm" in
    *%s*) matched=1 ;;
  esac
  if [ "$matched" -eq 0 ] && [ -r "$dir/cmdline" ]; then
    cmd=$(tr '\000' ' ' < "$dir/cmdline" 2>/dev/null)
    case "$cmd" in
      *%s*) matched=1 ;;
    esac
  fi
  if [ "$matched" -eq 1 ]; then
    kb=$(awk '/^VmRSS:/{print $2}' "$dir/status" 2>/dev/null)
    if [ -n "$kb" ]; then
      rss_kb=$((rss_kb + kb))
      found=1
    fi
  fi
done
if [ "$found" -eq 1 ]; then
  awk "BEGIN{printf \"%%.2f\n\", $rss_kb/1024}"
else
  echo "NA"
fi
`, procMatch, procMatch)
	out, err := dockerCmd("exec", container, "sh", "-c", script)
	if err != nil {
		return math.NaN()
	}
	return parseFloatOrNaN(out)
}

func parseCgroupSampleOutput(out string) CgroupSample {
	s := newUnavailableSample()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	lineAt := func(idx int) string {
		if idx >= len(lines) {
			return "NA"
		}
		return strings.TrimSpace(lines[idx])
	}
	if v := parseFloatOrNaN(lineAt(0)); !math.IsNaN(v) {
		s.MemCurrentMB = v / (1024 * 1024)
	}
	if v := parseFloatOrNaN(lineAt(1)); !math.IsNaN(v) {
		s.MemPeakMB = v / (1024 * 1024)
	}
	if v := parseFloatOrNaN(lineAt(2)); !math.IsNaN(v) {
		s.MemMaxMB = v / (1024 * 1024)
	}
	if parts := strings.Fields(lineAt(3)); len(parts) == 2 && parts[0] != "NA" {
		if a := parseFloatOrNaN(parts[0]); !math.IsNaN(a) {
			s.AnonMB = a / (1024 * 1024)
		}
		if f := parseFloatOrNaN(parts[1]); !math.IsNaN(f) {
			s.FileMB = f / (1024 * 1024)
		}
	}
	if parts := strings.Fields(lineAt(4)); len(parts) == 3 && parts[0] != "NA" {
		h, err1 := strconv.ParseUint(parts[0], 10, 64)
		o, err2 := strconv.ParseUint(parts[1], 10, 64)
		k, err3 := strconv.ParseUint(parts[2], 10, 64)
		if err1 == nil && err2 == nil && err3 == nil {
			s.HighEvents = h
			s.OOMEvents = o
			s.OOMKillEvents = k
			s.EventsAvail = true
		}
	}
	s.MemPSISome10 = parseFloatOrNaN(lineAt(5))
	s.CPUPSISome10 = parseFloatOrNaN(lineAt(6))
	s.IOPSIMax10 = parseFloatOrNaN(lineAt(7))
	if u, err := strconv.ParseUint(lineAt(8), 10, 64); err == nil {
		s.CPUUsageUsec = u
		s.CPUUsageAvail = true
	}
	s.PGRSSMB = parseFloatOrNaN(lineAt(9))
	return s
}

func sampleContainerCgroup(container string) CgroupSample {
	script := `
cat /sys/fs/cgroup/memory.current 2>/dev/null || echo NA
cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo NA
cat /sys/fs/cgroup/memory.max 2>/dev/null || echo NA
if [ -r /sys/fs/cgroup/memory.stat ]; then
  awk '/^anon /{a=$2} /^file /{f=$2} END{printf "%d %d\n", a+0, f+0}' /sys/fs/cgroup/memory.stat
else
  echo "NA NA"
fi
if [ -r /sys/fs/cgroup/memory.events ]; then
  awk '/^high /{h=$2} /^oom /{o=$2} /^oom_kill /{k=$2} END{printf "%d %d %d\n", h+0, o+0, k+0}' /sys/fs/cgroup/memory.events
else
  echo "NA NA NA"
fi
if [ -r /sys/fs/cgroup/memory.pressure ]; then
  awk '/^some /{for(i=1;i<=NF;i++) if($i~/^avg10=/) {split($i,x,"="); print x[2]; found=1}} END{if(!found) print "NA"}' /sys/fs/cgroup/memory.pressure
else
  echo NA
fi
if [ -r /sys/fs/cgroup/cpu.pressure ]; then
  awk '/^some /{for(i=1;i<=NF;i++) if($i~/^avg10=/) {split($i,x,"="); print x[2]; found=1}} END{if(!found) print "NA"}' /sys/fs/cgroup/cpu.pressure
else
  echo NA
fi
if [ -r /sys/fs/cgroup/io.pressure ]; then
  awk '/^some /{for(i=1;i<=NF;i++) if($i~/^avg10=/) {split($i,x,"="); print x[2]; found=1}} END{if(!found) print "NA"}' /sys/fs/cgroup/io.pressure
else
  echo NA
fi
if [ -r /sys/fs/cgroup/cpu.stat ]; then
  awk '/^usage_usec /{print $2; found=1} END{if(!found) print "NA"}' /sys/fs/cgroup/cpu.stat
else
  echo NA
fi
rss_kb=0
found=0
for p in /proc/[0-9]*/comm; do
  [ -r "$p" ] || continue
  dir=${p%/comm}
  comm=$(cat "$p" 2>/dev/null) || continue
  matched=0
  case "$comm" in
    *postgres*) matched=1 ;;
  esac
  if [ "$matched" -eq 0 ] && [ -r "$dir/cmdline" ]; then
    cmd=$(tr '\000' ' ' < "$dir/cmdline" 2>/dev/null)
    case "$cmd" in
      *postgres*) matched=1 ;;
    esac
  fi
  if [ "$matched" -eq 1 ]; then
    kb=$(awk '/^VmRSS:/{print $2}' "$dir/status" 2>/dev/null)
    if [ -n "$kb" ]; then
      rss_kb=$((rss_kb + kb))
      found=1
    fi
  fi
done
if [ "$found" -eq 1 ]; then
  awk "BEGIN{printf \"%.2f\n\", $rss_kb/1024}"
else
  echo "NA"
fi
`
	out, err := dockerCmd("exec", container, "sh", "-c", script)
	if err != nil {
		return newUnavailableSample()
	}
	s := parseCgroupSampleOutput(out)
	s.GatewayRSSMB = sampleContainerProcRSSMB("air-p21-gateway", "air-gateway")
	s.PageserverMB = sampleContainerProcRSSMB("air-p21-pageserver", "air-pageserver")
	s.ReadEngineRSSMB = sampleContainerProcRSSMB("air-p21-read-engine", "air-read-engine")
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
