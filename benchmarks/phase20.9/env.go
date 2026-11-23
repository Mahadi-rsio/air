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
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/jackc/pgx/v5"
)

const (
	defaultInternalToken = "phase209-bench-token"
	defaultAppPassword   = "bench_app_pw_209"
	defaultPGBinDir      = "/usr/lib/postgresql/17/bin"
	defaultCgroupRoot    = "/sys/fs/cgroup/air_phase209"
)

type GatewayProfile struct {
	Name                   string
	WriteGovernorEnabled   bool
	WriteAdaptiveEnabled   bool
	WriteLazyLSNEnabled    bool
	WriteMaxGlobal         int
	WriteMinGlobal         int
	WriteMaxPerTenant      int
	WriteQueueMax          int
	WriteQueueMaxBytes     int64
	WriteQueueTimeout      time.Duration
	WriteControllerIntv    time.Duration
	WriteBufferGlobalMax   int64
	WriteBufferSessionMax  int64
	PoolMaxBackends        int
	ReadEnginePort         int
	ReadEngineHost         string
}

func ProfileNoGovernor() GatewayProfile {
	return GatewayProfile{
		Name:                  "A_no_governor",
		WriteGovernorEnabled:  false,
		WriteAdaptiveEnabled:  false,
		WriteLazyLSNEnabled:   false,
		WriteMaxGlobal:        32,
		WriteMinGlobal:        2,
		WriteMaxPerTenant:     32,
		WriteQueueMax:         256,
		WriteQueueMaxBytes:    64 * 1024 * 1024,
		WriteQueueTimeout:     5 * time.Second,
		WriteControllerIntv:   500 * time.Millisecond,
		WriteBufferGlobalMax:  0,
		WriteBufferSessionMax: 32 * 1024 * 1024,
		PoolMaxBackends:       32,
	}
}

func ProfileStaticGovernor() GatewayProfile {
	return GatewayProfile{
		Name:                  "B_static_governor",
		WriteGovernorEnabled:  true,
		WriteAdaptiveEnabled:  false,
		WriteLazyLSNEnabled:   false,
		WriteMaxGlobal:        16,
		WriteMinGlobal:        2,
		WriteMaxPerTenant:     8,
		WriteQueueMax:         256,
		WriteQueueMaxBytes:    64 * 1024 * 1024,
		WriteQueueTimeout:     5 * time.Second,
		WriteControllerIntv:   500 * time.Millisecond,
		WriteBufferGlobalMax:  0,
		WriteBufferSessionMax: 32 * 1024 * 1024,
		PoolMaxBackends:       32,
	}
}

func ProfileAdaptiveGovernor() GatewayProfile {
	return GatewayProfile{
		Name:                  "C_adaptive_governor",
		WriteGovernorEnabled:  true,
		WriteAdaptiveEnabled:  true,
		WriteLazyLSNEnabled:   false,
		WriteMaxGlobal:        16,
		WriteMinGlobal:        2,
		WriteMaxPerTenant:     8,
		WriteQueueMax:         256,
		WriteQueueMaxBytes:    64 * 1024 * 1024,
		WriteQueueTimeout:     5 * time.Second,
		WriteControllerIntv:   500 * time.Millisecond,
		WriteBufferGlobalMax:  0,
		WriteBufferSessionMax: 32 * 1024 * 1024,
		PoolMaxBackends:       32,
	}
}

func ProfileGovBufLazyLSN() GatewayProfile {
	return GatewayProfile{
		Name:                  "D_gov_buf_lazylsn",
		WriteGovernorEnabled:  true,
		WriteAdaptiveEnabled:  false,
		WriteLazyLSNEnabled:   true,
		WriteMaxGlobal:        16,
		WriteMinGlobal:        2,
		WriteMaxPerTenant:     8,
		WriteQueueMax:         256,
		WriteQueueMaxBytes:    64 * 1024 * 1024,
		WriteQueueTimeout:     5 * time.Second,
		WriteControllerIntv:   500 * time.Millisecond,
		WriteBufferGlobalMax:  64 * 1024 * 1024,
		WriteBufferSessionMax: 8 * 1024 * 1024,
		PoolMaxBackends:       32,
	}
}

func ProfileFullPhase20() GatewayProfile {
	return GatewayProfile{
		Name:                  "E_full_phase20",
		WriteGovernorEnabled:  true,
		WriteAdaptiveEnabled:  true,
		WriteLazyLSNEnabled:   true,
		WriteMaxGlobal:        16,
		WriteMinGlobal:        2,
		WriteMaxPerTenant:     8,
		WriteQueueMax:         256,
		WriteQueueMaxBytes:    64 * 1024 * 1024,
		WriteQueueTimeout:     5 * time.Second,
		WriteControllerIntv:   500 * time.Millisecond,
		WriteBufferGlobalMax:  64 * 1024 * 1024,
		WriteBufferSessionMax: 8 * 1024 * 1024,
		PoolMaxBackends:       32,
	}
}

type CgroupSample struct {
	Timestamp          time.Time `json:"timestamp"`
	MemoryCurrentBytes uint64    `json:"memory_current_bytes"`
	MemoryPeakBytes    uint64    `json:"memory_peak_bytes"`
	MemoryHighBytes    uint64    `json:"memory_high_bytes"`
	MemoryMaxBytes     uint64    `json:"memory_max_bytes"`
	MemoryAnonBytes    uint64    `json:"memory_anon_bytes"`
	MemoryFileBytes    uint64    `json:"memory_file_bytes"`
	MemorySlabBytes    uint64    `json:"memory_slab_bytes"`
	MemoryHighEvents   uint64    `json:"memory_high_events"`
	MemoryMaxEvents    uint64    `json:"memory_max_events"`
	MemoryOOMEvents    uint64    `json:"memory_oom_events"`
	MemoryOOMKill      uint64    `json:"memory_oom_kill"`
	MemoryPSISomeAvg10 float64   `json:"memory_psi_some_avg10"`
	MemoryPSIFullAvg10 float64   `json:"memory_psi_full_avg10"`
	CPUUsageUsec       uint64    `json:"cpu_usage_usec"`
	CPUThrottledUsec   uint64    `json:"cpu_throttled_usec"`
	CPUPSISomeAvg10    float64   `json:"cpu_psi_some_avg10"`
	IOPSIMaxAvg10      float64   `json:"io_psi_max_avg10"`
	PGRSSBytes         uint64    `json:"pg_rss_bytes"`
	GatewayRSSBytes    uint64    `json:"gateway_rss_bytes"`
}

type PGStatsSnapshot struct {
	ActiveConns       int     `json:"active_conns"`
	ActiveTx          int     `json:"active_tx"`
	LockWaits         int     `json:"lock_waits"`
	Deadlocks         int64   `json:"deadlocks"`
	XactCommit        int64   `json:"xact_commit"`
	XactRollback      int64   `json:"xact_rollback"`
	WALBytes          int64   `json:"wal_bytes"`
	WALWriteTimeMs    float64 `json:"wal_write_time_ms"`
	WALSyncTimeMs     float64 `json:"wal_sync_time_ms"`
	CheckpointsTimed  int64   `json:"checkpoints_timed"`
	CheckpointsReq    int64   `json:"checkpoints_req"`
	BuffersCheckpoint int64   `json:"buffers_checkpoint"`
	BuffersClean      int64   `json:"buffers_clean"`
}

type GovSnapshot struct {
	Enabled              bool   `json:"Enabled"`
	ActiveWrites         int64  `json:"ActiveWrites"`
	QueueDepth           int64  `json:"QueueDepth"`
	QueueBytes           int64  `json:"QueueBytes"`
	EffectiveGlobalLimit int    `json:"EffectiveGlobalLimit"`
	MaxGlobal            int    `json:"MaxGlobal"`
	MinGlobal            int    `json:"MinGlobal"`
	MaxPerTenant         int    `json:"MaxPerTenant"`
	ControllerState      int    `json:"ControllerState"`
	CompletedTotal       uint64 `json:"CompletedTotal"`
	RejectedTotal        uint64 `json:"RejectedTotal"`
	CancelledTotal       uint64 `json:"CancelledTotal"`
	TimeoutTotal         uint64 `json:"TimeoutTotal"`
	ErrorsTotal          uint64 `json:"ErrorsTotal"`
	OpenTransactions     int64  `json:"OpenTransactions"`
	QueueWaitP50Us       uint64 `json:"QueueWaitP50Us"`
	QueueWaitP95Us       uint64 `json:"QueueWaitP95Us"`
	QueueWaitP99Us       uint64 `json:"QueueWaitP99Us"`
	ExecP50Us            uint64 `json:"ExecP50Us"`
	ExecP95Us            uint64 `json:"ExecP95Us"`
	ExecP99Us            uint64 `json:"ExecP99Us"`
}

type BenchEnv struct {
	WorkDir     string
	GatewayBin  string
	CgroupRoot  string
	Agent       *cell.Agent
	Tenants     map[string]*cell.TenantRuntime
	GatewayPort int
	AdminPort   int
	gwCmd       *exec.Cmd
	gwLog       *os.File
	telCancel   context.CancelFunc
	mu          sync.Mutex
}

func findFreePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func NewBenchEnv(workDir, gatewayBin, cgroupRoot string, limits cell.CgroupLimits) (*BenchEnv, error) {
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return nil, err
	}
	ag, err := cell.NewAgent(workDir, defaultPGBinDir, nil)
	if err != nil {
		return nil, fmt.Errorf("NewAgent: %w", err)
	}
	if cgroupRoot != "" {
		ag.SetCgroupRoot(cgroupRoot)
	}
	ag.SetCgroupLimits(limits)
	return &BenchEnv{
		WorkDir:    workDir,
		GatewayBin: gatewayBin,
		CgroupRoot: cgroupRoot,
		Agent:      ag,
		Tenants:    make(map[string]*cell.TenantRuntime),
	}, nil
}

func (e *BenchEnv) ProvisionTenant(tenantID string) (*cell.TenantRuntime, error) {
	if _, err := e.Agent.ClaimCluster(tenantID); err != nil {
		return nil, fmt.Errorf("ClaimCluster(%s): %w", tenantID, err)
	}
	rt, err := e.Agent.ActivateCluster(tenantID, defaultAppPassword, 1)
	if err != nil {
		return nil, fmt.Errorf("ActivateCluster(%s): %w", tenantID, err)
	}
	// Disable WAL archiving spool accumulation during heavy benchmarks so 4GB disk doesn't fill,
	// while keeping wal_level=replica, fsync=on, full_page_writes=on, synchronous_commit=on.
	e.mu.Lock()
	e.Tenants[tenantID] = rt
	e.mu.Unlock()
	return rt, nil
}

func (e *BenchEnv) StartGateway(prof GatewayProfile, telTenantID string) error {
	e.StopGateway()
	gwPort, err := findFreePort()
	if err != nil {
		return err
	}
	admPort, err := findFreePort()
	if err != nil {
		return err
	}
	e.GatewayPort = gwPort
	e.AdminPort = admPort

	logPath := filepath.Join(e.WorkDir, fmt.Sprintf("gateway_%s.log", prof.Name))
	logF, err := os.Create(logPath)
	if err != nil {
		return err
	}
	e.gwLog = logF

	cmd := exec.Command(e.GatewayBin)
	cmd.Stdout = logF
	cmd.Stderr = logF
	env := append(os.Environ(),
		fmt.Sprintf("AIR_GATEWAY_ADDR=127.0.0.1:%d", gwPort),
		fmt.Sprintf("AIR_GATEWAY_ADMIN_ADDR=127.0.0.1:%d", admPort),
		"AIR_GATEWAY_ADMIN_GRPC_ADDR=",
		"AIR_CONTROL_PLANE_URL=http://127.0.0.1:1",
		"AIR_DB_GRPC_ADDR=127.0.0.1:1",
		"AIR_META_DSN=",
		"AIR_INTERNAL_TOKEN="+defaultInternalToken,
		"AIR_POOL_MODE=true",
		fmt.Sprintf("AIR_POOL_MAX_BACKENDS=%d", prof.PoolMaxBackends),
		"AIR_MAX_CLIENT_CONNS=1024",
		"AIR_MAX_TENANT_CLIENT_CONNS=512",
		fmt.Sprintf("AIR_READ_ENGINE_HOST=%s", prof.ReadEngineHost),
		fmt.Sprintf("AIR_READ_ENGINE_PORT=%d", prof.ReadEnginePort),
		fmt.Sprintf("AIR_WRITE_GOVERNOR_ENABLED=%v", prof.WriteGovernorEnabled),
		fmt.Sprintf("AIR_WRITE_ADAPTIVE_ENABLED=%v", prof.WriteAdaptiveEnabled),
		fmt.Sprintf("AIR_WRITE_LAZY_LSN_ENABLED=%v", prof.WriteLazyLSNEnabled),
		fmt.Sprintf("AIR_WRITE_MAX_GLOBAL=%d", prof.WriteMaxGlobal),
		fmt.Sprintf("AIR_WRITE_MIN_GLOBAL=%d", prof.WriteMinGlobal),
		fmt.Sprintf("AIR_WRITE_MAX_PER_TENANT=%d", prof.WriteMaxPerTenant),
		fmt.Sprintf("AIR_WRITE_QUEUE_MAX=%d", prof.WriteQueueMax),
		fmt.Sprintf("AIR_WRITE_QUEUE_MAX_BYTES=%d", prof.WriteQueueMaxBytes),
		fmt.Sprintf("AIR_WRITE_QUEUE_TIMEOUT=%s", prof.WriteQueueTimeout),
		fmt.Sprintf("AIR_WRITE_CONTROLLER_INTERVAL=%s", prof.WriteControllerIntv),
		fmt.Sprintf("AIR_WRITE_BUFFER_GLOBAL_MAX=%d", prof.WriteBufferGlobalMax),
		fmt.Sprintf("AIR_WRITE_BUFFER_SESSION_MAX=%d", prof.WriteBufferSessionMax),
		fmt.Sprintf("AIR_METERING_LOG_DIR=%s", filepath.Join(e.WorkDir, "metering")),
	)
	cmd.Env = env
	if err := cmd.Start(); err != nil {
		logF.Close()
		return fmt.Errorf("start air-gateway: %w", err)
	}
	e.gwCmd = cmd

	// Wait for /healthz
	healthURL := fmt.Sprintf("http://127.0.0.1:%d/healthz", admPort)
	deadline := time.Now().Add(10 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				ready = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ready {
		e.StopGateway()
		return fmt.Errorf("air-gateway failed to become healthy on %s", healthURL)
	}

	// Register all provisioned tenants via POST /v1/namespaces
	e.mu.Lock()
	tenantsCopy := make(map[string]*cell.TenantRuntime, len(e.Tenants))
	for k, v := range e.Tenants {
		tenantsCopy[k] = v
	}
	e.mu.Unlock()
	for tid, rt := range tenantsCopy {
		if err := e.RegisterTenant(tid, rt, prof.ReadEngineHost, prof.ReadEnginePort); err != nil {
			e.StopGateway()
			return err
		}
	}

	// Start real Cell-to-Gateway telemetry publisher if requested
	if telTenantID != "" || e.CgroupRoot != "" {
		tctx, cancel := context.WithCancel(context.Background())
		e.telCancel = cancel
		pub := cell.NewTelemetryPublisher(e.Agent, fmt.Sprintf("http://127.0.0.1:%d", admPort), defaultInternalToken, telTenantID, 500*time.Millisecond)
		pub.Start(tctx)
	}
	return nil
}

func (e *BenchEnv) RegisterTenant(tenantID string, rt *cell.TenantRuntime, readHost string, readPort int) error {
	payload := map[string]any{
		"tenant_id":        tenantID,
		"state":            "ACTIVE",
		"host":             "127.0.0.1",
		"port":             rt.Port,
		"sock_dir":         rt.SockDir,
		"app_password":     defaultAppPassword,
		"read_engine_host": readHost,
		"read_engine_port": readPort,
	}
	b, _ := json.Marshal(payload)
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/v1/namespaces/%s", e.AdminPort, tenantID), bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Air-Internal-Token", defaultInternalToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("register tenant %s failed HTTP %d: %s", tenantID, resp.StatusCode, string(body))
	}
	return nil
}

func (e *BenchEnv) StopGateway() {
	if e.telCancel != nil {
		e.telCancel()
		e.telCancel = nil
	}
	if e.gwCmd != nil && e.gwCmd.Process != nil {
		_ = e.gwCmd.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- e.gwCmd.Wait() }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = e.gwCmd.Process.Kill()
			<-done
		}
		e.gwCmd = nil
	}
	if e.gwLog != nil {
		_ = e.gwLog.Close()
		e.gwLog = nil
	}
}

func (e *BenchEnv) StopTelemetryPublisher() {
	if e.telCancel != nil {
		e.telCancel()
		e.telCancel = nil
	}
}

func (e *BenchEnv) GatewayDSN(tenantID string) string {
	return fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", defaultAppPassword, e.GatewayPort, tenantID)
}

func (e *BenchEnv) DirectPGDSN(tenantID string) string {
	e.mu.Lock()
	rt := e.Tenants[tenantID]
	e.mu.Unlock()
	return fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", defaultAppPassword, rt.Port, tenantID)
}

func (e *BenchEnv) SuperuserDSN(tenantID string) string {
	e.mu.Lock()
	rt := e.Tenants[tenantID]
	e.mu.Unlock()
	return fmt.Sprintf("host=%s port=%d user=postgres dbname=%s", rt.SockDir, rt.Port, tenantID)
}

func (e *BenchEnv) Cleanup() {
	e.StopGateway()
	e.mu.Lock()
	tids := make([]string, 0, len(e.Tenants))
	for tid := range e.Tenants {
		tids = append(tids, tid)
	}
	e.mu.Unlock()
	for _, tid := range tids {
		_ = e.Agent.StopAndPurgeTenant(tid)
	}
	e.Agent.Close()
	_ = os.RemoveAll(e.WorkDir)
}

func (e *BenchEnv) FetchGovernorSnapshot() (GovSnapshot, error) {
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/internal/v1/write-governor", e.AdminPort), nil)
	req.Header.Set("X-Air-Internal-Token", defaultInternalToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return GovSnapshot{}, err
	}
	defer resp.Body.Close()
	var gs GovSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&gs); err != nil {
		return GovSnapshot{}, err
	}
	return gs, nil
}

func (e *BenchEnv) FetchPrometheusMetrics() (string, error) {
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/metrics", e.AdminPort), nil)
	req.Header.Set("X-Air-Internal-Token", defaultInternalToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

func (e *BenchEnv) SampleCgroup(tenantID string) CgroupSample {
	var snap cell.ResourceSnapshot
	cgDir := e.CgroupRoot
	if tenantID != "" {
		snap = e.Agent.CollectTenantTelemetry(tenantID)
		cgDir = filepath.Join(e.CgroupRoot, "air_tenant_"+tenantID)
	} else {
		snap = e.Agent.CollectCellTelemetry()
	}
	s := CgroupSample{
		Timestamp:          snap.Timestamp,
		MemoryCurrentBytes: snap.MemoryCurrentBytes,
		MemoryHighBytes:    snap.MemoryHighBytes,
		MemoryMaxBytes:     snap.MemoryMaxBytes,
		MemoryHighEvents:   snap.MemoryHighEvents,
		MemoryMaxEvents:    snap.MemoryMaxEvents,
		MemoryOOMEvents:    snap.MemoryOOMEvents,
		MemoryPSISomeAvg10: snap.MemoryPSISomeAvg10,
		MemoryPSIFullAvg10: snap.MemoryPSIFullAvg10,
		CPUUsageUsec:       snap.CPUUsageUsec,
		CPUThrottledUsec:   snap.CPUThrottledUsec,
		CPUPSISomeAvg10:    snap.CPUPSISomeAvg10,
		IOPSIMaxAvg10:      snap.IOPSIMaxAvg10,
	}
	if raw, err := os.ReadFile(filepath.Join(cgDir, "memory.peak")); err == nil {
		s.MemoryPeakBytes, _ = strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	}
	if raw, err := os.ReadFile(filepath.Join(cgDir, "memory.events")); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			f := strings.Fields(line)
			if len(f) == 2 && f[0] == "oom_kill" {
				s.MemoryOOMKill, _ = strconv.ParseUint(f[1], 10, 64)
			}
		}
	}
	if raw, err := os.ReadFile(filepath.Join(cgDir, "memory.stat")); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			f := strings.Fields(line)
			if len(f) != 2 {
				continue
			}
			v, _ := strconv.ParseUint(f[1], 10, 64)
			switch f[0] {
			case "anon":
				s.MemoryAnonBytes = v
			case "file":
				s.MemoryFileBytes = v
			case "slab":
				s.MemorySlabBytes = v
			}
		}
	}
	// Measure PostgreSQL process tree RSS from cgroup.procs
	if raw, err := os.ReadFile(filepath.Join(cgDir, "cgroup.procs")); err == nil {
		for _, field := range strings.Fields(string(raw)) {
			if pid, err := strconv.Atoi(field); err == nil && pid > 0 {
				s.PGRSSBytes += readProcRSSBytes(pid)
			}
		}
	}
	if e.gwCmd != nil && e.gwCmd.Process != nil {
		s.GatewayRSSBytes = readProcRSSBytes(e.gwCmd.Process.Pid)
	}
	return s
}

func readProcRSSBytes(pid int) uint64 {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, _ := strconv.ParseUint(fields[1], 10, 64)
				return kb * 1024
			}
		}
	}
	return 0
}

func (e *BenchEnv) SamplePGStats(ctx context.Context, tenantID string) (PGStatsSnapshot, error) {
	conn, err := pgx.Connect(ctx, e.SuperuserDSN(tenantID))
	if err != nil {
		return PGStatsSnapshot{}, err
	}
	defer conn.Close(ctx)

	var st PGStatsSnapshot
	_ = conn.QueryRow(ctx, `
		SELECT
			(SELECT count(*)::int FROM pg_stat_activity WHERE datname = current_database()),
			(SELECT count(*)::int FROM pg_stat_activity WHERE datname = current_database() AND state IN ('active','idle in transaction')),
			(SELECT count(*)::int FROM pg_locks WHERE NOT granted),
			(SELECT COALESCE(deadlocks,0) FROM pg_stat_database WHERE datname = current_database()),
			(SELECT COALESCE(xact_commit,0) FROM pg_stat_database WHERE datname = current_database()),
			(SELECT COALESCE(xact_rollback,0) FROM pg_stat_database WHERE datname = current_database()),
			(SELECT COALESCE(wal_bytes,0)::bigint FROM pg_stat_wal),
			(SELECT COALESCE(wal_write_time,0)::float8 FROM pg_stat_wal),
			(SELECT COALESCE(wal_sync_time,0)::float8 FROM pg_stat_wal),
			(SELECT COALESCE(num_timed,0)::bigint FROM pg_stat_checkpointer),
			(SELECT COALESCE(num_requested,0)::bigint FROM pg_stat_checkpointer),
			(SELECT COALESCE(buffers_written,0)::bigint FROM pg_stat_checkpointer),
			(SELECT COALESCE(buffers_clean,0)::bigint FROM pg_stat_bgwriter)
	`).Scan(
		&st.ActiveConns, &st.ActiveTx, &st.LockWaits,
		&st.Deadlocks, &st.XactCommit, &st.XactRollback,
		&st.WALBytes, &st.WALWriteTimeMs, &st.WALSyncTimeMs,
		&st.CheckpointsTimed, &st.CheckpointsReq, &st.BuffersCheckpoint, &st.BuffersClean,
	)
	return st, nil
}

func (e *BenchEnv) CleanWALSpool(tenantID string) {
	e.mu.Lock()
	rt := e.Tenants[tenantID]
	e.mu.Unlock()
	if rt != nil && rt.WALSpoolDir != "" {
		entries, _ := os.ReadDir(rt.WALSpoolDir)
		for _, ent := range entries {
			_ = os.Remove(filepath.Join(rt.WALSpoolDir, ent.Name()))
		}
	}
}

type LatencySummary struct {
	Count int64   `json:"count"`
	Errs  int64   `json:"errors"`
	TPS   float64 `json:"tps"`
	P50Ms float64 `json:"p50_ms"`
	P95Ms float64 `json:"p95_ms"`
	P99Ms float64 `json:"p99_ms"`
	MaxMs float64 `json:"max_ms"`
	AvgMs float64 `json:"avg_ms"`
}

func summarizeLatencies(latsUs []int64, errs int64, elapsed time.Duration) LatencySummary {
	n := len(latsUs)
	if n == 0 {
		return LatencySummary{Errs: errs}
	}
	sorted := make([]int64, n)
	copy(sorted, latsUs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum int64
	for _, v := range sorted {
		sum += v
	}
	pct := func(p float64) float64 {
		idx := int(float64(n-1) * p)
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return float64(sorted[idx]) / 1000.0
	}
	sec := elapsed.Seconds()
	if sec <= 0 {
		sec = 1e-6
	}
	return LatencySummary{
		Count: int64(n),
		Errs:  errs,
		TPS:   float64(n) / sec,
		P50Ms: pct(0.50),
		P95Ms: pct(0.95),
		P99Ms: pct(0.99),
		MaxMs: float64(sorted[n-1]) / 1000.0,
		AvgMs: float64(sum) / float64(n) / 1000.0,
	}
}
