package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	HTTPAddr         string
	ListenHost       string
	PGPort           int
	DataDir          string
	PGBinDir         string
	AirRoSO          string
	PageserverSocket string
	SharedBuffers    string
	PrimaryConnInfo  string
	TenantID         string
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func loadConfig(args []string) Config {
	fs := flag.NewFlagSet("air-read-engine", flag.ContinueOnError)
	cfg := Config{}
	fs.StringVar(&cfg.HTTPAddr, "http-addr", envOr("AIR_READ_ENGINE_HTTP_ADDR", "127.0.0.1:8082"), "HTTP admin/health listen address")
	fs.StringVar(&cfg.ListenHost, "listen-host", envOr("AIR_READ_ENGINE_HOST", "127.0.0.1"), "PostgreSQL listen address")
	fs.IntVar(&cfg.PGPort, "pg-port", envIntOr("AIR_READ_ENGINE_PG_PORT", 5433), "Base PostgreSQL wire port for read-only engine")
	fs.StringVar(&cfg.DataDir, "data-dir", envOr("AIR_READ_ENGINE_DATA_DIR", "/tmp/air-read-engine"), "Read engine data directory")
	fs.StringVar(&cfg.PGBinDir, "pg-bin-dir", envOr("AIR_PG_BIN_DIR", "/usr/lib/postgresql/17/bin"), "PostgreSQL 17 binary directory")
	fs.StringVar(&cfg.AirRoSO, "air-ro-so", envOr("AIR_RO_SO_PATH", ""), "Path to air_ro.so extension")
	fs.StringVar(&cfg.PageserverSocket, "pageserver-socket", envOr("AIR_PAGESERVER_SOCKET", ""), "Optional Pageserver Unix socket path")
	fs.StringVar(&cfg.SharedBuffers, "shared-buffers", envOr("AIR_READ_ENGINE_SHARED_BUFFERS", "32MB"), "PostgreSQL shared_buffers setting")
	fs.StringVar(&cfg.PrimaryConnInfo, "primary-conninfo", envOr("AIR_READ_ENGINE_PRIMARY_CONNINFO", ""), "Optional primary_conninfo for hot_standby streaming")
	fs.StringVar(&cfg.TenantID, "tenant-id", envOr("AIR_READ_ENGINE_TENANT_ID", ""), "Optional default tenant ID to initialize and start on boot")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("parse flags: %v", err)
	}
	if cfg.AirRoSO == "" {
		for _, cand := range []string{
			"/usr/lib/postgresql/17/lib/air_ro.so",
			"extensions/air_ro/air_ro.so",
		} {
			if _, err := os.Stat(cand); err == nil {
				abs, _ := filepath.Abs(cand)
				cfg.AirRoSO = abs
				break
			}
		}
	}
	return cfg
}

type TenantReadInstance struct {
	TenantID string `json:"tenant_id"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	SockDir  string `json:"sock_dir"`
	DataDir  string `json:"data_dir"`
	State    string `json:"state"`
}

type ReadEngine struct {
	cfg      Config
	mu       sync.Mutex
	nextPort int
	tenants  map[string]*TenantReadInstance
}

func NewReadEngine(cfg Config) (*ReadEngine, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir data-dir: %w", err)
	}
	return &ReadEngine{
		cfg:      cfg,
		nextPort: cfg.PGPort,
		tenants:  make(map[string]*TenantReadInstance),
	}, nil
}

func (re *ReadEngine) StartTenant(tenantID, primaryConnInfo string) (*TenantReadInstance, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant_id is required")
	}
	if primaryConnInfo == "" {
		primaryConnInfo = re.cfg.PrimaryConnInfo
	}
	re.mu.Lock()
	if inst, ok := re.tenants[tenantID]; ok && inst.State == "ACTIVE" {
		re.mu.Unlock()
		return inst, nil
	}
	port := re.nextPort
	re.nextPort++
	re.mu.Unlock()

	tRoot := filepath.Join(re.cfg.DataDir, "tenants", tenantID)
	pgData := filepath.Join(tRoot, "pgdata")
	sockDir := filepath.Join(tRoot, "sock")
	if err := os.MkdirAll(sockDir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir sockDir: %w", err)
	}

	if _, err := os.Stat(filepath.Join(pgData, "PG_VERSION")); os.IsNotExist(err) {
		if primaryConnInfo != "" {
			// Clone real data, roles, and passwords from the Cell primary as a streaming standby.
			bbCmd := exec.Command(filepath.Join(re.cfg.PGBinDir, "pg_basebackup"),
				"-D", pgData,
				"-d", primaryConnInfo,
				"-R", "-X", "stream", "-c", "fast",
			)
			if out, err := bbCmd.CombinedOutput(); err != nil {
				return nil, fmt.Errorf("pg_basebackup failed: %w: %s", err, string(out))
			}
			if err := re.configurePG(pgData, sockDir, port, tenantID); err != nil {
				return nil, err
			}
		} else {
			initCmd := exec.Command(filepath.Join(re.cfg.PGBinDir, "initdb"),
				"-D", pgData, "-U", "postgres", "--auth=trust", "--no-sync")
			if out, err := initCmd.CombinedOutput(); err != nil {
				return nil, fmt.Errorf("initdb failed: %w: %s", err, string(out))
			}
			if err := re.configurePG(pgData, sockDir, port, tenantID); err != nil {
				return nil, err
			}
			// Start briefly with air_ro.enabled = off to create tenant database & app_user if needed.
			if err := re.pgCtl(pgData, tRoot, "start"); err != nil {
				return nil, fmt.Errorf("bootstrap start: %w", err)
			}
			_ = re.runPSQL(sockDir, port, "postgres", fmt.Sprintf(
				"CREATE ROLE app_user LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION; CREATE DATABASE %q OWNER app_user;",
				tenantID))
			_ = re.pgCtl(pgData, tRoot, "stop")
		}
	}

	// Enforce read-only mode in postgresql.conf before starting for serving.
	if err := re.enableReadOnlyConf(pgData, primaryConnInfo); err != nil {
		return nil, err
	}
	if primaryConnInfo != "" {
		_ = os.WriteFile(filepath.Join(pgData, "standby.signal"), []byte(""), 0o600)
	}
	if err := re.pgCtl(pgData, tRoot, "start"); err != nil {
		return nil, fmt.Errorf("start read-only pg: %w", err)
	}

	inst := &TenantReadInstance{
		TenantID: tenantID,
		Host:     re.cfg.ListenHost,
		Port:     port,
		SockDir:  sockDir,
		DataDir:  pgData,
		State:    "ACTIVE",
	}
	re.mu.Lock()
	re.tenants[tenantID] = inst
	re.mu.Unlock()
	return inst, nil
}

func (re *ReadEngine) StopTenant(tenantID string) error {
	re.mu.Lock()
	inst, ok := re.tenants[tenantID]
	if !ok {
		re.mu.Unlock()
		return nil
	}
	inst.State = "STOPPED"
	re.mu.Unlock()
	return re.pgCtl(inst.DataDir, filepath.Dir(inst.DataDir), "stop")
}

func (re *ReadEngine) StopAll() {
	re.mu.Lock()
	ids := make([]string, 0, len(re.tenants))
	for id, inst := range re.tenants {
		if inst.State == "ACTIVE" {
			ids = append(ids, id)
		}
	}
	re.mu.Unlock()
	for _, id := range ids {
		_ = re.StopTenant(id)
	}
}

func (re *ReadEngine) configurePG(pgData, sockDir string, port int, tenantID string) error {
	confPath := filepath.Join(pgData, "postgresql.conf")
	var preload []string
	if re.cfg.AirRoSO != "" {
		preload = append(preload, re.cfg.AirRoSO)
	}
	useSMGR := re.cfg.PageserverSocket != ""
	if useSMGR {
		preload = append(preload, "air_smgr")
	}
	extra := fmt.Sprintf(`
# AIR Read Engine base config
port = %d
listen_addresses = '%s'
unix_socket_directories = '%s'
shared_buffers = %s
fsync = off
autovacuum = off
hot_standby = on
`, port, re.cfg.ListenHost, sockDir, re.cfg.SharedBuffers)
	if len(preload) > 0 {
		extra += fmt.Sprintf("shared_preload_libraries = '%s'\nair_ro.enabled = off\n", strings.Join(preload, ","))
	}
	if useSMGR {
		extra += fmt.Sprintf("air_smgr.pageserver_conn = '%s'\nair_smgr.tenant_id = '%s'\nair_smgr.read_only = on\n",
			re.cfg.PageserverSocket, tenantID)
	}
	f, err := os.OpenFile(confPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(extra)
	return err
}

func (re *ReadEngine) enableReadOnlyConf(pgData, primaryConnInfo string) error {
	confPath := filepath.Join(pgData, "postgresql.conf")
	raw, err := os.ReadFile(confPath)
	if err != nil {
		return err
	}
	content := strings.ReplaceAll(string(raw), "air_ro.enabled = off", "air_ro.enabled = on")
	if !strings.Contains(content, "default_transaction_read_only = on") {
		content += "\ndefault_transaction_read_only = on\n"
	}
	if primaryConnInfo != "" && !strings.Contains(content, "primary_conninfo") {
		content += fmt.Sprintf("primary_conninfo = '%s'\n", primaryConnInfo)
	}
	return os.WriteFile(confPath, []byte(content), 0o644)
}

func (re *ReadEngine) pgCtl(pgData, logRoot, action string) error {
	args := []string{"-D", pgData, "-w"}
	if action == "start" {
		args = append(args, "-l", filepath.Join(logRoot, "read-engine-pg.log"), "start")
	} else {
		args = append(args, "-m", "fast", action)
	}
	cmd := exec.Command(filepath.Join(re.cfg.PGBinDir, "pg_ctl"), args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("pg_ctl %s: %w: %s", action, err, string(out))
	}
	return nil
}

func (re *ReadEngine) runPSQL(sockDir string, port int, db, sql string) error {
	cmd := exec.Command(filepath.Join(re.cfg.PGBinDir, "psql"),
		"-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", "-d", db,
		"-v", "ON_ERROR_STOP=1", "-At", "-c", sql)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("psql: %w: %s", err, string(out))
	}
	return nil
}

func (re *ReadEngine) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":     "ok",
			"service":    "air-read-engine",
			"air_ro_so":  re.cfg.AirRoSO,
			"pg_port":    re.cfg.PGPort,
		})
	})
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		re.mu.Lock()
		list := make([]*TenantReadInstance, 0, len(re.tenants))
		for _, inst := range re.tenants {
			list = append(list, inst)
		}
		re.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"tenants": list})
	})
	mux.HandleFunc("/v1/tenants/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/v1/tenants/")
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if len(parts) < 2 {
			http.Error(w, "invalid path", http.StatusBadRequest)
			return
		}
		tenantID, action := parts[0], parts[1]
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && action == "start":
			var body struct {
				PrimaryConnInfo string `json:"primary_conninfo"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			inst, err := re.StartTenant(tenantID, body.PrimaryConnInfo)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(inst)
		case r.Method == http.MethodPost && action == "stop":
			if err := re.StopTenant(tenantID); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "stopped", "tenant_id": tenantID})
		case r.Method == http.MethodGet && action == "route":
			re.mu.Lock()
			inst, ok := re.tenants[tenantID]
			re.mu.Unlock()
			if !ok || inst.State != "ACTIVE" {
				http.Error(w, "tenant read instance not active", http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(inst)
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	})
	return mux
}

func main() {
	cfg := loadConfig(os.Args[1:])
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	re, err := NewReadEngine(cfg)
	if err != nil {
		log.Fatalf("init read engine: %v", err)
	}

	if cfg.TenantID != "" {
		inst, err := re.StartTenant(cfg.TenantID, cfg.PrimaryConnInfo)
		if err != nil {
			log.Fatalf("start default tenant %s: %v", cfg.TenantID, err)
		}
		log.Printf("[air-read-engine] started read-only PG17 for %s on %s:%d (sock=%s)",
			inst.TenantID, inst.Host, inst.Port, inst.SockDir)
	}

	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		re.StopAll()
		log.Fatalf("listen %s: %v", cfg.HTTPAddr, err)
	}
	srv := &http.Server{
		Handler:           re.HTTPHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("[air-read-engine] HTTP control listening on %s (air_ro=%s, shared_buffers=%s)",
			cfg.HTTPAddr, cfg.AirRoSO, cfg.SharedBuffers)
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("[air-read-engine] shutting down...")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
	re.StopAll()
}
