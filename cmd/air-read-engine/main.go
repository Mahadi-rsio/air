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

	"github.com/Mahadi-rsio/air/internal/readadapter"
)

type Config struct {
	Engine           string
	HTTPAddr         string
	ListenHost       string
	PGPort           int
	DataDir          string
	PGBinDir         string
	AirRoSO          string
	PageserverSocket string
	SharedBuffers    string
	PrimaryConnInfo  string
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

func parseMemoryToKB(s string, fallbackKB int) int {
	s = strings.TrimSpace(strings.ToUpper(s))
	if strings.HasSuffix(s, "GB") {
		if n, err := strconv.Atoi(strings.TrimSuffix(s, "GB")); err == nil && n > 0 {
			return n * 1024 * 1024
		}
	}
	if strings.HasSuffix(s, "MB") {
		if n, err := strconv.Atoi(strings.TrimSuffix(s, "MB")); err == nil && n > 0 {
			return n * 1024
		}
	}
	if strings.HasSuffix(s, "KB") {
		if n, err := strconv.Atoi(strings.TrimSuffix(s, "KB")); err == nil && n > 0 {
			return n
		}
	}
	return fallbackKB
}

func loadConfig(args []string) Config {
	fs := flag.NewFlagSet("air-read-engine", flag.ContinueOnError)
	cfg := Config{}
	fs.StringVar(&cfg.Engine, "engine", envOr("AIR_READ_ENGINE_TYPE", "sqlite"), "Read engine backend type ('sqlite' default or 'postgres')")
	fs.StringVar(&cfg.HTTPAddr, "http-addr", envOr("AIR_READ_ENGINE_HTTP_ADDR", "127.0.0.1:8082"), "HTTP admin/health listen address")
	fs.StringVar(&cfg.ListenHost, "listen-host", envOr("AIR_READ_ENGINE_HOST", "127.0.0.1"), "Shared PostgreSQL wire listen address")
	fs.IntVar(&cfg.PGPort, "pg-port", envIntOr("AIR_READ_ENGINE_PG_PORT", 5433), "Shared PostgreSQL wire port for all tenants")
	fs.StringVar(&cfg.DataDir, "data-dir", envOr("AIR_READ_ENGINE_DATA_DIR", "/tmp/air-read-engine"), "Read engine data directory")
	fs.StringVar(&cfg.PGBinDir, "pg-bin-dir", envOr("AIR_PG_BIN_DIR", "/usr/lib/postgresql/17/bin"), "PostgreSQL 17 binary directory (used when --engine=postgres)")
	fs.StringVar(&cfg.AirRoSO, "air-ro-so", envOr("AIR_RO_SO_PATH", ""), "Path to air_ro.so extension (used when --engine=postgres)")
	fs.StringVar(&cfg.PageserverSocket, "pageserver-socket", envOr("AIR_PAGESERVER_SOCKET", ""), "Optional Pageserver Unix socket path")
	fs.StringVar(&cfg.SharedBuffers, "shared-buffers", envOr("AIR_READ_ENGINE_SHARED_BUFFERS", "32MB"), "Read engine page cache / shared_buffers size")
	fs.StringVar(&cfg.PrimaryConnInfo, "primary-conninfo", envOr("AIR_READ_ENGINE_PRIMARY_CONNINFO", ""), "Optional primary_conninfo for syncing from Write Engine")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("parse flags: %v", err)
	}
	cfg.Engine = strings.ToLower(strings.TrimSpace(cfg.Engine))
	if cfg.Engine == "" {
		cfg.Engine = "sqlite"
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

type SharedReadEngine struct {
	cfg      Config
	mu       sync.Mutex
	pgData   string
	sockDir  string
	logRoot  string
	state    string
	tenants  map[string]bool
	sqliteRe *readadapter.SQLiteReadServer
}

func NewSharedReadEngine(cfg Config) (*SharedReadEngine, error) {
	if cfg.Engine == "sqlite" {
		sq, err := readadapter.NewSQLiteReadServer(readadapter.SQLiteServerConfig{
			ListenHost:      cfg.ListenHost,
			PGPort:          cfg.PGPort,
			DataDir:         cfg.DataDir,
			PrimaryConnInfo: cfg.PrimaryConnInfo,
			CacheKB:         parseMemoryToKB(cfg.SharedBuffers, 32768),
		})
		if err != nil {
			return nil, err
		}
		return &SharedReadEngine{
			cfg:      cfg,
			sockDir:  sq.SockDir(),
			state:    "STOPPED",
			tenants:  make(map[string]bool),
			sqliteRe: sq,
		}, nil
	}

	root := filepath.Join(cfg.DataDir, "shared")
	pgData := filepath.Join(root, "pgdata")
	sockDir := filepath.Join(root, "sock")
	if err := os.MkdirAll(sockDir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir sockDir: %w", err)
	}
	return &SharedReadEngine{
		cfg:     cfg,
		pgData:  pgData,
		sockDir: sockDir,
		logRoot: root,
		state:   "STOPPED",
		tenants: make(map[string]bool),
	}, nil
}

func (re *SharedReadEngine) Start() error {
	re.mu.Lock()
	defer re.mu.Unlock()
	if re.state == "ACTIVE" {
		return nil
	}

	if re.sqliteRe != nil {
		if err := re.sqliteRe.Start(); err != nil {
			return err
		}
		re.cfg.PGPort = re.sqliteRe.Port()
		re.sockDir = re.sqliteRe.SockDir()
		re.state = "ACTIVE"
		return nil
	}

	if _, err := os.Stat(filepath.Join(re.pgData, "PG_VERSION")); os.IsNotExist(err) {
		if re.cfg.PrimaryConnInfo != "" {
			bbCmd := exec.Command(filepath.Join(re.cfg.PGBinDir, "pg_basebackup"),
				"-D", re.pgData,
				"-d", re.cfg.PrimaryConnInfo,
				"-R", "-X", "stream", "-c", "fast",
			)
			if out, err := bbCmd.CombinedOutput(); err != nil {
				return fmt.Errorf("pg_basebackup failed: %w: %s", err, string(out))
			}
			if err := re.configurePG(); err != nil {
				return err
			}
		} else {
			initCmd := exec.Command(filepath.Join(re.cfg.PGBinDir, "initdb"),
				"-D", re.pgData, "-U", "postgres", "--auth=trust", "--no-sync")
			if out, err := initCmd.CombinedOutput(); err != nil {
				return fmt.Errorf("initdb failed: %w: %s", err, string(out))
			}
			if err := re.configurePG(); err != nil {
				return err
			}
			if err := re.pgCtl("start"); err != nil {
				return fmt.Errorf("bootstrap start: %w", err)
			}
			_ = re.runPSQL("postgres", "CREATE ROLE app_user LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;")
			_ = re.pgCtl("stop")
		}
	}

	if err := re.enableReadOnlyConf(); err != nil {
		return err
	}
	if re.cfg.PrimaryConnInfo != "" {
		_ = os.WriteFile(filepath.Join(re.pgData, "standby.signal"), []byte(""), 0o600)
	}
	if err := re.pgCtl("start"); err != nil {
		return fmt.Errorf("start shared read-only pg: %w", err)
	}
	re.state = "ACTIVE"
	return nil
}

func (re *SharedReadEngine) Stop() error {
	re.mu.Lock()
	defer re.mu.Unlock()
	if re.state != "ACTIVE" {
		return nil
	}
	re.state = "STOPPED"
	if re.sqliteRe != nil {
		return re.sqliteRe.Stop()
	}
	return re.pgCtl("stop")
}

// EnsureTenantDB creates or registers the tenant database in the shared read engine.
func (re *SharedReadEngine) EnsureTenantDB(tenantID string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant_id is required")
	}
	if re.sqliteRe != nil {
		if err := re.sqliteRe.EnsureTenantDB(tenantID); err != nil {
			return err
		}
		re.mu.Lock()
		re.tenants[tenantID] = true
		re.mu.Unlock()
		return nil
	}

	re.mu.Lock()
	defer re.mu.Unlock()
	if re.tenants[tenantID] {
		return nil
	}
	if re.cfg.PrimaryConnInfo != "" {
		// In streaming standby mode, databases replicate from primary automatically.
		re.tenants[tenantID] = true
		return nil
	}
	// Briefly restart with air_ro.enabled = off to create the tenant database, then re-enable air_ro.
	if re.state == "ACTIVE" {
		_ = re.pgCtl("stop")
	}
	_ = re.setAirRoConf(false)
	if err := re.pgCtl("start"); err != nil {
		return err
	}
	err := re.runPSQL("postgres", fmt.Sprintf("CREATE DATABASE %q OWNER app_user;", tenantID))
	_ = re.pgCtl("stop")
	_ = re.setAirRoConf(true)
	if startErr := re.pgCtl("start"); startErr != nil {
		re.state = "STOPPED"
		return startErr
	}
	re.state = "ACTIVE"
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}
	re.tenants[tenantID] = true
	return nil
}

func (re *SharedReadEngine) configurePG() error {
	confPath := filepath.Join(re.pgData, "postgresql.conf")
	var preload []string
	if re.cfg.AirRoSO != "" {
		preload = append(preload, re.cfg.AirRoSO)
	}
	if re.cfg.PageserverSocket != "" {
		preload = append(preload, "air_smgr")
	}
	extra := fmt.Sprintf(`
# AIR Shared Read Engine config
port = %d
listen_addresses = '%s'
unix_socket_directories = '%s'
shared_buffers = %s
fsync = off
autovacuum = off
hot_standby = on
`, re.cfg.PGPort, re.cfg.ListenHost, re.sockDir, re.cfg.SharedBuffers)
	if len(preload) > 0 {
		extra += fmt.Sprintf("shared_preload_libraries = '%s'\nair_ro.enabled = off\n", strings.Join(preload, ","))
	}
	if re.cfg.PageserverSocket != "" {
		extra += fmt.Sprintf("air_smgr.pageserver_conn = '%s'\nair_smgr.read_only = on\n", re.cfg.PageserverSocket)
	}
	f, err := os.OpenFile(confPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(extra)
	return err
}

func (re *SharedReadEngine) setAirRoConf(enabled bool) error {
	confPath := filepath.Join(re.pgData, "postgresql.conf")
	raw, err := os.ReadFile(confPath)
	if err != nil {
		return err
	}
	s := string(raw)
	if enabled {
		s = strings.ReplaceAll(s, "air_ro.enabled = off", "air_ro.enabled = on")
	} else {
		s = strings.ReplaceAll(s, "air_ro.enabled = on", "air_ro.enabled = off")
	}
	return os.WriteFile(confPath, []byte(s), 0o644)
}

func (re *SharedReadEngine) enableReadOnlyConf() error {
	if err := re.setAirRoConf(true); err != nil {
		return err
	}
	confPath := filepath.Join(re.pgData, "postgresql.conf")
	raw, err := os.ReadFile(confPath)
	if err != nil {
		return err
	}
	content := string(raw)
	if !strings.Contains(content, "default_transaction_read_only = on") {
		content += "\ndefault_transaction_read_only = on\n"
	}
	if re.cfg.PrimaryConnInfo != "" && !strings.Contains(content, "primary_conninfo") {
		content += fmt.Sprintf("primary_conninfo = '%s'\n", re.cfg.PrimaryConnInfo)
	}
	return os.WriteFile(confPath, []byte(content), 0o644)
}

func (re *SharedReadEngine) pgCtl(action string) error {
	args := []string{"-D", re.pgData, "-w"}
	if action == "start" {
		args = append(args, "-l", filepath.Join(re.logRoot, "read-engine-pg.log"), "start")
	} else {
		args = append(args, "-m", "fast", action)
	}
	cmd := exec.Command(filepath.Join(re.cfg.PGBinDir, "pg_ctl"), args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("pg_ctl %s: %w: %s", action, err, string(out))
	}
	return nil
}

func (re *SharedReadEngine) runPSQL(db, sql string) error {
	cmd := exec.Command(filepath.Join(re.cfg.PGBinDir, "psql"),
		"-h", re.sockDir, "-p", strconv.Itoa(re.cfg.PGPort), "-U", "postgres", "-d", db,
		"-v", "ON_ERROR_STOP=1", "-At", "-c", sql)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("psql: %w: %s", err, string(out))
	}
	return nil
}

func (re *SharedReadEngine) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		re.mu.Lock()
		state := re.state
		port := re.cfg.PGPort
		sockDir := re.sockDir
		re.mu.Unlock()
		resp := map[string]any{
			"status":   "ok",
			"service":  "air-read-engine",
			"engine":   re.cfg.Engine,
			"mode":     "shared",
			"state":    state,
			"host":     re.cfg.ListenHost,
			"pg_port":  port,
			"sock_dir": sockDir,
		}
		if re.sqliteRe != nil {
			allowed, blocked := re.sqliteRe.Stats()
			resp["sqlite_version"] = readadapter.SQLiteVersion()
			resp["queries_allowed"] = allowed
			resp["writes_blocked"] = blocked
		} else {
			resp["air_ro_so"] = re.cfg.AirRoSO
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		re.mu.Lock()
		var tenants []string
		if re.sqliteRe != nil {
			tenants = re.sqliteRe.Tenants()
		} else {
			tenants = make([]string, 0, len(re.tenants))
			for id := range re.tenants {
				tenants = append(tenants, id)
			}
		}
		state := re.state
		port := re.cfg.PGPort
		sockDir := re.sockDir
		re.mu.Unlock()
		resp := map[string]any{
			"engine":   re.cfg.Engine,
			"mode":     "shared",
			"state":    state,
			"host":     re.cfg.ListenHost,
			"pg_port":  port,
			"sock_dir": sockDir,
			"tenants":  tenants,
		}
		if re.sqliteRe != nil {
			allowed, blocked := re.sqliteRe.Stats()
			resp["sqlite_version"] = readadapter.SQLiteVersion()
			resp["queries_allowed"] = allowed
			resp["writes_blocked"] = blocked
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
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
		case r.Method == http.MethodPost && (action == "start" || action == "register"):
			if err := re.EnsureTenantDB(tenantID); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"tenant_id": tenantID,
				"engine":    re.cfg.Engine,
				"host":      re.cfg.ListenHost,
				"port":      re.cfg.PGPort,
				"sock_dir":  re.sockDir,
				"state":     "ACTIVE",
				"mode":      "shared",
			})
		case r.Method == http.MethodPost && action == "sync":
			if re.sqliteRe == nil {
				http.Error(w, "sync endpoint is only supported on sqlite read engine", http.StatusBadRequest)
				return
			}
			var req struct {
				SQL         string `json:"sql"`
				LSN         string `json:"lsn"`
				PrimaryConn string `json:"primary_conninfo"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.PrimaryConn != "" {
				if err := re.sqliteRe.SyncFromPrimary(r.Context(), tenantID, req.PrimaryConn); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			} else if req.SQL != "" {
				lsn := readadapter.ParsePGLSN(req.LSN)
				if err := re.sqliteRe.ExecInternal(tenantID, req.SQL, lsn); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"tenant_id": tenantID,
				"engine":    re.cfg.Engine,
				"status":    "synced",
			})
		case r.Method == http.MethodGet && action == "route":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"tenant_id": tenantID,
				"engine":    re.cfg.Engine,
				"host":      re.cfg.ListenHost,
				"port":      re.cfg.PGPort,
				"sock_dir":  re.sockDir,
				"state":     re.state,
				"mode":      "shared",
			})
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

	re, err := NewSharedReadEngine(cfg)
	if err != nil {
		log.Fatalf("init shared read engine: %v", err)
	}
	if err := re.Start(); err != nil {
		log.Fatalf("start shared read engine: %v", err)
	}
	if re.sqliteRe != nil {
		log.Printf("[air-read-engine] shared read-only SQLite %s engine listening on %s:%d (sock=%s, cache=%s)",
			readadapter.SQLiteVersion(), cfg.ListenHost, re.cfg.PGPort, re.sockDir, cfg.SharedBuffers)
	} else {
		log.Printf("[air-read-engine] shared read-only PG17 listening on %s:%d (sock=%s, shared_buffers=%s)",
			cfg.ListenHost, re.cfg.PGPort, re.sockDir, cfg.SharedBuffers)
	}

	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		_ = re.Stop()
		log.Fatalf("listen %s: %v", cfg.HTTPAddr, err)
	}
	srv := &http.Server{
		Handler:           re.HTTPHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("[air-read-engine] HTTP control listening on %s", cfg.HTTPAddr)
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("[air-read-engine] shutting down...")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
	_ = re.Stop()
}
