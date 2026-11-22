package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Mahadi-rsio/air/internal/pb"
	"github.com/Mahadi-rsio/air/internal/readadapter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Config struct {
	HTTPAddr                string
	GRPCAddr                string
	ListenHost              string
	PGPort                  int
	DataDir                 string
	SharedBuffers           string
	PrimaryConnInfo         string
	PageserverSocket        string
	PageserverGRPCAddr      string
	PageserverInternalToken string
	CacheDir                string
	Debug                   bool
}

func envBoolOr(key string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
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
	defaultHost := envOr("AIR_READ_ENGINE_HOST", envOr("AIR_READ_ENGINE_LISTEN_HOST", "127.0.0.1"))
	fs.StringVar(&cfg.HTTPAddr, "http-addr", envOr("AIR_READ_ENGINE_HTTP_ADDR", "127.0.0.1:8082"), "HTTP admin/health listen address")
	fs.StringVar(&cfg.GRPCAddr, "grpc-addr", envOr("AIR_READ_ENGINE_GRPC_ADDR", "127.0.0.1:9092"), "ReadEngineService gRPC listen address")
	fs.StringVar(&cfg.ListenHost, "listen-host", defaultHost, "Shared SQLite PG-wire listen address")
	fs.IntVar(&cfg.PGPort, "pg-port", envIntOr("AIR_READ_ENGINE_PG_PORT", 5433), "Shared SQLite PG-wire port for all tenants")
	fs.StringVar(&cfg.DataDir, "data-dir", envOr("AIR_READ_ENGINE_DATA_DIR", "/tmp/air-read-engine"), "Read engine data directory")
	fs.StringVar(&cfg.SharedBuffers, "shared-buffers", envOr("AIR_READ_ENGINE_SHARED_BUFFERS", "32MB"), "SQLite page cache size")
	fs.StringVar(&cfg.PrimaryConnInfo, "primary-conninfo", envOr("AIR_READ_ENGINE_PRIMARY_CONNINFO", ""), "Optional primary_conninfo for syncing from Write Engine")
	fs.StringVar(&cfg.PageserverSocket, "pageserver-socket", envOr("AIR_PAGESERVER_SOCKET", ""), "AIR Pageserver Unix socket path for native zero-sync storage reads")
	fs.StringVar(&cfg.PageserverGRPCAddr, "pageserver-grpc-addr", envOr("AIR_PAGESERVER_GRPC_ADDR", ""), "Standalone AIR Pageserver gRPC address")
	fs.StringVar(&cfg.PageserverInternalToken, "internal-token", envOr("AIR_INTERNAL_TOKEN", ""), "Shared internal token for Pageserver gRPC auth")
	fs.StringVar(&cfg.CacheDir, "cache-dir", envOr("AIR_READ_ENGINE_CACHE_DIR", ""), "Optional local NVMe L2 page cache directory")
	fs.BoolVar(&cfg.Debug, "debug", envBoolOr("AIR_READ_ENGINE_DEBUG", true), "Enable verbose debug logging")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("parse flags: %v", err)
	}
	return cfg
}

type SharedReadEngine struct {
	cfg      Config
	mu       sync.Mutex
	sockDir  string
	state    string
	sqliteRe *readadapter.SQLiteReadServer
}

func NewSharedReadEngine(cfg Config) (*SharedReadEngine, error) {
	readadapter.SetDebugLogging(cfg.Debug)
	sq, err := readadapter.NewSQLiteReadServer(readadapter.SQLiteServerConfig{
		ListenHost:              cfg.ListenHost,
		PGPort:                  cfg.PGPort,
		DataDir:                 cfg.DataDir,
		PrimaryConnInfo:         cfg.PrimaryConnInfo,
		PageserverSocket:        cfg.PageserverSocket,
		PageserverGRPCAddr:      cfg.PageserverGRPCAddr,
		PageserverInternalToken: cfg.PageserverInternalToken,
		CacheDir:                cfg.CacheDir,
		CacheKB:                 parseMemoryToKB(cfg.SharedBuffers, 32768),
		Debug:                   cfg.Debug,
	})
	if err != nil {
		return nil, err
	}
	return &SharedReadEngine{
		cfg:      cfg,
		sockDir:  sq.SockDir(),
		state:    "STOPPED",
		sqliteRe: sq,
	}, nil
}

func (re *SharedReadEngine) Start() error {
	re.mu.Lock()
	defer re.mu.Unlock()
	if re.state == "ACTIVE" {
		return nil
	}
	if err := re.sqliteRe.Start(); err != nil {
		return err
	}
	re.cfg.PGPort = re.sqliteRe.Port()
	re.sockDir = re.sqliteRe.SockDir()
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
	return re.sqliteRe.Stop()
}

// EnsureTenantDB creates or registers the tenant database in the shared SQLite read engine.
func (re *SharedReadEngine) EnsureTenantDB(tenantID string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant_id is required")
	}
	return re.sqliteRe.EnsureTenantDB(tenantID)
}

// DeleteTenantDB removes a tenant namespace from the shared SQLite read engine.
func (re *SharedReadEngine) DeleteTenantDB(tenantID string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant_id is required")
	}
	return re.sqliteRe.DeleteTenantDB(tenantID)
}

func (re *SharedReadEngine) isAuthorizedHTTP(r *http.Request) bool {
	tok := re.cfg.PageserverInternalToken
	if tok == "" {
		return true
	}
	if h := strings.TrimSpace(r.Header.Get("X-Air-Internal-Token")); h != "" {
		if subtle.ConstantTimeCompare([]byte(h), []byte(tok)) == 1 {
			return true
		}
	}
	if auth := r.Header.Get("Authorization"); len(auth) > 7 && strings.EqualFold(auth[:7], "Bearer ") {
		if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(auth[7:])), []byte(tok)) == 1 {
			return true
		}
	}
	return false
}

func (re *SharedReadEngine) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		re.mu.Lock()
		state := re.state
		port := re.cfg.PGPort
		sockDir := re.sockDir
		re.mu.Unlock()
		allowed, blocked := re.sqliteRe.Stats()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":          "ok",
			"service":         "air-read-engine",
			"engine":          "sqlite",
			"mode":            "shared",
			"state":           state,
			"host":            re.cfg.ListenHost,
			"pg_port":         port,
			"sock_dir":        sockDir,
			"sqlite_version":  readadapter.SQLiteVersion(),
			"queries_allowed": allowed,
			"writes_blocked":  blocked,
		})
	})
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		re.mu.Lock()
		state := re.state
		port := re.cfg.PGPort
		sockDir := re.sockDir
		re.mu.Unlock()
		allowed, blocked := re.sqliteRe.Stats()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"engine":          "sqlite",
			"mode":            "shared",
			"state":           state,
			"host":            re.cfg.ListenHost,
			"pg_port":         port,
			"sock_dir":        sockDir,
			"tenants":         re.sqliteRe.Tenants(),
			"sqlite_version":  readadapter.SQLiteVersion(),
			"queries_allowed": allowed,
			"writes_blocked":  blocked,
		})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(re.sqliteRe.DetailedMetrics())
	})
	mux.HandleFunc("/v1/tenants/", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		path := strings.TrimPrefix(r.URL.Path, "/v1/tenants/")
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if len(parts) < 2 {
			http.Error(w, "invalid path", http.StatusBadRequest)
			return
		}
		tenantID, action := parts[0], parts[1]
		if re.cfg.Debug {
			log.Printf("[air-read-engine][DEBUG][http] %s %s tenant=%s action=%s remote=%s",
				r.Method, r.URL.Path, tenantID, action, r.RemoteAddr)
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && (action == "start" || action == "register"):
			if err := re.EnsureTenantDB(tenantID); err != nil {
				if re.cfg.Debug {
					log.Printf("[air-read-engine][DEBUG][http] %s %s tenant=%s err=%v dur=%s",
						r.Method, r.URL.Path, tenantID, err, time.Since(start))
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if re.cfg.Debug {
				log.Printf("[air-read-engine][DEBUG][http] %s %s tenant=%s -> ACTIVE dur=%s",
					r.Method, r.URL.Path, tenantID, time.Since(start))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"tenant_id": tenantID,
				"engine":    "sqlite",
				"host":      re.cfg.ListenHost,
				"port":      re.cfg.PGPort,
				"sock_dir":  re.sockDir,
				"state":     "ACTIVE",
				"mode":      "shared",
			})
		case r.Method == http.MethodPost && action == "sync":
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
				"engine":    "sqlite",
				"status":    "synced",
			})
		case r.Method == http.MethodGet && action == "route":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"tenant_id": tenantID,
				"engine":    "sqlite",
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
	handleNamespaces := func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		w.Header().Set("Content-Type", "application/json")
		if !re.isAuthorizedHTTP(r) {
			if re.cfg.Debug {
				log.Printf("[air-read-engine][DEBUG][http] %s %s remote=%s status=401 unauthorized dur=%s",
					r.Method, r.URL.Path, r.RemoteAddr, time.Since(start))
			}
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
			return
		}
		suffix := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/namespaces"), "/")
		if re.cfg.Debug {
			log.Printf("[air-read-engine][DEBUG][http] %s %s suffix=%q remote=%s",
				r.Method, r.URL.Path, suffix, r.RemoteAddr)
		}
		switch {
		case suffix == "" && r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"service":    "air-read-engine",
				"namespaces": re.sqliteRe.Tenants(),
			})
		case r.Method == http.MethodPost:
			var req struct {
				TenantID  string `json:"tenant_id"`
				Namespace string `json:"namespace"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			tid := strings.TrimSpace(suffix)
			if tid == "" {
				tid = strings.TrimSpace(req.TenantID)
			}
			if tid == "" {
				tid = strings.TrimSpace(req.Namespace)
			}
			if tid == "" {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "tenant_id or namespace is required"})
				return
			}
			if err := re.EnsureTenantDB(tid); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":    "active",
				"service":   "air-read-engine",
				"tenant_id": tid,
				"namespace": tid,
				"engine":    "sqlite",
				"host":      re.cfg.ListenHost,
				"pg_port":   re.cfg.PGPort,
			})
		case suffix != "" && r.Method == http.MethodGet:
			found := false
			for _, t := range re.sqliteRe.Tenants() {
				if t == suffix {
					found = true
					break
				}
			}
			if !found {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "namespace not found"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"service":   "air-read-engine",
				"tenant_id": suffix,
				"namespace": suffix,
				"engine":    "sqlite",
				"host":      re.cfg.ListenHost,
				"pg_port":   re.cfg.PGPort,
				"state":     re.state,
			})
		case suffix != "" && r.Method == http.MethodDelete:
			if err := re.DeleteTenantDB(suffix); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":    "deleted",
				"service":   "air-read-engine",
				"tenant_id": suffix,
			})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		}
	}
	mux.HandleFunc("/v1/namespaces", handleNamespaces)
	mux.HandleFunc("/v1/namespaces/", handleNamespaces)
	return mux
}

type readEngineGRPCServer struct {
	pb.UnimplementedReadEngineServiceServer
	re            *SharedReadEngine
	internalToken string
}

func (s *readEngineGRPCServer) authorize(ctx context.Context) error {
	if s.internalToken == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	for _, key := range []string{"x-air-internal-token", "x-internal-token"} {
		for _, v := range md.Get(key) {
			if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(v)), []byte(s.internalToken)) == 1 {
				return nil
			}
		}
	}
	return status.Error(codes.Unauthenticated, "unauthorized")
}

func (s *readEngineGRPCServer) authUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()
	if err := s.authorize(ctx); err != nil {
		if s.re.cfg.Debug {
			log.Printf("[air-read-engine][DEBUG][grpc] method=%s auth=DENIED code=%s err=%v dur=%s",
				info.FullMethod, status.Code(err), err, time.Since(start))
		}
		return nil, err
	}
	resp, err := handler(ctx, req)
	if s.re.cfg.Debug {
		if err != nil {
			log.Printf("[air-read-engine][DEBUG][grpc] method=%s status=%s err=%v dur=%s",
				info.FullMethod, status.Code(err), err, time.Since(start))
		} else {
			log.Printf("[air-read-engine][DEBUG][grpc] method=%s status=OK dur=%s",
				info.FullMethod, time.Since(start))
		}
	}
	return resp, err
}

func (s *readEngineGRPCServer) Health(_ context.Context, _ *pb.ReadEngineEmpty) (*pb.ReadEngineHealthResponse, error) {
	s.re.mu.Lock()
	state := s.re.state
	port := s.re.cfg.PGPort
	sockDir := s.re.sockDir
	s.re.mu.Unlock()
	allowed, blocked := s.re.sqliteRe.Stats()
	return &pb.ReadEngineHealthResponse{
		Status:         "ok",
		Service:        "air-read-engine",
		State:          state,
		Host:           s.re.cfg.ListenHost,
		PgPort:         int32(port),
		SockDir:        sockDir,
		QueriesAllowed: allowed,
		WritesBlocked:  blocked,
	}, nil
}

func (s *readEngineGRPCServer) EnsureTenant(_ context.Context, req *pb.ReadEngineTenantRequest) (*pb.ReadEngineTenantRouteResponse, error) {
	if err := s.re.EnsureTenantDB(req.GetTenantId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.re.mu.Lock()
	port := s.re.cfg.PGPort
	sockDir := s.re.sockDir
	s.re.mu.Unlock()
	return &pb.ReadEngineTenantRouteResponse{
		TenantId: req.GetTenantId(),
		Engine:   "sqlite",
		Host:     s.re.cfg.ListenHost,
		PgPort:   int32(port),
		SockDir:  sockDir,
		State:    "ACTIVE",
	}, nil
}

func (s *readEngineGRPCServer) GetTenantRoute(_ context.Context, req *pb.ReadEngineTenantRequest) (*pb.ReadEngineTenantRouteResponse, error) {
	s.re.mu.Lock()
	port := s.re.cfg.PGPort
	sockDir := s.re.sockDir
	state := s.re.state
	s.re.mu.Unlock()
	return &pb.ReadEngineTenantRouteResponse{
		TenantId: req.GetTenantId(),
		Engine:   "sqlite",
		Host:     s.re.cfg.ListenHost,
		PgPort:   int32(port),
		SockDir:  sockDir,
		State:    state,
	}, nil
}

func (s *readEngineGRPCServer) DeleteTenant(_ context.Context, req *pb.ReadEngineTenantRequest) (*pb.ReadEngineStatusResponse, error) {
	if err := s.re.DeleteTenantDB(req.GetTenantId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.ReadEngineStatusResponse{
		TenantId: req.GetTenantId(),
		Status:   "deleted",
	}, nil
}

func (s *readEngineGRPCServer) ListTenants(_ context.Context, _ *pb.ReadEngineEmpty) (*pb.ReadEngineListTenantsResponse, error) {
	return &pb.ReadEngineListTenantsResponse{
		TenantIds: s.re.sqliteRe.Tenants(),
	}, nil
}

func (s *readEngineGRPCServer) SyncTenant(ctx context.Context, req *pb.SyncTenantRequest) (*pb.ReadEngineStatusResponse, error) {
	if req.GetPrimaryConninfo() != "" {
		if err := s.re.sqliteRe.SyncFromPrimary(ctx, req.GetTenantId(), req.GetPrimaryConninfo()); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else if req.GetSql() != "" {
		lsn := readadapter.ParsePGLSN(req.GetLsn())
		if err := s.re.sqliteRe.ExecInternal(req.GetTenantId(), req.GetSql(), lsn); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &pb.ReadEngineStatusResponse{
		TenantId: req.GetTenantId(),
		Status:   "synced",
	}, nil
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
	log.Printf("[air-read-engine] shared read-only SQLite %s engine listening on %s:%d (sock=%s, cache=%s, pageserver_grpc=%q, debug=%v)",
		readadapter.SQLiteVersion(), cfg.ListenHost, re.cfg.PGPort, re.sockDir, cfg.SharedBuffers, cfg.PageserverGRPCAddr, cfg.Debug)
	if cfg.Debug {
		log.Printf("[air-read-engine][DEBUG][startup] http=%s grpc=%s data_dir=%s cache_dir=%q pageserver_socket=%q pageserver_grpc=%q primary_conninfo_set=%v internal_token_set=%v",
			cfg.HTTPAddr, cfg.GRPCAddr, cfg.DataDir, cfg.CacheDir, cfg.PageserverSocket, cfg.PageserverGRPCAddr,
			cfg.PrimaryConnInfo != "", cfg.PageserverInternalToken != "")
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

	var grpcSrv *grpc.Server
	if cfg.GRPCAddr != "" {
		grpcLn, gErr := net.Listen("tcp", cfg.GRPCAddr)
		if gErr != nil {
			_ = srv.Close()
			_ = re.Stop()
			log.Fatalf("grpc listen %s: %v", cfg.GRPCAddr, gErr)
		}
		reSvc := &readEngineGRPCServer{re: re, internalToken: cfg.PageserverInternalToken}
		grpcSrv = grpc.NewServer(grpc.UnaryInterceptor(reSvc.authUnaryInterceptor))
		pb.RegisterReadEngineServiceServer(grpcSrv, reSvc)
		go func() {
			log.Printf("[air-read-engine] gRPC listening on %s", cfg.GRPCAddr)
			if err := grpcSrv.Serve(grpcLn); err != nil {
				log.Fatalf("grpc serve: %v", err)
			}
		}()
	}

	<-ctx.Done()
	log.Printf("[air-read-engine] shutting down...")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if grpcSrv != nil {
		grpcSrv.GracefulStop()
	}
	_ = srv.Shutdown(shutCtx)
	_ = re.Stop()
}
