package main

import (
	"context"
	"crypto/subtle"
	"flag"
	"fmt"
	"log"
	"net"
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
		log.Printf("[air-read-engine][DEBUG][startup] grpc=%s data_dir=%s cache_dir=%q pageserver_socket=%q pageserver_grpc=%q primary_conninfo_set=%v internal_token_set=%v",
			cfg.GRPCAddr, cfg.DataDir, cfg.CacheDir, cfg.PageserverSocket, cfg.PageserverGRPCAddr,
			cfg.PrimaryConnInfo != "", cfg.PageserverInternalToken != "")
	}

	var grpcSrv *grpc.Server
	if cfg.GRPCAddr != "" {
		grpcLn, gErr := net.Listen("tcp", cfg.GRPCAddr)
		if gErr != nil {
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
	if grpcSrv != nil {
		grpcSrv.GracefulStop()
	}
	_ = re.Stop()
}
