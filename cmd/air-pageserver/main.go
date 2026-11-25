package main

import (
	"context"
	"crypto/subtle"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	DataDir           string
	MasterKeyPath     string
	GRPCAddr          string
	MetricsAddr       string
	SocketPath        string
	CacheMB           int
	MemoryLimitMB     int
	WalRedoBin        string
	InternalToken     string
	R2Endpoint        string
	R2Bucket          string
	R2AccessKeyID     string
	R2SecretAccessKey string
	R2Region          string
	Debug             bool
}

func envBoolOr(key string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return fallback
	}
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

func loadConfig(args []string) Config {
	dataDir := envOr("AIR_DATA_DIR", "/var/lib/air-pageserver")
	cfg := Config{
		DataDir:           dataDir,
		MasterKeyPath:     envOr("AIR_MASTER_KEY_PATH", filepath.Join(dataDir, "master.key")),
		GRPCAddr:          envOr("AIR_PAGESERVER_GRPC_ADDR", "0.0.0.0:9091"),
		MetricsAddr:       envOr("AIR_PAGESERVER_METRICS_ADDR", "[::]:9101"),
		SocketPath:        envOr("AIR_PAGESERVER_SOCKET", ""),
		CacheMB:           envIntOr("AIR_PAGESERVER_CACHE_MB", 64),
		MemoryLimitMB:     envIntOr("AIR_PAGESERVER_MEMORY_MB", 0),
		WalRedoBin:        envOr("AIR_WALREDO_BIN", ""),
		InternalToken:     envOr("AIR_INTERNAL_TOKEN", ""),
		R2Endpoint:        envOr("AIR_R2_ENDPOINT", ""),
		R2Bucket:          envOr("AIR_R2_BUCKET", ""),
		R2AccessKeyID:     envOr("AIR_R2_ACCESS_KEY_ID", ""),
		R2SecretAccessKey: envOr("AIR_R2_SECRET_ACCESS_KEY", ""),
		R2Region:          envOr("AIR_R2_REGION", "auto"),
		Debug:             envBoolOr("AIR_PAGESERVER_DEBUG", true),
	}

	fs := flag.NewFlagSet("air-pageserver", flag.ContinueOnError)
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "Root directory for pageserver layers and cache")
	fs.StringVar(&cfg.MasterKeyPath, "master-key-path", cfg.MasterKeyPath, "Path to 32-byte master KEK file")
	fs.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "Pageserver gRPC listen address")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", cfg.MetricsAddr, "Optional HTTP /metrics listen address")
	fs.StringVar(&cfg.SocketPath, "socket-path", cfg.SocketPath, "Optional Unix socket path for co-located air_smgr/read-engine")
	fs.IntVar(&cfg.CacheMB, "cache-mb", cfg.CacheMB, "Bounded LRU disk cache size in MB")
	fs.IntVar(&cfg.MemoryLimitMB, "memory-mb", cfg.MemoryLimitMB, "Pageserver memory limit in MB (0 = auto-detect from cgroup v2)")
	fs.StringVar(&cfg.WalRedoBin, "walredo-bin", cfg.WalRedoBin, "Optional path to air-walredo binary")
	fs.StringVar(&cfg.InternalToken, "internal-token", cfg.InternalToken, "Shared secret token for internal gRPC authentication")
	fs.StringVar(&cfg.R2Endpoint, "r2-endpoint", cfg.R2Endpoint, "S3/R2 endpoint URL")
	fs.StringVar(&cfg.R2Bucket, "r2-bucket", cfg.R2Bucket, "S3/R2 bucket name")
	fs.StringVar(&cfg.R2AccessKeyID, "r2-access-key-id", cfg.R2AccessKeyID, "S3/R2 access key ID")
	fs.StringVar(&cfg.R2SecretAccessKey, "r2-secret-access-key", cfg.R2SecretAccessKey, "S3/R2 secret access key")
	fs.StringVar(&cfg.R2Region, "r2-region", cfg.R2Region, "S3/R2 region")
	fs.BoolVar(&cfg.Debug, "debug", cfg.Debug, "Enable verbose debug logging")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("parse flags: %v", err)
	}
	return cfg
}

func buildS3Client(cfg Config) (*s3.Client, error) {
	anySet := cfg.R2Endpoint != "" || cfg.R2Bucket != "" || cfg.R2AccessKeyID != "" || cfg.R2SecretAccessKey != ""
	if !anySet {
		return nil, nil
	}
	if cfg.R2Endpoint == "" || cfg.R2Bucket == "" || cfg.R2AccessKeyID == "" || cfg.R2SecretAccessKey == "" {
		return nil, fmt.Errorf("incomplete R2 configuration: AIR_R2_ENDPOINT, AIR_R2_BUCKET, AIR_R2_ACCESS_KEY_ID, and AIR_R2_SECRET_ACCESS_KEY must all be set")
	}
	region := cfg.R2Region
	if region == "" {
		region = "auto"
	}
	awsCfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.R2AccessKeyID, cfg.R2SecretAccessKey, ""),
	}
	s3c := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.R2Endpoint)
		o.UsePathStyle = true
	})
	return s3c, nil
}

func main() {
	cfg := loadConfig(os.Args[1:])

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		log.Fatalf("mkdir data dir: %v", err)
	}

	kms, err := crypto.LoadOrCreateEnvelopeKMS(cfg.MasterKeyPath)
	if err != nil {
		log.Fatalf("init kms: %v", err)
	}

	s3c, err := buildS3Client(cfg)
	if err != nil {
		log.Fatalf("init s3 client: %v", err)
	}

	pageserver.SetDebugLogging(cfg.Debug)
	log.Printf("[air-pageserver] starting (debug=%v, data_dir=%s, cache_mb=%d, memory_mb=%d, walredo_bin=%q, r2_enabled=%v, r2_bucket=%q, auth_enabled=%v)",
		cfg.Debug, cfg.DataDir, cfg.CacheMB, cfg.MemoryLimitMB, cfg.WalRedoBin, s3c != nil, cfg.R2Bucket, cfg.InternalToken != "")

	ps, err := pageserver.New(pageserver.Config{
		SocketPath:        cfg.SocketPath,
		DisableUnixSocket: cfg.SocketPath == "",
		LayerDir:          filepath.Join(cfg.DataDir, "ps_layers"),
		CacheDir:          filepath.Join(cfg.DataDir, "ps_cache"),
		CacheMaxBytes:     int64(cfg.CacheMB) * 1024 * 1024,
		WalRedoBin:        cfg.WalRedoBin,
		S3Client:          s3c,
		R2Bucket:          cfg.R2Bucket,
		KMS:               kms,
		MemoryBudget: pageserver.MemoryBudgetConfig{
			MemoryLimitBytes:    int64(cfg.MemoryLimitMB) * 1024 * 1024,
			ApplyGoRuntimeLimit: true,
		},
		Debug: cfg.Debug,
	})
	if err != nil {
		log.Fatalf("init pageserver: %v", err)
	}
	if err := ps.Start(ctx); err != nil {
		_ = ps.Close()
		log.Fatalf("start pageserver unix socket: %v", err)
	}

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		_ = ps.Close()
		log.Fatalf("grpc listen %s: %v", cfg.GRPCAddr, err)
	}
	grpcSrv := pageserver.NewGRPCServer(ps, cfg.InternalToken)
	go func() {
		log.Printf("[air-pageserver] gRPC listening on %s (cache_mb=%d, unix_socket=%q)", cfg.GRPCAddr, cfg.CacheMB, cfg.SocketPath)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	var metricsSrv *http.Server
	if cfg.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			if cfg.InternalToken != "" {
				tok := strings.TrimSpace(r.Header.Get("X-Air-Internal-Token"))
				if tok == "" {
					tok = strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
				}
				if subtle.ConstantTimeCompare([]byte(tok), []byte(cfg.InternalToken)) != 1 {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
			}
			w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
			w.Header().Set("Connection", "close")
			_, _ = io.WriteString(w, ps.PrometheusMetrics())
		})
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "ok\n")
		})
		metricsSrv = &http.Server{
			Addr:              cfg.MetricsAddr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			log.Printf("[air-pageserver] HTTP /metrics listening on %s", cfg.MetricsAddr)
			if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("[air-pageserver] metrics server error: %v", err)
			}
		}()
	}

	<-ctx.Done()
	log.Println("[air-pageserver] shutting down...")
	if metricsSrv != nil {
		mctx, mcancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = metricsSrv.Shutdown(mctx)
		mcancel()
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	grpcDone := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(grpcDone)
	}()
	select {
	case <-grpcDone:
	case <-time.After(2 * time.Second):
		log.Println("[air-pageserver] gRPC GracefulStop timed out after 2s; forcing Stop()")
		grpcSrv.Stop()
	}

	if err := ps.FlushAll(shutCtx); err != nil {
		log.Printf("[air-pageserver] flush all error: %v", err)
	}
	if err := ps.Close(); err != nil {
		log.Printf("[air-pageserver] close error: %v", err)
	}
}
