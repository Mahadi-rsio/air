package config

import (
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime configuration for the AIR single-cell server.
type Config struct {
	DataDir            string
	MetaDir            string
	MetaDSN            string
	MasterKeyPath      string
	PGBinDir           string
	MetaPort           int
	HTTPAddr           string
	GRPCAddr           string
	GatewayAddr        string
	GatewayAdminAddr     string
	GatewayAdminURL      string
	GatewayAdminGRPCAddr string
	ControlPlaneURL    string
	InternalToken      string
	BackendHost        string
	BootstrapAPIKey    string
	BootstrapAccountID string
	RequireTLS         bool
	TLSCertPath        string
	TLSKeyPath         string
	PoolMode           bool
	PoolMaxBackends    int
	IdleTimeout        time.Duration
	R2Endpoint         string
	R2Bucket           string
	R2AccessKeyID      string
	R2SecretAccessKey  string
	R2Region           string
	R2Primary          bool
	EvictOnSuspend     bool
	PageserverEnabled  bool
	PageserverSocket   string
	PageserverGRPCAddr string
	PageserverCacheMB  int
}

// Default returns safe defaults rooted under dataDir.
func Default(dataDir string) Config {
	if dataDir == "" {
		dataDir = "/var/lib/air"
	}
	return Config{
		DataDir:            dataDir,
		MetaDir:            filepath.Join(dataDir, "meta"),
		MasterKeyPath:      filepath.Join(dataDir, "master.key"),
		PGBinDir:           "/usr/lib/postgresql/17/bin",
		MetaPort:           15432,
		HTTPAddr:           "127.0.0.1:8080",
		GatewayAddr:        "127.0.0.1:5432",
		GRPCAddr:           "127.0.0.1:9090",
		GatewayAdminAddr:   "127.0.0.1:8081",
		ControlPlaneURL:    "http://127.0.0.1:8080",
		BootstrapAccountID: "acct_default",
		PoolMaxBackends:    16,
		R2Region:           "auto",
		PageserverSocket:   filepath.Join(dataDir, "pageserver.sock"),
		PageserverCacheMB:  256,
	}
}

// Load parses flags and environment overrides into a Config.
func Load(args []string) (Config, error) {
	cfg := Default(envOr("AIR_DATA_DIR", "/tmp/air-data"))
	cfg.MetaDir = envOr("AIR_META_DIR", cfg.MetaDir)
	cfg.MetaDSN = envOr("AIR_META_DSN", cfg.MetaDSN)
	cfg.MasterKeyPath = envOr("AIR_MASTER_KEY_PATH", cfg.MasterKeyPath)
	cfg.PGBinDir = envOr("AIR_PG_BIN_DIR", cfg.PGBinDir)
	cfg.HTTPAddr = envOr("AIR_HTTP_ADDR", cfg.HTTPAddr)
	cfg.GRPCAddr = envOr("AIR_GRPC_ADDR", cfg.GRPCAddr)
	cfg.GatewayAddr = envOr("AIR_GATEWAY_ADDR", cfg.GatewayAddr)
	cfg.GatewayAdminAddr = envOr("AIR_GATEWAY_ADMIN_ADDR", cfg.GatewayAdminAddr)
	cfg.GatewayAdminURL = envOr("AIR_GATEWAY_ADMIN_URL", cfg.GatewayAdminURL)
	cfg.GatewayAdminGRPCAddr = envOr("AIR_GATEWAY_ADMIN_GRPC_ADDR", cfg.GatewayAdminGRPCAddr)
	cfg.ControlPlaneURL = envOr("AIR_CONTROL_PLANE_URL", cfg.ControlPlaneURL)
	cfg.InternalToken = envOr("AIR_INTERNAL_TOKEN", cfg.InternalToken)
	cfg.BackendHost = envOr("AIR_BACKEND_HOST", cfg.BackendHost)
	cfg.BootstrapAPIKey = envOr("AIR_BOOTSTRAP_API_KEY", cfg.BootstrapAPIKey)
	cfg.BootstrapAccountID = envOr("AIR_BOOTSTRAP_ACCOUNT_ID", cfg.BootstrapAccountID)
	cfg.TLSCertPath = envOr("AIR_TLS_CERT", cfg.TLSCertPath)
	cfg.TLSKeyPath = envOr("AIR_TLS_KEY", cfg.TLSKeyPath)
	cfg.R2Endpoint = envOr("AIR_R2_ENDPOINT", cfg.R2Endpoint)
	cfg.R2Bucket = envOr("AIR_R2_BUCKET", cfg.R2Bucket)
	cfg.R2AccessKeyID = envOr("AIR_R2_ACCESS_KEY_ID", cfg.R2AccessKeyID)
	cfg.R2SecretAccessKey = envOr("AIR_R2_SECRET_ACCESS_KEY", cfg.R2SecretAccessKey)
	cfg.R2Region = envOr("AIR_R2_REGION", cfg.R2Region)
	cfg.PageserverSocket = envOr("AIR_PAGESERVER_SOCKET", filepath.Join(cfg.DataDir, "pageserver.sock"))
	cfg.PageserverGRPCAddr = envOr("AIR_PAGESERVER_GRPC_ADDR", cfg.PageserverGRPCAddr)
	if v := strings.TrimSpace(os.Getenv("AIR_PAGESERVER_ENABLED")); v != "" {
		cfg.PageserverEnabled = v == "1" || strings.EqualFold(v, "true")
	}
	if p := os.Getenv("AIR_PAGESERVER_CACHE_MB"); p != "" {
		if v, err := strconv.Atoi(p); err == nil && v > 0 {
			cfg.PageserverCacheMB = v
		}
	}
	if v := strings.TrimSpace(os.Getenv("AIR_REQUIRE_TLS")); v != "" {
		cfg.RequireTLS = v == "1" || strings.EqualFold(v, "true")
	}
	if v := strings.TrimSpace(os.Getenv("AIR_R2_PRIMARY")); v != "" {
		cfg.R2Primary = v == "1" || strings.EqualFold(v, "true")
	}
	if v := strings.TrimSpace(os.Getenv("AIR_EVICT_ON_SUSPEND")); v != "" {
		cfg.EvictOnSuspend = v == "1" || strings.EqualFold(v, "true")
	}
	if v := strings.TrimSpace(os.Getenv("AIR_POOL_MODE")); v != "" {
		cfg.PoolMode = v == "1" || strings.EqualFold(v, "true")
	}
	if p := os.Getenv("AIR_POOL_MAX_BACKENDS"); p != "" {
		if v, err := strconv.Atoi(p); err == nil && v > 0 {
			cfg.PoolMaxBackends = v
		}
	}
	if d := strings.TrimSpace(os.Getenv("AIR_IDLE_TIMEOUT")); d != "" {
		if parsed, err := time.ParseDuration(d); err == nil && parsed >= 0 {
			cfg.IdleTimeout = parsed
		}
	}
	if p := os.Getenv("AIR_META_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil && v > 0 {
			cfg.MetaPort = v
		}
	}

	fs := flag.NewFlagSet("air-server", flag.ContinueOnError)
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "Root directory for AIR persistent tenant and metadata storage")
	fs.StringVar(&cfg.MetaDir, "meta-dir", cfg.MetaDir, "Metadata PostgreSQL directory")
	fs.StringVar(&cfg.MetaDSN, "meta-dsn", cfg.MetaDSN, "Optional external HA PostgreSQL DSN for Control Plane metadata (disables local meta-dir)")
	fs.StringVar(&cfg.MasterKeyPath, "master-key-path", cfg.MasterKeyPath, "Path to 32-byte master KEK file")
	fs.StringVar(&cfg.PGBinDir, "pg-bin-dir", cfg.PGBinDir, "Path to PostgreSQL 17 bin directory")
	fs.IntVar(&cfg.MetaPort, "meta-port", cfg.MetaPort, "TCP port for internal Control Plane metadata PostgreSQL instance")
	fs.StringVar(&cfg.HTTPAddr, "http-addr", cfg.HTTPAddr, "Control Plane HTTP API listen address")
	fs.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "Air DB gRPC server listen address for air-gateway")
	fs.StringVar(&cfg.GatewayAddr, "gateway-addr", cfg.GatewayAddr, "PostgreSQL wire-protocol gateway listen address")
	fs.StringVar(&cfg.GatewayAdminAddr, "gateway-admin-addr", cfg.GatewayAdminAddr, "Gateway internal admin HTTP listen address (healthz and drain)")
	fs.StringVar(&cfg.GatewayAdminURL, "gateway-admin-url", cfg.GatewayAdminURL, "Optional Gateway admin HTTP base URL used by air-server for cooperative pool draining")
	fs.StringVar(&cfg.GatewayAdminGRPCAddr, "gateway-admin-grpc-addr", cfg.GatewayAdminGRPCAddr, "Optional Gateway admin gRPC address used by air-server for cooperative pool draining")
	fs.StringVar(&cfg.ControlPlaneURL, "control-plane-url", cfg.ControlPlaneURL, "Control Plane HTTP base URL used by air-gateway")
	fs.StringVar(&cfg.InternalToken, "internal-token", cfg.InternalToken, "Shared secret token for internal air-gateway <-> air-server HTTP APIs")
	fs.StringVar(&cfg.BackendHost, "backend-host", cfg.BackendHost, "Backend PostgreSQL host returned/used for remote gateway routing (defaults to 127.0.0.1)")
	fs.StringVar(&cfg.BootstrapAPIKey, "bootstrap-api-key", cfg.BootstrapAPIKey, "Optional bootstrap Bearer API key to seed on startup")
	fs.StringVar(&cfg.BootstrapAccountID, "bootstrap-account-id", cfg.BootstrapAccountID, "Account ID bound to --bootstrap-api-key")
	fs.BoolVar(&cfg.RequireTLS, "require-tls", cfg.RequireTLS, "Require SSL/TLS negotiation on all Gateway client connections")
	fs.StringVar(&cfg.TLSCertPath, "tls-cert", cfg.TLSCertPath, "Optional path to PEM TLS certificate for Gateway")
	fs.StringVar(&cfg.TLSKeyPath, "tls-key", cfg.TLSKeyPath, "Optional path to PEM TLS private key for Gateway")
	fs.BoolVar(&cfg.PoolMode, "pool-mode", cfg.PoolMode, "Enable transaction-mode connection pooling in the Gateway")
	fs.IntVar(&cfg.PoolMaxBackends, "pool-max-backends", cfg.PoolMaxBackends, "Max pooled backend PostgreSQL connections per tenant")
	fs.DurationVar(&cfg.IdleTimeout, "idle-timeout", cfg.IdleTimeout, "Idle duration before cooperative tenant scale-to-zero (0 disables)")
	fs.StringVar(&cfg.R2Endpoint, "r2-endpoint", cfg.R2Endpoint, "S3/R2 endpoint URL for encrypted backups and fencing manifests")
	fs.StringVar(&cfg.R2Bucket, "r2-bucket", cfg.R2Bucket, "S3/R2 bucket name for encrypted backups and fencing manifests")
	fs.StringVar(&cfg.R2AccessKeyID, "r2-access-key-id", cfg.R2AccessKeyID, "S3/R2 access key ID")
	fs.StringVar(&cfg.R2SecretAccessKey, "r2-secret-access-key", cfg.R2SecretAccessKey, "S3/R2 secret access key")
	fs.StringVar(&cfg.R2Region, "r2-region", cfg.R2Region, "S3/R2 region (default: auto)")
	fs.BoolVar(&cfg.R2Primary, "r2-primary", cfg.R2Primary, "Use R2 as primary durable storage and evict local pgdata on suspend")
	fs.BoolVar(&cfg.EvictOnSuspend, "evict-on-suspend", cfg.EvictOnSuspend, "Evict local tenant pgdata cache after cold data is safely uploaded to R2")
	fs.BoolVar(&cfg.PageserverEnabled, "pageserver", cfg.PageserverEnabled, "Enable in-process AIR Pageserver over Unix socket")
	fs.StringVar(&cfg.PageserverSocket, "pageserver-socket", cfg.PageserverSocket, "Unix socket path for AIR Pageserver")
	fs.StringVar(&cfg.PageserverGRPCAddr, "pageserver-grpc-addr", cfg.PageserverGRPCAddr, "Remote standalone AIR Pageserver gRPC address")
	fs.IntVar(&cfg.PageserverCacheMB, "pageserver-cache-mb", cfg.PageserverCacheMB, "Bounded LRU disk cache size in MB for Pageserver")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func envOr(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
