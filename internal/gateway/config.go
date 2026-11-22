package gateway

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime configuration for air-gateway, loaded from environment
// variables and overridden by CLI flags.
type Config struct {
	GatewayAddr           string
	AdminAddr             string
	AdminGRPCAddr         string
	MetaDSN               string
	MetaPort              int
	ControlPlaneURL       string
	GRPCAddr              string
	InternalToken         string
	BackendHost           string
	RequireTLS            bool
	TLSCertPath           string
	TLSKeyPath            string
	PoolMode              bool
	PoolMaxBackends       int
	PoolIdleTimeoutSec    int
	AuthTimeoutMS         int
	ClientIdleTimeoutMS   int
	MaxClientConns        int
	MaxTenantClientConns  int
	MaxUserClientConns    int
	DiscoveryIntervalMS   int
	ReadEngineHost        string
	ReadEngineSockDir     string
	ReadEnginePort        int
	ReadEngineGRPCAddr    string
	PageserverGRPCAddr    string
	MeteringLogDir        string
	MeteringMaxFileBytes  int64
	MeteringFlushInterval time.Duration
}

// DefaultConfig returns a Config initialized with default values.
func DefaultConfig() Config {
	return Config{
		GatewayAddr:           "127.0.0.1:5432",
		AdminAddr:             "127.0.0.1:8081",
		MetaPort:              15432,
		ControlPlaneURL:       "http://127.0.0.1:8080",
		GRPCAddr:              "127.0.0.1:9090",
		PoolMode:              true,
		PoolMaxBackends:       16,
		PoolIdleTimeoutSec:    300,
		AuthTimeoutMS:         5000,
		ClientIdleTimeoutMS:   0,
		MaxClientConns:        1024,
		MaxTenantClientConns:  128,
		MaxUserClientConns:    0,
		DiscoveryIntervalMS:   2000,
		ReadEngineHost:        "127.0.0.1",
		ReadEnginePort:        5433,
		MeteringLogDir:        filepath.Join(os.TempDir(), "air-gateway-metering"),
		MeteringMaxFileBytes:  10 * 1024 * 1024,
		MeteringFlushInterval: 2 * time.Minute,
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		return v == "1" || v == "true" || v == "TRUE" || v == "True"
	}
	return fallback
}

func envIntMin(key string, fallback, minVal int) int {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed >= minVal {
			return parsed
		}
	}
	return fallback
}

func envInt(key string, fallback int) int {
	return envIntMin(key, fallback, 1)
}

// LoadConfig populates Config from AIR_* environment variables and CLI arguments.
func LoadConfig(args []string) Config {
	c := DefaultConfig()
	c.GatewayAddr = envOr("AIR_GATEWAY_ADDR", c.GatewayAddr)
	c.AdminAddr = envOr("AIR_GATEWAY_ADMIN_ADDR", c.AdminAddr)
	c.AdminGRPCAddr = envOr("AIR_GATEWAY_ADMIN_GRPC_ADDR", c.AdminGRPCAddr)
	c.MetaDSN = envOr("AIR_META_DSN", "")
	c.MetaPort = envInt("AIR_META_PORT", c.MetaPort)
	c.ControlPlaneURL = envOr("AIR_CONTROL_PLANE_URL", c.ControlPlaneURL)
	c.GRPCAddr = envOr("AIR_DB_GRPC_ADDR", c.GRPCAddr)
	c.InternalToken = envOr("AIR_INTERNAL_TOKEN", "")
	c.BackendHost = envOr("AIR_BACKEND_HOST", "")
	c.RequireTLS = envBool("AIR_REQUIRE_TLS", c.RequireTLS)
	c.TLSCertPath = envOr("AIR_TLS_CERT", "")
	c.TLSKeyPath = envOr("AIR_TLS_KEY", "")
	c.PoolMode = envBool("AIR_POOL_MODE", c.PoolMode)
	c.PoolMaxBackends = envInt("AIR_POOL_MAX_BACKENDS", c.PoolMaxBackends)
	c.PoolIdleTimeoutSec = envInt("AIR_POOL_IDLE_TIMEOUT_SEC", c.PoolIdleTimeoutSec)
	c.AuthTimeoutMS = envInt("AIR_AUTH_TIMEOUT_MS", c.AuthTimeoutMS)
	c.ClientIdleTimeoutMS = envIntMin("AIR_CLIENT_IDLE_TIMEOUT_MS", c.ClientIdleTimeoutMS, 0)
	c.MaxClientConns = envInt("AIR_MAX_CLIENT_CONNS", c.MaxClientConns)
	c.MaxTenantClientConns = envInt("AIR_MAX_TENANT_CLIENT_CONNS", c.MaxTenantClientConns)
	c.MaxUserClientConns = envInt("AIR_MAX_USER_CLIENT_CONNS", c.MaxUserClientConns)
	c.DiscoveryIntervalMS = envInt("AIR_DISCOVERY_INTERVAL_MS", c.DiscoveryIntervalMS)
	c.ReadEngineHost = envOr("AIR_READ_ENGINE_HOST", c.ReadEngineHost)
	c.ReadEngineSockDir = envOr("AIR_READ_ENGINE_SOCK_DIR", "")
	c.ReadEnginePort = envIntMin("AIR_READ_ENGINE_PORT", c.ReadEnginePort, 0)
	c.ReadEngineGRPCAddr = envOr("AIR_READ_ENGINE_GRPC_ADDR", "")
	c.PageserverGRPCAddr = envOr("AIR_PAGESERVER_GRPC_ADDR", "")
	c.MeteringLogDir = envOr("AIR_METERING_LOG_DIR", c.MeteringLogDir)
	if v := os.Getenv("AIR_METERING_MAX_FILE_BYTES"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed > 0 {
			c.MeteringMaxFileBytes = parsed
		}
	}
	if sec := envInt("AIR_METERING_FLUSH_INTERVAL_SEC", int(c.MeteringFlushInterval.Seconds())); sec > 0 {
		c.MeteringFlushInterval = time.Duration(sec) * time.Second
	}

	for i := 0; i < len(args); i++ {
		arg := args[i]
		nextStr := func(dst *string) {
			if i+1 < len(args) {
				i++
				*dst = args[i]
			}
		}
		nextInt := func(dst *int) {
			if i+1 < len(args) {
				i++
				if v, err := strconv.Atoi(args[i]); err == nil {
					*dst = v
				}
			}
		}
		switch arg {
		case "--gateway-addr":
			nextStr(&c.GatewayAddr)
		case "--gateway-admin-addr":
			nextStr(&c.AdminAddr)
		case "--gateway-admin-grpc-addr":
			nextStr(&c.AdminGRPCAddr)
		case "--meta-dsn":
			nextStr(&c.MetaDSN)
		case "--control-plane-url":
			nextStr(&c.ControlPlaneURL)
		case "--db-grpc-addr":
			nextStr(&c.GRPCAddr)
		case "--internal-token":
			nextStr(&c.InternalToken)
		case "--backend-host":
			nextStr(&c.BackendHost)
		case "--tls-cert":
			nextStr(&c.TLSCertPath)
		case "--tls-key":
			nextStr(&c.TLSKeyPath)
		case "--require-tls":
			c.RequireTLS = true
		case "--pool-mode":
			c.PoolMode = true
		case "--no-pool-mode":
			c.PoolMode = false
		case "--pool-max-backends":
			nextInt(&c.PoolMaxBackends)
		case "--pool-idle-timeout-sec":
			nextInt(&c.PoolIdleTimeoutSec)
		case "--auth-timeout-ms":
			nextInt(&c.AuthTimeoutMS)
		case "--client-idle-timeout-ms":
			nextInt(&c.ClientIdleTimeoutMS)
		case "--max-client-conns":
			nextInt(&c.MaxClientConns)
		case "--max-tenant-client-conns":
			nextInt(&c.MaxTenantClientConns)
		case "--max-user-client-conns":
			nextInt(&c.MaxUserClientConns)
		case "--read-engine-host":
			nextStr(&c.ReadEngineHost)
		case "--read-engine-sock-dir":
			nextStr(&c.ReadEngineSockDir)
		case "--read-engine-port":
			nextInt(&c.ReadEnginePort)
		case "--read-engine-grpc-addr":
			nextStr(&c.ReadEngineGRPCAddr)
		case "--pageserver-grpc-addr":
			nextStr(&c.PageserverGRPCAddr)
		case "--metering-log-dir":
			nextStr(&c.MeteringLogDir)
		case "--metering-max-file-bytes":
			if i+1 < len(args) {
				i++
				if v, err := strconv.ParseInt(args[i], 10, 64); err == nil && v > 0 {
					c.MeteringMaxFileBytes = v
				}
			}
		case "--metering-flush-interval-sec":
			if i+1 < len(args) {
				i++
				if v, err := strconv.Atoi(args[i]); err == nil && v > 0 {
					c.MeteringFlushInterval = time.Duration(v) * time.Second
				}
			}
		}
	}

	if c.MetaDSN == "" {
		c.MetaDSN = fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=postgres connect_timeout=3", c.MetaPort)
	}
	c.ControlPlaneURL = strings.TrimRight(c.ControlPlaneURL, "/")
	return c
}
