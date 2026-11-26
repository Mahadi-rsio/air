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
	MeteringDirExplicit   bool
	BillingEnabled        bool
	GatewayID             string
	MeteringMaxFileBytes  int64
	MeteringMaxSpoolBytes int64
	MeteringFlushInterval time.Duration
	MeteringSyncOnWrite   bool
	CordonedOnStart       bool
	GatewayPeers          []string
	GatewayDiscoveryHost  string
	PeerAdminPort         int
	PrivateHTTPAddr       string
	MaxAggregateBackends  int

	// Phase 20: Resource-Aware Write Governor
	WriteGovernorEnabled    bool
	WriteMaxGlobal          int
	WriteMinGlobal          int
	WriteMaxPerTenant       int
	WriteQueueMax           int
	WriteQueueMaxBytes      int64
	WriteQueueTimeout       time.Duration
	WriteControllerInterval time.Duration
	WriteBufferGlobalMax    int64
	WriteBufferSessionMax   int64
	WriteAdaptiveEnabled    bool
	WriteLazyLSNEnabled     bool
}

// DefaultConfig returns a Config initialized with default values.
func DefaultConfig() Config {
	return Config{
		GatewayAddr:             "127.0.0.1:5432",
		AdminAddr:               "127.0.0.1:8081",
		MetaPort:                15432,
		ControlPlaneURL:         "http://127.0.0.1:8080",
		GRPCAddr:                "127.0.0.1:9090",
		PoolMode:                true,
		PoolMaxBackends:         16,
		PoolIdleTimeoutSec:      300,
		AuthTimeoutMS:           5000,
		ClientIdleTimeoutMS:     0,
		MaxClientConns:          1024,
		MaxTenantClientConns:    128,
		MaxUserClientConns:      0,
		DiscoveryIntervalMS:     2000,
		ReadEngineHost:          "127.0.0.1",
		ReadEnginePort:          5433,
		MeteringLogDir:          filepath.Join(os.TempDir(), "air-gateway-metering"),
		MeteringMaxFileBytes:    10 * 1024 * 1024,
		MeteringMaxSpoolBytes:   256 * 1024 * 1024,
		MeteringFlushInterval:   2 * time.Minute,
		PeerAdminPort:           9093,
		WriteGovernorEnabled:    true,
		WriteMaxGlobal:          32,
		WriteMinGlobal:          2,
		WriteMaxPerTenant:       8,
		WriteQueueMax:           256,
		WriteQueueMaxBytes:      64 * 1024 * 1024,
		WriteQueueTimeout:       5 * time.Second,
		WriteControllerInterval: 1 * time.Second,
		WriteBufferGlobalMax:    64 * 1024 * 1024,
		WriteBufferSessionMax:   8 * 1024 * 1024,
		WriteAdaptiveEnabled:    false,
		WriteLazyLSNEnabled:     true,
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
	if v := strings.TrimSpace(os.Getenv("AIR_METERING_LOG_DIR")); v != "" {
		c.MeteringLogDir = v
		c.MeteringDirExplicit = true
	}
	c.BillingEnabled = envBool("AIR_BILLING_ENABLED", false)
	c.GatewayID = envOr("AIR_GATEWAY_ID", envOr("FLY_MACHINE_ID", ""))
	if c.GatewayID == "" {
		if hn, err := os.Hostname(); err == nil && hn != "" {
			c.GatewayID = hn
		}
	}
	if v := os.Getenv("AIR_METERING_MAX_FILE_BYTES"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed > 0 {
			c.MeteringMaxFileBytes = parsed
		}
	}
	if v := os.Getenv("AIR_METERING_MAX_SPOOL_BYTES"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed >= 0 {
			c.MeteringMaxSpoolBytes = parsed
		}
	}
	if sec := envInt("AIR_METERING_FLUSH_INTERVAL_SEC", int(c.MeteringFlushInterval.Seconds())); sec > 0 {
		c.MeteringFlushInterval = time.Duration(sec) * time.Second
	}
	c.MeteringSyncOnWrite = envBool("AIR_METERING_SYNC_ON_WRITE", false)
	c.CordonedOnStart = envBool("AIR_GATEWAY_CORDONED", false)
	if peers := strings.TrimSpace(os.Getenv("AIR_GATEWAY_PEERS")); peers != "" {
		for _, p := range strings.Split(peers, ",") {
			if p = strings.TrimSpace(p); p != "" {
				c.GatewayPeers = append(c.GatewayPeers, p)
			}
		}
	}
	c.GatewayDiscoveryHost = envOr("AIR_GATEWAY_DISCOVERY_HOST", "")
	c.PeerAdminPort = envInt("AIR_GATEWAY_PEER_ADMIN_PORT", c.PeerAdminPort)
	c.PrivateHTTPAddr = envOr("AIR_GATEWAY_PRIVATE_HTTP_ADDR", "")
	c.MaxAggregateBackends = envIntMin("AIR_MAX_AGGREGATE_BACKENDS", 0, 0)

	c.WriteGovernorEnabled = envBool("AIR_WRITE_GOVERNOR_ENABLED", c.WriteGovernorEnabled)
	c.WriteMaxGlobal = envInt("AIR_WRITE_MAX_GLOBAL", c.WriteMaxGlobal)
	c.WriteMinGlobal = envInt("AIR_WRITE_MIN_GLOBAL", c.WriteMinGlobal)
	c.WriteMaxPerTenant = envInt("AIR_WRITE_MAX_PER_TENANT", c.WriteMaxPerTenant)
	c.WriteQueueMax = envIntMin("AIR_WRITE_QUEUE_MAX", c.WriteQueueMax, 0)
	if v := os.Getenv("AIR_WRITE_QUEUE_MAX_BYTES"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed >= 0 {
			c.WriteQueueMaxBytes = parsed
		}
	}
	if v := os.Getenv("AIR_WRITE_QUEUE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			c.WriteQueueTimeout = d
		}
	}
	if v := os.Getenv("AIR_WRITE_CONTROLLER_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			c.WriteControllerInterval = d
		}
	}
	if v := os.Getenv("AIR_WRITE_BUFFER_GLOBAL_MAX"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed >= 0 {
			c.WriteBufferGlobalMax = parsed
		}
	}
	if v := os.Getenv("AIR_WRITE_BUFFER_SESSION_MAX"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed >= 0 {
			c.WriteBufferSessionMax = parsed
		}
	}
	c.WriteAdaptiveEnabled = envBool("AIR_WRITE_ADAPTIVE_ENABLED", c.WriteAdaptiveEnabled)
	c.WriteLazyLSNEnabled = envBool("AIR_WRITE_LAZY_LSN_ENABLED", c.WriteLazyLSNEnabled)

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
			c.MeteringDirExplicit = true
		case "--billing-enabled":
			c.BillingEnabled = true
		case "--gateway-id":
			nextStr(&c.GatewayID)
		case "--cordoned":
			c.CordonedOnStart = true
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
		case "--write-governor":
			c.WriteGovernorEnabled = true
		case "--no-write-governor":
			c.WriteGovernorEnabled = false
		case "--write-max-global":
			nextInt(&c.WriteMaxGlobal)
		case "--write-min-global":
			nextInt(&c.WriteMinGlobal)
		case "--write-max-per-tenant":
			nextInt(&c.WriteMaxPerTenant)
		case "--write-queue-max":
			nextInt(&c.WriteQueueMax)
		case "--write-adaptive":
			c.WriteAdaptiveEnabled = true
		case "--no-write-adaptive":
			c.WriteAdaptiveEnabled = false
		}
	}

	if c.MetaDSN == "" {
		c.MetaDSN = fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=postgres connect_timeout=3", c.MetaPort)
	}
	c.ControlPlaneURL = strings.TrimRight(c.ControlPlaneURL, "/")
	c.NormalizeWriteGovernor()
	return c
}

// NormalizeWriteGovernor clamps invalid Write Governor settings to safe bounds.
func (c *Config) NormalizeWriteGovernor() {
	if c.WriteMinGlobal <= 0 {
		c.WriteMinGlobal = 1
	}
	if c.WriteMaxGlobal <= 0 {
		c.WriteMaxGlobal = 32
	}
	if c.WriteMinGlobal > c.WriteMaxGlobal {
		c.WriteMinGlobal = c.WriteMaxGlobal
	}
	if c.WriteMaxPerTenant <= 0 {
		c.WriteMaxPerTenant = c.WriteMaxGlobal
	}
	if c.WriteMaxPerTenant > c.WriteMaxGlobal {
		c.WriteMaxPerTenant = c.WriteMaxGlobal
	}
	if c.WriteQueueMax < 0 {
		c.WriteQueueMax = 0
	}
	if c.WriteQueueMaxBytes < 0 {
		c.WriteQueueMaxBytes = 0
	}
	if c.WriteQueueTimeout <= 0 {
		c.WriteQueueTimeout = 5 * time.Second
	}
	if c.WriteControllerInterval <= 0 {
		c.WriteControllerInterval = 1 * time.Second
	}
	if c.WriteBufferSessionMax > 0 && c.WriteBufferGlobalMax > 0 && c.WriteBufferSessionMax > c.WriteBufferGlobalMax {
		c.WriteBufferSessionMax = c.WriteBufferGlobalMax
	}
}

// Validate checks configuration invariants and returns an error if invalid.
func (c Config) Validate() error {
	if c.WriteMinGlobal <= 0 {
		return fmt.Errorf("write_min_global must be > 0, got %d", c.WriteMinGlobal)
	}
	if c.WriteMaxGlobal < c.WriteMinGlobal {
		return fmt.Errorf("write_max_global (%d) must be >= write_min_global (%d)", c.WriteMaxGlobal, c.WriteMinGlobal)
	}
	if c.WriteMaxPerTenant <= 0 {
		return fmt.Errorf("write_max_per_tenant must be > 0, got %d", c.WriteMaxPerTenant)
	}
	if c.WriteQueueTimeout <= 0 {
		return fmt.Errorf("write_queue_timeout must be > 0")
	}
	if c.WriteControllerInterval <= 0 {
		return fmt.Errorf("write_controller_interval must be > 0")
	}
	if c.WriteBufferSessionMax > 0 && c.WriteBufferGlobalMax > 0 && c.WriteBufferSessionMax > c.WriteBufferGlobalMax {
		return fmt.Errorf("write_buffer_session_max (%d) cannot exceed write_buffer_global_max (%d)", c.WriteBufferSessionMax, c.WriteBufferGlobalMax)
	}
	return nil
}
