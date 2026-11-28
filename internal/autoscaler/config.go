package autoscaler

import (
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ModeDecisionOnly = "decision_only"
	ModeEnforce      = "enforce"
)

// Config holds runtime configuration for the standalone Fly.io gateway autoscaler.
type Config struct {
	AppName                string
	FlyAPIBase             string
	FlyAPIToken            string
	Region                 string
	InternalToken          string
	Mode                   string
	AutoAddEnabled         bool
	AutoRemoveEnabled      bool
	BaselineMachineID      string
	MaxReplicas            int
	SustainedHighTicks     int
	SustainedLowTicks      int
	ScaleOutCooldown       time.Duration
	ScaleInCooldown        time.Duration
	HighConnUtilPct        float64
	LowConnUtilPct         float64
	MinSpareConns          int
	BurstCPUKind           string
	BurstCPUs              int
	BurstMemoryMB          int
	BurstVolumeSizeGB      int
	DrainTimeout           time.Duration
	ReadinessMaxAttempts   int
	ReadinessRetryInterval time.Duration
	GatewayPrivateHTTPPort int
	GatewayPGPort          int
	GatewayTLSServerName   string
	GatewayTLSCAPath       string
	StateDir               string
	HTTPAddr               string
	TickInterval           time.Duration
	FlyAPITimeout          time.Duration
}

// DefaultConfig returns safe defaults: decision_only mode with automatic scale-in disabled.
func DefaultConfig() Config {
	return Config{
		AppName:                "air-pg-gateway",
		FlyAPIBase:             "https://api.machines.dev/v1",
		Region:                 "sin",
		Mode:                   ModeDecisionOnly,
		AutoAddEnabled:         true,
		AutoRemoveEnabled:      false,
		MaxReplicas:            2,
		SustainedHighTicks:     3,
		SustainedLowTicks:      5,
		ScaleOutCooldown:       300 * time.Second,
		ScaleInCooldown:        600 * time.Second,
		HighConnUtilPct:        75,
		LowConnUtilPct:         25,
		MinSpareConns:          64,
		BurstCPUKind:           "shared",
		BurstCPUs:              4,
		BurstMemoryMB:          1024,
		BurstVolumeSizeGB:      1,
		DrainTimeout:           30 * time.Second,
		ReadinessMaxAttempts:   8,
		ReadinessRetryInterval: 1500 * time.Millisecond,
		GatewayPrivateHTTPPort: 8082,
		GatewayPGPort:          5432,
		GatewayTLSServerName:   "air-pg-gateway.fly.dev",
		StateDir:               "/var/lib/air-autoscaler",
		HTTPAddr:               "[::]:8080",
		TickInterval:           60 * time.Second,
		FlyAPITimeout:          15 * time.Second,
	}
}

func envStr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
	}
	return fallback
}

func envInt(key string, fallback, minVal int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= minVal {
			return n
		}
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

// LoadConfig loads autoscaler configuration from environment variables.
func LoadConfig() Config {
	c := DefaultConfig()
	c.AppName = envStr("FLY_APP_NAME", c.AppName)
	c.FlyAPIBase = strings.TrimRight(envStr("FLY_API_BASE", c.FlyAPIBase), "/")
	c.FlyAPIToken = envStr("FLY_API_TOKEN", "")
	c.Region = envStr("FLY_REGION", c.Region)
	c.InternalToken = envStr("AIR_INTERNAL_TOKEN", "")

	mode := strings.ToLower(envStr("AUTOSCALER_MODE", c.Mode))
	if mode == ModeEnforce {
		c.Mode = ModeEnforce
	} else {
		c.Mode = ModeDecisionOnly
	}

	c.AutoAddEnabled = envBool("AUTO_ADD_ENABLED", c.AutoAddEnabled)
	c.AutoRemoveEnabled = envBool("AUTO_REMOVE_ENABLED", c.AutoRemoveEnabled)
	c.BaselineMachineID = envStr("BASELINE_MACHINE_ID", "")
	c.MaxReplicas = envInt("MAX_GATEWAY_REPLICAS", c.MaxReplicas, 1)
	c.SustainedHighTicks = envInt("SUSTAINED_HIGH_TICKS", c.SustainedHighTicks, 1)
	c.SustainedLowTicks = envInt("SUSTAINED_LOW_TICKS", c.SustainedLowTicks, 1)
	c.ScaleOutCooldown = time.Duration(envInt("SCALE_OUT_COOLDOWN_SEC", int(c.ScaleOutCooldown.Seconds()), 0)) * time.Second
	c.ScaleInCooldown = time.Duration(envInt("SCALE_IN_COOLDOWN_SEC", int(c.ScaleInCooldown.Seconds()), 0)) * time.Second
	c.HighConnUtilPct = envFloat("HIGH_CONN_UTIL_PCT", c.HighConnUtilPct)
	c.LowConnUtilPct = envFloat("LOW_CONN_UTIL_PCT", c.LowConnUtilPct)
	c.MinSpareConns = envInt("MIN_SPARE_CONNS", c.MinSpareConns, 0)
	c.BurstCPUKind = envStr("BURST_CPU_KIND", c.BurstCPUKind)
	c.BurstCPUs = envInt("BURST_CPUS", c.BurstCPUs, 1)
	c.BurstMemoryMB = envInt("BURST_MEMORY_MB", c.BurstMemoryMB, 256)
	c.BurstVolumeSizeGB = envInt("BURST_VOLUME_SIZE_GB", c.BurstVolumeSizeGB, 1)
	c.DrainTimeout = time.Duration(envInt("DRAIN_TIMEOUT_MS", int(c.DrainTimeout.Milliseconds()), 1000)) * time.Millisecond
	c.ReadinessMaxAttempts = envInt("READINESS_MAX_ATTEMPTS", c.ReadinessMaxAttempts, 1)
	c.ReadinessRetryInterval = time.Duration(envInt("READINESS_RETRY_MS", int(c.ReadinessRetryInterval.Milliseconds()), 0)) * time.Millisecond
	c.GatewayPrivateHTTPPort = envInt("GATEWAY_PRIVATE_HTTP_PORT", c.GatewayPrivateHTTPPort, 1)
	c.GatewayPGPort = envInt("GATEWAY_PG_PORT", c.GatewayPGPort, 1)
	c.GatewayTLSServerName = envStr("AIR_GATEWAY_TLS_SERVER_NAME", c.AppName+".fly.dev")
	c.GatewayTLSCAPath = envStr("AIR_GATEWAY_TLS_CA", "")
	c.StateDir = envStr("AUTOSCALER_STATE_DIR", c.StateDir)
	c.HTTPAddr = envStr("AUTOSCALER_HTTP_ADDR", c.HTTPAddr)
	c.TickInterval = time.Duration(envInt("AUTOSCALER_TICK_INTERVAL_SEC", int(c.TickInterval.Seconds()), 1)) * time.Second
	c.FlyAPITimeout = time.Duration(envInt("FLY_API_TIMEOUT_MS", int(c.FlyAPITimeout.Milliseconds()), 500)) * time.Millisecond
	return c
}
