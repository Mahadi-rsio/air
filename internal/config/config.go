package config

import (
	"flag"
	"os"
	"path/filepath"
	"strconv"
)

// Config holds runtime configuration for the AIR v1 single-cell server.
type Config struct {
	DataDir       string
	MetaDir       string
	MasterKeyPath string
	PGBinDir      string
	MetaPort      int
	HTTPAddr      string
	GatewayAddr   string
}

// Default returns safe defaults rooted under dataDir.
func Default(dataDir string) Config {
	if dataDir == "" {
		dataDir = "/var/lib/air"
	}
	return Config{
		DataDir:       dataDir,
		MetaDir:       filepath.Join(dataDir, "meta"),
		MasterKeyPath: filepath.Join(dataDir, "master.key"),
		PGBinDir:      "/usr/lib/postgresql/17/bin",
		MetaPort:      15432,
		HTTPAddr:      "127.0.0.1:8080",
		GatewayAddr:   "127.0.0.1:5432",
	}
}

// Load parses flags and environment overrides into a Config.
func Load(args []string) (Config, error) {
	cfg := Default(envOr("AIR_DATA_DIR", "/tmp/air-data"))
	cfg.MetaDir = envOr("AIR_META_DIR", cfg.MetaDir)
	cfg.MasterKeyPath = envOr("AIR_MASTER_KEY_PATH", cfg.MasterKeyPath)
	cfg.PGBinDir = envOr("AIR_PG_BIN_DIR", cfg.PGBinDir)
	cfg.HTTPAddr = envOr("AIR_HTTP_ADDR", cfg.HTTPAddr)
	cfg.GatewayAddr = envOr("AIR_GATEWAY_ADDR", cfg.GatewayAddr)
	if p := os.Getenv("AIR_META_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil && v > 0 {
			cfg.MetaPort = v
		}
	}

	fs := flag.NewFlagSet("air-server", flag.ContinueOnError)
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "Root directory for AIR persistent tenant and metadata storage")
	fs.StringVar(&cfg.MetaDir, "meta-dir", cfg.MetaDir, "Metadata PostgreSQL directory")
	fs.StringVar(&cfg.MasterKeyPath, "master-key-path", cfg.MasterKeyPath, "Path to 32-byte master KEK file")
	fs.StringVar(&cfg.PGBinDir, "pg-bin-dir", cfg.PGBinDir, "Path to PostgreSQL 17 bin directory")
	fs.IntVar(&cfg.MetaPort, "meta-port", cfg.MetaPort, "TCP port for internal Control Plane metadata PostgreSQL instance")
	fs.StringVar(&cfg.HTTPAddr, "http-addr", cfg.HTTPAddr, "Control Plane HTTP API listen address")
	fs.StringVar(&cfg.GatewayAddr, "gateway-addr", cfg.GatewayAddr, "PostgreSQL wire-protocol gateway listen address")
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
