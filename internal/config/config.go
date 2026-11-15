package config

import (
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Config holds runtime configuration for the AIR single-cell server.
type Config struct {
	DataDir            string
	MetaDir            string
	MasterKeyPath      string
	PGBinDir           string
	MetaPort           int
	HTTPAddr           string
	GatewayAddr        string
	BootstrapAPIKey    string
	BootstrapAccountID string
	RequireTLS         bool
	TLSCertPath        string
	TLSKeyPath         string
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
		BootstrapAccountID: "acct_default",
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
	cfg.BootstrapAPIKey = envOr("AIR_BOOTSTRAP_API_KEY", cfg.BootstrapAPIKey)
	cfg.BootstrapAccountID = envOr("AIR_BOOTSTRAP_ACCOUNT_ID", cfg.BootstrapAccountID)
	cfg.TLSCertPath = envOr("AIR_TLS_CERT", cfg.TLSCertPath)
	cfg.TLSKeyPath = envOr("AIR_TLS_KEY", cfg.TLSKeyPath)
	if v := strings.TrimSpace(os.Getenv("AIR_REQUIRE_TLS")); v != "" {
		cfg.RequireTLS = v == "1" || strings.EqualFold(v, "true")
	}
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
	fs.StringVar(&cfg.BootstrapAPIKey, "bootstrap-api-key", cfg.BootstrapAPIKey, "Optional bootstrap Bearer API key to seed on startup")
	fs.StringVar(&cfg.BootstrapAccountID, "bootstrap-account-id", cfg.BootstrapAccountID, "Account ID bound to --bootstrap-api-key")
	fs.BoolVar(&cfg.RequireTLS, "require-tls", cfg.RequireTLS, "Require SSL/TLS negotiation on all Gateway client connections")
	fs.StringVar(&cfg.TLSCertPath, "tls-cert", cfg.TLSCertPath, "Optional path to PEM TLS certificate for Gateway")
	fs.StringVar(&cfg.TLSKeyPath, "tls-key", cfg.TLSKeyPath, "Optional path to PEM TLS private key for Gateway")
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
