package meshtransport

import (
	"crypto/tls"
	"errors"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgconn"
)

// ValidateMetadataDSN requires server identity verification for remote metadata connections.
func ValidateMetadataDSN(dsn string) error {
	if dsn == "" {
		return nil
	}
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		// pgconn parse errors can contain connection string parameters.
		return errors.New("invalid metadata database connection configuration")
	}
	if !secureMetadataEndpoint(cfg.Host, cfg.TLSConfig) {
		return errors.New("remote metadata database connections require sslmode=verify-full")
	}
	for _, fallback := range cfg.Fallbacks {
		if !secureMetadataEndpoint(fallback.Host, fallback.TLSConfig) {
			return errors.New("remote metadata database fallbacks require sslmode=verify-full")
		}
	}
	return nil
}

func secureMetadataEndpoint(host string, cfg *tls.Config) bool {
	return filepath.IsAbs(host) || isLoopbackHost(host) ||
		(cfg != nil && !cfg.InsecureSkipVerify && cfg.ServerName == host)
}
