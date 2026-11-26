// Package meshtransport configures authenticated transport for AIR services.
package meshtransport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func LoadTLS() (*tls.Config, error) {
	caPath := os.Getenv("AIR_MESH_TLS_CA")
	certPath := os.Getenv("AIR_MESH_TLS_CERT")
	keyPath := os.Getenv("AIR_MESH_TLS_KEY")
	if caPath == "" && certPath == "" && keyPath == "" {
		return nil, nil
	}
	if caPath == "" || certPath == "" || keyPath == "" {
		return nil, fmt.Errorf("AIR_MESH_TLS_CA, AIR_MESH_TLS_CERT and AIR_MESH_TLS_KEY must be set together")
	}

	pem, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read mesh CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("mesh CA file has no certificates")
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load mesh key pair: %w", err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		RootCAs:      roots,
		ClientCAs:    roots,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, nil
}

func RequireForAddress(addr string, cfg *tls.Config) error {
	if addr == "" || cfg != nil {
		return nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err == nil && isLoopbackHost(host) {
		return nil
	}
	return fmt.Errorf("mesh TLS is required for non-loopback address %q", addr)
}

func isLoopbackHost(host string) bool {
	return host == "localhost" || net.ParseIP(host).IsLoopback()
}

func ServerOptions(cfg *tls.Config) []grpc.ServerOption {
	if cfg == nil {
		return nil
	}
	return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(cfg))}
}

func DialOption(addr string) (grpc.DialOption, error) {
	cfg, err := LoadTLS()
	if err != nil {
		return nil, err
	}
	if err := RequireForAddress(addr, cfg); err != nil {
		return nil, err
	}
	if cfg == nil {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(cfg)), nil
}
