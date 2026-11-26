package gateway

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"time"
)

// TLSContext manages the server tls.Config for PostgreSQL SSLRequest upgrades.
// Uses explicit cert/key files when provided, or generates an ephemeral ECDSA P-256
// self-signed certificate for *.air.local / localhost in development.
type TLSContext struct {
	cfg *tls.Config
}

// Init initializes TLSv1.2+ server credentials from certPath/keyPath or an ephemeral self-signed cert.
func (t *TLSContext) Init(certPath, keyPath string) error {
	var cert tls.Certificate
	if certPath != "" && keyPath != "" {
		var err error
		cert, err = tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return fmt.Errorf("failed to load TLS cert/key: %w", err)
		}
	} else {
		var err error
		cert, err = generateSelfSignedCert()
		if err != nil {
			return err
		}
	}
	t.cfg = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	return nil
}

// UpgradeConn performs a TLS handshake on conn and returns the upgraded *tls.Conn and optional SNI host.
func (t *TLSContext) UpgradeConn(conn net.Conn) (*tls.Conn, string, error) {
	if t.cfg == nil {
		return nil, "", fmt.Errorf("TLSContext not initialized")
	}
	tlsConn := tls.Server(conn, t.cfg)
	if err := tlsConn.Handshake(); err != nil {
		return nil, "", err
	}
	return tlsConn, tlsConn.ConnectionState().ServerName, nil
}

func generateSelfSignedCert() (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("ECDSA P-256 keygen failed: %w", err)
	}
	now := time.Now()
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("serial number generation failed: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: "*.air.local"},
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"*.air.local", "localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("x509.CreateCertificate failed: %w", err)
	}
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
	}, nil
}
