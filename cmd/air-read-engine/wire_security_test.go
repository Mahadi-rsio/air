package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/readadapter"
)

func TestReadEngineWireRequiresGatewayCertificate(t *testing.T) {
	now := time.Now()
	ca := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, caKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	issue := func(serial int64, name string, use x509.ExtKeyUsage) tls.Certificate {
		t.Helper()
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			DNSNames:     []string{name},
			NotBefore:    now.Add(-time.Minute),
			NotAfter:     now.Add(time.Hour),
			ExtKeyUsage:  []x509.ExtKeyUsage{use},
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, key.Public(), caKey)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}
	serverTLS := gatewayListenerTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{issue(2, "air-read-engine", x509.ExtKeyUsageServerAuth)},
		ClientCAs:    roots,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	})
	server, err := readadapter.NewSQLiteReadServer(readadapter.SQLiteServerConfig{
		DataDir: t.TempDir(),
		WireTLS: serverTLS,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	startupBody := append([]byte{0, 3, 0, 0}, []byte("user\x00postgres\x00database\x00postgres\x00\x00")...)
	startup := binary.BigEndian.AppendUint32(nil, uint32(4+len(startupBody)))
	startup = append(startup, startupBody...)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(server.Port()))
	for _, tc := range []struct {
		name string
		cert []tls.Certificate
		want bool
	}{
		{name: "missing certificate"},
		{name: "other mesh service", cert: []tls.Certificate{issue(3, "air-server", x509.ExtKeyUsageClientAuth)}},
		{name: "gateway", cert: []tls.Certificate{issue(4, "air-gateway", x509.ExtKeyUsageClientAuth)}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			client := tls.Client(conn, &tls.Config{
				MinVersion:   tls.VersionTLS13,
				RootCAs:      roots,
				ServerName:   "air-read-engine",
				Certificates: tc.cert,
			})
			var reply [9]byte
			if _, err = client.Write(startup); err == nil {
				_, err = io.ReadFull(client, reply[:])
			}
			authOK := err == nil && bytes.Equal(reply[:], []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0})
			if authOK != tc.want {
				t.Fatalf("authentication succeeded = %t, want %t (reply %x, error %v)", authOK, tc.want, reply, err)
			}
		})
	}
}
