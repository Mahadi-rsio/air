package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/config"
	"github.com/Mahadi-rsio/air/internal/control"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/pageserver"
)

func TestConfigurePageserver_FailureModes(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	ag, err := cell.NewAgent(tmpDir, "/nonexistent-pg-bin", kms)
	if err != nil {
		t.Fatal(err)
	}
	defer ag.Close()

	// ControlPlane with nil DB is sufficient for SetPageserver/SetPageserverClient checks in configurePageserver.
	cp := &control.ControlPlane{}

	// 1. Unavailable remote pageserver at startup must fail fast after timeout.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := ln.Addr().String()
	_ = ln.Close()

	cfgUnavail := config.Default(tmpDir)
	cfgUnavail.PageserverGRPCAddr = deadAddr
	if _, err := configurePageserver(ctx, cp, ag, cfgUnavail, kms, nil, 300*time.Millisecond); err == nil {
		t.Fatalf("expected configurePageserver to fail when remote pageserver is unavailable")
	}

	// 2. Empty socket path when PageserverEnabled=true must fail.
	cfgEmptySock := config.Default(tmpDir)
	cfgEmptySock.PageserverEnabled = true
	cfgEmptySock.PageserverSocket = ""
	if _, err := configurePageserver(ctx, cp, ag, cfgEmptySock, kms, nil, time.Second); err == nil {
		t.Fatalf("expected configurePageserver to fail when PageserverEnabled=true and PageserverSocket is empty")
	}

	// 3. Missing air_smgr.so when PageserverEnabled=true must fail rather than silently falling back.
	cfgNoSmgr := config.Default(tmpDir)
	cfgNoSmgr.PGBinDir = "/nonexistent-pg-bin"
	cfgNoSmgr.PageserverEnabled = true
	cfgNoSmgr.PageserverSocket = filepath.Join(tmpDir, "ps.sock")
	if _, err := configurePageserver(ctx, cp, ag, cfgNoSmgr, kms, nil, time.Second); err == nil {
		t.Fatalf("expected configurePageserver to fail when air_smgr.so is missing")
	}
	if ag.IsPageserverEnabled() {
		t.Fatalf("expected agent pageserver config to be rolled back on error")
	}

	// 4. Auth failure against live remote pageserver must fail immediately.
	ps, err := pageserver.New(pageserver.Config{
		DisableUnixSocket: true,
		LayerDir:          filepath.Join(tmpDir, "layers"),
		CacheDir:          filepath.Join(tmpDir, "cache"),
		CacheMaxBytes:     16 * 1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcSrv := pageserver.NewGRPCServer(ps, "secret-token")
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	cfgBadAuth := config.Default(tmpDir)
	cfgBadAuth.PageserverGRPCAddr = lis.Addr().String()
	cfgBadAuth.InternalToken = "wrong-token"
	start := time.Now()
	if _, err := configurePageserver(ctx, cp, ag, cfgBadAuth, kms, nil, 5*time.Second); err == nil {
		t.Fatalf("expected configurePageserver to fail on bad token")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("expected auth failure to fail fast without waiting for full timeout, took %s", time.Since(start))
	}
}

func TestConfigurePageserver_RemoteBridgeWiresAgent(t *testing.T) {
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("/tmp", "air-p21-cmd-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a fake pg_bin/../lib/air_smgr.so so HasAirSmgr() succeeds in unit test.
	fakePG := filepath.Join(tmpDir, "pg17")
	if err := os.MkdirAll(filepath.Join(fakePG, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(fakePG, "lib"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fakePG, "lib", "air_smgr.so"), []byte("so"), 0o644); err != nil {
		t.Fatal(err)
	}

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	ag, err := cell.NewAgent(filepath.Join(tmpDir, "vol"), filepath.Join(fakePG, "bin"), kms)
	if err != nil {
		t.Fatal(err)
	}
	defer ag.Close()
	cp := &control.ControlPlane{}

	ps, err := pageserver.New(pageserver.Config{
		DisableUnixSocket: true,
		LayerDir:          filepath.Join(tmpDir, "layers"),
		CacheDir:          filepath.Join(tmpDir, "cache"),
		CacheMaxBytes:     16 * 1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcSrv := pageserver.NewGRPCServer(ps, "good-token")
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	cfg := config.Default(filepath.Join(tmpDir, "vol"))
	cfg.PGBinDir = filepath.Join(fakePG, "bin")
	cfg.PageserverGRPCAddr = lis.Addr().String()
	cfg.InternalToken = "good-token"
	cfg.PageserverEnabled = true
	cfg.PageserverSocket = filepath.Join(tmpDir, "ps.sock")
	cfg.PageserverCacheMB = 64

	cleanup, err := configurePageserver(ctx, cp, ag, cfg, kms, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("configurePageserver: %v", err)
	}
	defer cleanup(ctx)

	if !ag.IsPageserverEnabled() || !ag.HasAirSmgr() {
		t.Fatalf("expected agent pageserver + air_smgr to be enabled")
	}
	if _, err := os.Stat(cfg.PageserverSocket); err != nil {
		t.Fatalf("expected pageserver unix socket bridge %s to exist: %v", cfg.PageserverSocket, err)
	}
}
