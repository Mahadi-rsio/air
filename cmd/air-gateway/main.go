package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Mahadi-rsio/air/internal/gateway"
)

func main() {
	cfg := gateway.LoadConfig(os.Args[1:])

	var tlsCtx gateway.TLSContext
	if err := tlsCtx.Init(cfg.TLSCertPath, cfg.TLSKeyPath); err != nil {
		log.Fatalf("[air-gateway] TLS init failed: %v", err)
	}

	metrics := gateway.NewMetricsRegistry(cfg.MaxClientConns, cfg.MaxTenantClientConns, cfg.MaxUserClientConns)
	pools := gateway.NewPoolManager(cfg.PoolMaxBackends, cfg.PoolIdleTimeoutSec)
	if cfg.ReadEnginePort > 0 || cfg.ReadEngineSockDir != "" {
		pools.SetReadEngineEndpoint(cfg.ReadEngineHost, cfg.ReadEngineSockDir, cfg.ReadEnginePort)
	}
	cancelReg := gateway.NewCancelRegistry()

	discovery, err := gateway.NewTenantDiscovery(cfg)
	if err != nil {
		log.Fatalf("[air-gateway] discovery init failed: %v", err)
	}
	discovery.SetMetrics(metrics)
	discovery.SetCallbacks(
		func(r gateway.TenantRoute) { pools.UpsertDiscovered(r) },
		func(tid string) { pools.RemoveTenant(tid) },
	)
	discovery.Start()

	adminSrv := gateway.NewAdminHTTPServer(cfg.AdminAddr, cfg.InternalToken, discovery, pools, metrics)
	if err := adminSrv.Start(); err != nil {
		discovery.Stop()
		log.Fatalf("[air-gateway] failed to bind admin HTTP on %s: %v", cfg.AdminAddr, err)
	}

	adminGRPCSrv := gateway.NewAdminGRPCServer(cfg.AdminGRPCAddr, cfg.InternalToken, pools, metrics)
	if cfg.AdminGRPCAddr != "" {
		if _, err := adminGRPCSrv.Start(); err != nil {
			adminSrv.Stop()
			discovery.Stop()
			log.Fatalf("[air-gateway] failed to bind admin gRPC on %s: %v", cfg.AdminGRPCAddr, err)
		}
	}

	ln, err := net.Listen("tcp", cfg.GatewayAddr)
	if err != nil {
		adminGRPCSrv.Stop()
		adminSrv.Stop()
		discovery.Stop()
		log.Fatalf("[air-gateway] failed to listen on %s: %v", cfg.GatewayAddr, err)
	}

	adminGRPCLabel := "disabled"
	if cfg.AdminGRPCAddr != "" {
		adminGRPCLabel = cfg.AdminGRPCAddr
	}
	readEngineLabel := "disabled"
	if cfg.ReadEnginePort > 0 {
		readEngineLabel = fmt.Sprintf("%s:%d", cfg.ReadEngineHost, cfg.ReadEnginePort)
	}
	log.Printf("[air-gateway] Go PG Gateway listening on %s (admin=%s, admin_grpc=%s, db_grpc=%s, pool_mode=%v, max_client_conns=%d, max_tenant_client_conns=%d, max_user_client_conns=%d, read_engine=%s, require_tls=%v)",
		cfg.GatewayAddr, cfg.AdminAddr, adminGRPCLabel, cfg.GRPCAddr,
		cfg.PoolMode, cfg.MaxClientConns, cfg.MaxTenantClientConns, cfg.MaxUserClientConns,
		readEngineLabel, cfg.RequireTLS,
	)

	var running atomic.Bool
	running.Store(true)

	reaperStop := make(chan struct{})
	var reaperWG sync.WaitGroup
	reaperWG.Add(1)
	go func() {
		defer reaperWG.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-reaperStop:
				return
			case <-ticker.C:
				pools.EvictExpiredIdle()
			}
		}
	}()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		running.Store(false)
		_ = ln.Close()
	}()

	var clientsWG sync.WaitGroup
	for running.Load() {
		conn, err := ln.Accept()
		if err != nil {
			if !running.Load() {
				break
			}
			continue
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		if !metrics.TryAcquireGlobal() {
			_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
			_, _ = conn.Write(gateway.BuildErrorResponse("FATAL", "53300", "sorry, too many clients already (gateway global connection limit reached)"))
			_ = conn.Close()
			continue
		}
		clientsWG.Add(1)
		go func(c net.Conn) {
			defer clientsWG.Done()
			gateway.HandleClientConnection(c, cfg, &tlsCtx, discovery, pools, metrics, cancelReg)
		}(conn)
	}

	log.Println("[air-gateway] shutting down...")
	close(reaperStop)
	reaperWG.Wait()
	adminGRPCSrv.Stop()
	adminSrv.Stop()
	discovery.Stop()
	pools.CloseAll()

	done := make(chan struct{})
	go func() {
		clientsWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
}
