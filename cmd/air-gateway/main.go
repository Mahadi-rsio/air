package main

import (
	"context"
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
	if err := cfg.Validate(); err != nil {
		log.Fatalf("[air-gateway] invalid config: %v", err)
	}

	var tlsCtx gateway.TLSContext
	if err := tlsCtx.Init(cfg.TLSCertPath, cfg.TLSKeyPath); err != nil {
		log.Fatalf("[air-gateway] TLS init failed: %v", err)
	}

	metrics := gateway.NewMetricsRegistry(cfg.MaxClientConns, cfg.MaxTenantClientConns, cfg.MaxUserClientConns)
	pools := gateway.NewPoolManager(cfg.PoolMaxBackends, cfg.PoolIdleTimeoutSec)

	bufBudget := gateway.NewBufferBudget(cfg.WriteBufferGlobalMax)
	writeGov := gateway.NewWriteGovernor(cfg)
	telStore := gateway.NewTelemetryStore(15 * time.Second)
	pools.SetBufferBudget(bufBudget, cfg.WriteBufferSessionMax, 4096)
	pools.SetWriteGovernor(writeGov)
	pools.SetLazyLSN(cfg.WriteLazyLSNEnabled)
	metrics.SetWriteGovernorComponents(writeGov, bufBudget, telStore)

	var adaptiveCtrl *gateway.AdaptiveController
	if cfg.WriteGovernorEnabled && cfg.WriteAdaptiveEnabled {
		adaptiveCtrl = gateway.NewAdaptiveController(writeGov, telStore, cfg.WriteControllerInterval)
		adaptiveCtrl.Start()
	}
	var meteringSaver gateway.MeteringSaver
	if cfg.MetaDSN != "" {
		meteringSaver = gateway.NewPGMeteringSaver(cfg.MetaDSN)
	}
	meteringLogger, err := gateway.NewMeteringLogger(cfg.MeteringLogDir, cfg.MeteringMaxFileBytes, cfg.MeteringFlushInterval, meteringSaver)
	if err != nil {
		log.Printf("[air-gateway] metering logger disabled: %v", err)
	} else {
		meteringLogger.Start()
		pools.SetMeteringLogger(meteringLogger)
		log.Printf("[air-gateway] query metering enabled (dir=%s, max_file_bytes=%d, flush_interval=%s)",
			cfg.MeteringLogDir, cfg.MeteringMaxFileBytes, cfg.MeteringFlushInterval)
	}
	if cfg.ReadEnginePort > 0 || cfg.ReadEngineSockDir != "" {
		pools.SetReadEngineEndpoint(cfg.ReadEngineHost, cfg.ReadEngineSockDir, cfg.ReadEnginePort)
	}
	var reGRPC *gateway.ReadEngineGRPCClient
	if cfg.ReadEngineGRPCAddr != "" {
		var reErr error
		reGRPC, reErr = gateway.NewReadEngineGRPCClient(cfg.ReadEngineGRPCAddr, cfg.InternalToken)
		if reErr != nil {
			log.Printf("[air-gateway] read-engine gRPC dial warning (%s): %v", cfg.ReadEngineGRPCAddr, reErr)
		} else {
			if h, hErr := reGRPC.Health(context.Background()); hErr == nil && h.GetPgPort() > 0 && cfg.ReadEnginePort == 0 {
				cfg.ReadEnginePort = int(h.GetPgPort())
				pools.SetReadEngineEndpoint(cfg.ReadEngineHost, cfg.ReadEngineSockDir, cfg.ReadEnginePort)
			}
			log.Printf("[air-gateway] connected to air-read-engine via gRPC (%s)", cfg.ReadEngineGRPCAddr)
		}
	}
	var psGRPC *gateway.PageserverGRPCClient
	if cfg.PageserverGRPCAddr != "" {
		var psErr error
		psGRPC, psErr = gateway.NewPageserverGRPCClient(cfg.PageserverGRPCAddr, cfg.InternalToken)
		if psErr != nil {
			log.Printf("[air-gateway] pageserver gRPC dial warning (%s): %v", cfg.PageserverGRPCAddr, psErr)
		} else {
			log.Printf("[air-gateway] connected to air-pageserver via gRPC (%s)", cfg.PageserverGRPCAddr)
		}
	}
	cancelReg := gateway.NewCancelRegistry()

	discovery, err := gateway.NewTenantDiscovery(cfg)
	if err != nil {
		log.Fatalf("[air-gateway] discovery init failed: %v", err)
	}
	discovery.SetMetrics(metrics)
	discovery.SetCallbacks(
		func(r gateway.TenantRoute) {
			pools.UpsertDiscovered(r)
			if reGRPC != nil && r.State == "ACTIVE" {
				go func(tid string) {
					_, _ = reGRPC.EnsureTenant(context.Background(), tid)
				}(r.TenantID)
			}
		},
		func(tid string) { pools.RemoveTenant(tid) },
	)
	discovery.Start()

	adminSrv := gateway.NewAdminHTTPServer(cfg.AdminAddr, cfg.InternalToken, discovery, pools, metrics)
	adminSrv.SetNamespaceBackends(reGRPC, psGRPC)
	adminSrv.SetTelemetryStore(telStore)
	if err := adminSrv.Start(); err != nil {
		discovery.Stop()
		log.Fatalf("[air-gateway] failed to bind admin HTTP on %s: %v", cfg.AdminAddr, err)
	}

	ln, err := net.Listen("tcp", cfg.GatewayAddr)
	if err != nil {
		adminSrv.Stop()
		discovery.Stop()
		log.Fatalf("[air-gateway] failed to listen on %s: %v", cfg.GatewayAddr, err)
	}

	adminGRPCLabel := "disabled"
	var adminGRPCSrv *gateway.AdminGRPCServer
	if cfg.AdminGRPCAddr != "" {
		adminGRPCSrv = gateway.NewAdminGRPCServer(cfg.AdminGRPCAddr, cfg.InternalToken, pools, metrics)
		adminGRPCSrv.SetNamespaceBackends(discovery, reGRPC, psGRPC)
		if _, gErr := adminGRPCSrv.Start(); gErr != nil {
			adminSrv.Stop()
			discovery.Stop()
			_ = ln.Close()
			log.Fatalf("[air-gateway] failed to bind admin gRPC on %s: %v", cfg.AdminGRPCAddr, gErr)
		}
		adminGRPCLabel = cfg.AdminGRPCAddr
		defer adminGRPCSrv.Stop()
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
	if adaptiveCtrl != nil {
		adaptiveCtrl.Stop()
	}
	close(reaperStop)
	reaperWG.Wait()
	adminSrv.Stop()
	discovery.Stop()
	if reGRPC != nil {
		_ = reGRPC.Close()
	}
	if psGRPC != nil {
		_ = psGRPC.Close()
	}
	pools.CloseAll()
	if meteringLogger != nil {
		meteringLogger.Stop()
	}

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
