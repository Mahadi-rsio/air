package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Mahadi-rsio/air/internal/gateway"
	"github.com/Mahadi-rsio/air/internal/meshtransport"
)

func isLoopbackListenAddr(addr string) bool {
	if addr == "" {
		return true
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if host == "" || host == "0.0.0.0" || host == "::" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func main() {
	for _, a := range os.Args[1:] {
		if a == "--metering-reconcile" {
			runMeteringReconcileCLI(os.Args[1:])
			return
		}
	}

	cfg := gateway.LoadConfig(os.Args[1:])
	if err := cfg.Validate(); err != nil {
		log.Fatalf("[air-gateway] invalid config: %v", err)
	}
	if err := meshtransport.ValidateMetadataDSN(cfg.MetaDSN); err != nil {
		log.Fatal(err)
	}
	if !isLoopbackListenAddr(cfg.AdminAddr) {
		log.Fatal("[air-gateway] the HTTP admin endpoint must listen on loopback")
	}
	if cfg.InternalToken == "" && (!isLoopbackListenAddr(cfg.AdminAddr) || !isLoopbackListenAddr(cfg.AdminGRPCAddr)) {
		log.Fatalf("[air-gateway] AIR_INTERNAL_TOKEN must be set when admin endpoints listen on non-loopback addresses (admin=%q admin_grpc=%q)", cfg.AdminAddr, cfg.AdminGRPCAddr)
	}

	var tlsCtx gateway.TLSContext
	if err := tlsCtx.Init(cfg.TLSCertPath, cfg.TLSKeyPath); err != nil {
		log.Fatalf("[air-gateway] TLS init failed: %v", err)
	}

	metrics := gateway.NewMetricsRegistry(cfg.MaxClientConns, cfg.MaxTenantClientConns, cfg.MaxUserClientConns)
	meshTLS, err := meshtransport.LoadTLS()
	if err != nil {
		log.Fatal(err)
	}
	pools := gateway.NewPoolManager(cfg.PoolMaxBackends, cfg.PoolIdleTimeoutSec, meshTLS)

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
	var pgMeteringSaver *gateway.PGMeteringSaver
	var meteringSaver gateway.MeteringSaver
	if cfg.MetaDSN != "" {
		pgMeteringSaver = gateway.NewPGMeteringSaver(cfg.MetaDSN)
		meteringSaver = pgMeteringSaver
	}
	meteringLogDir := cfg.MeteringLogDir
	if cfg.BillingEnabled && !cfg.MeteringDirExplicit {
		meteringLogDir = ""
	}
	meteringLogger, err := gateway.NewMeteringLoggerWithOptions(gateway.MeteringLoggerOptions{
		LogDir:          meteringLogDir,
		GatewayID:       cfg.GatewayID,
		BillingEnabled:  cfg.BillingEnabled,
		MaxFileBytes:    cfg.MeteringMaxFileBytes,
		MaxSpoolBytes:   cfg.MeteringMaxSpoolBytes,
		FlushInterval:   cfg.MeteringFlushInterval,
		SyncOnWrite:     cfg.MeteringSyncOnWrite,
		Saver:           meteringSaver,
		RequireWritable: true,
	})
	if err != nil {
		log.Printf("[air-gateway] metering logger unavailable (readiness will fail if billing enabled): %v", err)
	} else {
		meteringLogger.Start()
		pools.SetMeteringLogger(meteringLogger)
		log.Printf("[air-gateway] query metering enabled (dir=%s, max_file_bytes=%d, max_spool_bytes=%d, flush_interval=%s, billing=%v)",
			meteringLogDir, cfg.MeteringMaxFileBytes, cfg.MeteringMaxSpoolBytes, cfg.MeteringFlushInterval, cfg.BillingEnabled)
	}
	if cfg.MaxAggregateBackends > 0 {
		pools.SetAggregateMaxBackends(cfg.MaxAggregateBackends)
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
			if pgMeteringSaver != nil {
				pgMeteringSaver.SetStorageProvider(psGRPC)
			}
			log.Printf("[air-gateway] connected to air-pageserver via gRPC (%s)", cfg.PageserverGRPCAddr)
		}
	}
	cancelReg := gateway.NewCancelRegistry()
	cancelRouter := gateway.NewCancelRouter(cancelReg, cfg.InternalToken, cfg.GatewayPeers, cfg.GatewayDiscoveryHost, cfg.PeerAdminPort)
	defer cancelRouter.Close()

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

	lifecycle := gateway.NewGatewayLifecycle(cfg.GatewayID, cfg.RequireTLS, &tlsCtx, discovery, pools, metrics, meteringLogger, reGRPC, psGRPC)
	if cfg.CordonedOnStart {
		lifecycle.Cordon()
	}

	adminSrv := gateway.NewAdminHTTPServer(cfg.AdminAddr, cfg.InternalToken, discovery, pools, metrics)
	adminSrv.SetNamespaceBackends(reGRPC, psGRPC)
	adminSrv.SetTelemetryStore(telStore)
	adminSrv.SetLifecycle(lifecycle, meteringLogger)
	if err := adminSrv.Start(); err != nil {
		discovery.Stop()
		log.Fatalf("[air-gateway] failed to bind admin HTTP on %s: %v", cfg.AdminAddr, err)
	}

	var privateAdminSrv *gateway.AdminHTTPServer
	if cfg.PrivateHTTPAddr != "" {
		if cfg.InternalToken == "" {
			adminSrv.Stop()
			discovery.Stop()
			log.Fatalf("[air-gateway] AIR_INTERNAL_TOKEN must be set when AIR_GATEWAY_PRIVATE_HTTP_ADDR is configured (%s)", cfg.PrivateHTTPAddr)
		}
		privateAdminSrv = gateway.NewAdminHTTPServer(cfg.PrivateHTTPAddr, cfg.InternalToken, discovery, pools, metrics)
		privateAdminSrv.SetNamespaceBackends(reGRPC, psGRPC)
		privateAdminSrv.SetTelemetryStore(telStore)
		privateAdminSrv.SetLifecycle(lifecycle, meteringLogger)
		if err := privateAdminSrv.Start(); err != nil {
			adminSrv.Stop()
			discovery.Stop()
			log.Fatalf("[air-gateway] failed to bind private admin HTTP on %s: %v", cfg.PrivateHTTPAddr, err)
		}
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
		adminGRPCSrv.SetMeshHandlers(cancelReg, lifecycle)
		if _, gErr := adminGRPCSrv.Start(); gErr != nil {
			adminSrv.Stop()
			if privateAdminSrv != nil {
				privateAdminSrv.Stop()
			}
			discovery.Stop()
			_ = ln.Close()
			log.Fatalf("[air-gateway] failed to bind admin gRPC on %s: %v", cfg.AdminGRPCAddr, gErr)
		}
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
		lifecycle.MarkShuttingDown()
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
			gateway.HandleClientConnectionWithCancelRouter(c, cfg, &tlsCtx, discovery, pools, metrics, cancelReg, cancelRouter)
		}(conn)
	}

	log.Println("[air-gateway] shutting down, draining active client sessions...")
	done := make(chan struct{})
	go func() {
		clientsWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
	}

	if adaptiveCtrl != nil {
		adaptiveCtrl.Stop()
	}
	close(reaperStop)
	reaperWG.Wait()
	if adminGRPCSrv != nil {
		adminGRPCSrv.Stop()
	}
	if privateAdminSrv != nil {
		privateAdminSrv.Stop()
	}
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
}

func runMeteringReconcileCLI(args []string) {
	cfg := gateway.LoadConfig(args)
	apply := false
	for _, a := range args {
		if a == "--apply" {
			apply = true
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reports, err := gateway.ReconcileLegacySpool(ctx, cfg.MeteringLogDir, cfg.MetaDSN, apply)
	if err != nil {
		log.Fatalf("[air-gateway] metering reconcile failed: %v", err)
	}
	fmt.Println(gateway.MarshalLegacyReport(reports))
}
