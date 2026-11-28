package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Mahadi-rsio/air/internal/autoscaler"
)

func main() {
	cfg := autoscaler.LoadConfig()

	store, err := autoscaler.NewStore(cfg.StateDir)
	if err != nil {
		log.Fatalf("[air-autoscaler] failed to open state store in %s: %v", cfg.StateDir, err)
	}

	rootCAs, err := autoscaler.LoadCertPoolFromPEM(cfg.GatewayTLSCAPath)
	if err != nil {
		log.Fatalf("[air-autoscaler] failed to load gateway TLS CA %s: %v", cfg.GatewayTLSCAPath, err)
	}

	flyClient := autoscaler.NewHTTPFlyClient(cfg.FlyAPIBase, cfg.AppName, cfg.FlyAPIToken, cfg.FlyAPITimeout)
	prober := autoscaler.NewNetGatewayProber(
		cfg.InternalToken,
		cfg.GatewayPrivateHTTPPort,
		cfg.GatewayPGPort,
		cfg.GatewayTLSServerName,
		rootCAs,
	)

	ctrl := autoscaler.NewController(cfg, store, flyClient, prober)

	reconcileCtx, cancelReconcile := context.WithTimeout(context.Background(), 20*time.Second)
	if err := ctrl.ReconcileOnStartup(reconcileCtx, time.Now().UnixMilli()); err != nil {
		log.Printf("[air-autoscaler] startup inventory reconciliation warning: %v", err)
	} else {
		log.Printf("[air-autoscaler] startup inventory reconciliation complete")
	}
	cancelReconcile()

	httpSrv := autoscaler.NewHTTPServer(cfg.HTTPAddr, cfg.InternalToken, ctrl)
	if err := httpSrv.Start(); err != nil {
		log.Fatalf("[air-autoscaler] failed to bind HTTP server on %s: %v", cfg.HTTPAddr, err)
	}

	log.Printf("[air-autoscaler] started (app=%s region=%s mode=%s auto_add=%v auto_remove=%v baseline=%q max_replicas=%d http=%s state_dir=%s tick=%s)",
		cfg.AppName, cfg.Region, cfg.Mode, cfg.AutoAddEnabled, cfg.AutoRemoveEnabled,
		cfg.BaselineMachineID, cfg.MaxReplicas, httpSrv.Addr(), cfg.StateDir, cfg.TickInterval,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(cfg.TickInterval)
	defer ticker.Stop()

	runTick := func() {
		tickCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		res, tickErr := ctrl.HandleTick(tickCtx, time.Now().UnixMilli())
		if tickErr != nil {
			log.Printf("[air-autoscaler] tick action=%s err=%v", res.Action, tickErr)
		} else if res.Action != "noop" {
			log.Printf("[air-autoscaler] tick action=%s mode=%s active=%d util=%.1f%% spare=%d op=%s machine=%s reason=%s",
				res.Action, res.Mode, res.ActiveMachines, res.UtilPct, res.SpareConns, res.OpID, res.MachineID, res.Reason)
		}
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("[air-autoscaler] shutting down...")
			httpSrv.Stop()
			os.Exit(0)
		case <-ticker.C:
			runTick()
		}
	}
}
