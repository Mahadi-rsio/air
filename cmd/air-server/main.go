package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/config"
	"github.com/Mahadi-rsio/air/internal/control"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/gateway"
)

func main() {
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	kms, err := crypto.LoadOrCreateEnvelopeKMS(cfg.MasterKeyPath)
	if err != nil {
		log.Fatalf("init kms: %v", err)
	}

	ag, err := cell.NewAgent(cfg.DataDir, cfg.PGBinDir, kms)
	if err != nil {
		log.Fatalf("init cell agent: %v", err)
	}
	defer ag.Close()

	cp, err := control.NewControlPlane(ctx, cfg.MetaDir, cfg.PGBinDir, cfg.MetaPort, ag, kms)
	if err != nil {
		log.Fatalf("init control plane: %v", err)
	}
	defer cp.Close()

	if cfg.BootstrapAPIKey != "" {
		keyInfo, err := cp.SeedAPIKey(ctx, cfg.BootstrapAccountID, cfg.BootstrapAPIKey, "bootstrap")
		if err != nil {
			log.Fatalf("seed bootstrap API key: %v", err)
		}
		log.Printf("[air-server] seeded bootstrap API key %s for account %s", keyInfo.KeyID, keyInfo.AccountID)
	}

	recovered, err := cp.RecoverProvisioningOperations(ctx)
	if err != nil {
		log.Fatalf("recover provisioning ops: %v", err)
	}
	if recovered > 0 {
		log.Printf("[air-server] recovered %d interrupted provisioning operations", recovered)
	}

	gw, err := gateway.NewGatewayWithOptions(cfg.GatewayAddr, cp, gateway.GatewayOptions{
		RequireTLS:  cfg.RequireTLS,
		TLSCertPath: cfg.TLSCertPath,
		TLSKeyPath:  cfg.TLSKeyPath,
	})
	if err != nil {
		log.Fatalf("init gateway: %v", err)
	}
	defer gw.Close()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           cp.HTTPHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("[air-server] Control Plane HTTP listening on %s, PG Gateway listening on %s (require_tls=%v)", cfg.HTTPAddr, gw.Addr(), cfg.RequireTLS)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[air-server] shutting down...")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}
