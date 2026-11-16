package main

import (
	"context"
	"fmt"
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
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

	cp, err := control.NewControlPlaneWithConfig(ctx, control.ControlPlaneConfig{
		MetaDir:   cfg.MetaDir,
		MetaDSN:   cfg.MetaDSN,
		PGBinDir:  cfg.PGBinDir,
		MetaPort:  cfg.MetaPort,
		Agent:     ag,
		KMS:       kms,
		R2Primary: cfg.R2Primary,
	})
	if err != nil {
		log.Fatalf("init control plane: %v", err)
	}
	defer cp.Close()

	if err := configureBackupStore(cp, cfg); err != nil {
		log.Fatalf("init backup store: %v", err)
	}

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
		RequireTLS:      cfg.RequireTLS,
		TLSCertPath:     cfg.TLSCertPath,
		TLSKeyPath:      cfg.TLSKeyPath,
		PoolMode:        cfg.PoolMode,
		PoolMaxBackends: cfg.PoolMaxBackends,
	})
	if err != nil {
		log.Fatalf("init gateway: %v", err)
	}
	defer gw.Close()

	if cfg.IdleTimeout > 0 {
		cp.StartIdleReaper(ctx, cfg.IdleTimeout, 0, gw.RequestTenantDrain)
		log.Printf("[air-server] cooperative scale-to-zero enabled (idle_timeout=%s)", cfg.IdleTimeout)
	}

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

func configureBackupStore(cp *control.ControlPlane, cfg config.Config) error {
	anySet := cfg.R2Endpoint != "" || cfg.R2Bucket != "" || cfg.R2AccessKeyID != "" || cfg.R2SecretAccessKey != ""
	if !anySet {
		if cfg.R2Primary {
			return fmt.Errorf("AIR_R2_PRIMARY=true requires R2 configuration (AIR_R2_ENDPOINT, AIR_R2_BUCKET, AIR_R2_ACCESS_KEY_ID, AIR_R2_SECRET_ACCESS_KEY)")
		}
		return nil
	}
	if cfg.R2Endpoint == "" || cfg.R2Bucket == "" || cfg.R2AccessKeyID == "" || cfg.R2SecretAccessKey == "" {
		return fmt.Errorf("incomplete R2 configuration: AIR_R2_ENDPOINT, AIR_R2_BUCKET, AIR_R2_ACCESS_KEY_ID, and AIR_R2_SECRET_ACCESS_KEY must all be set")
	}
	region := cfg.R2Region
	if region == "" {
		region = "auto"
	}
	awsCfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.R2AccessKeyID, cfg.R2SecretAccessKey, ""),
	}
	s3c := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.R2Endpoint)
		o.UsePathStyle = true
	})
	cp.SetBackupStore(s3c, cfg.R2Bucket)
	log.Printf("[air-server] backup store enabled (bucket=%s, endpoint=%s, region=%s)", cfg.R2Bucket, cfg.R2Endpoint, region)
	return nil
}
