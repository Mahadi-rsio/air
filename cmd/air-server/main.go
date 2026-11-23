package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/config"
	"github.com/Mahadi-rsio/air/internal/control"
	"github.com/Mahadi-rsio/air/internal/crypto"
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

	cp, err := control.NewControlPlaneWithConfig(ctx, control.ControlPlaneConfig{
		MetaDir:        cfg.MetaDir,
		MetaDSN:        cfg.MetaDSN,
		PGBinDir:       cfg.PGBinDir,
		MetaPort:       cfg.MetaPort,
		Agent:          ag,
		KMS:            kms,
		R2Primary:      cfg.R2Primary,
		EvictOnSuspend: cfg.EvictOnSuspend,
		InternalToken:  cfg.InternalToken,
		BackendHost:    cfg.BackendHost,
	})
	if err != nil {
		ag.Close()
		log.Fatalf("init control plane: %v", err)
	}

	_, err = configureBackupStore(cp, cfg)
	if err != nil {
		cp.Close()
		ag.Close()
		log.Fatalf("init backup store: %v", err)
	}

	var psClient control.PageserverClient
	if cfg.PageserverGRPCAddr != "" {
		var psErr error
		psClient, psErr = control.NewGRPCPageserverClient(cfg.PageserverGRPCAddr, cfg.InternalToken)
		if psErr != nil {
			cp.Close()
			ag.Close()
			log.Fatalf("init remote pageserver gRPC client: %v", psErr)
		}
		cp.SetPageserverClient(psClient)
		log.Printf("[air-server] connected to standalone pageserver via gRPC (%s)", cfg.PageserverGRPCAddr)
	}

	var reClient control.ReadEngineClient
	if cfg.ReadEngineGRPCAddr != "" {
		var reErr error
		reClient, reErr = control.NewGRPCReadEngineClient(cfg.ReadEngineGRPCAddr, cfg.InternalToken)
		if reErr != nil {
			log.Printf("[air-server] read-engine gRPC dial warning (%s): %v", cfg.ReadEngineGRPCAddr, reErr)
		} else {
			cp.SetReadEngineClient(reClient)
			defer func() { _ = reClient.Close() }()
			log.Printf("[air-server] connected to air-read-engine via gRPC (%s)", cfg.ReadEngineGRPCAddr)
		}
	}

	var gwAdminClient control.GatewayAdminClient
	if cfg.GatewayAdminGRPCAddr != "" {
		var gwErr error
		gwAdminClient, gwErr = control.NewGRPCGatewayAdminClient(cfg.GatewayAdminGRPCAddr, cfg.InternalToken)
		if gwErr != nil {
			log.Printf("[air-server] gateway admin gRPC dial warning (%s): %v", cfg.GatewayAdminGRPCAddr, gwErr)
		} else {
			cp.SetGatewayAdminClient(gwAdminClient)
			defer func() { _ = gwAdminClient.Close() }()
			log.Printf("[air-server] connected to air-gateway admin via gRPC (%s)", cfg.GatewayAdminGRPCAddr)
		}
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

	cp.StartColdOffloadWorker(ctx, 2)
	log.Printf("[air-server] async R2 cold-offload queue started (workers=2, evict_on_suspend=%v)", cfg.EvictOnSuspend)

	var poolDrainFn func(tenantID string, timeout time.Duration) error
	var closePoolDrain func() error
	if cfg.GatewayAdminGRPCAddr != "" {
		var drainErr error
		poolDrainFn, closePoolDrain, drainErr = control.NewGRPCPoolDrainFn(cfg.GatewayAdminGRPCAddr, cfg.InternalToken)
		if drainErr != nil {
			log.Fatalf("init gateway admin gRPC drain client: %v", drainErr)
		}
		defer func() { _ = closePoolDrain() }()
		log.Printf("[air-server] cooperative gateway gRPC drain enabled (gateway_admin_grpc=%s)", cfg.GatewayAdminGRPCAddr)
	} else if cfg.GatewayAdminURL != "" {
		poolDrainFn = control.NewHTTPPoolDrainFn(cfg.GatewayAdminURL, cfg.InternalToken)
		log.Printf("[air-server] cooperative gateway drain enabled (gateway_admin_url=%s)", cfg.GatewayAdminURL)
	}

	if cfg.GatewayAdminURL != "" {
		telPub := cell.NewTelemetryPublisher(ag, cfg.GatewayAdminURL, cfg.InternalToken, "", time.Second)
		telPub.Start(ctx)
		log.Printf("[air-server] cell resource telemetry publisher enabled (gateway_admin_url=%s)", cfg.GatewayAdminURL)
	}

	if cfg.IdleTimeout > 0 {
		cp.StartIdleReaper(ctx, cfg.IdleTimeout, 0, poolDrainFn)
		log.Printf("[air-server] cooperative scale-to-zero enabled (idle_timeout=%s)", cfg.IdleTimeout)
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           cp.HTTPHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("[air-server] Control Plane HTTP listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("grpc listen %s: %v", cfg.GRPCAddr, err)
	}
	grpcSrv := control.NewGRPCServer(cp)
	go func() {
		log.Printf("[air-server] Air DB gRPC listening on %s", cfg.GRPCAddr)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[air-server] shutting down...")
	shutCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	grpcSrv.GracefulStop()
	_ = srv.Shutdown(shutCtx)
	if err := cp.ShutdownActiveTenants(shutCtx); err != nil {
		log.Printf("[air-server] shutdown active tenants error: %v", err)
	}
	if psClient != nil {
		if err := psClient.FlushAll(shutCtx); err != nil {
			log.Printf("[air-server] remote pageserver flush all error: %v", err)
		}
		if err := psClient.Close(); err != nil {
			log.Printf("[air-server] remote pageserver close error: %v", err)
		}
	}
	cp.Close()
	ag.Close()
}

func configureBackupStore(cp *control.ControlPlane, cfg config.Config) (*s3.Client, error) {
	anySet := cfg.R2Endpoint != "" || cfg.R2Bucket != "" || cfg.R2AccessKeyID != "" || cfg.R2SecretAccessKey != ""
	if !anySet {
		if cfg.R2Primary {
			return nil, fmt.Errorf("AIR_R2_PRIMARY=true requires R2 configuration (AIR_R2_ENDPOINT, AIR_R2_BUCKET, AIR_R2_ACCESS_KEY_ID, AIR_R2_SECRET_ACCESS_KEY)")
		}
		return nil, nil
	}
	if cfg.R2Endpoint == "" || cfg.R2Bucket == "" || cfg.R2AccessKeyID == "" || cfg.R2SecretAccessKey == "" {
		return nil, fmt.Errorf("incomplete R2 configuration: AIR_R2_ENDPOINT, AIR_R2_BUCKET, AIR_R2_ACCESS_KEY_ID, and AIR_R2_SECRET_ACCESS_KEY must all be set")
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
	return s3c, nil
}
