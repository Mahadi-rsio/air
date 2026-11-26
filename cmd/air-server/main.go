package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/config"
	"github.com/Mahadi-rsio/air/internal/control"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/meshtransport"
	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if err := meshtransport.ValidateMetadataDSN(cfg.MetaDSN); err != nil {
		log.Fatal(err)
	}
	if cfg.InternalToken == "" && (!isLoopbackListenAddr(cfg.GRPCAddr) || !isLoopbackListenAddr(cfg.HTTPAddr)) {
		log.Fatalf("[air-server] AIR_INTERNAL_TOKEN must be set when listening on non-loopback addresses (grpc=%q http=%q)", cfg.GRPCAddr, cfg.HTTPAddr)
	}
	if strings.TrimSpace(os.Getenv("AIR_MASTER_KEY_HEX")) == "" {
		log.Printf("[air-server] WARNING: AIR_MASTER_KEY_HEX is not set; using local master key file %s", cfg.MasterKeyPath)
	}

	meshTLS, meshErr := meshtransport.LoadTLS()
	if meshErr == nil {
		meshErr = meshtransport.RequireForAddress(cfg.GRPCAddr, meshTLS)
	}
	if meshErr != nil {
		log.Fatal(meshErr)
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

	s3c, err := configureBackupStore(cp, cfg)
	if err != nil {
		cp.Close()
		ag.Close()
		log.Fatalf("init backup store: %v", err)
	}

	psCleanup, err := configurePageserver(ctx, cp, ag, cfg, kms, s3c, 15*time.Second)
	if err != nil {
		cp.Close()
		ag.Close()
		log.Fatalf("init pageserver: %v", err)
	}

	if cfg.ReadEngineGRPCAddr != "" {
		reClient, err := control.NewGRPCReadEngineClient(cfg.ReadEngineGRPCAddr, cfg.InternalToken)
		if err != nil {
			log.Fatalf("[air-server] configure read-engine gRPC (%s): %v", cfg.ReadEngineGRPCAddr, err)
		}
		cp.SetReadEngineClient(reClient)
		defer func() { _ = reClient.Close() }()
		log.Printf("[air-server] connected to air-read-engine via gRPC (%s)", cfg.ReadEngineGRPCAddr)
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
	if cfg.GatewayAdminGRPCAddr != "" {
		var staticAddrs []string
		var discHost string
		discPort := 9093
		for _, part := range strings.Split(cfg.GatewayAdminGRPCAddr, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if h, p, err := net.SplitHostPort(part); err == nil && strings.HasSuffix(h, ".internal") {
				discHost = h
				if parsed, perr := fmt.Sscanf(p, "%d", &discPort); parsed != 1 || perr != nil {
					discPort = 9093
				}
			} else {
				staticAddrs = append(staticAddrs, part)
			}
		}
		poolDrainFn = control.NewCoordinatedGRPCPoolDrainFn(staticAddrs, discHost, discPort, cfg.InternalToken, true)
		cp.SetPoolDrainFn(poolDrainFn)
	}
	if cfg.IdleTimeout > 0 {
		cp.StartIdleReaper(ctx, cfg.IdleTimeout, 0, poolDrainFn)
		log.Printf("[air-server] cooperative scale-to-zero enabled (idle_timeout=%s)", cfg.IdleTimeout)
	}

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("grpc listen %s: %v", cfg.GRPCAddr, err)
	}
	grpcSrv := control.NewGRPCServer(cp, meshtransport.ServerOptions(meshTLS)...)
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
	if err := cp.ShutdownActiveTenants(shutCtx); err != nil {
		log.Printf("[air-server] shutdown active tenants error: %v", err)
	}
	if psCleanup != nil {
		psCleanup(shutCtx)
	}
	cp.Close()
	ag.Close()
}

func configurePageserver(
	ctx context.Context,
	cp *control.ControlPlane,
	ag *cell.Agent,
	cfg config.Config,
	kms *crypto.EnvelopeKMS,
	s3c *s3.Client,
	startupTimeout time.Duration,
) (func(context.Context), error) {
	if cfg.PageserverGRPCAddr != "" {
		psClient, err := control.NewGRPCPageserverClient(cfg.PageserverGRPCAddr, cfg.InternalToken)
		if err != nil {
			return nil, fmt.Errorf("init remote pageserver gRPC client: %w", err)
		}
		if err := psClient.WaitForHealthy(ctx, startupTimeout); err != nil {
			_ = psClient.Close()
			return nil, err
		}
		cp.SetPageserverClient(psClient)
		log.Printf("[air-server] connected to standalone pageserver via gRPC (%s)", cfg.PageserverGRPCAddr)

		var unixSrv *pageserver.UnixServer
		if cfg.PageserverEnabled {
			if cfg.PageserverSocket == "" {
				_ = psClient.Close()
				return nil, fmt.Errorf("AIR_PAGESERVER_ENABLED=true requires a non-empty pageserver socket path")
			}
			ag.SetPageserverConfig(true, cfg.PageserverSocket, cfg.PageserverCacheMB)
			if !ag.HasAirSmgr() {
				ag.SetPageserverConfig(false, "", 0)
				_ = psClient.Close()
				return nil, fmt.Errorf("AIR_PAGESERVER_ENABLED=true but air_smgr.so was not found alongside pg_bin_dir (%s)", cfg.PGBinDir)
			}
			unixSrv = pageserver.NewUnixServer(cfg.PageserverSocket, psClient)
			if err := unixSrv.Start(ctx); err != nil {
				ag.SetPageserverConfig(false, "", 0)
				_ = psClient.Close()
				return nil, fmt.Errorf("start pageserver unix socket bridge (%s): %w", cfg.PageserverSocket, err)
			}
			log.Printf("[air-server] air_smgr enabled via unix socket bridge (%s -> %s, lfc_max_mb=%d)",
				cfg.PageserverSocket, cfg.PageserverGRPCAddr, cfg.PageserverCacheMB)
		}

		return func(shutCtx context.Context) {
			if unixSrv != nil {
				if err := unixSrv.Close(); err != nil {
					log.Printf("[air-server] pageserver unix socket bridge close error: %v", err)
				}
			}
			if err := psClient.FlushAll(shutCtx); err != nil {
				log.Printf("[air-server] remote pageserver flush all error: %v", err)
			}
			if err := psClient.Close(); err != nil {
				log.Printf("[air-server] remote pageserver close error: %v", err)
			}
		}, nil
	}

	if cfg.PageserverEnabled {
		if cfg.PageserverSocket == "" {
			return nil, fmt.Errorf("AIR_PAGESERVER_ENABLED=true requires a non-empty pageserver socket path")
		}
		ag.SetPageserverConfig(true, cfg.PageserverSocket, cfg.PageserverCacheMB)
		if !ag.HasAirSmgr() {
			ag.SetPageserverConfig(false, "", 0)
			return nil, fmt.Errorf("AIR_PAGESERVER_ENABLED=true but air_smgr.so was not found alongside pg_bin_dir (%s)", cfg.PGBinDir)
		}
		ps, err := pageserver.New(pageserver.Config{
			SocketPath:    cfg.PageserverSocket,
			LayerDir:      filepath.Join(cfg.DataDir, "ps_layers"),
			CacheDir:      filepath.Join(cfg.DataDir, "ps_cache"),
			CacheMaxBytes: int64(cfg.PageserverCacheMB) * 1024 * 1024,
			S3Client:      s3c,
			R2Bucket:      cfg.R2Bucket,
			KMS:           kms,
		})
		if err != nil {
			ag.SetPageserverConfig(false, "", 0)
			return nil, fmt.Errorf("init in-process pageserver: %w", err)
		}
		if err := ps.Start(ctx); err != nil {
			_ = ps.Close()
			ag.SetPageserverConfig(false, "", 0)
			return nil, fmt.Errorf("start in-process pageserver (%s): %w", cfg.PageserverSocket, err)
		}
		cp.SetPageserver(ps)
		log.Printf("[air-server] in-process pageserver + air_smgr enabled (socket=%s, lfc_max_mb=%d)",
			cfg.PageserverSocket, cfg.PageserverCacheMB)
		return func(shutCtx context.Context) {
			if err := ps.FlushAll(shutCtx); err != nil {
				log.Printf("[air-server] in-process pageserver flush all error: %v", err)
			}
			if err := ps.Close(); err != nil {
				log.Printf("[air-server] in-process pageserver close error: %v", err)
			}
		}, nil
	}

	return func(context.Context) {}, nil
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
