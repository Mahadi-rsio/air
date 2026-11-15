package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"air-stage4/pkg/backup"
	"air-stage4/pkg/capacity"
	"air-stage4/pkg/cell"
	"air-stage4/pkg/crypto"
	"air-stage4/pkg/fencing"
	"air-stage4/pkg/isolation"
	"air-stage4/pkg/migration"
	"air-stage4/pkg/wire"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Stage4Report struct {
	Timestamp             string                           `json:"timestamp"`
	Stage                 string                           `json:"stage"`
	Workstream1Fencing    fencing.Workstream1Report        `json:"workstream_1_fencing_leases"`
	Workstream2BackupPITR backup.Workstream2Report         `json:"workstream_2_backup_pitr_gc"`
	Workstream3Volume     isolation.Workstream3Report      `json:"workstream_3_volume_corruption"`
	Workstream4And10Sec   isolation.Workstream4And10Report `json:"workstream_4_10_security_isolation"`
	Workstream5Soak       isolation.Workstream5Report      `json:"workstream_5_soak_leak_audit"`
	Workstream6Migration  migration.Workstream6Report      `json:"workstream_6_live_migration"`
	Workstream7CrossRegDR migration.Workstream7Report      `json:"workstream_7_cross_region_dr_kms"`
	Workstream8Wire       wire.Workstream8Report           `json:"workstream_8_wire_protocol_drain"`
	Workstream9Capacity   capacity.Workstream9Report       `json:"workstream_9_capacity_pss_finops"`
	GatesPassed           map[string]bool                  `json:"gates_passed"`
	AllPassed             bool                             `json:"all_passed"`
}

func newR2ClientFromEnv(ctx context.Context) (*s3.Client, string, error) {
	accessKey := strings.Trim(os.Getenv("ACCESS_KEY"), "\"' \t")
	secretKey := strings.Trim(os.Getenv("SECRET_KEY"), "\"' \t")
	endpoint := strings.Trim(os.Getenv("ENDPOINT"), "\"' \t")
	bucket := strings.Trim(os.Getenv("R2_BUCKET"), "\"' \t")
	if bucket == "" {
		bucket = "air-mvp-test"
	}
	if accessKey == "" || secretKey == "" || endpoint == "" {
		return nil, "", fmt.Errorf("missing R2 env vars (ACCESS_KEY, SECRET_KEY, ENDPOINT)")
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("auto"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, "", err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return client, bucket, nil
}

func cleanupR2Prefix(ctx context.Context, s3c *s3.Client, bucket, prefix string) {
	if s3c == nil || bucket == "" {
		return
	}
	p := s3.NewListObjectsV2Paginator(s3c, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return
		}
		for _, obj := range page.Contents {
			_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			})
		}
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("=== AIR STAGE 4: PRINCIPAL DATABASE ENGINEER PRODUCTION READINESS SUITE ===")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	baseDir := "/tmp/air-stage4"
	_ = os.RemoveAll(baseDir)

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		log.Fatalf("Failed to initialize Stage 4 KMS: %v", err)
	}

	agent, err := cell.NewAgent(baseDir, 2, kms)
	if err != nil {
		log.Fatalf("Failed to initialize Stage 4 Cell Agent: %v", err)
	}
	defer agent.Close()

	s3c, bucket, err := newR2ClientFromEnv(ctx)
	if err != nil {
		log.Fatalf("Failed to initialize Cloudflare R2 S3 client: %v", err)
	}
	cleanupR2Prefix(ctx, s3c, bucket, "stage4/")
	defer cleanupR2Prefix(ctx, s3c, bucket, "stage4/")

	log.Println("[1/9] Running Workstream 1: Monotonic Lease Watchdog Self-Fencing & R2 Conditional CAS Epoch Fencing...")
	ws1, err := fencing.RunWorkstream1Fencing(ctx, baseDir, s3c, bucket)
	if err != nil {
		log.Fatalf("Workstream 1 failed: %v", err)
	}
	log.Printf("  -> WS1 Complete: Pass=%v, SelfKillMs=%d (<PromoteBarrier=%dms), CAS HTTP 412=%v, SplitBrainCommits=%d",
		ws1.Pass, ws1.MeasuredWatchdogKillMs, ws1.ControlPromoteBarrierMs, ws1.ZombieRawIfMatch412RejectedByR2, ws1.SplitBrainCommitsAfterPromotion)

	log.Println("[2/9] Running Workstream 2: Multi-Step Encrypted Incremental PITR (B0->I1->I2 + WAL Replay) & DAG Mark-and-Sweep GC...")
	ws2, err := backup.RunWorkstream2BackupPITRAndGC(ctx, agent, s3c, bucket)
	if err != nil {
		log.Fatalf("Workstream 2 failed: %v", err)
	}
	log.Printf("  -> WS2 Complete: Pass=%v, RestoredPreDropRows=%d/%d, WALSegs=%d, Amcheck=%v",
		ws2.Pass, ws2.RestoredRowCountAfterCombinePITR, ws2.ExpectedRowCountBeforeDropTable, ws2.ArchivedWALSegmentsUploadedToR2, ws2.PgAmcheckPassedPostPITR)

	log.Println("[3/9] Running Workstream 3: Sub-Second Quota Flip, 3-Tier Ballast Watermarks & Heap Page Corruption Recovery...")
	ws3, err := isolation.RunWorkstream3VolumeAndCorruption(ctx, agent, s3c, bucket)
	if err != nil {
		log.Fatalf("Workstream 3 failed: %v", err)
	}
	log.Printf("  -> WS3 Complete: Pass=%v, QuotaReadOnly=%v (%dms), Ballast=%d->%d, SQLState=%s, RestoredRows=%d",
		ws3.Pass, ws3.SubsequentWriteRejectedReadOnly, ws3.QuotaDetectionLatencyMs, ws3.BallastBytesBefore92, ws3.BallastBytesAfter92,
		ws3.DetectedCorruptionSQLState, ws3.RecoveredRowsFromR2AfterReplacement)

	log.Println("[4/9] Running Workstreams 4 & 10: 7-Vector Red-Team Security Isolation & Temp Spill Bomb Defense...")
	ws4, err := isolation.RunWorkstream4And10SecurityIsolation(ctx, agent)
	if err != nil {
		log.Fatalf("Workstreams 4 & 10 failed: %v", err)
	}
	log.Printf("  -> WS4/10 Complete: Pass=%v, TempSpillCode=%s, CrossUID0700Blocked=%v, VictimDrops=%d",
		ws4.Pass, ws4.Vector6SQLState, ws4.Vector7CrossUID0700ReadBlocked, ws4.VictimDroppedQueries)

	log.Println("[5/9] Running Workstream 5: 25-Cycle Wake/Sleep Soak & FD / Ephemeral Port Leak Audit...")
	ws5, err := isolation.RunWorkstream5SoakAndLeakAudit(ctx, agent)
	if err != nil {
		log.Fatalf("Workstream 5 failed: %v", err)
	}
	log.Printf("  -> WS5 Complete: Pass=%v, Cycles=%d, NetLeakedLockFDs=%d, DistinctPorts=%d",
		ws5.Pass, ws5.SoakCyclesCompleted, ws5.NetLeakedLockFDs, ws5.DistinctPortsAssignedToTenant)

	log.Println("[6/9] Running Workstream 6: Live Logical Replication Migration + Sequence Synchronization...")
	ws6, err := migration.RunWorkstream6LiveMigration(ctx, agent)
	if err != nil {
		log.Fatalf("Workstream 6 failed: %v", err)
	}
	log.Printf("  -> WS6 Complete: Pass=%v, CutoverMs=%d, Rows=%d/%d (Lost=%d), SeqCollisions=%d",
		ws6.Pass, ws6.CutoverPauseDurationMs, ws6.FinalTargetRowCount, ws6.TotalAcknowledgedCommits, ws6.LostCommittedRows, ws6.SequenceCollisionErrorsPostCutover)

	log.Println("[7/9] Running Workstream 7: Zero-Shared-Memory Cross-Region DR (iad->ord) & Online KEK Rotation (kek-v1->kek-v2)...")
	ws7, err := migration.RunWorkstream7CrossRegionDRAndKMS(ctx, baseDir, s3c, bucket)
	if err != nil {
		log.Fatalf("Workstream 7 failed: %v", err)
	}
	log.Printf("  -> WS7 Complete: Pass=%v, ORDRestoredRows=%d, RotatedKey=%s, ReUploadedR2Bytes=%d, DecryptPostV1Revoke=%v",
		ws7.Pass, ws7.ORDRestoredRowCountFromR2, ws7.OnlineKEKRotationTargetVersion, ws7.R2BackupBytesReUploadedDuringKEKRotation, ws7.DecryptBackupAfterKEKRotationAndV1Revoke)

	log.Println("[8/9] Running Workstream 8: Wire Virtualizer Auth, COPY FROM STDIN, Mid-Pipeline Flush, DISCARD ALL & 2PC Drain Veto...")
	ws8, err := wire.RunWorkstream8WireAndDrain(ctx, agent)
	if err != nil {
		log.Fatalf("Workstream 8 failed: %v", err)
	}
	log.Printf("  -> WS8 Complete: Pass=%v, Auth28P01=%v, Role=%s, CopyRows=%d, MidFlush=%v, Discard=%v, Dealloc=%v, 2PCVeto=%v",
		ws8.Pass, ws8.WrongPasswordRejected28P01, ws8.BackendUserLeastPrivilege, ws8.CopyFromStdinRowsStreamed,
		ws8.MidPipelineFlushSucceeded, ws8.DiscardAllClearedState, ws8.DeallocateRePrepareSuccess, ws8.DrainVetoedOnOpen2PC)

	log.Println("[9/9] Running Workstream 9: Linux Kernel /proc/<pid>/smaps_rollup PSS/USS Memory & FinOps Sensitivity Matrix...")
	ws9, err := capacity.RunWorkstream9CapacityAndFinOps(ctx, agent)
	if err != nil {
		log.Fatalf("Workstream 9 failed: %v", err)
	}
	log.Printf("  -> WS9 Complete: Pass=%v, LoadedRSS=%.2fMB vs TruePSS=%.2fMB (Overcount=%.2fx), TrueUSS=%.2fMB",
		ws9.Pass, ws9.LoadedClusterProfile.NaiveSummedRSSMB, ws9.LoadedClusterProfile.TrueKernelPSSMB,
		ws9.LoadedClusterProfile.RSSOvercountRatio, ws9.LoadedClusterProfile.TrueKernelUSSMB)

	gates := map[string]bool{
		"GATE-01_fencing_and_monotonic_leases":          ws1.Pass,
		"GATE-02_pitr_dag_and_gc":                       ws2.Pass,
		"GATE-03_volume_ballast_and_corruption_recovery": ws3.Pass,
		"GATE-04_10_red_team_security_and_temp_spill":   ws4.Pass,
		"GATE-05_soak_zero_fd_and_port_leaks":           ws5.Pass,
		"GATE-06_live_logical_migration_and_sequences":  ws6.Pass,
		"GATE-07_cross_region_dr_and_kek_rotation":      ws7.Pass,
		"GATE-08_wire_protocol_and_2pc_drain_guard":     ws8.Pass,
		"GATE-09_pss_memory_and_finops_matrix":          ws9.Pass,
	}

	allPassed := true
	for name, ok := range gates {
		if !ok {
			log.Printf("GATE FAILED: %s", name)
			allPassed = false
		} else {
			log.Printf("GATE PASSED: %s", name)
		}
	}

	report := Stage4Report{
		Timestamp:             time.Now().UTC().Format(time.RFC3339),
		Stage:                 "Stage 4: Principal Database Engineer Production Readiness Verification",
		Workstream1Fencing:    ws1,
		Workstream2BackupPITR: ws2,
		Workstream3Volume:     ws3,
		Workstream4And10Sec:   ws4,
		Workstream5Soak:       ws5,
		Workstream6Migration:  ws6,
		Workstream7CrossRegDR: ws7,
		Workstream8Wire:       ws8,
		Workstream9Capacity:   ws9,
		GatesPassed:           gates,
		AllPassed:             allPassed,
	}

	outBytes, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Fatalf("marshal report: %v", err)
	}

	outDir := "/app/out"
	if _, err := os.Stat(outDir); err != nil {
		outDir = "."
	}
	outFile := filepath.Join(outDir, "stage4-results.json")
	if err := os.WriteFile(outFile, outBytes, 0644); err != nil {
		log.Fatalf("write %s: %v", outFile, err)
	}

	fmt.Println(string(outBytes))
	if !allPassed {
		os.Exit(1)
	}
	log.Println("=== ALL STAGE 4 PRODUCTION READINESS GATES PASSED ===")
}
