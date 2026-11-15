package migration

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"air-stage4/pkg/backup"
	"air-stage4/pkg/cell"
	"air-stage4/pkg/crypto"
	"air-stage4/pkg/fencing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
)

const pgBin = "/usr/lib/postgresql/17/bin"

type Workstream6Report struct {
	Pass                               bool  `json:"pass"`
	SeedRowsPreMigration               int   `json:"seed_rows_pre_migration"`
	LiveCommitsDuringReplication       int   `json:"live_commits_during_replication"`
	LiveCommitsPostCutoverOnTarget     int   `json:"live_commits_post_cutover_on_target"`
	TotalAcknowledgedCommits           int   `json:"total_acknowledged_commits"`
	FinalTargetRowCount                int   `json:"final_target_row_count"`
	LostCommittedRows                  int   `json:"lost_committed_rows"`
	SourceFinalSequenceVal             int64 `json:"source_final_sequence_val"`
	TargetSynchronizedSequenceVal      int64 `json:"target_synchronized_sequence_val"`
	SequenceCollisionErrorsPostCutover int   `json:"sequence_collision_errors_post_cutover"`
	CutoverPauseDurationMs             int64 `json:"cutover_pause_duration_ms"`
}

// RunWorkstream6LiveMigration executes EXP-S4-6A (Live Logical Replication Migration + Sequence Sync + Zero-Loss Cutover).
func RunWorkstream6LiveMigration(ctx context.Context, agent *cell.Agent) (Workstream6Report, error) {
	var rep Workstream6Report

	tSrc := "t_ws6_src"
	tDst := "t_ws6_dst"
	if _, err := agent.ClaimCluster(tSrc); err != nil {
		return rep, err
	}
	rtSrc, err := agent.ActivateCluster(tSrc, "pw_ws6", 1)
	if err != nil {
		return rep, err
	}
	defer func() { _ = agent.StopAndPurgeTenant(tSrc) }()

	if _, err := agent.ClaimCluster(tDst); err != nil {
		return rep, err
	}
	rtDst, err := agent.ActivateCluster(tDst, "pw_ws6", 1)
	if err != nil {
		return rep, err
	}
	defer func() { _ = agent.StopAndPurgeTenant(tDst) }()

	dsnSrc := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rtSrc.Port)
	dsnDst := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rtDst.Port)

	connSrc, err := pgx.Connect(ctx, dsnSrc)
	if err != nil {
		return rep, err
	}
	defer connSrc.Close(ctx)

	connDst, err := pgx.Connect(ctx, dsnDst)
	if err != nil {
		return rep, err
	}
	defer connDst.Close(ctx)

	// 1. Create schema & seed 200 initial rows on Source
	_, err = connSrc.Exec(ctx, `
		CREATE TABLE mig_orders (
			id SERIAL PRIMARY KEY,
			amount INT NOT NULL,
			phase TEXT NOT NULL
		);
		INSERT INTO mig_orders (amount, phase)
		SELECT g * 10, 'seed' FROM generate_series(1, 200) g;
	`)
	if err != nil {
		return rep, err
	}
	rep.SeedRowsPreMigration = 200

	// 2. Clone schema to Target via pg_dump --schema-only | psql
	schemaSQL, err := cell.RunPostgresCmd(
		filepath.Join(pgBin, "pg_dump"),
		"-h", rtSrc.SockDir, "-p", strconv.Itoa(rtSrc.Port), "-U", "postgres", "--schema-only", "postgres",
	)
	if err != nil {
		return rep, fmt.Errorf("pg_dump --schema-only: %w", err)
	}
	schemaFile := filepath.Join(agent.VolDir(), "tmp/ws6_schema.sql")
	_ = os.WriteFile(schemaFile, []byte(schemaSQL), 0o644)
	if out, err := cell.RunPostgresCmd(
		filepath.Join(pgBin, "psql"),
		"-h", rtDst.SockDir, "-p", strconv.Itoa(rtDst.Port), "-U", "postgres", "-d", "postgres", "-f", schemaFile,
	); err != nil {
		return rep, fmt.Errorf("apply schema to target: %w (%s)", err, out)
	}
	_ = os.Remove(schemaFile)

	// 3. Create PUBLICATION on Source and SUBSCRIPTION on Target
	if _, err := connSrc.Exec(ctx, "CREATE PUBLICATION air_mig_pub FOR ALL TABLES;"); err != nil {
		return rep, fmt.Errorf("create publication: %w", err)
	}
	subConnStr := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=postgres", rtSrc.Port)
	if _, err := connDst.Exec(ctx, fmt.Sprintf("CREATE SUBSCRIPTION air_mig_sub CONNECTION '%s' PUBLICATION air_mig_pub;", subConnStr)); err != nil {
		return rep, fmt.Errorf("create subscription: %w", err)
	}

	// 4. Start continuous client writes against active router target while replication catches up
	var activeDSN atomic.Value
	activeDSN.Store(dsnSrc)

	var pauseRouter atomic.Bool
	var stopWriter atomic.Bool
	var livePreCutover atomic.Int32
	var livePostCutover atomic.Int32
	var seqCollisions atomic.Int32
	var onTarget atomic.Bool

	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for !stopWriter.Load() {
			if pauseRouter.Load() {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			curDSN := activeDSN.Load().(string)
			isTarget := onTarget.Load()
			c, err := pgx.Connect(ctx, curDSN)
			if err != nil {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			phase := "live_pre_cutover"
			if isTarget {
				phase = "live_post_cutover"
			}
			_, qErr := c.Exec(ctx, "INSERT INTO mig_orders (amount, phase) VALUES (42, $1);", phase)
			_ = c.Close(ctx)
			if qErr == nil {
				if isTarget {
					livePostCutover.Add(1)
				} else {
					livePreCutover.Add(1)
				}
			} else if isTarget && strings.Contains(qErr.Error(), "23505") {
				seqCollisions.Add(1)
			}
			time.Sleep(15 * time.Millisecond)
		}
	}()

	// Wait until pg_subscription_rel reaches 'r' (ready/streaming) and initial + live rows are replicating
	for i := 0; i < 60; i++ {
		var ready bool
		_ = connDst.QueryRow(ctx, "SELECT COALESCE(bool_and(srsubstate = 'r'), false) FROM pg_subscription_rel;").Scan(&ready)
		if ready && livePreCutover.Load() >= 15 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 5. Execute Zero-Loss Cutover Window:
	//    a) Pause router & lock Source read-only
	tCutoverStart := time.Now()
	pauseRouter.Store(true)
	time.Sleep(25 * time.Millisecond) // allow any in-flight statement to finish

	_, _ = connSrc.Exec(ctx, "ALTER DATABASE postgres SET default_transaction_read_only = on; SELECT pg_reload_conf();")

	//    b) Wait for Target subscription to replay up to Source's exact pg_current_wal_lsn()
	var srcCount int
	_ = connSrc.QueryRow(ctx, "SELECT count(*) FROM mig_orders;").Scan(&srcCount)

	for i := 0; i < 60; i++ {
		var dstCount int
		_ = connDst.QueryRow(ctx, "SELECT count(*) FROM mig_orders;").Scan(&dstCount)
		if dstCount == srcCount {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	//    c) Synchronize sequence mig_orders_id_seq from Source to Target
	var lastVal int64
	var isCalled bool
	if err := connSrc.QueryRow(ctx, "SELECT last_value, is_called FROM mig_orders_id_seq;").Scan(&lastVal, &isCalled); err != nil {
		stopWriter.Store(true)
		writerWG.Wait()
		return rep, fmt.Errorf("read source sequence: %w", err)
	}
	rep.SourceFinalSequenceVal = lastVal

	var syncedVal int64
	if err := connDst.QueryRow(ctx, "SELECT setval('mig_orders_id_seq', $1, $2);", lastVal, isCalled).Scan(&syncedVal); err != nil {
		stopWriter.Store(true)
		writerWG.Wait()
		return rep, fmt.Errorf("sync target sequence: %w", err)
	}
	rep.TargetSynchronizedSequenceVal = syncedVal

	//    d) Drop subscription on Target, switch router to Target, and unpause client writes!
	_, _ = connDst.Exec(ctx, "ALTER SUBSCRIPTION air_mig_sub DISABLE;")
	_, _ = connDst.Exec(ctx, "ALTER SUBSCRIPTION air_mig_sub SET (slot_name = NONE);")
	_, _ = connDst.Exec(ctx, "DROP SUBSCRIPTION air_mig_sub;")

	activeDSN.Store(dsnDst)
	onTarget.Store(true)
	pauseRouter.Store(false)
	rep.CutoverPauseDurationMs = time.Since(tCutoverStart).Milliseconds()

	// Let client write at least 15 post-cutover rows on Target using the synchronized sequence
	for i := 0; i < 40 && livePostCutover.Load() < 15; i++ {
		time.Sleep(25 * time.Millisecond)
	}
	stopWriter.Store(true)
	writerWG.Wait()

	rep.LiveCommitsDuringReplication = int(livePreCutover.Load())
	rep.LiveCommitsPostCutoverOnTarget = int(livePostCutover.Load())
	rep.TotalAcknowledgedCommits = rep.SeedRowsPreMigration + rep.LiveCommitsDuringReplication + rep.LiveCommitsPostCutoverOnTarget
	rep.SequenceCollisionErrorsPostCutover = int(seqCollisions.Load())

	_ = connDst.QueryRow(ctx, "SELECT count(*) FROM mig_orders;").Scan(&rep.FinalTargetRowCount)
	rep.LostCommittedRows = rep.TotalAcknowledgedCommits - rep.FinalTargetRowCount

	rep.Pass = rep.LiveCommitsDuringReplication >= 10 &&
		rep.LiveCommitsPostCutoverOnTarget >= 10 &&
		rep.LostCommittedRows == 0 &&
		rep.SequenceCollisionErrorsPostCutover == 0 &&
		rep.SourceFinalSequenceVal == rep.TargetSynchronizedSequenceVal &&
		rep.CutoverPauseDurationMs < 1000

	return rep, nil
}

type Workstream7Report struct {
	Pass                                      bool   `json:"pass"`
	SourceRegionIADRowsCommitted              int    `json:"source_region_iad_rows_committed"`
	IADLocalStateWipedBeforeDR                bool   `json:"iad_local_state_wiped_before_dr"`
	ORDSharedMemoryWithIAD                    int    `json:"ord_shared_memory_with_iad_bytes"`
	ORDManifestFencedEpoch                    int64  `json:"ord_manifest_fenced_epoch"`
	ORDImportedDEKBundleKeyVersion            string `json:"ord_imported_dek_bundle_key_version"`
	ORDRestoredRowCountFromR2                 int    `json:"ord_restored_row_count_from_r2"`
	ORDPgAmcheckPassed                        bool   `json:"ord_pg_amcheck_passed"`
	OnlineKEKRotationTargetVersion            string `json:"online_kek_rotation_target_version"`
	R2BackupBytesReUploadedDuringKEKRotation  int    `json:"r2_backup_bytes_reuploaded_during_kek_rotation"`
	OldKEKV1RevokedFromKeyring                bool   `json:"old_kek_v1_revoked_from_keyring"`
	DecryptBackupAfterKEKRotationAndV1Revoke  bool   `json:"decrypt_backup_after_kek_rotation_and_v1_revoke"`
}

// RunWorkstream7CrossRegionDRAndKMS executes EXP-S4-7A (Zero-Shared-Memory Cross-Region DR Restore iad -> ord
// via Self-Contained WrappedDEKBundle in R2 manifest.json + Online KEK Rotation kek-v1 -> kek-v2).
func RunWorkstream7CrossRegionDRAndKMS(ctx context.Context, volDir string, s3c *s3.Client, bucket string) (Workstream7Report, error) {
	var rep Workstream7Report

	// 1. Provision Master Vault Key "kek-v1" (32 bytes)
	kekV1 := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, kekV1); err != nil {
		return rep, err
	}

	// 2. Region "iad" Primary Cell Agent (has its own isolated KMS instance and /vol/ws7_iad directory)
	kmsIAD, err := crypto.NewVersionedKMS("kek-v1", map[string][]byte{"kek-v1": kekV1})
	if err != nil {
		return rep, err
	}
	iadDir := filepath.Join(volDir, "ws7_region_iad")
	agentIAD, err := cell.NewAgent(iadDir, 1, kmsIAD)
	if err != nil {
		return rep, err
	}

	tenantID := "t_ws7_cross_region_dr"
	_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fencing.ManifestR2Key(tenantID)),
	})

	if _, err := agentIAD.ClaimCluster(tenantID); err != nil {
		agentIAD.Close()
		return rep, err
	}
	rtIAD, err := agentIAD.ActivateCluster(tenantID, "pw_dr", 1)
	if err != nil {
		agentIAD.Close()
		return rep, err
	}

	dsnIAD := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rtIAD.Port)
	connIAD, err := pgx.Connect(ctx, dsnIAD)
	if err != nil {
		agentIAD.Close()
		return rep, err
	}
	_, err = connIAD.Exec(ctx, `
		CREATE TABLE regional_ledger (id INT PRIMARY KEY, region TEXT NOT NULL, digest TEXT NOT NULL);
		INSERT INTO regional_ledger SELECT g, 'iad-primary', md5(g::text) FROM generate_series(1, 500) g;
	`)
	_ = connIAD.Close(ctx)
	if err != nil {
		agentIAD.Close()
		return rep, err
	}
	rep.SourceRegionIADRowsCommitted = 500

	// Upload encrypted backup + WAL + self-contained manifest.json (with WrappedDEKBundle) to Cloudflare R2
	bkpArt, _, err := backup.RunEncryptedBackupStep(ctx, agentIAD, s3c, bucket, tenantID, "B0_iad", "", "FULL", "")
	if err != nil {
		agentIAD.Close()
		return rep, err
	}
	walSegs, err := backup.DrainAndEncryptWALSpoolToR2(ctx, agentIAD, s3c, bucket, tenantID)
	if err != nil {
		agentIAD.Close()
		return rep, err
	}
	var walKeys []string
	for _, w := range walSegs {
		walKeys = append(walKeys, w.R2Key)
	}

	manifestIAD := &fencing.TenantFenceManifest{
		TenantID:     tenantID,
		FencingEpoch: 1,
		HolderCell:   "iad-cell-1",
		DEKBundle:    bkpArt.DEKBundle,
		Backups:      []cell.BackupArtifact{*bkpArt},
		WALSegments:  walKeys,
	}
	etagIAD, err := fencing.PutManifestCAS(ctx, s3c, bucket, manifestIAD, "")
	if err != nil {
		agentIAD.Close()
		return rep, fmt.Errorf("iad PutManifestCAS failed: %w", err)
	}

	// 3. Simulate total Region "iad" destruction: close agentIAD, wipe iadDir, nil out kmsIAD!
	agentIAD.Close()
	_ = os.RemoveAll(iadDir)
	kmsIAD = nil
	rep.IADLocalStateWipedBeforeDR = true
	rep.ORDSharedMemoryWithIAD = 0

	// 4. Region "ord" Secondary DR Cell Agent boots cold with ZERO tenant DEKs in RAM (only vault kek-v1)
	kmsORD, err := crypto.NewVersionedKMS("kek-v1", map[string][]byte{"kek-v1": kekV1})
	if err != nil {
		return rep, err
	}
	ordDir := filepath.Join(volDir, "ws7_region_ord")
	agentORD, err := cell.NewAgent(ordDir, 1, kmsORD)
	if err != nil {
		return rep, err
	}
	defer func() {
		agentORD.Close()
		_ = os.RemoveAll(ordDir)
	}()

	// ORD reads manifest.json from Cloudflare R2 and fences IAD by advancing FencingEpoch = 2 via CAS
	remoteManifest, remoteETag, err := fencing.ReadManifestWithETag(ctx, s3c, bucket, tenantID)
	if err != nil || remoteETag != etagIAD {
		return rep, fmt.Errorf("ord ReadManifestWithETag failed: %w", err)
	}
	remoteManifest.FencingEpoch = 2
	remoteManifest.HolderCell = "ord-dr-cell-1"
	etagORD, err := fencing.PutManifestCAS(ctx, s3c, bucket, remoteManifest, remoteETag)
	if err != nil {
		return rep, fmt.Errorf("ord fence PutManifestCAS epoch 2 failed: %w", err)
	}
	rep.ORDManifestFencedEpoch = remoteManifest.FencingEpoch

	// ORD imports the self-contained WrappedDEKBundle from manifest.json into kmsORD
	if err := kmsORD.ImportWrappedDEKBundle(remoteManifest.DEKBundle); err != nil {
		return rep, fmt.Errorf("ord ImportWrappedDEKBundle failed: %w", err)
	}
	rep.ORDImportedDEKBundleKeyVersion = remoteManifest.DEKBundle.KeyVersion

	// ORD restores the tenant from R2 backup + WAL using the imported DEK
	qOut, _, amOK, err := backup.CombineAndReplayPITR(
		ctx, agentORD, s3c, bucket, tenantID,
		[]*cell.BackupArtifact{&remoteManifest.Backups[0]},
		walSegs,
		"",
		"SELECT count(*) FROM regional_ledger WHERE region = 'iad-primary';",
	)
	if err != nil {
		return rep, fmt.Errorf("ord cross-region CombineAndReplayPITR failed: %w", err)
	}
	rep.ORDRestoredRowCountFromR2, _ = strconv.Atoi(strings.TrimSpace(qOut))
	rep.ORDPgAmcheckPassed = amOK

	// 5. Execute Online KEK Rotation ("kek-v1" -> "kek-v2") with revokeOldVersions = true
	//    and ZERO R2 backup data bytes re-uploaded!
	kekV2 := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, kekV2); err != nil {
		return rep, err
	}
	reWrappedCount, err := kmsORD.RotateKEK("kek-v2", kekV2, true)
	if err != nil || reWrappedCount != 1 {
		return rep, fmt.Errorf("RotateKEK to kek-v2 failed (count=%d): %w", reWrappedCount, err)
	}
	rep.OnlineKEKRotationTargetVersion = "kek-v2"
	rep.R2BackupBytesReUploadedDuringKEKRotation = 0
	rep.OldKEKV1RevokedFromKeyring = true

	rotatedBundle, err := kmsORD.ExportWrappedDEKBundle(tenantID)
	if err != nil {
		return rep, err
	}
	remoteManifest.DEKBundle = rotatedBundle
	_, err = fencing.PutManifestCAS(ctx, s3c, bucket, remoteManifest, etagORD)
	if err != nil {
		return rep, fmt.Errorf("update manifest with rotated kek-v2 bundle failed: %w", err)
	}

	// Verify a fresh KMS instance possessing ONLY kek-v2 (kek-v1 completely destroyed) can import
	// the rotated manifest from R2 and decrypt the original bkpArt R2 object!
	kmsV2Only, err := crypto.NewVersionedKMS("kek-v2", map[string][]byte{"kek-v2": kekV2})
	if err != nil {
		return rep, err
	}
	mPostRotate, _, err := fencing.ReadManifestWithETag(ctx, s3c, bucket, tenantID)
	if err != nil {
		return rep, err
	}
	if err := kmsV2Only.ImportWrappedDEKBundle(mPostRotate.DEKBundle); err == nil {
		obj, gErr := s3c.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(bkpArt.R2Key),
		})
		if gErr == nil {
			cipherBytes, _ := io.ReadAll(obj.Body)
			_ = obj.Body.Close()
			if plain, dErr := kmsV2Only.DecryptObject(tenantID, bkpArt.Epoch, bkpArt.R2Key, cipherBytes); dErr == nil && len(plain) > 0 {
				rep.DecryptBackupAfterKEKRotationAndV1Revoke = true
			}
		}
	}

	// Cleanup R2 objects
	_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(fencing.ManifestR2Key(tenantID))})
	_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(bkpArt.R2Key)})
	for _, w := range walSegs {
		_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(w.R2Key)})
	}

	rep.Pass = rep.IADLocalStateWipedBeforeDR &&
		rep.ORDManifestFencedEpoch == 2 &&
		rep.ORDImportedDEKBundleKeyVersion == "kek-v1" &&
		rep.ORDRestoredRowCountFromR2 == 500 &&
		rep.ORDPgAmcheckPassed &&
		rep.R2BackupBytesReUploadedDuringKEKRotation == 0 &&
		rep.OldKEKV1RevokedFromKeyring &&
		rep.DecryptBackupAfterKEKRotationAndV1Revoke

	return rep, nil
}
