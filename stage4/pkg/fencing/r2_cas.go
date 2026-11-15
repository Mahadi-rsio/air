package fencing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"air-stage4/pkg/cell"
	"air-stage4/pkg/crypto"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
)

var (
	ErrStaleFencingEpoch = errors.New("fencing: local epoch is older than R2 manifest fencing_epoch (CAS rejected)")
	ErrCASPrecondition   = errors.New("fencing: R2 conditional If-Match ETag precondition failed (HTTP 412)")
)

// TenantFenceManifest is stored in Cloudflare R2 at stage4/tenants/<id>/manifest.json
// and guarded by HTTP ETag conditional writes (If-None-Match / If-Match).
type TenantFenceManifest struct {
	TenantID     string                  `json:"tenant_id"`
	FencingEpoch int64                   `json:"fencing_epoch"`
	HolderCell   string                  `json:"holder_cell"`
	UpdatedAt    string                  `json:"updated_at"`
	DEKBundle    crypto.WrappedDEKBundle `json:"dek_bundle"`
	Backups      []cell.BackupArtifact   `json:"backups"`
	WALSegments  []string                `json:"wal_segments"`
}

func ManifestR2Key(tenantID string) string {
	return fmt.Sprintf("stage4/tenants/%s/manifest.json", tenantID)
}

// ReadManifestWithETag fetches the current TenantFenceManifest and its HTTP ETag from Cloudflare R2.
func ReadManifestWithETag(ctx context.Context, s3c *s3.Client, bucket, tenantID string) (*TenantFenceManifest, string, error) {
	key := ManifestR2Key(tenantID)
	out, err := s3c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, "", err
	}
	defer out.Body.Close()
	raw, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", err
	}
	var m TenantFenceManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, "", err
	}
	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return &m, etag, nil
}

// PutManifestCAS atomically creates (expectedETag == "") or updates (If-Match: expectedETag)
// the tenant's manifest in Cloudflare R2, rejecting any writer whose epoch < remote epoch or whose ETag is stale.
func PutManifestCAS(ctx context.Context, s3c *s3.Client, bucket string, m *TenantFenceManifest, expectedETag string) (string, error) {
	// 1. If updating an existing manifest, verify remote epoch hasn't advanced past m.FencingEpoch
	if expectedETag != "" {
		remote, remoteETag, err := ReadManifestWithETag(ctx, s3c, bucket, m.TenantID)
		if err == nil && remote != nil {
			if m.FencingEpoch < remote.FencingEpoch {
				return "", fmt.Errorf("%w: writer epoch=%d < remote epoch=%d", ErrStaleFencingEpoch, m.FencingEpoch, remote.FencingEpoch)
			}
			if expectedETag != remoteETag {
				// Still send If-Match with expectedETag to R2 so R2 itself enforces HTTP 412 Precondition Failed
			}
		}
	}

	m.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", err
	}
	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(ManifestR2Key(m.TenantID)),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
	}
	if expectedETag == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(expectedETag)
	}

	putOut, err := s3c.PutObject(ctx, input)
	if err != nil {
		if strings.Contains(err.Error(), "412") || strings.Contains(err.Error(), "PreconditionFailed") {
			return "", fmt.Errorf("%w: %v", ErrCASPrecondition, err)
		}
		return "", err
	}
	newETag := ""
	if putOut.ETag != nil {
		newETag = *putOut.ETag
	}
	return newETag, nil
}

type Workstream1Report struct {
	Pass                             bool    `json:"pass"`
	WatchdogSelfFenceDeadlineMs      int64   `json:"watchdog_self_fence_deadline_ms"`
	ControlPromoteBarrierMs          int64   `json:"control_promote_barrier_ms"`
	MeasuredWatchdogKillMs           int64   `json:"measured_watchdog_kill_ms"`
	MeasuredNewPrimaryPromoteMs      int64   `json:"measured_new_primary_promote_ms"`
	DisjointSafetyMarginMs           int64   `json:"disjoint_safety_margin_ms"`
	CommitsBeforeWatchdogKill        int64   `json:"commits_before_watchdog_kill"`
	SplitBrainCommitsAfterPromotion  int64   `json:"split_brain_commits_after_promotion"`
	OldPrimaryQuarantinedInAgent     bool    `json:"old_primary_quarantined_in_agent"`
	R2Epoch1InitialETag              string  `json:"r2_epoch1_initial_etag"`
	R2Epoch2PromotedETag             string  `json:"r2_epoch2_promoted_etag"`
	ZombieEpoch1CASRejectedByR2      bool    `json:"zombie_epoch1_cas_rejected_by_r2"`
	ZombieRawIfMatch412RejectedByR2  bool    `json:"zombie_raw_if_match_412_rejected_by_r2"`
	R2ManifestPreservedEpoch2        bool    `json:"r2_manifest_preserved_epoch2"`
}

// RunWorkstream1Fencing executes EXP-S4-1A (Real Agent Monotonic Watchdog Self-Fencing)
// and EXP-S4-1B (Live Cloudflare R2 Conditional If-Match ETag CAS Fencing).
func RunWorkstream1Fencing(ctx context.Context, volDir string, s3c *s3.Client, bucket string) (Workstream1Report, error) {
	var rep Workstream1Report

	// Use 2000ms self-fence deadline vs 3200ms Control Plane promotion barrier (preserving 8s:12s disjoint ratio)
	const selfFenceDeadline = 2000 * time.Millisecond
	const promoteBarrier = 3200 * time.Millisecond
	rep.WatchdogSelfFenceDeadlineMs = selfFenceDeadline.Milliseconds()
	rep.ControlPromoteBarrierMs = promoteBarrier.Milliseconds()

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		return rep, err
	}
	// Spawn isolated Cell Agent #1 (Old Primary Cell)
	cell1Dir := fmt.Sprintf("%s/ws1_cell1", volDir)
	agent1, err := cell.NewAgent(cell1Dir, 1, kms)
	if err != nil {
		return rep, err
	}
	defer agent1.Close()

	// Spawn isolated Cell Agent #2 (New Primary Cell)
	cell2Dir := fmt.Sprintf("%s/ws1_cell2", volDir)
	agent2, err := cell.NewAgent(cell2Dir, 1, kms)
	if err != nil {
		return rep, err
	}
	defer agent2.Close()

	tenantID := "t_ws1_fencing"
	if _, err := agent1.ClaimCluster(tenantID); err != nil {
		return rep, err
	}
	rt1, err := agent1.ActivateCluster(tenantID, "pw_ws1", 1)
	if err != nil {
		return rep, err
	}
	if _, err := agent2.ClaimCluster(tenantID); err != nil {
		return rep, err
	}

	dsn1 := fmt.Sprintf("postgres://app_user:pw_ws1@127.0.0.1:%d/postgres?sslmode=disable", rt1.Port)
	conn1, err := pgx.Connect(ctx, dsn1)
	if err != nil {
		return rep, err
	}
	_, _ = conn1.Exec(ctx, "CREATE TABLE fence_ledger (id SERIAL PRIMARY KEY, epoch INT, written_at TIMESTAMPTZ DEFAULT now())")
	_ = conn1.Close(ctx)

	// 1. EXP-S4-1B Step 1: Cell 1 registers Epoch 1 manifest in live Cloudflare R2 with If-None-Match: "*"
	_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(ManifestR2Key(tenantID)),
	})
	dekBundle, err := kms.EnsureTenantDEK(tenantID)
	if err != nil {
		return rep, err
	}
	mEpoch1 := &TenantFenceManifest{
		TenantID:     tenantID,
		FencingEpoch: 1,
		HolderCell:   "cell-1",
		DEKBundle:    dekBundle,
	}
	etag1, err := PutManifestCAS(ctx, s3c, bucket, mEpoch1, "")
	if err != nil {
		return rep, fmt.Errorf("initial PutManifestCAS epoch 1 failed: %w", err)
	}
	rep.R2Epoch1InitialETag = etag1

	// 2. EXP-S4-1A: Start Cell 1's built-in CLOCK_MONOTONIC Lease Watchdog and cut off Lease ACKs!
	tPartition := time.Now()
	agent1.StartLeaseWatchdog(ctx, selfFenceDeadline)

	var promoted atomic.Bool
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for time.Since(tPartition) < promoteBarrier+600*time.Millisecond {
			c, err := pgx.Connect(ctx, dsn1)
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			_, qErr := c.Exec(ctx, "INSERT INTO fence_ledger (epoch) VALUES (1)")
			_ = c.Close(ctx)
			if qErr == nil {
				if promoted.Load() {
					atomic.AddInt64(&rep.SplitBrainCommitsAfterPromotion, 1)
				} else {
					atomic.AddInt64(&rep.CommitsBeforeWatchdogKill, 1)
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Wait for Control Plane promotion barrier (3200ms > 2000ms watchdog deadline)
	time.Sleep(promoteBarrier)

	// Promote Cell 2 at Epoch 2 in R2 using conditional If-Match: etag1
	mEpoch2 := &TenantFenceManifest{
		TenantID:     tenantID,
		FencingEpoch: 2,
		HolderCell:   "cell-2",
		DEKBundle:    dekBundle,
	}
	etag2, err := PutManifestCAS(ctx, s3c, bucket, mEpoch2, etag1)
	if err != nil {
		return rep, fmt.Errorf("promote PutManifestCAS epoch 2 failed: %w", err)
	}
	rep.R2Epoch2PromotedETag = etag2

	_, err = agent2.ActivateCluster(tenantID, "pw_ws1", 2)
	if err != nil {
		return rep, fmt.Errorf("activate cell2 epoch 2 failed: %w", err)
	}
	promoted.Store(true)
	rep.MeasuredNewPrimaryPromoteMs = time.Since(tPartition).Milliseconds()

	writerWG.Wait()

	fenced, killMs := agent1.WatchdogStatus()
	rep.OldPrimaryQuarantinedInAgent = fenced
	rep.MeasuredWatchdogKillMs = killMs
	rep.DisjointSafetyMarginMs = rep.MeasuredNewPrimaryPromoteMs - rep.MeasuredWatchdogKillMs

	// 3. EXP-S4-1B Step 2: Zombie Cell 1 (epoch 1, holding stale etag1) attempts to overwrite manifest in R2
	_, zombieErr := PutManifestCAS(ctx, s3c, bucket, mEpoch1, etag1)
	rep.ZombieEpoch1CASRejectedByR2 = errors.Is(zombieErr, ErrStaleFencingEpoch) || errors.Is(zombieErr, ErrCASPrecondition)

	// Also test raw S3 PutObject with stale If-Match: etag1 directly against Cloudflare R2 to prove HTTP 412
	staleBody, _ := json.Marshal(mEpoch1)
	_, rawPutErr := s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(ManifestR2Key(tenantID)),
		Body:    bytes.NewReader(staleBody),
		IfMatch: aws.String(etag1),
	})
	if rawPutErr != nil && (strings.Contains(rawPutErr.Error(), "412") || strings.Contains(rawPutErr.Error(), "PreconditionFailed")) {
		rep.ZombieRawIfMatch412RejectedByR2 = true
	}

	// Verify R2 manifest is still untouched at Epoch 2 and HolderCell == "cell-2"
	finalManifest, finalETag, err := ReadManifestWithETag(ctx, s3c, bucket, tenantID)
	if err == nil && finalManifest.FencingEpoch == 2 && finalManifest.HolderCell == "cell-2" && finalETag == etag2 {
		rep.R2ManifestPreservedEpoch2 = true
	}

	// Cleanup R2 test key
	_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(ManifestR2Key(tenantID)),
	})

	rep.Pass = rep.OldPrimaryQuarantinedInAgent &&
		rep.MeasuredWatchdogKillMs > 0 &&
		rep.MeasuredWatchdogKillMs < rep.ControlPromoteBarrierMs &&
		rep.DisjointSafetyMarginMs >= 800 &&
		rep.CommitsBeforeWatchdogKill > 0 &&
		rep.SplitBrainCommitsAfterPromotion == 0 &&
		rep.ZombieEpoch1CASRejectedByR2 &&
		rep.ZombieRawIfMatch412RejectedByR2 &&
		rep.R2ManifestPreservedEpoch2

	return rep, nil
}
