package control

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/Mahadi-rsio/air/internal/fencing"
	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/Mahadi-rsio/air/internal/tenant"
	"github.com/jackc/pgx/v5"
)

// OffloadStatus describes the durable state of a tenant cold-offload operation.
type OffloadStatus struct {
	OpID            string     `json:"op_id"`
	TenantID        string     `json:"tenant_id"`
	Generation      string     `json:"generation"`
	State           string     `json:"state"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	UploadedObjects int        `json:"uploaded_objects"`
	FailedObjects   int        `json:"failed_objects"`
	ErrorDetails    string     `json:"error_details,omitempty"`
	RetryCount      int        `json:"retry_count"`
}

type coldOffloadJob struct {
	tenantID   string
	generation string
	opID       string
	seq        int64
}

func (cp *ControlPlane) shouldEvictLocalOnSuspend() bool {
	return cp.evictOnSuspend || os.Getenv("AIR_EVICT_ON_SUSPEND") == "true"
}

func (cp *ControlPlane) cancelActiveOffload(tenantID string) {
	cp.offloadMu.Lock()
	cancel, ok := cp.activeOffloadCancels[tenantID]
	cp.offloadMu.Unlock()
	if ok && cancel != nil {
		cancel()
	}
}

func (cp *ControlPlane) registerActiveOffloadCancel(tenantID string, cancel context.CancelFunc) {
	cp.offloadMu.Lock()
	if cp.activeOffloadCancels == nil {
		cp.activeOffloadCancels = make(map[string]context.CancelFunc)
	}
	if prev, ok := cp.activeOffloadCancels[tenantID]; ok && prev != nil {
		prev()
	}
	cp.activeOffloadCancels[tenantID] = cancel
	cp.offloadMu.Unlock()
}

func (cp *ControlPlane) clearActiveOffloadCancel(tenantID string) {
	cp.offloadMu.Lock()
	delete(cp.activeOffloadCancels, tenantID)
	cp.offloadMu.Unlock()
}

func (cp *ControlPlane) createOffloadOp(ctx context.Context, tenantID, generation, initialState string) string {
	opID := newOffloadOpID()
	if cp.db == nil {
		return opID
	}
	var startedAt any
	if initialState == "running" {
		startedAt = time.Now().UTC()
	}
	_, _ = cp.db.Exec(ctx,
		`INSERT INTO offload_ops (op_id, tenant_id, generation, state, started_at) VALUES ($1, $2, $3, $4, $5)`,
		opID, tenantID, generation, initialState, startedAt,
	)
	return opID
}

func (cp *ControlPlane) updateOffloadOpRunning(ctx context.Context, opID string, retryCount int) {
	if cp.db == nil || opID == "" {
		return
	}
	_, _ = cp.db.Exec(ctx,
		`UPDATE offload_ops SET state = 'running', started_at = COALESCE(started_at, now()), retry_count = $2, updated_at = now()
		 WHERE op_id = $1 AND state NOT IN ('succeeded', 'cancelled')`,
		opID, retryCount,
	)
}

func (cp *ControlPlane) finishOffloadOpSucceeded(ctx context.Context, opID string, uploaded, failed int) {
	if cp.db == nil || opID == "" {
		return
	}
	_, _ = cp.db.Exec(ctx,
		`UPDATE offload_ops SET state = 'succeeded', completed_at = now(), uploaded_objects = $2, failed_objects = $3, error_details = '', updated_at = now()
		 WHERE op_id = $1 AND state <> 'cancelled'`,
		opID, uploaded, failed,
	)
}

func (cp *ControlPlane) finishOffloadOpFailed(ctx context.Context, opID string, uploaded, failed, retryCount int, errMsg string) {
	if cp.db == nil || opID == "" {
		return
	}
	_, _ = cp.db.Exec(ctx,
		`UPDATE offload_ops SET state = 'failed', completed_at = now(), uploaded_objects = $2, failed_objects = $3, retry_count = $4, error_details = $5, updated_at = now()
		 WHERE op_id = $1 AND state <> 'cancelled'`,
		opID, uploaded, failed, retryCount, errMsg,
	)
}

func (cp *ControlPlane) finishOffloadOpCancelled(ctx context.Context, opID, reason string) {
	if cp.db == nil || opID == "" {
		return
	}
	_, _ = cp.db.Exec(ctx,
		`UPDATE offload_ops SET state = 'cancelled', completed_at = COALESCE(completed_at, now()), error_details = CASE WHEN error_details = '' THEN $2 ELSE error_details END, updated_at = now()
		 WHERE op_id = $1 AND state IN ('pending', 'running')`,
		opID, reason,
	)
}

// GetLatestOffloadStatus returns the most recent offload operation for tenantID.
func (cp *ControlPlane) GetLatestOffloadStatus(ctx context.Context, accountID, tenantID string) (*OffloadStatus, error) {
	if accountID != "" {
		if err := tenant.ValidateAccountID(accountID); err != nil {
			return nil, err
		}
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	if accountID != "" {
		var owner string
		err := cp.db.QueryRow(ctx, `SELECT account_id FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&owner)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, err
		}
		if owner != accountID {
			return nil, ErrForbidden
		}
	}
	var st OffloadStatus
	err := cp.db.QueryRow(ctx,
		`SELECT op_id, tenant_id, generation, state, started_at, completed_at, uploaded_objects, failed_objects, error_details, retry_count
		 FROM offload_ops WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 1`,
		tenantID,
	).Scan(&st.OpID, &st.TenantID, &st.Generation, &st.State, &st.StartedAt, &st.CompletedAt, &st.UploadedObjects, &st.FailedObjects, &st.ErrorDetails, &st.RetryCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &st, nil
}

// GetOffloadStatus returns a specific offload operation by opID.
func (cp *ControlPlane) GetOffloadStatus(ctx context.Context, opID string) (*OffloadStatus, error) {
	var st OffloadStatus
	err := cp.db.QueryRow(ctx,
		`SELECT op_id, tenant_id, generation, state, started_at, completed_at, uploaded_objects, failed_objects, error_details, retry_count
		 FROM offload_ops WHERE op_id = $1`,
		opID,
	).Scan(&st.OpID, &st.TenantID, &st.Generation, &st.State, &st.StartedAt, &st.CompletedAt, &st.UploadedObjects, &st.FailedObjects, &st.ErrorDetails, &st.RetryCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &st, nil
}

// StartColdOffloadWorker starts background workers that upload cold data of suspended tenants
// to R2 and delete local pgdata/WAL cache once durable R2 sync succeeds.
func (cp *ControlPlane) StartColdOffloadWorker(ctx context.Context, workers int) {
	if workers <= 0 {
		workers = 2
	}
	cp.coldMu.Lock()
	if cp.coldQueueStarted {
		cp.coldMu.Unlock()
		return
	}
	workerCtx, workerCancel := context.WithCancel(ctx)
	cp.coldQueueStarted = true
	cp.coldWorkerCancel = workerCancel
	cp.coldPending = make(map[string]bool)
	cp.coldQueueCh = make(chan coldOffloadJob, 256)
	if cp.coldRetryDelay <= 0 {
		cp.coldRetryDelay = 5 * time.Second
	}
	if cp.coldMaxAttempts <= 0 {
		cp.coldMaxAttempts = 5
	}
	cp.coldMu.Unlock()

	for i := 0; i < workers; i++ {
		cp.coldWorkersWg.Add(1)
		go func() {
			defer cp.coldWorkersWg.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case job := <-cp.coldQueueCh:
					cp.coldMu.Lock()
					delete(cp.coldPending, job.tenantID)
					cp.coldMu.Unlock()
					cp.runColdOffloadWithRetry(workerCtx, job)
				}
			}
		}()
	}

	// On startup, recover any interrupted offload_ops and enqueue suspended tenants with local pgdata.
	if cp.db != nil {
		_, _ = cp.db.Exec(ctx,
			`UPDATE offload_ops o SET state = 'cancelled', completed_at = COALESCE(completed_at, now()),
			 error_details = CASE WHEN error_details = '' THEN 'cancelled after server restart' ELSE error_details END,
			 updated_at = now()
			 WHERE o.state IN ('pending', 'running')
			   AND NOT EXISTS (
			     SELECT 1 FROM clusters c
			     WHERE c.tenant_id = o.tenant_id AND c.state = 'SUSPENDED' AND COALESCE(c.generation, '') = o.generation
			   )`,
		)
	}
	if cp.agent != nil && cp.db != nil {
		rows, err := cp.db.Query(ctx, `SELECT tenant_id, COALESCE(generation, ''), COALESCE(lifecycle_seq, 1) FROM clusters WHERE state = 'SUSPENDED'`)
		if err == nil {
			type suspRow struct {
				tid string
				gen string
				seq int64
			}
			var suspended []suspRow
			for rows.Next() {
				var r suspRow
				if scanErr := rows.Scan(&r.tid, &r.gen, &r.seq); scanErr == nil {
					suspended = append(suspended, r)
				}
			}
			rows.Close()
			for _, r := range suspended {
				if cp.agent.HasLocalPGData(r.tid) {
					var existingOp string
					_ = cp.db.QueryRow(ctx,
						`SELECT op_id FROM offload_ops WHERE tenant_id = $1 AND generation = $2 AND state IN ('pending', 'running') ORDER BY created_at DESC LIMIT 1`,
						r.tid, r.gen,
					).Scan(&existingOp)
					if existingOp == "" {
						existingOp = cp.createOffloadOp(ctx, r.tid, r.gen, "pending")
					}
					cp.enqueueColdOffloadJob(coldOffloadJob{tenantID: r.tid, generation: r.gen, opID: existingOp, seq: r.seq})
				}
			}
		}
	}
}

// EnqueueColdOffload queues a suspended tenant for asynchronous R2 cold upload and local cache eviction.
func (cp *ControlPlane) EnqueueColdOffload(tenantID string) {
	if err := tenant.ValidateID(tenantID); err != nil {
		return
	}
	var gen string
	var seq int64
	if cp.db != nil {
		_ = cp.db.QueryRow(context.Background(), `SELECT COALESCE(generation, ''), COALESCE(lifecycle_seq, 1) FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&gen, &seq)
	}
	opID := cp.createOffloadOp(context.Background(), tenantID, gen, "pending")
	cp.enqueueColdOffloadJob(coldOffloadJob{tenantID: tenantID, generation: gen, opID: opID, seq: seq})
}

func (cp *ControlPlane) enqueueColdOffloadJob(job coldOffloadJob) {
	if err := tenant.ValidateID(job.tenantID); err != nil {
		return
	}
	cp.coldMu.Lock()
	if !cp.coldQueueStarted || cp.coldQueueCh == nil {
		cp.coldMu.Unlock()
		return
	}
	if cp.coldPending[job.tenantID] {
		cp.coldMu.Unlock()
		return
	}
	cp.coldPending[job.tenantID] = true
	ch := cp.coldQueueCh
	cp.coldMu.Unlock()

	select {
	case ch <- job:
	default:
		cp.coldMu.Lock()
		delete(cp.coldPending, job.tenantID)
		cp.coldMu.Unlock()
		log.Printf("[cold-offload] queue full, skipping enqueue for %s", job.tenantID)
	}
}

func (cp *ControlPlane) runColdOffloadWithRetry(ctx context.Context, job coldOffloadJob) {
	cp.coldMu.Lock()
	maxAttempts := cp.coldMaxAttempts
	delay := cp.coldRetryDelay
	cp.coldMu.Unlock()
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	if delay <= 0 {
		delay = 5 * time.Second
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			cp.finishOffloadOpCancelled(context.Background(), job.opID, "cancelled by server shutdown")
			return
		}
		cp.updateOffloadOpRunning(ctx, job.opID, attempt-1)
		jobCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		evicted, uploaded, failed, cancelled, err := cp.processColdOffloadJob(jobCtx, job)
		cancel()
		if cancelled {
			cp.finishOffloadOpCancelled(context.Background(), job.opID, "cancelled by lifecycle transition")
			return
		}
		if err == nil {
			cp.finishOffloadOpSucceeded(context.Background(), job.opID, uploaded, failed)
			if evicted {
				log.Printf("[cold-offload] tenant=%s gen=%s r2_sync=ok evicted=true", job.tenantID, job.generation)
			}
			return
		}
		log.Printf("[cold-offload] tenant=%s gen=%s attempt=%d/%d failed (keeping local cache): %v", job.tenantID, job.generation, attempt, maxAttempts, err)
		if attempt == maxAttempts {
			cp.finishOffloadOpFailed(context.Background(), job.opID, uploaded, failed, attempt-1, err.Error())
			return
		}
		backoff := delay * time.Duration(1<<uint(attempt-1))
		if backoff > 60*time.Second {
			backoff = 60 * time.Second
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			cp.finishOffloadOpCancelled(context.Background(), job.opID, "cancelled by server shutdown")
			return
		case <-timer.C:
		}
	}
}

func (cp *ControlPlane) processColdOffload(ctx context.Context, tenantID string) (bool, error) {
	var gen string
	var seq int64
	if cp.db != nil {
		_ = cp.db.QueryRow(ctx, `SELECT COALESCE(generation, ''), COALESCE(lifecycle_seq, 1) FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&gen, &seq)
	}
	evicted, _, _, _, err := cp.processColdOffloadJob(ctx, coldOffloadJob{tenantID: tenantID, generation: gen, seq: seq})
	return evicted, err
}

func (cp *ControlPlane) processColdOffloadJob(ctx context.Context, job coldOffloadJob) (evicted bool, uploaded int, failed int, cancelled bool, err error) {
	if cp.agent == nil {
		return false, 0, 0, false, nil
	}

	// Phase 1 (brief lock): validate tenant state, generation, and lifecycle_seq; register cancellable context.
	unlock := cp.lockTenantLifecycle(job.tenantID)
	var state, curGen string
	var epoch, curSeq int64
	qErr := cp.db.QueryRow(ctx,
		`SELECT state, fencing_epoch, COALESCE(generation, ''), COALESCE(lifecycle_seq, 1) FROM clusters WHERE tenant_id = $1`,
		job.tenantID,
	).Scan(&state, &epoch, &curGen, &curSeq)
	if errors.Is(qErr, pgx.ErrNoRows) || state != "SUSPENDED" || (job.generation != "" && curGen != job.generation) || (job.seq != 0 && curSeq != job.seq) {
		unlock()
		return false, 0, 0, true, nil
	}
	if qErr != nil {
		unlock()
		return false, 0, 0, false, qErr
	}
	offloadCtx, cancel := context.WithCancel(ctx)
	cp.registerActiveOffloadCancel(job.tenantID, cancel)
	unlock()
	defer func() {
		cancel()
		cp.clearActiveOffloadCancel(job.tenantID)
	}()

	// Phase 2 (NO lifecycle lock held): perform potentially slow R2 network uploads.
	ps := cp.getPageserver()
	if ps != nil {
		if curGen != "" {
			ps.SetTenantGeneration(offloadCtx, job.tenantID, curGen)
		}
		lm, fErr := ps.FlushTenant(offloadCtx, job.tenantID)
		if fErr != nil {
			if offloadCtx.Err() != nil && ctx.Err() == nil {
				return false, uploaded, failed, true, nil
			}
			if errors.Is(fErr, pageserver.ErrStaleGeneration) {
				return false, uploaded, failed, true, nil
			}
			return false, uploaded, failed + 1, false, fmt.Errorf("flush pageserver layers: %w", fErr)
		}
		if lm != nil {
			uploaded++
		}
		if pgDataDir, pdErr := cp.agent.TenantPGDataDir(job.tenantID); pdErr == nil {
			if _, stErr := os.Stat(filepath.Join(pgDataDir, "PG_VERSION")); stErr == nil {
				if sErr := ps.SaveMetadataSkeleton(offloadCtx, job.tenantID, epoch, pgDataDir); sErr != nil {
					if offloadCtx.Err() != nil && ctx.Err() == nil {
						return false, uploaded, failed, true, nil
					}
					return false, uploaded, failed + 1, false, fmt.Errorf("save metadata skeleton: %w", sErr)
				}
				uploaded++
			}
		}
	}

	walCnt, wErr := cp.drainWALSpoolAndUpdateManifestGen(offloadCtx, job.tenantID, curGen, epoch, true)
	if wErr != nil {
		if offloadCtx.Err() != nil && ctx.Err() == nil {
			return false, uploaded, failed, true, nil
		}
		return false, uploaded, failed + 1, false, fmt.Errorf("drain WAL spool to R2: %w", wErr)
	}
	uploaded += walCnt

	if !cp.shouldEvictLocalOnSuspend() {
		return false, uploaded, failed, false, nil
	}

	// Phase 3 (brief lock): re-verify generation, lifecycle_seq, and cancellation before evicting local pgdata.
	unlock2 := cp.lockTenantLifecycle(job.tenantID)
	defer unlock2()

	if offloadCtx.Err() != nil {
		return false, uploaded, failed, true, nil
	}
	var postState, postGen string
	var postSeq int64
	pErr := cp.db.QueryRow(offloadCtx,
		`SELECT state, COALESCE(generation, ''), COALESCE(lifecycle_seq, 1) FROM clusters WHERE tenant_id = $1`,
		job.tenantID,
	).Scan(&postState, &postGen, &postSeq)
	if errors.Is(pErr, pgx.ErrNoRows) || postState != "SUSPENDED" || postGen != curGen || postSeq != curSeq {
		return false, uploaded, failed, true, nil
	}
	if pErr != nil {
		return false, uploaded, failed, false, pErr
	}

	// Safety guard: never evict local pgdata unless durable state is confirmed in R2/pageserver.
	if ps != nil {
		if !ps.HasMetadataSkeleton(offloadCtx, job.tenantID) {
			return false, uploaded, failed + 1, false, fmt.Errorf("refusing to evict %s: metadata skeleton not confirmed", job.tenantID)
		}
	} else {
		s3c, bucket := cp.backupStore()
		if !cp.r2Primary || s3c == nil || bucket == "" {
			return false, uploaded, failed, false, nil
		}
		if m, _, mErr := fencing.ReadManifestWithETagGen(offloadCtx, s3c, bucket, job.tenantID, curGen); mErr != nil || m == nil || len(m.Backups) == 0 {
			return false, uploaded, failed, false, nil
		}
	}

	if !cp.agent.HasLocalPGData(job.tenantID) {
		return false, uploaded, failed, false, nil
	}
	if eErr := cp.agent.EvictTenantLocalData(job.tenantID); eErr != nil {
		return false, uploaded, failed, false, fmt.Errorf("evict local pgdata: %w", eErr)
	}
	return true, uploaded, failed, false, nil
}
