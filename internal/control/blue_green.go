package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrBlueGreenEpochConflict = errors.New("control: blue/green swap epoch conflict")
	ErrBlueGreenNotFound      = errors.New("control: blue/green state not found")
)

// GatewaySwapper abstracts the atomic gateway database-name -> tenant-ID swap and rollback.
type GatewaySwapper interface {
	SwapDatabaseTenant(dbName, newTenantID string, expectedEpoch int64, retention time.Duration) error
	RollbackDatabaseTenant(dbName string, expectedEpoch int64) error
}

// BlueGreenRestoreRequest configures a zero-downtime blue/green restore for an existing database name.
type BlueGreenRestoreRequest struct {
	AccountID       string        `json:"account_id"`
	DatabaseName    string        `json:"database_name"`
	SourceTenantID  string        `json:"source_tenant_id"`
	NewTenantID     string        `json:"new_tenant_id"`
	BackupID        string        `json:"backup_id,omitempty"`
	TargetLSN       string        `json:"target_lsn,omitempty"`
	TargetTime      string        `json:"target_time,omitempty"`
	VerifyQuery     string        `json:"verify_query,omitempty"`
	ExpectedEpoch   int64         `json:"expected_epoch"`
	RetentionWindow time.Duration `json:"retention_window,omitempty"`
}

// BlueGreenRestoreResult describes the completed blue/green swap.
type BlueGreenRestoreResult struct {
	DatabaseName     string    `json:"database_name"`
	ActiveTenantID   string    `json:"active_tenant_id"`
	PreviousTenantID string    `json:"previous_tenant_id"`
	Epoch            int64     `json:"epoch"`
	AmcheckOK        bool      `json:"amcheck_ok"`
	RetentionUntil   time.Time `json:"retention_until"`
}

// BlueGreenCoordinator tracks per-database swap epochs and retention windows with CAS fencing.
type BlueGreenCoordinator struct {
	mu    sync.Mutex
	state map[string]*BlueGreenRestoreResult
}

func NewBlueGreenCoordinator() *BlueGreenCoordinator {
	return &BlueGreenCoordinator{
		state: make(map[string]*BlueGreenRestoreResult),
	}
}

// BlueGreenRestore restores into req.NewTenantID, verifies integrity (pg_verifybackup / pg_amcheck),
// and atomically swaps req.DatabaseName -> req.NewTenantID protected by epoch CAS fencing.
func (cp *ControlPlane) BlueGreenRestore(
	ctx context.Context,
	coord *BlueGreenCoordinator,
	swapper GatewaySwapper,
	req BlueGreenRestoreRequest,
) (*BlueGreenRestoreResult, error) {
	if req.DatabaseName == "" || req.SourceTenantID == "" || req.NewTenantID == "" {
		return nil, fmt.Errorf("control: database_name, source_tenant_id, and new_tenant_id are required")
	}
	if req.RetentionWindow <= 0 {
		req.RetentionWindow = time.Hour
	}

	// 1. Restore into isolated new_tenant_id (includes StagePITRRestore + pg_amcheck / verify query).
	rResp, err := cp.RestoreDatabaseWithOptions(ctx, req.AccountID, req.SourceTenantID, RestoreOptions{
		BackupID:    req.BackupID,
		TargetLSN:   req.TargetLSN,
		TargetTime:  req.TargetTime,
		NewTenantID: req.NewTenantID,
		VerifyQuery: req.VerifyQuery,
	})
	if err != nil {
		return nil, err
	}
	if !rResp.AmcheckOK {
		return nil, fmt.Errorf("control: blue/green restore aborted: pg_amcheck validation did not pass")
	}

	// 2. Epoch CAS swap (two concurrent swaps with the same ExpectedEpoch cannot both win).
	coord.mu.Lock()
	defer coord.mu.Unlock()
	cur := coord.state[req.DatabaseName]
	var curEpoch int64
	prevTenant := req.SourceTenantID
	if cur != nil {
		curEpoch = cur.Epoch
		prevTenant = cur.ActiveTenantID
	}
	if curEpoch != req.ExpectedEpoch {
		return nil, fmt.Errorf("%w: db=%s have_epoch=%d want_epoch=%d", ErrBlueGreenEpochConflict, req.DatabaseName, curEpoch, req.ExpectedEpoch)
	}
	if swapper != nil {
		if err := swapper.SwapDatabaseTenant(req.DatabaseName, req.NewTenantID, req.ExpectedEpoch, req.RetentionWindow); err != nil {
			return nil, err
		}
	}
	res := &BlueGreenRestoreResult{
		DatabaseName:     req.DatabaseName,
		ActiveTenantID:   req.NewTenantID,
		PreviousTenantID: prevTenant,
		Epoch:            curEpoch + 1,
		AmcheckOK:        rResp.AmcheckOK,
		RetentionUntil:   time.Now().UTC().Add(req.RetentionWindow),
	}
	coord.state[req.DatabaseName] = res
	return res, nil
}
