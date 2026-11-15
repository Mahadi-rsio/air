package cell

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
)

// max_concurrent_starts = 5 protects 256 MB - 2 GB Cells from CPU/IOPS thrashing during wake storms.
var wakeStartSemaphore = make(chan struct{}, 5)

// GetTenantRuntime returns the cached TenantRuntime if present.
func (a *Agent) GetTenantRuntime(tenantID string) (*TenantRuntime, bool) {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()
	rt, ok := a.tenants[tenantID]
	return rt, ok
}

// WakeTenant acquires the max_concurrent_starts=5 semaphore and starts/adopts the tenant PG17 process.
func (a *Agent) WakeTenant(ctx context.Context, tenantID string, epoch int64) (*TenantRuntime, error) {
	select {
	case wakeStartSemaphore <- struct{}{}:
		defer func() { <-wakeStartSemaphore }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return a.StartOrAdoptTenant(tenantID, epoch)
}

// StopTenantClean stops a tenant PG17 cluster cleanly (smart with fast fallback) and verifies pg_controldata state.
func (a *Agent) StopTenantClean(tenantID string, mode string) (string, error) {
	if mode == "" {
		mode = "smart"
	}
	pgdata := filepath.Join(a.volDir, "tenants/active", tenantID, "pgdata")
	cmd := exec.Command("su", "-", "postgres", "-c",
		fmt.Sprintf("/usr/lib/postgresql/17/bin/pg_ctl -D %s -m %s -w -t 25 stop", pgdata, mode))
	out, err := cmd.CombinedOutput()
	if err != nil && mode == "smart" {
		cmdFast := exec.Command("su", "-", "postgres", "-c",
			fmt.Sprintf("/usr/lib/postgresql/17/bin/pg_ctl -D %s -m fast -w -t 15 stop", pgdata))
		if outFast, errFast := cmdFast.CombinedOutput(); errFast != nil {
			if state, sErr := a.ReadPgControlState(tenantID); sErr == nil && strings.Contains(state, "shut down") {
				a.tenantMu.Lock()
				if rt, ok := a.tenants[tenantID]; ok {
					rt.PID = 0
				}
				a.tenantMu.Unlock()
				return state, nil
			}
			return "", fmt.Errorf("pg_ctl stop smart (%v: %s) and fast (%v: %s) failed", err, string(out), errFast, string(outFast))
		}
	} else if err != nil {
		if state, sErr := a.ReadPgControlState(tenantID); sErr == nil && strings.Contains(state, "shut down") {
			a.tenantMu.Lock()
			if rt, ok := a.tenants[tenantID]; ok {
				rt.PID = 0
			}
			a.tenantMu.Unlock()
			return state, nil
		}
		return "", fmt.Errorf("pg_ctl stop -m %s failed: %v (%s)", mode, err, string(out))
	}

	a.tenantMu.Lock()
	if rt, ok := a.tenants[tenantID]; ok {
		rt.PID = 0
	}
	a.tenantMu.Unlock()

	return a.ReadPgControlState(tenantID)
}

// ReadPgControlState parses "Database cluster state" from pg_controldata.
func (a *Agent) ReadPgControlState(tenantID string) (string, error) {
	pgdata := filepath.Join(a.volDir, "tenants/active", tenantID, "pgdata")
	out, err := exec.Command("/usr/lib/postgresql/17/bin/pg_controldata", pgdata).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("pg_controldata failed: %v (%s)", err, string(out))
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "Database cluster state:") {
			val := strings.TrimSpace(strings.TrimPrefix(line, "Database cluster state:"))
			return val, nil
		}
	}
	return "", fmt.Errorf("Database cluster state not found in pg_controldata")
}

// DeepFreezeTenant executes VACUUM (FREEZE), takes an encrypted FULL backup to R2,
// stops PG17 cleanly, and removes local PGDATA to transition the tenant to DEEP_FROZEN.
func (a *Agent) DeepFreezeTenant(ctx context.Context, s3c *s3.Client, bucket, tenantID string) (*BackupArtifact, string, error) {
	rt, ok := a.GetTenantRuntime(tenantID)
	if !ok || rt.Port == 0 {
		return nil, "", fmt.Errorf("tenant %s runtime not found for deep freeze", tenantID)
	}

	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rt.Port)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, "", fmt.Errorf("connect for pre-freeze VACUUM: %w", err)
	}
	_, err = conn.Exec(ctx, "VACUUM (FREEZE)")
	conn.Close(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("pre-freeze VACUUM (FREEZE) failed: %w", err)
	}

	art, manifestPath, err := a.RunEncryptedBackup(ctx, s3c, bucket, tenantID, "FULL", "")
	if err != nil {
		return nil, "", fmt.Errorf("deep freeze R2 backup failed: %w", err)
	}

	state, err := a.StopTenantClean(tenantID, "fast")
	if err != nil {
		return nil, "", fmt.Errorf("deep freeze stop failed: %w", err)
	}
	if !strings.Contains(state, "shut down") {
		return nil, "", fmt.Errorf("unexpected pg_controldata state before freeze eviction: %q", state)
	}

	pgdata := filepath.Join(a.volDir, "tenants/active", tenantID, "pgdata")
	if err := os.RemoveAll(pgdata); err != nil {
		return nil, "", fmt.Errorf("remove local pgdata for deep freeze: %w", err)
	}
	return art, manifestPath, nil
}

// WakeDeepFrozenTenant hydrates PGDATA from R2, runs pg_verifybackup,
// starts PG17, and verifies SELECT 1.
func (a *Agent) WakeDeepFrozenTenant(ctx context.Context, s3c *s3.Client, bucket, tenantID string, art *BackupArtifact, epoch int64) (*TenantRuntime, error) {
	select {
	case wakeStartSemaphore <- struct{}{}:
		defer func() { <-wakeStartSemaphore }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	obj, err := s3c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(art.R2Key),
	})
	if err != nil {
		return nil, fmt.Errorf("deep freeze R2 GetObject %s: %w", art.R2Key, err)
	}
	cipherBytes, err := io.ReadAll(obj.Body)
	_ = obj.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("deep freeze read R2 body: %w", err)
	}

	plainBytes, err := a.kms.DecryptObject(tenantID, art.Epoch, art.R2Key, cipherBytes)
	if err != nil {
		return nil, fmt.Errorf("deep freeze decrypt: %w", err)
	}

	pgdata := filepath.Join(a.volDir, "tenants/active", tenantID, "pgdata")
	_ = os.RemoveAll(pgdata)
	if err := os.MkdirAll(pgdata, 0o700); err != nil {
		return nil, err
	}
	tarZst := filepath.Join(a.volDir, "tmp", fmt.Sprintf("wake_%s_%d.tar.zst", tenantID, time.Now().UnixNano()))
	if err := os.WriteFile(tarZst, plainBytes, 0o600); err != nil {
		return nil, err
	}
	defer os.Remove(tarZst)
	if out, err := exec.Command("tar", "--zstd", "-xf", tarZst, "-C", pgdata).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("deep freeze untar zstd failed: %w (%s)", err, string(out))
	}

	_ = os.Remove(filepath.Join(pgdata, "recovery.signal"))
	_ = os.Remove(filepath.Join(pgdata, "standby.signal"))
	_ = exec.Command("chown", "-R", "postgres:postgres", pgdata).Run()
	_ = exec.Command("chmod", "700", pgdata).Run()

	if out, err := runPostgresCmd("/usr/lib/postgresql/17/bin/pg_verifybackup", pgdata); err != nil {
		return nil, fmt.Errorf("deep freeze pg_verifybackup failed: %w (%s)", err, out)
	}

	rt, err := a.StartOrAdoptTenant(tenantID, epoch)
	if err != nil {
		return nil, fmt.Errorf("start hydrated tenant failed: %w", err)
	}

	var one int
	for i := 0; i < 10; i++ {
		dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rt.Port)
		c, cErr := pgx.Connect(ctx, dsn)
		if cErr == nil {
			qErr := c.QueryRow(ctx, "SELECT 1").Scan(&one)
			c.Close(ctx)
			if qErr == nil && one == 1 {
				return rt, nil
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	return rt, nil
}
