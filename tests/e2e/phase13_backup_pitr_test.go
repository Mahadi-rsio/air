package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/backup"
	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/control"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/jackc/pgx/v5"
)

func TestPhase13_EndToEnd_BackupManifest_Verification_PITR_And_RestoreToNewTenant(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/initdb"); err != nil {
		t.Fatalf("PostgreSQL 17 is required: %v", err)
	}

	ctx := context.Background()
	rootDir, err := os.MkdirTemp("/tmp", "air_phase13_e2e_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	srv, s3c, mock := startMockS3(t)
	defer srv.Close()
	bucket := "air-p13-backups"

	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(rootDir, "master.key"))
	if err != nil {
		t.Fatal(err)
	}
	ag, err := cell.NewAgent(rootDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, tid := range []string{"t_p13_src", "t_p13_restored_lsn", "t_p13_restored_time", "t_p13_fault"} {
			_ = ag.StopAndPurgeTenant(tid)
		}
		ag.Close()
	}()

	cp, err := control.NewControlPlane(ctx, filepath.Join(rootDir, "meta"), "/usr/lib/postgresql/17/bin", freeTCPPort(t), ag, kms)
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	defer cp.Close()
	cp.SetBackupStore(s3c, bucket)

	schemaVer, err := cp.SchemaVersion(ctx)
	if err != nil || schemaVer < 5 {
		t.Fatalf("expected schema version >= 5, got %d (err=%v)", schemaVer, err)
	}

	keyA, err := cp.CreateAPIKey(ctx, "acct_p13", "p13 key")
	if err != nil {
		t.Fatal(err)
	}
	httpSrv := httptest.NewServer(cp.HTTPHandler())
	defer httpSrv.Close()

	// 1. Provision source tenant t_p13_src
	srcProv := httpCreateDatabase(t, httpSrv.URL, keyA.Token, "idem-p13-src", "t_p13_src")
	if srcProv.Generation == "" {
		t.Fatalf("expected non-empty generation on provisioned tenant: %+v", srcProv)
	}
	srcDSN := fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", srcProv.Username, srcProv.Password, srcProv.DirectPort, srcProv.Database)
	srcConn, err := pgx.Connect(ctx, srcDSN)
	if err != nil {
		t.Fatalf("connect source tenant: %v", err)
	}
	defer srcConn.Close(ctx)

	if _, err := srcConn.Exec(ctx, `
		CREATE TABLE ledger (id INT PRIMARY KEY, amount INT NOT NULL, note TEXT NOT NULL);
		INSERT INTO ledger SELECT g, g * 10, 'batch_full' FROM generate_series(1, 100) g;
	`); err != nil {
		t.Fatalf("seed batch_full: %v", err)
	}

	// 2. Trigger FULL backup via HTTP -> verify immutable BackupManifest is published under generation prefix
	fullReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/databases/t_p13_src/backups", strings.NewReader(`{"type":"FULL"}`))
	fullReq.Header.Set("Authorization", "Bearer "+keyA.Token)
	fullResp, err := http.DefaultClient.Do(fullReq)
	if err != nil || fullResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(fullResp.Body)
		t.Fatalf("FULL backup failed: status=%d err=%v body=%s", fullResp.StatusCode, err, string(body))
	}
	var fullOut control.BackupResponse
	_ = json.NewDecoder(fullResp.Body).Decode(&fullOut)
	_ = fullResp.Body.Close()

	wantManifestKey := backup.BackupManifestKey("t_p13_src", srcProv.Generation, fullOut.BackupID)
	if fullOut.ManifestKey != wantManifestKey || !mock.hasKey(bucket, wantManifestKey) {
		t.Fatalf("expected immutable manifest at %s, got %q (exists=%v)", wantManifestKey, fullOut.ManifestKey, mock.hasKey(bucket, wantManifestKey))
	}

	// 3. List backups via GET /v1/databases/t_p13_src/backups and verify via POST .../verify
	listReq, _ := http.NewRequest(http.MethodGet, httpSrv.URL+"/v1/databases/t_p13_src/backups", nil)
	listReq.Header.Set("Authorization", "Bearer "+keyA.Token)
	listResp, err := http.DefaultClient.Do(listReq)
	if err != nil || listResp.StatusCode != http.StatusOK {
		t.Fatalf("GET backups failed: status=%d err=%v", listResp.StatusCode, err)
	}
	var listBody struct {
		Backups []*backup.BackupManifest `json:"backups"`
	}
	_ = json.NewDecoder(listResp.Body).Decode(&listBody)
	_ = listResp.Body.Close()
	if len(listBody.Backups) != 1 || listBody.Backups[0].BackupID != fullOut.BackupID {
		t.Fatalf("unexpected listed backups: %+v", listBody.Backups)
	}

	verReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/databases/t_p13_src/backups/"+fullOut.BackupID+"/verify", strings.NewReader(`{"stream_hash":true}`))
	verReq.Header.Set("Authorization", "Bearer "+keyA.Token)
	verResp, err := http.DefaultClient.Do(verReq)
	if err != nil || verResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(verResp.Body)
		t.Fatalf("verify backup failed: status=%d err=%v body=%s", verResp.StatusCode, err, string(body))
	}
	var verReport backup.VerificationReport
	_ = json.NewDecoder(verResp.Body).Decode(&verReport)
	_ = verResp.Body.Close()
	if !verReport.Valid || !verReport.ManifestOK || !verReport.ObjectsOK || !verReport.WALContinuityOK {
		t.Fatalf("expected valid verification report, got %+v", verReport)
	}

	// 4. Insert 50 more rows (150 total), capture targetLSN and targetTime, then corrupt/delete rows on source
	if _, err := srcConn.Exec(ctx, `INSERT INTO ledger SELECT g, g * 10, 'batch_pitr' FROM generate_series(101, 150) g;`); err != nil {
		t.Fatalf("insert batch_pitr: %v", err)
	}
	var pitrLSN, pitrTime string
	if err := srcConn.QueryRow(ctx, `SELECT pg_current_wal_insert_lsn()::text, clock_timestamp()::text`).Scan(&pitrLSN, &pitrTime); err != nil {
		t.Fatalf("capture LSN/time: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	// Accidental destructive DELETE on source tenant after pitrLSN
	if _, err := srcConn.Exec(ctx, `DELETE FROM ledger WHERE id > 10;`); err != nil {
		t.Fatalf("destructive delete: %v", err)
	}

	// Drain WAL spool so the WAL covering both batch_pitr and the accidental DELETE is in R2
	walReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/databases/t_p13_src/backups", strings.NewReader(`{"type":"WAL"}`))
	walReq.Header.Set("Authorization", "Bearer "+keyA.Token)
	walResp, err := http.DefaultClient.Do(walReq)
	if err != nil || walResp.StatusCode != http.StatusCreated {
		t.Fatalf("WAL backup failed: status=%d err=%v", walResp.StatusCode, err)
	}
	_ = walResp.Body.Close()

	// 5. Restore to a NEW isolated tenant (t_p13_restored_lsn) at pitrLSN
	restoreBody := fmt.Sprintf(`{"target_lsn":%q,"new_tenant_id":"t_p13_restored_lsn","verify_query":"SELECT count(*) FROM ledger;"}`, pitrLSN)
	resReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/databases/t_p13_src/restore", strings.NewReader(restoreBody))
	resReq.Header.Set("Authorization", "Bearer "+keyA.Token)
	resResp, err := http.DefaultClient.Do(resReq)
	if err != nil || resResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resResp.Body)
		t.Fatalf("restore to new tenant failed: status=%d err=%v body=%s", resResp.StatusCode, err, string(body))
	}
	var resOut control.RestoreResponse
	_ = json.NewDecoder(resResp.Body).Decode(&resOut)
	_ = resResp.Body.Close()

	if resOut.RestoredTenantID != "t_p13_restored_lsn" || resOut.RestoredGeneration == "" || resOut.RestoredGeneration == srcProv.Generation {
		t.Fatalf("expected distinct generation for restored tenant: %+v (srcGen=%s)", resOut, srcProv.Generation)
	}
	if resOut.VerifyQueryOutput != "150" || !resOut.AmcheckOK {
		t.Fatalf("expected 150 rows recovered at pitrLSN, got %+v", resOut)
	}

	// Verify restored tenant is ACTIVE, independently queryable, and source tenant still has only 10 rows
	_, rPort, _, rPass, err := cp.GetTenantRouteAndCredentials(ctx, "t_p13_restored_lsn")
	if err != nil {
		t.Fatalf("GetTenantRouteAndCredentials restored: %v", err)
	}
	restoredDSN := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/t_p13_restored_lsn?sslmode=disable", rPass, rPort)
	rConn, err := pgx.Connect(ctx, restoredDSN)
	if err != nil {
		t.Fatalf("connect restored tenant: %v", err)
	}
	var restoredCount, srcCount int
	if err := rConn.QueryRow(ctx, `SELECT count(*) FROM ledger`).Scan(&restoredCount); err != nil || restoredCount != 150 {
		t.Fatalf("expected 150 rows in live restored tenant, got %d (err=%v)", restoredCount, err)
	}
	_ = rConn.Close(ctx)

	if err := srcConn.QueryRow(ctx, `SELECT count(*) FROM ledger`).Scan(&srcCount); err != nil || srcCount != 10 {
		t.Fatalf("expected source tenant to remain untouched with 10 rows, got %d (err=%v)", srcCount, err)
	}

	// 6. Fault injection during restore into t_p13_fault -> must leave no half-restored ACTIVE tenant
	cp.SetFaultHook(func(stage string) error {
		if stage == "restore_after_start_before_commit" {
			return errors.New("simulated crash before restore metadata commit")
		}
		return nil
	})
	_, faultErr := cp.RestoreDatabaseWithOptions(ctx, "acct_p13", "t_p13_src", control.RestoreOptions{
		TargetLSN:   pitrLSN,
		NewTenantID: "t_p13_fault",
		VerifyQuery: "SELECT count(*) FROM ledger;",
	})
	cp.SetFaultHook(nil)
	if faultErr == nil {
		t.Fatalf("expected simulated fault error during restore")
	}
	if _, err := cp.GetDatabase(ctx, "acct_p13", "t_p13_fault"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("expected t_p13_fault to not exist after failed restore, got err=%v", err)
	}
	if _, ok := ag.GetTenant("t_p13_fault"); ok {
		t.Fatalf("expected t_p13_fault runtime to be stopped/purged after failed restore")
	}
}
