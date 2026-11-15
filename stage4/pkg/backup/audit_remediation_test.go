package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"air-stage4/pkg/cell"
	"air-stage4/pkg/crypto"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func newMockR2Server(t *testing.T) (*httptest.Server, *s3.Client) {
	t.Helper()
	var mu sync.Mutex
	objs := make(map[string][]byte)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			objs[key] = b
			w.Header().Set("ETag", "\"mock-etag-1\"")
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			b, ok := objs[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(b)))
			w.Header().Set("ETag", "\"mock-etag-1\"")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			b, ok := objs[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(b)))
			_, _ = io.Copy(w, bytes.NewReader(b))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("auto"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("ak", "sk", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	cli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
	})
	return srv, cli
}

func TestRemediation_DrainWAL_FailClosedAndFaultInjection(t *testing.T) {
	if _, err := os.Stat(filepath.Join(pgBin, "initdb")); err != nil {
		t.Skip("PG17 binary not installed on host")
	}

	volDir, err := os.MkdirTemp("/tmp", "air_wal_rem_")
	if err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(volDir, 0o755)
	defer os.RemoveAll(volDir)

	kms, _ := crypto.NewEnvelopeKMS()
	a, err := cell.NewAgent(volDir, 0, kms)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	tenantID := "t_wal_rem"
	_, _ = kms.EnsureTenantDEK(tenantID)
	if _, err := a.ClaimCluster(tenantID); err != nil {
		t.Fatal(err)
	}
	rt, err := a.ActivateCluster(tenantID, "pw_wal", 1)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Fail closed if R2 client is nil
	if _, err := DrainAndEncryptWALSpoolToR2(context.Background(), a, nil, "bucket", tenantID); !errors.Is(err, ErrR2ClientRequired) {
		t.Fatalf("expected ErrR2ClientRequired, got %v", err)
	}

	srv, s3c := newMockR2Server(t)
	defer srv.Close()

	// 2. Crash after R2 PutObject+HeadObject but BEFORE manifest commit -> local WAL MUST survive!
	_, err = DrainAndEncryptWALSpoolToR2WithFault(context.Background(), a, s3c, "test-bucket", tenantID, func(stage, seg string) error {
		if stage == "after_upload_before_manifest" {
			return fmt.Errorf("simulated crash before manifest commit")
		}
		return nil
	})
	if err == nil {
		t.Fatalf("expected injected crash error")
	}
	entries, _ := os.ReadDir(rt.WALSpoolDir)
	if len(entries) == 0 {
		t.Fatalf("local WAL deleted despite crash before manifest commit!")
	}

	// 3. Crash after manifest commit but BEFORE local deletion -> manifest persisted, local WAL still exists, retry finishes safely!
	_, err = DrainAndEncryptWALSpoolToR2WithFault(context.Background(), a, s3c, "test-bucket", tenantID, func(stage, seg string) error {
		if stage == "after_manifest_before_delete" {
			return fmt.Errorf("simulated crash before local delete")
		}
		return nil
	})
	if err == nil {
		t.Fatalf("expected injected crash error")
	}
	manifestPath := filepath.Join(rt.WALSpoolDir, "wal_upload_manifest.json")
	m, mErr := loadWALManifest(manifestPath, tenantID)
	if mErr != nil || len(m.Segments) == 0 {
		t.Fatalf("expected durable manifest committed, got err=%v len=%d", mErr, len(m.Segments))
	}

	// 4. Retry cleanly -> verifies HeadObject, deletes local WAL, marks LOCAL_DELETED
	uploaded, err := DrainAndEncryptWALSpoolToR2(context.Background(), a, s3c, "test-bucket", tenantID)
	if err != nil || len(uploaded) == 0 {
		t.Fatalf("retry failed: err=%v uploaded=%d", err, len(uploaded))
	}
	m2, _ := loadWALManifest(manifestPath, tenantID)
	for name, seg := range m2.Segments {
		if seg.Status != "LOCAL_DELETED" {
			t.Fatalf("segment %s expected LOCAL_DELETED, got %s", name, seg.Status)
		}
	}
}

func TestRemediation_WALDurabilityUnit_FailClosedAndFaultInjection(t *testing.T) {
	ctx := context.Background()
	kms, _ := crypto.NewEnvelopeKMS()
	tenantID := "t_wal_unit"
	_, _ = kms.EnsureTenantDEK(tenantID)
	spoolDir := t.TempDir()

	// 1. Fail closed if R2 client is nil
	if _, err := drainWALSpoolDirWithFault(ctx, kms, nil, "bucket", tenantID, 1, spoolDir, nil); !errors.Is(err, ErrR2ClientRequired) {
		t.Fatalf("expected ErrR2ClientRequired, got %v", err)
	}

	// 2. Reject invalid WAL segment filename
	badSeg := filepath.Join(spoolDir, "bad_wal_name.txt")
	_ = os.WriteFile(badSeg, []byte("bad"), 0o600)
	srv, s3c := newMockR2Server(t)
	defer srv.Close()
	if _, err := drainWALSpoolDirWithFault(ctx, kms, s3c, "test-bucket", tenantID, 1, spoolDir, nil); !errors.Is(err, ErrInvalidWALName) {
		t.Fatalf("expected ErrInvalidWALName, got %v", err)
	}
	_ = os.Remove(badSeg)

	// Populate valid WAL segments + timeline history in spoolDir
	seg1 := "000000010000000000000001"
	seg2 := "000000010000000000000002"
	hist := "00000001.history"
	_ = os.WriteFile(filepath.Join(spoolDir, seg1), []byte("wal-segment-1-payload"), 0o600)
	_ = os.WriteFile(filepath.Join(spoolDir, seg2), []byte("wal-segment-2-payload"), 0o600)
	_ = os.WriteFile(filepath.Join(spoolDir, hist), []byte("1\t0/1000000\tno recovery target\n"), 0o600)

	// 3. Crash after R2 PutObject+HeadObject but BEFORE manifest commit -> local WAL MUST survive!
	_, err := drainWALSpoolDirWithFault(ctx, kms, s3c, "test-bucket", tenantID, 1, spoolDir, func(stage, seg string) error {
		if stage == "after_upload_before_manifest" {
			return fmt.Errorf("injected crash before manifest")
		}
		return nil
	})
	if err == nil {
		t.Fatalf("expected injected crash error")
	}
	if _, statErr := os.Stat(filepath.Join(spoolDir, seg1)); statErr != nil {
		t.Fatalf("local WAL deleted despite crash before manifest commit!")
	}

	// 4. Crash after manifest commit but BEFORE local deletion -> manifest committed, local WAL survives!
	_, err = drainWALSpoolDirWithFault(ctx, kms, s3c, "test-bucket", tenantID, 1, spoolDir, func(stage, seg string) error {
		if stage == "after_manifest_before_delete" && seg == seg1 {
			return fmt.Errorf("injected crash before local delete")
		}
		return nil
	})
	if err == nil {
		t.Fatalf("expected injected crash error")
	}
	manifestPath := filepath.Join(spoolDir, "wal_upload_manifest.json")
	m, err := loadWALManifest(manifestPath, tenantID)
	if err != nil || m.Segments[seg1] == nil || m.Segments[seg1].Status != "VERIFIED_IN_R2" {
		t.Fatalf("expected %s VERIFIED_IN_R2 in manifest, got err=%v seg=%+v", seg1, err, m.Segments[seg1])
	}
	if _, statErr := os.Stat(filepath.Join(spoolDir, seg1)); statErr != nil {
		t.Fatalf("local WAL deleted despite crash before local delete!")
	}

	// 5. Retry cleanly -> finishes remaining segments (seg1 + seg2) & deletes local WAL only after manifest commit
	uploaded, err := drainWALSpoolDirWithFault(ctx, kms, s3c, "test-bucket", tenantID, 1, spoolDir, nil)
	if err != nil || len(uploaded) != 2 {
		t.Fatalf("retry failed: err=%v uploaded=%d", err, len(uploaded))
	}
	m2, _ := loadWALManifest(manifestPath, tenantID)
	for _, name := range []string{seg1, seg2, hist} {
		if m2.Segments[name] == nil || m2.Segments[name].Status != "LOCAL_DELETED" {
			t.Fatalf("segment %s expected LOCAL_DELETED, got %+v", name, m2.Segments[name])
		}
		if _, statErr := os.Stat(filepath.Join(spoolDir, name)); !os.IsNotExist(statErr) {
			t.Fatalf("local WAL %s still exists after LOCAL_DELETED", name)
		}
	}
}

func TestRemediation_BackupValidationAndChainGuards(t *testing.T) {
	ctx := context.Background()
	volDir := t.TempDir()
	kms, _ := crypto.NewEnvelopeKMS()
	ag, err := cell.NewAgent(volDir, 0, kms)
	if err != nil {
		t.Fatal(err)
	}
	defer ag.Close()

	// RunEncryptedBackupStep must fail closed on nil R2 client
	if _, _, err := RunEncryptedBackupStep(ctx, ag, nil, "bucket", "t_1", "b_1", "", "FULL", ""); !errors.Is(err, ErrR2ClientRequired) {
		t.Fatalf("RunEncryptedBackupStep expected ErrR2ClientRequired, got %v", err)
	}

	srv, s3c := newMockR2Server(t)
	defer srv.Close()

	// Invalid tenantID / bkpID must be rejected
	if _, _, err := RunEncryptedBackupStep(ctx, ag, s3c, "bucket", "../escape", "b_1", "", "FULL", ""); err == nil {
		t.Fatalf("RunEncryptedBackupStep accepted invalid tenantID")
	}
	if _, _, err := RunEncryptedBackupStep(ctx, ag, s3c, "bucket", "t_1", "../escape", "", "FULL", ""); err == nil {
		t.Fatalf("RunEncryptedBackupStep accepted invalid bkpID")
	}

	// CombineAndReplayPITR must reject broken chain / epoch mismatch / invalid WAL filename
	brokenChain := []*cell.BackupArtifact{
		{BackupID: "b0", Type: "FULL", Epoch: 1},
		{BackupID: "i1", ParentID: "b0", Type: "INCR", Epoch: 2},
	}
	if _, _, _, err := CombineAndReplayPITR(ctx, ag, s3c, "bucket", "t_1", brokenChain, nil, "", ""); err == nil {
		t.Fatalf("CombineAndReplayPITR accepted epoch mismatch in backup chain")
	}
}
