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
