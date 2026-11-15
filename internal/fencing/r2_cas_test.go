package fencing

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type mockObject struct {
	data        []byte
	etag        string
	contentType string
}

type mockR2Server struct {
	mu      sync.Mutex
	objects map[string]mockObject
}

func newMockR2Server() (*httptest.Server, *s3.Client) {
	m := &mockR2Server{objects: make(map[string]mockObject)}
	srv := httptest.NewServer(http.HandlerFunc(m.handle))
	cfg := aws.Config{
		Region:      "auto",
		Credentials: credentials.NewStaticCredentialsProvider("test-ak", "test-sk", ""),
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
	})
	return srv, client
}

func (m *mockR2Server) handle(w http.ResponseWriter, r *http.Request) {
	// Path style: /<bucket>/<key...> or /<bucket>?list-type=2
	trimmed := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	bucket := parts[0]
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}
	fullKey := bucket + "/" + key

	m.mu.Lock()
	defer m.mu.Unlock()

	if r.Method == http.MethodGet && key == "" && r.URL.Query().Get("list-type") == "2" {
		prefix := bucket + "/" + r.URL.Query().Get("prefix")
		type contents struct {
			Key  string `xml:"Key"`
			Size int    `xml:"Size"`
			ETag string `xml:"ETag"`
		}
		type listResp struct {
			XMLName  xml.Name   `xml:"ListBucketResult"`
			Name     string     `xml:"Name"`
			Contents []contents `xml:"Contents"`
		}
		resp := listResp{Name: bucket}
		var keys []string
		for k := range m.objects {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			obj := m.objects[k]
			resp.Contents = append(resp.Contents, contents{
				Key:  strings.TrimPrefix(k, bucket+"/"),
				Size: len(obj.data),
				ETag: obj.etag,
			})
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(resp)
		return
	}

	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		existing, exists := m.objects[fullKey]
		if inm := r.Header.Get("If-None-Match"); inm == "*" && exists {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
			return
		}
		if im := r.Header.Get("If-Match"); im != "" {
			if !exists || strings.Trim(im, `"`) != strings.Trim(existing.etag, `"`) {
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
				return
			}
		}
		sum := md5.Sum(body)
		etag := `"` + hex.EncodeToString(sum[:]) + `"`
		m.objects[fullKey] = mockObject{
			data:        body,
			etag:        etag,
			contentType: r.Header.Get("Content-Type"),
		}
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		obj, exists := m.objects[fullKey]
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
			return
		}
		w.Header().Set("ETag", obj.etag)
		if obj.contentType != "" {
			w.Header().Set("Content-Type", obj.contentType)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(obj.data)

	case http.MethodHead:
		obj, exists := m.objects[fullKey]
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", obj.etag)
		w.Header().Set("Content-Length", itoa(len(obj.data)))
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		delete(m.objects, fullKey)
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestPutManifestCAS_EpochAndETagFencing(t *testing.T) {
	srv, s3c := newMockR2Server()
	defer srv.Close()

	ctx := context.Background()
	bucket := "air-test"
	tenantID := "tenant_cas"

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	dek, err := kms.ExportWrappedDEKBundle(tenantID)
	if err != nil {
		t.Fatal(err)
	}

	m1 := &TenantFenceManifest{
		TenantID:     tenantID,
		FencingEpoch: 1,
		HolderCell:   "cell-1",
		DEKBundle:    dek,
	}
	etag1, err := PutManifestCAS(ctx, s3c, bucket, m1, "")
	if err != nil {
		t.Fatalf("initial PutManifestCAS failed: %v", err)
	}
	if etag1 == "" {
		t.Fatal("expected non-empty ETag")
	}

	// Duplicate If-None-Match: "*" must fail with ErrCASPrecondition
	if _, err := PutManifestCAS(ctx, s3c, bucket, m1, ""); !errors.Is(err, ErrCASPrecondition) {
		t.Fatalf("expected ErrCASPrecondition on duplicate create, got: %v", err)
	}

	// Promote to Epoch 2 using etag1
	m2 := &TenantFenceManifest{
		TenantID:     tenantID,
		FencingEpoch: 2,
		HolderCell:   "cell-2",
		DEKBundle:    dek,
	}
	etag2, err := PutManifestCAS(ctx, s3c, bucket, m2, etag1)
	if err != nil {
		t.Fatalf("promote PutManifestCAS epoch 2 failed: %v", err)
	}
	if etag2 == etag1 {
		t.Fatalf("expected new ETag on epoch 2 update, got same %s", etag2)
	}

	// Stale epoch 1 writer holding etag1 must fail with ErrStaleFencingEpoch
	if _, err := PutManifestCAS(ctx, s3c, bucket, m1, etag1); !errors.Is(err, ErrStaleFencingEpoch) {
		t.Fatalf("expected ErrStaleFencingEpoch for stale epoch 1, got: %v", err)
	}

	// Concurrent epoch 2 writer holding stale etag1 must fail with ErrCASPrecondition (HTTP 412)
	m2Concurrent := &TenantFenceManifest{
		TenantID:     tenantID,
		FencingEpoch: 2,
		HolderCell:   "cell-3",
		DEKBundle:    dek,
	}
	if _, err := PutManifestCAS(ctx, s3c, bucket, m2Concurrent, etag1); !errors.Is(err, ErrCASPrecondition) {
		t.Fatalf("expected ErrCASPrecondition for stale etag1, got: %v", err)
	}

	// Verify Epoch 2 manifest is intact
	got, gotETag, err := ReadManifestWithETag(ctx, s3c, bucket, tenantID)
	if err != nil {
		t.Fatalf("ReadManifestWithETag: %v", err)
	}
	if got.FencingEpoch != 2 || got.HolderCell != "cell-2" || gotETag != etag2 {
		t.Fatalf("unexpected manifest state: %+v etag=%s", got, gotETag)
	}
}
