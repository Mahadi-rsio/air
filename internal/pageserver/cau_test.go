package pageserver

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type mockS3Object struct {
	data           []byte
	etag           string
	checksumSHA256 string
	metadata       map[string]string
}

type mockMultipart struct {
	key      string
	metadata map[string]string
	parts    map[int][]byte
}

type mockR2Server struct {
	mu         sync.Mutex
	objects    map[string]*mockS3Object
	multiparts map[string]*mockMultipart
	nextUpload atomic.Int64

	headCount       atomic.Int64
	putCount        atomic.Int64
	getCount        atomic.Int64
	bytesUploaded   atomic.Int64
	bytesDownloaded atomic.Int64
	abortCount      atomic.Int64

	// Fault injection hooks
	failPut        atomic.Bool
	failUploadPart atomic.Bool
	failManifest   atomic.Bool
	failGet        atomic.Bool
	stripChecksum  atomic.Bool
}

func newMockR2Server(t *testing.T) (*httptest.Server, *s3.Client, *mockR2Server) {
	t.Helper()
	m := &mockR2Server{
		objects:    make(map[string]*mockS3Object),
		multiparts: make(map[string]*mockMultipart),
	}
	srv := httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(srv.Close)

	s3c := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("test-ak", "test-sk", ""),
	})
	return srv, s3c, m
}

func (m *mockR2Server) handle(w http.ResponseWriter, r *http.Request) {
	// Path style: /<bucket>/<key...>
	trimmed := strings.TrimPrefix(r.URL.Path, "/")
	slash := strings.IndexByte(trimmed, '/')
	var key string
	if slash >= 0 {
		key = trimmed[slash+1:]
	}
	q := r.URL.Query()

	switch r.Method {
	case http.MethodHead:
		m.headCount.Add(1)
		m.mu.Lock()
		obj, ok := m.objects[key]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		for mk, mv := range obj.metadata {
			w.Header().Set("X-Amz-Meta-"+mk, mv)
		}
		if !m.stripChecksum.Load() && obj.checksumSHA256 != "" {
			w.Header().Set("X-Amz-Checksum-Sha256", obj.checksumSHA256)
		}
		w.Header().Set("ETag", obj.etag)
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		if q.Get("list-type") == "2" {
			prefix := q.Get("prefix")
			type s3Obj struct {
				Key  string `xml:"Key"`
				Size int    `xml:"Size"`
				ETag string `xml:"ETag"`
			}
			type listResp struct {
				XMLName     xml.Name `xml:"ListBucketResult"`
				IsTruncated bool     `xml:"IsTruncated"`
				Contents    []s3Obj  `xml:"Contents"`
			}
			m.mu.Lock()
			var out listResp
			keys := make([]string, 0, len(m.objects))
			for k := range m.objects {
				if strings.HasPrefix(k, prefix) {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			for _, k := range keys {
				o := m.objects[k]
				out.Contents = append(out.Contents, s3Obj{Key: k, Size: len(o.data), ETag: o.etag})
			}
			m.mu.Unlock()
			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(out)
			return
		}

		m.getCount.Add(1)
		if m.failGet.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		m.mu.Lock()
		obj, ok := m.objects[key]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
			spec := strings.TrimPrefix(rng, "bytes=")
			parts := strings.SplitN(spec, "-", 2)
			start, _ := strconv.Atoi(parts[0])
			end, _ := strconv.Atoi(parts[1])
			if end >= len(obj.data) {
				end = len(obj.data) - 1
			}
			if start < 0 || start > end {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			slice := obj.data[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.data)))
			w.Header().Set("Content-Length", strconv.Itoa(len(slice)))
			w.WriteHeader(http.StatusPartialContent)
			m.bytesDownloaded.Add(int64(len(slice)))
			_, _ = w.Write(slice)
			return
		}
		w.Header().Set("ETag", obj.etag)
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
		w.WriteHeader(http.StatusOK)
		m.bytesDownloaded.Add(int64(len(obj.data)))
		_, _ = w.Write(obj.data)

	case http.MethodPost:
		if q.Has("uploads") {
			uid := fmt.Sprintf("up-%d", m.nextUpload.Add(1))
			meta := extractAmzMeta(r.Header)
			m.mu.Lock()
			m.multiparts[uid] = &mockMultipart{key: key, metadata: meta, parts: make(map[int][]byte)}
			m.mu.Unlock()
			type initResp struct {
				XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
				UploadId string   `xml:"UploadId"`
			}
			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(initResp{UploadId: uid})
			return
		}
		if uid := q.Get("uploadId"); uid != "" {
			if inm := r.Header.Get("If-None-Match"); inm == "*" {
				m.mu.Lock()
				_, exists := m.objects[key]
				m.mu.Unlock()
				if exists {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}
			m.mu.Lock()
			mp, ok := m.multiparts[uid]
			if !ok {
				m.mu.Unlock()
				w.WriteHeader(http.StatusNotFound)
				return
			}
			partNums := make([]int, 0, len(mp.parts))
			for pn := range mp.parts {
				partNums = append(partNums, pn)
			}
			sort.Ints(partNums)
			var combined bytes.Buffer
			for _, pn := range partNums {
				combined.Write(mp.parts[pn])
			}
			delete(m.multiparts, uid)
			data := combined.Bytes()
			sum := md5.Sum(data)
			etag := fmt.Sprintf(`"%s-%d"`, hex.EncodeToString(sum[:]), len(partNums))
			sha := sha256.Sum256(data)
			m.objects[key] = &mockS3Object{
				data:           data,
				etag:           etag,
				checksumSHA256: base64.StdEncoding.EncodeToString(sha[:]),
				metadata:       mp.metadata,
			}
			m.mu.Unlock()
			type compResp struct {
				XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
				ETag    string   `xml:"ETag"`
			}
			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(compResp{ETag: etag})
			return
		}

	case http.MethodPut:
		if uid := q.Get("uploadId"); uid != "" {
			if m.failUploadPart.Load() {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			pn, _ := strconv.Atoi(q.Get("partNumber"))
			body, _ := io.ReadAll(r.Body)
			m.putCount.Add(1)
			m.bytesUploaded.Add(int64(len(body)))
			m.mu.Lock()
			if mp, ok := m.multiparts[uid]; ok {
				mp.parts[pn] = body
			}
			m.mu.Unlock()
			sum := md5.Sum(body)
			w.Header().Set("ETag", fmt.Sprintf(`"%s"`, hex.EncodeToString(sum[:])))
			w.WriteHeader(http.StatusOK)
			return
		}

		if strings.HasSuffix(key, "layer_manifest.json") && m.failManifest.Load() {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if !strings.HasSuffix(key, "layer_manifest.json") && m.failPut.Load() {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		body, _ := io.ReadAll(r.Body)
		m.putCount.Add(1)
		m.bytesUploaded.Add(int64(len(body)))

		m.mu.Lock()
		defer m.mu.Unlock()
		if r.Header.Get("If-None-Match") == "*" {
			if _, exists := m.objects[key]; exists {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
		}
		if im := r.Header.Get("If-Match"); im != "" {
			cur, exists := m.objects[key]
			if !exists || cur.etag != im {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
		}
		sum := md5.Sum(body)
		etag := fmt.Sprintf(`"%s"`, hex.EncodeToString(sum[:]))
		sha := sha256.Sum256(body)
		m.objects[key] = &mockS3Object{
			data:           body,
			etag:           etag,
			checksumSHA256: base64.StdEncoding.EncodeToString(sha[:]),
			metadata:       extractAmzMeta(r.Header),
		}
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		if uid := q.Get("uploadId"); uid != "" {
			m.abortCount.Add(1)
			m.mu.Lock()
			delete(m.multiparts, uid)
			m.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		m.mu.Lock()
		delete(m.objects, key)
		m.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}
}

func extractAmzMeta(h http.Header) map[string]string {
	out := make(map[string]string)
	for k, vals := range h {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-meta-") && len(vals) > 0 {
			out[strings.TrimPrefix(lk, "x-amz-meta-")] = vals[0]
		}
	}
	return out
}

func TestCAU_HashAndKeyGeneration(t *testing.T) {
	dataA := []byte("immutable-layer-content-A")
	dataB := []byte("immutable-layer-content-B")

	hA1, sA1, err := StreamSHA256(bytes.NewReader(dataA))
	if err != nil || sA1 != int64(len(dataA)) {
		t.Fatalf("StreamSHA256 A1: err=%v size=%d", err, sA1)
	}
	hA2, _, err := StreamSHA256(bytes.NewReader(dataA))
	if err != nil || hA1 != hA2 {
		t.Fatalf("same content produced different hashes: %s vs %s", hA1, hA2)
	}
	hB, _, err := StreamSHA256(bytes.NewReader(dataB))
	if err != nil || hA1 == hB {
		t.Fatalf("different content produced same hash")
	}

	k1, err := ContentAddressedLayerKey("tenant_123", hA1)
	if err != nil {
		t.Fatalf("ContentAddressedLayerKey: %v", err)
	}
	want := fmt.Sprintf("tenants/tenant_123/layers/sha256/%s/%s", hA1[:2], hA1)
	if k1 != want {
		t.Fatalf("key=%q want=%q", k1, want)
	}
	if !IsContentAddressedLayerKey(k1) {
		t.Fatalf("IsContentAddressedLayerKey(%q) = false", k1)
	}

	// Uppercase hex is normalized to lowercase.
	kUpper, err := ContentAddressedLayerKey("tenant_123", strings.ToUpper(hA1))
	if err != nil || kUpper != k1 {
		t.Fatalf("uppercase normalization failed: %v %q", err, kUpper)
	}

	// Invalid hash and invalid tenant rejected.
	if _, err := ContentAddressedLayerKey("tenant_123", "abc"); err == nil {
		t.Fatal("expected error for short hash")
	}
	if _, err := ContentAddressedLayerKey("../bad", hA1); err == nil {
		t.Fatal("expected error for invalid tenant")
	}

	// Empty file & large file (> 256 KiB streaming buffer).
	hEmpty, szEmpty, err := StreamSHA256(bytes.NewReader(nil))
	if err != nil || szEmpty != 0 || hEmpty != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Fatalf("empty hash mismatch: %s size=%d err=%v", hEmpty, szEmpty, err)
	}
	large := bytes.Repeat([]byte{0x5A}, 512*1024)
	hLarge, szLarge, err := StreamSHA256(bytes.NewReader(large))
	if err != nil || szLarge != int64(len(large)) {
		t.Fatalf("large hash err=%v size=%d", err, szLarge)
	}
	expectedLarge := sha256.Sum256(large)
	if hLarge != hex.EncodeToString(expectedLarge[:]) {
		t.Fatalf("large hash mismatch")
	}
}

func TestCAU_DeduplicationAndIntegrityRejection(t *testing.T) {
	ctx := context.Background()
	_, s3c, mock := newMockR2Server(t)
	tmp := t.TempDir()
	store := NewLayerStore(tmp, s3c, "test-bucket", nil, nil)

	recs := []LayerRecord{{
		Key:   RelBlockKey(1663, 5, 100, 0, 0),
		Delta: WALDelta{LSN: 100, Flags: EntryFlagPageImage, PageImage: bytes.Repeat([]byte{0x33}, PageSize)},
	}}
	meta := LayerMeta{TenantID: "tenant_dedup", Epoch: 1, Kind: LayerKindL0, FileName: "l0_000001_0000000000000064_0000000000000065.layer", EndLSN: 101}

	// First upload creates object.
	m1, err := store.WriteLayer(ctx, meta, recs)
	if err != nil {
		t.Fatalf("first WriteLayer: %v", err)
	}
	if !IsContentAddressedLayerKey(m1.R2Key) || !m1.RemotelyAvailable {
		t.Fatalf("unexpected LayerMeta: %+v", m1)
	}
	stats1 := store.LastUploadMetrics()
	if stats1.Deduplicated || stats1.PutRequests != 1 || stats1.BytesTransferred != m1.FileSize {
		t.Fatalf("unexpected first upload stats: %+v", stats1)
	}

	// Second upload of identical content reuses object with 0 bytes transferred.
	m2, err := store.UploadLayerFile(ctx, m1, filepath.Join(tmp, "tenant_dedup", meta.FileName))
	if err != nil {
		t.Fatalf("second UploadLayerFile: %v", err)
	}
	if m2.R2Key != m1.R2Key {
		t.Fatalf("dedup key mismatch: %s vs %s", m2.R2Key, m1.R2Key)
	}
	stats2 := store.LastUploadMetrics()
	if !stats2.Deduplicated || stats2.PutRequests != 0 || stats2.BytesTransferred != 0 {
		t.Fatalf("expected deduplicated upload with 0 puts/bytes, got %+v", stats2)
	}

	// Different content creates a different object.
	recsDiff := []LayerRecord{{
		Key:   RelBlockKey(1663, 5, 100, 0, 1),
		Delta: WALDelta{LSN: 102, Flags: EntryFlagPageImage, PageImage: bytes.Repeat([]byte{0x44}, PageSize)},
	}}
	metaDiff := LayerMeta{TenantID: "tenant_dedup", Epoch: 1, Kind: LayerKindL0, FileName: "l0_000002_0000000000000065_0000000000000066.layer", EndLSN: 102}
	m3, err := store.WriteLayer(ctx, metaDiff, recsDiff)
	if err != nil {
		t.Fatalf("different content WriteLayer: %v", err)
	}
	if m3.R2Key == m1.R2Key {
		t.Fatal("different content produced same CAU key")
	}

	// Existing object with wrong size is rejected.
	mock.mu.Lock()
	origData := append([]byte(nil), mock.objects[m1.R2Key].data...)
	mock.objects[m1.R2Key].data = origData[:len(origData)-5]
	mock.mu.Unlock()
	if _, err := store.UploadLayerFile(ctx, m1, filepath.Join(tmp, "tenant_dedup", meta.FileName)); !errors.Is(err, ErrLayerSizeMismatch) {
		t.Fatalf("expected ErrLayerSizeMismatch, got %v", err)
	}

	// Existing object with invalid checksum is rejected.
	mock.mu.Lock()
	mock.objects[m1.R2Key].data = origData
	mock.objects[m1.R2Key].checksumSHA256 = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0xFF}, 32))
	mock.mu.Unlock()
	if _, err := store.UploadLayerFile(ctx, m1, filepath.Join(tmp, "tenant_dedup", meta.FileName)); !errors.Is(err, ErrLayerChecksumMismatch) {
		t.Fatalf("expected ErrLayerChecksumMismatch, got %v", err)
	}

	// Existing object with no checksum metadata falls back to streaming GetObject and rejects corrupt bytes.
	mock.stripChecksum.Store(true)
	mock.mu.Lock()
	delete(mock.objects[m1.R2Key].metadata, "air-sha256")
	mock.objects[m1.R2Key].data[0] ^= 0xFF
	mock.mu.Unlock()
	if _, err := store.UploadLayerFile(ctx, m1, filepath.Join(tmp, "tenant_dedup", meta.FileName)); !errors.Is(err, ErrLayerChecksumMismatch) {
		t.Fatalf("expected ErrLayerChecksumMismatch on streaming fallback, got %v", err)
	}
	mock.stripChecksum.Store(false)
}

func TestCAU_MultipartAndAbortOnFailure(t *testing.T) {
	ctx := context.Background()
	_, s3c, mock := newMockR2Server(t)
	tmp := t.TempDir()
	store := NewLayerStore(tmp, s3c, "test-bucket", nil, nil)
	store.SetMultipartConfig(2048, 1024) // force multipart on ~8 KiB layer

	incompressible := make([]byte, PageSize)
	for i := range incompressible {
		incompressible[i] = byte((i*73 + i*i*13 + 0x77) ^ (i >> 3))
	}
	recs := []LayerRecord{{
		Key:   RelBlockKey(1663, 5, 200, 0, 0),
		Delta: WALDelta{LSN: 50, Flags: EntryFlagPageImage, PageImage: incompressible},
	}}
	meta := LayerMeta{TenantID: "tenant_mp", Epoch: 1, Kind: LayerKindL0, FileName: "l0_000001_0000000000000032_0000000000000033.layer", EndLSN: 51}

	// Inject failure on part upload -> must abort multipart upload.
	mock.failUploadPart.Store(true)
	if _, err := store.WriteLayer(ctx, meta, recs); err == nil {
		t.Fatal("expected multipart upload failure")
	}
	mock.failUploadPart.Store(false)
	if mock.abortCount.Load() != 1 {
		t.Fatalf("expected AbortMultipartUpload to be called once, got %d", mock.abortCount.Load())
	}

	// Retry succeeds via multipart.
	written, err := store.WriteLayer(ctx, meta, recs)
	if err != nil {
		t.Fatalf("retry WriteLayer failed: %v", err)
	}
	stats := store.LastUploadMetrics()
	if stats.MultipartParts < 2 || !written.RemotelyAvailable {
		t.Fatalf("expected multipart upload with >=2 parts, got %+v", stats)
	}
}

func TestCAU_ConcurrencyIdenticalAndDifferent(t *testing.T) {
	ctx := context.Background()
	_, s3c, mock := newMockR2Server(t)
	tmp := t.TempDir()
	store := NewLayerStore(tmp, s3c, "test-bucket", nil, nil)

	recs := []LayerRecord{{
		Key:   RelBlockKey(1663, 5, 300, 0, 0),
		Delta: WALDelta{LSN: 10, Flags: EntryFlagPageImage, PageImage: bytes.Repeat([]byte{0x99}, PageSize)},
	}}
	raw, err := EncodeLayerBytes("tenant_conc", 1, "", LayerKindL0, 11, recs, nil)
	if err != nil {
		t.Fatal(err)
	}
	layerPath := filepath.Join(tmp, "shared.layer")
	if err := os.WriteFile(layerPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	keys := make(chan string, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m, err := store.UploadLayerFile(ctx, LayerMeta{TenantID: "tenant_conc", Epoch: 1, FileName: "shared.layer"}, layerPath)
			if err != nil {
				errs <- err
				return
			}
			keys <- m.R2Key
		}()
	}
	wg.Wait()
	close(errs)
	close(keys)
	for err := range errs {
		t.Fatalf("concurrent identical upload failed: %v", err)
	}
	var firstKey string
	for k := range keys {
		if firstKey == "" {
			firstKey = k
		} else if k != firstKey {
			t.Fatalf("workers derived different keys: %s vs %s", firstKey, k)
		}
	}
	// Verify stored object bytes are uncorrupted.
	mock.mu.Lock()
	stored := mock.objects[firstKey]
	mock.mu.Unlock()
	if stored == nil || !bytes.Equal(stored.data, raw) {
		t.Fatal("concurrent uploads corrupted remote object")
	}
}

func TestCAU_CrashOrderingAndOrphanReuse(t *testing.T) {
	ctx := context.Background()
	_, s3c, mock := newMockR2Server(t)
	tmp := t.TempDir()
	store := NewLayerStore(tmp, s3c, "test-bucket", nil, nil)
	tl := NewTimeline("tenant_crash", 1, store, nil)

	// 1. Failure before upload (unfinalized file).
	badPath := filepath.Join(tmp, "not_finalized.layer")
	_ = os.WriteFile(badPath, []byte("partial"), 0o600)
	if _, err := store.UploadLayerFile(ctx, LayerMeta{TenantID: "tenant_crash"}, badPath); !errors.Is(err, ErrLayerNotFinalized) {
		t.Fatalf("expected ErrLayerNotFinalized, got %v", err)
	}
	if tl.LayerCount() != 0 {
		t.Fatal("expected 0 layers")
	}

	// 2. Failure during upload -> no manifest, 0 visible layers, in-memory preserved.
	tl.PutPage(RelBlockKey(1663, 5, 400, 0, 0), 20, bytes.Repeat([]byte{0xAA}, PageSize))
	mock.failPut.Store(true)
	if _, err := tl.FlushInMemory(ctx); err == nil {
		t.Fatal("expected FlushInMemory failure on PutObject fault")
	}
	mock.failPut.Store(false)
	if tl.LayerCount() != 0 {
		t.Fatal("expected 0 layers after upload failure")
	}

	// 3. Failure after upload but before metadata publication (manifest PutObject fails).
	mock.failManifest.Store(true)
	if _, err := tl.FlushInMemory(ctx); err == nil {
		t.Fatal("expected FlushInMemory failure on manifest publish fault")
	}
	mock.failManifest.Store(false)
	if tl.LayerCount() != 0 {
		t.Fatal("layer must not become visible if manifest publication failed")
	}
	// Verify orphan object exists in R2.
	if mock.bytesUploaded.Load() == 0 {
		t.Fatal("expected orphan object to have been uploaded before manifest failure")
	}

	// 4. Retry succeeds and reuses the orphan object!
	prevBytes := mock.bytesUploaded.Load()
	flushed, err := tl.FlushInMemory(ctx)
	if err != nil || flushed == nil {
		t.Fatalf("retry FlushInMemory failed: %v", err)
	}
	if tl.LayerCount() != 1 || !flushed.RemotelyAvailable {
		t.Fatalf("expected 1 remotely available layer, count=%d meta=%+v", tl.LayerCount(), flushed)
	}
	// Only the small manifest JSON was uploaded on retry, NOT the layer bytes again.
	stats := store.LastUploadMetrics()
	if !stats.Deduplicated {
		t.Fatalf("expected orphan layer reuse (Deduplicated=true), got %+v (delta=%d fileSize=%d)", stats, mock.bytesUploaded.Load()-prevBytes, flushed.FileSize)
	}
}

func TestCAU_DownloadVerifyAndLegacyCompat(t *testing.T) {
	ctx := context.Background()
	_, s3c, mock := newMockR2Server(t)
	tmp := t.TempDir()

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("tenant_compat"); err != nil {
		t.Fatal(err)
	}
	cache, err := NewDiskCache(filepath.Join(tmp, "cache"), 64*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	store := NewLayerStore(filepath.Join(tmp, "layers"), s3c, "test-bucket", kms, cache)

	// 1. Seed a legacy-format encrypted layer directly into mock R2.
	legacyFileName := "l0_000001_0000000000000001_000000000000000a.layer"
	legacyKey := "tenants/tenant_compat/timeline_1/layers/" + legacyFileName
	legacyRecs := []LayerRecord{{
		Key:   RelBlockKey(1663, 5, 500, 0, 0),
		Delta: WALDelta{LSN: 5, Flags: EntryFlagPageImage, PageImage: bytes.Repeat([]byte{0x12}, PageSize)},
	}}
	legacyBytes, err := EncodeLayerBytes("tenant_compat", 1, legacyKey, LayerKindL0, 10, legacyRecs, kms)
	if err != nil {
		t.Fatal(err)
	}
	mock.mu.Lock()
	mock.objects[legacyKey] = &mockS3Object{data: legacyBytes, etag: `"legacy"`}
	mock.mu.Unlock()

	// 2. Write a new CAU encrypted layer through Timeline.
	tl := NewTimeline("tenant_compat", 1, store, nil)
	tl.PutPage(RelBlockKey(1663, 5, 500, 0, 1), 20, bytes.Repeat([]byte{0x34}, PageSize))
	cauMeta, err := tl.FlushInMemory(ctx)
	if err != nil {
		t.Fatalf("FlushInMemory CAU: %v", err)
	}

	// 3. Simulate restart with fresh Timeline + empty localDir/cache, loading both legacy & CAU from R2!
	cache2, _ := NewDiskCache(filepath.Join(tmp, "cache2"), 64*1024*1024)
	store2 := NewLayerStore(filepath.Join(tmp, "layers2"), s3c, "test-bucket", kms, cache2)
	tl2 := NewTimeline("tenant_compat", 1, store2, nil)
	if err := tl2.LoadLayersFromR2(ctx); err != nil {
		t.Fatalf("LoadLayersFromR2: %v", err)
	}
	if tl2.LayerCount() != 2 {
		t.Fatalf("expected 2 layers (1 legacy + 1 CAU), got %d", tl2.LayerCount())
	}

	p0, err := tl2.GetPageAtLSN(ctx, RelBlockKey(1663, 5, 500, 0, 0), 0)
	if err != nil || p0[0] != 0x12 {
		t.Fatalf("legacy layer read failed: err=%v", err)
	}
	p1, err := tl2.GetPageAtLSN(ctx, RelBlockKey(1663, 5, 500, 0, 1), 0)
	if err != nil || p1[0] != 0x34 {
		t.Fatalf("CAU encrypted layer read failed: err=%v", err)
	}

	// 4. DownloadLayer verifies SHA-256 and rejects corruption.
	dlPath := filepath.Join(tmp, "downloaded.layer")
	if err := store2.DownloadLayer(ctx, *cauMeta, dlPath); err != nil {
		t.Fatalf("DownloadLayer: %v", err)
	}
	mock.mu.Lock()
	mock.objects[cauMeta.R2Key].data[5] ^= 0xFF
	mock.mu.Unlock()
	if err := store2.DownloadLayer(ctx, *cauMeta, filepath.Join(tmp, "corrupt.layer")); !errors.Is(err, ErrLayerChecksumMismatch) {
		t.Fatalf("expected ErrLayerChecksumMismatch on corrupt download, got %v", err)
	}
}

func TestCAU_RealR2Integration(t *testing.T) {
	ak := os.Getenv("ACCESS_KEY")
	sk := os.Getenv("SECRET_KEY")
	ep := os.Getenv("ENDPOINT")
	if ak == "" || sk == "" || ep == "" {
		t.Skip("R2 credentials (ACCESS_KEY, SECRET_KEY, ENDPOINT) not set")
	}

	ctx := context.Background()
	s3c := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(ep),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(ak, sk, ""),
	})

	bucket := os.Getenv("AIR_R2_BUCKET")
	if bucket == "" {
		lb, err := s3c.ListBuckets(ctx, &s3.ListBucketsInput{})
		if err != nil || len(lb.Buckets) == 0 {
			t.Skipf("no R2 bucket available (err=%v)", err)
		}
		bucket = aws.ToString(lb.Buckets[0].Name)
	}

	tenantID := fmt.Sprintf("cau_it_%d", os.Getpid())
	tmp := t.TempDir()
	cache, _ := NewDiskCache(filepath.Join(tmp, "cache"), 32*1024*1024)
	store := NewLayerStore(filepath.Join(tmp, "layers"), s3c, bucket, nil, cache)

	tl := NewTimeline(tenantID, 1, store, nil)
	page := bytes.Repeat([]byte{0x5C}, PageSize)
	tl.PutPage(RelBlockKey(1663, 5, 700, 0, 0), 100, page)
	m1, err := tl.FlushInMemory(ctx)
	if err != nil {
		t.Fatalf("FlushInMemory to real R2: %v", err)
	}
	defer func() {
		_, _ = s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(m1.R2Key)})
		_, _ = s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(LayerManifestR2Key(tenantID, 1))})
	}()

	// Upload identical content again -> must reuse same key with 0 bytes transferred.
	localPath := filepath.Join(tmp, "layers", tenantID, m1.FileName)
	m2, err := store.UploadLayerFile(ctx, *m1, localPath)
	if err != nil {
		t.Fatalf("second UploadLayerFile to real R2: %v", err)
	}
	stats2 := store.LastUploadMetrics()
	if m2.R2Key != m1.R2Key || !stats2.Deduplicated || stats2.BytesTransferred != 0 {
		t.Fatalf("expected real R2 deduplication, got m2=%+v stats=%+v", m2, stats2)
	}

	// Download layer and verify SHA-256.
	dlPath := filepath.Join(tmp, "dl.layer")
	if err := store.DownloadLayer(ctx, m1.LayerMetaCopy(), dlPath); err != nil {
		t.Fatalf("DownloadLayer from real R2: %v", err)
	}
	gotSHA, _, err := StreamSHA256File(dlPath)
	if err != nil || gotSHA != m1.SHA256 {
		t.Fatalf("downloaded SHA256 mismatch: got=%s want=%s err=%v", gotSHA, m1.SHA256, err)
	}

	// Simulate component restart with fresh LayerStore & Timeline.
	storeRestart := NewLayerStore(filepath.Join(tmp, "layers_restart"), s3c, bucket, nil, nil)
	tlRestart := NewTimeline(tenantID, 1, storeRestart, nil)
	if err := tlRestart.LoadLayersFromR2(ctx); err != nil {
		t.Fatalf("LoadLayersFromR2 after restart: %v", err)
	}
	gotPage, err := tlRestart.GetPageAtLSN(ctx, RelBlockKey(1663, 5, 700, 0, 0), 0)
	if err != nil || !bytes.Equal(gotPage, page) {
		t.Fatalf("read after restart failed: err=%v", err)
	}
	t.Logf("Real R2 integration passed: bucket=%s key=%s sha256=%s size=%d", bucket, m1.R2Key, m1.SHA256, m1.FileSize)
}

func TestCAU_MetricsBreakdown(t *testing.T) {
	ctx := context.Background()
	_, s3c, mock := newMockR2Server(t)
	tmp := t.TempDir()
	store := NewLayerStore(tmp, s3c, "bench", nil, nil)

	buildRecs := func(pattern byte) []LayerRecord {
		recs := make([]LayerRecord, 128)
		for i := range recs {
			recs[i] = LayerRecord{
				Key:   RelBlockKey(1663, 5, 900, 0, uint32(i)),
				Delta: WALDelta{LSN: uint64(i + 1), Flags: EntryFlagPageImage, PageImage: bytes.Repeat([]byte{pattern}, PageSize)},
			}
		}
		return recs
	}

	// Case A: First upload of a new layer (~1 MiB).
	mA, err := store.WriteLayer(ctx, LayerMeta{TenantID: "t_m", Epoch: 1, Kind: LayerKindL0, FileName: "a.layer", EndLSN: 200}, buildRecs(0x11))
	if err != nil {
		t.Fatal(err)
	}
	sA := store.LastUploadMetrics()

	// Case B: Repeated upload of identical content.
	if _, err := store.UploadLayerFile(ctx, mA, filepath.Join(tmp, "t_m", "a.layer")); err != nil {
		t.Fatal(err)
	}
	sB := store.LastUploadMetrics()

	// Case C: Upload of a different layer.
	mC, err := store.WriteLayer(ctx, LayerMeta{TenantID: "t_m", Epoch: 1, Kind: LayerKindL0, FileName: "c.layer", EndLSN: 300}, buildRecs(0x22))
	if err != nil {
		t.Fatal(err)
	}
	sC := store.LastUploadMetrics()
	if mC.R2Key == mA.R2Key {
		t.Fatal("Case C expected different key")
	}

	// Case D: Concurrent upload of identical content (4 workers) for a fresh layer.
	rawD, err := EncodeLayerBytes("t_m", 1, "", LayerKindL0, 400, buildRecs(0x33), nil)
	if err != nil {
		t.Fatal(err)
	}
	pD := filepath.Join(tmp, "t_m", "d.layer")
	if err := os.WriteFile(pD, rawD, 0o600); err != nil {
		t.Fatal(err)
	}
	prevBytes := mock.bytesUploaded.Load()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.UploadLayerFile(ctx, LayerMeta{TenantID: "t_m", Epoch: 1, FileName: "d.layer"}, pD); err != nil {
				t.Errorf("Case D worker err: %v", err)
			}
		}()
	}
	wg.Wait()
	sD := store.LastUploadMetrics()
	dBytes := mock.bytesUploaded.Load() - prevBytes

	t.Logf("Case A (First):      size=%d hash=%v dedup=%v upload=%v verify=%v total=%v bytes=%d puts=%d heads=%d",
		sA.FileSize, sA.HashDuration, sA.DedupCheckDuration, sA.UploadDuration, sA.VerifyDuration, sA.TotalDuration, sA.BytesTransferred, sA.PutRequests, sA.HeadRequests)
	t.Logf("Case B (Repeated):   size=%d hash=%v dedup=%v upload=%v verify=%v total=%v bytes=%d puts=%d heads=%d deduped=%v",
		sB.FileSize, sB.HashDuration, sB.DedupCheckDuration, sB.UploadDuration, sB.VerifyDuration, sB.TotalDuration, sB.BytesTransferred, sB.PutRequests, sB.HeadRequests, sB.Deduplicated)
	t.Logf("Case C (Different):  size=%d hash=%v dedup=%v upload=%v verify=%v total=%v bytes=%d puts=%d heads=%d",
		sC.FileSize, sC.HashDuration, sC.DedupCheckDuration, sC.UploadDuration, sC.VerifyDuration, sC.TotalDuration, sC.BytesTransferred, sC.PutRequests, sC.HeadRequests)
	t.Logf("Case D (Concurrent): size=%d total_bytes_uploaded=%d last_worker_total=%v last_worker_deduped=%v",
		sD.FileSize, dBytes, sD.TotalDuration, sD.Deduplicated)
}

func (m LayerMeta) LayerMetaCopy() LayerMeta {
	return m
}

func BenchmarkCAU_UploadPaths(b *testing.B) {
	ctx := context.Background()
	raw := bytes.Repeat([]byte{0x42}, 1024*1024) // 1 MiB payload
	recs := make([]LayerRecord, 128)
	for i := range recs {
		recs[i] = LayerRecord{
			Key:   RelBlockKey(1663, 5, 800, 0, uint32(i)),
			Delta: WALDelta{LSN: uint64(i + 1), Flags: EntryFlagPageImage, PageImage: raw[:PageSize]},
		}
	}

	b.Run("A_FirstUpload", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			_, s3c, _ := newMockR2Bench(b)
			tmp := b.TempDir()
			store := NewLayerStore(tmp, s3c, "bench", nil, nil)
			meta := LayerMeta{TenantID: "t_bench", Epoch: 1, Kind: LayerKindL0, FileName: "l0.layer", EndLSN: 200}
			b.StartTimer()
			if _, err := store.WriteLayer(ctx, meta, recs); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("B_RepeatedIdentical", func(b *testing.B) {
		_, s3c, _ := newMockR2Bench(b)
		tmp := b.TempDir()
		store := NewLayerStore(tmp, s3c, "bench", nil, nil)
		meta := LayerMeta{TenantID: "t_bench", Epoch: 1, Kind: LayerKindL0, FileName: "l0.layer", EndLSN: 200}
		written, err := store.WriteLayer(ctx, meta, recs)
		if err != nil {
			b.Fatal(err)
		}
		lp := filepath.Join(tmp, "t_bench", "l0.layer")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := store.UploadLayerFile(ctx, written, lp); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("C_LegacyPutObject", func(b *testing.B) {
		_, s3c, _ := newMockR2Bench(b)
		layerBytes, _ := EncodeLayerBytes("t_bench", 1, "", LayerKindL0, 200, recs, nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := s3c.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String("bench"),
				Key:    aws.String(fmt.Sprintf("tenants/t_bench/timeline_1/layers/l0_%d.layer", i)),
				Body:   bytes.NewReader(layerBytes),
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func newMockR2Bench(b *testing.B) (*httptest.Server, *s3.Client, *mockR2Server) {
	b.Helper()
	m := &mockR2Server{
		objects:    make(map[string]*mockS3Object),
		multiparts: make(map[string]*mockMultipart),
	}
	srv := httptest.NewServer(http.HandlerFunc(m.handle))
	b.Cleanup(srv.Close)
	s3c := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("test-ak", "test-sk", ""),
	})
	return srv, s3c, m
}
