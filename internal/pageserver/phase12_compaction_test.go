package pageserver

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// memR2Server is an in-memory S3/R2-compatible HTTP mock that supports PUT, GET (with Range),
// HEAD, DELETE, ListObjectsV2, and fault injection + request counting.
type memR2Server struct {
	mu          sync.RWMutex
	objects     map[string][]byte
	etags       map[string]string
	seq         atomic.Int64
	getCount    atomic.Int64
	putCount    atomic.Int64
	failNextPut atomic.Int32
	failKeySub  string
	srv         *httptest.Server
}

func newMemR2Server() *memR2Server {
	m := &memR2Server{
		objects: make(map[string][]byte),
		etags:   make(map[string]string),
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	return m
}

func (m *memR2Server) Close() { m.srv.Close() }

func (m *memR2Server) Client() *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint:     aws.String(m.srv.URL),
		UsePathStyle:     true,
		Region:           "auto",
		Credentials:      credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
		RetryMaxAttempts: 1,
	})
}

func (m *memR2Server) handle(w http.ResponseWriter, r *http.Request) {
	// Path style: /<bucket>/<key...> or /<bucket>?list-type=2&prefix=...
	trimmed := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}

	if r.Method == http.MethodGet && key == "" && r.URL.Query().Get("list-type") == "2" {
		prefix := r.URL.Query().Get("prefix")
		m.mu.RLock()
		var keys []string
		for k := range m.objects {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		b.WriteString(fmt.Sprintf("<Name>%s</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount><IsTruncated>false</IsTruncated>",
			parts[0], prefix, len(keys)))
		for _, k := range keys {
			b.WriteString(fmt.Sprintf("<Contents><Key>%s</Key><Size>%d</Size><ETag>%q</ETag></Contents>",
				k, len(m.objects[k]), m.etags[k]))
		}
		m.mu.RUnlock()
		b.WriteString(`</ListBucketResult>`)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(b.String()))
		return
	}

	switch r.Method {
	case http.MethodPut:
		m.putCount.Add(1)
		if m.failNextPut.Load() > 0 && (m.failKeySub == "" || strings.Contains(key, m.failKeySub)) {
			m.failNextPut.Add(-1)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`<Error><Code>InternalError</Code></Error>`))
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		etag := fmt.Sprintf("etag-%d", m.seq.Add(1))
		m.mu.Lock()
		m.objects[key] = body
		m.etags[key] = etag
		m.mu.Unlock()
		w.Header().Set("ETag", fmt.Sprintf("%q", etag))
		w.WriteHeader(http.StatusOK)
	case http.MethodGet, http.MethodHead:
		if r.Method == http.MethodGet {
			m.getCount.Add(1)
		}
		m.mu.RLock()
		data, ok := m.objects[key]
		etag := m.etags[key]
		m.mu.RUnlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
			return
		}
		w.Header().Set("ETag", fmt.Sprintf("%q", etag))
		if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
			spec := strings.TrimPrefix(rng, "bytes=")
			bounds := strings.SplitN(spec, "-", 2)
			start, _ := strconv.Atoi(bounds[0])
			end := len(data) - 1
			if bounds[1] != "" {
				end, _ = strconv.Atoi(bounds[1])
			}
			if start < 0 || start >= len(data) || end < start {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if end >= len(data) {
				end = len(data) - 1
			}
			slice := data[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			w.Header().Set("Content-Length", strconv.Itoa(len(slice)))
			w.WriteHeader(http.StatusPartialContent)
			if r.Method == http.MethodGet {
				_, _ = w.Write(slice)
			}
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write(data)
		}
	case http.MethodDelete:
		m.mu.Lock()
		delete(m.objects, key)
		delete(m.etags, key)
		m.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func makeStampedPage(rel uint32, blk uint32, lsn uint64) []byte {
	p := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(p[0:4], rel)
	binary.LittleEndian.PutUint32(p[4:8], blk)
	binary.LittleEndian.PutUint64(p[8:16], lsn)
	return p
}

// TestPhase12L0FragmentationAndCompaction measures the actual read amplification,
// manifest size, R2 GET request count, and lookup latency of 40 overlapping L0 layers
// before and after compaction (Part 5).
func TestPhase12L0FragmentationAndCompaction(t *testing.T) {
	ctx := context.Background()
	r2 := newMemR2Server()
	defer r2.Close()

	dir := t.TempDir()
	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(dir, "master.key"))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := "t_p5_frag"
	gen := "gen_1"
	if _, err := kms.EnsureTenantDEK(tenantID); err != nil {
		t.Fatal(err)
	}

	// Build 40 L0 layers with overlapping block ranges (simulating 1M-row workload updates).
	storeWrite := NewLayerStore(filepath.Join(dir, "write"), r2.Client(), "air-bucket", kms, nil)
	tl := NewTimeline(tenantID, 1, storeWrite, nil)
	tl.SetGeneration(gen)

	const numLayers = 40
	const totalBlocks = 400
	expectedLSN := make([]uint64, totalBlocks)
	var lsn uint64 = 1000

	for layerIdx := 0; layerIdx < numLayers; layerIdx++ {
		// Each L0 touches a sliding window of 60 blocks so blocks overlap across many L0s.
		startBlk := (layerIdx * 10) % (totalBlocks - 60)
		for j := 0; j < 60; j++ {
			blk := uint32(startBlk + j)
			lsn += 10
			if err := tl.PutPageBatch(ctx, 1663, 16385, 20001, 0, blk, 1, lsn, makeStampedPage(20001, blk, lsn)); err != nil {
				t.Fatal(err)
			}
			expectedLSN[blk] = lsn
		}
		if _, err := tl.FlushInMemory(ctx); err != nil {
			t.Fatal(err)
		}
	}

	manifestKey := LayerManifestR2KeyGen(tenantID, gen, 1)
	r2.mu.RLock()
	manifestBeforeBytes := len(r2.objects[manifestKey])
	r2.mu.RUnlock()

	// Simulate cold wake into a fresh Pageserver/LayerStore with no local disk cache.
	coldRead := func(label string) (gets int64, p50, p95, p99 time.Duration) {
		r2.getCount.Store(0)
		storeCold := NewLayerStore(filepath.Join(dir, "cold_"+label), r2.Client(), "air-bucket", kms, nil)
		tlCold := NewTimeline(tenantID, 1, storeCold, nil)
		tlCold.SetGeneration(gen)
		if err := tlCold.LoadLayersFromR2(ctx); err != nil {
			t.Fatalf("LoadLayersFromR2(%s): %v", label, err)
		}

		latencies := make([]time.Duration, 0, totalBlocks)
		for blk := uint32(0); blk < totalBlocks; blk++ {
			exp := expectedLSN[blk]
			if exp == 0 {
				continue
			}
			t0 := time.Now()
			pg, err := tlCold.GetPageAtLSN(ctx, RelBlockKey(1663, 16385, 20001, 0, blk), lsn)
			dt := time.Since(t0)
			if err != nil {
				t.Fatalf("GetPage(%s, blk=%d): %v", label, blk, err)
			}
			got := binary.LittleEndian.Uint64(pg[8:16])
			if got != exp {
				t.Fatalf("%s blk %d: got lsn=%d want %d", label, blk, got, exp)
			}
			latencies = append(latencies, dt)
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		gets = r2.getCount.Load()
		p50 = latencies[len(latencies)*50/100]
		p95 = latencies[len(latencies)*95/100]
		p99 = latencies[len(latencies)*99/100]
		return gets, p50, p95, p99
	}

	getsBefore, p50Before, p95Before, p99Before := coldRead("before")

	// Now compact the 40 L0 layers into 1 materialized Image layer.
	meta, err := tl.CompactIfNeeded(ctx, 4, true)
	if err != nil {
		t.Fatalf("CompactIfNeeded: %v", err)
	}
	if meta == nil || meta.Kind != LayerKindImage {
		t.Fatalf("expected Image layer from compaction, got %+v", meta)
	}

	r2.mu.RLock()
	manifestAfterBytes := len(r2.objects[manifestKey])
	r2.mu.RUnlock()

	getsAfter, p50After, p95After, p99After := coldRead("after")

	t.Logf("L0 Fragmentation (40 L0s -> 1 Image): manifest %dB -> %dB; R2 GETs %d -> %d; p50 %v -> %v; p95 %v -> %v; p99 %v -> %v",
		manifestBeforeBytes, manifestAfterBytes, getsBefore, getsAfter,
		p50Before, p50After, p95Before, p95After, p99Before, p99After)

	if getsAfter >= getsBefore {
		t.Fatalf("expected compaction to reduce R2 GET requests, got before=%d after=%d", getsBefore, getsAfter)
	}
	if manifestAfterBytes >= manifestBeforeBytes {
		t.Fatalf("expected compaction to shrink manifest, got before=%d after=%d", manifestBeforeBytes, manifestAfterBytes)
	}
	_ = types.Object{}
}

func TestPhase12CompactionConcurrentReadsAndWrites(t *testing.T) {
	ctx := context.Background()
	r2 := newMemR2Server()
	defer r2.Close()

	dir := t.TempDir()
	store := NewLayerStore(dir, r2.Client(), "air-bucket", nil, nil)
	tl := NewTimeline("t_conc_cmp", 1, store, nil)
	tl.SetGeneration("g1")

	var lsn atomic.Uint64
	lsn.Store(100)
	for i := 0; i < 8; i++ {
		l := lsn.Add(10)
		if err := tl.PutPageBatch(ctx, 1663, 16385, 30001, 0, uint32(i%4), 1, l, makeStampedPage(30001, uint32(i%4), l)); err != nil {
			t.Fatal(err)
		}
		if _, err := tl.FlushInMemory(ctx); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 16)

	// Reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			curLSN := lsn.Load()
			pg, err := tl.GetPageAtLSN(ctx, RelBlockKey(1663, 16385, 30001, 0, uint32(i%4)), curLSN)
			if err != nil {
				errCh <- fmt.Errorf("read: %w", err)
				return
			}
			if len(pg) != PageSize {
				errCh <- fmt.Errorf("short page: %d", len(pg))
				return
			}
		}
	}()

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			l := lsn.Add(10)
			if err := tl.PutPageBatch(ctx, 1663, 16385, 30001, 0, uint32(i%4), 1, l, makeStampedPage(30001, uint32(i%4), l)); err != nil {
				errCh <- fmt.Errorf("write: %w", err)
				return
			}
			if _, err := tl.FlushInMemory(ctx); err != nil {
				errCh <- fmt.Errorf("flush: %w", err)
				return
			}
		}
	}()

	// Compactor goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			if _, err := tl.CompactIfNeeded(ctx, 4, false); err != nil {
				errCh <- fmt.Errorf("compact: %w", err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestPhase12CompactionR2FailurePreservesOldLayers(t *testing.T) {
	ctx := context.Background()
	r2 := newMemR2Server()
	defer r2.Close()

	store := NewLayerStore(t.TempDir(), r2.Client(), "air-bucket", nil, nil)
	tl := NewTimeline("t_cmp_fail", 1, store, nil)
	tl.SetGeneration("g1")

	var lsn uint64 = 100
	for i := 0; i < 5; i++ {
		lsn += 10
		if err := tl.PutPageBatch(ctx, 1663, 16385, 40001, 0, uint32(i), 1, lsn, makeStampedPage(40001, uint32(i), lsn)); err != nil {
			t.Fatal(err)
		}
		if _, err := tl.FlushInMemory(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Case 1: Layer upload fails during compaction (exhaust internal upload retries).
	r2.failKeySub = "/layers/"
	r2.failNextPut.Store(20)
	if _, err := tl.CompactIfNeeded(ctx, 4, false); err == nil {
		t.Fatal("expected compaction error on layer PUT failure")
	}

	// Case 2: Manifest upload fails after layer upload succeeds.
	r2.failKeySub = "manifest"
	r2.failNextPut.Store(20)
	if _, err := tl.CompactIfNeeded(ctx, 4, false); err == nil {
		t.Fatal("expected compaction error on manifest PUT failure")
	}
	r2.failNextPut.Store(0)
	r2.failKeySub = ""

	// Verify all 5 pages are still readable and old L0s are preserved.
	tl.mu.RLock()
	layerCount := len(tl.layers)
	tl.mu.RUnlock()
	if layerCount != 5 {
		t.Fatalf("expected 5 intact L0 layers after failed compactions, got %d", layerCount)
	}
	for i := 0; i < 5; i++ {
		pg, err := tl.GetPageAtLSN(ctx, RelBlockKey(1663, 16385, 40001, 0, uint32(i)), lsn)
		if err != nil {
			t.Fatalf("GetPage(%d) after failed compaction: %v", i, err)
		}
		if got := binary.LittleEndian.Uint32(pg[4:8]); got != uint32(i) {
			t.Fatalf("blk %d: got %d", i, got)
		}
	}
}

func TestPhase12CompactionRestartAndManifestRecovery(t *testing.T) {
	ctx := context.Background()
	r2 := newMemR2Server()
	defer r2.Close()

	dir := t.TempDir()
	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(dir, "master.key"))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := "t_cmp_rec"
	gen := "gen_cmp"
	if _, err := kms.EnsureTenantDEK(tenantID); err != nil {
		t.Fatal(err)
	}

	store1 := NewLayerStore(filepath.Join(dir, "node1"), r2.Client(), "air-bucket", kms, nil)
	tl1 := NewTimeline(tenantID, 1, store1, nil)
	tl1.SetGeneration(gen)

	var lsn uint64 = 100
	for i := 0; i < 6; i++ {
		lsn += 10
		blk := uint32(i % 3)
		if err := tl1.PutPageBatch(ctx, 1663, 16385, 50001, 0, blk, 1, lsn, makeStampedPage(50001, blk, lsn)); err != nil {
			t.Fatal(err)
		}
		if _, err := tl1.FlushInMemory(ctx); err != nil {
			t.Fatal(err)
		}
	}

	meta, err := tl1.CompactIfNeeded(ctx, 4, true)
	if err != nil {
		t.Fatalf("CompactIfNeeded: %v", err)
	}
	if meta == nil || meta.Kind != LayerKindImage {
		t.Fatalf("expected Image layer, got %+v", meta)
	}

	// Simulate fresh restart from R2 manifest only.
	store2 := NewLayerStore(filepath.Join(dir, "node2"), r2.Client(), "air-bucket", kms, nil)
	tl2 := NewTimeline(tenantID, 1, store2, nil)
	tl2.SetGeneration(gen)
	if err := tl2.LoadLayersFromR2(ctx); err != nil {
		t.Fatalf("LoadLayersFromR2: %v", err)
	}
	if got := tl2.LayerCount(); got != 1 {
		t.Fatalf("expected 1 compacted layer after manifest recovery, got %d", got)
	}

	// Blocks 0, 1, 2 should have their newest LSNs (140, 150, 160).
	for blk := uint32(0); blk < 3; blk++ {
		pg, err := tl2.GetPageAtLSN(ctx, RelBlockKey(1663, 16385, 50001, 0, blk), lsn)
		if err != nil {
			t.Fatalf("GetPageAtLSN(blk=%d): %v", blk, err)
		}
		wantLSN := uint64(140 + blk*10)
		if gotLSN := binary.LittleEndian.Uint64(pg[8:16]); gotLSN != wantLSN {
			t.Fatalf("blk %d: got LSN %d want %d", blk, gotLSN, wantLSN)
		}
	}

	sz, err := tl2.GetRelSize(ctx, 1663, 16385, 50001, 0, lsn)
	if err != nil {
		t.Fatalf("GetRelSize: %v", err)
	}
	if sz != 3 {
		t.Fatalf("GetRelSize = %d want 3", sz)
	}
}
