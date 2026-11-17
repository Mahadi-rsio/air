package pageserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func makePhase9IncompressiblePage(seed uint64) []byte {
	p := make([]byte, PageSize)
	x := seed*0x9e3779b97f4a7c15 + 1
	for i := 0; i < PageSize; i += 8 {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		binary.LittleEndian.PutUint64(p[i:i+8], x)
	}
	return p
}

func makePhase9CompressiblePage(seed uint64) []byte {
	p := make([]byte, PageSize)
	binary.LittleEndian.PutUint64(p[0:8], seed)
	copy(p[8:64], bytes.Repeat([]byte("AIR-PG17-COMP-"), 4))
	return p
}

// --- Part 1: V2 Storage Validation ---

func TestPhase9_Part1_V2LargeCompressibleIncompressibleAndMetrics(t *testing.T) {
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("t-p9-v2"); err != nil {
		t.Fatal(err)
	}

	const numRecs = 1024 // ~8 MiB raw page data per layer
	for _, mode := range []string{"compressible", "incompressible"} {
		t.Run(mode, func(t *testing.T) {
			recs := make([]LayerRecord, numRecs)
			sharedWAL := bytes.Repeat([]byte{0xAB, 0xCD, 0xEF, 0x01}, 32)
			for i := 0; i < numRecs; i++ {
				var img []byte
				if mode == "compressible" {
					img = makePhase9CompressiblePage(uint64(i + 1))
				} else {
					img = makePhase9IncompressiblePage(uint64(i + 1))
				}
				recs[i] = LayerRecord{
					Key: RelBlockKey(1663, 1, 5000, 0, uint32(i)),
					Delta: WALDelta{
						LSN:       uint64(100 + i),
						Flags:     EntryFlagPageImage | EntryFlagWALRecord,
						PageImage: img,
						WALRecord: sharedWAL,
					},
				}
			}

			var mBefore, mAfter runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&mBefore)

			var buf bytes.Buffer
			t0 := time.Now()
			res, err := EncodeLayerV2ToWriter(&buf, "t-p9-v2", 1, "t-p9-v2/l0.layer", LayerKindL0, uint64(100+numRecs), recs, kms, v2EncodeOptions{})
			encDur := time.Since(t0)
			if err != nil {
				t.Fatalf("EncodeLayerV2ToWriter: %v", err)
			}

			runtime.ReadMemStats(&mAfter)
			rawBytes := int64(numRecs * PageSize)
			ratio := float64(rawBytes) / float64(res.FileSize)

			dir := t.TempDir()
			layerPath := filepath.Join(dir, "t-p9-v2", "l0.layer")
			if err := os.MkdirAll(filepath.Dir(layerPath), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(layerPath, buf.Bytes(), 0o600); err != nil {
				t.Fatal(err)
			}

			store := NewLayerStore(dir, nil, "", kms, nil)
			meta := LayerMeta{
				TenantID:  "t-p9-v2",
				Epoch:     1,
				Kind:      LayerKindL0,
				FileName:  "l0.layer",
				R2Key:     "t-p9-v2/l0.layer",
				AADKey:    "t-p9-v2/l0.layer",
				FileSize:  res.FileSize,
				Encrypted: true,
			}

			keys := make([]Key, numRecs)
			for i := range keys {
				keys[i] = recs[i].Key
			}
			decStart := time.Now()
			got, err := store.LookupKeysInLayer(context.Background(), meta, keys, 0)
			decDur := time.Since(decStart)
			if err != nil {
				t.Fatalf("LookupKeysInLayer: %v", err)
			}
			for i := 0; i < numRecs; i++ {
				if len(got[i]) != 1 || !bytes.Equal(got[i][0].PageImage, recs[i].Delta.PageImage) {
					t.Fatalf("decoded page mismatch at idx=%d", i)
				}
			}
			decMBps := (float64(rawBytes) / (1024 * 1024)) / decDur.Seconds()
			t.Logf("V2 %s: raw=%d encoded=%d ratio=%.2fx enc=%v dec=%v (%.1f MB/s) totalAllocDelta=%d KiB",
				mode, rawBytes, res.FileSize, ratio, encDur, decDur, decMBps, (mAfter.TotalAlloc-mBefore.TotalAlloc)/1024)
		})
	}
}

func TestPhase9_Part1_V2CorruptionMalformedAndBounds(t *testing.T) {
	// 1. Malformed unencrypted index with huge rawLen must fail without OOM/panic.
	var hugeHdr [v2IndexHeaderSize]byte
	hugeHdr[0] = LayerCodecZstd
	binary.LittleEndian.PutUint32(hugeHdr[4:8], 0xffffffff)
	malformedIdx := append(hugeHdr[:], []byte{0x28, 0xb5, 0x2f, 0xfd}...)
	if _, err := decodeLayerV2Index(malformedIdx, LayerMeta{Encrypted: false}, 1, nil); err == nil {
		t.Fatalf("expected error for malformed huge rawLen in unencrypted v2 index")
	}

	// 2. Malformed unencrypted WAL table with huge count must fail without panic.
	malformedWAL := make([]byte, 16)
	binary.LittleEndian.PutUint32(malformedWAL[0:4], 0xffffffff)
	if _, err := decodeV2WALTable(malformedWAL, LayerMeta{Encrypted: false}, v2WALTableMeta{
		CipherLen: uint32(len(malformedWAL)),
		RawLen:    uint32(len(malformedWAL)),
		Codec:     LayerCodecNone,
	}, nil); err == nil {
		t.Fatalf("expected error for malformed huge count in v2 WAL table")
	}

	// 3. Invalid codec ID.
	if _, err := decompressV2Payload([]byte("abc"), 99, 3); err == nil {
		t.Fatalf("expected error for unsupported codec 99")
	}

	// 4. Encrypted V2 AAD mismatch, truncated chunk, bit-flip, and nonce uniqueness.
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("t-corrupt"); err != nil {
		t.Fatal(err)
	}

	recs := []LayerRecord{
		{
			Key:   RelBlockKey(1663, 1, 100, 0, 0),
			Delta: WALDelta{LSN: 10, Flags: EntryFlagPageImage, PageImage: makePhase9IncompressiblePage(1)},
		},
	}
	raw, err := EncodeLayerV2Bytes("t-corrupt", 1, "t-corrupt/l0.layer", LayerKindL0, 10, recs, kms)
	if err != nil {
		t.Fatal(err)
	}

	// AAD mismatch via wrong tenant / wrong aadKey.
	footer, err := DecodeLayerFooter(raw[len(raw)-LayerFooterSize:])
	if err != nil {
		t.Fatal(err)
	}
	idxRaw := raw[footer.IndexOffset : footer.IndexOffset+uint64(footer.IndexLen)]
	if _, err := decodeLayerV2Index(idxRaw, LayerMeta{TenantID: "t-corrupt", Epoch: 1, AADKey: "wrong-key", Encrypted: true}, footer.EntryCount, kms); !errors.Is(err, crypto.ErrAADMismatch) {
		t.Fatalf("expected ErrAADMismatch for wrong AADKey, got %v", err)
	}

	// Bit-flip in chunk ciphertext must fail AES-GCM authentication.
	corrupted := append([]byte(nil), raw...)
	corrupted[5] ^= 0xff
	idxDecoded, err := decodeLayerV2Index(idxRaw, LayerMeta{TenantID: "t-corrupt", Epoch: 1, AADKey: "t-corrupt/l0.layer", Encrypted: true}, footer.EntryCount, kms)
	if err != nil {
		t.Fatal(err)
	}
	cm := idxDecoded.chunks[0]
	if _, err := decodeV2ChunkRaw(corrupted[cm.Offset:cm.Offset+uint64(cm.CipherLen)], LayerMeta{TenantID: "t-corrupt", Epoch: 1, AADKey: "t-corrupt/l0.layer", Encrypted: true}, 0, cm, kms); !errors.Is(err, crypto.ErrAADMismatch) {
		t.Fatalf("expected ErrAADMismatch on corrupted chunk, got %v", err)
	}

	// Nonce uniqueness check across 5,000 encryptions.
	seenNonces := make(map[[12]byte]struct{}, 5000)
	pt := []byte("nonce-check")
	for i := 0; i < 5000; i++ {
		ct, err := kms.EncryptObject("t-corrupt", 1, "nonce-test", pt)
		if err != nil {
			t.Fatal(err)
		}
		var n [12]byte
		copy(n[:], ct[:12])
		if _, dup := seenNonces[n]; dup {
			t.Fatalf("duplicate AES-GCM nonce detected at iteration %d", i)
		}
		seenNonces[n] = struct{}{}
	}
}

func TestPhase9_Part1_V2MixedTimelinesHistoricalLSNAndStreamCleanup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("t-mix"); err != nil {
		t.Fatal(err)
	}
	store := NewLayerStore(dir, nil, "", kms, nil)
	tl := NewTimeline("t-mix", 1, store, NewDefaultWALRedoer(""))

	k1 := RelBlockKey(1663, 1, 200, 0, 0)
	p10 := makePhase9CompressiblePage(10)
	p20 := makePhase9CompressiblePage(20)
	p30 := makePhase9CompressiblePage(30)

	// V1 layer at LSN 10, V2 layers at LSN 20 and 30.
	store.SetLayerFormatVersion(LayerVersion)
	tl.PutPage(k1, 10, p10)
	if _, err := tl.FlushInMemory(ctx); err != nil {
		t.Fatal(err)
	}
	store.SetLayerFormatVersion(LayerVersionV2)
	tl.PutPage(k1, 20, p20)
	if _, err := tl.FlushInMemory(ctx); err != nil {
		t.Fatal(err)
	}
	tl.PutPage(k1, 30, p30)
	if _, err := tl.FlushInMemory(ctx); err != nil {
		t.Fatal(err)
	}
	tl.PutPage(k1, 40, makePhase9CompressiblePage(40))
	if _, err := tl.FlushInMemory(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := tl.CompactIfNeeded(ctx, 4, false); err != nil {
		t.Fatalf("CompactIfNeeded: %v", err)
	}

	for _, tc := range []struct {
		lsn  uint64
		want []byte
	}{
		{10, p10},
		{20, p20},
		{30, p30},
	} {
		got, err := tl.GetPageAtLSN(ctx, k1, tc.lsn)
		if err != nil {
			t.Fatalf("GetPageAtLSN(%d): %v", tc.lsn, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Fatalf("historical LSN %d mismatch", tc.lsn)
		}
	}

	// Streaming writer failure must clean up .tmp file.
	injectedErr := errors.New("injected stream failure")
	_, err = store.writeLayerFromStream(ctx, LayerMeta{
		TenantID: "t-mix",
		Epoch:    1,
		Kind:     LayerKindL0,
		FileName: "fail_stream.layer",
		R2Key:    "t-mix/fail_stream.layer",
	}, func(sw *v2StreamWriter) error {
		_ = sw.Add(LayerRecord{Key: k1, Delta: WALDelta{LSN: 50, Flags: EntryFlagPageImage, PageImage: p10}})
		return injectedErr
	})
	if !errors.Is(err, injectedErr) {
		t.Fatalf("expected injectedErr, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "t-mix", "fail_stream.layer.tmp")); !os.IsNotExist(statErr) {
		t.Fatalf("expected .tmp file to be cleaned up, statErr=%v", statErr)
	}
}

// --- Part 3: CAU and Manifest Failure Testing ---

func TestPhase9_Part3_CAUMultipartFailuresAndIdempotency(t *testing.T) {
	ctx := context.Background()
	_, s3c, mock := newMockR2Server(t)

	// Build a real finalized V2 layer file > 2 KiB (incompressible so > multipartThreshold).
	dir := t.TempDir()
	localStore := NewLayerStore(dir, nil, "", nil, nil)
	localStore.SetLayerFormatVersion(LayerVersionV2)
	recs := []LayerRecord{
		{Key: RelBlockKey(1663, 1, 700, 0, 0), Delta: WALDelta{LSN: 10, Flags: EntryFlagPageImage, PageImage: makePhase9IncompressiblePage(700)}},
		{Key: RelBlockKey(1663, 1, 700, 0, 1), Delta: WALDelta{LSN: 11, Flags: EntryFlagPageImage, PageImage: makePhase9IncompressiblePage(701)}},
	}
	lm, err := localStore.WriteLayer(ctx, LayerMeta{
		TenantID: "t-cau",
		Epoch:    1,
		Kind:     LayerKindL0,
		FileName: "l0_cau.layer",
		R2Key:    "tenants/t-cau/timeline_1/layers/l0_cau.layer",
		StartLSN: 10,
		EndLSN:   11,
	}, recs)
	if err != nil {
		t.Fatal(err)
	}
	layerPath := filepath.Join(dir, "t-cau", "l0_cau.layer")

	// 1. Individual part retry exhaustion -> MultipartUpload aborted.
	store := NewLayerStore("", s3c, "air-bucket", nil, nil)
	store.SetMultipartConfig(2048, 2048)
	mock.failUploadPart.Store(true)
	if _, err := store.UploadLayerFile(ctx, lm, layerPath); err == nil {
		t.Fatalf("expected upload failure on persistent part error")
	}
	aborted := mock.abortCount.Load()
	mock.mu.Lock()
	activeUploads := len(mock.multiparts)
	mock.mu.Unlock()
	mock.failUploadPart.Store(false)
	if aborted == 0 || activeUploads != 0 {
		t.Fatalf("expected AbortMultipartUpload cleanup, aborted=%d active=%d", aborted, activeUploads)
	}

	// 2. Context cancellation during multipart upload.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.UploadLayerFile(cancelCtx, lm, layerPath); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// 3. Successful multipart upload followed by duplicate publication (dedup).
	uploaded, err := store.UploadLayerFile(ctx, lm, layerPath)
	if err != nil {
		t.Fatalf("UploadLayerFile: %v", err)
	}
	if !uploaded.RemotelyAvailable {
		t.Fatalf("expected RemotelyAvailable=true")
	}
	if _, err := store.UploadLayerFile(ctx, lm, layerPath); err != nil {
		t.Fatalf("duplicate UploadLayerFile: %v", err)
	}
	if !store.LastUploadMetrics().Deduplicated {
		t.Fatalf("expected duplicate upload to be deduplicated")
	}

	// 4. PUT success followed by client timeout / 412 race -> verifyRemoteObject confirms remote object.
	storeSingle := NewLayerStore("", s3c, "air-bucket", nil, nil)
	storeSingle.SetMultipartConfig(64*1024*1024, 8*1024*1024)
	if _, err := storeSingle.UploadLayerFile(ctx, lm, layerPath); err != nil {
		t.Fatalf("single-part dedup upload: %v", err)
	}
}

func TestPhase9_Part3_ManifestCASConflictDoesNotOverwrite(t *testing.T) {
	ctx := context.Background()

	// Custom HTTP test server simulating R2 ETag CAS on manifest.json.
	var mu sync.Mutex
	var storedBody []byte
	var storedETag string
	var seq int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			if storedBody == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", storedETag)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(storedBody)))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			if ifNone := r.Header.Get("If-None-Match"); ifNone == "*" && storedBody != nil {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			if ifMatch := r.Header.Get("If-Match"); ifMatch != "" && ifMatch != storedETag {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			seq++
			storedBody = body
			storedETag = fmt.Sprintf("\"etag-%d\"", seq)
			w.Header().Set("ETag", storedETag)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if storedBody == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", storedETag)
			_, _ = w.Write(storedBody)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	s3c := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("k", "s", ""),
	})

	store := NewLayerStore("", s3c, "b", nil, nil)
	// Publish initial manifest v1.
	l1 := LayerMeta{TenantID: "t-cas", Epoch: 1, FileName: "l0_1.layer", R2Key: "k1"}
	if err := store.PublishLayerManifest(ctx, "t-cas", 1, []LayerMeta{l1}, 1); err != nil {
		t.Fatal(err)
	}

	// Simulate a racing writer advancing the manifest to v2 (with l2) between HeadObject and PutObject of stale writer!
	l2 := LayerMeta{TenantID: "t-cas", Epoch: 1, FileName: "l0_2.layer", R2Key: "k2"}
	var injectedOnce atomic.Bool
	racedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && !injectedOnce.Swap(true) {
			// Advance remote manifest before the stale writer's PUT arrives.
			_ = store.PublishLayerManifest(ctx, "t-cas", 1, []LayerMeta{l1, l2}, 2)
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("ETag", storedETag)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(storedBody)))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			if ifMatch := r.Header.Get("If-Match"); ifMatch != "" && ifMatch != storedETag {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			seq++
			storedBody = body
			storedETag = fmt.Sprintf("\"etag-%d\"", seq)
			w.Header().Set("ETag", storedETag)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer racedSrv.Close()

	racedS3 := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(racedSrv.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("k", "s", ""),
	})
	staleStore := NewLayerStore("", racedS3, "b", nil, nil)
	l3 := LayerMeta{TenantID: "t-cas", Epoch: 1, FileName: "l0_3.layer", R2Key: "k3"}
	if err := staleStore.PublishLayerManifest(ctx, "t-cas", 1, []LayerMeta{l1, l3}, 3); !errors.Is(err, ErrManifestCASConflict) {
		t.Fatalf("expected ErrManifestCASConflict on stale ETag race, got %v (stale writer blindly overwrote newer manifest!)", err)
	}
	loaded, err := store.LoadLayerManifest(ctx, "t-cas", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Layers) != 2 || loaded.Layers[1].FileName != "l0_2.layer" {
		t.Fatalf("manifest was clobbered: got %+v", loaded.Layers)
	}
}

// --- Part 4: Streaming Compaction and Bootstrap ---

func TestPhase9_Part4_Compaction16_64_256_AndConcurrentSafety(t *testing.T) {
	ctx := context.Background()
	for _, numL0 := range []int{16, 64, 256} {
		t.Run(fmt.Sprintf("L0=%d", numL0), func(t *testing.T) {
			dir := t.TempDir()
			store := NewLayerStore(dir, nil, "", nil, nil)
			tl := NewTimeline("t-comp", 1, store, NewDefaultWALRedoer(""))
			for i := 0; i < numL0; i++ {
				k := RelBlockKey(1663, 1, 100, 0, uint32(i%8))
				tl.PutPage(k, uint64(i+1), makePhase9CompressiblePage(uint64(i+1)))
				if _, err := tl.FlushInMemory(ctx); err != nil {
					t.Fatal(err)
				}
			}

			// Concurrent readers + concurrent CompactIfNeeded calls.
			var wg sync.WaitGroup
			var readErrs atomic.Int64
			for r := 0; r < 4; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 20; j++ {
						if _, err := tl.GetPageAtLSN(ctx, RelBlockKey(1663, 1, 100, 0, 0), 0); err != nil {
							readErrs.Add(1)
						}
					}
				}()
			}
			for c := 0; c < 2; c++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, _ = tl.CompactIfNeeded(ctx, numL0, false)
				}()
			}
			wg.Wait()

			if readErrs.Load() > 0 {
				t.Fatalf("concurrent readers saw %d errors during compaction", readErrs.Load())
			}
			if cnt := tl.LayerCount(); cnt != 1 {
				t.Fatalf("expected exactly 1 compacted L1 layer (no duplicate compaction), got %d", cnt)
			}
		})
	}
}

func TestPhase9_Part4_BootstrapMultiSegmentAndErrors(t *testing.T) {
	ctx := context.Background()
	pgData := t.TempDir()
	dbDir := filepath.Join(pgData, "base", "16384")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Segment 0 has 3 pages, Segment 1 has 2 pages -> total RelSize should be 131072 + 2 = 131074!
	if err := os.WriteFile(filepath.Join(dbDir, "20001"), bytes.Repeat(makePhase9CompressiblePage(1), 3), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dbDir, "20001.1"), bytes.Repeat(makePhase9CompressiblePage(2), 2), 0o600); err != nil {
		t.Fatal(err)
	}

	ps, err := New(Config{
		SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
		LayerDir:   t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	lm, err := ps.BootstrapTenantFromPGData(ctx, "t-boot", 1, pgData, 100)
	if err != nil || lm == nil {
		t.Fatalf("BootstrapTenantFromPGData: lm=%v err=%v", lm, err)
	}
	gotSize, err := ps.GetRelSize(ctx, "t-boot", 1663, 16384, 20001, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	const wantSize = uint32(131072 + 2)
	if gotSize != wantSize {
		t.Fatalf("multi-segment RelSize = %d, want %d", gotSize, wantSize)
	}

	// Verify block in segment 1 is readable.
	gotPage, err := ps.GetPageAtLSN(ctx, "t-boot", RelBlockKey(1663, 16384, 20001, 0, 131073), 100)
	if err != nil || len(gotPage) != PageSize {
		t.Fatalf("GetPageAtLSN seg1 block: len=%d err=%v", len(gotPage), err)
	}
}

// --- Part 6: Failure and Recovery Testing ---

func TestPhase9_Part6_FailureAndRecovery(t *testing.T) {
	ctx := context.Background()
	cacheDir := t.TempDir()
	_, s3c, _ := newMockR2Server(t)

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := kms.ExportWrappedDEKBundle("t-rec")
	if err != nil {
		t.Fatal(err)
	}

	// 1. Write and publish encrypted V2 layer to R2 + cache.
	cache1, err := NewDiskCache(cacheDir, 4*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	store1 := NewLayerStore("", s3c, "air-bucket", kms, cache1)
	tl1 := NewTimeline("t-rec", 1, store1, NewDefaultWALRedoer(""))
	k := RelBlockKey(1663, 1, 900, 0, 0)
	wantPage := makePhase9IncompressiblePage(900)
	tl1.PutPage(k, 50, wantPage)
	lm, err := tl1.FlushInMemory(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Corrupt cached file on disk; reading with an uncached fallback or re-fetch detects corruption cleanly.
	entries, _ := os.ReadDir(cacheDir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".layer" {
			p := filepath.Join(cacheDir, e.Name())
			b, _ := os.ReadFile(p)
			if len(b) > 10 {
				b[5] ^= 0xff
				_ = os.WriteFile(p, b, 0o600)
			}
		}
	}
	// Corrupted cached file must NEVER silently return bad plaintext!
	if _, err := tl1.GetPageAtLSN(ctx, k, 0); err == nil {
		t.Fatalf("expected authentication/decode failure on corrupted cached file")
	}

	// 3. Simulated Pageserver restart: new DiskCache + new EnvelopeKMS with imported bundle + LoadLayersFromR2.
	if err := kms.ImportWrappedDEKBundle(bundle); err != nil {
		t.Fatal(err)
	}
	cache2, err := NewDiskCache(t.TempDir(), 4*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	store2 := NewLayerStore("", s3c, "air-bucket", kms, cache2)
	tl2 := NewTimeline("t-rec", 1, store2, NewDefaultWALRedoer(""))
	if err := tl2.LoadLayersFromR2(ctx); err != nil {
		t.Fatalf("LoadLayersFromR2: %v", err)
	}
	gotPage, err := tl2.GetPageAtLSN(ctx, k, 0)
	if err != nil || !bytes.Equal(gotPage, wantPage) {
		t.Fatalf("recovery after restart failed for layer %s: err=%v", lm.R2Key, err)
	}

	// 4. WAL redo pool request cancellation under full queue.
	redoer := NewWALRedoPool(WALRedoPoolConfig{Workers: 1, QueueSize: 1, RequestTimeout: 200 * time.Millisecond})
	defer redoer.Close()
	cctx, ccancel := context.WithCancel(ctx)
	ccancel()
	if _, err := redoer.Redo(cctx, k, nil, []WALDelta{{LSN: 60, Flags: EntryFlagWALRecord, WALRecord: []byte{1, 2, 3}}}); err == nil {
		t.Fatalf("expected error on canceled WAL redo request")
	}
}
