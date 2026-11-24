package pageserver

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CAWALBlobMagic   uint32 = 0x42574143 // "CAWB"
	CAWALBlobVersion uint16 = 1
	CAWALHeaderSize  int    = 48

	maxCAWALBlobRawBytes  = 16 * 1024 * 1024
	maxCAWALManifestBytes = 16 * 1024 * 1024
)

var (
	ErrCAWALInvalidBlob       = errors.New("pageserver: invalid CA-WAL blob")
	ErrCAWALIntegrityMismatch = errors.New("pageserver: CA-WAL blob integrity mismatch")
	ErrCAWALBlobMissing       = errors.New("pageserver: CA-WAL blob missing")
	ErrCAWALFaultInjected     = errors.New("pageserver: CA-WAL injected fault")
)

var cawalCompPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 64*1024)
		return &b
	},
}

// CAWALPutResult describes the outcome of PutBlobIfAbsent.
type CAWALPutResult struct {
	Hash         string
	RawLen       uint32
	StoredLen    uint32
	Codec        uint8
	Deduplicated bool
}

// CAWALRef maps a logical WAL chunk position on a timeline to an immutable physical blob.
// Logical WAL ordering and LSN/timeline semantics remain strictly independent of physical storage.
type CAWALRef struct {
	TenantID    string `json:"tenant_id"`
	Generation  string `json:"generation,omitempty"`
	TimelineID  string `json:"timeline_id"`
	Epoch       int64  `json:"epoch"`
	Seq         uint64 `json:"seq"`
	StartLSN    uint64 `json:"start_lsn"`
	EndLSN      uint64 `json:"end_lsn"`
	BlobHash    string `json:"blob_hash"`
	RawLen      uint32 `json:"raw_len"`
	RecordCount uint32 `json:"record_count"`
}

// CAWALManifest durably records ordered logical WAL chunk references for a timeline.
type CAWALManifest struct {
	Version    int        `json:"version"`
	TenantID   string     `json:"tenant_id"`
	Generation string     `json:"generation,omitempty"`
	TimelineID string     `json:"timeline_id"`
	Epoch      int64      `json:"epoch"`
	UpdatedAt  string     `json:"updated_at"`
	Refs       []CAWALRef `json:"refs"`
}

// CAWALMetrics tracks observability counters for content-addressed WAL operations.
type CAWALMetrics struct {
	BlobWrites           int64 `json:"blob_writes"`
	BlobBytesWritten     int64 `json:"blob_bytes_written"`
	BlobBytesLogical     int64 `json:"blob_bytes_logical"`
	DedupHits            int64 `json:"dedup_hits"`
	HashFailures         int64 `json:"hash_failures"`
	CorruptBlobsDetected int64 `json:"corrupt_blobs_detected"`
	OrphanBlobsCleaned   int64 `json:"orphan_blobs_cleaned"`
	GCRuns               int64 `json:"gc_runs"`
	GCBytesReclaimed     int64 `json:"gc_bytes_reclaimed"`
}

// CAWALGCResult summarizes a single garbage collection pass.
type CAWALGCResult struct {
	ReachableBlobs int   `json:"reachable_blobs"`
	ScannedBlobs   int   `json:"scanned_blobs"`
	DeletedBlobs   int   `json:"deleted_blobs"`
	DeletedTemps   int   `json:"deleted_temps"`
	ReclaimedBytes int64 `json:"reclaimed_bytes"`
	Aborted        bool  `json:"aborted"`
}

// CAWALStore manages immutable SHA-256 content-addressed WAL chunk blobs,
// active write pins, durable logical manifests, and crash-safe GC.
type CAWALStore struct {
	rootDir string // base LayerDir; blobs live under <rootDir>/_wal_cas/sha256/<hh>/<hash>.walb
	casDir  string

	pinMu sync.Mutex
	pins  map[string]int

	memMu    sync.RWMutex
	memBlobs map[string][]byte // used when rootDir == ""

	tmpSeq atomic.Uint64

	blobWrites           atomic.Int64
	blobBytesWritten     atomic.Int64
	blobBytesLogical     atomic.Int64
	dedupHits            atomic.Int64
	hashFailures         atomic.Int64
	corruptBlobsDetected atomic.Int64
	orphanBlobsCleaned   atomic.Int64
	gcRuns               atomic.Int64
	gcBytesReclaimed     atomic.Int64

	// faultHook is for deterministic crash/fault-injection testing.
	faultHook func(stage string) error
}

// NewCAWALStore creates a CAWALStore rooted at rootDir (or in-memory if rootDir == "").
func NewCAWALStore(rootDir string) *CAWALStore {
	var casDir string
	if rootDir != "" {
		casDir = filepath.Join(rootDir, "_wal_cas")
	}
	return &CAWALStore{
		rootDir:  rootDir,
		casDir:   casDir,
		pins:     make(map[string]int),
		memBlobs: make(map[string][]byte),
	}
}

// Metrics returns a point-in-time snapshot of CAWALStore observability counters.
func (s *CAWALStore) Metrics() CAWALMetrics {
	return CAWALMetrics{
		BlobWrites:           s.blobWrites.Load(),
		BlobBytesWritten:     s.blobBytesWritten.Load(),
		BlobBytesLogical:     s.blobBytesLogical.Load(),
		DedupHits:            s.dedupHits.Load(),
		HashFailures:         s.hashFailures.Load(),
		CorruptBlobsDetected: s.corruptBlobsDetected.Load(),
		OrphanBlobsCleaned:   s.orphanBlobsCleaned.Load(),
		GCRuns:               s.gcRuns.Load(),
		GCBytesReclaimed:     s.gcBytesReclaimed.Load(),
	}
}

// Pin prevents GC from deleting hashHex until a corresponding Unpin is called.
func (s *CAWALStore) Pin(hashHex string) {
	if hashHex == "" {
		return
	}
	s.pinMu.Lock()
	s.pins[hashHex]++
	s.pinMu.Unlock()
}

// Unpin releases a reference pin acquired via Pin.
func (s *CAWALStore) Unpin(hashHex string) {
	if hashHex == "" {
		return
	}
	s.pinMu.Lock()
	if cur := s.pins[hashHex]; cur <= 1 {
		delete(s.pins, hashHex)
	} else {
		s.pins[hashHex] = cur - 1
	}
	s.pinMu.Unlock()
}

func (s *CAWALStore) isPinned(hashHex string) bool {
	s.pinMu.Lock()
	_, ok := s.pins[hashHex]
	s.pinMu.Unlock()
	return ok
}

func (s *CAWALStore) blobPath(hashHex string) string {
	return filepath.Join(s.casDir, "sha256", hashHex[:2], hashHex+".walb")
}

func encodeCAWALBlob(raw []byte, sum [32]byte) ([]byte, uint8, error) {
	compPtr := cawalCompPool.Get().(*[]byte)
	if cap(*compPtr) < len(raw) {
		*compPtr = make([]byte, 0, len(raw))
	}
	defer func() {
		if cap(*compPtr) <= 512*1024 {
			cawalCompPool.Put(compPtr)
		}
	}()

	payload, codec, err := compressV2PayloadInto(raw, *compPtr, false)
	if err != nil {
		return nil, 0, err
	}

	out := make([]byte, CAWALHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(out[0:4], CAWALBlobMagic)
	binary.LittleEndian.PutUint16(out[4:6], CAWALBlobVersion)
	out[6] = codec
	out[7] = 0
	binary.LittleEndian.PutUint32(out[8:12], uint32(len(raw)))
	binary.LittleEndian.PutUint32(out[12:16], uint32(len(payload)))
	copy(out[16:48], sum[:])
	copy(out[48:], payload)
	return out, codec, nil
}

func decodeAndVerifyCAWALBlob(blobBytes []byte, expectedSum [32]byte) ([]byte, uint8, error) {
	if len(blobBytes) < CAWALHeaderSize {
		return nil, 0, fmt.Errorf("%w: truncated header len=%d", ErrCAWALInvalidBlob, len(blobBytes))
	}
	magic := binary.LittleEndian.Uint32(blobBytes[0:4])
	ver := binary.LittleEndian.Uint16(blobBytes[4:6])
	if magic != CAWALBlobMagic || ver != CAWALBlobVersion {
		return nil, 0, fmt.Errorf("%w: magic=0x%x ver=%d", ErrCAWALInvalidBlob, magic, ver)
	}
	codec := blobBytes[6]
	rawLen := binary.LittleEndian.Uint32(blobBytes[8:12])
	payloadLen := binary.LittleEndian.Uint32(blobBytes[12:16])
	if rawLen == 0 || rawLen > maxCAWALBlobRawBytes {
		return nil, 0, fmt.Errorf("%w: invalid rawLen=%d", ErrCAWALInvalidBlob, rawLen)
	}
	if uint64(CAWALHeaderSize)+uint64(payloadLen) != uint64(len(blobBytes)) {
		return nil, 0, fmt.Errorf("%w: payloadLen=%d fileLen=%d", ErrCAWALInvalidBlob, payloadLen, len(blobBytes))
	}
	var hdrSum [32]byte
	copy(hdrSum[:], blobBytes[16:48])
	if hdrSum != expectedSum {
		return nil, 0, fmt.Errorf("%w: header sha256 mismatch", ErrCAWALIntegrityMismatch)
	}

	raw, err := decompressV2Payload(blobBytes[CAWALHeaderSize:], codec, rawLen)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: %v", ErrCAWALIntegrityMismatch, err)
	}
	actualSum := sha256.Sum256(raw)
	if actualSum != expectedSum {
		return nil, 0, fmt.Errorf("%w: payload sha256 mismatch", ErrCAWALIntegrityMismatch)
	}
	if codec == LayerCodecNone {
		// Return an owned copy if uncompressed so callers cannot alias blobBytes.
		cp := make([]byte, len(raw))
		copy(cp, raw)
		return cp, codec, nil
	}
	return raw, codec, nil
}

// PutBlobIfAbsent atomically stores raw under its SHA-256 content address if not already present.
// The caller should Pin(res.Hash) or keep it pinned until the referencing manifest is durable;
// PutBlobIfAbsent itself pins during the write and leaves the caller to manage post-write pinning
// if needed via Pin/Unpin.
func (s *CAWALStore) PutBlobIfAbsent(ctx context.Context, raw []byte) (CAWALPutResult, error) {
	if err := ctx.Err(); err != nil {
		return CAWALPutResult{}, err
	}
	if len(raw) == 0 || len(raw) > maxCAWALBlobRawBytes {
		return CAWALPutResult{}, fmt.Errorf("%w: invalid raw chunk size %d", ErrCAWALInvalidBlob, len(raw))
	}

	sum := sha256.Sum256(raw)
	hashHex := hex.EncodeToString(sum[:])
	s.blobBytesLogical.Add(int64(len(raw)))

	if s.casDir == "" {
		s.memMu.Lock()
		defer s.memMu.Unlock()
		if existing, ok := s.memBlobs[hashHex]; ok {
			if err := s.verifyExistingHeaderOnly(existing, sum, uint32(len(raw))); err != nil {
				s.hashFailures.Add(1)
				s.corruptBlobsDetected.Add(1)
				return CAWALPutResult{}, err
			}
			s.dedupHits.Add(1)
			return CAWALPutResult{
				Hash:         hashHex,
				RawLen:       uint32(len(raw)),
				StoredLen:    uint32(len(existing)),
				Codec:        existing[6],
				Deduplicated: true,
			}, nil
		}
		encoded, codec, err := encodeCAWALBlob(raw, sum)
		if err != nil {
			return CAWALPutResult{}, err
		}
		s.memBlobs[hashHex] = encoded
		s.blobWrites.Add(1)
		s.blobBytesWritten.Add(int64(len(encoded)))
		return CAWALPutResult{
			Hash:      hashHex,
			RawLen:    uint32(len(raw)),
			StoredLen: uint32(len(encoded)),
			Codec:     codec,
		}, nil
	}

	finalPath := s.blobPath(hashHex)
	if existing, err := s.statAndVerifyHeader(finalPath, sum, uint32(len(raw))); err == nil {
		s.dedupHits.Add(1)
		return CAWALPutResult{
			Hash:         hashHex,
			RawLen:       uint32(len(raw)),
			StoredLen:    existing.StoredLen,
			Codec:        existing.Codec,
			Deduplicated: true,
		}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		s.hashFailures.Add(1)
		s.corruptBlobsDetected.Add(1)
		return CAWALPutResult{}, err
	}

	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return CAWALPutResult{}, err
	}

	encoded, codec, err := encodeCAWALBlob(raw, sum)
	if err != nil {
		return CAWALPutResult{}, err
	}

	seq := s.tmpSeq.Add(1)
	tmpPath := fmt.Sprintf("%s.tmp.%d.%d", finalPath, os.Getpid(), seq)
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return CAWALPutResult{}, err
	}
	cleanupTmp := func() {
		_ = os.Remove(tmpPath)
	}

	if _, err := f.Write(encoded); err != nil {
		_ = f.Close()
		cleanupTmp()
		return CAWALPutResult{}, err
	}
	if s.faultHook != nil {
		if err := s.faultHook("during_blob_write"); err != nil {
			_ = f.Close()
			cleanupTmp()
			return CAWALPutResult{}, err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		cleanupTmp()
		return CAWALPutResult{}, err
	}
	if err := f.Close(); err != nil {
		cleanupTmp()
		return CAWALPutResult{}, err
	}

	// Atomic create-if-absent via POSIX hard link.
	if err := os.Link(tmpPath, finalPath); err != nil {
		cleanupTmp()
		if errors.Is(err, os.ErrExist) {
			existing, vErr := s.statAndVerifyHeader(finalPath, sum, uint32(len(raw)))
			if vErr != nil {
				s.hashFailures.Add(1)
				s.corruptBlobsDetected.Add(1)
				return CAWALPutResult{}, vErr
			}
			s.dedupHits.Add(1)
			return CAWALPutResult{
				Hash:         hashHex,
				RawLen:       uint32(len(raw)),
				StoredLen:    existing.StoredLen,
				Codec:        existing.Codec,
				Deduplicated: true,
			}, nil
		}
		return CAWALPutResult{}, err
	}
	cleanupTmp()
	syncDirBestEffort(filepath.Dir(finalPath))

	s.blobWrites.Add(1)
	s.blobBytesWritten.Add(int64(len(encoded)))
	return CAWALPutResult{
		Hash:      hashHex,
		RawLen:    uint32(len(raw)),
		StoredLen: uint32(len(encoded)),
		Codec:     codec,
	}, nil
}

func (s *CAWALStore) verifyExistingHeaderOnly(blob []byte, expectedSum [32]byte, expectedRawLen uint32) error {
	if len(blob) < CAWALHeaderSize {
		return fmt.Errorf("%w: truncated existing blob", ErrCAWALIntegrityMismatch)
	}
	if binary.LittleEndian.Uint32(blob[0:4]) != CAWALBlobMagic || binary.LittleEndian.Uint16(blob[4:6]) != CAWALBlobVersion {
		return fmt.Errorf("%w: corrupt existing blob header", ErrCAWALIntegrityMismatch)
	}
	if binary.LittleEndian.Uint32(blob[8:12]) != expectedRawLen {
		return fmt.Errorf("%w: raw length mismatch on existing blob", ErrCAWALIntegrityMismatch)
	}
	var hdrSum [32]byte
	copy(hdrSum[:], blob[16:48])
	if hdrSum != expectedSum {
		return fmt.Errorf("%w: hash mismatch on existing blob", ErrCAWALIntegrityMismatch)
	}
	return nil
}

func (s *CAWALStore) statAndVerifyHeader(path string, expectedSum [32]byte, expectedRawLen uint32) (CAWALPutResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return CAWALPutResult{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return CAWALPutResult{}, err
	}
	var hdr [CAWALHeaderSize]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		return CAWALPutResult{}, fmt.Errorf("%w: %v", ErrCAWALIntegrityMismatch, err)
	}
	if err := s.verifyExistingHeaderOnly(hdr[:], expectedSum, expectedRawLen); err != nil {
		return CAWALPutResult{}, err
	}
	payloadLen := binary.LittleEndian.Uint32(hdr[12:16])
	if int64(CAWALHeaderSize)+int64(payloadLen) != st.Size() {
		return CAWALPutResult{}, fmt.Errorf("%w: truncated blob file size=%d want=%d", ErrCAWALIntegrityMismatch, st.Size(), int64(CAWALHeaderSize)+int64(payloadLen))
	}
	return CAWALPutResult{
		RawLen:    expectedRawLen,
		StoredLen: uint32(st.Size()),
		Codec:     hdr[6],
	}, nil
}

// GetBlobVerified reads and verifies a content-addressed WAL blob by its SHA-256 hex digest.
func (s *CAWALStore) GetBlobVerified(ctx context.Context, hashHex string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	norm, err := ValidateSHA256Hex(hashHex)
	if err != nil {
		return nil, err
	}
	rawHash, _ := hex.DecodeString(norm)
	var expectedSum [32]byte
	copy(expectedSum[:], rawHash)

	var blobBytes []byte
	if s.casDir == "" {
		s.memMu.RLock()
		b, ok := s.memBlobs[norm]
		s.memMu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrCAWALBlobMissing, norm)
		}
		blobBytes = b
	} else {
		b, rErr := os.ReadFile(s.blobPath(norm))
		if rErr != nil {
			if errors.Is(rErr, os.ErrNotExist) {
				return nil, fmt.Errorf("%w: %s", ErrCAWALBlobMissing, norm)
			}
			return nil, rErr
		}
		blobBytes = b
	}

	raw, _, err := decodeAndVerifyCAWALBlob(blobBytes, expectedSum)
	if err != nil {
		s.hashFailures.Add(1)
		s.corruptBlobsDetected.Add(1)
		return nil, err
	}
	return raw, nil
}

// HasBlob reports whether a valid blob exists for hashHex.
func (s *CAWALStore) HasBlob(hashHex string) bool {
	norm, err := ValidateSHA256Hex(hashHex)
	if err != nil {
		return false
	}
	if s.casDir == "" {
		s.memMu.RLock()
		_, ok := s.memBlobs[norm]
		s.memMu.RUnlock()
		return ok
	}
	_, err = os.Stat(s.blobPath(norm))
	return err == nil
}

// ManifestPath returns the local path for a timeline's CA-WAL manifest.
func (s *CAWALStore) ManifestPath(tenantID, generation string, epoch int64) string {
	dir := filepath.Join(s.rootDir, tenantID)
	if generation != "" {
		dir = filepath.Join(dir, generation)
	}
	return filepath.Join(dir, fmt.Sprintf("timeline_%d_cawal_manifest.json", epoch))
}

// PublishManifest atomically persists m after verifying every referenced blob is present and valid.
func (s *CAWALStore) PublishManifest(ctx context.Context, m *CAWALManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m == nil || m.TenantID == "" {
		return fmt.Errorf("%w: empty tenant_id", ErrCAWALInvalidBlob)
	}
	for _, ref := range m.Refs {
		if _, err := ValidateSHA256Hex(ref.BlobHash); err != nil {
			return err
		}
		if !s.HasBlob(ref.BlobHash) {
			return fmt.Errorf("%w: manifest references missing blob %s", ErrCAWALBlobMissing, ref.BlobHash)
		}
	}
	if s.faultHook != nil {
		if err := s.faultHook("before_manifest_commit"); err != nil {
			return err
		}
	}
	m.Version = 1
	m.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if s.rootDir == "" {
		return nil
	}
	path := s.ManifestPath(m.TenantID, m.Generation, m.Epoch)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp.%d", path, s.tmpSeq.Add(1))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if s.faultHook != nil {
		if err := s.faultHook("during_manifest_update"); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	syncDirBestEffort(filepath.Dir(path))
	return nil
}

// LoadManifest reads and validates a CAWALManifest from disk.
func (s *CAWALStore) LoadManifest(tenantID, generation string, epoch int64) (*CAWALManifest, error) {
	if s.rootDir == "" {
		return nil, nil
	}
	path := s.ManifestPath(tenantID, generation, epoch)
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if len(raw) > maxCAWALManifestBytes {
		return nil, fmt.Errorf("%w: manifest exceeds %d bytes", ErrCAWALInvalidBlob, maxCAWALManifestBytes)
	}
	var m CAWALManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// GC performs crash-safe, idempotent reachability-based garbage collection of unreferenced
// CA-WAL blobs and stale temporary files older than minAge.
func (s *CAWALStore) GC(ctx context.Context, minAge time.Duration, shouldAbort func() bool) (CAWALGCResult, error) {
	s.gcRuns.Add(1)
	var res CAWALGCResult
	if s.casDir == "" {
		return res, nil
	}
	if shouldAbort != nil && shouldAbort() {
		res.Aborted = true
		return res, nil
	}

	reachable := make(map[string]struct{})
	// 1. Collect all active in-flight pins.
	s.pinMu.Lock()
	for h := range s.pins {
		reachable[h] = struct{}{}
	}
	s.pinMu.Unlock()

	// 2. Mark all blobs reachable from durable CAWAL manifests and V2 layer manifests.
	if err := s.collectReachableFromDisk(ctx, reachable); err != nil {
		return res, err
	}
	res.ReachableBlobs = len(reachable)

	shaRoot := filepath.Join(s.casDir, "sha256")
	buckets, err := os.ReadDir(shaRoot)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return res, nil
		}
		return res, err
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Name() < buckets[j].Name() })

	now := time.Now()
	for _, b := range buckets {
		if !b.IsDir() {
			continue
		}
		if ctx.Err() != nil || (shouldAbort != nil && shouldAbort()) {
			res.Aborted = true
			return res, ctx.Err()
		}
		bucketDir := filepath.Join(shaRoot, b.Name())
		entries, err := os.ReadDir(bucketDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			fullPath := filepath.Join(bucketDir, name)
			info, iErr := e.Info()
			if iErr != nil {
				continue
			}
			if minAge > 0 && now.Sub(info.ModTime()) < minAge {
				continue
			}
			if strings.Contains(name, ".tmp.") {
				if os.Remove(fullPath) == nil {
					res.DeletedTemps++
					res.ReclaimedBytes += info.Size()
				}
				continue
			}
			if !strings.HasSuffix(name, ".walb") {
				continue
			}
			hashHex := strings.TrimSuffix(name, ".walb")
			res.ScannedBlobs++
			if _, ok := reachable[hashHex]; ok {
				continue
			}
			if s.faultHook != nil {
				if err := s.faultHook("during_gc_sweep"); err != nil {
					res.Aborted = true
					return res, err
				}
			}
			// Re-check pin right before unlink to prevent racing with a concurrent writer.
			if s.isPinned(hashHex) {
				continue
			}
			if err := os.Remove(fullPath); err == nil {
				res.DeletedBlobs++
				res.ReclaimedBytes += info.Size()
				s.orphanBlobsCleaned.Add(1)
				s.gcBytesReclaimed.Add(info.Size())
			}
		}
	}
	return res, nil
}

func (s *CAWALStore) collectReachableFromDisk(ctx context.Context, reachable map[string]struct{}) error {
	if s.rootDir == "" {
		return nil
	}
	return filepath.WalkDir(s.rootDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() {
			if d.Name() == "_wal_cas" {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if strings.HasSuffix(name, "_cawal_manifest.json") {
			raw, rErr := os.ReadFile(path)
			if rErr != nil {
				return nil
			}
			var m CAWALManifest
			if json.Unmarshal(raw, &m) == nil {
				for _, ref := range m.Refs {
					if norm, vErr := ValidateSHA256Hex(ref.BlobHash); vErr == nil {
						reachable[norm] = struct{}{}
					}
				}
			}
		} else if strings.HasSuffix(name, "_layer_manifest.json") {
			raw, rErr := os.ReadFile(path)
			if rErr != nil {
				return nil
			}
			var lm TimelineLayerManifest
			if json.Unmarshal(raw, &lm) == nil {
				for _, layer := range lm.Layers {
					for _, h := range layer.CAWALBlobHashes {
						if norm, vErr := ValidateSHA256Hex(h); vErr == nil {
							reachable[norm] = struct{}{}
						}
					}
				}
			}
		}
		return nil
	})
}

func syncDirBestEffort(dir string) {
	df, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = df.Sync()
	_ = df.Close()
}
