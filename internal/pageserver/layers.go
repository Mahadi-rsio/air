package pageserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	LayerMagic      uint32 = 0x4149524C // "AIRL"
	LayerVersion    uint16 = 1
	LayerFooterSize int    = 32
	LayerIndexSize  int    = 48
)

// LayerKind identifies L0 delta, L1 compacted delta, or Image snapshot layers.
type LayerKind uint8

const (
	LayerKindL0    LayerKind = 0
	LayerKindL1    LayerKind = 1
	LayerKindImage LayerKind = 2
)

func (k LayerKind) String() string {
	switch k {
	case LayerKindL0:
		return "L0"
	case LayerKindL1:
		return "L1"
	case LayerKindImage:
		return "IMG"
	default:
		return "UNK"
	}
}

// Entry flags inside a layer file.
const (
	EntryFlagPageImage uint32 = 1 << 0
	EntryFlagWALRecord uint32 = 1 << 1
	EntryFlagWillInit  uint32 = 1 << 2
	EntryFlagRelSize   uint32 = 1 << 3
)

// WALDelta represents a single versioned state entry at a given LSN.
type WALDelta struct {
	LSN       uint64
	Flags     uint32
	PageImage []byte // 8192 bytes when EntryFlagPageImage is set
	WALRecord []byte // raw XLogRecord when EntryFlagWALRecord is set
	RelSize   uint32 // block count when EntryFlagRelSize is set
}

// InMemoryLayer buffers recent WAL deltas and FPIs before flushing to an L0 layer.
type InMemoryLayer struct {
	mu          sync.RWMutex
	StartLSN    uint64
	EndLSN      uint64
	ApproxBytes int64
	entries     map[Key][]WALDelta
}

// NewInMemoryLayer creates an empty ephemeral InMemoryLayer starting at startLSN.
func NewInMemoryLayer(startLSN uint64) *InMemoryLayer {
	return &InMemoryLayer{
		StartLSN: startLSN,
		EndLSN:   startLSN,
		entries:  make(map[Key][]WALDelta),
	}
}

// Put appends a delta entry for key at delta.LSN.
func (m *InMemoryLayer) Put(key Key, delta WALDelta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.StartLSN == 0 || delta.LSN < m.StartLSN {
		m.StartLSN = delta.LSN
	}
	if delta.LSN >= m.EndLSN {
		m.EndLSN = delta.LSN + 1
	}
	m.entries[key] = append(m.entries[key], delta)
	m.ApproxBytes += int64(len(delta.PageImage) + len(delta.WALRecord) + 32)
}

// Lookup returns all deltas for key with LSN <= lsn in ascending LSN order.
func (m *InMemoryLayer) Lookup(key Key, lsn uint64) []WALDelta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := m.entries[key]
	if len(list) == 0 {
		return nil
	}
	var out []WALDelta
	for _, d := range list {
		if lsn == 0 || d.LSN <= lsn {
			out = append(out, d)
		}
	}
	return out
}

// SnapshotAll returns a sorted slice of all (Key, WALDelta) pairs for flushing to L0.
func (m *InMemoryLayer) SnapshotAll() ([]LayerRecord, uint64, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var recs []LayerRecord
	for k, deltas := range m.entries {
		for _, d := range deltas {
			recs = append(recs, LayerRecord{Key: k, Delta: d})
		}
	}
	sort.Slice(recs, func(i, j int) bool {
		if c := recs[i].Key.Compare(recs[j].Key); c != 0 {
			return c < 0
		}
		return recs[i].Delta.LSN < recs[j].Delta.LSN
	})
	return recs, m.StartLSN, m.EndLSN
}

// LayerRecord is a flattened (Key, WALDelta) pair stored inside an immutable layer file.
type LayerRecord struct {
	Key   Key
	Delta WALDelta
}

// LayerIndexEntry is a 48-byte sorted index record in a layer file's footer index.
type LayerIndexEntry struct {
	Key    Key
	LSN    uint64
	Offset uint64
	Length uint32
	Flags  uint32
}

// LayerFooter is the 32-byte trailer at EOF of every immutable layer file.
type LayerFooter struct {
	Magic       uint32
	Version     uint16
	Kind        LayerKind
	Encrypted   bool
	IndexOffset uint64
	IndexLen    uint32
	EntryCount  uint32
	EndLSN      uint64
}

// LayerMeta describes an immutable layer file in the LayerMap.
type LayerMeta struct {
	TenantID          string    `json:"tenant_id"`
	Epoch             int64     `json:"epoch"`
	Kind              LayerKind `json:"kind"`
	Seq               uint64    `json:"seq,omitempty"`
	FileName          string    `json:"file_name"`
	R2Key             string    `json:"r2_key"`
	SHA256            string    `json:"sha256,omitempty"`
	AADKey            string    `json:"aad_key,omitempty"`
	StartKey          Key       `json:"start_key"`
	EndKey            Key       `json:"end_key"`
	StartLSN          uint64    `json:"start_lsn"`
	EndLSN            uint64    `json:"end_lsn"`
	FileSize          int64     `json:"file_size"`
	Encrypted         bool      `json:"encrypted"`
	RemotelyAvailable bool      `json:"remotely_available,omitempty"`
}

func (m LayerMeta) aadKey() string {
	if m.AADKey != "" {
		return m.AADKey
	}
	return m.R2Key
}

// EncodeLayerBytes serializes sorted LayerRecords into our range-readable immutable layer format.
// When kms != nil, each payload chunk and the footer index are sealed individually with tenantID's DEK,
// allowing HTTP Range reads of the footer/index and individual entries without downloading the whole file.
func EncodeLayerBytes(tenantID string, epoch int64, r2Key string, kind LayerKind, endLSN uint64, recs []LayerRecord, kms *crypto.EnvelopeKMS) ([]byte, error) {
	sort.Slice(recs, func(i, j int) bool {
		if c := recs[i].Key.Compare(recs[j].Key); c != 0 {
			return c < 0
		}
		return recs[i].Delta.LSN < recs[j].Delta.LSN
	})

	encrypted := kms != nil
	var buf bytes.Buffer
	index := make([]LayerIndexEntry, 0, len(recs))

	for i, rec := range recs {
		payload := encodeDeltaPayload(rec.Delta)
		if encrypted {
			var err error
			payload, err = kms.EncryptObject(tenantID, epoch, fmt.Sprintf("%s#e%d", r2Key, i), payload)
			if err != nil {
				return nil, err
			}
		}
		off := uint64(buf.Len())
		buf.Write(payload)
		index = append(index, LayerIndexEntry{
			Key:    rec.Key,
			LSN:    rec.Delta.LSN,
			Offset: off,
			Length: uint32(len(payload)),
			Flags:  rec.Delta.Flags,
		})
	}

	idxBuf := make([]byte, len(index)*LayerIndexSize)
	for i, e := range index {
		off := i * LayerIndexSize
		e.Key.Encode(idxBuf[off : off+24])
		binary.LittleEndian.PutUint64(idxBuf[off+24:off+32], e.LSN)
		binary.LittleEndian.PutUint64(idxBuf[off+32:off+40], e.Offset)
		binary.LittleEndian.PutUint32(idxBuf[off+40:off+44], e.Length)
		binary.LittleEndian.PutUint32(idxBuf[off+44:off+48], e.Flags)
	}

	if encrypted {
		var err error
		idxBuf, err = kms.EncryptObject(tenantID, epoch, r2Key+"#idx", idxBuf)
		if err != nil {
			return nil, err
		}
	}

	indexOff := uint64(buf.Len())
	buf.Write(idxBuf)

	var footer [LayerFooterSize]byte
	binary.LittleEndian.PutUint32(footer[0:4], LayerMagic)
	binary.LittleEndian.PutUint16(footer[4:6], LayerVersion)
	footer[6] = byte(kind)
	if encrypted {
		footer[7] = 1
	}
	binary.LittleEndian.PutUint64(footer[8:16], indexOff)
	binary.LittleEndian.PutUint32(footer[16:20], uint32(len(idxBuf)))
	binary.LittleEndian.PutUint32(footer[20:24], uint32(len(index)))
	binary.LittleEndian.PutUint64(footer[24:32], endLSN)
	buf.Write(footer[:])

	return buf.Bytes(), nil
}

func encodeDeltaPayload(d WALDelta) []byte {
	// Format: [4B relSize][4B pageLen][page][4B walLen][wal]
	pLen := len(d.PageImage)
	wLen := len(d.WALRecord)
	out := make([]byte, 12+pLen+wLen)
	binary.LittleEndian.PutUint32(out[0:4], d.RelSize)
	binary.LittleEndian.PutUint32(out[4:8], uint32(pLen))
	copy(out[8:8+pLen], d.PageImage)
	binary.LittleEndian.PutUint32(out[8+pLen:12+pLen], uint32(wLen))
	copy(out[12+pLen:], d.WALRecord)
	return out
}

func decodeDeltaPayload(raw []byte, lsn uint64, flags uint32) (WALDelta, error) {
	if len(raw) < 12 {
		return WALDelta{}, errors.New("pageserver: truncated delta payload")
	}
	relSize := binary.LittleEndian.Uint32(raw[0:4])
	pLen := int(binary.LittleEndian.Uint32(raw[4:8]))
	if 12+pLen > len(raw) {
		return WALDelta{}, errors.New("pageserver: invalid page length in delta payload")
	}
	var page []byte
	if pLen > 0 {
		page = append([]byte(nil), raw[8:8+pLen]...)
	}
	wLen := int(binary.LittleEndian.Uint32(raw[8+pLen : 12+pLen]))
	if 12+pLen+wLen > len(raw) {
		return WALDelta{}, errors.New("pageserver: invalid WAL length in delta payload")
	}
	var wal []byte
	if wLen > 0 {
		wal = append([]byte(nil), raw[12+pLen:12+pLen+wLen]...)
	}
	return WALDelta{
		LSN:       lsn,
		Flags:     flags,
		PageImage: page,
		WALRecord: wal,
		RelSize:   relSize,
	}, nil
}

// DecodeLayerFooter parses the 32-byte footer at the end of a layer file.
func DecodeLayerFooter(b []byte) (LayerFooter, error) {
	if len(b) != LayerFooterSize {
		return LayerFooter{}, errors.New("pageserver: invalid layer footer size")
	}
	magic := binary.LittleEndian.Uint32(b[0:4])
	ver := binary.LittleEndian.Uint16(b[4:6])
	if magic != LayerMagic || ver != LayerVersion {
		return LayerFooter{}, fmt.Errorf("pageserver: bad layer footer magic=0x%x ver=%d", magic, ver)
	}
	return LayerFooter{
		Magic:       magic,
		Version:     ver,
		Kind:        LayerKind(b[6]),
		Encrypted:   b[7] == 1,
		IndexOffset: binary.LittleEndian.Uint64(b[8:16]),
		IndexLen:    binary.LittleEndian.Uint32(b[16:20]),
		EntryCount:  binary.LittleEndian.Uint32(b[20:24]),
		EndLSN:      binary.LittleEndian.Uint64(b[24:32]),
	}, nil
}

// DecodeLayerIndex decrypts (if needed) and parses the sorted LayerIndexEntry array.
func DecodeLayerIndex(raw []byte, meta LayerMeta, count uint32, kms *crypto.EnvelopeKMS) ([]LayerIndexEntry, error) {
	data := raw
	if meta.Encrypted {
		if kms == nil {
			return nil, errors.New("pageserver: encrypted layer requires KMS")
		}
		var err error
		data, err = kms.DecryptObject(meta.TenantID, meta.Epoch, meta.aadKey()+"#idx", raw)
		if err != nil {
			return nil, err
		}
	}
	if len(data) != int(count)*LayerIndexSize {
		return nil, fmt.Errorf("pageserver: index size mismatch got=%d want=%d", len(data), int(count)*LayerIndexSize)
	}
	out := make([]LayerIndexEntry, count)
	for i := 0; i < int(count); i++ {
		off := i * LayerIndexSize
		out[i] = LayerIndexEntry{
			Key:    DecodeKey(data[off : off+24]),
			LSN:    binary.LittleEndian.Uint64(data[off+24 : off+32]),
			Offset: binary.LittleEndian.Uint64(data[off+32 : off+40]),
			Length: binary.LittleEndian.Uint32(data[off+40 : off+44]),
			Flags:  binary.LittleEndian.Uint32(data[off+44 : off+48]),
		}
	}
	return out, nil
}

type layerDownload struct {
	done chan struct{}
	err  error
}

// LayerStore persists and range-reads immutable layer files across local disk and R2.
type LayerStore struct {
	localDir           string
	s3c                *s3.Client
	bucket             string
	kms                *crypto.EnvelopeKMS
	cache              *DiskCache
	multipartThreshold int64
	multipartPartSize  int64
	idxMu              sync.RWMutex
	indexCache         map[string][]LayerIndexEntry
	dlMu               sync.Mutex
	dlFlight           map[string]*layerDownload
	metricsMu          sync.Mutex
	lastMetrics        UploadMetrics
}

// NewLayerStore creates a LayerStore backed by localDir, optional S3/R2, optional KMS, and optional DiskCache.
func NewLayerStore(localDir string, s3c *s3.Client, bucket string, kms *crypto.EnvelopeKMS, cache *DiskCache) *LayerStore {
	return &LayerStore{
		localDir:           localDir,
		s3c:                s3c,
		bucket:             bucket,
		kms:                kms,
		cache:              cache,
		multipartThreshold: DefaultMultipartThreshold,
		multipartPartSize:  DefaultMultipartPartSize,
		indexCache:         make(map[string][]LayerIndexEntry),
		dlFlight:           make(map[string]*layerDownload),
	}
}

// SetMultipartConfig overrides the multipart upload threshold and part size (for testing/tuning).
func (s *LayerStore) SetMultipartConfig(threshold, partSize int64) {
	if threshold > 0 {
		s.multipartThreshold = threshold
	}
	if partSize > 0 {
		s.multipartPartSize = partSize
	}
}

// LastUploadMetrics returns the metrics captured by the most recent CAU upload.
func (s *LayerStore) LastUploadMetrics() UploadMetrics {
	s.metricsMu.Lock()
	defer s.metricsMu.Unlock()
	return s.lastMetrics
}

func (s *LayerStore) recordMetrics(m UploadMetrics) {
	s.metricsMu.Lock()
	s.lastMetrics = m
	s.metricsMu.Unlock()
}

// WriteLayer encodes recs into an immutable layer file, finalizes it atomically on disk,
// and uploads it to R2 using content-addressed storage if configured.
func (s *LayerStore) WriteLayer(ctx context.Context, meta LayerMeta, recs []LayerRecord) (LayerMeta, error) {
	meta.Encrypted = s.kms != nil
	if meta.Encrypted && meta.AADKey == "" {
		if meta.R2Key != "" {
			meta.AADKey = meta.R2Key
		} else {
			meta.AADKey = fmt.Sprintf("tenants/%s/timeline_%d/layers/%s", meta.TenantID, meta.Epoch, meta.FileName)
		}
	}

	data, err := EncodeLayerBytes(meta.TenantID, meta.Epoch, meta.aadKey(), meta.Kind, meta.EndLSN, recs, s.kms)
	if err != nil {
		return meta, err
	}
	meta.FileSize = int64(len(data))
	if len(recs) > 0 {
		meta.StartKey = recs[0].Key
		meta.EndKey = recs[len(recs)-1].Key
	}

	var localPath string
	var cleanupTemp bool
	if s.localDir != "" {
		localPath = filepath.Join(s.localDir, meta.TenantID, meta.FileName)
		if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
			return meta, err
		}
		tmp := localPath + ".tmp"
		if err := os.WriteFile(tmp, data, 0o600); err != nil {
			return meta, err
		}
		if err := os.Rename(tmp, localPath); err != nil {
			_ = os.Remove(tmp)
			return meta, err
		}
	} else if s.s3c != nil && s.bucket != "" {
		tmpFile, err := os.CreateTemp("", "air-layer-*.layer")
		if err != nil {
			return meta, err
		}
		localPath = tmpFile.Name()
		cleanupTemp = true
		if _, err := tmpFile.Write(data); err != nil {
			_ = tmpFile.Close()
			_ = os.Remove(localPath)
			return meta, err
		}
		if err := tmpFile.Close(); err != nil {
			_ = os.Remove(localPath)
			return meta, err
		}
	}
	if cleanupTemp {
		defer os.Remove(localPath)
	}

	if s.s3c != nil && s.bucket != "" {
		meta, err = s.UploadLayerFile(ctx, meta, localPath)
		if err != nil {
			return meta, err
		}
	} else {
		sum := sha256.Sum256(data)
		meta.SHA256 = hex.EncodeToString(sum[:])
		if cauKey, kErr := ContentAddressedLayerKey(meta.TenantID, meta.SHA256); kErr == nil {
			meta.R2Key = cauKey
		}
	}

	if s.cache != nil {
		_ = s.cache.Put(meta.R2Key, data)
	}
	return meta, nil
}

// UploadLayerFile performs an idempotent, integrity-verified Content-Addressed Upload (CAU)
// of a finalized immutable local layer file to R2 without loading the file into memory.
func (s *LayerStore) UploadLayerFile(ctx context.Context, meta LayerMeta, localPath string) (LayerMeta, error) {
	totalStart := time.Now()
	var metrics UploadMetrics

	info, err := os.Stat(localPath)
	if err != nil {
		return meta, fmt.Errorf("%w: %v", ErrLayerNotFinalized, err)
	}
	if !info.Mode().IsRegular() || info.Size() < int64(LayerFooterSize) {
		return meta, fmt.Errorf("%w: size=%d", ErrLayerNotFinalized, info.Size())
	}

	f, err := os.Open(localPath)
	if err != nil {
		return meta, err
	}
	defer f.Close()

	// Verify the layer file has a valid finalized footer before hashing/uploading.
	var footerBuf [LayerFooterSize]byte
	if _, err := f.ReadAt(footerBuf[:], info.Size()-int64(LayerFooterSize)); err != nil {
		return meta, fmt.Errorf("%w: read footer: %v", ErrLayerNotFinalized, err)
	}
	if _, err := DecodeLayerFooter(footerBuf[:]); err != nil {
		return meta, fmt.Errorf("%w: %v", ErrLayerNotFinalized, err)
	}

	hashStart := time.Now()
	sha256Hex, size, err := StreamSHA256(f)
	metrics.HashDuration = time.Since(hashStart)
	if err != nil {
		return meta, err
	}
	sha256B64, err := sha256HexToBase64(sha256Hex)
	if err != nil {
		return meta, err
	}

	cauKey, err := ContentAddressedLayerKey(meta.TenantID, sha256Hex)
	if err != nil {
		return meta, err
	}

	meta.SHA256 = sha256Hex
	meta.FileSize = size
	meta.R2Key = cauKey
	metrics.R2Key = cauKey
	metrics.SHA256 = sha256Hex
	metrics.FileSize = size

	if s.s3c == nil || s.bucket == "" {
		metrics.TotalDuration = time.Since(totalStart)
		s.recordMetrics(metrics)
		return meta, nil
	}

	// Step 4: Check whether the content-addressed object already exists in R2.
	dedupStart := time.Now()
	exists, err := s.verifyRemoteObject(ctx, cauKey, size, sha256Hex, sha256B64, &metrics)
	metrics.DedupCheckDuration = time.Since(dedupStart)
	if err != nil {
		return meta, err
	}
	if exists {
		metrics.Deduplicated = true
		metrics.TotalDuration = time.Since(totalStart)
		s.recordMetrics(metrics)
		meta.RemotelyAvailable = true
		return meta, nil
	}

	// Step 6: Upload the layer (single-part or multipart) with conditional creation when supported.
	uploadStart := time.Now()
	raced, err := s.uploadContentAddressed(ctx, f, cauKey, size, sha256Hex, sha256B64, &metrics)
	metrics.UploadDuration = time.Since(uploadStart)
	if err != nil {
		return meta, err
	}
	if raced {
		metrics.Deduplicated = true
	}

	// Step 7: Verify the uploaded (or raced) remote object before returning durable reference.
	verifyStart := time.Now()
	verified, err := s.verifyRemoteObject(ctx, cauKey, size, sha256Hex, sha256B64, &metrics)
	metrics.VerifyDuration = time.Since(verifyStart)
	if err != nil {
		return meta, err
	}
	if !verified {
		return meta, fmt.Errorf("%w: %s", ErrRemoteObjectMissing, cauKey)
	}

	metrics.TotalDuration = time.Since(totalStart)
	s.recordMetrics(metrics)
	meta.RemotelyAvailable = true
	return meta, nil
}

func (s *LayerStore) verifyRemoteObject(ctx context.Context, key string, expectedSize int64, expectedSHA256Hex, expectedSHA256B64 string, metrics *UploadMetrics) (bool, error) {
	metrics.HeadRequests++
	headOut, err := s.s3c.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		if isNotFoundS3Err(err) {
			return false, nil
		}
		return false, err
	}

	if headOut.ContentLength == nil || *headOut.ContentLength != expectedSize {
		got := int64(-1)
		if headOut.ContentLength != nil {
			got = *headOut.ContentLength
		}
		return false, fmt.Errorf("%w: key=%s got=%d want=%d", ErrLayerSizeMismatch, key, got, expectedSize)
	}

	verifiedChecksum := false
	if headOut.ChecksumSHA256 != nil && *headOut.ChecksumSHA256 != "" {
		if *headOut.ChecksumSHA256 != expectedSHA256B64 {
			return false, fmt.Errorf("%w: key=%s provider checksum mismatch", ErrLayerChecksumMismatch, key)
		}
		verifiedChecksum = true
	}
	if metaSHA, ok := headOut.Metadata[metaKeySHA256]; ok && metaSHA != "" {
		if strings.ToLower(metaSHA) != expectedSHA256Hex {
			return false, fmt.Errorf("%w: key=%s metadata sha256=%s want=%s", ErrLayerChecksumMismatch, key, metaSHA, expectedSHA256Hex)
		}
		verifiedChecksum = true
	}

	if !verifiedChecksum {
		// Provider returned neither ChecksumSHA256 nor air-sha256 metadata; stream-hash remote object directly.
		resp, getErr := s.s3c.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if getErr != nil {
			return false, getErr
		}
		gotSHA, gotSize, hashErr := StreamSHA256(resp.Body)
		_ = resp.Body.Close()
		if hashErr != nil {
			return false, hashErr
		}
		if gotSize != expectedSize {
			return false, fmt.Errorf("%w: key=%s streamed=%d want=%d", ErrLayerSizeMismatch, key, gotSize, expectedSize)
		}
		if gotSHA != expectedSHA256Hex {
			return false, fmt.Errorf("%w: key=%s streamed sha256=%s want=%s", ErrLayerChecksumMismatch, key, gotSHA, expectedSHA256Hex)
		}
	}

	return true, nil
}

func (s *LayerStore) uploadContentAddressed(ctx context.Context, f *os.File, key string, size int64, sha256Hex, sha256B64 string, metrics *UploadMetrics) (raced bool, err error) {
	objMeta := map[string]string{
		metaKeySHA256: sha256Hex,
		metaKeySize:   strconv.FormatInt(size, 10),
	}

	if size <= s.multipartThreshold {
		metrics.PutRequests++
		_, err := s.s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket:            aws.String(s.bucket),
			Key:               aws.String(key),
			Body:              io.NewSectionReader(f, 0, size),
			ContentLength:     aws.Int64(size),
			ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
			ChecksumSHA256:    aws.String(sha256B64),
			Metadata:          objMeta,
			IfNoneMatch:       aws.String("*"),
		})
		if err != nil {
			if isPreconditionS3Err(err) {
				return true, nil
			}
			return false, err
		}
		metrics.BytesTransferred += size
		return false, nil
	}

	// Multipart upload for large layers using streaming io.NewSectionReader per part.
	createOut, err := s.s3c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(s.bucket),
		Key:               aws.String(key),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		Metadata:          objMeta,
	})
	if err != nil {
		return false, err
	}
	uploadID := aws.ToString(createOut.UploadId)
	abortUpload := func() {
		if uploadID == "" {
			return
		}
		abortCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = s.s3c.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(s.bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
	}

	partSize := s.multipartPartSize
	if partSize <= 0 {
		partSize = DefaultMultipartPartSize
	}
	var completedParts []types.CompletedPart
	var partNum int32 = 1
	for off := int64(0); off < size; off += partSize {
		n := partSize
		if off+n > size {
			n = size - off
		}
		metrics.PutRequests++
		metrics.MultipartParts++
		partOut, partErr := s.s3c.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(key),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(partNum),
			Body:          io.NewSectionReader(f, off, n),
			ContentLength: aws.Int64(n),
		})
		if partErr != nil {
			abortUpload()
			return false, partErr
		}
		metrics.BytesTransferred += n
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       partOut.ETag,
			PartNumber: aws.Int32(partNum),
		})
		partNum++
	}

	_, compErr := s.s3c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
		IfNoneMatch: aws.String("*"),
	})
	if compErr != nil {
		abortUpload()
		if isPreconditionS3Err(compErr) {
			return true, nil
		}
		return false, compErr
	}
	return false, nil
}

func isNotFoundS3Err(err error) bool {
	if err == nil {
		return false
	}
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "NoSuchKey") || strings.Contains(msg, "404") || strings.Contains(msg, "NotFound")
}

func isPreconditionS3Err(err error) bool {
	if err == nil {
		return false
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusPreconditionFailed {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "412") || strings.Contains(msg, "PreconditionFailed")
}

const layerCacheChunkSize int64 = 4 * 1024 * 1024 // 4 MiB chunks for Neon-style on-demand R2 caching

func (s *LayerStore) readRangeLocalOrR2Direct(ctx context.Context, meta LayerMeta, offset, length int64) ([]byte, error) {
	if s.cache != nil {
		if b, ok := s.cache.GetRange(meta.R2Key, offset, length); ok {
			return b, nil
		}
	}
	if s.localDir != "" {
		localPath := filepath.Join(s.localDir, meta.TenantID, meta.FileName)
		if f, err := os.Open(localPath); err == nil {
			defer f.Close()
			buf := make([]byte, length)
			n, err := f.ReadAt(buf, offset)
			if err == nil || (errors.Is(err, io.EOF) && int64(n) == length) {
				return buf[:n], nil
			}
		}
	}
	if s.s3c == nil || s.bucket == "" {
		return nil, fmt.Errorf("pageserver: layer %s not found locally and R2 not configured", meta.R2Key)
	}
	rng := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	resp, err := s.s3c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(meta.R2Key),
		Range:  aws.String(rng),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := validateRangeResponseSize(resp.ContentRange, meta.FileSize); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != length {
		return nil, fmt.Errorf("%w: range read got=%d want=%d", ErrLayerSizeMismatch, len(data), length)
	}
	return data, nil
}

func validateRangeResponseSize(contentRange *string, expectedTotal int64) error {
	if expectedTotal <= 0 || contentRange == nil {
		return nil
	}
	// Format: "bytes start-end/total" or "bytes start-end/*"
	if idx := strings.LastIndexByte(*contentRange, '/'); idx >= 0 {
		totStr := (*contentRange)[idx+1:]
		if totStr == "*" {
			return nil
		}
		if tot, err := strconv.ParseInt(totStr, 10, 64); err == nil && tot != expectedTotal {
			return fmt.Errorf("%w: remote total=%d want=%d", ErrLayerSizeMismatch, tot, expectedTotal)
		}
	}
	return nil
}

func (s *LayerStore) ensureChunkCached(ctx context.Context, meta LayerMeta, chunkIdx int64) (string, error) {
	chunkKey := fmt.Sprintf("%s@c%d", meta.R2Key, chunkIdx)
	if s.cache == nil || s.s3c == nil || s.bucket == "" || meta.FileSize <= 0 || layerCacheChunkSize > s.cache.maxBytes {
		return chunkKey, nil
	}
	chunkStart := chunkIdx * layerCacheChunkSize
	if chunkStart >= meta.FileSize {
		return chunkKey, fmt.Errorf("pageserver: chunk %d out of range for %s (size=%d)", chunkIdx, meta.R2Key, meta.FileSize)
	}
	chunkEnd := chunkStart + layerCacheChunkSize
	if chunkEnd > meta.FileSize {
		chunkEnd = meta.FileSize
	}

	// Fast path: already in disk cache.
	if _, ok := s.cache.GetRange(chunkKey, 0, 1); ok {
		return chunkKey, nil
	}

	s.dlMu.Lock()
	if s.dlFlight == nil {
		s.dlFlight = make(map[string]*layerDownload)
	}
	if f, ok := s.dlFlight[chunkKey]; ok {
		s.dlMu.Unlock()
		select {
		case <-f.done:
			return chunkKey, f.err
		case <-ctx.Done():
			return chunkKey, ctx.Err()
		}
	}
	f := &layerDownload{done: make(chan struct{})}
	s.dlFlight[chunkKey] = f
	s.dlMu.Unlock()

	var err error
	rng := fmt.Sprintf("bytes=%d-%d", chunkStart, chunkEnd-1)
	resp, getErr := s.s3c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(meta.R2Key),
		Range:  aws.String(rng),
	})
	if getErr != nil {
		err = getErr
	} else {
		if vrErr := validateRangeResponseSize(resp.ContentRange, meta.FileSize); vrErr != nil {
			_ = resp.Body.Close()
			err = vrErr
		} else {
			data, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr != nil {
				err = readErr
			} else if int64(len(data)) != chunkEnd-chunkStart {
				err = fmt.Errorf("%w: chunk %d got=%d want=%d", ErrLayerSizeMismatch, chunkIdx, len(data), chunkEnd-chunkStart)
			} else {
				err = s.cache.Put(chunkKey, data)
			}
		}
	}

	s.dlMu.Lock()
	f.err = err
	close(f.done)
	delete(s.dlFlight, chunkKey)
	s.dlMu.Unlock()
	return chunkKey, err
}

// ReadRange fetches [offset, offset+length) from disk cache, localDir, or chunked R2 cache.
func (s *LayerStore) ReadRange(ctx context.Context, meta LayerMeta, offset, length int64) ([]byte, error) {
	if length <= 0 {
		return nil, nil
	}
	if s.cache != nil {
		if b, ok := s.cache.GetRange(meta.R2Key, offset, length); ok {
			return b, nil
		}
	}
	if s.localDir != "" {
		localPath := filepath.Join(s.localDir, meta.TenantID, meta.FileName)
		if f, err := os.Open(localPath); err == nil {
			defer f.Close()
			buf := make([]byte, length)
			n, err := f.ReadAt(buf, offset)
			if err == nil || (errors.Is(err, io.EOF) && int64(n) == length) {
				return buf[:n], nil
			}
		}
	}
	if s.s3c == nil || s.bucket == "" {
		return nil, fmt.Errorf("pageserver: layer %s not found locally and R2 not configured", meta.R2Key)
	}
	if s.cache != nil && meta.FileSize > 0 && layerCacheChunkSize <= s.cache.maxBytes {
		end := offset + length
		if end > meta.FileSize {
			end = meta.FileSize
		}
		firstChunk := offset / layerCacheChunkSize
		lastChunk := (end - 1) / layerCacheChunkSize
		if firstChunk == lastChunk {
			chunkKey, err := s.ensureChunkCached(ctx, meta, firstChunk)
			if err == nil {
				if b, ok := s.cache.GetRange(chunkKey, offset-firstChunk*layerCacheChunkSize, end-offset); ok {
					return b, nil
				}
			}
		} else {
			out := make([]byte, 0, end-offset)
			okAll := true
			for ci := firstChunk; ci <= lastChunk; ci++ {
				chunkKey, err := s.ensureChunkCached(ctx, meta, ci)
				if err != nil {
					okAll = false
					break
				}
				cStart := ci * layerCacheChunkSize
				readOff := int64(0)
				if offset > cStart {
					readOff = offset - cStart
				}
				cEnd := cStart + layerCacheChunkSize
				if end < cEnd {
					cEnd = end
				}
				readLen := cEnd - (cStart + readOff)
				b, ok := s.cache.GetRange(chunkKey, readOff, readLen)
				if !ok {
					okAll = false
					break
				}
				out = append(out, b...)
			}
			if okAll {
				return out, nil
			}
		}
	}
	return s.readRangeLocalOrR2Direct(ctx, meta, offset, length)
}

// LookupKeyInLayer reads only the footer, index (cached in memory per layer), and matching entry ranges for key <= lsn.
func (s *LayerStore) LookupKeyInLayer(ctx context.Context, meta LayerMeta, key Key, lsn uint64) ([]WALDelta, error) {
	if meta.FileSize < int64(LayerFooterSize) {
		return nil, errors.New("pageserver: layer file smaller than footer")
	}
	s.idxMu.RLock()
	entries, cached := s.indexCache[meta.R2Key]
	s.idxMu.RUnlock()

	if !cached {
		footerRaw, err := s.readRangeLocalOrR2Direct(ctx, meta, meta.FileSize-int64(LayerFooterSize), int64(LayerFooterSize))
		if err != nil {
			return nil, err
		}
		footer, err := DecodeLayerFooter(footerRaw)
		if err != nil {
			return nil, err
		}
		if footer.EntryCount == 0 {
			s.idxMu.Lock()
			if s.indexCache == nil {
				s.indexCache = make(map[string][]LayerIndexEntry)
			}
			s.indexCache[meta.R2Key] = []LayerIndexEntry{}
			s.idxMu.Unlock()
			return nil, nil
		}
		idxRaw, err := s.readRangeLocalOrR2Direct(ctx, meta, int64(footer.IndexOffset), int64(footer.IndexLen))
		if err != nil {
			return nil, err
		}
		entries, err = DecodeLayerIndex(idxRaw, meta, footer.EntryCount, s.kms)
		if err != nil {
			return nil, err
		}
		s.idxMu.Lock()
		if s.indexCache == nil {
			s.indexCache = make(map[string][]LayerIndexEntry)
		}
		s.indexCache[meta.R2Key] = entries
		s.idxMu.Unlock()
	}
	if len(entries) == 0 {
		return nil, nil
	}

	// Binary search for first index >= key.
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].Key.Compare(key) >= 0
	})
	var out []WALDelta
	for i := start; i < len(entries) && entries[i].Key.Compare(key) == 0; i++ {
		e := entries[i]
		if lsn != 0 && e.LSN > lsn {
			break
		}
		raw, err := s.ReadRange(ctx, meta, int64(e.Offset), int64(e.Length))
		if err != nil {
			return nil, err
		}
		if meta.Encrypted {
			raw, err = s.kms.DecryptObject(meta.TenantID, meta.Epoch, fmt.Sprintf("%s#e%d", meta.aadKey(), i), raw)
			if err != nil {
				return nil, err
			}
		}
		delta, err := decodeDeltaPayload(raw, e.LSN, e.Flags)
		if err != nil {
			return nil, err
		}
		out = append(out, delta)
	}
	return out, nil
}

// DownloadLayer streams a layer from R2 to dstPath (or verifies in-memory if dstPath == ""),
// verifying expected byte size and SHA-256 checksum (when known or content-addressed) before committing.
func (s *LayerStore) DownloadLayer(ctx context.Context, meta LayerMeta, dstPath string) error {
	if s.s3c == nil || s.bucket == "" {
		return fmt.Errorf("pageserver: R2 not configured for %s", meta.R2Key)
	}
	expectedSHA := meta.SHA256
	if expectedSHA == "" && IsContentAddressedLayerKey(meta.R2Key) {
		parts := strings.Split(meta.R2Key, "/")
		expectedSHA = parts[len(parts)-1]
	}

	resp, err := s.s3c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(meta.R2Key),
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var dstFile *os.File
	var tmpPath string
	if dstPath != "" {
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			return err
		}
		tmpPath = dstPath + ".tmp"
		dstFile, err = os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		defer func() {
			if dstFile != nil {
				_ = dstFile.Close()
				_ = os.Remove(tmpPath)
			}
		}()
	}

	h := sha256.New()
	var w io.Writer = h
	if dstFile != nil {
		w = io.MultiWriter(h, dstFile)
	}
	buf := make([]byte, streamHashBufSize)
	n, err := io.CopyBuffer(w, resp.Body, buf)
	if err != nil {
		return err
	}
	if meta.FileSize > 0 && n != meta.FileSize {
		return fmt.Errorf("%w: downloaded=%d want=%d", ErrLayerSizeMismatch, n, meta.FileSize)
	}
	gotSHA := hex.EncodeToString(h.Sum(nil))
	if expectedSHA != "" && gotSHA != strings.ToLower(expectedSHA) {
		return fmt.Errorf("%w: downloaded sha256=%s want=%s", ErrLayerChecksumMismatch, gotSHA, expectedSHA)
	}

	if dstFile != nil {
		if err := dstFile.Close(); err != nil {
			return err
		}
		dstFile = nil
		if err := os.Rename(tmpPath, dstPath); err != nil {
			_ = os.Remove(tmpPath)
			return err
		}
	}
	return nil
}

// PublishLayerManifest atomically persists the timeline's layer manifest to local disk (if configured)
// and R2 (using ETag CAS when updating an existing manifest).
func (s *LayerStore) PublishLayerManifest(ctx context.Context, tenantID string, epoch int64, layers []LayerMeta, maxSeq uint64) error {
	manifest := TimelineLayerManifest{
		TenantID:  tenantID,
		Epoch:     epoch,
		Version:   1,
		MaxSeq:    maxSeq,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Layers:    layers,
	}
	body, err := json.MarshalIndent(&manifest, "", "  ")
	if err != nil {
		return err
	}

	if s.localDir != "" {
		localManifest := filepath.Join(s.localDir, tenantID, fmt.Sprintf("timeline_%d_layer_manifest.json", epoch))
		if err := os.MkdirAll(filepath.Dir(localManifest), 0o755); err != nil {
			return err
		}
		tmp := localManifest + ".tmp"
		if err := os.WriteFile(tmp, body, 0o600); err != nil {
			return err
		}
		if err := os.Rename(tmp, localManifest); err != nil {
			_ = os.Remove(tmp)
			return err
		}
	}

	if s.s3c == nil || s.bucket == "" {
		return nil
	}

	key := LayerManifestR2Key(tenantID, epoch)
	for attempt := 0; attempt < 3; attempt++ {
		var expectedETag string
		headOut, headErr := s.s3c.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if headErr != nil {
			if !isNotFoundS3Err(headErr) {
				return headErr
			}
		} else if headOut.ETag != nil {
			expectedETag = *headOut.ETag
		}

		putIn := &s3.PutObjectInput{
			Bucket:      aws.String(s.bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(body),
			ContentType: aws.String("application/json"),
		}
		if expectedETag == "" {
			putIn.IfNoneMatch = aws.String("*")
		} else {
			putIn.IfMatch = aws.String(expectedETag)
		}
		if _, err := s.s3c.PutObject(ctx, putIn); err != nil {
			if isPreconditionS3Err(err) && attempt < 2 {
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("pageserver: manifest publish CAS conflict for %s", key)
}

// LoadLayerManifest loads the published TimelineLayerManifest from R2 (or local disk if R2 is not configured).
func (s *LayerStore) LoadLayerManifest(ctx context.Context, tenantID string, epoch int64) (*TimelineLayerManifest, error) {
	if s.s3c != nil && s.bucket != "" {
		key := LayerManifestR2Key(tenantID, epoch)
		out, err := s.s3c.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			if isNotFoundS3Err(err) {
				return nil, nil
			}
			return nil, err
		}
		raw, readErr := io.ReadAll(io.LimitReader(out.Body, 16*1024*1024))
		_ = out.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		var m TimelineLayerManifest
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, err
		}
		return &m, nil
	}
	if s.localDir != "" {
		localManifest := filepath.Join(s.localDir, tenantID, fmt.Sprintf("timeline_%d_layer_manifest.json", epoch))
		raw, err := os.ReadFile(localManifest)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, nil
			}
			return nil, err
		}
		var m TimelineLayerManifest
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, err
		}
		return &m, nil
	}
	return nil, nil
}

// ListLayersFromR2 loads published content-addressed layers from the timeline manifest
// and merges any legacy layers stored under tenants/{tenant}/timeline_{epoch}/layers/.
func (s *LayerStore) ListLayersFromR2(ctx context.Context, tenantID string, epoch int64) ([]LayerMeta, uint64, error) {
	if s.s3c == nil || s.bucket == "" {
		return nil, 0, nil
	}

	var (
		manifest *TimelineLayerManifest
		manErr   error
		legacy   []LayerMeta
		listErr  error
		wg       sync.WaitGroup
	)
	prefix := fmt.Sprintf("tenants/%s/timeline_%d/layers/", tenantID, epoch)

	wg.Add(2)
	go func() {
		defer wg.Done()
		manifest, manErr = s.LoadLayerManifest(ctx, tenantID, epoch)
	}()
	go func() {
		defer wg.Done()
		var token *string
		for {
			out, err := s.s3c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket:            aws.String(s.bucket),
				Prefix:            aws.String(prefix),
				ContinuationToken: token,
			})
			if err != nil {
				listErr = err
				return
			}
			for _, obj := range out.Contents {
				if obj.Key == nil || obj.Size == nil || *obj.Size < int64(LayerFooterSize) {
					continue
				}
				fileName := strings.TrimPrefix(*obj.Key, prefix)
				kind, seq, startLSN, endLSN, ok := parseLayerFileName(fileName)
				if !ok {
					continue
				}
				legacy = append(legacy, LayerMeta{
					TenantID:          tenantID,
					Epoch:             epoch,
					Kind:              kind,
					Seq:               seq,
					FileName:          fileName,
					R2Key:             *obj.Key,
					StartLSN:          startLSN,
					EndLSN:            endLSN,
					FileSize:          *obj.Size,
					Encrypted:         s.kms != nil,
					RemotelyAvailable: true,
				})
			}
			if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
				break
			}
			token = out.NextContinuationToken
		}
	}()
	wg.Wait()

	if manErr != nil {
		return nil, 0, manErr
	}
	if listErr != nil {
		return nil, 0, listErr
	}

	var layers []LayerMeta
	var maxSeq uint64
	seen := make(map[string]struct{})
	if manifest != nil {
		if manifest.MaxSeq > maxSeq {
			maxSeq = manifest.MaxSeq
		}
		for _, lm := range manifest.Layers {
			if lm.Seq > maxSeq {
				maxSeq = lm.Seq
			}
			seen[lm.R2Key] = struct{}{}
			if lm.FileName != "" {
				seen[lm.FileName] = struct{}{}
			}
			layers = append(layers, lm)
		}
	}
	for _, lm := range legacy {
		if _, ok := seen[lm.R2Key]; ok {
			continue
		}
		if _, ok := seen[lm.FileName]; ok {
			continue
		}
		if lm.Seq > maxSeq {
			maxSeq = lm.Seq
		}
		layers = append(layers, lm)
	}

	// Order oldest -> newest: bootstrap/image layers first, then L1/L0 by LSN & filename.
	sort.Slice(layers, func(i, j int) bool {
		if layers[i].EndLSN != layers[j].EndLSN {
			return layers[i].EndLSN < layers[j].EndLSN
		}
		if layers[i].Kind != layers[j].Kind {
			return layers[i].Kind > layers[j].Kind // Image > L1 > L0 at same LSN
		}
		return layers[i].FileName < layers[j].FileName
	})
	return layers, maxSeq, nil
}

func parseLayerFileName(name string) (kind LayerKind, seq, startLSN, endLSN uint64, ok bool) {
	if !strings.HasSuffix(name, ".layer") {
		return 0, 0, 0, 0, false
	}
	stem := strings.TrimSuffix(name, ".layer")
	if strings.HasPrefix(stem, "img_bootstrap_") {
		lsnHex := strings.TrimPrefix(stem, "img_bootstrap_")
		lsn, err := strconv.ParseUint(lsnHex, 16, 64)
		if err != nil {
			return 0, 0, 0, 0, false
		}
		return LayerKindImage, 0, lsn, lsn, true
	}
	parts := strings.Split(stem, "_")
	if len(parts) != 4 {
		return 0, 0, 0, 0, false
	}
	switch parts[0] {
	case "img":
		kind = LayerKindImage
	case "l1":
		kind = LayerKindL1
	case "l0":
		kind = LayerKindL0
	default:
		return 0, 0, 0, 0, false
	}
	var err error
	if seq, err = strconv.ParseUint(parts[1], 10, 64); err != nil {
		return 0, 0, 0, 0, false
	}
	if startLSN, err = strconv.ParseUint(parts[2], 16, 64); err != nil {
		return 0, 0, 0, 0, false
	}
	if endLSN, err = strconv.ParseUint(parts[3], 16, 64); err != nil {
		return 0, 0, 0, 0, false
	}
	return kind, seq, startLSN, endLSN, true
}
