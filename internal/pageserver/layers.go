package pageserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	TenantID  string
	Epoch     int64
	Kind      LayerKind
	FileName  string
	R2Key     string
	StartKey  Key
	EndKey    Key
	StartLSN  uint64
	EndLSN    uint64
	FileSize  int64
	Encrypted bool
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
		data, err = kms.DecryptObject(meta.TenantID, meta.Epoch, meta.R2Key+"#idx", raw)
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

// LayerStore persists and range-reads immutable layer files across local disk and R2.
type LayerStore struct {
	localDir string
	s3c      *s3.Client
	bucket   string
	kms      *crypto.EnvelopeKMS
	cache    *DiskCache
}

// NewLayerStore creates a LayerStore backed by localDir, optional S3/R2, optional KMS, and optional DiskCache.
func NewLayerStore(localDir string, s3c *s3.Client, bucket string, kms *crypto.EnvelopeKMS, cache *DiskCache) *LayerStore {
	return &LayerStore{
		localDir: localDir,
		s3c:      s3c,
		bucket:   bucket,
		kms:      kms,
		cache:    cache,
	}
}

// WriteLayer encodes recs into an immutable layer file, caches locally, and uploads to R2 if configured.
func (s *LayerStore) WriteLayer(ctx context.Context, meta LayerMeta, recs []LayerRecord) (LayerMeta, error) {
	data, err := EncodeLayerBytes(meta.TenantID, meta.Epoch, meta.R2Key, meta.Kind, meta.EndLSN, recs, s.kms)
	if err != nil {
		return meta, err
	}
	meta.FileSize = int64(len(data))
	meta.Encrypted = s.kms != nil
	if len(recs) > 0 {
		meta.StartKey = recs[0].Key
		meta.EndKey = recs[len(recs)-1].Key
	}

	if s.localDir != "" {
		localPath := filepath.Join(s.localDir, meta.TenantID, meta.FileName)
		if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
			return meta, err
		}
		if err := os.WriteFile(localPath, data, 0o600); err != nil {
			return meta, err
		}
	}
	if s.cache != nil {
		_ = s.cache.Put(meta.R2Key, data)
	}
	if s.s3c != nil && s.bucket != "" {
		_, err := s.s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(meta.R2Key),
			Body:   bytes.NewReader(data),
		})
		if err != nil {
			return meta, err
		}
	}
	return meta, nil
}

// ReadRange fetches [offset, offset+length) from disk cache, localDir, or R2 HTTP Range.
func (s *LayerStore) ReadRange(ctx context.Context, meta LayerMeta, offset, length int64) ([]byte, error) {
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
	return io.ReadAll(resp.Body)
}

// LookupKeyInLayer reads only the footer, index, and matching entry ranges for key <= lsn.
func (s *LayerStore) LookupKeyInLayer(ctx context.Context, meta LayerMeta, key Key, lsn uint64) ([]WALDelta, error) {
	if meta.FileSize < int64(LayerFooterSize) {
		return nil, errors.New("pageserver: layer file smaller than footer")
	}
	footerRaw, err := s.ReadRange(ctx, meta, meta.FileSize-int64(LayerFooterSize), int64(LayerFooterSize))
	if err != nil {
		return nil, err
	}
	footer, err := DecodeLayerFooter(footerRaw)
	if err != nil {
		return nil, err
	}
	if footer.EntryCount == 0 {
		return nil, nil
	}
	idxRaw, err := s.ReadRange(ctx, meta, int64(footer.IndexOffset), int64(footer.IndexLen))
	if err != nil {
		return nil, err
	}
	entries, err := DecodeLayerIndex(idxRaw, meta, footer.EntryCount, s.kms)
	if err != nil {
		return nil, err
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
			raw, err = s.kms.DecryptObject(meta.TenantID, meta.Epoch, fmt.Sprintf("%s#e%d", meta.R2Key, i), raw)
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
