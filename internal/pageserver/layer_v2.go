package pageserver

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/klauspost/compress/zstd"
)

const (
	LayerVersionV2 uint16 = 2

	LayerCodecNone uint8 = 0
	LayerCodecZstd uint8 = 1

	v2TargetChunkRawBytes = 64 * 1024
	v2MaxRecordsPerChunk  = 64
	v2ChunkDirEntrySize   = 20
	v2WALMetaSize         = 20
	v2IndexHeaderSize     = 8
	v2IndexEntrySize      = 44
	maxV2DecompressedSize = 256 * 1024 * 1024
)

var (
	v2ZstdOnce sync.Once
	v2ZstdEnc  *zstd.Encoder
	v2ZstdDec  *zstd.Decoder
	v2ZstdErr  error
)

func getV2Zstd() (*zstd.Encoder, *zstd.Decoder, error) {
	v2ZstdOnce.Do(func() {
		v2ZstdEnc, v2ZstdErr = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if v2ZstdErr != nil {
			return
		}
		v2ZstdDec, v2ZstdErr = zstd.NewReader(nil)
	})
	return v2ZstdEnc, v2ZstdDec, v2ZstdErr
}

func compressV2Payload(raw []byte, forceNone bool) ([]byte, uint8, error) {
	if forceNone || len(raw) < 32 {
		return raw, LayerCodecNone, nil
	}
	enc, _, err := getV2Zstd()
	if err != nil {
		return nil, 0, err
	}
	comp := enc.EncodeAll(raw, make([]byte, 0, len(raw)))
	if len(comp) >= len(raw) {
		// Preserve original uncompressed data when compression is ineffective.
		return raw, LayerCodecNone, nil
	}
	return comp, LayerCodecZstd, nil
}

func decompressV2Payload(data []byte, codec uint8, rawLen uint32) ([]byte, error) {
	if uint64(rawLen) > maxV2DecompressedSize {
		return nil, fmt.Errorf("pageserver: v2 rawLen %d exceeds max %d", rawLen, maxV2DecompressedSize)
	}
	switch codec {
	case LayerCodecNone:
		if uint32(len(data)) != rawLen {
			return nil, fmt.Errorf("pageserver: v2 raw length mismatch got=%d want=%d", len(data), rawLen)
		}
		return data, nil
	case LayerCodecZstd:
		_, dec, err := getV2Zstd()
		if err != nil {
			return nil, err
		}
		hint := int(rawLen)
		if hint > 1<<20 {
			hint = 1 << 20
		}
		out, err := dec.DecodeAll(data, make([]byte, 0, hint))
		if err != nil {
			return nil, fmt.Errorf("pageserver: v2 zstd decode: %w", err)
		}
		if uint32(len(out)) != rawLen {
			return nil, fmt.Errorf("pageserver: v2 zstd length mismatch got=%d want=%d", len(out), rawLen)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("pageserver: unsupported v2 codec %d", codec)
	}
}

type v2ChunkMeta struct {
	Offset    uint64
	CipherLen uint32
	RawLen    uint32
	Codec     uint8
}

type v2WALTableMeta struct {
	Offset    uint64
	CipherLen uint32
	RawLen    uint32
	Codec     uint8
}

type v2EncodeResult struct {
	FileSize  int64
	SHA256Hex string
	StartKey  Key
	EndKey    Key
}

type v2EncodeOptions struct {
	DisableCompression bool
}

func v2ChunkAAD(aadKey string, chunkIdx int, codec uint8, rawLen uint32) string {
	return fmt.Sprintf("%s#v2c%d|codec=%d|raw=%d", aadKey, chunkIdx, codec, rawLen)
}

func v2WALTableAAD(aadKey string, codec uint8, rawLen uint32) string {
	return fmt.Sprintf("%s#v2wal|codec=%d|raw=%d", aadKey, codec, rawLen)
}

func v2IndexAAD(aadKey string, codec uint8, rawLen uint32) string {
	return fmt.Sprintf("%s#v2idx|codec=%d|raw=%d", aadKey, codec, rawLen)
}

// EncodeLayerV2Bytes encodes sorted records into Layer Format V2 in memory (primarily for tests/benchmarks).
func EncodeLayerV2Bytes(tenantID string, epoch int64, aadKey string, kind LayerKind, endLSN uint64, recs []LayerRecord, kms *crypto.EnvelopeKMS) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := EncodeLayerV2ToWriter(&buf, tenantID, epoch, aadKey, kind, endLSN, recs, kms, v2EncodeOptions{}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// EncodeLayerV2ToWriter streams sorted LayerRecords into Layer Format V2 directly to w
type v2PendingRec struct {
	rec          LayerRecord
	walIDPlusOne uint32
}

// v2StreamWriter encodes already-sorted LayerRecords into Layer Format V2 incrementally
// with bounded chunk buffers (~64 KiB) and single-pass incremental SHA-256 calculation.
type v2StreamWriter struct {
	tenantID      string
	epoch         int64
	aadKey        string
	kind          LayerKind
	endLSN        uint64
	kms           *crypto.EnvelopeKMS
	opts          v2EncodeOptions
	encrypted     bool
	hasher        interface{ Sum([]byte) []byte }
	bw            *bufio.Writer
	offset        uint64
	walTable      [][]byte
	walBuckets    map[[32]byte][]uint32
	chunks        []v2ChunkMeta
	index         []LayerIndexEntry
	chunkBuf      []v2PendingRec
	chunkRawBytes int
	hasKeys       bool
	startKey      Key
	endKey        Key
}

func newV2StreamWriter(w io.Writer, tenantID string, epoch int64, aadKey string, kind LayerKind, endLSN uint64, kms *crypto.EnvelopeKMS, opts v2EncodeOptions) *v2StreamWriter {
	h := sha256.New()
	return &v2StreamWriter{
		tenantID:   tenantID,
		epoch:      epoch,
		aadKey:     aadKey,
		kind:       kind,
		endLSN:     endLSN,
		kms:        kms,
		opts:       opts,
		encrypted:  kms != nil,
		hasher:     h,
		bw:         bufio.NewWriterSize(io.MultiWriter(w, h), 64*1024),
		walBuckets: make(map[[32]byte][]uint32),
		chunkBuf:   make([]v2PendingRec, 0, v2MaxRecordsPerChunk),
	}
}

func (sw *v2StreamWriter) internWAL(wal []byte) uint32 {
	if len(wal) == 0 {
		return 0
	}
	sum := sha256.Sum256(wal)
	for _, idx := range sw.walBuckets[sum] {
		if bytes.Equal(sw.walTable[idx], wal) {
			return idx + 1
		}
	}
	idx := uint32(len(sw.walTable))
	sw.walTable = append(sw.walTable, wal)
	sw.walBuckets[sum] = append(sw.walBuckets[sum], idx)
	return idx + 1
}

func (sw *v2StreamWriter) flushChunk() error {
	if len(sw.chunkBuf) == 0 {
		return nil
	}
	chunkIdx := len(sw.chunks)
	headerSize := 4 + len(sw.chunkBuf)*4
	totalRaw := headerSize
	for _, pr := range sw.chunkBuf {
		totalRaw += 12 + len(pr.rec.Delta.PageImage)
	}
	rawChunk := make([]byte, totalRaw)
	binary.LittleEndian.PutUint32(rawChunk[0:4], uint32(len(sw.chunkBuf)))

	pos := headerSize
	for i, pr := range sw.chunkBuf {
		binary.LittleEndian.PutUint32(rawChunk[4+i*4:8+i*4], uint32(pos))
		binary.LittleEndian.PutUint32(rawChunk[pos:pos+4], pr.rec.Delta.RelSize)
		pLen := len(pr.rec.Delta.PageImage)
		binary.LittleEndian.PutUint32(rawChunk[pos+4:pos+8], uint32(pLen))
		copy(rawChunk[pos+8:pos+8+pLen], pr.rec.Delta.PageImage)
		binary.LittleEndian.PutUint32(rawChunk[pos+8+pLen:pos+12+pLen], pr.walIDPlusOne)
		pos += 12 + pLen

		sw.index = append(sw.index, LayerIndexEntry{
			Key:    pr.rec.Key,
			LSN:    pr.rec.Delta.LSN,
			Offset: uint64(chunkIdx),
			Length: uint32(i),
			Flags:  pr.rec.Delta.Flags,
		})
	}

	payload, codec, err := compressV2Payload(rawChunk, sw.opts.DisableCompression)
	if err != nil {
		return err
	}
	rawLen := uint32(len(rawChunk))
	if sw.encrypted {
		payload, err = sw.kms.EncryptObject(sw.tenantID, sw.epoch, v2ChunkAAD(sw.aadKey, chunkIdx, codec, rawLen), payload)
		if err != nil {
			return err
		}
	}

	if _, err := sw.bw.Write(payload); err != nil {
		return err
	}
	sw.chunks = append(sw.chunks, v2ChunkMeta{
		Offset:    sw.offset,
		CipherLen: uint32(len(payload)),
		RawLen:    rawLen,
		Codec:     codec,
	})
	sw.offset += uint64(len(payload))
	sw.chunkBuf = sw.chunkBuf[:0]
	sw.chunkRawBytes = 0
	return nil
}

// Add appends a record (must be called in non-decreasing (Key, LSN) order).
func (sw *v2StreamWriter) Add(rec LayerRecord) error {
	if !sw.hasKeys {
		sw.startKey = rec.Key
		sw.hasKeys = true
	}
	sw.endKey = rec.Key

	walID := sw.internWAL(rec.Delta.WALRecord)
	sw.chunkBuf = append(sw.chunkBuf, v2PendingRec{rec: rec, walIDPlusOne: walID})
	sw.chunkRawBytes += 12 + len(rec.Delta.PageImage)
	if sw.chunkRawBytes >= v2TargetChunkRawBytes || len(sw.chunkBuf) >= v2MaxRecordsPerChunk {
		return sw.flushChunk()
	}
	return nil
}

func (sw *v2StreamWriter) Finish() (v2EncodeResult, error) {
	var res v2EncodeResult
	if sw.hasKeys {
		res.StartKey = sw.startKey
		res.EndKey = sw.endKey
	}
	if err := sw.flushChunk(); err != nil {
		return res, err
	}

	var walMeta v2WALTableMeta
	if len(sw.walTable) > 0 {
		walRawSize := 4
		for _, wBytes := range sw.walTable {
			walRawSize += 4 + len(wBytes)
		}
		walRaw := make([]byte, walRawSize)
		binary.LittleEndian.PutUint32(walRaw[0:4], uint32(len(sw.walTable)))
		pos := 4
		for _, wBytes := range sw.walTable {
			binary.LittleEndian.PutUint32(walRaw[pos:pos+4], uint32(len(wBytes)))
			copy(walRaw[pos+4:pos+4+len(wBytes)], wBytes)
			pos += 4 + len(wBytes)
		}
		walPayload, walCodec, err := compressV2Payload(walRaw, sw.opts.DisableCompression)
		if err != nil {
			return res, err
		}
		walRawLen := uint32(len(walRaw))
		if sw.encrypted {
			walPayload, err = sw.kms.EncryptObject(sw.tenantID, sw.epoch, v2WALTableAAD(sw.aadKey, walCodec, walRawLen), walPayload)
			if err != nil {
				return res, err
			}
		}
		if _, err := sw.bw.Write(walPayload); err != nil {
			return res, err
		}
		walMeta = v2WALTableMeta{
			Offset:    sw.offset,
			CipherLen: uint32(len(walPayload)),
			RawLen:    walRawLen,
			Codec:     walCodec,
		}
		sw.offset += uint64(len(walPayload))
	}

	idxRawSize := 4 + len(sw.chunks)*v2ChunkDirEntrySize + v2WALMetaSize + len(sw.index)*v2IndexEntrySize
	idxRaw := make([]byte, idxRawSize)
	binary.LittleEndian.PutUint32(idxRaw[0:4], uint32(len(sw.chunks)))
	pos := 4
	for _, cm := range sw.chunks {
		binary.LittleEndian.PutUint64(idxRaw[pos:pos+8], cm.Offset)
		binary.LittleEndian.PutUint32(idxRaw[pos+8:pos+12], cm.CipherLen)
		binary.LittleEndian.PutUint32(idxRaw[pos+12:pos+16], cm.RawLen)
		idxRaw[pos+16] = cm.Codec
		pos += v2ChunkDirEntrySize
	}
	binary.LittleEndian.PutUint64(idxRaw[pos:pos+8], walMeta.Offset)
	binary.LittleEndian.PutUint32(idxRaw[pos+8:pos+12], walMeta.CipherLen)
	binary.LittleEndian.PutUint32(idxRaw[pos+12:pos+16], walMeta.RawLen)
	idxRaw[pos+16] = walMeta.Codec
	pos += v2WALMetaSize

	for _, e := range sw.index {
		e.Key.Encode(idxRaw[pos : pos+24])
		binary.LittleEndian.PutUint64(idxRaw[pos+24:pos+32], e.LSN)
		binary.LittleEndian.PutUint32(idxRaw[pos+32:pos+36], uint32(e.Offset))
		binary.LittleEndian.PutUint32(idxRaw[pos+36:pos+40], e.Length)
		binary.LittleEndian.PutUint32(idxRaw[pos+40:pos+44], e.Flags)
		pos += v2IndexEntrySize
	}

	idxPayload, idxCodec, err := compressV2Payload(idxRaw, sw.opts.DisableCompression)
	if err != nil {
		return res, err
	}
	idxRawLen := uint32(len(idxRaw))
	if sw.encrypted {
		idxPayload, err = sw.kms.EncryptObject(sw.tenantID, sw.epoch, v2IndexAAD(sw.aadKey, idxCodec, idxRawLen), idxPayload)
		if err != nil {
			return res, err
		}
	}

	var idxHdr [v2IndexHeaderSize]byte
	idxHdr[0] = idxCodec
	binary.LittleEndian.PutUint32(idxHdr[4:8], idxRawLen)

	indexOff := sw.offset
	if _, err := sw.bw.Write(idxHdr[:]); err != nil {
		return res, err
	}
	if _, err := sw.bw.Write(idxPayload); err != nil {
		return res, err
	}
	indexTotalLen := uint32(v2IndexHeaderSize + len(idxPayload))
	sw.offset += uint64(indexTotalLen)

	var footer [LayerFooterSize]byte
	binary.LittleEndian.PutUint32(footer[0:4], LayerMagic)
	binary.LittleEndian.PutUint16(footer[4:6], LayerVersionV2)
	footer[6] = byte(sw.kind)
	if sw.encrypted {
		footer[7] = 1
	}
	binary.LittleEndian.PutUint64(footer[8:16], indexOff)
	binary.LittleEndian.PutUint32(footer[16:20], indexTotalLen)
	binary.LittleEndian.PutUint32(footer[20:24], uint32(len(sw.index)))
	binary.LittleEndian.PutUint64(footer[24:32], sw.endLSN)
	if _, err := sw.bw.Write(footer[:]); err != nil {
		return res, err
	}
	sw.offset += uint64(LayerFooterSize)

	if err := sw.bw.Flush(); err != nil {
		return res, err
	}
	res.FileSize = int64(sw.offset)
	res.SHA256Hex = hex.EncodeToString(sw.hasher.Sum(nil))
	return res, nil
}

// EncodeLayerV2ToWriter streams sorted LayerRecords into Layer Format V2 directly to w
// with bounded chunk buffers and single-pass incremental SHA-256 computation.
func EncodeLayerV2ToWriter(w io.Writer, tenantID string, epoch int64, aadKey string, kind LayerKind, endLSN uint64, recs []LayerRecord, kms *crypto.EnvelopeKMS, opts v2EncodeOptions) (v2EncodeResult, error) {
	sort.Slice(recs, func(i, j int) bool {
		if c := recs[i].Key.Compare(recs[j].Key); c != 0 {
			return c < 0
		}
		return recs[i].Delta.LSN < recs[j].Delta.LSN
	})
	sw := newV2StreamWriter(w, tenantID, epoch, aadKey, kind, endLSN, kms, opts)
	for _, rec := range recs {
		if err := sw.Add(rec); err != nil {
			return v2EncodeResult{}, err
		}
	}
	return sw.Finish()
}

type decodedV2Index struct {
	chunks   []v2ChunkMeta
	walTable v2WALTableMeta
	entries  []LayerIndexEntry
}

func decodeLayerV2Index(raw []byte, meta LayerMeta, count uint32, kms *crypto.EnvelopeKMS) (decodedV2Index, error) {
	var out decodedV2Index
	if len(raw) < v2IndexHeaderSize {
		return out, errors.New("pageserver: truncated v2 index header")
	}
	idxCodec := raw[0]
	idxRawLen := binary.LittleEndian.Uint32(raw[4:8])
	payload := raw[v2IndexHeaderSize:]

	if meta.Encrypted {
		if kms == nil {
			return out, errors.New("pageserver: encrypted v2 layer requires KMS")
		}
		var err error
		payload, err = kms.DecryptObject(meta.TenantID, meta.Epoch, v2IndexAAD(meta.aadKey(), idxCodec, idxRawLen), payload)
		if err != nil {
			return out, err
		}
	}

	data, err := decompressV2Payload(payload, idxCodec, idxRawLen)
	if err != nil {
		return out, err
	}
	if len(data) < 4+v2WALMetaSize {
		return out, errors.New("pageserver: truncated v2 index body")
	}

	chunkCount := uint64(binary.LittleEndian.Uint32(data[0:4]))
	expectedSize := 4 + chunkCount*v2ChunkDirEntrySize + v2WALMetaSize + uint64(count)*v2IndexEntrySize
	if uint64(len(data)) != expectedSize {
		return out, fmt.Errorf("pageserver: v2 index size mismatch got=%d want=%d", len(data), expectedSize)
	}

	chunks := make([]v2ChunkMeta, int(chunkCount))
	pos := 4
	for i := 0; i < int(chunkCount); i++ {
		chunks[i] = v2ChunkMeta{
			Offset:    binary.LittleEndian.Uint64(data[pos : pos+8]),
			CipherLen: binary.LittleEndian.Uint32(data[pos+8 : pos+12]),
			RawLen:    binary.LittleEndian.Uint32(data[pos+12 : pos+16]),
			Codec:     data[pos+16],
		}
		pos += v2ChunkDirEntrySize
	}

	walMeta := v2WALTableMeta{
		Offset:    binary.LittleEndian.Uint64(data[pos : pos+8]),
		CipherLen: binary.LittleEndian.Uint32(data[pos+8 : pos+12]),
		RawLen:    binary.LittleEndian.Uint32(data[pos+12 : pos+16]),
		Codec:     data[pos+16],
	}
	pos += v2WALMetaSize

	entries := make([]LayerIndexEntry, count)
	for i := 0; i < int(count); i++ {
		entries[i] = LayerIndexEntry{
			Key:    DecodeKey(data[pos : pos+24]),
			LSN:    binary.LittleEndian.Uint64(data[pos+24 : pos+32]),
			Offset: uint64(binary.LittleEndian.Uint32(data[pos+32 : pos+36])), // chunkIdx
			Length: binary.LittleEndian.Uint32(data[pos+36 : pos+40]),         // recInChunkIdx
			Flags:  binary.LittleEndian.Uint32(data[pos+40 : pos+44]),
		}
		pos += v2IndexEntrySize
	}

	out.chunks = chunks
	out.walTable = walMeta
	out.entries = entries
	return out, nil
}

func decodeV2WALTable(raw []byte, meta LayerMeta, wm v2WALTableMeta, kms *crypto.EnvelopeKMS) ([][]byte, error) {
	if wm.CipherLen == 0 {
		return nil, nil
	}
	payload := raw
	if meta.Encrypted {
		if kms == nil {
			return nil, errors.New("pageserver: encrypted v2 WAL table requires KMS")
		}
		var err error
		payload, err = kms.DecryptObject(meta.TenantID, meta.Epoch, v2WALTableAAD(meta.aadKey(), wm.Codec, wm.RawLen), payload)
		if err != nil {
			return nil, err
		}
	}
	data, err := decompressV2Payload(payload, wm.Codec, wm.RawLen)
	if err != nil {
		return nil, err
	}
	if len(data) < 4 {
		return nil, errors.New("pageserver: truncated v2 WAL table")
	}
	count := uint64(binary.LittleEndian.Uint32(data[0:4]))
	if count*4 > uint64(len(data)-4) {
		return nil, fmt.Errorf("pageserver: malformed v2 WAL table count=%d len=%d", count, len(data))
	}
	out := make([][]byte, int(count))
	pos := 4
	for i := 0; i < int(count); i++ {
		if pos+4 > len(data) {
			return nil, errors.New("pageserver: truncated v2 WAL entry length")
		}
		n := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+n > len(data) {
			return nil, errors.New("pageserver: truncated v2 WAL entry body")
		}
		if n > 0 {
			out[i] = append([]byte(nil), data[pos:pos+n]...)
		}
		pos += n
	}
	return out, nil
}

func decodeV2ChunkRaw(rawCipher []byte, meta LayerMeta, chunkIdx int, cm v2ChunkMeta, kms *crypto.EnvelopeKMS) ([]byte, error) {
	payload := rawCipher
	if meta.Encrypted {
		if kms == nil {
			return nil, errors.New("pageserver: encrypted v2 chunk requires KMS")
		}
		var err error
		payload, err = kms.DecryptObject(meta.TenantID, meta.Epoch, v2ChunkAAD(meta.aadKey(), chunkIdx, cm.Codec, cm.RawLen), payload)
		if err != nil {
			return nil, err
		}
	}
	return decompressV2Payload(payload, cm.Codec, cm.RawLen)
}

func extractV2RecordFromChunk(chunkRaw []byte, recIdx uint32, lsn, flags uint64) (WALDelta, uint32, error) {
	if len(chunkRaw) < 4 {
		return WALDelta{}, 0, errors.New("pageserver: truncated v2 chunk")
	}
	recCount := binary.LittleEndian.Uint32(chunkRaw[0:4])
	if recIdx >= recCount {
		return WALDelta{}, 0, fmt.Errorf("pageserver: v2 record index %d out of bounds (%d)", recIdx, recCount)
	}
	offPos := 4 + int(recIdx)*4
	if offPos+4 > len(chunkRaw) {
		return WALDelta{}, 0, errors.New("pageserver: truncated v2 chunk offset table")
	}
	pos := int(binary.LittleEndian.Uint32(chunkRaw[offPos : offPos+4]))
	if pos+12 > len(chunkRaw) {
		return WALDelta{}, 0, errors.New("pageserver: truncated v2 record header")
	}
	relSize := binary.LittleEndian.Uint32(chunkRaw[pos : pos+4])
	pLen := int(binary.LittleEndian.Uint32(chunkRaw[pos+4 : pos+8]))
	if pos+12+pLen > len(chunkRaw) {
		return WALDelta{}, 0, errors.New("pageserver: truncated v2 record page")
	}
	var page []byte
	if pLen > 0 {
		page = append([]byte(nil), chunkRaw[pos+8:pos+8+pLen]...)
	}
	walIDPlusOne := binary.LittleEndian.Uint32(chunkRaw[pos+8+pLen : pos+12+pLen])
	return WALDelta{
		LSN:       lsn,
		Flags:     uint32(flags),
		PageImage: page,
		RelSize:   relSize,
	}, walIDPlusOne, nil
}

func (s *LayerStore) loadV2WALTable(ctx context.Context, meta LayerMeta, idxData *layerIndexData) ([][]byte, error) {
	if idxData.walMeta.CipherLen == 0 {
		return nil, nil
	}
	idxData.walMu.Lock()
	if idxData.walLoaded {
		wt := idxData.walTable
		idxData.walMu.Unlock()
		return wt, nil
	}
	idxData.walMu.Unlock()

	raw, err := s.ReadRange(ctx, meta, int64(idxData.walMeta.Offset), int64(idxData.walMeta.CipherLen))
	if err != nil {
		return nil, err
	}
	wt, err := decodeV2WALTable(raw, meta, idxData.walMeta, s.kms)
	if err != nil {
		if !errors.Is(err, crypto.ErrDEKMissing) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			if s.cache != nil && s.s3c != nil && s.bucket != "" {
				s.evictCorruptDiskCache(meta, int64(idxData.walMeta.Offset), int64(idxData.walMeta.CipherLen))
				if raw2, rErr := s.ReadRange(ctx, meta, int64(idxData.walMeta.Offset), int64(idxData.walMeta.CipherLen)); rErr == nil {
					wt, err = decodeV2WALTable(raw2, meta, idxData.walMeta, s.kms)
				}
			}
		}
		if err != nil {
			return nil, err
		}
	}

	idxData.walMu.Lock()
	if !idxData.walLoaded {
		idxData.walTable = wt
		idxData.walLoaded = true
	} else {
		wt = idxData.walTable
	}
	idxData.walMu.Unlock()
	return wt, nil
}

func (s *LayerStore) lookupKeysInLayerV2(ctx context.Context, meta LayerMeta, idxData *layerIndexData, keys []Key, lsn uint64) ([][]WALDelta, error) {
	out := make([][]WALDelta, len(keys))
	type v2Match struct {
		keySlot  int
		order    int
		chunkIdx uint32
		recIdx   uint32
		lsn      uint64
		flags    uint32
	}
	var matches []v2Match
	chunkNeeded := make(map[uint32]struct{})
	for ki, k := range keys {
		if idxData.bloom != nil && !idxData.bloom.mayContain(k) {
			continue
		}
		start := findKeyStart(idxData.entries, idxData.sparse, k)
		ord := 0
		for i := start; i < len(idxData.entries) && idxData.entries[i].Key.Compare(k) == 0; i++ {
			e := idxData.entries[i]
			if lsn != 0 && e.LSN > lsn {
				break
			}
			cIdx := uint32(e.Offset)
			if int(cIdx) >= len(idxData.chunks) {
				return nil, fmt.Errorf("pageserver: v2 chunk index %d out of bounds (%d)", cIdx, len(idxData.chunks))
			}
			matches = append(matches, v2Match{
				keySlot:  ki,
				order:    ord,
				chunkIdx: cIdx,
				recIdx:   e.Length,
				lsn:      e.LSN,
				flags:    e.Flags,
			})
			chunkNeeded[cIdx] = struct{}{}
			ord++
		}
	}
	if len(matches) == 0 {
		return out, nil
	}

	uniqueChunks := make([]uint32, 0, len(chunkNeeded))
	for cIdx := range chunkNeeded {
		uniqueChunks = append(uniqueChunks, cIdx)
	}
	sort.Slice(uniqueChunks, func(i, j int) bool {
		return idxData.chunks[uniqueChunks[i]].Offset < idxData.chunks[uniqueChunks[j]].Offset
	})

	var keyEpoch uint64
	if s.decodedChunks != nil && s.kms != nil {
		ep, err := s.kms.TenantKeyEpoch(meta.TenantID)
		if err != nil {
			return nil, err
		}
		keyEpoch = ep
	}

	decompressedChunks := make(map[uint32][]byte, len(uniqueChunks))
	if s.decodedChunks != nil {
		for _, cIdx := range uniqueChunks {
			cm := idxData.chunks[cIdx]
			dKey := DecodedChunkKey{
				TenantID: meta.TenantID,
				LayerID:  meta.R2Key,
				ChunkIdx: cIdx,
				KeyEpoch: keyEpoch,
			}
			chunkRaw, err := s.decodedChunks.GetOrDecode(ctx, dKey, func() ([]byte, error) {
				return s.readAndDecodeV2ChunkWithRecovery(ctx, meta, int(cIdx), cm)
			})
			if err != nil {
				return nil, err
			}
			decompressedChunks[cIdx] = chunkRaw
		}
	} else {
		i := 0
		for i < len(uniqueChunks) {
			firstMeta := idxData.chunks[uniqueChunks[i]]
			spanStart := int64(firstMeta.Offset)
			spanEnd := spanStart + int64(firstMeta.CipherLen)
			j := i + 1
			for j < len(uniqueChunks) {
				cm := idxData.chunks[uniqueChunks[j]]
				nextStart := int64(cm.Offset)
				nextEnd := nextStart + int64(cm.CipherLen)
				if nextStart-spanEnd > coalesceGapBytes || nextEnd-spanStart > maxCoalescedSpan {
					break
				}
				if nextEnd > spanEnd {
					spanEnd = nextEnd
				}
				j++
			}

			spanBuf, err := s.ReadRange(ctx, meta, spanStart, spanEnd-spanStart)
			if err != nil {
				return nil, err
			}
			for k := i; k < j; k++ {
				cIdx := uniqueChunks[k]
				cm := idxData.chunks[cIdx]
				relOff := int64(cm.Offset) - spanStart
				cipherSlice := spanBuf[relOff : relOff+int64(cm.CipherLen)]
				chunkRaw, err := decodeV2ChunkRaw(cipherSlice, meta, int(cIdx), cm, s.kms)
				if err != nil {
					if !errors.Is(err, crypto.ErrDEKMissing) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						if s.cache != nil && s.s3c != nil && s.bucket != "" {
							s.evictCorruptDiskCache(meta, int64(cm.Offset), int64(cm.CipherLen))
							chunkRaw, err = s.readAndDecodeV2ChunkOnce(ctx, meta, int(cIdx), cm)
						}
					}
					if err != nil {
						return nil, err
					}
				}
				decompressedChunks[cIdx] = chunkRaw
			}
			i = j
		}
	}

	type resolvedMatch struct {
		keySlot      int
		order        int
		delta        WALDelta
		walIDPlusOne uint32
	}
	resolved := make([]resolvedMatch, 0, len(matches))
	needWAL := false
	for _, m := range matches {
		delta, walIDPlusOne, err := extractV2RecordFromChunk(decompressedChunks[m.chunkIdx], m.recIdx, m.lsn, uint64(m.flags))
		if err != nil {
			if s.decodedChunks != nil {
				s.decodedChunks.Evict(DecodedChunkKey{
					TenantID: meta.TenantID,
					LayerID:  meta.R2Key,
					ChunkIdx: m.chunkIdx,
					KeyEpoch: keyEpoch,
				})
			}
			cm := idxData.chunks[m.chunkIdx]
			s.evictCorruptDiskCache(meta, int64(cm.Offset), int64(cm.CipherLen))
			return nil, err
		}
		if walIDPlusOne > 0 {
			needWAL = true
		}
		resolved = append(resolved, resolvedMatch{
			keySlot:      m.keySlot,
			order:        m.order,
			delta:        delta,
			walIDPlusOne: walIDPlusOne,
		})
	}

	var walTable [][]byte
	if needWAL {
		var err error
		walTable, err = s.loadV2WALTable(ctx, meta, idxData)
		if err != nil {
			return nil, err
		}
	}

	for _, rm := range resolved {
		if rm.walIDPlusOne > 0 {
			idx := int(rm.walIDPlusOne - 1)
			if idx >= len(walTable) {
				return nil, fmt.Errorf("pageserver: v2 WAL index %d out of bounds (%d)", idx, len(walTable))
			}
			rm.delta.WALRecord = append([]byte(nil), walTable[idx]...)
		}
		out[rm.keySlot] = append(out[rm.keySlot], rm.delta)
	}
	return out, nil
}

func (s *LayerStore) readAndDecodeV2ChunkOnce(ctx context.Context, meta LayerMeta, chunkIdx int, cm v2ChunkMeta) ([]byte, error) {
	cipherSlice, err := s.ReadRange(ctx, meta, int64(cm.Offset), int64(cm.CipherLen))
	if err != nil {
		return nil, err
	}
	return decodeV2ChunkRaw(cipherSlice, meta, chunkIdx, cm, s.kms)
}

func (s *LayerStore) readAndDecodeV2ChunkWithRecovery(ctx context.Context, meta LayerMeta, chunkIdx int, cm v2ChunkMeta) ([]byte, error) {
	chunkRaw, err := s.readAndDecodeV2ChunkOnce(ctx, meta, chunkIdx, cm)
	if err == nil {
		return chunkRaw, nil
	}
	if errors.Is(err, crypto.ErrDEKMissing) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}
	if s.cache != nil && s.s3c != nil && s.bucket != "" {
		s.evictCorruptDiskCache(meta, int64(cm.Offset), int64(cm.CipherLen))
		return s.readAndDecodeV2ChunkOnce(ctx, meta, chunkIdx, cm)
	}
	return nil, err
}
