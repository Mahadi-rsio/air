package pageserver

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// PostgreSQL 17 WAL constants (access/xlog_internal.h, access/xlogrecord.h).
const (
	XLogPageMagic     uint16 = 0xD116
	XLogPageMagicPG15 uint16 = 0xD110
	XLogPageSize      int    = 8192
	SizeOfXLogRecord  int    = 24
	SizeOfShortPHD    int    = 24
	SizeOfLongPHD     int    = 40

	XLPFirstIsContRecord        uint16 = 0x0001
	XLPLongHeader               uint16 = 0x0002
	XLPFirstIsOverwriteContRec  uint16 = 0x0008

	XLRBlockIDDataShort   uint8 = 255
	XLRBlockIDDataLong    uint8 = 254
	XLRBlockIDOrigin      uint8 = 253
	XLRBlockIDToplevelXID uint8 = 252
	XLRMaxBlockID         uint8 = 32

	BkpBlockForkMask uint8 = 0x0F
	BkpBlockHasImage uint8 = 0x10
	BkpBlockHasData  uint8 = 0x20
	BkpBlockWillInit uint8 = 0x40
	BkpBlockSameRel  uint8 = 0x80

	BkpImageHasHole      uint8 = 0x01
	BkpImageApply        uint8 = 0x02
	BkpImageCompressPGLZ uint8 = 0x04
	BkpImageCompressLZ4  uint8 = 0x08
	BkpImageCompressZSTD uint8 = 0x10

	RmXLogID uint8 = 0
	RmXactID uint8 = 1
	RmSmgrID uint8 = 2

	XLogXactCommit         uint8 = 0x00
	XLogXactAbort          uint8 = 0x20
	XLogXactCommitPrepared uint8 = 0x30
	XLogXactAbortPrepared  uint8 = 0x40
	XLogXactAssignment     uint8 = 0x60
	XLogXactOpMask         uint8 = 0x70

	XLogSmgrCreate   uint8 = 0x10
	XLogSmgrTruncate uint8 = 0x20
	XLogInfoOpMask   uint8 = 0xF0
)

var (
	ErrInvalidWALMagic  = errors.New("pageserver: invalid PG17 XLOG page magic")
	ErrTruncatedWAL     = errors.New("pageserver: truncated WAL record")
	ErrCompressedFPI    = errors.New("pageserver: compressed FPI requires external decompressor")
)

// WALBlockRef describes one block reference inside a decoded PG17 XLogRecord.
type WALBlockRef struct {
	BlockID       uint8
	Rel           RelFileLocator
	ForkNum       uint8
	BlockNum      uint32
	WillInit      bool
	HasImage      bool
	HasData       bool
	ImageApply    bool
	ImageInfo     uint8
	HoleOffset    uint16
	HoleLength    uint16
	ImagePayload  []byte
	BlockData     []byte
}

// DecodedWALRecord is a parsed PostgreSQL 17 WAL record.
type DecodedWALRecord struct {
	LSN       uint64
	EndLSN    uint64
	TotalLen  uint32
	XID       uint32
	PrevLSN   uint64
	Info      uint8
	Rmid      uint8
	CRC       uint32
	Blocks    []WALBlockRef
	MainData  []byte
	RawRecord []byte
}

// DecodeWALRecord decodes a single raw PG17 XLogRecord (xl_tot_len bytes) at lsn.
func DecodeWALRecord(lsn uint64, raw []byte) (*DecodedWALRecord, error) {
	if len(raw) < SizeOfXLogRecord {
		return nil, ErrTruncatedWAL
	}
	totLen := binary.LittleEndian.Uint32(raw[0:4])
	if int(totLen) < SizeOfXLogRecord || len(raw) < int(totLen) {
		return nil, fmt.Errorf("%w: totLen=%d have=%d", ErrTruncatedWAL, totLen, len(raw))
	}
	recBuf := raw[:totLen]
	rec := &DecodedWALRecord{
		LSN:       lsn,
		EndLSN:    lsn + uint64(totLen),
		TotalLen:  totLen,
		XID:       binary.LittleEndian.Uint32(recBuf[4:8]),
		PrevLSN:   binary.LittleEndian.Uint64(recBuf[8:16]),
		Info:      recBuf[16],
		Rmid:      recBuf[17],
		CRC:       binary.LittleEndian.Uint32(recBuf[20:24]),
		RawRecord: append([]byte(nil), recBuf...),
	}

	off := SizeOfXLogRecord
	var mainDataLen uint32
	var lastRel RelFileLocator
	hasLastRel := false

	type blkHeader struct {
		ref      WALBlockRef
		imgLen   uint16
		dataLen  uint16
	}
	var hdrs []blkHeader
	payloadBytes := 0

	// Parse block and metadata headers before payload section.
	for off+payloadBytes < int(totLen) {
		id := recBuf[off]
		off++
		switch id {
		case XLRBlockIDDataShort:
			if off >= int(totLen) {
				return nil, fmt.Errorf("%w: data_short off=%d totLen=%d", ErrTruncatedWAL, off, totLen)
			}
			mainDataLen = uint32(recBuf[off])
			off++
			goto payloads
		case XLRBlockIDDataLong:
			if off+4 > int(totLen) {
				return nil, ErrTruncatedWAL
			}
			mainDataLen = binary.LittleEndian.Uint32(recBuf[off : off+4])
			off += 4
			goto payloads
		case XLRBlockIDOrigin:
			if off+2 > int(totLen) {
				return nil, ErrTruncatedWAL
			}
			off += 2
		case XLRBlockIDToplevelXID:
			if off+4 > int(totLen) {
				return nil, ErrTruncatedWAL
			}
			off += 4
		default:
			if id > XLRMaxBlockID {
				return nil, fmt.Errorf("pageserver: unknown WAL block id %d", id)
			}
			if off+3 > int(totLen) {
				return nil, fmt.Errorf("%w: blk_hdr id=%d off=%d totLen=%d", ErrTruncatedWAL, id, off, totLen)
			}
			forkFlags := recBuf[off]
			dataLen := binary.LittleEndian.Uint16(recBuf[off+1 : off+3])
			off += 3

			ref := WALBlockRef{
				BlockID:  id,
				ForkNum:  forkFlags & BkpBlockForkMask,
				WillInit: (forkFlags & BkpBlockWillInit) != 0,
				HasImage: (forkFlags & BkpBlockHasImage) != 0,
				HasData:  (forkFlags & BkpBlockHasData) != 0,
			}

			var imgLen uint16
			if ref.HasImage {
				if off+5 > int(totLen) {
					return nil, ErrTruncatedWAL
				}
				imgLen = binary.LittleEndian.Uint16(recBuf[off : off+2])
				ref.HoleOffset = binary.LittleEndian.Uint16(recBuf[off+2 : off+4])
				ref.ImageInfo = recBuf[off+4]
				ref.ImageApply = (ref.ImageInfo & BkpImageApply) != 0
				off += 5

				compressed := (ref.ImageInfo & (BkpImageCompressPGLZ | BkpImageCompressLZ4 | BkpImageCompressZSTD)) != 0
				if (ref.ImageInfo&BkpImageHasHole) != 0 && compressed {
					if off+2 > int(totLen) {
						return nil, ErrTruncatedWAL
					}
					ref.HoleLength = binary.LittleEndian.Uint16(recBuf[off : off+2])
					off += 2
				} else if (ref.ImageInfo&BkpImageHasHole) != 0 && !compressed {
					if int(imgLen) <= PageSize {
						ref.HoleLength = uint16(PageSize - int(imgLen))
					}
				}
			}

			if (forkFlags & BkpBlockSameRel) == 0 {
				if off+12 > int(totLen) {
					return nil, fmt.Errorf("%w: rel_loc off=%d totLen=%d", ErrTruncatedWAL, off, totLen)
				}
				lastRel = RelFileLocator{
					SpcOID:    binary.LittleEndian.Uint32(recBuf[off : off+4]),
					DbOID:     binary.LittleEndian.Uint32(recBuf[off+4 : off+8]),
					RelNumber: binary.LittleEndian.Uint32(recBuf[off+8 : off+12]),
				}
				hasLastRel = true
				off += 12
			} else if !hasLastRel {
				return nil, fmt.Errorf("pageserver: BKPBLOCK_SAME_REL without prior locator")
			}
			ref.Rel = lastRel

			if off+4 > int(totLen) {
				return nil, ErrTruncatedWAL
			}
			ref.BlockNum = binary.LittleEndian.Uint32(recBuf[off : off+4])
			off += 4

			hdrs = append(hdrs, blkHeader{ref: ref, imgLen: imgLen, dataLen: dataLen})
			if ref.HasImage {
				payloadBytes += int(imgLen)
			}
			if ref.HasData {
				payloadBytes += int(dataLen)
			}
		}
	}

payloads:
	for i := range hdrs {
		if hdrs[i].ref.HasImage {
			n := int(hdrs[i].imgLen)
			if off+n > int(totLen) {
				return nil, ErrTruncatedWAL
			}
			hdrs[i].ref.ImagePayload = append([]byte(nil), recBuf[off:off+n]...)
			off += n
		}
	}
	for i := range hdrs {
		if hdrs[i].ref.HasData {
			n := int(hdrs[i].dataLen)
			if off+n > int(totLen) {
				return nil, ErrTruncatedWAL
			}
			hdrs[i].ref.BlockData = append([]byte(nil), recBuf[off:off+n]...)
			off += n
		}
		rec.Blocks = append(rec.Blocks, hdrs[i].ref)
	}
	if mainDataLen > 0 {
		if off+int(mainDataLen) > int(totLen) {
			return nil, ErrTruncatedWAL
		}
		rec.MainData = append([]byte(nil), recBuf[off:off+int(mainDataLen)]...)
	}
	return rec, nil
}

// RestoreBkpImage reconstructs an 8192-byte page from a WALBlockRef FPI (uncompressed or PGLZ/LZ4/ZSTD compressed).
func RestoreBkpImage(ref WALBlockRef) ([]byte, error) {
	if !ref.HasImage {
		return nil, errors.New("pageserver: block ref has no full-page image")
	}
	payload := ref.ImagePayload
	compressed := (ref.ImageInfo & (BkpImageCompressPGLZ | BkpImageCompressLZ4 | BkpImageCompressZSTD)) != 0
	if compressed {
		rawSize := PageSize
		if (ref.ImageInfo & BkpImageHasHole) != 0 {
			rawSize = PageSize - int(ref.HoleLength)
		}
		var err error
		payload, err = decompressFPIPayload(ref.ImageInfo, ref.ImagePayload, rawSize)
		if err != nil {
			return nil, err
		}
	}
	page := make([]byte, PageSize)
	if (ref.ImageInfo & BkpImageHasHole) == 0 {
		if len(payload) != PageSize {
			return nil, fmt.Errorf("pageserver: invalid FPI size %d", len(payload))
		}
		copy(page, payload)
		return page, nil
	}
	holeOff := int(ref.HoleOffset)
	holeLen := int(ref.HoleLength)
	if holeOff+holeLen > PageSize || len(payload) != PageSize-holeLen {
		return nil, fmt.Errorf("pageserver: invalid FPI hole offset=%d len=%d payload=%d", holeOff, holeLen, len(payload))
	}
	copy(page[:holeOff], payload[:holeOff])
	// page[holeOff : holeOff+holeLen] remains zeroed.
	copy(page[holeOff+holeLen:], payload[holeOff:])
	return page, nil
}

// ParseWALPages decodes all complete PG17 WAL records from a sequence of 8192-byte WAL pages.
func ParseWALPages(data []byte) ([]*DecodedWALRecord, error) {
	if len(data)%XLogPageSize != 0 {
		return nil, fmt.Errorf("pageserver: WAL data length %d is not a multiple of %d", len(data), XLogPageSize)
	}
	var out []*DecodedWALRecord
	var contBuf []byte
	var contLSN uint64
	var contNeed int

	for pageOff := 0; pageOff < len(data); pageOff += XLogPageSize {
		page := data[pageOff : pageOff+XLogPageSize]
		magic := binary.LittleEndian.Uint16(page[0:2])
		if magic != XLogPageMagic && magic != XLogPageMagicPG15 {
			return out, fmt.Errorf("%w: got 0x%04x at offset %d", ErrInvalidWALMagic, magic, pageOff)
		}
		info := binary.LittleEndian.Uint16(page[2:4])
		pageAddr := binary.LittleEndian.Uint64(page[8:16])
		remLen := int(binary.LittleEndian.Uint32(page[16:20]))

		hdrSize := SizeOfShortPHD
		if (info & XLPLongHeader) != 0 {
			hdrSize = SizeOfLongPHD
		}
		pos := hdrSize

		if (info & XLPFirstIsContRecord) != 0 {
			if (info & XLPFirstIsOverwriteContRec) != 0 {
				contBuf = nil
				contNeed = 0
				pos += maxAlign8(remLen)
			} else if len(contBuf) > 0 && contNeed > 0 {
				take := remLen
				if pos+take > XLogPageSize {
					take = XLogPageSize - pos
				}
				contBuf = append(contBuf, page[pos:pos+take]...)
				contNeed -= take
				pos += maxAlign8(take)
				if contNeed <= 0 {
					rec, err := DecodeWALRecord(contLSN, contBuf)
					if err != nil {
						return out, err
					}
					out = append(out, rec)
					contBuf = nil
					contNeed = 0
				}
			} else {
				// Started mid-stream; skip partial continuation at page start.
				pos += maxAlign8(remLen)
			}
		}

		for pos+SizeOfXLogRecord <= XLogPageSize {
			totLen := int(binary.LittleEndian.Uint32(page[pos : pos+4]))
			if totLen == 0 {
				break
			}
			recLSN := pageAddr + uint64(pos)
			avail := XLogPageSize - pos
			if totLen <= avail {
				rec, err := DecodeWALRecord(recLSN, page[pos:pos+totLen])
				if err != nil {
					return out, err
				}
				out = append(out, rec)
				pos += maxAlign8(totLen)
			} else {
				contLSN = recLSN
				contBuf = append([]byte(nil), page[pos:XLogPageSize]...)
				contNeed = totLen - avail
				break
			}
		}
	}
	return out, nil
}

func maxAlign8(n int) int {
	return (n + 7) &^ 7
}

// WALStreamParser incrementally decodes PG17 WAL records from streaming walsender chunks
// starting at an 8192-byte WAL page boundary.
type WALStreamParser struct {
	buf      []byte
	pos      int
	pageAddr uint64
	inPage   bool
	contBuf  []byte
	contLSN  uint64
	contNeed int
}

func (p *WALStreamParser) consumePage() {
	if len(p.buf) <= XLogPageSize {
		p.buf = p.buf[:0]
	} else {
		n := copy(p.buf, p.buf[XLogPageSize:])
		p.buf = p.buf[:n]
	}
	// Shrink backing array if it grew excessively (> 256 KiB) and is now small.
	if cap(p.buf) > 256*1024 && len(p.buf) <= 2*XLogPageSize {
		shrunk := make([]byte, len(p.buf), 4*XLogPageSize)
		copy(shrunk, p.buf)
		p.buf = shrunk
	}
	p.pos = 0
	p.inPage = false
}

// Push appends a WAL chunk and returns all newly completed WAL records.
func (p *WALStreamParser) Push(chunk []byte) ([]*DecodedWALRecord, error) {
	p.buf = append(p.buf, chunk...)
	var out []*DecodedWALRecord

	for len(p.buf) >= SizeOfShortPHD {
		pageLen := len(p.buf)
		if pageLen > XLogPageSize {
			pageLen = XLogPageSize
		}

		if !p.inPage {
			magic := binary.LittleEndian.Uint16(p.buf[0:2])
			if magic == 0 {
				if len(p.buf) < XLogPageSize {
					break
				}
				p.consumePage()
				continue
			}
			if magic != XLogPageMagic && magic != XLogPageMagicPG15 {
				return out, fmt.Errorf("%w: got 0x%04x", ErrInvalidWALMagic, magic)
			}
			info := binary.LittleEndian.Uint16(p.buf[2:4])
			hdrSize := SizeOfShortPHD
			if (info & XLPLongHeader) != 0 {
				hdrSize = SizeOfLongPHD
			}
			if pageLen < hdrSize {
				break
			}
			p.pageAddr = binary.LittleEndian.Uint64(p.buf[8:16])
			remLen := int(binary.LittleEndian.Uint32(p.buf[16:20]))
			p.pos = hdrSize
			p.inPage = true

			if (info & XLPFirstIsContRecord) != 0 {
				take := remLen
				if p.pos+take > XLogPageSize {
					take = XLogPageSize - p.pos
				}
				if pageLen < p.pos+take {
					p.inPage = false
					break
				}
				if (info & XLPFirstIsOverwriteContRec) != 0 {
					p.contBuf = nil
					p.contNeed = 0
				} else if len(p.contBuf) > 0 && p.contNeed > 0 {
					p.contBuf = append(p.contBuf, p.buf[p.pos:p.pos+take]...)
					p.contNeed -= take
					if p.contNeed <= 0 {
						rec, err := DecodeWALRecord(p.contLSN, p.contBuf)
						if err != nil {
							return out, err
						}
						out = append(out, rec)
						p.contBuf = nil
						p.contNeed = 0
					}
				}
				p.pos += maxAlign8(take)
				if pageLen < p.pos && pageLen < XLogPageSize {
					p.pos -= maxAlign8(take)
					p.inPage = false
					break
				}
			}
		}

		advancedPage := false
		for p.pos+SizeOfXLogRecord <= pageLen {
			totLen := int(binary.LittleEndian.Uint32(p.buf[p.pos : p.pos+4]))
			if totLen == 0 {
				if pageLen == XLogPageSize {
					p.consumePage()
					advancedPage = true
				}
				break
			}
			recLSN := p.pageAddr + uint64(p.pos)
			availInPage := XLogPageSize - p.pos
			if totLen <= availInPage {
				if p.pos+totLen > pageLen {
					break
				}
				rec, err := DecodeWALRecord(recLSN, p.buf[p.pos:p.pos+totLen])
				if err != nil {
					return out, fmt.Errorf("pageAddr=%x pos=%d totLen=%d: %w", p.pageAddr, p.pos, totLen, err)
				}
				out = append(out, rec)
				p.pos += maxAlign8(totLen)
			} else {
				if pageLen < XLogPageSize {
					break
				}
				p.contLSN = recLSN
				p.contBuf = append(p.contBuf[:0], p.buf[p.pos:XLogPageSize]...)
				p.contNeed = totLen - availInPage
				p.consumePage()
				advancedPage = true
				break
			}
		}
		if advancedPage {
			continue
		}
		if pageLen == XLogPageSize && p.pos+SizeOfXLogRecord > XLogPageSize {
			remaining := XLogPageSize - p.pos
			if remaining >= 4 {
				totLen := int(binary.LittleEndian.Uint32(p.buf[p.pos : p.pos+4]))
				if totLen > remaining {
					p.contLSN = p.pageAddr + uint64(p.pos)
					p.contBuf = append(p.contBuf[:0], p.buf[p.pos:XLogPageSize]...)
					p.contNeed = totLen - remaining
				}
			}
			p.consumePage()
			continue
		}
		break
	}
	return out, nil
}
