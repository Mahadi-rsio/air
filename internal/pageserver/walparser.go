package pageserver

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// PostgreSQL 17 WAL constants (access/xlog_internal.h, access/xlogrecord.h).
const (
	XLogPageMagic     uint16 = 0xD116
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
	RmSmgrID uint8 = 2

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

	// Parse block and metadata headers before payload section.
	for off < int(totLen) {
		id := recBuf[off]
		off++
		switch id {
		case XLRBlockIDDataShort:
			if off >= int(totLen) {
				return nil, ErrTruncatedWAL
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
				return nil, ErrTruncatedWAL
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
					return nil, ErrTruncatedWAL
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

// RestoreBkpImage reconstructs an 8192-byte page from an uncompressed WALBlockRef FPI.
func RestoreBkpImage(ref WALBlockRef) ([]byte, error) {
	if !ref.HasImage {
		return nil, errors.New("pageserver: block ref has no full-page image")
	}
	compressed := (ref.ImageInfo & (BkpImageCompressPGLZ | BkpImageCompressLZ4 | BkpImageCompressZSTD)) != 0
	if compressed {
		return nil, ErrCompressedFPI
	}
	page := make([]byte, PageSize)
	if (ref.ImageInfo & BkpImageHasHole) == 0 {
		if len(ref.ImagePayload) != PageSize {
			return nil, fmt.Errorf("pageserver: invalid FPI size %d", len(ref.ImagePayload))
		}
		copy(page, ref.ImagePayload)
		return page, nil
	}
	holeOff := int(ref.HoleOffset)
	holeLen := int(ref.HoleLength)
	if holeOff+holeLen > PageSize || len(ref.ImagePayload) != PageSize-holeLen {
		return nil, fmt.Errorf("pageserver: invalid FPI hole offset=%d len=%d payload=%d", holeOff, holeLen, len(ref.ImagePayload))
	}
	copy(page[:holeOff], ref.ImagePayload[:holeOff])
	// page[holeOff : holeOff+holeLen] remains zeroed.
	copy(page[holeOff+holeLen:], ref.ImagePayload[holeOff:])
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
		if magic != XLogPageMagic {
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
