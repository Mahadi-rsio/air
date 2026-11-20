package readadapter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const (
	PGPageSize             = 8192
	PGPageHeaderSize       = 24
	PGPageLayoutVersion    = 4
	PGExpectedPageSizeVer  = PGPageSize | PGPageLayoutVersion // 0x2004
	PGSizeOfHeapTupleHdr   = 23
	PGSizeOfBTPageOpaque   = 16
	PGBTMetaMagic          = 0x053162

	LPUnused   uint16 = 0
	LPNormal   uint16 = 1
	LPRedirect uint16 = 2
	LPDead     uint16 = 3

	HeapHasNull       uint16 = 0x0001
	HeapHasVarwidth   uint16 = 0x0002
	HeapXminCommitted uint16 = 0x0100
	HeapXminInvalid   uint16 = 0x0200
	HeapXminFrozen    uint16 = HeapXminCommitted | HeapXminInvalid
	HeapXmaxCommitted uint16 = 0x0400
	HeapXmaxInvalid   uint16 = 0x0800
	HeapXmaxIsMulti   uint16 = 0x1000
	HeapXmaxLockOnly  uint16 = 0x0080

	InvalidTransactionID     uint32 = 0
	BootstrapTransactionID   uint32 = 1
	FrozenTransactionID      uint32 = 2
	FirstNormalTransactionID uint32 = 3

	BTPLeaf uint16 = 1 << 0
	BTPRoot uint16 = 1 << 1
	BTPMeta uint16 = 1 << 3
)

var (
	ErrInvalidPage = errors.New("readadapter: invalid PostgreSQL 17 page")
)

// PageHeader represents PostgreSQL 17 PageHeaderData (24 bytes).
type PageHeader struct {
	LSN             uint64
	Checksum        uint16
	Flags           uint16
	Lower           uint16
	Upper           uint16
	Special         uint16
	PageSizeVersion uint16
	PruneXID        uint32
}

// ParsePageHeader decodes and validates a PostgreSQL 17 8192-byte page header.
func ParsePageHeader(page []byte) (PageHeader, error) {
	if len(page) != PGPageSize {
		return PageHeader{}, fmt.Errorf("%w: length %d != %d", ErrInvalidPage, len(page), PGPageSize)
	}
	lsnHi := uint64(binary.LittleEndian.Uint32(page[0:4]))
	lsnLo := uint64(binary.LittleEndian.Uint32(page[4:8]))
	hdr := PageHeader{
		LSN:             (lsnHi << 32) | lsnLo,
		Checksum:        binary.LittleEndian.Uint16(page[8:10]),
		Flags:           binary.LittleEndian.Uint16(page[10:12]),
		Lower:           binary.LittleEndian.Uint16(page[12:14]),
		Upper:           binary.LittleEndian.Uint16(page[14:16]),
		Special:         binary.LittleEndian.Uint16(page[16:18]),
		PageSizeVersion: binary.LittleEndian.Uint16(page[18:20]),
		PruneXID:        binary.LittleEndian.Uint32(page[20:24]),
	}
	if hdr.PageSizeVersion != PGExpectedPageSizeVer {
		return hdr, fmt.Errorf("%w: pd_pagesize_version=0x%04x want=0x%04x", ErrInvalidPage, hdr.PageSizeVersion, PGExpectedPageSizeVer)
	}
	if hdr.Lower < PGPageHeaderSize || hdr.Lower > hdr.Upper || hdr.Upper > hdr.Special || hdr.Special > PGPageSize {
		return hdr, fmt.Errorf("%w: invalid offsets lower=%d upper=%d special=%d", ErrInvalidPage, hdr.Lower, hdr.Upper, hdr.Special)
	}
	return hdr, nil
}

// LinePointer represents PostgreSQL ItemIdData (4 bytes).
type LinePointer struct {
	Offset uint16
	Flags  uint16
	Length uint16
}

// ParseLinePointers extracts all ItemIdData entries between PGPageHeaderSize and hdr.Lower.
func ParseLinePointers(page []byte, hdr PageHeader) []LinePointer {
	n := int(hdr.Lower-PGPageHeaderSize) / 4
	out := make([]LinePointer, 0, n)
	for i := 0; i < n; i++ {
		raw := binary.LittleEndian.Uint32(page[PGPageHeaderSize+i*4 : PGPageHeaderSize+(i+1)*4])
		out = append(out, LinePointer{
			Offset: uint16(raw & 0x7FFF),
			Flags:  uint16((raw >> 15) & 0x0003),
			Length: uint16((raw >> 17) & 0x7FFF),
		})
	}
	return out
}

// HeapTupleHeader represents PostgreSQL 17 HeapTupleHeaderData.
type HeapTupleHeader struct {
	Xmin      uint32
	Xmax      uint32
	Cid       uint32
	Infomask2 uint16
	Infomask  uint16
	Hoff      uint8
	NullBits  []byte
}

// ParseHeapTupleHeader decodes HeapTupleHeaderData from a tuple slice.
func ParseHeapTupleHeader(tup []byte) (HeapTupleHeader, error) {
	if len(tup) < PGSizeOfHeapTupleHdr {
		return HeapTupleHeader{}, fmt.Errorf("%w: tuple too short (%d)", ErrInvalidPage, len(tup))
	}
	h := HeapTupleHeader{
		Xmin:      binary.LittleEndian.Uint32(tup[0:4]),
		Xmax:      binary.LittleEndian.Uint32(tup[4:8]),
		Cid:       binary.LittleEndian.Uint32(tup[8:12]),
		Infomask2: binary.LittleEndian.Uint16(tup[18:20]),
		Infomask:  binary.LittleEndian.Uint16(tup[20:22]),
		Hoff:      tup[22],
	}
	if int(h.Hoff) < PGSizeOfHeapTupleHdr || int(h.Hoff) > len(tup) {
		return h, fmt.Errorf("%w: invalid t_hoff=%d len=%d", ErrInvalidPage, h.Hoff, len(tup))
	}
	if (h.Infomask&HeapHasNull) != 0 && int(h.Hoff) > PGSizeOfHeapTupleHdr {
		h.NullBits = tup[PGSizeOfHeapTupleHdr:h.Hoff]
	}
	return h, nil
}

// IsVisible evaluates MVCC snapshot visibility for a heap tuple header.
func (h HeapTupleHeader) IsVisible(snap *ReadSnapshot, committedXIDs map[uint32]bool) bool {
	if (h.Infomask & HeapXminInvalid) != 0 {
		return false
	}
	if !xidVisible(h.Xmin, snap, committedXIDs, (h.Infomask&HeapXminCommitted) != 0) {
		return false
	}
	if (h.Infomask & HeapXmaxInvalid) != 0 || h.Xmax == 0 {
		return true
	}
	// If xmax committed and is visible in snapshot, tuple is deleted.
	if xidVisible(h.Xmax, snap, committedXIDs, (h.Infomask&HeapXmaxCommitted) != 0) {
		return false
	}
	return true
}

func xidVisible(xid uint32, snap *ReadSnapshot, committedXIDs map[uint32]bool, hintCommitted bool) bool {
	if xid == 0 {
		return false
	}
	if xid == 1 || xid == 2 { // Bootstrap / FrozenTransactionId
		return true
	}
	if snap != nil && snap.Xmax != 0 {
		if xid >= snap.Xmax {
			return false
		}
		for _, active := range snap.ActiveXIPs {
			if xid == active {
				return false
			}
		}
	}
	if hintCommitted {
		return true
	}
	if committedXIDs != nil {
		return committedXIDs[xid]
	}
	return false
}

// ColumnType identifies a PostgreSQL column type for tuple decoding proof.
type ColumnType int

const (
	ColInt4 ColumnType = iota
	ColInt8
	ColFloat8
	ColText
)

func alignUp(off, align int) int {
	return (off + align - 1) &^ (align - 1)
}

// DecodeHeapColumns decodes attributes from a PG17 heap tuple according to cols.
func DecodeHeapColumns(tup []byte, hdr HeapTupleHeader, cols []ColumnType) ([]any, error) {
	off := int(hdr.Hoff)
	vals := make([]any, len(cols))
	for i, ct := range cols {
		if (hdr.Infomask&HeapHasNull) != 0 && len(hdr.NullBits) > i/8 {
			if (hdr.NullBits[i/8] & (1 << uint(i%8))) == 0 {
				vals[i] = nil
				continue
			}
		}
		switch ct {
		case ColInt4:
			off = alignUp(off, 4)
			if off+4 > len(tup) {
				return nil, fmt.Errorf("%w: int4 out of bounds", ErrInvalidPage)
			}
			vals[i] = int32(binary.LittleEndian.Uint32(tup[off : off+4]))
			off += 4
		case ColInt8:
			off = alignUp(off, 8)
			if off+8 > len(tup) {
				return nil, fmt.Errorf("%w: int8 out of bounds", ErrInvalidPage)
			}
			vals[i] = int64(binary.LittleEndian.Uint64(tup[off : off+8]))
			off += 8
		case ColFloat8:
			off = alignUp(off, 8)
			if off+8 > len(tup) {
				return nil, fmt.Errorf("%w: float8 out of bounds", ErrInvalidPage)
			}
			vals[i] = math.Float64frombits(binary.LittleEndian.Uint64(tup[off : off+8]))
			off += 8
		case ColText:
			if off >= len(tup) {
				return nil, fmt.Errorf("%w: text out of bounds", ErrInvalidPage)
			}
			b0 := tup[off]
			if (b0 & 0x01) != 0 {
				// 1-byte varlena header (short or external TOAST pointer)
				if b0 == 0x01 {
					// External TOAST pointer: 1-byte tag + 1-byte vartag_external + varatt_external (16B)
					if off+18 > len(tup) {
						return nil, fmt.Errorf("%w: toast pointer out of bounds", ErrInvalidPage)
					}
					rawSize := binary.LittleEndian.Uint32(tup[off+2 : off+6])
					vals[i] = fmt.Sprintf("<TOAST_EXTERNAL len=%d>", rawSize)
					off += 18
				} else {
					totalLen := int(b0 >> 1)
					if off+totalLen > len(tup) {
						return nil, fmt.Errorf("%w: short varlena out of bounds", ErrInvalidPage)
					}
					vals[i] = string(tup[off+1 : off+totalLen])
					off += totalLen
				}
			} else {
				// 4-byte varlena header (4-byte aligned)
				off = alignUp(off, 4)
				if off+4 > len(tup) {
					return nil, fmt.Errorf("%w: varlena4 header out of bounds", ErrInvalidPage)
				}
				hdr4 := binary.LittleEndian.Uint32(tup[off : off+4])
				totalLen := int(hdr4 >> 2)
				if (hdr4 & 0x02) != 0 {
					// PGLZ/LZ4 compressed inline varlena
					vals[i] = fmt.Sprintf("<COMPRESSED_VARLENA len=%d>", totalLen)
					off += totalLen
				} else {
					if off+totalLen > len(tup) {
						return nil, fmt.Errorf("%w: varlena4 body out of bounds", ErrInvalidPage)
					}
					vals[i] = string(tup[off+4 : off+totalLen])
					off += totalLen
				}
			}
		}
	}
	return vals, nil
}

// BTPageOpaque represents PostgreSQL 17 BTPageOpaqueData (16 bytes at pd_special).
type BTPageOpaque struct {
	Prev    uint32
	Next    uint32
	Level   uint32
	Flags   uint16
	CycleID uint16
}

// ParseBTPageOpaque decodes and validates a PG17 B-tree page opaque area.
func ParseBTPageOpaque(page []byte, hdr PageHeader) (BTPageOpaque, error) {
	if int(hdr.Special) != PGPageSize-PGSizeOfBTPageOpaque {
		return BTPageOpaque{}, fmt.Errorf("%w: unexpected bt special=%d", ErrInvalidPage, hdr.Special)
	}
	sp := page[hdr.Special:]
	return BTPageOpaque{
		Prev:    binary.LittleEndian.Uint32(sp[0:4]),
		Next:    binary.LittleEndian.Uint32(sp[4:8]),
		Level:   binary.LittleEndian.Uint32(sp[8:12]),
		Flags:   binary.LittleEndian.Uint16(sp[12:14]),
		CycleID: binary.LittleEndian.Uint16(sp[14:16]),
	}, nil
}

// ParseBTMetaMagic reads btm_magic from B-tree metapage (block 0).
func ParseBTMetaMagic(page []byte, hdr PageHeader) (uint32, error) {
	if int(hdr.Lower) < PGPageHeaderSize+4 || len(page) < PGPageHeaderSize+4 {
		return 0, fmt.Errorf("%w: metapage too short", ErrInvalidPage)
	}
	return binary.LittleEndian.Uint32(page[PGPageHeaderSize : PGPageHeaderSize+4]), nil
}
