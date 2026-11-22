package readadapter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unsafe"
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

	HeapHasNull        uint16 = 0x0001
	HeapHasVarwidth    uint16 = 0x0002
	HeapHasExternal    uint16 = 0x0004
	HeapXmaxKeyShrLock uint16 = 0x0010
	HeapXmaxExclLock   uint16 = 0x0040
	HeapXmaxLockOnly   uint16 = 0x0080
	HeapXmaxLockBits   uint16 = HeapXmaxExclLock | HeapXmaxKeyShrLock | HeapXmaxLockOnly
	HeapXminCommitted  uint16 = 0x0100
	HeapXminInvalid    uint16 = 0x0200
	HeapXminFrozen     uint16 = HeapXminCommitted | HeapXminInvalid
	HeapXmaxCommitted  uint16 = 0x0400
	HeapXmaxInvalid    uint16 = 0x0800
	HeapXmaxIsMulti    uint16 = 0x1000
	HeapUpdated        uint16 = 0x2000

	HeapNattsMask uint16 = 0x07FF
	HeapHOTUpdate uint16 = 0x4000
	HeapOnlyTuple uint16 = 0x8000

	InvalidTransactionID     uint32 = 0
	BootstrapTransactionID   uint32 = 1
	FrozenTransactionID      uint32 = 2
	FirstNormalTransactionID uint32 = 3

	BTPLeaf uint16 = 1 << 0
	BTPRoot uint16 = 1 << 1
	BTPMeta uint16 = 1 << 3
)

var (
	ErrInvalidPage       = errors.New("readadapter: invalid PostgreSQL 17 page")
	ErrTOASTUnsupported  = errors.New("readadapter: TOAST/compressed value requires primary fallback")
	ErrUnsupportedType   = errors.New("readadapter: unsupported PostgreSQL column type requires primary fallback")
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
	if hdr.Lower < PGPageHeaderSize || hdr.Lower > hdr.Upper || hdr.Upper > hdr.Special || hdr.Special > PGPageSize || (hdr.Lower-PGPageHeaderSize)%4 != 0 {
		return hdr, fmt.Errorf("%w: invalid offsets lower=%d upper=%d special=%d", ErrInvalidPage, hdr.Lower, hdr.Upper, hdr.Special)
	}
	return hdr, nil
}

// IsZeroPage returns true if an 8 KiB page is completely zeroed (uninitialized PG page).
func IsZeroPage(page []byte) bool {
	for _, b := range page {
		if b != 0 {
			return false
		}
	}
	return true
}

// LinePointer represents PostgreSQL ItemIdData (4 bytes).
type LinePointer struct {
	Offset uint16
	Flags  uint16
	Length uint16
}

// ValidateLinePointer checks that an LPNormal line pointer points within [hdr.Upper, hdr.Special]
// and is large enough for a HeapTupleHeaderData.
func ValidateLinePointer(lp LinePointer, hdr PageHeader) error {
	if lp.Flags != LPNormal {
		return nil
	}
	if lp.Length < PGSizeOfHeapTupleHdr {
		return fmt.Errorf("%w: line pointer length %d < %d", ErrInvalidPage, lp.Length, PGSizeOfHeapTupleHdr)
	}
	if lp.Offset < hdr.Upper || int(lp.Offset)+int(lp.Length) > int(hdr.Special) {
		return fmt.Errorf("%w: line pointer out of bounds off=%d len=%d upper=%d special=%d", ErrInvalidPage, lp.Offset, lp.Length, hdr.Upper, hdr.Special)
	}
	return nil
}

// ParseLinePointers extracts all ItemIdData entries between PGPageHeaderSize and hdr.Lower.
func ParseLinePointers(page []byte, hdr PageHeader) []LinePointer {
	n := int(hdr.Lower-PGPageHeaderSize) / 4
	return ParseLinePointersInto(page, hdr, make([]LinePointer, 0, n))
}

// ParseLinePointersInto appends all ItemIdData entries into dst[:0], reusing capacity.
func ParseLinePointersInto(page []byte, hdr PageHeader, dst []LinePointer) []LinePointer {
	n := int(hdr.Lower-PGPageHeaderSize) / 4
	out := dst[:0]
	if cap(out) < n {
		out = make([]LinePointer, 0, n)
	}
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

func heapXmaxIsLockedOnly(infomask uint16) bool {
	if (infomask & HeapXmaxLockOnly) != 0 {
		return true
	}
	return (infomask & (HeapXmaxIsMulti | HeapXmaxLockBits)) == HeapXmaxExclLock
}

// IsVisible evaluates MVCC snapshot visibility for a heap tuple header.
func (h HeapTupleHeader) IsVisible(snap *ReadSnapshot, committedXIDs map[uint32]bool) bool {
	xminFrozen := (h.Infomask & HeapXminFrozen) == HeapXminFrozen
	if !xminFrozen {
		if (h.Infomask&HeapXminCommitted) == 0 && (h.Infomask&HeapXminInvalid) != 0 {
			return false
		}
		if !xidVisible(h.Xmin, snap, committedXIDs, (h.Infomask&HeapXminCommitted) != 0) {
			return false
		}
	}
	if (h.Infomask&HeapXmaxInvalid) != 0 || h.Xmax == InvalidTransactionID || heapXmaxIsLockedOnly(h.Infomask) {
		return true
	}
	if (h.Infomask & HeapXmaxIsMulti) != 0 {
		// Cannot resolve MultiXact without pg_multixact SLRU.
		return false
	}
	// If xmax committed and is visible in snapshot, tuple is deleted.
	if xidVisible(h.Xmax, snap, committedXIDs, (h.Infomask&HeapXmaxCommitted) != 0) {
		return false
	}
	return true
}

func xidVisible(xid uint32, snap *ReadSnapshot, committedXIDs map[uint32]bool, hintCommitted bool) bool {
	if xid == InvalidTransactionID {
		return false
	}
	if xid == BootstrapTransactionID || xid == FrozenTransactionID {
		return true
	}
	if snap != nil {
		if snap.Xmin != 0 && TransactionIdPrecedes(xid, snap.Xmin) {
			return hintCommitted || (committedXIDs != nil && committedXIDs[xid])
		}
		if snap.Xmax != 0 && TransactionIdFollowsOrEquals(xid, snap.Xmax) {
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

// ColumnType identifies a PostgreSQL column type for tuple decoding.
type ColumnType int

const (
	ColInt4 ColumnType = iota
	ColInt8
	ColFloat8
	ColText
	ColBool
	ColChar
	ColInt2
	ColOID
	ColName
	ColFloat4
	ColBytea
	ColDate
	ColTimestamp
	ColUUID
	ColInt2Vector
	ColJSONB
	ColNumeric
	ColInet
	ColCIDR
	ColInt2Array
	ColInt4Array
	ColInt8Array
	ColTextArray
	ColBoolArray
	ColFloat4Array
	ColFloat8Array
	ColUnsupported
)

// PGTypeOIDToColumnType maps a PostgreSQL pg_type OID to a native tuple ColumnType
// and SQLite virtual table column declaration type.
func PGTypeOIDToColumnType(oid uint32) (ColumnType, string) {
	switch oid {
	case 22: // int2vector
		return ColInt2Vector, "TEXT"
	case 16: // bool
		return ColBool, "BOOLEAN"
	case 17: // bytea
		return ColBytea, "BLOB"
	case 18: // "char"
		return ColChar, "TEXT"
	case 19: // name
		return ColName, "TEXT"
	case 20: // int8 / bigint
		return ColInt8, "BIGINT"
	case 21: // int2 / smallint
		return ColInt2, "INTEGER"
	case 23: // int4 / integer
		return ColInt4, "INTEGER"
	case 25, 1042, 1043, 114, 142: // text, bpchar, varchar, json, xml
		return ColText, "TEXT"
	case 3802: // jsonb
		return ColJSONB, "TEXT"
	case 26, 28, 29: // oid, xid, cid
		return ColOID, "BIGINT"
	case 700: // float4 / real
		return ColFloat4, "REAL"
	case 701: // float8 / double precision
		return ColFloat8, "REAL"
	case 1082: // date
		return ColDate, "TEXT"
	case 1114, 1184: // timestamp, timestamptz
		return ColTimestamp, "TEXT"
	case 2950: // uuid
		return ColUUID, "TEXT"
	case 1700: // numeric
		return ColNumeric, "TEXT_NUMERIC COLLATE PG_NUMERIC"
	case 869: // inet
		return ColInet, "TEXT_INET COLLATE PG_INET"
	case 650: // cidr
		return ColCIDR, "TEXT_CIDR COLLATE PG_INET"
	case 1005: // int2[]
		return ColInt2Array, "TEXT_INT2ARRAY"
	case 1007: // int4[]
		return ColInt4Array, "TEXT_INT4ARRAY"
	case 1016: // int8[]
		return ColInt8Array, "TEXT_INT8ARRAY"
	case 1009: // text[]
		return ColTextArray, "TEXT_TEXTARRAY"
	case 1000: // bool[]
		return ColBoolArray, "TEXT_BOOLARRAY"
	case 1021: // float4[]
		return ColFloat4Array, "TEXT_FLOAT4ARRAY"
	case 1022: // float8[]
		return ColFloat8Array, "TEXT_FLOAT8ARRAY"
	default:
		return ColUnsupported, "TEXT"
	}
}

func alignUp(off, align int) int {
	return (off + align - 1) &^ (align - 1)
}

var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// pgLZDecompress implements PostgreSQL's PGLZ decompression algorithm.
func pgLZDecompress(src []byte, rawSize int) ([]byte, error) {
	if rawSize < 0 || rawSize > 64*1024*1024 {
		return nil, fmt.Errorf("%w: invalid pglz rawSize=%d", ErrInvalidPage, rawSize)
	}
	dst := make([]byte, 0, rawSize)
	sp := 0
	for sp < len(src) && len(dst) < rawSize {
		ctrl := src[sp]
		sp++
		for bit := 0; bit < 8 && sp < len(src) && len(dst) < rawSize; bit++ {
			if (ctrl & (1 << uint(bit))) != 0 {
				if sp+2 > len(src) {
					return nil, fmt.Errorf("%w: truncated pglz backreference", ErrInvalidPage)
				}
				b0 := int(src[sp])
				b1 := int(src[sp+1])
				sp += 2
				matchLen := (b0 & 0x0f) + 3
				matchOff := ((b0 & 0xf0) << 4) | b1
				if matchLen == 18 {
					if sp >= len(src) {
						return nil, fmt.Errorf("%w: truncated pglz extended length", ErrInvalidPage)
					}
					matchLen += int(src[sp])
					sp++
				}
				if matchOff <= 0 || matchOff > len(dst) {
					return nil, fmt.Errorf("%w: invalid pglz backreference offset=%d len=%d", ErrInvalidPage, matchOff, len(dst))
				}
				for k := 0; k < matchLen && len(dst) < rawSize; k++ {
					dst = append(dst, dst[len(dst)-matchOff])
				}
			} else {
				dst = append(dst, src[sp])
				sp++
			}
		}
	}
	if len(dst) != rawSize {
		return nil, fmt.Errorf("%w: pglz decompressed len=%d want=%d", ErrInvalidPage, len(dst), rawSize)
	}
	return dst, nil
}

func decodeVarlena(tup []byte, off int, needValue bool) ([]byte, int, error) {
	return decodeVarlenaWithTOAST(tup, off, needValue, nil)
}

func decodeVarlenaWithTOAST(tup []byte, off int, needValue bool, toastFn TOASTResolverFunc) ([]byte, int, error) {
	if off >= len(tup) {
		return nil, off, fmt.Errorf("%w: varlena out of bounds", ErrInvalidPage)
	}
	b0 := tup[off]
	if (b0 & 0x01) != 0 {
		// 1-byte varlena header (short or external TOAST pointer)
		if b0 == 0x01 {
			if off+18 > len(tup) {
				return nil, off, fmt.Errorf("%w: toast pointer out of bounds", ErrInvalidPage)
			}
			if !needValue {
				return nil, off + 18, nil
			}
			if toastFn == nil {
				return nil, off + 18, ErrTOASTUnsupported
			}
			ptr, err := ParseTOASTPointer(tup[off : off+18])
			if err != nil {
				return nil, off + 18, err
			}
			val, err := toastFn(ptr)
			if err != nil {
				return nil, off + 18, err
			}
			return val, off + 18, nil
		}
		totalLen := int(b0 >> 1)
		if totalLen < 1 || off+totalLen > len(tup) {
			return nil, off, fmt.Errorf("%w: short varlena out of bounds (len=%d)", ErrInvalidPage, totalLen)
		}
		if !needValue {
			return nil, off + totalLen, nil
		}
		return tup[off+1 : off+totalLen], off + totalLen, nil
	}

	// 4-byte varlena header (4-byte aligned)
	off = alignUp(off, 4)
	if off+4 > len(tup) {
		return nil, off, fmt.Errorf("%w: varlena4 header out of bounds", ErrInvalidPage)
	}
	hdr4 := binary.LittleEndian.Uint32(tup[off : off+4])
	totalLen := int(hdr4 >> 2)
	if totalLen < 4 || off+totalLen > len(tup) {
		return nil, off, fmt.Errorf("%w: invalid varlena4 length %d", ErrInvalidPage, totalLen)
	}
	if (hdr4 & 0x02) != 0 {
		if !needValue {
			return nil, off + totalLen, nil
		}
		if totalLen < 8 {
			return nil, off, fmt.Errorf("%w: compressed varlena too short (%d)", ErrInvalidPage, totalLen)
		}
		tcinfo := binary.LittleEndian.Uint32(tup[off+4 : off+8])
		rawSize := int(tcinfo & 0x3FFFFFFF)
		method := tcinfo >> 30
		switch method {
		case PGToastCompPGLZ:
			decompressed, err := pgLZDecompress(tup[off+8:off+totalLen], rawSize)
			if err != nil {
				return nil, off, err
			}
			return decompressed, off + totalLen, nil
		case PGToastCompLZ4:
			decompressed, err := pgLZ4Decompress(tup[off+8:off+totalLen], rawSize)
			if err != nil {
				return nil, off, err
			}
			return decompressed, off + totalLen, nil
		default:
			return nil, off + totalLen, ErrTOASTUnsupported
		}
	}
	if !needValue {
		return nil, off + totalLen, nil
	}
	return tup[off+4 : off+totalLen], off + totalLen, nil
}

// DecodeHeapColumns decodes all attributes from a PG17 heap tuple according to cols.
func DecodeHeapColumns(tup []byte, hdr HeapTupleHeader, cols []ColumnType) ([]any, error) {
	return DecodeHeapColumnsProjected(tup, hdr, cols, nil)
}

// DecodeHeapColumnsProjected decodes attributes from a PG17 heap tuple.
// When needed != nil, columns with needed[i] == false are skipped without
// allocating or decompressing values (returning nil in their slot).
func DecodeHeapColumnsProjected(tup []byte, hdr HeapTupleHeader, cols []ColumnType, needed []bool) ([]any, error) {
	off := int(hdr.Hoff)
	vals := make([]any, len(cols))
	natts := int(hdr.Infomask2 & HeapNattsMask)
	for i, ct := range cols {
		if natts > 0 && i >= natts {
			vals[i] = nil
			continue
		}
		if (hdr.Infomask & HeapHasNull) != 0 {
			if i/8 >= len(hdr.NullBits) {
				return nil, fmt.Errorf("%w: null bitmap too short for att %d", ErrInvalidPage, i)
			}
			if (hdr.NullBits[i/8] & (1 << uint(i%8))) == 0 {
				vals[i] = nil
				continue
			}
		}
		want := needed == nil || (i < len(needed) && needed[i])
		switch ct {
		case ColBool:
			if off+1 > len(tup) {
				return nil, fmt.Errorf("%w: bool out of bounds", ErrInvalidPage)
			}
			if want {
				vals[i] = tup[off] != 0
			}
			off++
		case ColChar:
			if off+1 > len(tup) {
				return nil, fmt.Errorf("%w: char out of bounds", ErrInvalidPage)
			}
			if want {
				vals[i] = byte(tup[off])
			}
			off++
		case ColInt2:
			off = alignUp(off, 2)
			if off+2 > len(tup) {
				return nil, fmt.Errorf("%w: int2 out of bounds", ErrInvalidPage)
			}
			if want {
				vals[i] = int16(binary.LittleEndian.Uint16(tup[off : off+2]))
			}
			off += 2
		case ColOID:
			off = alignUp(off, 4)
			if off+4 > len(tup) {
				return nil, fmt.Errorf("%w: oid out of bounds", ErrInvalidPage)
			}
			if want {
				vals[i] = binary.LittleEndian.Uint32(tup[off : off+4])
			}
			off += 4
		case ColName:
			// PostgreSQL NameData is 64 bytes, 1-byte aligned, NUL-terminated.
			if off+64 > len(tup) {
				return nil, fmt.Errorf("%w: name out of bounds", ErrInvalidPage)
			}
			if want {
				raw := tup[off : off+64]
				end := 0
				for end < 64 && raw[end] != 0 {
					end++
				}
				vals[i] = string(raw[:end])
			}
			off += 64
		case ColFloat4:
			off = alignUp(off, 4)
			if off+4 > len(tup) {
				return nil, fmt.Errorf("%w: float4 out of bounds", ErrInvalidPage)
			}
			if want {
				vals[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(tup[off : off+4])))
			}
			off += 4
		case ColBytea:
			raw, nextOff, err := decodeVarlena(tup, off, want)
			if err != nil {
				return nil, err
			}
			if want {
				cp := make([]byte, len(raw))
				copy(cp, raw)
				vals[i] = cp
			}
			off = nextOff
		case ColInt4:
			off = alignUp(off, 4)
			if off+4 > len(tup) {
				return nil, fmt.Errorf("%w: int4 out of bounds", ErrInvalidPage)
			}
			if want {
				vals[i] = int32(binary.LittleEndian.Uint32(tup[off : off+4]))
			}
			off += 4
		case ColInt8:
			off = alignUp(off, 8)
			if off+8 > len(tup) {
				return nil, fmt.Errorf("%w: int8 out of bounds", ErrInvalidPage)
			}
			if want {
				vals[i] = int64(binary.LittleEndian.Uint64(tup[off : off+8]))
			}
			off += 8
		case ColFloat8:
			off = alignUp(off, 8)
			if off+8 > len(tup) {
				return nil, fmt.Errorf("%w: float8 out of bounds", ErrInvalidPage)
			}
			if want {
				vals[i] = math.Float64frombits(binary.LittleEndian.Uint64(tup[off : off+8]))
			}
			off += 8
		case ColText:
			raw, nextOff, err := decodeVarlena(tup, off, want)
			if err != nil {
				return nil, err
			}
			if want {
				vals[i] = string(raw)
			}
			off = nextOff
		case ColDate:
			off = alignUp(off, 4)
			if off+4 > len(tup) {
				return nil, fmt.Errorf("%w: date out of bounds", ErrInvalidPage)
			}
			if want {
				days := int32(binary.LittleEndian.Uint32(tup[off : off+4]))
				vals[i] = pgEpoch.AddDate(0, 0, int(days)).Format("2006-01-02")
			}
			off += 4
		case ColTimestamp:
			off = alignUp(off, 8)
			if off+8 > len(tup) {
				return nil, fmt.Errorf("%w: timestamp out of bounds", ErrInvalidPage)
			}
			if want {
				usec := int64(binary.LittleEndian.Uint64(tup[off : off+8]))
				t := pgEpoch.Add(time.Duration(usec) * time.Microsecond)
				vals[i] = t.Format("2006-01-02 15:04:05.999999")
			}
			off += 8
		case ColUUID:
			if off+16 > len(tup) {
				return nil, fmt.Errorf("%w: uuid out of bounds", ErrInvalidPage)
			}
			if want {
				u := tup[off : off+16]
				vals[i] = fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
					binary.BigEndian.Uint32(u[0:4]),
					binary.BigEndian.Uint16(u[4:6]),
					binary.BigEndian.Uint16(u[6:8]),
					binary.BigEndian.Uint16(u[8:10]),
					u[10:16])
			}
			off += 16
		case ColInt2Vector:
			raw, nextOff, err := decodeVarlena(tup, off, want)
			if err != nil {
				return nil, err
			}
			if want {
				// PostgreSQL ArrayType payload after varlena header:
				// ndim (int4), dataoffset (int4), elemtype (Oid), dim1 (int4), lbound1 (int4), then int16[dim1]
				if len(raw) >= 20 {
					n := int(int32(binary.LittleEndian.Uint32(raw[12:16])))
					if n >= 0 && 20+n*2 <= len(raw) {
						vec := make([]int16, n)
						for k := 0; k < n; k++ {
							vec[k] = int16(binary.LittleEndian.Uint16(raw[20+k*2 : 22+k*2]))
						}
						vals[i] = vec
					} else {
						vals[i] = []int16(nil)
					}
				} else {
					vals[i] = []int16(nil)
				}
			}
			off = nextOff
		case ColJSONB:
			raw, nextOff, err := decodeVarlena(tup, off, want)
			if err != nil {
				return nil, err
			}
			if want {
				s, err := decodePGJSONBToText(raw)
				if err != nil {
					return nil, err
				}
				vals[i] = s
			}
			off = nextOff
		case ColNumeric, ColInet, ColCIDR,
			ColInt2Array, ColInt4Array, ColInt8Array, ColTextArray, ColBoolArray, ColFloat4Array, ColFloat8Array:
			raw, nextOff, err := decodeVarlena(tup, off, want)
			if err != nil {
				return nil, err
			}
			if want {
				s, derr := decodeExtColType(ct, raw)
				if derr != nil {
					return nil, derr
				}
				vals[i] = s
			}
			off = nextOff
		default:
			return nil, fmt.Errorf("%w: column %d type %d", ErrUnsupportedType, i, ct)
		}
	}
	return vals, nil
}

// ValKind discriminates a decoded HeapVal without interface{} boxing.
type ValKind uint8

const (
	ValNull ValKind = iota
	ValInt64
	ValFloat64
	ValBool
	ValByte
	ValString
	ValBytes
)

// HeapVal stores a decoded PostgreSQL column value without interface{} boxing.
type HeapVal struct {
	Kind ValKind
	I64  int64
	F64  float64
	Str  string
	Raw  []byte
}

// CloneStrings copies any transient string/byte slices so the HeapVal is safe
// to retain beyond the lifetime of the underlying page buffer.
func (v *HeapVal) Materialize() {
	if v.Kind == ValString && len(v.Str) > 0 {
		v.Str = strings.Clone(v.Str)
	} else if v.Kind == ValBytes && len(v.Raw) > 0 {
		cp := make([]byte, len(v.Raw))
		copy(cp, v.Raw)
		v.Raw = cp
	}
}

// DecodeHeapColumnsInto decodes projected columns directly into dst (len(dst) >= len(cols))
// without []any or interface{} allocations. When copyStrings is false, string/byte
// values alias tup directly and must be materialized before outliving tup.
func DecodeHeapColumnsInto(tup []byte, hdr HeapTupleHeader, cols []ColumnType, needed []bool, dst []HeapVal, copyStrings bool) error {
	return DecodeHeapColumnsWithTOASTInto(tup, hdr, cols, needed, dst, copyStrings, nil)
}

// MissingColumnValue holds the pre-decoded PostgreSQL attmissingval default for a column
// added via ALTER TABLE ... ADD COLUMN ... DEFAULT without a table rewrite.
type MissingColumnValue struct {
	HasMissing  bool
	Unsupported bool
	Val         HeapVal
}

// DecodeMissingValArray decodes a PostgreSQL 17 pg_attribute.attmissingval (1-element ArrayType payload)
// into a HeapVal for the given ColumnType. Returns (val, true) on success, or (_, false) if the
// array is malformed, TOASTed, or of an unsupported type.
func DecodeMissingValArray(ct ColumnType, raw []byte) (HeapVal, bool) {
	if len(raw) < 20 {
		return HeapVal{}, false
	}
	ndim := int32(binary.LittleEndian.Uint32(raw[0:4]))
	dataOffset := int32(binary.LittleEndian.Uint32(raw[4:8]))
	dim1 := int32(binary.LittleEndian.Uint32(raw[12:16]))
	if ndim != 1 || dim1 != 1 {
		return HeapVal{}, false
	}
	elemOff := 20
	if dataOffset != 0 {
		if len(raw) < 21 || int(dataOffset) < 21 || int(dataOffset) > len(raw) {
			return HeapVal{}, false
		}
		if (raw[20] & 0x01) == 0 {
			return HeapVal{Kind: ValNull}, true
		}
		elemOff = int(dataOffset)
	}
	if elemOff > len(raw) {
		return HeapVal{}, false
	}
	switch ct {
	case ColBool:
		if elemOff+1 > len(raw) {
			return HeapVal{}, false
		}
		var b int64
		if raw[elemOff] != 0 {
			b = 1
		}
		return HeapVal{Kind: ValBool, I64: b}, true
	case ColChar:
		if elemOff+1 > len(raw) {
			return HeapVal{}, false
		}
		return HeapVal{Kind: ValByte, I64: int64(raw[elemOff])}, true
	case ColInt2:
		if elemOff+2 > len(raw) {
			return HeapVal{}, false
		}
		return HeapVal{Kind: ValInt64, I64: int64(int16(binary.LittleEndian.Uint16(raw[elemOff : elemOff+2])))}, true
	case ColInt4:
		if elemOff+4 > len(raw) {
			return HeapVal{}, false
		}
		return HeapVal{Kind: ValInt64, I64: int64(int32(binary.LittleEndian.Uint32(raw[elemOff : elemOff+4])))}, true
	case ColOID:
		if elemOff+4 > len(raw) {
			return HeapVal{}, false
		}
		return HeapVal{Kind: ValInt64, I64: int64(binary.LittleEndian.Uint32(raw[elemOff : elemOff+4]))}, true
	case ColInt8:
		if elemOff+8 > len(raw) {
			return HeapVal{}, false
		}
		return HeapVal{Kind: ValInt64, I64: int64(binary.LittleEndian.Uint64(raw[elemOff : elemOff+8]))}, true
	case ColFloat4:
		if elemOff+4 > len(raw) {
			return HeapVal{}, false
		}
		return HeapVal{Kind: ValFloat64, F64: float64(math.Float32frombits(binary.LittleEndian.Uint32(raw[elemOff : elemOff+4])))}, true
	case ColFloat8:
		if elemOff+8 > len(raw) {
			return HeapVal{}, false
		}
		return HeapVal{Kind: ValFloat64, F64: math.Float64frombits(binary.LittleEndian.Uint64(raw[elemOff : elemOff+8]))}, true
	case ColName:
		if elemOff+64 > len(raw) {
			return HeapVal{}, false
		}
		nameRaw := raw[elemOff : elemOff+64]
		end := 0
		for end < 64 && nameRaw[end] != 0 {
			end++
		}
		return HeapVal{Kind: ValString, Str: string(nameRaw[:end])}, true
	case ColText:
		v, _, err := decodeVarlenaWithTOAST(raw, elemOff, true, nil)
		if err != nil {
			return HeapVal{}, false
		}
		return HeapVal{Kind: ValString, Str: string(v)}, true
	case ColBytea:
		v, _, err := decodeVarlenaWithTOAST(raw, elemOff, true, nil)
		if err != nil {
			return HeapVal{}, false
		}
		cp := make([]byte, len(v))
		copy(cp, v)
		return HeapVal{Kind: ValBytes, Raw: cp}, true
	case ColJSONB:
		v, _, err := decodeVarlenaWithTOAST(raw, elemOff, true, nil)
		if err != nil {
			return HeapVal{}, false
		}
		s, err := decodePGJSONBToText(v)
		if err != nil {
			return HeapVal{}, false
		}
		return HeapVal{Kind: ValString, Str: s}, true
	case ColDate:
		if elemOff+4 > len(raw) {
			return HeapVal{}, false
		}
		days := int32(binary.LittleEndian.Uint32(raw[elemOff : elemOff+4]))
		return HeapVal{Kind: ValString, Str: pgEpoch.AddDate(0, 0, int(days)).Format("2006-01-02")}, true
	case ColTimestamp:
		if elemOff+8 > len(raw) {
			return HeapVal{}, false
		}
		usec := int64(binary.LittleEndian.Uint64(raw[elemOff : elemOff+8]))
		t := pgEpoch.Add(time.Duration(usec) * time.Microsecond)
		return HeapVal{Kind: ValString, Str: t.Format("2006-01-02 15:04:05.999999")}, true
	case ColUUID:
		if elemOff+16 > len(raw) {
			return HeapVal{}, false
		}
		u := raw[elemOff : elemOff+16]
		return HeapVal{Kind: ValString, Str: fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			binary.BigEndian.Uint32(u[0:4]),
			binary.BigEndian.Uint16(u[4:6]),
			binary.BigEndian.Uint16(u[6:8]),
			binary.BigEndian.Uint16(u[8:10]),
			u[10:16])}, true
	case ColNumeric, ColInet, ColCIDR,
		ColInt2Array, ColInt4Array, ColInt8Array, ColTextArray, ColBoolArray, ColFloat4Array, ColFloat8Array:
		v, _, err := decodeVarlenaWithTOAST(raw, elemOff, true, nil)
		if err != nil {
			return HeapVal{}, false
		}
		s, derr := decodeExtColType(ct, v)
		if derr != nil {
			return HeapVal{}, false
		}
		return HeapVal{Kind: ValString, Str: s}, true
	default:
		return HeapVal{}, false
	}
}

// DecodeHeapColumnsWithTOASTInto decodes projected columns directly into dst, resolving external TOAST pointers via toastFn when non-nil.
func DecodeHeapColumnsWithTOASTInto(tup []byte, hdr HeapTupleHeader, cols []ColumnType, needed []bool, dst []HeapVal, copyStrings bool, toastFn TOASTResolverFunc) error {
	return DecodeHeapColumnsWithTOASTAndMissingInto(tup, hdr, cols, needed, dst, copyStrings, toastFn, nil)
}

// DecodeHeapColumnsWithTOASTAndMissingInto decodes projected columns directly into dst,
// substituting attmissingval defaults for trailing attributes omitted from older tuples,
// and resolving external TOAST pointers via toastFn when non-nil.
func DecodeHeapColumnsWithTOASTAndMissingInto(tup []byte, hdr HeapTupleHeader, cols []ColumnType, needed []bool, dst []HeapVal, copyStrings bool, toastFn TOASTResolverFunc, missing []MissingColumnValue) error {
	off := int(hdr.Hoff)
	natts := int(hdr.Infomask2 & HeapNattsMask)
	for i, ct := range cols {
		if natts > 0 && i >= natts {
			want := needed == nil || (i < len(needed) && needed[i])
			if i < len(missing) && missing[i].HasMissing {
				if missing[i].Unsupported {
					if want {
						return fmt.Errorf("%w: missing default value for column %d requires primary fallback", ErrUnsupportedType, i)
					}
					dst[i] = HeapVal{Kind: ValNull}
					continue
				}
				if want {
					dst[i] = missing[i].Val
				} else {
					dst[i] = HeapVal{Kind: ValNull}
				}
			} else {
				dst[i] = HeapVal{Kind: ValNull}
			}
			continue
		}
		if (hdr.Infomask & HeapHasNull) != 0 {
			if i/8 >= len(hdr.NullBits) {
				return fmt.Errorf("%w: null bitmap too short for att %d", ErrInvalidPage, i)
			}
			if (hdr.NullBits[i/8] & (1 << uint(i%8))) == 0 {
				dst[i] = HeapVal{Kind: ValNull}
				continue
			}
		}
		want := needed == nil || (i < len(needed) && needed[i])
		if !want {
			dst[i] = HeapVal{Kind: ValNull}
		}
		switch ct {
		case ColBool:
			if off+1 > len(tup) {
				return fmt.Errorf("%w: bool out of bounds", ErrInvalidPage)
			}
			if want {
				var b int64
				if tup[off] != 0 {
					b = 1
				}
				dst[i] = HeapVal{Kind: ValBool, I64: b}
			}
			off++
		case ColChar:
			if off+1 > len(tup) {
				return fmt.Errorf("%w: char out of bounds", ErrInvalidPage)
			}
			if want {
				dst[i] = HeapVal{Kind: ValByte, I64: int64(tup[off])}
			}
			off++
		case ColInt2:
			off = alignUp(off, 2)
			if off+2 > len(tup) {
				return fmt.Errorf("%w: int2 out of bounds", ErrInvalidPage)
			}
			if want {
				dst[i] = HeapVal{Kind: ValInt64, I64: int64(int16(binary.LittleEndian.Uint16(tup[off : off+2])))}
			}
			off += 2
		case ColOID:
			off = alignUp(off, 4)
			if off+4 > len(tup) {
				return fmt.Errorf("%w: oid out of bounds", ErrInvalidPage)
			}
			if want {
				dst[i] = HeapVal{Kind: ValInt64, I64: int64(binary.LittleEndian.Uint32(tup[off : off+4]))}
			}
			off += 4
		case ColName:
			if off+64 > len(tup) {
				return fmt.Errorf("%w: name out of bounds", ErrInvalidPage)
			}
			if want {
				raw := tup[off : off+64]
				end := 0
				for end < 64 && raw[end] != 0 {
					end++
				}
				var s string
				if end > 0 {
					if copyStrings {
						s = string(raw[:end])
					} else {
						s = unsafe.String(&raw[0], end)
					}
				}
				dst[i] = HeapVal{Kind: ValString, Str: s}
			}
			off += 64
		case ColFloat4:
			off = alignUp(off, 4)
			if off+4 > len(tup) {
				return fmt.Errorf("%w: float4 out of bounds", ErrInvalidPage)
			}
			if want {
				dst[i] = HeapVal{Kind: ValFloat64, F64: float64(math.Float32frombits(binary.LittleEndian.Uint32(tup[off : off+4])))}
			}
			off += 4
		case ColBytea:
			raw, nextOff, err := decodeVarlenaWithTOAST(tup, off, want, toastFn)
			if err != nil {
				return err
			}
			if want {
				if copyStrings && len(raw) > 0 {
					cp := make([]byte, len(raw))
					copy(cp, raw)
					dst[i] = HeapVal{Kind: ValBytes, Raw: cp}
				} else {
					dst[i] = HeapVal{Kind: ValBytes, Raw: raw}
				}
			}
			off = nextOff
		case ColInt4:
			off = alignUp(off, 4)
			if off+4 > len(tup) {
				return fmt.Errorf("%w: int4 out of bounds", ErrInvalidPage)
			}
			if want {
				dst[i] = HeapVal{Kind: ValInt64, I64: int64(int32(binary.LittleEndian.Uint32(tup[off : off+4])))}
			}
			off += 4
		case ColInt8:
			off = alignUp(off, 8)
			if off+8 > len(tup) {
				return fmt.Errorf("%w: int8 out of bounds", ErrInvalidPage)
			}
			if want {
				dst[i] = HeapVal{Kind: ValInt64, I64: int64(binary.LittleEndian.Uint64(tup[off : off+8]))}
			}
			off += 8
		case ColFloat8:
			off = alignUp(off, 8)
			if off+8 > len(tup) {
				return fmt.Errorf("%w: float8 out of bounds", ErrInvalidPage)
			}
			if want {
				dst[i] = HeapVal{Kind: ValFloat64, F64: math.Float64frombits(binary.LittleEndian.Uint64(tup[off : off+8]))}
			}
			off += 8
		case ColText:
			raw, nextOff, err := decodeVarlenaWithTOAST(tup, off, want, toastFn)
			if err != nil {
				return err
			}
			if want {
				var s string
				if len(raw) > 0 {
					if copyStrings {
						s = string(raw)
					} else {
						s = unsafe.String(&raw[0], len(raw))
					}
				}
				dst[i] = HeapVal{Kind: ValString, Str: s}
			}
			off = nextOff
		case ColJSONB:
			raw, nextOff, err := decodeVarlenaWithTOAST(tup, off, want, toastFn)
			if err != nil {
				return err
			}
			if want {
				s, err := decodePGJSONBToText(raw)
				if err != nil {
					return err
				}
				dst[i] = HeapVal{Kind: ValString, Str: s}
			}
			off = nextOff
		case ColDate:
			off = alignUp(off, 4)
			if off+4 > len(tup) {
				return fmt.Errorf("%w: date out of bounds", ErrInvalidPage)
			}
			if want {
				days := int32(binary.LittleEndian.Uint32(tup[off : off+4]))
				dst[i] = HeapVal{Kind: ValString, Str: pgEpoch.AddDate(0, 0, int(days)).Format("2006-01-02")}
			}
			off += 4
		case ColTimestamp:
			off = alignUp(off, 8)
			if off+8 > len(tup) {
				return fmt.Errorf("%w: timestamp out of bounds", ErrInvalidPage)
			}
			if want {
				usec := int64(binary.LittleEndian.Uint64(tup[off : off+8]))
				t := pgEpoch.Add(time.Duration(usec) * time.Microsecond)
				dst[i] = HeapVal{Kind: ValString, Str: t.Format("2006-01-02 15:04:05.999999")}
			}
			off += 8
		case ColUUID:
			if off+16 > len(tup) {
				return fmt.Errorf("%w: uuid out of bounds", ErrInvalidPage)
			}
			if want {
				u := tup[off : off+16]
				dst[i] = HeapVal{Kind: ValString, Str: fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
					binary.BigEndian.Uint32(u[0:4]),
					binary.BigEndian.Uint16(u[4:6]),
					binary.BigEndian.Uint16(u[6:8]),
					binary.BigEndian.Uint16(u[8:10]),
					u[10:16])}
			}
			off += 16
		case ColInt2Vector:
			raw, nextOff, err := decodeVarlenaWithTOAST(tup, off, want, toastFn)
			if err != nil {
				return err
			}
			if want {
				if copyStrings && len(raw) > 0 {
					cp := make([]byte, len(raw))
					copy(cp, raw)
					dst[i] = HeapVal{Kind: ValBytes, Raw: cp}
				} else {
					dst[i] = HeapVal{Kind: ValBytes, Raw: raw}
				}
			}
			off = nextOff
		case ColNumeric, ColInet, ColCIDR,
			ColInt2Array, ColInt4Array, ColInt8Array, ColTextArray, ColBoolArray, ColFloat4Array, ColFloat8Array:
			raw, nextOff, err := decodeVarlenaWithTOAST(tup, off, want, toastFn)
			if err != nil {
				return err
			}
			if want {
				s, derr := decodeExtColType(ct, raw)
				if derr != nil {
					return derr
				}
				dst[i] = HeapVal{Kind: ValString, Str: s}
			}
			off = nextOff
		default:
			return fmt.Errorf("%w: column %d type %d", ErrUnsupportedType, i, ct)
		}
	}
	return nil
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

// BTMetaPageData represents PostgreSQL's BTMetaPageData stored on block 0 of a B-tree index.
type BTMetaPageData struct {
	Magic     uint32
	Version   uint32
	Root      uint32
	Level     uint32
	FastRoot  uint32
	FastLevel uint32
}

// ParseBTMetaPage decodes BTMetaPageData from block 0 of a PostgreSQL B-tree index.
func ParseBTMetaPage(page []byte, hdr PageHeader) (BTMetaPageData, error) {
	if int(hdr.Lower) < PGPageHeaderSize+24 || len(page) < PGPageHeaderSize+24 {
		return BTMetaPageData{}, fmt.Errorf("%w: btree metapage too short (lower=%d)", ErrInvalidPage, hdr.Lower)
	}
	m := page[PGPageHeaderSize:]
	meta := BTMetaPageData{
		Magic:     binary.LittleEndian.Uint32(m[0:4]),
		Version:   binary.LittleEndian.Uint32(m[4:8]),
		Root:      binary.LittleEndian.Uint32(m[8:12]),
		Level:     binary.LittleEndian.Uint32(m[12:16]),
		FastRoot:  binary.LittleEndian.Uint32(m[16:20]),
		FastLevel: binary.LittleEndian.Uint32(m[20:24]),
	}
	if meta.Magic != PGBTMetaMagic {
		return meta, fmt.Errorf("%w: invalid btree meta magic 0x%x", ErrInvalidPage, meta.Magic)
	}
	return meta, nil
}

// BTree block/flag/tuple constants.
const (
	BTPNone        uint32 = 0
	BTPDeleted     uint16 = 1 << 2
	BTPHalfDead    uint16 = 1 << 4
	BTPSplitEnd    uint16 = 1 << 5
	BTPHasGarbage  uint16 = 1 << 6
	BTPIncompleteSplit uint16 = 1 << 7

	PGSizeOfIndexTupleHdr = 8
	IndexSizeMask         uint16 = 0x1FFF
	IndexAltTIDMask       uint16 = 0x2000
	IndexVarWidthMask     uint16 = 0x4000
	IndexNullMask         uint16 = 0x8000

	BTIsPosting     uint16 = 0x2000
	BTPivotHeapTID  uint16 = 0x1000
	BTStatusMask    uint16 = 0xF000
)

// ItemPointer represents a PostgreSQL 6-byte ItemPointerData (BlockNumber + OffsetNumber).
type ItemPointer struct {
	BlockNum uint32
	OffNum   uint16
}

// DecodeItemPointer reads a 6-byte PostgreSQL ItemPointerData (bi_hi, bi_lo, ip_posid).
func DecodeItemPointer(b []byte) ItemPointer {
	hi := uint32(binary.LittleEndian.Uint16(b[0:2]))
	lo := uint32(binary.LittleEndian.Uint16(b[2:4]))
	pos := binary.LittleEndian.Uint16(b[4:6])
	return ItemPointer{
		BlockNum: (hi << 16) | lo,
		OffNum:   pos,
	}
}

// ParseHeapTupleCTID extracts t_ctid from a 23-byte HeapTupleHeaderData slice.
func ParseHeapTupleCTID(tup []byte) ItemPointer {
	if len(tup) < 18 {
		return ItemPointer{}
	}
	return DecodeItemPointer(tup[12:18])
}

// IndexTupleHeader describes a parsed PostgreSQL 8-byte IndexTupleData header.
type IndexTupleHeader struct {
	TID           ItemPointer
	Info          uint16
	DataOff       int
	KeyEnd        int
	NullBits      []byte
	IsPosting     bool
	PostingOff    int
	NumPosting    int
	IsPivot       bool
	NumKeyAtts    int // -1 if all key attributes are present
}

// ParseIndexTupleHeader decodes an IndexTupleData header from itup.
func ParseIndexTupleHeader(itup []byte, isLeaf bool, indNKeyAtts int) (IndexTupleHeader, error) {
	if len(itup) < PGSizeOfIndexTupleHdr {
		return IndexTupleHeader{}, fmt.Errorf("%w: index tuple too short (%d)", ErrInvalidPage, len(itup))
	}
	tid := DecodeItemPointer(itup[0:6])
	tinfo := binary.LittleEndian.Uint16(itup[6:8])
	tupSize := int(tinfo & IndexSizeMask)
	if tupSize < PGSizeOfIndexTupleHdr || tupSize > len(itup) {
		tupSize = len(itup)
	}

	h := IndexTupleHeader{
		TID:        tid,
		Info:       tinfo,
		DataOff:    PGSizeOfIndexTupleHdr,
		KeyEnd:     tupSize,
		NumKeyAtts: indNKeyAtts,
	}

	if (tinfo & IndexNullMask) != 0 {
		// IndexAttributeBitMapData is 4 bytes (INDEX_MAX_KEYS=32 bits), so header + bitmap is 12 bytes,
		// MAXALIGN(12) = 16 bytes!
		if tupSize < 16 {
			return h, fmt.Errorf("%w: index tuple with null bitmap too short (%d)", ErrInvalidPage, tupSize)
		}
		h.NullBits = itup[8:12]
		h.DataOff = 16
	}

	if (tinfo & IndexAltTIDMask) != 0 {
		if isLeaf && (tid.OffNum&BTIsPosting) != 0 {
			h.IsPosting = true
			h.PostingOff = int(tid.BlockNum)
			h.NumPosting = int(tid.OffNum &^ BTStatusMask)
			if h.PostingOff < h.DataOff || h.PostingOff+h.NumPosting*6 > tupSize {
				return h, fmt.Errorf("%w: invalid posting list off=%d n=%d size=%d", ErrInvalidPage, h.PostingOff, h.NumPosting, tupSize)
			}
			h.KeyEnd = h.PostingOff
		} else {
			// Pivot tuple (internal page downcast or leaf high key)
			h.IsPivot = true
			nkeys := int(tid.OffNum &^ BTStatusMask)
			if nkeys >= 0 && nkeys <= indNKeyAtts {
				h.NumKeyAtts = nkeys
			}
			if (tid.OffNum&BTPivotHeapTID) != 0 && h.KeyEnd >= h.DataOff+6 {
				h.KeyEnd -= 6
			}
		}
	} else if !isLeaf {
		h.IsPivot = true
	}

	return h, nil
}

// DecodeIndexTuplePostingTIDs appends all heap TIDs referenced by a leaf IndexTuple into dst.
func DecodeIndexTuplePostingTIDs(itup []byte, h IndexTupleHeader, dst []ItemPointer) []ItemPointer {
	if !h.IsPosting {
		return append(dst, h.TID)
	}
	for i := 0; i < h.NumPosting; i++ {
		off := h.PostingOff + i*6
		if off+6 <= len(itup) {
			dst = append(dst, DecodeItemPointer(itup[off:off+6]))
		}
	}
	return dst
}

// DecodeIndexTupleColumnsInto decodes up to h.NumKeyAtts key attributes from an IndexTuple into dst.
// Attributes truncated by suffix truncation (index >= h.NumKeyAtts) are not present; the caller uses h.NumKeyAtts.
func DecodeIndexTupleColumnsInto(itup []byte, h IndexTupleHeader, keyColTypes []ColumnType, dst []HeapVal) (int, error) {
	n := len(keyColTypes)
	if h.NumKeyAtts >= 0 && h.NumKeyAtts < n {
		n = h.NumKeyAtts
	}
	if n == 0 {
		return 0, nil
	}
	keyData := itup[:h.KeyEnd]
	off := h.DataOff
	for i := 0; i < n; i++ {
		if (h.Info & IndexNullMask) != 0 {
			if i/8 >= len(h.NullBits) {
				return i, fmt.Errorf("%w: index null bitmap too short for key %d", ErrInvalidPage, i)
			}
			if (h.NullBits[i/8] & (1 << uint(i%8))) == 0 {
				dst[i] = HeapVal{Kind: ValNull}
				continue
			}
		}
		ct := keyColTypes[i]
		switch ct {
		case ColBool:
			if off+1 > len(keyData) {
				return i, fmt.Errorf("%w: idx bool out of bounds", ErrInvalidPage)
			}
			var b int64
			if keyData[off] != 0 {
				b = 1
			}
			dst[i] = HeapVal{Kind: ValBool, I64: b}
			off++
		case ColChar:
			if off+1 > len(keyData) {
				return i, fmt.Errorf("%w: idx char out of bounds", ErrInvalidPage)
			}
			dst[i] = HeapVal{Kind: ValByte, I64: int64(keyData[off])}
			off++
		case ColInt2:
			off = alignUp(off, 2)
			if off+2 > len(keyData) {
				return i, fmt.Errorf("%w: idx int2 out of bounds", ErrInvalidPage)
			}
			dst[i] = HeapVal{Kind: ValInt64, I64: int64(int16(binary.LittleEndian.Uint16(keyData[off : off+2])))}
			off += 2
		case ColInt4:
			off = alignUp(off, 4)
			if off+4 > len(keyData) {
				return i, fmt.Errorf("%w: idx int4 out of bounds", ErrInvalidPage)
			}
			dst[i] = HeapVal{Kind: ValInt64, I64: int64(int32(binary.LittleEndian.Uint32(keyData[off : off+4])))}
			off += 4
		case ColOID:
			off = alignUp(off, 4)
			if off+4 > len(keyData) {
				return i, fmt.Errorf("%w: idx oid out of bounds", ErrInvalidPage)
			}
			dst[i] = HeapVal{Kind: ValInt64, I64: int64(binary.LittleEndian.Uint32(keyData[off : off+4]))}
			off += 4
		case ColInt8:
			off = alignUp(off, 8)
			if off+8 > len(keyData) {
				return i, fmt.Errorf("%w: idx int8 out of bounds", ErrInvalidPage)
			}
			dst[i] = HeapVal{Kind: ValInt64, I64: int64(binary.LittleEndian.Uint64(keyData[off : off+8]))}
			off += 8
		case ColFloat4:
			off = alignUp(off, 4)
			if off+4 > len(keyData) {
				return i, fmt.Errorf("%w: idx float4 out of bounds", ErrInvalidPage)
			}
			dst[i] = HeapVal{Kind: ValFloat64, F64: float64(math.Float32frombits(binary.LittleEndian.Uint32(keyData[off : off+4])))}
			off += 4
		case ColFloat8:
			off = alignUp(off, 8)
			if off+8 > len(keyData) {
				return i, fmt.Errorf("%w: idx float8 out of bounds", ErrInvalidPage)
			}
			dst[i] = HeapVal{Kind: ValFloat64, F64: math.Float64frombits(binary.LittleEndian.Uint64(keyData[off : off+8]))}
			off += 8
		case ColName:
			if off+64 > len(keyData) {
				return i, fmt.Errorf("%w: idx name out of bounds", ErrInvalidPage)
			}
			raw := keyData[off : off+64]
			end := 0
			for end < 64 && raw[end] != 0 {
				end++
			}
			var s string
			if end > 0 {
				s = unsafe.String(&raw[0], end)
			}
			dst[i] = HeapVal{Kind: ValString, Str: s}
			off += 64
		case ColText:
			raw, nextOff, err := decodeVarlena(keyData, off, true)
			if err != nil {
				return i, err
			}
			var s string
			if len(raw) > 0 {
				s = unsafe.String(&raw[0], len(raw))
			}
			dst[i] = HeapVal{Kind: ValString, Str: s}
			off = nextOff
		case ColBytea:
			raw, nextOff, err := decodeVarlena(keyData, off, true)
			if err != nil {
				return i, err
			}
			dst[i] = HeapVal{Kind: ValBytes, Raw: raw}
			off = nextOff
		case ColDate:
			off = alignUp(off, 4)
			if off+4 > len(keyData) {
				return i, fmt.Errorf("%w: idx date out of bounds", ErrInvalidPage)
			}
			days := int32(binary.LittleEndian.Uint32(keyData[off : off+4]))
			dst[i] = HeapVal{Kind: ValString, Str: pgEpoch.AddDate(0, 0, int(days)).Format("2006-01-02")}
			off += 4
		case ColTimestamp:
			off = alignUp(off, 8)
			if off+8 > len(keyData) {
				return i, fmt.Errorf("%w: idx timestamp out of bounds", ErrInvalidPage)
			}
			usec := int64(binary.LittleEndian.Uint64(keyData[off : off+8]))
			t := pgEpoch.Add(time.Duration(usec) * time.Microsecond)
			dst[i] = HeapVal{Kind: ValString, Str: t.Format("2006-01-02 15:04:05.999999")}
			off += 8
		case ColUUID:
			if off+16 > len(keyData) {
				return i, fmt.Errorf("%w: idx uuid out of bounds", ErrInvalidPage)
			}
			u := keyData[off : off+16]
			dst[i] = HeapVal{Kind: ValString, Str: fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
				binary.BigEndian.Uint32(u[0:4]),
				binary.BigEndian.Uint16(u[4:6]),
				binary.BigEndian.Uint16(u[6:8]),
				binary.BigEndian.Uint16(u[8:10]),
				u[10:16])}
			off += 16
		default:
			return i, fmt.Errorf("%w: idx column %d type %d", ErrUnsupportedType, i, ct)
		}
	}
	return n, nil
}

