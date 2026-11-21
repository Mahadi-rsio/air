package readadapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

const (
	// VARTAG_ONDISK is the 1-byte varlena tag for an 18-byte external on-disk TOAST pointer (1B header + 1B tag + 16B varatt_external).
	PGVarTagOnDisk uint8 = 0x12

	// PostgreSQL varlena/TOAST compression methods (top 2 bits of va_extinfo / va_tcinfo).
	PGToastCompPGLZ uint32 = 0
	PGToastCompLZ4  uint32 = 1

	maxTOASTValueBytes = 64 * 1024 * 1024
)

// TOASTPointer represents a parsed 18-byte PostgreSQL varatt_external on-disk TOAST pointer.
type TOASTPointer struct {
	RawSize    int    // Uncompressed payload size in bytes (va_rawsize - VARHDRSZ(4))
	ExtSize    int    // Stored external size in bytes (lower 30 bits of va_extinfo)
	CompMethod uint32 // Compression method: 0 = PGLZ, 1 = LZ4 (upper 2 bits of va_extinfo)
	ValueID    uint32 // chunk_id (Oid) in the TOAST relation
	ToastRelID uint32 // Oid of the TOAST relation (pg_class.oid)
}

// IsCompressed reports whether the external TOAST value is compressed.
func (p TOASTPointer) IsCompressed() bool {
	return p.ExtSize < p.RawSize
}

// ParseTOASTPointer decodes an 18-byte external TOAST pointer starting at b[0] == 0x01.
func ParseTOASTPointer(b []byte) (TOASTPointer, error) {
	if len(b) < 18 || b[0] != 0x01 {
		return TOASTPointer{}, fmt.Errorf("%w: invalid toast pointer length %d", ErrInvalidPage, len(b))
	}
	tag := b[1]
	if tag != PGVarTagOnDisk {
		return TOASTPointer{}, fmt.Errorf("%w: unsupported external varlena tag 0x%02x", ErrTOASTUnsupported, tag)
	}
	rawWithHdr := int(int32(binary.LittleEndian.Uint32(b[2:6])))
	rawSize := rawWithHdr - 4
	extInfo := binary.LittleEndian.Uint32(b[6:10])
	extSize := int(extInfo & 0x3FFFFFFF)
	cmethod := extInfo >> 30
	valueID := binary.LittleEndian.Uint32(b[10:14])
	toastRelID := binary.LittleEndian.Uint32(b[14:18])

	if rawSize < 0 || rawSize > maxTOASTValueBytes || extSize < 0 || extSize > rawSize || valueID == 0 || toastRelID == 0 {
		return TOASTPointer{}, fmt.Errorf("%w: malformed toast pointer (raw=%d ext=%d val=%d rel=%d)",
			ErrTOASTUnsupported, rawSize, extSize, valueID, toastRelID)
	}
	return TOASTPointer{
		RawSize:    rawSize,
		ExtSize:    extSize,
		CompMethod: cmethod,
		ValueID:    valueID,
		ToastRelID: toastRelID,
	}, nil
}

// TOASTResolverFunc resolves an external TOASTPointer into its reconstructed, decompressed payload bytes.
type TOASTResolverFunc func(ptr TOASTPointer) ([]byte, error)

// pgLZ4Decompress decompresses a raw LZ4 block (as produced by PostgreSQL's LZ4_compress_default)
// into a slice of exact length rawSize.
func pgLZ4Decompress(src []byte, rawSize int) ([]byte, error) {
	if rawSize < 0 || rawSize > maxTOASTValueBytes {
		return nil, fmt.Errorf("%w: invalid lz4 rawSize=%d", ErrInvalidPage, rawSize)
	}
	if rawSize == 0 {
		if len(src) == 0 {
			return []byte{}, nil
		}
		return nil, fmt.Errorf("%w: non-empty lz4 block for rawSize=0", ErrInvalidPage)
	}
	dst := make([]byte, 0, rawSize)
	sp := 0
	for sp < len(src) {
		token := src[sp]
		sp++

		// 1. Literal length
		litLen := int(token >> 4)
		if litLen == 15 {
			for {
				if sp >= len(src) {
					return nil, fmt.Errorf("%w: truncated lz4 literal length", ErrInvalidPage)
				}
				b := src[sp]
				sp++
				litLen += int(b)
				if b != 255 {
					break
				}
			}
		}
		if litLen < 0 || sp+litLen > len(src) || len(dst)+litLen > rawSize {
			return nil, fmt.Errorf("%w: lz4 literal length out of bounds (lit=%d sp=%d src=%d dst=%d raw=%d)",
				ErrInvalidPage, litLen, sp, len(src), len(dst), rawSize)
		}
		dst = append(dst, src[sp:sp+litLen]...)
		sp += litLen

		// The last sequence in an LZ4 raw block ends immediately after the literals.
		if sp == len(src) {
			break
		}

		// 2. Match offset (2-byte little-endian)
		if sp+2 > len(src) {
			return nil, fmt.Errorf("%w: truncated lz4 match offset", ErrInvalidPage)
		}
		matchOff := int(binary.LittleEndian.Uint16(src[sp : sp+2]))
		sp += 2
		if matchOff <= 0 || matchOff > len(dst) {
			return nil, fmt.Errorf("%w: invalid lz4 match offset=%d dstLen=%d", ErrInvalidPage, matchOff, len(dst))
		}

		// 3. Match length (minmatch = 4)
		matchLen := int(token&0x0F) + 4
		if (token & 0x0F) == 15 {
			for {
				if sp >= len(src) {
					return nil, fmt.Errorf("%w: truncated lz4 match length", ErrInvalidPage)
				}
				b := src[sp]
				sp++
				matchLen += int(b)
				if b != 255 {
					break
				}
			}
		}
		if matchLen < 4 || len(dst)+matchLen > rawSize {
			return nil, fmt.Errorf("%w: lz4 match length out of bounds (match=%d dst=%d raw=%d)",
				ErrInvalidPage, matchLen, len(dst), rawSize)
		}
		// Copy match bytes (supports overlapping RLE copies when matchOff < matchLen).
		for k := 0; k < matchLen; k++ {
			dst = append(dst, dst[len(dst)-matchOff])
		}
	}
	if len(dst) != rawSize {
		return nil, fmt.Errorf("%w: lz4 decompressed len=%d want=%d", ErrInvalidPage, len(dst), rawSize)
	}
	return dst, nil
}

type toastChunk struct {
	seq  int32
	data []byte
}

// resolveTOASTPointer reads the TOAST relation pages via Adapter.ReadPages at cur.snap,
// filters visible chunks by chunk_id (ptr.ValueID), sorts by chunk_seq, validates
// contiguous sequence numbers and total byte length, and decompresses PGLZ/LZ4 if needed.
func (cur *pgVTabCursor) resolveTOASTPointer(ptr TOASTPointer) ([]byte, error) {
	if cur == nil || cur.spec == nil || cur.spec.Adapter == nil || cur.snap == nil {
		return nil, ErrTOASTUnsupported
	}
	ctx := cur.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	toastRelNum := cur.spec.Table.ToastRelNumber
	if toastRelNum == 0 || (cur.spec.Table.ToastRelOID != 0 && ptr.ToastRelID != cur.spec.Table.ToastRelOID) {
		// Look up ptr.ToastRelID in pg_class at cur.snap to resolve its current relfilenode.
		if resolved, err := resolveRelFileNodeByOID(ctx, cur.spec.Adapter, cur.spec.CLOG, cur.snap, cur.spec.Table.SpcOID, cur.spec.Table.DbOID, ptr.ToastRelID); err == nil && resolved != 0 {
			toastRelNum = resolved
		} else if toastRelNum == 0 {
			toastRelNum = ptr.ToastRelID
		}
	}
	if toastRelNum == 0 {
		return nil, ErrTOASTUnsupported
	}

	if cur.toastRel == nil || cur.toastRel.Spec.RelNumber != toastRelNum {
		if cur.toastRel != nil {
			_ = cur.spec.Adapter.CloseRelation(cur.toastRel)
			cur.toastRel = nil
		}
		rel, err := cur.spec.Adapter.OpenRelation(ctx, RelationSpec{
			TenantID:   cur.spec.TenantID,
			Generation: cur.spec.Generation,
			SpcOID:     cur.spec.Table.SpcOID,
			DbOID:      cur.spec.Table.DbOID,
			RelNumber:  toastRelNum,
			ForkNum:    0,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: open toast relation %d: %v", ErrTOASTUnsupported, toastRelNum, err)
		}
		cur.toastRel = rel
	}

	meta, err := cur.spec.Adapter.GetRelationMetadata(ctx, cur.toastRel, cur.snap)
	if err != nil {
		return nil, fmt.Errorf("%w: get toast relation metadata %d: %v", ErrTOASTUnsupported, toastRelNum, err)
	}
	if !meta.Exists || meta.NBlocks == 0 {
		return nil, fmt.Errorf("%w: toast relation %d does not exist or has 0 blocks", ErrTOASTUnsupported, toastRelNum)
	}

	var chunks []toastChunk
	var lpBuf []LinePointer
	const batchBlocks uint32 = 16
	for blk := uint32(0); blk < meta.NBlocks; blk += batchBlocks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n := batchBlocks
		if blk+n > meta.NBlocks {
			n = meta.NBlocks - blk
		}
		pages, err := cur.spec.Adapter.ReadPages(ctx, cur.toastRel, cur.snap, blk, n)
		if err != nil {
			return nil, fmt.Errorf("%w: read toast pages: %v", ErrTOASTUnsupported, err)
		}
		for pi, pd := range pages {
			_ = pi
			vtabPagesRead.Add(1)
			if IsZeroPage(pd.Data) {
				continue
			}
			phdr, err := ParsePageHeader(pd.Data)
			if err != nil {
				return nil, fmt.Errorf("%w: invalid toast page header: %v", ErrTOASTUnsupported, err)
			}
			lpBuf = ParseLinePointersInto(pd.Data, phdr, lpBuf)
			for _, lp := range lpBuf {
				if lp.Flags != LPNormal {
					continue
				}
				if err := ValidateLinePointer(lp, phdr); err != nil {
					return nil, fmt.Errorf("%w: invalid toast line pointer: %v", ErrTOASTUnsupported, err)
				}
				tup := pd.Data[lp.Offset : lp.Offset+lp.Length]
				thdr, err := ParseHeapTupleHeader(tup)
				if err != nil {
					return nil, fmt.Errorf("%w: invalid toast tuple header: %v", ErrTOASTUnsupported, err)
				}
				if int(thdr.Infomask2&HeapNattsMask) < 3 || (thdr.Infomask&HeapHasNull) != 0 {
					continue
				}
				off := int(thdr.Hoff)
				off = alignUp(off, 4)
				if off+8 > len(tup) {
					return nil, fmt.Errorf("%w: toast tuple too short for chunk_id/chunk_seq", ErrTOASTUnsupported)
				}
				// Fast filter on chunk_id (attribute 1: Oid) before MVCC or varlena decode!
				cid := binary.LittleEndian.Uint32(tup[off : off+4])
				if cid != ptr.ValueID {
					continue
				}
				vis, err := IsVisibleWithCLOG(ctx, thdr, cur.snap, cur.spec.CLOG)
				if err != nil {
					return nil, err
				}
				if !vis {
					continue
				}
				cseq := int32(binary.LittleEndian.Uint32(tup[off+4 : off+8]))
				cdata, _, err := decodeVarlenaWithTOAST(tup, off+8, true, nil)
				if err != nil {
					return nil, fmt.Errorf("%w: decode toast chunk_data: %v", ErrTOASTUnsupported, err)
				}
				cp := make([]byte, len(cdata))
				copy(cp, cdata)
				chunks = append(chunks, toastChunk{
					seq:  cseq,
					data: cp,
				})
			}
		}
	}

	if len(chunks) == 0 {
		return nil, fmt.Errorf("%w: no visible toast chunks found for chunk_id=%d in rel=%d",
			ErrTOASTUnsupported, ptr.ValueID, toastRelNum)
	}

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].seq < chunks[j].seq
	})

	reconstructed := make([]byte, 0, ptr.ExtSize)
	for i, ch := range chunks {
		if ch.seq != int32(i) {
			seqs := make([]int32, len(chunks))
			for j, c := range chunks {
				seqs[j] = c.seq
			}
			return nil, fmt.Errorf("%w: non-contiguous toast chunk_seq at %d (got %d, chunk_id=%d nblocks=%d seqs=%v)",
				ErrTOASTUnsupported, i, ch.seq, ptr.ValueID, meta.NBlocks, seqs)
		}
		reconstructed = append(reconstructed, ch.data...)
	}

	if len(reconstructed) != ptr.ExtSize {
		return nil, fmt.Errorf("%w: reconstructed toast length %d != expected extsize %d",
			ErrTOASTUnsupported, len(reconstructed), ptr.ExtSize)
	}

	if !ptr.IsCompressed() {
		if len(reconstructed) != ptr.RawSize {
			return nil, fmt.Errorf("%w: uncompressed toast length %d != rawsize %d",
				ErrTOASTUnsupported, len(reconstructed), ptr.RawSize)
		}
		return reconstructed, nil
	}

	switch ptr.CompMethod {
	case PGToastCompPGLZ:
		out, err := pgLZDecompress(reconstructed, ptr.RawSize)
		if err != nil {
			return nil, fmt.Errorf("%w: toast pglz decompress: %v", ErrTOASTUnsupported, err)
		}
		return out, nil
	case PGToastCompLZ4:
		out, err := pgLZ4Decompress(reconstructed, ptr.RawSize)
		if err != nil {
			return nil, fmt.Errorf("%w: toast lz4 decompress: %v", ErrTOASTUnsupported, err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%w: unsupported toast compression method %d", ErrTOASTUnsupported, ptr.CompMethod)
	}
}

func resolveRelFileNodeByOID(
	ctx context.Context,
	adapter *Adapter,
	clog *CLOGResolver,
	snap *ReadSnapshot,
	spcOID, dbOID, targetRelOID uint32,
) (uint32, error) {
	rows, err := scanCatalogRelation(ctx, adapter, clog, snap, spcOID, dbOID, PGClassRelID, pgClassCols)
	if err != nil {
		return 0, err
	}
	for _, r := range rows {
		oid, _ := r[0].(uint32)
		if oid == targetRelOID {
			node, _ := r[7].(uint32)
			if node == 0 {
				node = oid
			}
			return node, nil
		}
	}
	return 0, nil
}

// PostgreSQL JSONB binary format constants (src/include/utils/jsonb.h).
const (
	jbCMask   uint32 = 0x0FFFFFFF
	jbFScalar uint32 = 0x10000000
	jbFObject uint32 = 0x20000000
	jbFArray  uint32 = 0x40000000

	jentryOffLenMask  uint32 = 0x0FFFFFFF
	jentryTypeMask    uint32 = 0x70000000
	jentryHasOff      uint32 = 0x80000000
	jentryIsString    uint32 = 0x00000000
	jentryIsNumeric   uint32 = 0x10000000
	jentryIsBoolFalse uint32 = 0x20000000
	jentryIsBoolTrue  uint32 = 0x30000000
	jentryIsNull      uint32 = 0x40000000
	jentryIsContainer uint32 = 0x50000000
)

// decodePGJSONBToText converts a PostgreSQL binary JSONB varlena payload into canonical JSON text
// matching PostgreSQL's jsonb_out formatting.
func decodePGJSONBToText(data []byte) (string, error) {
	var buf bytes.Buffer
	if err := writeJSONBContainer(data, 0, &buf); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func getJSONBEntryBounds(jentries []uint32, idx int) (startOff int, length int) {
	// Start offset is the end offset of idx-1 (or 0 if idx == 0).
	if idx > 0 {
		k := idx - 1
		var acc uint32
		for k >= 0 {
			je := jentries[k]
			acc += je & jentryOffLenMask
			if (je & jentryHasOff) != 0 {
				break
			}
			k--
		}
		startOff = int(acc)
	}
	je := jentries[idx]
	if (je & jentryHasOff) != 0 {
		endOff := int(je & jentryOffLenMask)
		length = endOff - startOff
	} else {
		length = int(je & jentryOffLenMask)
	}
	return startOff, length
}

func writeJSONBContainer(data []byte, baseAlignOff int, out *bytes.Buffer) error {
	if len(data) < 4 {
		return fmt.Errorf("%w: jsonb container too short (%d)", ErrInvalidPage, len(data))
	}
	hdr := binary.LittleEndian.Uint32(data[0:4])
	count := int(hdr & jbCMask)
	isScalar := (hdr & jbFScalar) != 0
	isObject := (hdr & jbFObject) != 0
	isArray := (hdr & jbFArray) != 0
	if !isObject && !isArray {
		return fmt.Errorf("%w: invalid jsonb container header 0x%08x", ErrInvalidPage, hdr)
	}
	nEntries := count
	if isObject {
		nEntries = count * 2
	}
	dataStart := 4 + nEntries*4
	if dataStart > len(data) {
		return fmt.Errorf("%w: jsonb jentries exceed container size", ErrInvalidPage)
	}
	jentries := make([]uint32, nEntries)
	for i := 0; i < nEntries; i++ {
		jentries[i] = binary.LittleEndian.Uint32(data[4+i*4 : 8+i*4])
	}
	payload := data[dataStart:]

	writeElem := func(idx int) error {
		je := jentries[idx]
		typ := je & jentryTypeMask
		off, length := getJSONBEntryBounds(jentries, idx)
		if off < 0 || length < 0 || off+length > len(payload) {
			return fmt.Errorf("%w: jsonb entry %d bounds [%d:%d] out of range %d", ErrInvalidPage, idx, off, off+length, len(payload))
		}
		raw := payload[off : off+length]
		switch typ {
		case jentryIsNull:
			out.WriteString("null")
		case jentryIsBoolFalse:
			out.WriteString("false")
		case jentryIsBoolTrue:
			out.WriteString("true")
		case jentryIsString:
			encoded, err := json.Marshal(string(raw))
			if err != nil {
				return err
			}
			out.Write(encoded)
		case jentryIsNumeric:
			// PostgreSQL aligns numeric and container payloads to 4 bytes relative to the root JsonbContainer!
			relOff := baseAlignOff + dataStart + off
			pad := (4 - (relOff & 3)) & 3
			if pad > len(raw) {
				pad = 0
			}
			numStr, err := decodePGNumericToString(raw[pad:])
			if err != nil {
				return err
			}
			out.WriteString(numStr)
		case jentryIsContainer:
			relOff := baseAlignOff + dataStart + off
			pad := (4 - (relOff & 3)) & 3
			if pad > len(raw) {
				pad = 0
			}
			if err := writeJSONBContainer(raw[pad:], relOff+pad, out); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: unknown jsonb jentry type 0x%08x", ErrInvalidPage, typ)
		}
		return nil
	}

	if isScalar && nEntries == 1 {
		return writeElem(0)
	}
	if isArray {
		out.WriteByte('[')
		for i := 0; i < count; i++ {
			if i > 0 {
				out.WriteString(", ")
			}
			if err := writeElem(i); err != nil {
				return err
			}
		}
		out.WriteByte(']')
		return nil
	}
	// Object: PostgreSQL stores ALL keys first (0 .. count-1) followed by ALL values (count .. 2*count-1)
	// wait: in PG jsonb, are keys stored at 0..count-1 and values at count..2*count-1, or interleaved?
	// In PostgreSQL src/include/utils/jsonb.h:
	// "In an object, there are 2*count JEntries: the first 'count' JEntries describe the keys,
	//  and the next 'count' JEntries describe the values!"
	out.WriteByte('{')
	for i := 0; i < count; i++ {
		if i > 0 {
			out.WriteString(", ")
		}
		if err := writeElem(i); err != nil {
			return err
		}
		out.WriteString(": ")
		if err := writeElem(count + i); err != nil {
			return err
		}
	}
	out.WriteByte('}')
	return nil
}

// decodePGNumericToString decodes a PostgreSQL NumericData varlena (with 1B or 4B varlena header,
// as embedded in JSONB JENTRY_ISNUMERIC) into its decimal string representation.
func decodePGNumericToString(b []byte) (string, error) {
	if len(b) < 2 {
		return "", fmt.Errorf("%w: pg numeric too short (%d)", ErrInvalidPage, len(b))
	}
	if (b[0] & 0x01) != 0 {
		b = b[1:]
	} else {
		if len(b) < 4 {
			return "", fmt.Errorf("%w: pg numeric varlena4 too short (%d)", ErrInvalidPage, len(b))
		}
		b = b[4:]
	}
	if len(b) < 2 {
		return "", fmt.Errorf("%w: pg numeric body too short (%d)", ErrInvalidPage, len(b))
	}
	nHeader := binary.LittleEndian.Uint16(b[0:2])
	flagBits := nHeader & 0xF000
	var weight int16
	var sign uint16
	var dscale int
	var digitsOff int

	if (flagBits & 0x8000) != 0 && flagBits != 0xC000 {
		// NUMERIC_SHORT (0x8000)
		sign = nHeader & 0x2000
		dscale = int((nHeader & 0x1F80) >> 7)
		w := int(nHeader & 0x007F)
		if (w & 0x0040) != 0 {
			w |= ^0x007F
		}
		weight = int16(w)
		digitsOff = 2
	} else if flagBits == 0xC000 {
		// NUMERIC_SPECIAL (NaN / Inf)
		return "null", nil
	} else {
		if len(b) < 6 {
			return "", fmt.Errorf("%w: long pg numeric too short (%d)", ErrInvalidPage, len(b))
		}
		weight = int16(binary.LittleEndian.Uint16(b[0:2]))
		sign = nHeader & 0xC000
		dscale = int(nHeader & 0x3FFF)
		digitsOff = 4
	}

	ndigits := (len(b) - digitsOff) / 2
	if ndigits == 0 {
		if dscale > 0 {
			return "0." + string(bytes.Repeat([]byte{'0'}, dscale)), nil
		}
		return "0", nil
	}
	digits := make([]int16, ndigits)
	for i := 0; i < ndigits; i++ {
		digits[i] = int16(binary.LittleEndian.Uint16(b[digitsOff+i*2 : digitsOff+i*2+2]))
	}

	var sb bytes.Buffer
	if sign != 0 {
		sb.WriteByte('-')
	}
	// Integral part (base-10000 digits 0 .. weight)
	if weight < 0 {
		sb.WriteByte('0')
	} else {
		for d := 0; d <= int(weight); d++ {
			var dig int16
			if d < ndigits {
				dig = digits[d]
			}
			if d == 0 {
				sb.WriteString(strconv.Itoa(int(dig)))
			} else {
				sb.WriteString(fmt.Sprintf("%04d", dig))
			}
		}
	}
	// Fractional part
	if dscale > 0 {
		sb.WriteByte('.')
		fracChars := 0
		for d := int(weight) + 1; fracChars < dscale; d++ {
			var dig int16
			if d >= 0 && d < ndigits {
				dig = digits[d]
			}
			s := fmt.Sprintf("%04d", dig)
			rem := dscale - fracChars
			if rem < 4 {
				sb.WriteString(s[:rem])
				fracChars += rem
			} else {
				sb.WriteString(s)
				fracChars += 4
			}
		}
	}
	return sb.String(), nil
}
