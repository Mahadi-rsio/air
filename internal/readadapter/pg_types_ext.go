package readadapter

import (
	"encoding/binary"
	"fmt"
	"math"
	"net/netip"
	"strconv"
	"strings"
)

// Additional PostgreSQL well-known type OIDs for Phase 18.
const (
	PGOIDChar        uint32 = 18
	PGOIDName        uint32 = 19
	PGOIDInt2        uint32 = 21
	PGOIDInt2Vector  uint32 = 22
	PGOIDOID         uint32 = 26
	PGOIDJSON        uint32 = 114
	PGOIDXML         uint32 = 142
	PGOIDCIDR        uint32 = 650
	PGOIDFloat4      uint32 = 700
	PGOIDInet        uint32 = 869
	PGOIDBoolArray   uint32 = 1000
	PGOIDInt2Array   uint32 = 1005
	PGOIDInt4Array   uint32 = 1007
	PGOIDTextArray   uint32 = 1009
	PGOIDVarchar     uint32 = 1043
	PGOIDBpchar      uint32 = 1042
	PGOIDDate        uint32 = 1082
	PGOIDTimestamp   uint32 = 1114
	PGOIDTimestamptz uint32 = 1184
	PGOIDVarcharArr  uint32 = 1015
	PGOIDInt8Array   uint32 = 1016
	PGOIDFloat4Array uint32 = 1021
	PGOIDFloat8Array uint32 = 1022
	PGOIDUUID        uint32 = 2950
	PGOIDJSONB       uint32 = 3802
)

const (
	pgNumericSignMask   uint16 = 0xC000
	pgNumericPos        uint16 = 0x0000
	pgNumericNeg        uint16 = 0x4000
	pgNumericShort      uint16 = 0x8000
	pgNumericSpecial    uint16 = 0xC000
	pgNumericExtMask    uint16 = 0xF000
	pgNumericNaN        uint16 = 0xC000
	pgNumericPInf       uint16 = 0xD000
	pgNumericNInf       uint16 = 0xF000
	pgNumericShortSign  uint16 = 0x2000
	pgNumericShortScale uint16 = 0x1F80
	pgNumericShortWSign uint16 = 0x0040
	pgNumericShortWMask uint16 = 0x003F
	pgNumericDscaleMask uint16 = 0x3FFF
	pgNumericNBase      int16  = 10000

	pgMaxArrayDims     = 6
	pgMaxArrayElements = 1_000_000
)

// DecodePGNumeric decodes a PostgreSQL 17 on-disk numeric varlena payload (without varlena header)
// into an exact decimal string without any float64 intermediate conversion.
func DecodePGNumeric(raw []byte) (string, error) {
	if len(raw) < 2 {
		return "", fmt.Errorf("%w: numeric payload too short (%d)", ErrInvalidPage, len(raw))
	}
	nHeader := binary.LittleEndian.Uint16(raw[0:2])
	signBits := nHeader & pgNumericSignMask

	if signBits == pgNumericSpecial {
		switch nHeader & pgNumericExtMask {
		case pgNumericNaN:
			return "NaN", nil
		case pgNumericPInf:
			return "Infinity", nil
		case pgNumericNInf:
			return "-Infinity", nil
		default:
			return "", fmt.Errorf("%w: invalid numeric special header 0x%04x", ErrInvalidPage, nHeader)
		}
	}

	var neg bool
	var dscale int
	var weight int
	var digStart int

	if signBits == pgNumericShort {
		neg = (nHeader & pgNumericShortSign) != 0
		dscale = int((nHeader & pgNumericShortScale) >> 7)
		weight = int(nHeader & pgNumericShortWMask)
		if (nHeader & pgNumericShortWSign) != 0 {
			weight |= ^int(pgNumericShortWMask)
		}
		digStart = 2
	} else {
		neg = signBits == pgNumericNeg
		dscale = int(nHeader & pgNumericDscaleMask)
		if len(raw) < 4 {
			return "", fmt.Errorf("%w: long numeric payload too short (%d)", ErrInvalidPage, len(raw))
		}
		weight = int(int16(binary.LittleEndian.Uint16(raw[2:4])))
		digStart = 4
	}

	if (len(raw)-digStart)%2 != 0 {
		return "", fmt.Errorf("%w: numeric digits unaligned (%d bytes)", ErrInvalidPage, len(raw)-digStart)
	}
	ndigits := (len(raw) - digStart) / 2
	digits := make([]int16, ndigits)
	for i := 0; i < ndigits; i++ {
		d := int16(binary.LittleEndian.Uint16(raw[digStart+i*2 : digStart+i*2+2]))
		if d < 0 || d >= pgNumericNBase {
			return "", fmt.Errorf("%w: numeric digit out of range (%d)", ErrInvalidPage, d)
		}
		digits[i] = d
	}

	if ndigits == 0 {
		if dscale == 0 {
			return "0", nil
		}
		var b strings.Builder
		b.Grow(2 + dscale)
		b.WriteString("0.")
		for i := 0; i < dscale; i++ {
			b.WriteByte('0')
		}
		return b.String(), nil
	}

	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}

	if weight < 0 {
		b.WriteByte('0')
	} else {
		for i := 0; i <= weight; i++ {
			var d int16
			if i < ndigits {
				d = digits[i]
			}
			if i == 0 {
				b.WriteString(strconv.Itoa(int(d)))
			} else {
				write4Digits(&b, d)
			}
		}
	}

	if dscale > 0 {
		b.WriteByte('.')
		groups := (dscale + 3) / 4
		rem := dscale % 4
		for i := 0; i < groups; i++ {
			dIdx := weight + 1 + i
			var d int16
			if dIdx >= 0 && dIdx < ndigits {
				d = digits[dIdx]
			}
			if i == groups-1 && rem != 0 {
				var buf [4]byte
				buf[0] = byte('0' + d/1000)
				buf[1] = byte('0' + (d/100)%10)
				buf[2] = byte('0' + (d/10)%10)
				buf[3] = byte('0' + d%10)
				b.Write(buf[:rem])
			} else {
				write4Digits(&b, d)
			}
		}
	}

	return b.String(), nil
}

func write4Digits(b *strings.Builder, d int16) {
	b.WriteByte(byte('0' + d/1000))
	b.WriteByte(byte('0' + (d/100)%10))
	b.WriteByte(byte('0' + (d/10)%10))
	b.WriteByte(byte('0' + d%10))
}

type parsedNumeric struct {
	special int // -2: -Inf, 0: finite, +2: +Inf, +3: NaN
	neg     bool
	intPart string // stripped of leading zeros; "0" if zero
	frac    string // stripped of trailing zeros; "" if none
}

func parseNumericForCmp(s string) (parsedNumeric, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return parsedNumeric{}, false
	}
	switch strings.ToLower(s) {
	case "nan":
		return parsedNumeric{special: 3}, true
	case "infinity", "+infinity", "inf", "+inf":
		return parsedNumeric{special: 2}, true
	case "-infinity", "-inf":
		return parsedNumeric{special: -2}, true
	}

	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	} else if s[0] == '+' {
		s = s[1:]
	}
	if s == "" {
		return parsedNumeric{}, false
	}

	// Support scientific notation if SQLite/client passes e.g. 1e5
	if expIdx := strings.IndexAny(s, "eE"); expIdx >= 0 {
		mant := s[:expIdx]
		exp, err := strconv.Atoi(s[expIdx+1:])
		if err != nil {
			return parsedNumeric{}, false
		}
		dotIdx := strings.IndexByte(mant, '.')
		var digits string
		var fracLen int
		if dotIdx >= 0 {
			digits = mant[:dotIdx] + mant[dotIdx+1:]
			fracLen = len(mant) - dotIdx - 1
		} else {
			digits = mant
		}
		if digits == "" || !allDigits(digits) {
			return parsedNumeric{}, false
		}
		newFrac := fracLen - exp
		if newFrac <= 0 {
			digits += strings.Repeat("0", -newFrac)
			s = digits
		} else if newFrac >= len(digits) {
			s = "0." + strings.Repeat("0", newFrac-len(digits)) + digits
		} else {
			split := len(digits) - newFrac
			s = digits[:split] + "." + digits[split:]
		}
	}

	dotIdx := strings.IndexByte(s, '.')
	var intPart, fracPart string
	if dotIdx >= 0 {
		intPart = s[:dotIdx]
		fracPart = s[dotIdx+1:]
	} else {
		intPart = s
	}
	if intPart == "" && fracPart == "" {
		return parsedNumeric{}, false
	}
	if !allDigits(intPart) || !allDigits(fracPart) {
		return parsedNumeric{}, false
	}
	intPart = strings.TrimLeft(intPart, "0")
	if intPart == "" {
		intPart = "0"
	}
	fracPart = strings.TrimRight(fracPart, "0")
	if intPart == "0" && fracPart == "" {
		neg = false
	}
	return parsedNumeric{
		neg:     neg,
		intPart: intPart,
		frac:    fracPart,
	}, true
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// ComparePGNumeric compares two PostgreSQL numeric string representations using exact
// PostgreSQL numeric ordering: -Infinity < finite numbers < +Infinity < NaN.
func ComparePGNumeric(a, b string) int {
	pa, okA := parseNumericForCmp(a)
	pb, okB := parseNumericForCmp(b)
	if !okA || !okB {
		return strings.Compare(a, b)
	}
	if pa.special != 0 || pb.special != 0 {
		if pa.special < pb.special {
			return -1
		}
		if pa.special > pb.special {
			return 1
		}
		return 0
	}
	if pa.neg != pb.neg {
		if pa.neg {
			return -1
		}
		return 1
	}
	mag := compareNumericMag(pa, pb)
	if pa.neg {
		return -mag
	}
	return mag
}

func compareNumericMag(a, b parsedNumeric) int {
	if len(a.intPart) != len(b.intPart) {
		if len(a.intPart) < len(b.intPart) {
			return -1
		}
		return 1
	}
	if a.intPart != b.intPart {
		if a.intPart < b.intPart {
			return -1
		}
		return 1
	}
	minLen := len(a.frac)
	if len(b.frac) < minLen {
		minLen = len(b.frac)
	}
	for i := 0; i < minLen; i++ {
		if a.frac[i] != b.frac[i] {
			if a.frac[i] < b.frac[i] {
				return -1
			}
			return 1
		}
	}
	if len(a.frac) < len(b.frac) {
		return -1
	}
	if len(a.frac) > len(b.frac) {
		return 1
	}
	return 0
}

// DecodePGInet decodes a PostgreSQL 17 on-disk inet/cidr varlena payload (without varlena header).
// PostgreSQL uses PGSQL_AF_INET (AF_INET + 0 = 2) and PGSQL_AF_INET6 (AF_INET + 1 = 3).
func DecodePGInet(raw []byte, isCIDR bool) (string, error) {
	if len(raw) < 2 {
		return "", fmt.Errorf("%w: inet payload too short (%d)", ErrInvalidPage, len(raw))
	}
	family := raw[0]
	bits := raw[1]
	switch family {
	case 2: // PGSQL_AF_INET
		if len(raw) != 6 || bits > 32 {
			return "", fmt.Errorf("%w: invalid ipv4 inet len=%d bits=%d", ErrInvalidPage, len(raw), bits)
		}
		addr := netip.AddrFrom4([4]byte(raw[2:6]))
		if isCIDR || bits != 32 {
			return fmt.Sprintf("%s/%d", addr.String(), bits), nil
		}
		return addr.String(), nil
	case 3: // PGSQL_AF_INET6
		if len(raw) != 18 || bits > 128 {
			return "", fmt.Errorf("%w: invalid ipv6 inet len=%d bits=%d", ErrInvalidPage, len(raw), bits)
		}
		addr := netip.AddrFrom16([16]byte(raw[2:18]))
		if isCIDR || bits != 128 {
			return fmt.Sprintf("%s/%d", addr.String(), bits), nil
		}
		return addr.String(), nil
	default:
		return "", fmt.Errorf("%w: unsupported inet family %d", ErrInvalidPage, family)
	}
}

type parsedInet struct {
	isV6 bool
	bits uint8
	addr [16]byte
}

func parseInetForCmp(s string) (parsedInet, bool) {
	s = strings.TrimSpace(s)
	var addrStr string
	var bits uint8
	hasMask := false
	if slash := strings.IndexByte(s, '/'); slash >= 0 {
		addrStr = s[:slash]
		b, err := strconv.Atoi(s[slash+1:])
		if err != nil || b < 0 || b > 128 {
			return parsedInet{}, false
		}
		bits = uint8(b)
		hasMask = true
	} else {
		addrStr = s
	}
	addr, err := netip.ParseAddr(addrStr)
	if err != nil {
		return parsedInet{}, false
	}
	var out parsedInet
	if addr.Is4() {
		if hasMask && bits > 32 {
			return parsedInet{}, false
		}
		if !hasMask {
			bits = 32
		}
		out.isV6 = false
		out.bits = bits
		a4 := addr.As4()
		copy(out.addr[:4], a4[:])
		return out, true
	}
	if !hasMask {
		bits = 128
	}
	out.isV6 = true
	out.bits = bits
	out.addr = addr.As16()
	return out, true
}

// ComparePGInet implements PostgreSQL's network_cmp_internal ordering for inet/cidr:
// 1. Address family (IPv4 < IPv6)
// 2. Network prefix bits up to min(a.bits, b.bits)
// 3. Prefix length (a.bits vs b.bits)
// 4. Full address bytes
func ComparePGInet(a, b string) int {
	pa, okA := parseInetForCmp(a)
	pb, okB := parseInetForCmp(b)
	if !okA || !okB {
		return strings.Compare(a, b)
	}
	if pa.isV6 != pb.isV6 {
		if !pa.isV6 {
			return -1
		}
		return 1
	}
	addrBytes := 4
	if pa.isV6 {
		addrBytes = 16
	}
	minBits := int(pa.bits)
	if int(pb.bits) < minBits {
		minBits = int(pb.bits)
	}
	if cmp := compareMaskedBits(pa.addr[:addrBytes], pb.addr[:addrBytes], minBits); cmp != 0 {
		return cmp
	}
	if pa.bits != pb.bits {
		if pa.bits < pb.bits {
			return -1
		}
		return 1
	}
	for i := 0; i < addrBytes; i++ {
		if pa.addr[i] != pb.addr[i] {
			if pa.addr[i] < pb.addr[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

func compareMaskedBits(a, b []byte, nbits int) int {
	fullBytes := nbits / 8
	for i := 0; i < fullBytes; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	rem := nbits % 8
	if rem > 0 {
		mask := byte(0xFF << (8 - rem))
		ba := a[fullBytes] & mask
		bb := b[fullBytes] & mask
		if ba != bb {
			if ba < bb {
				return -1
			}
			return 1
		}
	}
	return 0
}

// DecodePGArray decodes a PostgreSQL 17 ArrayType varlena payload (without varlena header)
// for supported scalar element types into canonical PostgreSQL array text format.
func DecodePGArray(raw []byte, expectedElemOID uint32) (string, error) {
	if len(raw) < 12 {
		return "", fmt.Errorf("%w: array payload too short (%d)", ErrInvalidPage, len(raw))
	}
	ndim := int(int32(binary.LittleEndian.Uint32(raw[0:4])))
	dataOffset := int(int32(binary.LittleEndian.Uint32(raw[4:8])))
	elemOID := binary.LittleEndian.Uint32(raw[8:12])
	if expectedElemOID != 0 && elemOID != expectedElemOID {
		return "", fmt.Errorf("%w: array elemtype %d != expected %d", ErrUnsupportedType, elemOID, expectedElemOID)
	}
	if ndim == 0 {
		return "{}", nil
	}
	if ndim < 0 || ndim > pgMaxArrayDims {
		return "", fmt.Errorf("%w: invalid array ndim=%d", ErrInvalidPage, ndim)
	}
	hdrEnd := 12 + ndim*8
	if len(raw) < hdrEnd {
		return "", fmt.Errorf("%w: array header truncated", ErrInvalidPage)
	}

	dims := make([]int, ndim)
	lbounds := make([]int, ndim)
	nElems := 1
	customBounds := false
	for i := 0; i < ndim; i++ {
		d := int(int32(binary.LittleEndian.Uint32(raw[12+i*4 : 16+i*4])))
		lb := int(int32(binary.LittleEndian.Uint32(raw[12+ndim*4+i*4 : 16+ndim*4+i*4])))
		if d < 0 || d > pgMaxArrayElements {
			return "", fmt.Errorf("%w: invalid array dim[%d]=%d", ErrInvalidPage, i, d)
		}
		if nElems > 0 && d > 0 && nElems > pgMaxArrayElements/d {
			return "", fmt.Errorf("%w: array element count overflow", ErrInvalidPage)
		}
		nElems *= d
		dims[i] = d
		lbounds[i] = lb
		if lb != 1 {
			customBounds = true
		}
	}
	if nElems == 0 {
		return "{}", nil
	}

	hasNulls := dataOffset != 0
	var elemOff int
	if hasNulls {
		nullBytes := (nElems + 7) / 8
		// PG dataoffset is relative to the start of ArrayType (including the 4-byte varlena header),
		// whereas raw here has the 4-byte varlena header already stripped.
		adjOff := dataOffset - 4
		if adjOff < hdrEnd+nullBytes || adjOff > len(raw) {
			return "", fmt.Errorf("%w: invalid array dataoffset=%d", ErrInvalidPage, dataOffset)
		}
		elemOff = adjOff
	} else {
		// PG ARR_OVERHEAD_NONULLS(ndims) = 16 + 8*ndims; without the 4-byte varlena header that is 12 + 8*ndims == hdrEnd.
		elemOff = hdrEnd
	}

	elemStrs := make([]string, nElems)
	for i := 0; i < nElems; i++ {
		if hasNulls {
			nullByte := raw[hdrEnd+i/8]
			if (nullByte & (1 << uint(i%8))) == 0 {
				elemStrs[i] = "NULL"
				continue
			}
		}
		s, nextOff, err := decodePGArrayElement(raw, elemOff, elemOID)
		if err != nil {
			return "", err
		}
		elemStrs[i] = s
		elemOff = nextOff
	}

	var b strings.Builder
	if customBounds {
		for i := 0; i < ndim; i++ {
			fmt.Fprintf(&b, "[%d:%d]", lbounds[i], lbounds[i]+dims[i]-1)
		}
		b.WriteByte('=')
	}
	idx := 0
	writeArrayDim(&b, dims, 0, elemStrs, &idx)
	return b.String(), nil
}

func decodePGArrayElement(raw []byte, off int, elemOID uint32) (string, int, error) {
	switch elemOID {
	case PGOIDBool:
		if off+1 > len(raw) {
			return "", off, fmt.Errorf("%w: bool[] elem out of bounds", ErrInvalidPage)
		}
		if raw[off] != 0 {
			return "t", off + 1, nil
		}
		return "f", off + 1, nil
	case PGOIDInt2:
		off = alignUp(off, 2)
		if off+2 > len(raw) {
			return "", off, fmt.Errorf("%w: int2[] elem out of bounds", ErrInvalidPage)
		}
		v := int16(binary.LittleEndian.Uint16(raw[off : off+2]))
		return strconv.FormatInt(int64(v), 10), off + 2, nil
	case PGOIDInt4:
		off = alignUp(off, 4)
		if off+4 > len(raw) {
			return "", off, fmt.Errorf("%w: int4[] elem out of bounds", ErrInvalidPage)
		}
		v := int32(binary.LittleEndian.Uint32(raw[off : off+4]))
		return strconv.FormatInt(int64(v), 10), off + 4, nil
	case PGOIDInt8:
		off = alignUp(off+4, 8) - 4
		if off+8 > len(raw) {
			return "", off, fmt.Errorf("%w: int8[] elem out of bounds", ErrInvalidPage)
		}
		v := int64(binary.LittleEndian.Uint64(raw[off : off+8]))
		return strconv.FormatInt(v, 10), off + 8, nil
	case PGOIDFloat4:
		off = alignUp(off, 4)
		if off+4 > len(raw) {
			return "", off, fmt.Errorf("%w: float4[] elem out of bounds", ErrInvalidPage)
		}
		v := math.Float32frombits(binary.LittleEndian.Uint32(raw[off : off+4]))
		return formatPGFloat32(v), off + 4, nil
	case PGOIDFloat8:
		off = alignUp(off+4, 8) - 4
		if off+8 > len(raw) {
			return "", off, fmt.Errorf("%w: float8[] elem out of bounds", ErrInvalidPage)
		}
		v := math.Float64frombits(binary.LittleEndian.Uint64(raw[off : off+8]))
		return formatPGFloat64(v), off + 8, nil
	case PGOIDText, PGOIDVarchar:
		v, nextOff, err := decodeVarlenaWithTOAST(raw, off, true, nil)
		if err != nil {
			return "", off, err
		}
		return quotePGArrayText(string(v)), nextOff, nil
	default:
		return "", off, fmt.Errorf("%w: unsupported array elemtype %d", ErrUnsupportedType, elemOID)
	}
}

func formatPGFloat32(v float32) string {
	if math.IsNaN(float64(v)) {
		return "NaN"
	}
	if math.IsInf(float64(v), 1) {
		return "Infinity"
	}
	if math.IsInf(float64(v), -1) {
		return "-Infinity"
	}
	return strconv.FormatFloat(float64(v), 'g', 6, 32)
}

func formatPGFloat64(v float64) string {
	if math.IsNaN(v) {
		return "NaN"
	}
	if math.IsInf(v, 1) {
		return "Infinity"
	}
	if math.IsInf(v, -1) {
		return "-Infinity"
	}
	return strconv.FormatFloat(v, 'g', 15, 64)
}

func quotePGArrayText(s string) string {
	if s == "" {
		return `""`
	}
	needsQuote := strings.EqualFold(s, "NULL")
	if !needsQuote {
		for i := 0; i < len(s); i++ {
			c := s[i]
			if c == '{' || c == '}' || c == ',' || c == '"' || c == '\\' || c <= ' ' {
				needsQuote = true
				break
			}
		}
	}
	if !needsQuote {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' || c == '\\' {
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	b.WriteByte('"')
	return b.String()
}

func writeArrayDim(b *strings.Builder, dims []int, level int, elems []string, idx *int) {
	b.WriteByte('{')
	n := dims[level]
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		if level == len(dims)-1 {
			b.WriteString(elems[*idx])
			*idx++
		} else {
			writeArrayDim(b, dims, level+1, elems, idx)
		}
	}
	b.WriteByte('}')
}

func decodeExtColType(ct ColumnType, raw []byte) (string, error) {
	switch ct {
	case ColNumeric:
		return DecodePGNumeric(raw)
	case ColInet:
		return DecodePGInet(raw, false)
	case ColCIDR:
		return DecodePGInet(raw, true)
	case ColInt2Array:
		return DecodePGArray(raw, PGOIDInt2)
	case ColInt4Array:
		return DecodePGArray(raw, PGOIDInt4)
	case ColInt8Array:
		return DecodePGArray(raw, PGOIDInt8)
	case ColTextArray:
		return DecodePGArray(raw, PGOIDText)
	case ColBoolArray:
		return DecodePGArray(raw, PGOIDBool)
	case ColFloat4Array:
		return DecodePGArray(raw, PGOIDFloat4)
	case ColFloat8Array:
		return DecodePGArray(raw, PGOIDFloat8)
	default:
		return "", fmt.Errorf("%w: unsupported ext type %d", ErrUnsupportedType, ct)
	}
}

// BuildExtColMap indexes Phase 18 extended column types by lowercase column name
// so query validation can detect unsupported operators/functions safely.
func BuildExtColMap(tables []DiscoveredTable) map[string]ColumnType {
	m := make(map[string]ColumnType)
	for _, tbl := range tables {
		for _, col := range tbl.Columns {
			switch col.ColType {
			case ColNumeric, ColInet, ColCIDR,
				ColInt2Array, ColInt4Array, ColInt8Array,
				ColTextArray, ColBoolArray, ColFloat4Array, ColFloat8Array:
				m[strings.ToLower(col.Name)] = col.ColType
			}
		}
	}
	return m
}

// stripSQLStringLiterals replaces single-quoted string literals with spaces
// so operator/function detection never matches inside literal values.
func stripSQLStringLiterals(sql string) string {
	var b strings.Builder
	b.Grow(len(sql))
	inStr := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if inStr {
			if c == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					b.WriteByte(' ')
					i++
					continue
				}
				inStr = false
			}
			b.WriteByte(' ')
			continue
		}
		if c == '\'' {
			inStr = true
			b.WriteByte(' ')
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

func isArrayColType(ct ColumnType) bool {
	switch ct {
	case ColInt2Array, ColInt4Array, ColInt8Array, ColTextArray, ColBoolArray, ColFloat4Array, ColFloat8Array:
		return true
	default:
		return false
	}
}

func containsSQLIdent(s, ident string) bool {
	i := 0
	for i < len(s) {
		c := s[i]
		if (c >= 'a' && c <= 'z') || c == '_' {
			j := i + 1
			for j < len(s) && ((s[j] >= 'a' && s[j] <= 'z') || (s[j] >= '0' && s[j] <= '9') || s[j] == '_') {
				j++
			}
			// Do not treat table alias "b.id" as column "b".
			if s[i:j] == ident && (j >= len(s) || s[j] != '.') {
				return true
			}
			i = j
			continue
		}
		i++
	}
	return false
}

// CheckUnsupportedExtOps returns ErrUnsupportedType when a query uses operators,
// functions, or ordering semantics on numeric, inet/cidr, or array columns that
// SQLite cannot reproduce with PostgreSQL-identical semantics.
func CheckUnsupportedExtOps(sql string, extCols map[string]ColumnType) error {
	cleaned := strings.ToLower(stripSQLStringLiterals(sql))

	// Global PostgreSQL-only network/array operators and functions.
	if strings.Contains(cleaned, "<<=") || strings.Contains(cleaned, ">>=") ||
		strings.Contains(cleaned, "@>") || strings.Contains(cleaned, "<@") ||
		strings.Contains(cleaned, "&&") {
		return fmt.Errorf("%w: postgresql network/array operator requires primary fallback", ErrUnsupportedType)
	}
	for _, fn := range []string{
		"host(", "network(", "masklen(", "set_masklen(", "abbrev(",
		"broadcast(", "netmask(", "hostmask(", "family(", "inet_same_family(", "inet_merge(",
		"unnest(", "array_length(", "array_dims(", "array_upper(", "array_lower(",
		"array_ndims(", "cardinality(", "array_agg(", "array_to_string(", "string_to_array(",
		"= any(", "<> any(", "= all(", "<> all(",
	} {
		if strings.Contains(cleaned, fn) {
			return fmt.Errorf("%w: postgresql function %q requires primary fallback", ErrUnsupportedType, strings.TrimSuffix(fn, "("))
		}
	}

	if len(extCols) == 0 {
		return nil
	}

	// Tokenize identifiers to see which extended column types appear in the query.
	hasNumeric := false
	hasInet := false
	hasArray := false
	i := 0
	for i < len(cleaned) {
		c := cleaned[i]
		if (c >= 'a' && c <= 'z') || c == '_' {
			j := i + 1
			for j < len(cleaned) && ((cleaned[j] >= 'a' && cleaned[j] <= 'z') || (cleaned[j] >= '0' && cleaned[j] <= '9') || cleaned[j] == '_') {
				j++
			}
			tok := cleaned[i:j]
			if ct, ok := extCols[tok]; ok {
				switch {
				case ct == ColNumeric:
					hasNumeric = true
				case ct == ColInet || ct == ColCIDR:
					hasInet = true
				case isArrayColType(ct):
					hasArray = true
				}
			}
			i = j
			continue
		}
		i++
	}

	if hasNumeric {
		for _, fn := range []string{"sum(", "avg(", "variance(", "stddev("} {
			if strings.Contains(cleaned, fn) {
				return fmt.Errorf("%w: numeric aggregate %q requires primary fallback", ErrUnsupportedType, strings.TrimSuffix(fn, "("))
			}
		}
		// Arithmetic operators (+, -, *, /) on numeric columns would coerce TEXT to float64 in SQLite.
		for k := 0; k < len(cleaned); k++ {
			c := cleaned[k]
			if c == '+' || c == '/' {
				return fmt.Errorf("%w: numeric arithmetic operator %q requires primary fallback", ErrUnsupportedType, string(c))
			}
			if c == '*' {
				// Allow SELECT * or COUNT(*), reject multiplication where * is not preceded by SELECT/,(/whitespace+FROM.
				// Check simple heuristic: if preceded by identifier/digit/closing-paren, it is multiplication.
				prev := k - 1
				for prev >= 0 && cleaned[prev] == ' ' {
					prev--
				}
				if prev >= 0 && ((cleaned[prev] >= 'a' && cleaned[prev] <= 'z') || (cleaned[prev] >= '0' && cleaned[prev] <= '9') || cleaned[prev] == ')') {
					return fmt.Errorf("%w: numeric multiplication requires primary fallback", ErrUnsupportedType)
				}
			}
			if c == '-' {
				// Allow negative numeric literal after comparison/comma/paren/operator, reject binary subtraction after ident/digit/closing-paren.
				prev := k - 1
				for prev >= 0 && cleaned[prev] == ' ' {
					prev--
				}
				if prev >= 0 && ((cleaned[prev] >= 'a' && cleaned[prev] <= 'z') || (cleaned[prev] >= '0' && cleaned[prev] <= '9') || cleaned[prev] == ')') {
					return fmt.Errorf("%w: numeric subtraction requires primary fallback", ErrUnsupportedType)
				}
			}
		}
	}

	if hasInet {
		if strings.Contains(cleaned, "<<") || strings.Contains(cleaned, ">>") {
			return fmt.Errorf("%w: inet containment operator requires primary fallback", ErrUnsupportedType)
		}
	}

	if hasArray {
		// Array indexing ([...]), concatenation (||), and ordering/range comparisons (<, >, <=, >=, between, order by, min, max)
		// on array columns cannot be reproduced by text collation and must fall back to PostgreSQL.
		if strings.Contains(cleaned, "[") || strings.Contains(cleaned, "||") {
			return fmt.Errorf("%w: array indexing/concat requires primary fallback", ErrUnsupportedType)
		}
		obClause := ""
		if obIdx := strings.Index(cleaned, "order by"); obIdx >= 0 {
			obClause = cleaned[obIdx:]
		}
		for col, ct := range extCols {
			if !isArrayColType(ct) {
				continue
			}
			if obClause != "" && containsSQLIdent(obClause, col) {
				return fmt.Errorf("%w: array ORDER BY requires primary fallback", ErrUnsupportedType)
			}
			if strings.Contains(cleaned, "min("+col+")") || strings.Contains(cleaned, "max("+col+")") ||
				strings.Contains(cleaned, col+" between") ||
				strings.Contains(cleaned, col+" <") || strings.Contains(cleaned, col+"<") ||
				strings.Contains(cleaned, col+" >") || strings.Contains(cleaned, col+">") {
				// Allow <> inequality check.
				if strings.Contains(cleaned, col+" <>") || strings.Contains(cleaned, col+"<>") {
					continue
				}
				return fmt.Errorf("%w: array ordering comparison requires primary fallback", ErrUnsupportedType)
			}
		}
	}

	return nil
}
