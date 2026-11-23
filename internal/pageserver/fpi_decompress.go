package pageserver

import (
	"errors"
	"fmt"
)

// decompressFPIPayload decompresses a PostgreSQL 17 full-page image payload
// compressed with PGLZ, LZ4, or ZSTD to rawSize bytes.
func decompressFPIPayload(imageInfo uint8, src []byte, rawSize int) ([]byte, error) {
	if rawSize <= 0 || rawSize > PageSize {
		return nil, fmt.Errorf("pageserver: invalid FPI rawSize=%d", rawSize)
	}
	switch {
	case (imageInfo & BkpImageCompressPGLZ) != 0:
		return pglzDecompressFPI(src, rawSize)
	case (imageInfo & BkpImageCompressLZ4) != 0:
		return lz4BlockDecompressFPI(src, rawSize)
	case (imageInfo & BkpImageCompressZSTD) != 0:
		_, dec, err := getV2Zstd()
		if err != nil {
			return nil, fmt.Errorf("pageserver: zstd FPI decoder init: %w", err)
		}
		out, err := dec.DecodeAll(src, make([]byte, 0, rawSize))
		if err != nil {
			return nil, fmt.Errorf("pageserver: zstd FPI decompress: %w", err)
		}
		if len(out) != rawSize {
			return nil, fmt.Errorf("pageserver: zstd FPI size mismatch got=%d want=%d", len(out), rawSize)
		}
		return out, nil
	default:
		return nil, ErrCompressedFPI
	}
}

// pglzDecompressFPI implements PostgreSQL's pglz_decompress for WAL FPIs.
func pglzDecompressFPI(src []byte, rawSize int) ([]byte, error) {
	dst := make([]byte, 0, rawSize)
	sp := 0
	for sp < len(src) && len(dst) < rawSize {
		ctrl := src[sp]
		sp++
		for bit := 0; bit < 8 && sp < len(src) && len(dst) < rawSize; bit++ {
			if (ctrl & (1 << bit)) != 0 {
				if sp+1 >= len(src) {
					return nil, errors.New("pageserver: truncated pglz FPI match")
				}
				b0 := src[sp]
				b1 := src[sp+1]
				sp += 2
				matchLen := int(b0&0x0F) + 3
				if matchLen == 18 {
					if sp >= len(src) {
						return nil, errors.New("pageserver: truncated pglz FPI extended len")
					}
					matchLen += int(src[sp])
					sp++
				}
				off := (int(b0&0xF0) << 4) | int(b1)
				if off <= 0 || off > len(dst) {
					return nil, fmt.Errorf("pageserver: invalid pglz FPI offset=%d dstLen=%d", off, len(dst))
				}
				for k := 0; k < matchLen && len(dst) < rawSize; k++ {
					dst = append(dst, dst[len(dst)-off])
				}
			} else {
				dst = append(dst, src[sp])
				sp++
			}
		}
	}
	if len(dst) != rawSize {
		return nil, fmt.Errorf("pageserver: pglz FPI size mismatch got=%d want=%d", len(dst), rawSize)
	}
	return dst, nil
}

// lz4BlockDecompressFPI implements raw LZ4 block decompression matching PostgreSQL's LZ4_decompress_safe.
func lz4BlockDecompressFPI(src []byte, rawSize int) ([]byte, error) {
	dst := make([]byte, 0, rawSize)
	sp := 0
	for sp < len(src) {
		token := src[sp]
		sp++
		litLen := int(token >> 4)
		if litLen == 15 {
			for {
				if sp >= len(src) {
					return nil, errors.New("pageserver: truncated lz4 literal length")
				}
				b := src[sp]
				sp++
				litLen += int(b)
				if b != 255 {
					break
				}
			}
		}
		if sp+litLen > len(src) || len(dst)+litLen > rawSize {
			return nil, errors.New("pageserver: lz4 literal out of bounds")
		}
		dst = append(dst, src[sp:sp+litLen]...)
		sp += litLen
		if sp >= len(src) {
			break
		}
		if sp+2 > len(src) {
			return nil, errors.New("pageserver: truncated lz4 match offset")
		}
		off := int(src[sp]) | (int(src[sp+1]) << 8)
		sp += 2
		if off <= 0 || off > len(dst) {
			return nil, fmt.Errorf("pageserver: invalid lz4 match offset=%d dstLen=%d", off, len(dst))
		}
		matchLen := int(token&0x0F) + 4
		if (token & 0x0F) == 15 {
			for {
				if sp >= len(src) {
					return nil, errors.New("pageserver: truncated lz4 match length")
				}
				b := src[sp]
				sp++
				matchLen += int(b)
				if b != 255 {
					break
				}
			}
		}
		if len(dst)+matchLen > rawSize {
			return nil, errors.New("pageserver: lz4 match exceeds rawSize")
		}
		for k := 0; k < matchLen; k++ {
			dst = append(dst, dst[len(dst)-off])
		}
	}
	if len(dst) != rawSize {
		return nil, fmt.Errorf("pageserver: lz4 FPI size mismatch got=%d want=%d", len(dst), rawSize)
	}
	return dst, nil
}
