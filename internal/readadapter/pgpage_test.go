package readadapter

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestPGPageDecoderValidationAndTypes(t *testing.T) {
	// 1. Corrupt varlena4 length=0 must return ErrInvalidPage, never panic.
	tup := buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen, []byte{0, 0, 0, 0})
	hdr, err := ParseHeapTupleHeader(tup)
	if err != nil {
		t.Fatalf("ParseHeapTupleHeader: %v", err)
	}
	if _, err := DecodeHeapColumns(tup, hdr, []ColumnType{ColText}); !errors.Is(err, ErrInvalidPage) {
		t.Fatalf("expected ErrInvalidPage for zero-length varlena4, got %v", err)
	}

	// 2. External TOAST pointer must return ErrTOASTUnsupported, never "<TOAST_EXTERNAL>" or empty slice.
	toastPayload := make([]byte, 18)
	toastPayload[0] = 0x01
	toastPayload[1] = 0x12
	binary.LittleEndian.PutUint32(toastPayload[2:6], 4096)
	toastTup := buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen, toastPayload)
	toastHdr, _ := ParseHeapTupleHeader(toastTup)
	if _, err := DecodeHeapColumns(toastTup, toastHdr, []ColumnType{ColText}); !errors.Is(err, ErrTOASTUnsupported) {
		t.Fatalf("expected ErrTOASTUnsupported for text toast pointer, got %v", err)
	}
	if _, err := DecodeHeapColumns(toastTup, toastHdr, []ColumnType{ColBytea}); !errors.Is(err, ErrTOASTUnsupported) {
		t.Fatalf("expected ErrTOASTUnsupported for bytea toast pointer, got %v", err)
	}

	// 3. Date, Timestamp, UUID decoding.
	var payload []byte
	// ColDate (4-byte aligned): 2026-01-01 is 9497 days since 2000-01-01 (26*365 + 7 leap days = 9497)
	dBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(dBuf, uint32(9497))
	payload = append(payload, dBuf...)
	// Pad to 8-byte boundary for timestamp
	payload = append(payload, 0, 0, 0, 0)
	// ColTimestamp: 0 usec = 2000-01-01 00:00:00
	tsBuf := make([]byte, 8)
	payload = append(payload, tsBuf...)
	// ColUUID: 16 bytes
	uuidBytes := []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
	payload = append(payload, uuidBytes...)

	multiTup := buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen, payload)
	multiHdr, err := ParseHeapTupleHeader(multiTup)
	if err != nil {
		t.Fatalf("ParseHeapTupleHeader multi: %v", err)
	}
	vals, err := DecodeHeapColumns(multiTup, multiHdr, []ColumnType{ColDate, ColTimestamp, ColUUID})
	if err != nil {
		t.Fatalf("DecodeHeapColumns multi: %v", err)
	}
	if vals[0] != "2026-01-01" {
		t.Fatalf("date got %v, want 2026-01-01", vals[0])
	}
	if vals[1] != "2000-01-01 00:00:00" {
		t.Fatalf("timestamp got %v, want 2000-01-01 00:00:00", vals[1])
	}
	if vals[2] != "01234567-89ab-cdef-0123-456789abcdef" {
		t.Fatalf("uuid got %v", vals[2])
	}

	// 4. Line pointer out-of-bounds validation.
	pg := buildPG17HeapPage(0x1000, nil)
	phdr, err := ParsePageHeader(pg)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}
	if err := ValidateLinePointer(LinePointer{Flags: LPNormal, Offset: 100, Length: 32}, phdr); !errors.Is(err, ErrInvalidPage) {
		t.Fatalf("expected ErrInvalidPage for LP below pd_upper, got %v", err)
	}

	// 5. ALTER TABLE ADD COLUMN: tuple with t_natts=1 decoded against 2-column schema -> col 1 is nil.
	var col1 [4]byte
	binary.LittleEndian.PutUint32(col1[:], 42)
	addColTup := buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen, col1[:])
	binary.LittleEndian.PutUint16(addColTup[18:20], 1) // t_natts = 1
	addColHdr, _ := ParseHeapTupleHeader(addColTup)
	addVals, err := DecodeHeapColumns(addColTup, addColHdr, []ColumnType{ColInt4, ColText})
	if err != nil {
		t.Fatalf("DecodeHeapColumns addCol: %v", err)
	}
	if addVals[0] != int32(42) || addVals[1] != nil {
		t.Fatalf("expected [42, nil], got %+v", addVals)
	}
}

func FuzzParseAndDecodePGPage(f *testing.F) {
	f.Add(buildPG17HeapPage(0x1000, [][]byte{
		buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen, []byte{1, 0, 0, 0, 5, 'h', 'i'}),
	}))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) != PGPageSize {
			return
		}
		hdr, err := ParsePageHeader(data)
		if err != nil {
			return
		}
		lps := ParseLinePointers(data, hdr)
		for _, lp := range lps {
			if lp.Flags != LPNormal {
				continue
			}
			if err := ValidateLinePointer(lp, hdr); err != nil {
				continue
			}
			tup := data[lp.Offset : lp.Offset+lp.Length]
			thdr, err := ParseHeapTupleHeader(tup)
			if err != nil {
				continue
			}
			_, _ = DecodeHeapColumns(tup, thdr, []ColumnType{ColInt4, ColText, ColBytea, ColFloat8, ColUUID})
		}
	})
}
