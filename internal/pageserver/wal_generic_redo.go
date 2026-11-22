package pageserver

import (
	"encoding/binary"
	"fmt"
)

const (
	// PostgreSQL 17 RmSequenceID (RM_SEQ_ID = 12) and RmGenericID (RM_GENERIC_ID = 17).
	RmSequenceID uint8 = 12
	RmGenericID  uint8 = 17

	XLogSeqLog     uint8 = 0x00
	XLogXLogFPI    uint8 = 0xB0
	XLogXLogFPIHint uint8 = 0xC0
)

// ApplyWALRecordsInGo applies a sequence of raw WAL records onto basePage for key.
// It supports Heap, Heap2, Btree, Sequence, Generic, and XLog FPI records.
// Returns (nil, false) if any record is unsupported or malformed—never silently serving a stale page.
func ApplyWALRecordsInGo(key Key, basePage []byte, records [][]byte) ([]byte, bool) {
	if len(records) == 0 {
		if len(basePage) == PageSize {
			out := make([]byte, PageSize)
			copy(out, basePage)
			return out, true
		}
		return nil, false
	}

	// Fast path: if all records are Heap/Heap2/Btree, use applyHeapWALRecords directly.
	if out, ok := applyHeapWALRecords(key, basePage, records); ok && len(out) == PageSize {
		return out, true
	}

	var cur []byte
	if len(basePage) == PageSize {
		cur = make([]byte, PageSize)
		copy(cur, basePage)
	}

	for _, raw := range records {
		rec, err := DecodeWALRecord(0, raw)
		if err != nil {
			return nil, false
		}
		// Check if any block ref in this record targets key with an FPI (including compressed FPIs).
		appliedFPI := false
		for _, blk := range rec.Blocks {
			if blk.Rel.SpcOID == key.SpcOID && blk.Rel.DbOID == key.DbOID &&
				blk.Rel.RelNumber == key.RelNumber && blk.ForkNum == key.ForkNum && blk.BlockNum == key.BlockNum {
				if blk.HasImage {
					img, ierr := RestoreBkpImage(blk)
					if ierr != nil {
						return nil, false
					}
					cur = img
					appliedFPI = true
					break
				}
			}
		}
		if appliedFPI {
			continue
		}

		// Try single-record Heap/Heap2/Btree redo.
		if next, ok := applyHeapWALRecords(key, cur, [][]byte{raw}); ok && len(next) == PageSize {
			cur = next
			continue
		}

		// Handle Sequence (RM_SEQ_ID = 12) and Generic (RM_GENERIC_ID = 17) records.
		switch rec.Rmid {
		case RmSequenceID:
			if (rec.Info & XLogInfoOpMask) != XLogSeqLog {
				return nil, false
			}
			next, ok := applySequenceRedo(cur, rec)
			if !ok {
				return nil, false
			}
			cur = next
		case RmGenericID:
			next, ok := applyGenericRedo(key, cur, rec)
			if !ok {
				return nil, false
			}
			cur = next
		case RmXLogID:
			op := rec.Info & XLogInfoOpMask
			if op == XLogXLogFPI || op == XLogXLogFPIHint {
				// FPI without matching block key means this record doesn't touch key.
				if len(cur) != PageSize {
					return nil, false
				}
				continue
			}
			return nil, false
		default:
			return nil, false
		}
	}

	if len(cur) != PageSize {
		return nil, false
	}
	return cur, true
}

func applySequenceRedo(cur []byte, rec *DecodedWALRecord) ([]byte, bool) {
	if len(rec.MainData) < 4 || len(rec.Blocks) == 0 {
		return nil, false
	}
	// xl_seq_rec: RelFileLocator (12B) + ... or main data contains the sequence tuple after xl_seq_rec (12B).
	// Sequence page is always reinitialized with PageInit + sequence tuple + special magic (0x1717).
	const sizeOfXlSeqRec = 12
	if len(rec.MainData) <= sizeOfXlSeqRec {
		return nil, false
	}
	tupData := rec.MainData[sizeOfXlSeqRec:]
	out := make([]byte, PageSize)
	if len(cur) == PageSize {
		copy(out, cur)
	}
	// Initialize PG page header (24B) + 4B special space at end (8188..8192)
	const pdLower = 24 + 4 // 1 line pointer
	const pdSpecial = PageSize - 4
	tupOff := pdSpecial - ((len(tupData) + 7) &^ 7)
	if tupOff < pdLower {
		return nil, false
	}
	// pd_lower, pd_upper, pd_special, pd_pagesize_version
	binary.LittleEndian.PutUint16(out[12:14], pdLower)
	binary.LittleEndian.PutUint16(out[14:16], uint16(tupOff))
	binary.LittleEndian.PutUint16(out[16:18], uint16(pdSpecial))
	binary.LittleEndian.PutUint16(out[18:20], uint16(PageSize|4))
	// Line pointer 1: offset=tupOff, flags=LP_NORMAL(1), len=len(tupData)
	lp := uint32(tupOff&0x7FFF) | (1 << 15) | (uint32(len(tupData)&0x7FFF) << 17)
	binary.LittleEndian.PutUint32(out[24:28], lp)
	copy(out[tupOff:tupOff+len(tupData)], tupData)
	// Sequence magic 0x1717 at pd_special
	binary.LittleEndian.PutUint32(out[pdSpecial:PageSize], 0x1717)
	return out, true
}

func applyGenericRedo(key Key, cur []byte, rec *DecodedWALRecord) ([]byte, bool) {
	if len(cur) != PageSize {
		return nil, false
	}
	out := make([]byte, PageSize)
	copy(out, cur)
	for _, blk := range rec.Blocks {
		if blk.Rel.SpcOID == key.SpcOID && blk.Rel.DbOID == key.DbOID &&
			blk.Rel.RelNumber == key.RelNumber && blk.ForkNum == key.ForkNum && blk.BlockNum == key.BlockNum {
			if !blk.HasData {
				continue
			}
			// PG GenericXLog block data format: sequence of (uint16 offset, uint16 length, [length]byte data).
			d := blk.BlockData
			off := 0
			for off+4 <= len(d) {
				pgOff := int(binary.LittleEndian.Uint16(d[off : off+2]))
				pgLen := int(binary.LittleEndian.Uint16(d[off+2 : off+4]))
				off += 4
				if pgOff < 0 || pgLen < 0 || pgOff+pgLen > PageSize || off+pgLen > len(d) {
					return nil, false
				}
				copy(out[pgOff:pgOff+pgLen], d[off:off+pgLen])
				off += pgLen
			}
			if off != len(d) {
				return nil, false
			}
		}
	}
	return out, true
}

// DecodeRedoFrame decodes one worker request frame (used by air-walredo).
func DecodeRedoFrame(keyBuf []byte, hasBase byte, basePage []byte) (Key, error) {
	if len(keyBuf) != 24 {
		return Key{}, fmt.Errorf("invalid key len %d", len(keyBuf))
	}
	return DecodeKey(keyBuf), nil
}
