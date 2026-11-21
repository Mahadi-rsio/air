package pageserver

import (
	"encoding/binary"
)

const (
	RmHeap2ID uint8 = 9
	RmHeapID  uint8 = 10

	XLogHeapInsert    uint8 = 0x00
	XLogHeapDelete    uint8 = 0x10
	XLogHeapUpdate    uint8 = 0x20
	XLogHeapHotUpdate uint8 = 0x40
	XLogHeapInplace   uint8 = 0x70
	XLogHeapOpMask    uint8 = 0x70
	XLogHeapInitPage  uint8 = 0x80

	XLogHeap2MultiInsert uint8 = 0x50

	xlhUpdatePrefixFromOld uint8 = 1 << 5
	xlhUpdateSuffixFromOld uint8 = 1 << 6

	pgPageHeaderSize     = 24
	pgPageSizeVersion    = 0x2004
	pgSizeOfHeapTupleHdr = 23
	pgLPNormal           = 1
	pgHeapXmaxInvalid    = 0x0800
)

func initPGHeapPage(page []byte) {
	for i := range page {
		page[i] = 0
	}
	binary.LittleEndian.PutUint16(page[12:14], pgPageHeaderSize)  // pd_lower
	binary.LittleEndian.PutUint16(page[14:16], uint16(PageSize))  // pd_upper
	binary.LittleEndian.PutUint16(page[16:18], uint16(PageSize))  // pd_special
	binary.LittleEndian.PutUint16(page[18:20], pgPageSizeVersion) // pd_pagesize_version
}

func heapMaxAlign(n int) int {
	return (n + 7) &^ 7
}

func heapShortAlign(n int) int {
	return (n + 1) &^ 1
}

func pageGetHeapTuple(page []byte, offnum uint16) []byte {
	if len(page) != PageSize || offnum == 0 {
		return nil
	}
	lower := int(binary.LittleEndian.Uint16(page[12:14]))
	lpOff := pgPageHeaderSize + (int(offnum)-1)*4
	if lpOff+4 > lower {
		return nil
	}
	raw := binary.LittleEndian.Uint32(page[lpOff : lpOff+4])
	if ((raw >> 15) & 0x03) != pgLPNormal {
		return nil
	}
	tupOff := int(raw & 0x7FFF)
	tupLen := int((raw >> 17) & 0x7FFF)
	if tupOff < pgPageHeaderSize || tupLen < pgSizeOfHeapTupleHdr || tupOff+tupLen > PageSize {
		return nil
	}
	return page[tupOff : tupOff+tupLen]
}

func pageAddHeapItem(page []byte, offnum uint16, tup []byte) {
	if len(page) != PageSize || offnum == 0 || len(tup) == 0 {
		return
	}
	lower := int(binary.LittleEndian.Uint16(page[12:14]))
	upper := int(binary.LittleEndian.Uint16(page[14:16]))
	if lower < pgPageHeaderSize || upper > PageSize || lower > upper {
		initPGHeapPage(page)
		lower = pgPageHeaderSize
		upper = PageSize
	}

	aligned := heapMaxAlign(len(tup))
	newUpper := upper - aligned
	needLower := pgPageHeaderSize + int(offnum)*4
	if needLower < lower {
		needLower = lower
	}
	if newUpper < needLower {
		return
	}

	copy(page[newUpper:newUpper+len(tup)], tup)
	itemID := uint32(newUpper&0x7FFF) | (uint32(pgLPNormal) << 15) | (uint32(len(tup)&0x7FFF) << 17)
	lpOff := pgPageHeaderSize + (int(offnum)-1)*4
	binary.LittleEndian.PutUint32(page[lpOff:lpOff+4], itemID)
	binary.LittleEndian.PutUint16(page[12:14], uint16(needLower))
	binary.LittleEndian.PutUint16(page[14:16], uint16(newUpper))
}

func pageMarkTupleDeleted(page []byte, offnum uint16, xmax uint32) {
	tup := pageGetHeapTuple(page, offnum)
	if tup == nil {
		return
	}
	binary.LittleEndian.PutUint32(tup[4:8], xmax)
	infomask := binary.LittleEndian.Uint16(tup[20:22])
	infomask &^= pgHeapXmaxInvalid
	binary.LittleEndian.PutUint16(tup[20:22], infomask)
}

func buildTupleFromXLHeader(xmin, xmax uint32, blkno uint32, offnum uint16, infomask2, infomask uint16, hoff uint8, body []byte) []byte {
	// PG17 WAL omits the 23-byte HeapTupleHeaderData and stores (xl_heap_header + data starting at offset 23).
	tup := make([]byte, pgSizeOfHeapTupleHdr+len(body))
	binary.LittleEndian.PutUint32(tup[0:4], xmin)
	binary.LittleEndian.PutUint32(tup[4:8], xmax)
	// t_ctid: BlockIdData (hi uint16, lo uint16) + OffsetNumber (uint16) at offsets 12..18
	binary.LittleEndian.PutUint16(tup[12:14], uint16(blkno>>16))
	binary.LittleEndian.PutUint16(tup[14:16], uint16(blkno&0xFFFF))
	binary.LittleEndian.PutUint16(tup[16:18], offnum)
	binary.LittleEndian.PutUint16(tup[18:20], infomask2)
	binary.LittleEndian.PutUint16(tup[20:22], infomask)
	tup[22] = hoff
	copy(tup[pgSizeOfHeapTupleHdr:], body)
	return tup
}

func findWALBlock(rec *DecodedWALRecord, blockID uint8, key Key) *WALBlockRef {
	for i := range rec.Blocks {
		b := &rec.Blocks[i]
		if b.BlockID == blockID &&
			b.Rel.SpcOID == key.SpcOID &&
			b.Rel.DbOID == key.DbOID &&
			b.Rel.RelNumber == key.RelNumber &&
			b.ForkNum == key.ForkNum &&
			b.BlockNum == key.BlockNum {
			return b
		}
	}
	return nil
}

// applyHeapWALRecords replays pure-Go PG17 Heap/Heap2 WAL records onto basePage
// when no external walredo worker binary is configured.
func applyHeapWALRecords(key Key, basePage []byte, records [][]byte) []byte {
	var page []byte
	if len(basePage) == PageSize {
		page = append([]byte(nil), basePage...)
	}

	for _, raw := range records {
		rec, err := DecodeWALRecord(0, raw)
		if err != nil {
			continue
		}
		if rec.Rmid == RmHeapID {
			op := rec.Info & XLogHeapOpMask
			isInit := (rec.Info & XLogHeapInitPage) != 0

			switch op {
			case XLogHeapInsert:
				blk := findWALBlock(rec, 0, key)
				if blk == nil || len(rec.MainData) < 3 || len(blk.BlockData) < 5 {
					continue
				}
				if isInit || len(page) != PageSize {
					page = make([]byte, PageSize)
					initPGHeapPage(page)
				}
				offnum := binary.LittleEndian.Uint16(rec.MainData[0:2])
				infomask2 := binary.LittleEndian.Uint16(blk.BlockData[0:2])
				infomask := binary.LittleEndian.Uint16(blk.BlockData[2:4])
				hoff := blk.BlockData[4]
				tup := buildTupleFromXLHeader(rec.XID, 0, key.BlockNum, offnum, infomask2, infomask|pgHeapXmaxInvalid, hoff, blk.BlockData[5:])
				pageAddHeapItem(page, offnum, tup)

			case XLogHeapDelete:
				blk := findWALBlock(rec, 0, key)
				if blk == nil || len(rec.MainData) < 8 || len(page) != PageSize {
					continue
				}
				xmax := binary.LittleEndian.Uint32(rec.MainData[0:4])
				offnum := binary.LittleEndian.Uint16(rec.MainData[4:6])
				pageMarkTupleDeleted(page, offnum, xmax)

			case XLogHeapUpdate, XLogHeapHotUpdate:
				if len(rec.MainData) < 14 {
					continue
				}
				oldXmax := binary.LittleEndian.Uint32(rec.MainData[0:4])
				oldOffnum := binary.LittleEndian.Uint16(rec.MainData[4:6])
				flags := rec.MainData[7]
				newXmax := binary.LittleEndian.Uint32(rec.MainData[8:12])
				newOffnum := binary.LittleEndian.Uint16(rec.MainData[12:14])

				// Block 1 (if present) is old tuple page; if absent, block 0 is both old and new.
				oldBlk := findWALBlock(rec, 1, key)
				newBlk := findWALBlock(rec, 0, key)
				if oldBlk != nil && len(page) == PageSize {
					pageMarkTupleDeleted(page, oldOffnum, oldXmax)
				}
				if newBlk != nil {
					var oldUserData []byte
					if isInit || len(page) != PageSize {
						page = make([]byte, PageSize)
						initPGHeapPage(page)
					} else if oldBlk == nil {
						// Same-page update: snapshot old user data for prefix/suffix reconstruction,
						// then mark old tuple deleted on this page.
						if oldTup := pageGetHeapTuple(page, oldOffnum); oldTup != nil {
							oldHoff := int(oldTup[22])
							if oldHoff >= pgSizeOfHeapTupleHdr && oldHoff <= len(oldTup) {
								oldUserData = append([]byte(nil), oldTup[oldHoff:]...)
							}
						}
						pageMarkTupleDeleted(page, oldOffnum, oldXmax)
					}

					data := newBlk.BlockData
					off := 0
					var prefixLen, suffixLen int
					if (flags & xlhUpdatePrefixFromOld) != 0 {
						if off+2 > len(data) {
							continue
						}
						prefixLen = int(binary.LittleEndian.Uint16(data[off : off+2]))
						off += 2
					}
					if (flags & xlhUpdateSuffixFromOld) != 0 {
						if off+2 > len(data) {
							continue
						}
						suffixLen = int(binary.LittleEndian.Uint16(data[off : off+2]))
						off += 2
					}
					if off+5 > len(data) {
						continue
					}
					infomask2 := binary.LittleEndian.Uint16(data[off : off+2])
					infomask := binary.LittleEndian.Uint16(data[off+2 : off+4])
					hoff := data[off+4]
					off += 5

					var body []byte
					if prefixLen == 0 && suffixLen == 0 {
						body = data[off:]
					} else {
						hdrExtra := int(hoff) - pgSizeOfHeapTupleHdr
						if hdrExtra < 0 || off+hdrExtra > len(data) {
							continue
						}
						if prefixLen > len(oldUserData) || suffixLen > len(oldUserData) {
							continue
						}
						hdrBytes := data[off : off+hdrExtra]
						midBytes := data[off+hdrExtra:]
						body = make([]byte, 0, len(hdrBytes)+prefixLen+len(midBytes)+suffixLen)
						body = append(body, hdrBytes...)
						body = append(body, oldUserData[:prefixLen]...)
						body = append(body, midBytes...)
						body = append(body, oldUserData[len(oldUserData)-suffixLen:]...)
					}

					im := infomask
					if newXmax == 0 {
						im |= pgHeapXmaxInvalid
					}
					tup := buildTupleFromXLHeader(rec.XID, newXmax, key.BlockNum, newOffnum, infomask2, im, hoff, body)
					pageAddHeapItem(page, newOffnum, tup)
				}

			case XLogHeapInplace:
				blk := findWALBlock(rec, 0, key)
				if blk == nil || len(rec.MainData) < 2 || len(page) != PageSize {
					continue
				}
				offnum := binary.LittleEndian.Uint16(rec.MainData[0:2])
				if tup := pageGetHeapTuple(page, offnum); tup != nil {
					hoff := int(tup[22])
					if hoff >= pgSizeOfHeapTupleHdr && hoff+len(blk.BlockData) <= len(tup) {
						copy(tup[hoff:], blk.BlockData)
					}
				}
			}
		} else if rec.Rmid == RmHeap2ID && (rec.Info&XLogHeapOpMask) == XLogHeap2MultiInsert {
			blk := findWALBlock(rec, 0, key)
			if blk == nil || len(rec.MainData) < 4 {
				continue
			}
			isInit := (rec.Info & XLogHeapInitPage) != 0
			if isInit || len(page) != PageSize {
				page = make([]byte, PageSize)
				initPGHeapPage(page)
			}
			ntuples := int(binary.LittleEndian.Uint16(rec.MainData[2:4]))
			data := blk.BlockData
			off := 0
			for i := 0; i < ntuples; i++ {
				off = heapShortAlign(off)
				if off+7 > len(data) {
					break
				}
				datalen := int(binary.LittleEndian.Uint16(data[off : off+2]))
				infomask2 := binary.LittleEndian.Uint16(data[off+2 : off+4])
				infomask := binary.LittleEndian.Uint16(data[off+4 : off+6])
				hoff := data[off+6]
				off += 7
				if off+datalen > len(data) {
					break
				}
				var offnum uint16
				if isInit {
					offnum = uint16(i + 1)
				} else if 4+i*2+2 <= len(rec.MainData) {
					offnum = binary.LittleEndian.Uint16(rec.MainData[4+i*2 : 4+i*2+2])
				} else {
					offnum = uint16(i + 1)
				}
				tup := buildTupleFromXLHeader(rec.XID, 0, key.BlockNum, offnum, infomask2, infomask|pgHeapXmaxInvalid, hoff, data[off:off+datalen])
				pageAddHeapItem(page, offnum, tup)
				off += datalen
			}
		}
	}
	return page
}
