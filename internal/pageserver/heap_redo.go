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

	XLogHeap2PruneOnAccess      uint8 = 0x10
	XLogHeap2PruneVacuumScan    uint8 = 0x20
	XLogHeap2PruneVacuumCleanup uint8 = 0x30
	XLogHeap2Visible            uint8 = 0x40
	XLogHeap2MultiInsert        uint8 = 0x50
	XLogHeap2LockUpdated        uint8 = 0x60

	xlhpCleanupLock        uint8 = 1 << 2
	xlhpHasConflictHorizon uint8 = 1 << 3
	xlhpHasFreezePlans     uint8 = 1 << 4
	xlhpHasRedirections    uint8 = 1 << 5
	xlhpHasDeadItems       uint8 = 1 << 6
	xlhpHasNowUnusedItems  uint8 = 1 << 7

	xlhFreezeXvac  uint8 = 0x02
	xlhInvalidXvac uint8 = 0x04

	pgLPUnused   = 0
	pgLPNormal   = 1
	pgLPRedirect = 2
	pgLPDead     = 3

	xlhUpdatePrefixFromOld uint8 = 1 << 5
	xlhUpdateSuffixFromOld uint8 = 1 << 6

	pgPageHeaderSize     = 24
	pgPageSizeVersion    = 0x2004
	pgSizeOfHeapTupleHdr = 23
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

func applyXLHeapInfobits(tup []byte, infobits uint8) {
	im1 := binary.LittleEndian.Uint16(tup[20:22])
	im2 := binary.LittleEndian.Uint16(tup[18:20])
	im1 &^= pgHeapXmaxInvalid | 0x0400 | 0x1000 | 0x0080 | 0x0040 | 0x0010
	im2 &^= 0x2000
	if (infobits & 0x01) != 0 { // XLHL_XMAX_IS_MULTI
		im1 |= 0x1000 // HEAP_XMAX_IS_MULTI
	}
	if (infobits & 0x02) != 0 { // XLHL_XMAX_LOCK_ONLY
		im1 |= 0x0080 // HEAP_XMAX_LOCK_ONLY
	}
	if (infobits & 0x04) != 0 { // XLHL_XMAX_EXCL_LOCK
		im1 |= 0x0040 // HEAP_XMAX_EXCL_LOCK
	}
	if (infobits & 0x08) != 0 { // XLHL_XMAX_KEYSHR_LOCK
		im1 |= 0x0010 // HEAP_XMAX_KEYSHR_LOCK
	}
	if (infobits & 0x10) != 0 { // XLHL_KEYS_UPDATED
		im2 |= 0x2000 // HEAP_KEYS_UPDATED
	}
	binary.LittleEndian.PutUint16(tup[20:22], im1)
	binary.LittleEndian.PutUint16(tup[18:20], im2)
}

func pageMarkTupleDeleted(page []byte, offnum uint16, xmax uint32, infobits uint8) {
	tup := pageGetHeapTuple(page, offnum)
	if tup == nil {
		return
	}
	binary.LittleEndian.PutUint32(tup[4:8], xmax)
	applyXLHeapInfobits(tup, infobits)
}

func pageMarkTupleUpdated(page []byte, oldOffnum uint16, oldXmax uint32, infobits uint8, newBlkno uint32, newOffnum uint16, isHOT bool) {
	tup := pageGetHeapTuple(page, oldOffnum)
	if tup == nil {
		return
	}
	binary.LittleEndian.PutUint32(tup[4:8], oldXmax)
	applyXLHeapInfobits(tup, infobits)
	// Update t_ctid to point to the updated tuple (newBlkno, newOffnum)
	binary.LittleEndian.PutUint16(tup[12:14], uint16(newBlkno>>16))
	binary.LittleEndian.PutUint16(tup[14:16], uint16(newBlkno&0xFFFF))
	binary.LittleEndian.PutUint16(tup[16:18], newOffnum)
	im2 := binary.LittleEndian.Uint16(tup[18:20])
	if isHOT {
		im2 |= 0x4000 // HEAP_HOT_UPDATED
	} else {
		im2 &^= 0x4000
	}
	binary.LittleEndian.PutUint16(tup[18:20], im2)
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
// Returns (page, true) if all WAL records were handled safely, or (nil, false)
// if an unsupported or malformed WAL record was encountered.
func applyHeapWALRecords(key Key, basePage []byte, records [][]byte) ([]byte, bool) {
	var page []byte
	if len(basePage) == PageSize {
		page = append([]byte(nil), basePage...)
	}

	for _, raw := range records {
		rec, err := DecodeWALRecord(0, raw)
		if err != nil {
			return nil, false
		}
		var ok bool
		page, ok = applyDecodedHeapWALRecord(key, page, rec)
		if !ok {
			return nil, false
		}
	}
	if len(page) != PageSize {
		return nil, false
	}
	return page, true
}

// applyDecodedHeapWALRecord applies a single decoded Heap/Heap2/Btree record onto page in-place.
func applyDecodedHeapWALRecord(key Key, page []byte, rec *DecodedWALRecord) ([]byte, bool) {
	for once := true; once; once = false {
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

			case XLogHeapDelete, 0x60: // XLogHeapDelete or XLogHeapLock
				blk := findWALBlock(rec, 0, key)
				if blk == nil || len(rec.MainData) < 8 || len(page) != PageSize {
					continue
				}
				xmax := binary.LittleEndian.Uint32(rec.MainData[0:4])
				offnum := binary.LittleEndian.Uint16(rec.MainData[4:6])
				infobits := rec.MainData[6]
				pageMarkTupleDeleted(page, offnum, xmax, infobits)

			case XLogHeapUpdate, XLogHeapHotUpdate:
				if len(rec.MainData) < 14 {
					continue
				}
				oldXmax := binary.LittleEndian.Uint32(rec.MainData[0:4])
				oldOffnum := binary.LittleEndian.Uint16(rec.MainData[4:6])
				oldInfobits := rec.MainData[6]
				flags := rec.MainData[7]
				newXmax := binary.LittleEndian.Uint32(rec.MainData[8:12])
				newOffnum := binary.LittleEndian.Uint16(rec.MainData[12:14])
				isHOT := op == XLogHeapHotUpdate

				newBlkno := key.BlockNum
				for _, b := range rec.Blocks {
					if b.BlockID == 0 {
						newBlkno = b.BlockNum
						break
					}
				}

				// Block 1 (if present in rec.Blocks) is the old tuple page for a cross-page update;
				// if BlockID 1 is absent from rec.Blocks, it is a same-page update where block 0 is both old and new.
				hasOldBlkRef := false
				for i := range rec.Blocks {
					if rec.Blocks[i].BlockID == 1 {
						hasOldBlkRef = true
						break
					}
				}
				oldBlk := findWALBlock(rec, 1, key)
				newBlk := findWALBlock(rec, 0, key)
				if oldBlk != nil && len(page) == PageSize {
					pageMarkTupleUpdated(page, oldOffnum, oldXmax, oldInfobits, newBlkno, newOffnum, isHOT)
				}
				if newBlk != nil {
					var oldUserData []byte
					if isInit || len(page) != PageSize {
						page = make([]byte, PageSize)
						initPGHeapPage(page)
					} else if !hasOldBlkRef {
						// Same-page update: snapshot old user data for prefix/suffix reconstruction,
						// then mark old tuple updated (with t_ctid -> newOffnum and HEAP_HOT_UPDATED) on this page.
						if oldTup := pageGetHeapTuple(page, oldOffnum); oldTup != nil {
							oldHoff := int(oldTup[22])
							if oldHoff >= pgSizeOfHeapTupleHdr && oldHoff <= len(oldTup) {
								oldUserData = append([]byte(nil), oldTup[oldHoff:]...)
							}
						}
						pageMarkTupleUpdated(page, oldOffnum, oldXmax, oldInfobits, newBlkno, newOffnum, isHOT)
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

			case 0x50: // XLOG_HEAP_CONFIRM: speculative insert confirmed; no visibility change needed.
				continue

			default:
				return nil, false
			}
		} else if rec.Rmid == RmHeap2ID {
			op := rec.Info & XLogHeapOpMask
			switch op {
			case XLogHeap2PruneOnAccess, XLogHeap2PruneVacuumScan, XLogHeap2PruneVacuumCleanup:
				if !applyHeap2Prune(key, page, rec) {
					return nil, false
				}

			case XLogHeap2Visible:
				// XLOG_HEAP2_VISIBLE: marks heap page PD_ALL_VISIBLE (0x0004) when base page is present.
				if len(page) == PageSize {
					flags := binary.LittleEndian.Uint16(page[10:12])
					binary.LittleEndian.PutUint16(page[10:12], flags|0x0004)
				}

			case XLogHeap2MultiInsert:
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

			case XLogHeap2LockUpdated:
				blk := findWALBlock(rec, 0, key)
				if blk == nil || len(rec.MainData) < 8 || len(page) != PageSize {
					continue
				}
				xmax := binary.LittleEndian.Uint32(rec.MainData[0:4])
				offnum := binary.LittleEndian.Uint16(rec.MainData[4:6])
				infobits := rec.MainData[6]
				pageMarkTupleDeleted(page, offnum, xmax, infobits)

			case 0x70: // XLOG_HEAP2_NEW_CID: logical decoding metadata only, does not modify heap page.
				continue

			default:
				return nil, false
			}
		} else if rec.Rmid == RmBtreeID {
			page = applyBtreeWALRecords(key, page, [][]byte{rec.RawRecord})
		} else {
			return nil, false
		}
	}
	return page, true
}

func pageSetLinePointer(page []byte, offnum uint16, lpVal uint32) {
	if len(page) != PageSize || offnum == 0 {
		return
	}
	lower := int(binary.LittleEndian.Uint16(page[12:14]))
	lpOff := pgPageHeaderSize + (int(offnum)-1)*4
	if lpOff+4 > lower {
		return
	}
	binary.LittleEndian.PutUint32(page[lpOff:lpOff+4], lpVal)
}

// applyHeap2Prune replays PG17 xl_heap_prune (PRUNE_ON_ACCESS, PRUNE_VACUUM_SCAN,
// PRUNE_VACUUM_CLEANUP) onto page.
func applyHeap2Prune(key Key, page []byte, rec *DecodedWALRecord) bool {
	blk := findWALBlock(rec, 0, key)
	if blk == nil {
		// Record does not target this block.
		return true
	}
	if len(page) != PageSize || len(rec.MainData) < 2 {
		return false
	}
	flags := rec.MainData[1]
	data := blk.BlockData
	off := 0

	type freezePlan struct {
		xmax       uint32
		infomask2  uint16
		infomask   uint16
		frzflags   uint8
		ntuples    uint16
	}
	var plans []freezePlan
	totalFrozen := 0

	if (flags & xlhpHasFreezePlans) != 0 {
		if off+2 > len(data) {
			return false
		}
		nplans := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2 // PG17 log_heap_prune_and_freeze: uint16 nplans (no alignment padding)
		if off+nplans*12 > len(data) {
			return false
		}
		plans = make([]freezePlan, nplans)
		for i := 0; i < nplans; i++ {
			p := freezePlan{
				xmax:      binary.LittleEndian.Uint32(data[off : off+4]),
				infomask2: binary.LittleEndian.Uint16(data[off+4 : off+6]),
				infomask:  binary.LittleEndian.Uint16(data[off+6 : off+8]),
				frzflags:  data[off+8],
				ntuples:   binary.LittleEndian.Uint16(data[off+10 : off+12]),
			}
			plans[i] = p
			totalFrozen += int(p.ntuples)
			off += 12
		}
	}

	if (flags & xlhpHasRedirections) != 0 {
		if off+2 > len(data) {
			return false
		}
		nredir := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2
		if off+nredir*4 > len(data) {
			return false
		}
		for i := 0; i < nredir; i++ {
			fromOff := binary.LittleEndian.Uint16(data[off : off+2])
			toOff := binary.LittleEndian.Uint16(data[off+2 : off+4])
			off += 4
			// ItemIdSetRedirect: LP_REDIRECT (2), offset = toOff, length = 0
			pageSetLinePointer(page, fromOff, uint32(toOff&0x7FFF)|(uint32(pgLPRedirect)<<15))
		}
	}

	if (flags & xlhpHasDeadItems) != 0 {
		if off+2 > len(data) {
			return false
		}
		ndead := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2
		if off+ndead*2 > len(data) {
			return false
		}
		for i := 0; i < ndead; i++ {
			deadOff := binary.LittleEndian.Uint16(data[off : off+2])
			off += 2
			// ItemIdSetDead: LP_DEAD (3), offset = 0, length = 0
			pageSetLinePointer(page, deadOff, uint32(pgLPDead)<<15)
		}
	}

	if (flags & xlhpHasNowUnusedItems) != 0 {
		if off+2 > len(data) {
			return false
		}
		nunused := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2
		if off+nunused*2 > len(data) {
			return false
		}
		for i := 0; i < nunused; i++ {
			unusedOff := binary.LittleEndian.Uint16(data[off : off+2])
			off += 2
			// ItemIdSetUnused: LP_UNUSED (0)
			pageSetLinePointer(page, unusedOff, 0)
		}
	}

	if totalFrozen > 0 {
		if off+totalFrozen*2 > len(data) {
			return false
		}
		frzIdx := 0
		for _, p := range plans {
			for k := 0; k < int(p.ntuples); k++ {
				offnum := binary.LittleEndian.Uint16(data[off+frzIdx*2 : off+frzIdx*2+2])
				frzIdx++
				if tup := pageGetHeapTuple(page, offnum); tup != nil {
					binary.LittleEndian.PutUint32(tup[4:8], p.xmax)
					binary.LittleEndian.PutUint16(tup[18:20], p.infomask2)
					binary.LittleEndian.PutUint16(tup[20:22], p.infomask)
					if (p.frzflags & xlhFreezeXvac) != 0 {
						binary.LittleEndian.PutUint32(tup[8:12], 2) // FrozenTransactionId
					} else if (p.frzflags & xlhInvalidXvac) != 0 {
						binary.LittleEndian.PutUint32(tup[8:12], 0) // InvalidTransactionId
					}
				}
			}
		}
	}

	if (flags & xlhpCleanupLock) != 0 {
		// Clear PD_PAGE_FULL (0x0002)
		pdFlags := binary.LittleEndian.Uint16(page[10:12])
		binary.LittleEndian.PutUint16(page[10:12], pdFlags&^0x0002)
	}
	return true
}
