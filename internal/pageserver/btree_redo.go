package pageserver

import (
	"encoding/binary"
)

const (
	RmBtreeID uint8 = 11

	XLogBtreeInsertLeaf  uint8 = 0x00
	XLogBtreeInsertUpper uint8 = 0x10
	XLogBtreeInsertMeta  uint8 = 0x20
	XLogBtreeSplitL      uint8 = 0x30
	XLogBtreeSplitR      uint8 = 0x40
	XLogBtreeInsertPost  uint8 = 0x50
	XLogBtreeDedup       uint8 = 0x60
	XLogBtreeDelete      uint8 = 0x70
	XLogBtreeUnlinkPage  uint8 = 0x80
	XLogBtreeUnlinkMeta  uint8 = 0x90
	XLogBtreeNewRoot     uint8 = 0xA0
	XLogBtreeReusePage   uint8 = 0xB0
	XLogBtreeVacuum      uint8 = 0xC0
	XLogBtreeOpMask      uint8 = 0xF0

	pgSizeOfBTPageOpaque = 16
	pgBTMetaMagic        = 0x053162
	pgBTPLeaf            = 1 << 0
	pgBTPRoot            = 1 << 1
	pgBTPDeleted         = 1 << 2
	pgBTPMeta            = 1 << 3
	pgBTPHalfDead        = 1 << 4
	pgBTPHasGarbage      = 1 << 6

	pgIndexSizeMask   uint16 = 0x1FFF
	pgIndexAltTidMask uint16 = 0x2000
	pgBtIsPosting     uint16 = 0x2000
	pgLpDead          uint16 = 3
)

func initPGBtreePage(page []byte, flags uint16, level uint32) {
	for i := range page {
		page[i] = 0
	}
	special := uint16(PageSize - pgSizeOfBTPageOpaque)
	binary.LittleEndian.PutUint16(page[12:14], pgPageHeaderSize)  // pd_lower
	binary.LittleEndian.PutUint16(page[14:16], special)           // pd_upper
	binary.LittleEndian.PutUint16(page[16:18], special)           // pd_special
	binary.LittleEndian.PutUint16(page[18:20], pgPageSizeVersion) // pd_pagesize_version

	sp := page[special:]
	binary.LittleEndian.PutUint32(sp[0:4], 0)       // btpo_prev
	binary.LittleEndian.PutUint32(sp[4:8], 0)       // btpo_next
	binary.LittleEndian.PutUint32(sp[8:12], level)  // btpo_level
	binary.LittleEndian.PutUint16(sp[12:14], flags) // btpo_flags
	binary.LittleEndian.PutUint16(sp[14:16], 0)     // btpo_cycleid
}

func btRestoreMetaPage(page []byte, data []byte) {
	if len(data) < 28 {
		return
	}
	initPGBtreePage(page, pgBTPMeta, 0)
	meta := page[pgPageHeaderSize:]
	binary.LittleEndian.PutUint32(meta[0:4], pgBTMetaMagic)
	binary.LittleEndian.PutUint32(meta[4:8], binary.LittleEndian.Uint32(data[0:4]))    // version
	binary.LittleEndian.PutUint32(meta[8:12], binary.LittleEndian.Uint32(data[4:8]))   // root
	binary.LittleEndian.PutUint32(meta[12:16], binary.LittleEndian.Uint32(data[8:12])) // level
	binary.LittleEndian.PutUint32(meta[16:20], binary.LittleEndian.Uint32(data[12:16])) // fastroot
	binary.LittleEndian.PutUint32(meta[20:24], binary.LittleEndian.Uint32(data[16:20])) // fastlevel
	binary.LittleEndian.PutUint32(meta[24:28], binary.LittleEndian.Uint32(data[20:24])) // num_delpages
	binary.LittleEndian.PutUint64(meta[32:40], 0xBFF0000000000000)                      // -1.0
	meta[40] = data[24]                                                                 // allequalimage
	binary.LittleEndian.PutUint16(page[12:14], pgPageHeaderSize+48)
}

func btPageMaxOff(page []byte) uint16 {
	lower := int(binary.LittleEndian.Uint16(page[12:14]))
	if lower <= pgPageHeaderSize {
		return 0
	}
	return uint16((lower - pgPageHeaderSize) / 4)
}

func btPageGetItem(page []byte, offnum uint16) []byte {
	maxOff := btPageMaxOff(page)
	if offnum < 1 || offnum > maxOff {
		return nil
	}
	lpOff := pgPageHeaderSize + (int(offnum)-1)*4
	raw := binary.LittleEndian.Uint32(page[lpOff : lpOff+4])
	off := int(raw & 0x7FFF)
	length := int((raw >> 17) & 0x7FFF)
	if off < pgPageHeaderSize || length < 8 || off+length > PageSize-pgSizeOfBTPageOpaque {
		return nil
	}
	return page[off : off+length]
}

func btPageGetItemFlags(page []byte, offnum uint16) uint16 {
	maxOff := btPageMaxOff(page)
	if offnum < 1 || offnum > maxOff {
		return 0
	}
	lpOff := pgPageHeaderSize + (int(offnum)-1)*4
	raw := binary.LittleEndian.Uint32(page[lpOff : lpOff+4])
	return uint16((raw >> 15) & 0x03)
}

func btPageInsertItem(page []byte, offnum uint16, item []byte) {
	if len(page) != PageSize || offnum < 1 || len(item) == 0 {
		return
	}
	lower := int(binary.LittleEndian.Uint16(page[12:14]))
	upper := int(binary.LittleEndian.Uint16(page[14:16]))
	special := int(binary.LittleEndian.Uint16(page[16:18]))
	if lower < pgPageHeaderSize || special != PageSize-pgSizeOfBTPageOpaque || lower > upper || upper > special {
		initPGBtreePage(page, pgBTPLeaf, 0)
		lower = pgPageHeaderSize
		upper = PageSize - pgSizeOfBTPageOpaque
	}
	maxOff := uint16((lower - pgPageHeaderSize) / 4)
	if offnum > maxOff+1 {
		offnum = maxOff + 1
	}

	aligned := heapMaxAlign(len(item))
	if upper-aligned < lower+4 {
		// Compact page if fragmented
		var items [][]byte
		var fls []uint16
		for o := uint16(1); o <= maxOff; o++ {
			it := btPageGetItem(page, o)
			if it != nil {
				cp := make([]byte, len(it))
				copy(cp, it)
				items = append(items, cp)
				fls = append(fls, btPageGetItemFlags(page, o))
			}
		}
		btPageRebuildWithItems(page, items, fls)
		lower = int(binary.LittleEndian.Uint16(page[12:14]))
		upper = int(binary.LittleEndian.Uint16(page[14:16]))
		if upper-aligned < lower+4 {
			return
		}
	}
	newUpper := upper - aligned
	copy(page[newUpper:newUpper+len(item)], item)

	insertLPOff := pgPageHeaderSize + (int(offnum)-1)*4
	if offnum <= maxOff {
		copy(page[insertLPOff+4:lower+4], page[insertLPOff:lower])
	}
	itemID := uint32(newUpper&0x7FFF) | (uint32(pgLPNormal) << 15) | (uint32(len(item)&0x7FFF) << 17)
	binary.LittleEndian.PutUint32(page[insertLPOff:insertLPOff+4], itemID)
	binary.LittleEndian.PutUint16(page[12:14], uint16(lower+4))
	binary.LittleEndian.PutUint16(page[14:16], uint16(newUpper))
}

func btPageRebuildWithItems(page []byte, items [][]byte, flagsList []uint16) {
	special := uint16(PageSize - pgSizeOfBTPageOpaque)
	var opaque [pgSizeOfBTPageOpaque]byte
	copy(opaque[:], page[special:])
	lsn0 := binary.LittleEndian.Uint32(page[0:4])
	lsn1 := binary.LittleEndian.Uint32(page[4:8])
	pflags := binary.LittleEndian.Uint16(page[10:12])

	for i := range page {
		page[i] = 0
	}
	binary.LittleEndian.PutUint32(page[0:4], lsn0)
	binary.LittleEndian.PutUint32(page[4:8], lsn1)
	binary.LittleEndian.PutUint16(page[10:12], pflags)
	binary.LittleEndian.PutUint16(page[16:18], special)
	binary.LittleEndian.PutUint16(page[18:20], pgPageSizeVersion)
	copy(page[special:], opaque[:])

	lower := uint16(pgPageHeaderSize)
	upper := special
	for i, it := range items {
		fl := uint16(pgLPNormal)
		if i < len(flagsList) && flagsList[i] != 0 {
			fl = flagsList[i]
		}
		aligned := uint16(heapMaxAlign(len(it)))
		if int(upper)-int(aligned) < int(lower)+4 {
			break
		}
		upper -= aligned
		copy(page[upper:upper+uint16(len(it))], it)
		itemID := uint32(upper&0x7FFF) | (uint32(fl&0x03) << 15) | (uint32(len(it)&0x7FFF) << 17)
		binary.LittleEndian.PutUint32(page[lower:lower+4], itemID)
		lower += 4
	}
	binary.LittleEndian.PutUint16(page[12:14], lower)
	binary.LittleEndian.PutUint16(page[14:16], upper)
}

func btIsPostingTuple(itup []byte) bool {
	if len(itup) < 8 {
		return false
	}
	posid := binary.LittleEndian.Uint16(itup[4:6])
	tinfo := binary.LittleEndian.Uint16(itup[6:8])
	return (tinfo&pgIndexAltTidMask) != 0 && (posid&pgBtIsPosting) != 0
}

// btSwapPosting implements PostgreSQL's _bt_swap_posting(newitem, oposting, postingoff).
// For XLOG_BTREE_INSERT_POST, postingoff is the 1-based TID index in oposting's posting list
// where newitem's TID should be inserted, while the last TID of oposting becomes the new plain tuple.
// For XLOG_BTREE_SPLIT_L/R, postingoff is the byte offset inside oposting where the posting list
// is split into leftPosting (kept on left page) and rightPosting (new item).
func btSwapPostingInsert(neworig []byte, oposting []byte, postingIdx uint16) (updatedPosting []byte, newPlain []byte) {
	if len(oposting) < 16 || len(neworig) < 8 || !btIsPostingTuple(oposting) {
		return oposting, neworig
	}
	poff := int(binary.LittleEndian.Uint32(oposting[0:4]))
	posid := binary.LittleEndian.Uint16(oposting[4:6])
	nposting := int(posid &^ pgBtIsPosting)
	if poff < 8 || poff+nposting*6 > len(oposting) || nposting < 2 || int(postingIdx) < 1 || int(postingIdx) >= nposting {
		return oposting, neworig
	}

	// Build plain tuple from neworig with the last TID from oposting.
	plain := make([]byte, len(neworig))
	copy(plain, neworig)
	lastTIDOff := poff + (nposting-1)*6
	copy(plain[0:6], oposting[lastTIDOff:lastTIDOff+6])
	// Ensure INDEX_ALT_TID_MASK is cleared on plain tuple.
	tinfo := binary.LittleEndian.Uint16(plain[6:8]) &^ pgIndexAltTidMask
	binary.LittleEndian.PutUint16(plain[6:8], tinfo)

	// Shift TIDs in oposting[poff + (postingIdx-1)*6 .. lastTIDOff) right by 6 bytes and insert neworig's TID.
	upd := make([]byte, len(oposting))
	copy(upd, oposting)
	insOff := poff + int(postingIdx-1)*6
	copy(upd[insOff+6:lastTIDOff+6], oposting[insOff:lastTIDOff])
	copy(upd[insOff:insOff+6], neworig[0:6])
	return upd, plain
}

// btUpdatePosting updates a posting list tuple during DELETE / VACUUM by removing the
// 0-based TID indices in deletedIndices. If 1 TID remains, converts to a plain tuple.
func btUpdatePosting(oposting []byte, deletedIndices []uint16) []byte {
	if len(oposting) < 16 || !btIsPostingTuple(oposting) {
		return oposting
	}
	poff := int(binary.LittleEndian.Uint32(oposting[0:4]))
	posid := binary.LittleEndian.Uint16(oposting[4:6])
	nposting := int(posid &^ pgBtIsPosting)
	if poff < 8 || poff+nposting*6 > len(oposting) || nposting < 2 {
		return oposting
	}
	delSet := make(map[int]bool, len(deletedIndices))
	for _, idx := range deletedIndices {
		delSet[int(idx)] = true
	}
	var kept [][6]byte
	for i := 0; i < nposting; i++ {
		if !delSet[i] {
			var tid [6]byte
			copy(tid[:], oposting[poff+i*6:poff+(i+1)*6])
			kept = append(kept, tid)
		}
	}
	if len(kept) == 0 {
		return oposting
	}
	if len(kept) == 1 {
		// Convert back to plain tuple of size heapMaxAlign(poff).
		plainLen := heapMaxAlign(poff)
		plain := make([]byte, plainLen)
		copy(plain, oposting[:poff])
		copy(plain[0:6], kept[0][:])
		tinfo := (binary.LittleEndian.Uint16(oposting[6:8]) &^ (pgIndexAltTidMask | pgIndexSizeMask)) | uint16(plainLen)
		binary.LittleEndian.PutUint16(plain[6:8], tinfo)
		return plain
	}
	newRawLen := poff + len(kept)*6
	newLen := heapMaxAlign(newRawLen)
	upd := make([]byte, newLen)
	copy(upd[:poff], oposting[:poff])
	binary.LittleEndian.PutUint32(upd[0:4], uint32(poff))
	binary.LittleEndian.PutUint16(upd[4:6], uint16(len(kept))|pgBtIsPosting)
	tinfo := (binary.LittleEndian.Uint16(oposting[6:8]) &^ pgIndexSizeMask) | pgIndexAltTidMask | uint16(newLen)
	binary.LittleEndian.PutUint16(upd[6:8], tinfo)
	for i, tid := range kept {
		copy(upd[poff+i*6:poff+(i+1)*6], tid[:])
	}
	return upd
}

// btFormPostingTuple merges baseTuple and the TIDs of all tuples in group into a single posting list tuple.
func btFormPostingTuple(group [][]byte) []byte {
	if len(group) == 0 {
		return nil
	}
	if len(group) == 1 {
		return group[0]
	}
	base := group[0]
	var poff int
	if btIsPostingTuple(base) {
		poff = int(binary.LittleEndian.Uint32(base[0:4]))
	} else {
		// Key ends before any trailing alignment padding? No, in PostgreSQL _bt_form_posting,
		// ip_blkid (posting offset) is IndexTupleSize(base) (which is already MAXALIGNed, or SHORTALIGNed in PG).
		poff = int(binary.LittleEndian.Uint16(base[6:8]) & pgIndexSizeMask)
	}
	var tids [][6]byte
	for _, it := range group {
		if btIsPostingTuple(it) {
			ip := int(binary.LittleEndian.Uint32(it[0:4]))
			np := int(binary.LittleEndian.Uint16(it[4:6]) &^ pgBtIsPosting)
			for k := 0; k < np && ip+(k+1)*6 <= len(it); k++ {
				var t [6]byte
				copy(t[:], it[ip+k*6:ip+(k+1)*6])
				tids = append(tids, t)
			}
		} else if len(it) >= 8 {
			var t [6]byte
			copy(t[:], it[0:6])
			tids = append(tids, t)
		}
	}
	totalRaw := poff + len(tids)*6
	totalLen := heapMaxAlign(totalRaw)
	out := make([]byte, totalLen)
	copy(out[:poff], base[:poff])
	binary.LittleEndian.PutUint32(out[0:4], uint32(poff))
	binary.LittleEndian.PutUint16(out[4:6], uint16(len(tids))|pgBtIsPosting)
	tinfo := (binary.LittleEndian.Uint16(base[6:8]) &^ pgIndexSizeMask) | pgIndexAltTidMask | uint16(totalLen)
	binary.LittleEndian.PutUint16(out[6:8], tinfo)
	for i, t := range tids {
		copy(out[poff+i*6:poff+(i+1)*6], t[:])
	}
	return out
}

func parsePackedItups(data []byte) [][]byte {
	var out [][]byte
	off := 0
	for off+8 <= len(data) {
		tinfo := binary.LittleEndian.Uint16(data[off+6 : off+8])
		sz := int(tinfo & pgIndexSizeMask)
		if sz < 8 || off+sz > len(data) {
			break
		}
		cp := make([]byte, sz)
		copy(cp, data[off:off+sz])
		out = append(out, cp)
		off += heapMaxAlign(sz)
	}
	return out
}

// btRestorePageItems implements PostgreSQL's _bt_restore_page: in WAL records that dump
// a page's [pd_upper .. pd_special) region (XLOG_BTREE_SPLIT_L/R block 1 and XLOG_BTREE_NEWROOT block 0),
// items appear in reverse OffsetNumber order (from highest OffsetNumber at pd_upper to OffsetNumber 1 at pd_special).
func btRestorePageItems(data []byte) [][]byte {
	items := parsePackedItups(data)
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
	return items
}

// btTruncatePostingLeft truncates a posting list tuple at byte offset postingoff (used when
// a B-tree page split happens in the middle of a posting list at firstrightoff-1).
func btTruncatePostingLeft(oposting []byte, postingoff uint16) []byte {
	if len(oposting) < 16 || !btIsPostingTuple(oposting) {
		return oposting
	}
	poff := int(binary.LittleEndian.Uint32(oposting[0:4]))
	cut := int(postingoff)
	if cut <= poff || cut > len(oposting) || (cut-poff)%6 != 0 {
		return oposting
	}
	nLeftTIDs := (cut - poff) / 6
	if nLeftTIDs == 1 {
		// Convert back to plain index tuple
		plain := make([]byte, poff)
		copy(plain[0:6], oposting[poff:poff+6])
		tinfo := (binary.LittleEndian.Uint16(oposting[6:8]) &^ (pgIndexSizeMask | pgIndexAltTidMask)) | uint16(poff)
		binary.LittleEndian.PutUint16(plain[6:8], tinfo)
		copy(plain[8:poff], oposting[8:poff])
		return plain
	}
	out := make([]byte, cut)
	copy(out, oposting[:cut])
	binary.LittleEndian.PutUint32(out[0:4], uint32(poff))
	binary.LittleEndian.PutUint16(out[4:6], uint16(nLeftTIDs)|pgBtIsPosting)
	tinfo := (binary.LittleEndian.Uint16(oposting[6:8]) &^ pgIndexSizeMask) | pgIndexAltTidMask | uint16(cut)
	binary.LittleEndian.PutUint16(out[6:8], tinfo)
	return out
}

// applyBtreeWALRecords replays PostgreSQL 15/17 RmBtreeID (11) WAL records onto basePage.
func applyBtreeWALRecords(key Key, basePage []byte, records [][]byte) []byte {
	var page []byte
	if len(basePage) == PageSize {
		page = append([]byte(nil), basePage...)
	}

	special := PageSize - pgSizeOfBTPageOpaque

	for _, raw := range records {
		rec, err := DecodeWALRecord(0, raw)
		if err != nil || rec.Rmid != RmBtreeID {
			continue
		}
		op := rec.Info & XLogBtreeOpMask

		switch op {
		case XLogBtreeInsertLeaf, XLogBtreeInsertUpper, XLogBtreeInsertMeta:
			// Block 0: target leaf/internal page
			if blk0 := findWALBlock(rec, 0, key); blk0 != nil && len(rec.MainData) >= 2 && len(page) == PageSize {
				offnum := binary.LittleEndian.Uint16(rec.MainData[0:2])
				if len(blk0.BlockData) >= 8 {
					btPageInsertItem(page, offnum, blk0.BlockData)
				}
			}
			// Block 2 (for INSERT_META): metapage (block 0)
			if op == XLogBtreeInsertMeta {
				if blk2 := findWALBlock(rec, 2, key); blk2 != nil && len(blk2.BlockData) >= 28 {
					if len(page) != PageSize {
						page = make([]byte, PageSize)
					}
					btRestoreMetaPage(page, blk2.BlockData)
				}
			}

		case XLogBtreeInsertPost:
			if blk0 := findWALBlock(rec, 0, key); blk0 != nil && len(rec.MainData) >= 2 && len(blk0.BlockData) >= 10 && len(page) == PageSize {
				offnum := binary.LittleEndian.Uint16(rec.MainData[0:2])
				postingIdx := binary.LittleEndian.Uint16(blk0.BlockData[0:2])
				neworig := blk0.BlockData[2:]
				maxOff := btPageMaxOff(page)
				var items [][]byte
				var fls []uint16
				for o := uint16(1); o <= maxOff; o++ {
					it := btPageGetItem(page, o)
					fl := btPageGetItemFlags(page, o)
					if o+1 == offnum && it != nil {
						updPost, plain := btSwapPostingInsert(neworig, it, postingIdx)
						items = append(items, updPost, plain)
						fls = append(fls, fl, pgLPNormal)
					} else if it != nil {
						cp := make([]byte, len(it))
						copy(cp, it)
						items = append(items, cp)
						fls = append(fls, fl)
					}
				}
				btPageRebuildWithItems(page, items, fls)
			}

		case XLogBtreeSplitL, XLogBtreeSplitR:
			if len(rec.MainData) < 10 {
				continue
			}
			level := binary.LittleEndian.Uint32(rec.MainData[0:4])
			firstrightoff := binary.LittleEndian.Uint16(rec.MainData[4:6])
			newitemoff := binary.LittleEndian.Uint16(rec.MainData[6:8])
			postingoff := binary.LittleEndian.Uint16(rec.MainData[8:10])
			isLeaf := level == 0
			var rnext uint32
			for _, b := range rec.Blocks {
				if b.BlockID == 2 {
					rnext = b.BlockNum
				}
			}
			var rblkno, lblkno uint32
			for _, b := range rec.Blocks {
				if b.BlockID == 0 {
					lblkno = b.BlockNum
				} else if b.BlockID == 1 {
					rblkno = b.BlockNum
				}
			}

			// Block 0: Left page
			if blk0 := findWALBlock(rec, 0, key); blk0 != nil && len(page) == PageSize {
				maxOff := btPageMaxOff(page)
				oldNext := binary.LittleEndian.Uint32(page[special+4 : special+8])
				firstDataOff := uint16(1)
				if oldNext != 0 {
					firstDataOff = 2
				}
				// Parse blk0.BlockData: if op == XLogBtreeSplitL, newitem is first, followed by left high key.
				// If op == XLogBtreeSplitR, blk0.BlockData contains only the left high key.
				extra := parsePackedItups(blk0.BlockData)
				var newItem, leftHighKey []byte
				if op == XLogBtreeSplitL {
					if len(extra) >= 2 {
						newItem = extra[0]
						leftHighKey = extra[1]
					} else if len(extra) == 1 {
						leftHighKey = extra[0]
					}
				} else if len(extra) >= 1 {
					leftHighKey = extra[0]
				}

				var newLeftItems [][]byte
				if leftHighKey != nil {
					newLeftItems = append(newLeftItems, leftHighKey)
				}
				for off := firstDataOff; off < firstrightoff && off <= maxOff; off++ {
					if op == XLogBtreeSplitL && newItem != nil && off == newitemoff {
						newLeftItems = append(newLeftItems, newItem)
					}
					if it := btPageGetItem(page, off); it != nil {
						if postingoff != 0 && off == firstrightoff-1 {
							newLeftItems = append(newLeftItems, btTruncatePostingLeft(it, postingoff))
						} else {
							cp := make([]byte, len(it))
							copy(cp, it)
							newLeftItems = append(newLeftItems, cp)
						}
					}
				}
				if op == XLogBtreeSplitL && newItem != nil && firstrightoff == newitemoff {
					newLeftItems = append(newLeftItems, newItem)
				}

				// Update left page opaque: clear BTP_ROOT, set btpo_next = rblkno
				flags := binary.LittleEndian.Uint16(page[special+12 : special+14])
				flags &^= pgBTPRoot
				if isLeaf {
					flags |= pgBTPLeaf
				}
				binary.LittleEndian.PutUint32(page[special+4:special+8], rblkno)
				binary.LittleEndian.PutUint32(page[special+8:special+12], level)
				binary.LittleEndian.PutUint16(page[special+12:special+14], flags)
				btPageRebuildWithItems(page, newLeftItems, nil)
			}

			// Block 1: Right page (will_init, populated via _bt_restore_page)
			if blk1 := findWALBlock(rec, 1, key); blk1 != nil {
				if len(page) != PageSize {
					page = make([]byte, PageSize)
				}
				var flags uint16
				if isLeaf {
					flags |= pgBTPLeaf
				}
				initPGBtreePage(page, flags, level)
				binary.LittleEndian.PutUint32(page[special+0:special+4], lblkno)
				binary.LittleEndian.PutUint32(page[special+4:special+8], rnext)
				rightItems := btRestorePageItems(blk1.BlockData)
				btPageRebuildWithItems(page, rightItems, nil)
			}

			// Block 2: Right sibling's btpo_prev updated to rblkno
			if blk2 := findWALBlock(rec, 2, key); blk2 != nil && len(page) == PageSize {
				binary.LittleEndian.PutUint32(page[special+0:special+4], rblkno)
			}

		case XLogBtreeNewRoot:
			if len(rec.MainData) < 8 {
				continue
			}
			rootBlk := binary.LittleEndian.Uint32(rec.MainData[0:4])
			level := binary.LittleEndian.Uint32(rec.MainData[4:8])
			_ = rootBlk
			// Block 0: new root page (populated via _bt_restore_page)
			if blk0 := findWALBlock(rec, 0, key); blk0 != nil {
				if len(page) != PageSize {
					page = make([]byte, PageSize)
				}
				flags := uint16(pgBTPRoot)
				if level == 0 {
					flags |= pgBTPLeaf
				}
				initPGBtreePage(page, flags, level)
				if len(blk0.BlockData) > 0 {
					items := btRestorePageItems(blk0.BlockData)
					btPageRebuildWithItems(page, items, nil)
				}
			}
			// Block 2: metapage (block 0)
			if blk2 := findWALBlock(rec, 2, key); blk2 != nil && len(blk2.BlockData) >= 28 {
				if len(page) != PageSize {
					page = make([]byte, PageSize)
				}
				btRestoreMetaPage(page, blk2.BlockData)
			}

		case XLogBtreeDedup:
			if blk0 := findWALBlock(rec, 0, key); blk0 != nil && len(rec.MainData) >= 2 && len(page) == PageSize {
				nintervals := int(binary.LittleEndian.Uint16(rec.MainData[0:2]))
				if len(blk0.BlockData) < nintervals*4 {
					continue
				}
				type dedupInterval struct {
					baseOff uint16
					ntups   uint16
				}
				intervals := make([]dedupInterval, nintervals)
				for i := 0; i < nintervals; i++ {
					intervals[i] = dedupInterval{
						baseOff: binary.LittleEndian.Uint16(blk0.BlockData[i*4 : i*4+2]),
						ntups:   binary.LittleEndian.Uint16(blk0.BlockData[i*4+2 : i*4+4]),
					}
				}
				maxOff := btPageMaxOff(page)
				var newItems [][]byte
				var newFlags []uint16
				intIdx := 0
				o := uint16(1)
				for o <= maxOff {
					if intIdx < len(intervals) && o == intervals[intIdx].baseOff && intervals[intIdx].ntups > 1 {
						cnt := intervals[intIdx].ntups
						var group [][]byte
						for k := uint16(0); k < cnt && o+k <= maxOff; k++ {
							if it := btPageGetItem(page, o+k); it != nil {
								group = append(group, it)
							}
						}
						if formed := btFormPostingTuple(group); formed != nil {
							newItems = append(newItems, formed)
							newFlags = append(newFlags, pgLPNormal)
						}
						o += cnt
						intIdx++
					} else {
						if it := btPageGetItem(page, o); it != nil {
							cp := make([]byte, len(it))
							copy(cp, it)
							newItems = append(newItems, cp)
							newFlags = append(newFlags, btPageGetItemFlags(page, o))
						}
						o++
					}
				}
				btPageRebuildWithItems(page, newItems, newFlags)
			}

		case XLogBtreeDelete:
			// xl_btree_delete: uint32 latestRemovedXid, uint16 ndeleted, uint16 nupdated
			if blk0 := findWALBlock(rec, 0, key); blk0 != nil && len(rec.MainData) >= 8 && len(page) == PageSize {
				ndeleted := int(binary.LittleEndian.Uint16(rec.MainData[4:6]))
				nupdated := int(binary.LittleEndian.Uint16(rec.MainData[6:8]))
				data := blk0.BlockData
				if len(data) < (ndeleted+nupdated)*2 {
					continue
				}
				for i := 0; i < ndeleted; i++ {
					offnum := binary.LittleEndian.Uint16(data[i*2 : i*2+2])
					if offnum >= 1 && offnum <= btPageMaxOff(page) {
						lpOff := pgPageHeaderSize + (int(offnum)-1)*4
						rawLP := binary.LittleEndian.Uint32(page[lpOff : lpOff+4])
						rawLP = (rawLP &^ (0x03 << 15)) | (uint32(pgLpDead) << 15)
						binary.LittleEndian.PutUint32(page[lpOff:lpOff+4], rawLP)
					}
				}
				if nupdated > 0 {
					updOffsets := make([]uint16, nupdated)
					for i := 0; i < nupdated; i++ {
						updOffsets[i] = binary.LittleEndian.Uint16(data[(ndeleted+i)*2 : (ndeleted+i)*2+2])
					}
					pos := (ndeleted + nupdated) * 2
					updMap := make(map[uint16][]uint16, nupdated)
					for i := 0; i < nupdated && pos+2 <= len(data); i++ {
						nitems := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
						pos += 2
						var idxs []uint16
						for k := 0; k < nitems && pos+2 <= len(data); k++ {
							idxs = append(idxs, binary.LittleEndian.Uint16(data[pos:pos+2]))
							pos += 2
						}
						updMap[updOffsets[i]] = idxs
					}
					maxOff := btPageMaxOff(page)
					var items [][]byte
					var fls []uint16
					for o := uint16(1); o <= maxOff; o++ {
						it := btPageGetItem(page, o)
						fl := btPageGetItemFlags(page, o)
						if idxs, ok := updMap[o]; ok && it != nil {
							items = append(items, btUpdatePosting(it, idxs))
							fls = append(fls, fl)
						} else if it != nil {
							cp := make([]byte, len(it))
							copy(cp, it)
							items = append(items, cp)
							fls = append(fls, fl)
						}
					}
					btPageRebuildWithItems(page, items, fls)
				}
				flags := binary.LittleEndian.Uint16(page[special+12 : special+14])
				binary.LittleEndian.PutUint16(page[special+12:special+14], flags|pgBTPHasGarbage)
			}

		case XLogBtreeVacuum:
			// xl_btree_vacuum: uint16 ndeleted, uint16 nupdated
			if blk0 := findWALBlock(rec, 0, key); blk0 != nil && len(rec.MainData) >= 4 && len(page) == PageSize {
				ndeleted := int(binary.LittleEndian.Uint16(rec.MainData[0:2]))
				nupdated := int(binary.LittleEndian.Uint16(rec.MainData[2:4]))
				data := blk0.BlockData
				if len(data) < (ndeleted+nupdated)*2 {
					continue
				}
				delSet := make(map[uint16]bool, ndeleted)
				for i := 0; i < ndeleted; i++ {
					delSet[binary.LittleEndian.Uint16(data[i*2:i*2+2])] = true
				}
				updMap := make(map[uint16][]uint16, nupdated)
				if nupdated > 0 {
					updOffsets := make([]uint16, nupdated)
					for i := 0; i < nupdated; i++ {
						updOffsets[i] = binary.LittleEndian.Uint16(data[(ndeleted+i)*2 : (ndeleted+i)*2+2])
					}
					pos := (ndeleted + nupdated) * 2
					for i := 0; i < nupdated && pos+2 <= len(data); i++ {
						nitems := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
						pos += 2
						var idxs []uint16
						for k := 0; k < nitems && pos+2 <= len(data); k++ {
							idxs = append(idxs, binary.LittleEndian.Uint16(data[pos:pos+2]))
							pos += 2
						}
						updMap[updOffsets[i]] = idxs
					}
				}
				maxOff := btPageMaxOff(page)
				var items [][]byte
				var fls []uint16
				for o := uint16(1); o <= maxOff; o++ {
					if delSet[o] {
						continue
					}
					it := btPageGetItem(page, o)
					fl := btPageGetItemFlags(page, o)
					if idxs, ok := updMap[o]; ok && it != nil {
						items = append(items, btUpdatePosting(it, idxs))
						fls = append(fls, fl)
					} else if it != nil {
						cp := make([]byte, len(it))
						copy(cp, it)
						items = append(items, cp)
						fls = append(fls, fl)
					}
				}
				flags := binary.LittleEndian.Uint16(page[special+12 : special+14])
				binary.LittleEndian.PutUint16(page[special+12:special+14], flags&^pgBTPHasGarbage)
				btPageRebuildWithItems(page, items, fls)
			}
		}
	}
	return page
}
