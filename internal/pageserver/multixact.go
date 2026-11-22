package pageserver

import (
	"context"
	"encoding/binary"
)

// PostgreSQL MultiXact SLRU layout (pg_multixact/offsets and pg_multixact/members).
//
// SLRU pages are stored under Key{Kind: KeyKindSLRU, SpcOID: <kind>, BlockNum: <absolute page>}.
const (
	SLRUMultiXactOffsetsKind uint32 = 2
	SLRUMultiXactMembersKind uint32 = 3

	RmMultiXactID uint8 = 6 // RM_MULTIXACT_ID in PostgreSQL access/rmgrlist.h

	// xl_info opcodes for RM_MULTIXACT_ID.
	XLogMultiXactZeroOffPage uint8 = 0x00
	XLogMultiXactZeroMemPage uint8 = 0x10
	XLogMultiXactCreateID    uint8 = 0x20
	XLogMultiXactTruncateID  uint8 = 0x30

	// MultiXactOffset is a uint32, 2048 entries per 8 KiB page.
	MultiXactOffsetsPerPage uint32 = uint32(PageSize) / 4

	// Members: groups of 4 members = 4 flag bytes + 4 xids = 20 bytes; 409 groups per page.
	MultiXactMembersPerGroup = 4
	MultiXactGroupSize       = 20
	MultiXactGroupsPerPage   = PageSize / MultiXactGroupSize // 409
	MultiXactMembersPerPage  uint32 = uint32(MultiXactGroupsPerPage * MultiXactMembersPerGroup)

	SLRUPagesPerSegment = 32
)

// MultiXactOffsetPos returns (page, byte offset) of mxid's entry in pg_multixact/offsets.
func MultiXactOffsetPos(mxid uint32) (page uint32, byteOff int) {
	return mxid / MultiXactOffsetsPerPage, int(mxid%MultiXactOffsetsPerPage) * 4
}

// MultiXactMemberPos returns (page, xid byte offset, flags byte offset, flag bit shift)
// for member slot `off` in pg_multixact/members.
func MultiXactMemberPos(off uint32) (page uint32, xidOff, flagsOff int, shift uint) {
	page = off / MultiXactMembersPerPage
	entry := int(off % MultiXactMembersPerPage)
	group := entry / MultiXactMembersPerGroup
	slot := entry % MultiXactMembersPerGroup
	flagsOff = group * MultiXactGroupSize
	xidOff = flagsOff + 4 + slot*4
	shift = uint(slot * 8)
	return
}

// slruModifyLocked copies the latest version of an SLRU page (zeroed if absent), applies fn,
// and records the new version at lsn so historical LSN reads stay snapshot-correct.
// Caller must hold t.mu.
func (t *Timeline) slruModifyLocked(key Key, lsn uint64, zero bool, fn func(page []byte)) {
	page := make([]byte, PageSize)
	if !zero {
		if cur, ok := t.slruPages[key]; ok && len(cur) == PageSize {
			copy(page, cur)
		} else if deltas := t.inMem.Lookup(key, ^uint64(0)); len(deltas) > 0 {
			for i := len(deltas) - 1; i >= 0; i-- {
				if (deltas[i].Flags&EntryFlagPageImage) != 0 && len(deltas[i].PageImage) == PageSize {
					copy(page, deltas[i].PageImage)
					break
				}
			}
		}
	}
	if fn != nil {
		fn(page)
	}
	t.slruPages[key] = page
	t.inMem.Put(key, WALDelta{LSN: lsn, Flags: EntryFlagPageImage, PageImage: page})
}

// MultiXactOldest returns the oldest retained (multixact id, member offset) recorded by the
// most recent XLOG_MULTIXACT_TRUNCATE_ID, if any. Pages are never physically dropped so that
// older snapshots remain readable.
func (t *Timeline) MultiXactOldest() (mxid, off uint32, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mxOldestMulti, t.mxOldestOffset, t.mxTruncated
}

// applyMultiXactWALLocked replays RM_MULTIXACT_ID records into the SLRU page versions.
// Caller must hold t.mu.
func (t *Timeline) applyMultiXactWALLocked(rec *DecodedWALRecord) {
	op := rec.Info & 0x70
	d := rec.MainData
	switch op {
	case XLogMultiXactZeroOffPage, XLogMultiXactZeroMemPage:
		var pageno uint32
		switch {
		case len(d) >= 8:
			pageno = uint32(binary.LittleEndian.Uint64(d[0:8])) // PG17: int64
		case len(d) >= 4:
			pageno = binary.LittleEndian.Uint32(d[0:4]) // PG15: int
		default:
			return
		}
		kind := SLRUMultiXactOffsetsKind
		if op == XLogMultiXactZeroMemPage {
			kind = SLRUMultiXactMembersKind
		}
		t.slruModifyLocked(Key{Kind: KeyKindSLRU, SpcOID: kind, BlockNum: pageno}, rec.EndLSN, true, nil)

	case XLogMultiXactCreateID:
		// xl_multixact_create { MultiXactId mid; MultiXactOffset moff; int32 nmembers; {xid,status}[] }
		if len(d) < 12 {
			return
		}
		mid := binary.LittleEndian.Uint32(d[0:4])
		moff := binary.LittleEndian.Uint32(d[4:8])
		n := int(int32(binary.LittleEndian.Uint32(d[8:12])))
		if n < 0 || 12+n*8 > len(d) {
			return
		}
		next := mid + 1
		if next == 0 {
			next = 1
		}
		pg1, bo1 := MultiXactOffsetPos(mid)
		pg2, bo2 := MultiXactOffsetPos(next)
		if pg1 == pg2 {
			t.slruModifyLocked(Key{Kind: KeyKindSLRU, SpcOID: SLRUMultiXactOffsetsKind, BlockNum: pg1}, rec.EndLSN, false, func(p []byte) {
				binary.LittleEndian.PutUint32(p[bo1:bo1+4], moff)
				binary.LittleEndian.PutUint32(p[bo2:bo2+4], moff+uint32(n))
			})
		} else {
			t.slruModifyLocked(Key{Kind: KeyKindSLRU, SpcOID: SLRUMultiXactOffsetsKind, BlockNum: pg1}, rec.EndLSN, false, func(p []byte) {
				binary.LittleEndian.PutUint32(p[bo1:bo1+4], moff)
			})
			// Record the next multixact's start offset so member counts are derivable for the newest id
			// (overwritten by its own CREATE_ID later). Skip InvalidMultiXactId (0) on wraparound.
			t.slruModifyLocked(Key{Kind: KeyKindSLRU, SpcOID: SLRUMultiXactOffsetsKind, BlockNum: pg2}, rec.EndLSN, false, func(p []byte) {
				binary.LittleEndian.PutUint32(p[bo2:bo2+4], moff+uint32(n))
			})
		}
		// Group member writes by page so each SLRU page receives a single WALDelta at rec.EndLSN.
		type memberWrite struct {
			xid, status uint32
			xo, fo      int
			sh          uint
		}
		var pageOrder []uint32
		byPage := make(map[uint32][]memberWrite)
		for i := 0; i < n; i++ {
			xid := binary.LittleEndian.Uint32(d[12+i*8 : 16+i*8])
			status := binary.LittleEndian.Uint32(d[16+i*8 : 20+i*8])
			pg, xo, fo, sh := MultiXactMemberPos(moff + uint32(i))
			if _, seen := byPage[pg]; !seen {
				pageOrder = append(pageOrder, pg)
			}
			byPage[pg] = append(byPage[pg], memberWrite{xid: xid, status: status, xo: xo, fo: fo, sh: sh})
		}
		for _, pg := range pageOrder {
			writes := byPage[pg]
			t.slruModifyLocked(Key{Kind: KeyKindSLRU, SpcOID: SLRUMultiXactMembersKind, BlockNum: pg}, rec.EndLSN, false, func(p []byte) {
				for _, w := range writes {
					binary.LittleEndian.PutUint32(p[w.xo:w.xo+4], w.xid)
					fl := binary.LittleEndian.Uint32(p[w.fo : w.fo+4])
					fl = (fl &^ (uint32(0xFF) << w.sh)) | ((w.status & 0xFF) << w.sh)
					binary.LittleEndian.PutUint32(p[w.fo:w.fo+4], fl)
				}
			})
		}

	case XLogMultiXactTruncateID:
		// xl_multixact_truncate { Oid oldestMultiDB; MultiXactId startTruncOff, endTruncOff;
		//   MultiXactOffset startTruncMemb, endTruncMemb; }  (PG15/17: oldest is endTruncOff/endTruncMemb)
		if len(d) >= 20 {
			t.mxOldestMulti = binary.LittleEndian.Uint32(d[8:12])
			t.mxOldestOffset = binary.LittleEndian.Uint32(d[16:20])
			t.mxTruncated = true
		}
	}
}

// multiXactKeysTouched lists the SLRU page keys a multixact record will modify.
func multiXactKeysTouched(rec *DecodedWALRecord) []Key {
	d := rec.MainData
	var keys []Key
	switch rec.Info & 0x70 {
	case XLogMultiXactCreateID:
		if len(d) < 12 {
			return nil
		}
		mid := binary.LittleEndian.Uint32(d[0:4])
		moff := binary.LittleEndian.Uint32(d[4:8])
		n := int(int32(binary.LittleEndian.Uint32(d[8:12])))
		p1, _ := MultiXactOffsetPos(mid)
		p2, _ := MultiXactOffsetPos(mid + 1)
		keys = append(keys, Key{Kind: KeyKindSLRU, SpcOID: SLRUMultiXactOffsetsKind, BlockNum: p1},
			Key{Kind: KeyKindSLRU, SpcOID: SLRUMultiXactOffsetsKind, BlockNum: p2})
		if n > 0 && n < 1<<20 {
			first, _, _, _ := MultiXactMemberPos(moff)
			last, _, _, _ := MultiXactMemberPos(moff + uint32(n-1))
			keys = append(keys, Key{Kind: KeyKindSLRU, SpcOID: SLRUMultiXactMembersKind, BlockNum: first})
			if last != first {
				keys = append(keys, Key{Kind: KeyKindSLRU, SpcOID: SLRUMultiXactMembersKind, BlockNum: last})
			}
		}
	}
	return keys
}

// warmMultiXactSLRU loads the latest persisted version of SLRU pages about to be modified when
// the in-process cache is cold (e.g. after a pageserver restart), so WAL replay never rebuilds a
// page from zeroes and silently drops previously recorded multixact members.
// Must be called without t.mu held.
func (t *Timeline) warmMultiXactSLRU(rec *DecodedWALRecord) {
	for _, k := range multiXactKeysTouched(rec) {
		t.mu.RLock()
		_, have := t.slruPages[k]
		t.mu.RUnlock()
		if have {
			continue
		}
		pg, err := t.GetPageAtLSN(context.Background(), k, ^uint64(0))
		if err != nil || len(pg) != PageSize {
			continue
		}
		t.mu.Lock()
		if _, ok := t.slruPages[k]; !ok {
			t.slruPages[k] = append([]byte(nil), pg...)
		}
		t.mu.Unlock()
	}
}
