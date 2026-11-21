package readadapter

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

// MultiXactStatus mirrors PostgreSQL's MultiXactStatus enum stored in pg_multixact/members.
type MultiXactStatus uint8

const (
	MultiXactForKeyShare   MultiXactStatus = 0
	MultiXactForShare      MultiXactStatus = 1
	MultiXactForNoKeyUpd   MultiXactStatus = 2
	MultiXactForUpdate     MultiXactStatus = 3
	MultiXactNoKeyUpdate   MultiXactStatus = 4 // actual (non-key) update
	MultiXactUpdate        MultiXactStatus = 5 // actual update/delete

	// SLRU kinds encoded in Key.SpcOID (must match the pageserver).
	SLRUMultiXactOffsetsKind uint32 = 2
	SLRUMultiXactMembersKind uint32 = 3

	multiXactOffsetsPerPage = PGPageSize / 4
	multiXactGroupSize      = 20
	multiXactMembersPerPage = (PGPageSize / multiXactGroupSize) * 4 // 1636

	// maxMultiXactMembers bounds a single multixact to defend against corrupt offsets.
	maxMultiXactMembers = 1 << 20
)

// IsUpdate reports whether the member status denotes a row-modifying (update/delete) transaction,
// as opposed to a lock-only member. Equivalent to PostgreSQL ISUPDATE_from_mxstatus().
func (s MultiXactStatus) IsUpdate() bool { return s > MultiXactForUpdate }

// MultiXactMember is one (xid, status) member of a MultiXactId.
type MultiXactMember struct {
	Xid    uint32
	Status MultiXactStatus
}

// ErrMultiXactNotFound indicates the multixact offsets/members SLRU data needed is unavailable
// at the requested LSN (e.g. truncated, not ingested). Callers must fall back to the primary.
var ErrMultiXactNotFound = fmt.Errorf("%w: multixact SLRU data unavailable", ErrMultiXactUnsupported)

// nextMultiXactID returns the multixact id following id, skipping InvalidMultiXactId (0) on wraparound.
func nextMultiXactID(id uint32) uint32 {
	n := id + 1
	if n == 0 {
		n = 1
	}
	return n
}

// ResolveMultiXact returns the members of mxid as visible at lsn for (tenant, generation).
func (r *CLOGResolver) ResolveMultiXact(ctx context.Context, tenantID, generation string, lsn uint64, mxid uint32) ([]MultiXactMember, error) {
	if mxid == 0 {
		return nil, fmt.Errorf("%w: invalid multixact id 0", ErrMultiXactNotFound)
	}
	readOffset := func(id uint32) (uint32, error) {
		pg, err := r.fetchSLRUPageLSN(ctx, tenantID, generation, lsn, SLRUMultiXactOffsetsKind, id/multiXactOffsetsPerPage)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return 0, fmt.Errorf("%w: offsets page for mxid %d: %v", ErrMultiXactNotFound, id, err)
			}
			return 0, err
		}
		bo := int(id%multiXactOffsetsPerPage) * 4
		return binary.LittleEndian.Uint32(pg[bo : bo+4]), nil
	}
	start, err := readOffset(mxid)
	if err != nil {
		return nil, err
	}
	end, err := readOffset(nextMultiXactID(mxid))
	if err != nil {
		return nil, err
	}
	if start == 0 || end == 0 {
		return nil, fmt.Errorf("%w: mxid %d has no recorded offset (start=%d end=%d)", ErrMultiXactNotFound, mxid, start, end)
	}
	n := end - start // uint32 wraparound-safe
	if n == 0 || n > maxMultiXactMembers {
		return nil, fmt.Errorf("%w: mxid %d invalid member count %d", ErrMultiXactNotFound, mxid, n)
	}

	members := make([]MultiXactMember, 0, n)
	var curPage uint32 = ^uint32(0)
	var pg []byte
	for i := uint32(0); i < n; i++ {
		off := start + i // wraps like MultiXactOffset
		page := off / multiXactMembersPerPage
		if page != curPage {
			pg, err = r.fetchSLRUPageLSN(ctx, tenantID, generation, lsn, SLRUMultiXactMembersKind, page)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					return nil, fmt.Errorf("%w: members page %d for mxid %d: %v", ErrMultiXactNotFound, page, mxid, err)
				}
				return nil, err
			}
			curPage = page
		}
		entry := int(off % multiXactMembersPerPage)
		group, slot := entry/4, entry%4
		flagsOff := group * multiXactGroupSize
		xidOff := flagsOff + 4 + slot*4
		xid := binary.LittleEndian.Uint32(pg[xidOff : xidOff+4])
		fl := binary.LittleEndian.Uint32(pg[flagsOff : flagsOff+4])
		st := MultiXactStatus((fl >> uint(slot*8)) & 0xFF)
		if st > MultiXactUpdate || xid == InvalidTransactionID {
			return nil, fmt.Errorf("%w: mxid %d member %d invalid (xid=%d status=%d)", ErrMultiXactNotFound, mxid, i, xid, st)
		}
		members = append(members, MultiXactMember{Xid: xid, Status: st})
	}
	return members, nil
}

// MultiXactUpdateXid returns the single updating/deleting member of mxid, or ok=false when all
// members are lock-only (HeapTupleGetUpdateXid equivalent).
func (r *CLOGResolver) MultiXactUpdateXid(ctx context.Context, tenantID, generation string, lsn uint64, mxid uint32) (xid uint32, ok bool, err error) {
	members, err := r.ResolveMultiXact(ctx, tenantID, generation, lsn, mxid)
	if err != nil {
		return 0, false, err
	}
	for _, m := range members {
		if m.Status.IsUpdate() {
			return m.Xid, true, nil
		}
	}
	return 0, false, nil
}
