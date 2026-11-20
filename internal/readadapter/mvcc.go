package readadapter

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

// XactStatus represents the 2-bit transaction commit status in PostgreSQL pg_xact (CLOG).
type XactStatus uint8

const (
	XactStatusInProgress   XactStatus = 0x00
	XactStatusCommitted    XactStatus = 0x01
	XactStatusAborted      XactStatus = 0x02
	XactStatusSubCommitted XactStatus = 0x03

	clogXactsPerByte    = 4
	clogXactsPerPage    = PGPageSize * clogXactsPerByte // 32768
	subtransXactsPerPg  = PGPageSize / 4                // 2048
	maxSubtransDepth    = 64

	// SLRU segment kinds encoded in Key.SpcOID for pageserver.KeyKindSLRU.
	SLRUXactKind     uint32 = 0
	SLRUSubtransKind uint32 = 1
)

// SLRUKey constructs a Pageserver Key for a pg_xact or pg_subtrans SLRU page.
func SLRUKey(slruKind uint32, pageNum uint32) pageserver.Key {
	return pageserver.Key{
		Kind:     pageserver.KeyKindSLRU,
		SpcOID:   slruKind,
		BlockNum: pageNum,
	}
}

// DecodeCLOGStatus extracts the 2-bit XactStatus for xid from an 8 KiB pg_xact page.
func DecodeCLOGStatus(page []byte, xid uint32) (XactStatus, error) {
	if len(page) != PGPageSize {
		return XactStatusInProgress, fmt.Errorf("%w: pg_xact page length %d != %d", ErrInvalidPage, len(page), PGPageSize)
	}
	pageIdx := xid % clogXactsPerPage
	byteIdx := pageIdx / clogXactsPerByte
	bitShift := (pageIdx % clogXactsPerByte) * 2
	return XactStatus((page[byteIdx] >> bitShift) & 0x03), nil
}

// SetCLOGStatus writes the 2-bit XactStatus for xid into an 8 KiB pg_xact page.
func SetCLOGStatus(page []byte, xid uint32, status XactStatus) error {
	if len(page) != PGPageSize {
		return fmt.Errorf("%w: pg_xact page length %d != %d", ErrInvalidPage, len(page), PGPageSize)
	}
	pageIdx := xid % clogXactsPerPage
	byteIdx := pageIdx / clogXactsPerByte
	bitShift := (pageIdx % clogXactsPerByte) * 2
	mask := byte(0x03 << bitShift)
	page[byteIdx] = (page[byteIdx] &^ mask) | (byte(status&0x03) << bitShift)
	return nil
}

// DecodeSubtransParent extracts the parent TransactionId for xid from an 8 KiB pg_subtrans page.
func DecodeSubtransParent(page []byte, xid uint32) (uint32, error) {
	if len(page) != PGPageSize {
		return 0, fmt.Errorf("%w: pg_subtrans page length %d != %d", ErrInvalidPage, len(page), PGPageSize)
	}
	off := (xid % subtransXactsPerPg) * 4
	return binary.LittleEndian.Uint32(page[off : off+4]), nil
}

// SetSubtransParent writes the parent TransactionId for xid into an 8 KiB pg_subtrans page.
func SetSubtransParent(page []byte, xid, parentXid uint32) error {
	if len(page) != PGPageSize {
		return fmt.Errorf("%w: pg_subtrans page length %d != %d", ErrInvalidPage, len(page), PGPageSize)
	}
	off := (xid % subtransXactsPerPg) * 4
	binary.LittleEndian.PutUint32(page[off:off+4], parentXid)
	return nil
}

type slruCacheKey struct {
	tenantID string
	lsn      uint64
	slruKind uint32
	pageNum  uint32
}

// CLOGResolver resolves authoritative transaction commit statuses from pg_xact
// and pg_subtrans SLRU pages fetched from StorageBackend at the pinned snapshot LSN.
type CLOGResolver struct {
	backend StorageBackend

	mu    sync.RWMutex
	cache map[slruCacheKey][]byte
}

// NewCLOGResolver creates a CLOGResolver backed by backend.
func NewCLOGResolver(backend StorageBackend) *CLOGResolver {
	return &CLOGResolver{
		backend: backend,
		cache:   make(map[slruCacheKey][]byte),
	}
}

func (r *CLOGResolver) fetchSLRUPage(ctx context.Context, tenantID string, lsn, slruKind, pageNum uint32) ([]byte, error) {
	return r.fetchSLRUPageLSN(ctx, tenantID, uint64(lsn), slruKind, pageNum)
}

func (r *CLOGResolver) fetchSLRUPageLSN(ctx context.Context, tenantID string, lsn uint64, slruKind, pageNum uint32) ([]byte, error) {
	ck := slruCacheKey{
		tenantID: tenantID,
		lsn:      lsn,
		slruKind: slruKind,
		pageNum:  pageNum,
	}

	r.mu.RLock()
	if pg, ok := r.cache[ck]; ok {
		r.mu.RUnlock()
		return pg, nil
	}
	r.mu.RUnlock()

	if r.backend == nil {
		return nil, fmt.Errorf("readadapter: CLOGResolver has nil StorageBackend")
	}

	pg, err := r.backend.GetPageAtLSN(ctx, tenantID, SLRUKey(slruKind, pageNum), lsn)
	if err != nil {
		return nil, err
	}
	if len(pg) != PGPageSize {
		return nil, fmt.Errorf("%w: SLRU page length %d != %d", ErrInvalidPage, len(pg), PGPageSize)
	}

	r.mu.Lock()
	// Bound cache size simply to prevent unbounded growth across many LSNs.
	if len(r.cache) >= 4096 {
		r.cache = make(map[slruCacheKey][]byte)
	}
	r.cache[ck] = pg
	r.mu.Unlock()
	return pg, nil
}

// XidStatus returns the authoritative commit status of xid at snapshot lsn,
// following pg_subtrans parent links when pg_xact records XactStatusSubCommitted.
func (r *CLOGResolver) XidStatus(ctx context.Context, tenantID string, lsn uint64, xid uint32) (XactStatus, error) {
	cur := xid
	for depth := 0; depth < maxSubtransDepth; depth++ {
		if cur == InvalidTransactionID {
			return XactStatusAborted, nil
		}
		if cur == BootstrapTransactionID || cur == FrozenTransactionID {
			return XactStatusCommitted, nil
		}

		pageNum := cur / clogXactsPerPage
		pg, err := r.fetchSLRUPageLSN(ctx, tenantID, lsn, SLRUXactKind, pageNum)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return XactStatusInProgress, nil
			}
			return XactStatusInProgress, err
		}
		st, err := DecodeCLOGStatus(pg, cur)
		if err != nil {
			return XactStatusInProgress, err
		}
		if st != XactStatusSubCommitted {
			return st, nil
		}

		subPageNum := cur / subtransXactsPerPg
		subPg, err := r.fetchSLRUPageLSN(ctx, tenantID, lsn, SLRUSubtransKind, subPageNum)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return XactStatusInProgress, nil
			}
			return XactStatusInProgress, err
		}
		parent, err := DecodeSubtransParent(subPg, cur)
		if err != nil {
			return XactStatusInProgress, err
		}
		if parent == InvalidTransactionID || parent == cur {
			return XactStatusInProgress, nil
		}
		cur = parent
	}
	return XactStatusInProgress, fmt.Errorf("readadapter: pg_subtrans parent chain exceeded %d levels for xid %d", maxSubtransDepth, xid)
}

func xidInSnapshot(xid uint32, snap *ReadSnapshot) bool {
	if snap == nil {
		return false
	}
	if xid < snap.Xmin {
		return false
	}
	if xid >= snap.Xmax {
		return true
	}
	for _, active := range snap.ActiveXIPs {
		if active == xid {
			return true
		}
	}
	return false
}

func xidCommittedAtSnapshot(ctx context.Context, tenantID string, xid uint32, snap *ReadSnapshot, hintCommitted bool, resolver *CLOGResolver) (bool, error) {
	if xid == InvalidTransactionID {
		return false, nil
	}
	if xid == BootstrapTransactionID || xid == FrozenTransactionID {
		return true, nil
	}
	if xidInSnapshot(xid, snap) {
		return false, nil
	}
	if hintCommitted {
		return true, nil
	}
	if resolver == nil {
		return false, nil
	}
	st, err := resolver.XidStatus(ctx, tenantID, snap.LSN, xid)
	if err != nil {
		return false, err
	}
	return st == XactStatusCommitted, nil
}

// IsVisibleWithCLOG evaluates PostgreSQL 17 MVCC HeapTupleSatisfiesMVCC visibility
// for a tuple header under snap, consulting pg_xact/pg_subtrans at snap.LSN
// whenever hint bits are not yet set.
func IsVisibleWithCLOG(ctx context.Context, h HeapTupleHeader, snap *ReadSnapshot, resolver *CLOGResolver) (bool, error) {
	if snap == nil || snap.released.Load() {
		return false, ErrInvalidSnapshot
	}

	xminFrozen := (h.Infomask & HeapXminFrozen) == HeapXminFrozen
	if !xminFrozen {
		if (h.Infomask & HeapXminInvalid) != 0 {
			return false, nil
		}
		ok, err := xidCommittedAtSnapshot(ctx, snap.TenantID, h.Xmin, snap, (h.Infomask&HeapXminCommitted) != 0, resolver)
		if err != nil || !ok {
			return false, err
		}
	}

	if (h.Infomask&HeapXmaxInvalid) != 0 || (h.Infomask&HeapXmaxLockOnly) != 0 || h.Xmax == InvalidTransactionID {
		return true, nil
	}

	if (h.Infomask & HeapXmaxIsMulti) != 0 {
		// MultiXact without HeapXmaxLockOnly: conservative visibility unless hint committed.
		return (h.Infomask & HeapXmaxCommitted) == 0, nil
	}

	deleted, err := xidCommittedAtSnapshot(ctx, snap.TenantID, h.Xmax, snap, (h.Infomask&HeapXmaxCommitted) != 0, resolver)
	if err != nil {
		return false, err
	}
	return !deleted, nil
}

// SnapshotCoordinator publishes authoritative Write Engine commit snapshots and
// allows read queries to wait for causal LSN visibility (read-your-own-write).
type SnapshotCoordinator struct {
	mu        sync.Mutex
	cond      *sync.Cond
	snapshots map[string]publishedSnapshot
}

// NewSnapshotCoordinator creates a SnapshotCoordinator.
func NewSnapshotCoordinator() *SnapshotCoordinator {
	sc := &SnapshotCoordinator{
		snapshots: make(map[string]publishedSnapshot),
	}
	sc.cond = sync.NewCond(&sc.mu)
	return sc
}

// Publish records a new committed snapshot for (tenantID, generation) and wakes waiters.
func (sc *SnapshotCoordinator) Publish(tenantID, generation string, info SnapshotInfo) {
	sc.mu.Lock()
	cur, ok := sc.snapshots[tenantID]
	if !ok || cur.generation != generation || info.LSN >= cur.info.LSN {
		sc.snapshots[tenantID] = publishedSnapshot{
			generation: generation,
			info:       info,
		}
	}
	sc.cond.Broadcast()
	sc.mu.Unlock()
}

// CurrentSnapshot implements SnapshotProvider.
func (sc *SnapshotCoordinator) CurrentSnapshot(_ context.Context, tenantID, generation string) (SnapshotInfo, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	pub, ok := sc.snapshots[tenantID]
	if !ok {
		return SnapshotInfo{}, fmt.Errorf("no snapshot published for tenant %q", tenantID)
	}
	if generation != "" && pub.generation != "" && pub.generation != generation {
		return SnapshotInfo{}, fmt.Errorf("%w: tenant %q got %q want %q", ErrStaleGeneration, tenantID, pub.generation, generation)
	}
	return pub.info, nil
}

// AwaitLSN blocks until the published snapshot for (tenantID, generation) reaches
// at least minLSN, or until ctx is canceled/expires.
func (sc *SnapshotCoordinator) AwaitLSN(ctx context.Context, tenantID, generation string, minLSN uint64) (SnapshotInfo, error) {
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			sc.mu.Lock()
			sc.cond.Broadcast()
			sc.mu.Unlock()
		case <-done:
		}
	}()

	sc.mu.Lock()
	defer sc.mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return SnapshotInfo{}, err
		}
		if pub, ok := sc.snapshots[tenantID]; ok {
			if generation == "" || pub.generation == "" || pub.generation == generation {
				if pub.info.LSN >= minLSN {
					return pub.info, nil
				}
			}
		}
		sc.cond.Wait()
	}
}
