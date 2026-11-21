package readadapter

import (
	"errors"
	"encoding/binary"
	"testing"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

func TestMultiXactStatusIsUpdate(t *testing.T) {
	for st, want := range map[MultiXactStatus]bool{
		MultiXactForKeyShare: false, MultiXactForShare: false, MultiXactForNoKeyUpd: false,
		MultiXactForUpdate: false, MultiXactNoKeyUpdate: true, MultiXactUpdate: true,
	} {
		if st.IsUpdate() != want {
			t.Errorf("status %d IsUpdate=%v want %v", st, st.IsUpdate(), want)
		}
	}
	if nextMultiXactID(0xFFFFFFFF) != 1 || nextMultiXactID(7) != 8 {
		t.Fatal("multixact id wraparound must skip InvalidMultiXactId")
	}
}

// buildMultiXactWAL synthesizes an XLOG_MULTIXACT_CREATE_ID record body.
func buildMultiXactCreateMain(mid, moff uint32, members []MultiXactMember) []byte {
	b := make([]byte, 12+8*len(members))
	binary.LittleEndian.PutUint32(b[0:], mid)
	binary.LittleEndian.PutUint32(b[4:], moff)
	binary.LittleEndian.PutUint32(b[8:], uint32(len(members)))
	for i, m := range members {
		binary.LittleEndian.PutUint32(b[12+i*8:], m.Xid)
		binary.LittleEndian.PutUint32(b[16+i*8:], uint32(m.Status))
	}
	return b
}

// TestPhase14C_Unit_SLRUReplayAndResolve replays synthetic RM_MULTIXACT_ID records through the
// Timeline (WAL recovery path), including snapshot-LSN history, member page spill, wraparound
// of MultiXactOffset and missing/invalid data handling.
func TestPhase14C_Unit_SLRUReplayAndResolve(t *testing.T) {
	ps, err := pageserver.New(pageserver.Config{SocketPath: t.TempDir() + "/ps.sock", LayerDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	const tenant, gen = "t_mx_unit", "g1"
	tl := ps.EnsureTenant(tenant, 1)
	ps.SetTenantGeneration(tenant, gen)

	apply := func(info uint8, lsn uint64, main []byte) {
		tl.ApplyWALRecord(&pageserver.DecodedWALRecord{Rmid: pageserver.RmMultiXactID, Info: info, EndLSN: lsn, MainData: main})
	}
	// mxid 5: key-share(100), update(101) at offset 1.
	apply(pageserver.XLogMultiXactCreateID, 0x100, buildMultiXactCreateMain(5, 100, []MultiXactMember{{100, MultiXactForKeyShare}, {101, MultiXactNoKeyUpdate}}))
	// mxid 6: all lock-only (200 share, 201 for-update) at offset 3.
	apply(pageserver.XLogMultiXactCreateID, 0x200, buildMultiXactCreateMain(6, 102, []MultiXactMember{{200, MultiXactForShare}, {201, MultiXactForUpdate}}))
	// mxid 7: delete member (status Update) and spans offsets 5..6.
	apply(pageserver.XLogMultiXactCreateID, 0x300, buildMultiXactCreateMain(7, 104, []MultiXactMember{{300, MultiXactForKeyShare}, {301, MultiXactUpdate}}))
	// Member offset near the page boundary and wraparound of MultiXactOffset (uint32).
	nearEnd := uint32(0xFFFFFFFF - 1)
	apply(pageserver.XLogMultiXactCreateID, 0x400, buildMultiXactCreateMain(2047, nearEnd, []MultiXactMember{{400, MultiXactForShare}, {401, MultiXactForKeyShare}, {402, MultiXactUpdate}}))
	// Multixact id spanning offsets-page boundary (2047 -> 2048).
	apply(pageserver.XLogMultiXactCreateID, 0x500, buildMultiXactCreateMain(2048, nearEnd+3, []MultiXactMember{{500, MultiXactForNoKeyUpd}}))

	be := ps
	r := NewCLOGResolver(be)
	ctx := t.Context()

	got, err := r.ResolveMultiXact(ctx, tenant, gen, 0x1000, 5)
	if err != nil || len(got) != 2 || got[0] != (MultiXactMember{100, MultiXactForKeyShare}) || got[1] != (MultiXactMember{101, MultiXactNoKeyUpdate}) {
		t.Fatalf("mxid5: %v %v", got, err)
	}
	if x, ok, err := r.MultiXactUpdateXid(ctx, tenant, gen, 0x1000, 5); err != nil || !ok || x != 101 {
		t.Fatalf("update xid mxid5: %d %v %v", x, ok, err)
	}
	if _, ok, err := r.MultiXactUpdateXid(ctx, tenant, gen, 0x1000, 6); err != nil || ok {
		t.Fatalf("mxid6 must be lock-only: ok=%v err=%v", ok, err)
	}
	if x, ok, err := r.MultiXactUpdateXid(ctx, tenant, gen, 0x1000, 7); err != nil || !ok || x != 301 {
		t.Fatalf("delete member mxid7: %d %v %v", x, ok, err)
	}
	got, err = r.ResolveMultiXact(ctx, tenant, gen, 0x1000, 2047)
	if err != nil || len(got) != 3 || got[2].Xid != 402 || got[2].Status != MultiXactUpdate {
		t.Fatalf("wraparound mxid2047: %v %v", got, err)
	}
	if got, err = r.ResolveMultiXact(ctx, tenant, gen, 0x1000, 2048); err == nil {
		// mxid 2049's offset was derived from the CREATE of 2048 (start+n); members are unreadable → count>0 required.
		if len(got) != 1 || got[0].Xid != 500 {
			// 2049 has no recorded next offset beyond derived value, derived value makes n=1.
			t.Fatalf("mxid2048: %v", got)
		}
	}

	// Snapshot-LSN history: before LSN 0x200, mxid 6 must be unavailable (not yet created).
	if _, err := r.ResolveMultiXact(ctx, tenant, gen, 0x150, 6); err == nil {
		t.Fatal("mxid 6 must not resolve at an LSN before its creation")
	}
	// Unknown multixact => safe-fallback error.
	if _, err := r.ResolveMultiXact(ctx, tenant, gen, 0x1000, 4000); err == nil {
		t.Fatal("unknown mxid must fail")
	} else if !isMultiXactFallback(err) {
		t.Fatalf("unknown mxid must map to ErrMultiXactUnsupported, got %v", err)
	}
	if _, err := r.ResolveMultiXact(ctx, tenant, gen, 0x1000, 0); err == nil {
		t.Fatal("mxid 0 invalid")
	}

	// TRUNCATE_ID is tracked but history stays readable for older snapshots.
	trunc := make([]byte, 20)
	binary.LittleEndian.PutUint32(trunc[8:], 6)
	binary.LittleEndian.PutUint32(trunc[16:], 102)
	apply(pageserver.XLogMultiXactTruncateID, 0x600, trunc)
	if m, o, ok := tl.MultiXactOldest(); !ok || m != 6 || o != 102 {
		t.Fatalf("oldest multixact %d/%d/%v", m, o, ok)
	}
	if _, err := r.ResolveMultiXact(ctx, tenant, gen, 0x1000, 5); err != nil {
		t.Fatalf("old snapshot must still read truncated multixact pages: %v", err)
	}

	// ZERO_*_PAGE wipes the page at that LSN only.
	zero := make([]byte, 8)
	apply(pageserver.XLogMultiXactZeroOffPage, 0x700, zero)
	if _, err := r.ResolveMultiXact(ctx, tenant, gen, 0x800, 5); err == nil {
		t.Fatal("zeroed offsets page must not resolve")
	}
	if _, err := r.ResolveMultiXact(ctx, tenant, gen, 0x1000-1, 5); err == nil {
		// 0x7ff >= 0x700 so zeroed; this is just sanity that reads at/after the zeroing LSN fail.
		t.Fatal("zeroed offsets page must not resolve after zero LSN")
	}
	if _, err := r.ResolveMultiXact(ctx, tenant, gen, 0x650, 5); err != nil {
		t.Fatalf("pre-zero snapshot must still resolve: %v", err)
	}
}

func isMultiXactFallback(err error) bool { return err != nil && errors.Is(err, ErrMultiXactUnsupported) }

// TestPhase14C_LivePG15_MultiXact exercises real PostgreSQL multixacts: shared lockers, update and
// delete multixacts, in-progress vs committed vs aborted updaters, snapshot reads and zero fallbacks.
