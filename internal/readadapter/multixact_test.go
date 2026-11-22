package readadapter

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
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

// TestPhase14C_RestartRecovery verifies that MultiXact SLRU pages flushed to L0 layers survive a
// pageserver restart and that replaying additional MultiXact WAL after restart warms from layers
// without losing earlier members.
func TestPhase14C_RestartRecovery(t *testing.T) {
	layerDir := t.TempDir()
	const tenant, gen = "t_mx_restart", "g1"
	ctx := t.Context()

	// 1. First pageserver: replay mxid 5 & 6, flush to L0 layers, close.
	ps1, err := pageserver.New(pageserver.Config{SocketPath: t.TempDir() + "/ps1.sock", LayerDir: layerDir})
	if err != nil {
		t.Fatal(err)
	}
	ps1.SetTenantGeneration(tenant, gen)
	tl1 := ps1.EnsureTenant(tenant, 1)
	tl1.ApplyWALRecord(&pageserver.DecodedWALRecord{
		Rmid: pageserver.RmMultiXactID, Info: pageserver.XLogMultiXactCreateID, EndLSN: 0x100,
		MainData: buildMultiXactCreateMain(5, 100, []MultiXactMember{{100, MultiXactForKeyShare}, {101, MultiXactNoKeyUpdate}}),
	})
	tl1.ApplyWALRecord(&pageserver.DecodedWALRecord{
		Rmid: pageserver.RmMultiXactID, Info: pageserver.XLogMultiXactCreateID, EndLSN: 0x200,
		MainData: buildMultiXactCreateMain(6, 102, []MultiXactMember{{200, MultiXactForShare}, {201, MultiXactForUpdate}}),
	})
	if err := ps1.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	_ = ps1.Close()

	// 2. Restart pageserver on the same LayerDir, load manifest layers, replay mxid 7.
	ps2, err := pageserver.New(pageserver.Config{SocketPath: t.TempDir() + "/ps2.sock", LayerDir: layerDir})
	if err != nil {
		t.Fatal(err)
	}
	defer ps2.Close()
	ps2.SetTenantGeneration(tenant, gen)
	tl2 := ps2.EnsureTenant(tenant, 1)
	manifest, err := ps2.Store().LoadLayerManifestGen(ctx, tenant, gen, 1)
	if err != nil || manifest == nil || len(manifest.Layers) == 0 {
		t.Fatalf("LoadLayerManifestGen: manifest=%v err=%v", manifest, err)
	}
	for _, m := range manifest.Layers {
		tl2.AddLayer(m)
	}
	// Replay new WAL after restart on the same SLRU page (offsets page 0).
	tl2.ApplyWALRecord(&pageserver.DecodedWALRecord{
		Rmid: pageserver.RmMultiXactID, Info: pageserver.XLogMultiXactCreateID, EndLSN: 0x300,
		MainData: buildMultiXactCreateMain(7, 104, []MultiXactMember{{300, MultiXactForKeyShare}, {301, MultiXactUpdate}}),
	})

	r := NewCLOGResolver(ps2)
	// Pre-restart mxid 5 must still resolve (warmed from flushed layer, not wiped by mxid 7).
	if x, ok, err := r.MultiXactUpdateXid(ctx, tenant, gen, 0x1000, 5); err != nil || !ok || x != 101 {
		t.Fatalf("pre-restart mxid 5: x=%d ok=%v err=%v", x, ok, err)
	}
	// Pre-restart lock-only mxid 6 must still be lock-only.
	if _, ok, err := r.MultiXactUpdateXid(ctx, tenant, gen, 0x1000, 6); err != nil || ok {
		t.Fatalf("pre-restart mxid 6: ok=%v err=%v", ok, err)
	}
	// Post-restart mxid 7 must resolve.
	if x, ok, err := r.MultiXactUpdateXid(ctx, tenant, gen, 0x1000, 7); err != nil || !ok || x != 301 {
		t.Fatalf("post-restart mxid 7: x=%d ok=%v err=%v", x, ok, err)
	}
	// Historical LSN before mxid 6 (0x150) must still not resolve mxid 6.
	if _, err := r.ResolveMultiXact(ctx, tenant, gen, 0x150, 6); err == nil {
		t.Fatal("mxid 6 must not resolve at pre-creation LSN 0x150 after restart")
	}
}

// TestPhase14C_PG17_MultiXactLive exercises real PostgreSQL 17 multixacts (lock-only,
// committed key-share+update, aborted key-share+update, in-progress update visibility)
// and compares AIR's output against PostgreSQL row-for-row with zero MultiXactFallbacks.
func TestPhase14C_PG17_MultiXactLive(t *testing.T) {
	initdbBin := "/usr/lib/postgresql/17/bin/initdb"
	pgBin := "/usr/lib/postgresql/17/bin/postgres"
	if _, err := os.Stat(initdbBin); err != nil {
		t.Skip("PostgreSQL 17 binaries not installed")
	}

	tmpDir := t.TempDir()
	pgData := filepath.Join(tmpDir, "pgdata")
	sockDir := filepath.Join(tmpDir, "pgsock")
	_ = os.MkdirAll(sockDir, 0o755)

	if out, err := exec.Command(initdbBin, "-D", pgData, "-U", "postgres", "-A", "trust", "--no-sync").CombinedOutput(); err != nil {
		t.Fatalf("initdb failed: %v\n%s", err, out)
	}
	f, _ := os.OpenFile(filepath.Join(pgData, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = f.WriteString("\nwal_level = replica\nmax_wal_senders = 4\n")
	_ = f.Close()
	hf, _ := os.OpenFile(filepath.Join(pgData, "pg_hba.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = hf.WriteString("\nlocal replication postgres trust\n")
	_ = hf.Close()

	cmd := exec.Command(pgBin, "-D", pgData, "-k", sockDir, "-h", "", "-p", "55445", "-F")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("host=%s port=55445 user=postgres dbname=postgres sslmode=disable", sockDir)
	var c1 *pgx.Conn
	for i := 0; i < 40; i++ {
		var err error
		c1, err = pgx.Connect(ctx, dsn)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if c1 == nil {
		t.Fatal("could not connect to PG17")
	}
	defer c1.Close(ctx)
	c2, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("c2: %v", err)
	}
	defer c2.Close(ctx)

	if _, err := c1.Exec(ctx, `
		CREATE TABLE mx_live (id int4 PRIMARY KEY, v int4);
		INSERT INTO mx_live VALUES (1, 10), (2, 20), (3, 30), (4, 40);
	`); err != nil {
		t.Fatalf("setup mx_live: %v", err)
	}
	if _, err := c1.Exec(ctx, "CHECKPOINT"); err != nil {
		t.Fatalf("CHECKPOINT: %v", err)
	}
	var lsnStr string
	_ = c1.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsnStr)
	var hi, lo uint32
	_, _ = fmt.Sscanf(lsnStr, "%X/%X", &hi, &lo)
	initLSN := (uint64(hi) << 32) | uint64(lo)

	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(tmpDir, "ps.sock"),
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()
	if err := ps.Start(ctx); err != nil {
		t.Fatalf("ps.Start: %v", err)
	}

	const tenantID = "tenant_mx_pg17"
	if _, err := ps.BootstrapTenantFromPGData(ctx, tenantID, 1, pgData, initLSN); err != nil {
		t.Fatalf("BootstrapTenantFromPGData: %v", err)
	}
	tl := ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, "gen-1")

	rxCtx, rxCancel := context.WithCancel(ctx)
	defer rxCancel()
	rx := pageserver.NewWALReceiver(dsn+" replication=database", tl, tl.LastLSN())
	go func() {
		if rerr := rx.Run(rxCtx); rerr != nil && rxCtx.Err() == nil {
			t.Logf("walreceiver exited: %v", rerr)
		}
	}()

	// Row 1: lock-only MultiXact (two concurrent FOR SHARE transactions).
	tx1, _ := c1.Begin(ctx)
	tx2, _ := c2.Begin(ctx)
	_, _ = tx1.Exec(ctx, "SELECT * FROM mx_live WHERE id = 1 FOR SHARE")
	_, _ = tx2.Exec(ctx, "SELECT * FROM mx_live WHERE id = 1 FOR SHARE")
	_ = tx1.Commit(ctx)
	_ = tx2.Commit(ctx)

	// Row 2: non-lock-only MultiXact (FOR KEY SHARE + committed UPDATE on non-key col v).
	tx1, _ = c1.Begin(ctx)
	tx2, _ = c2.Begin(ctx)
	_, _ = tx1.Exec(ctx, "SELECT * FROM mx_live WHERE id = 2 FOR KEY SHARE")
	if _, err := tx2.Exec(ctx, "UPDATE mx_live SET v = 200 WHERE id = 2"); err != nil {
		t.Fatalf("update row 2: %v", err)
	}
	_ = tx2.Commit(ctx)
	_ = tx1.Commit(ctx)

	// Row 3: FOR KEY SHARE + aborted UPDATE on non-key col v.
	tx1, _ = c1.Begin(ctx)
	tx2, _ = c2.Begin(ctx)
	_, _ = tx1.Exec(ctx, "SELECT * FROM mx_live WHERE id = 3 FOR KEY SHARE")
	if _, err := tx2.Exec(ctx, "UPDATE mx_live SET v = 300 WHERE id = 3"); err != nil {
		t.Fatalf("update row 3: %v", err)
	}
	_ = tx2.Rollback(ctx)
	_ = tx1.Commit(ctx)

	// Row 4: FOR KEY SHARE + in-progress UPDATE (still open while AIR reads).
	tx1, _ = c1.Begin(ctx)
	tx2, _ = c2.Begin(ctx)
	_, _ = tx1.Exec(ctx, "SELECT * FROM mx_live WHERE id = 4 FOR KEY SHARE")
	if _, err := tx2.Exec(ctx, "UPDATE mx_live SET v = 400 WHERE id = 4"); err != nil {
		t.Fatalf("update row 4: %v", err)
	}
	_ = tx1.Commit(ctx)

	// Wait for WALReceiver to catch up to current WAL LSN.
	_ = c1.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsnStr)
	_, _ = fmt.Sscanf(lsnStr, "%X/%X", &hi, &lo)
	targetLSN := (uint64(hi) << 32) | uint64(lo)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && tl.LastLSN() < targetLSN-8 {
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("WAL catchup: tl.LastLSN=%#x targetLSN=%#x", tl.LastLSN(), targetLSN)

	rs, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost:       "127.0.0.1",
		PGPort:           0,
		DataDir:          filepath.Join(tmpDir, "re"),
		PageserverSocket: filepath.Join(tmpDir, "ps.sock"),
		CacheDir:         filepath.Join(tmpDir, "l2"),
		CacheKB:          8192,
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := rs.Start(); err != nil {
		t.Fatalf("rs.Start: %v", err)
	}
	defer rs.Stop()

	ts, err := rs.getOrCreateTenant(tenantID)
	if err != nil {
		t.Fatalf("getOrCreateTenant: %v", err)
	}

	checkMatchesPG := func(label string) {
		t.Helper()
		pgRows, err := c1.Query(ctx, "SELECT id, v FROM mx_live ORDER BY id")
		if err != nil {
			t.Fatalf("%s PG query: %v", label, err)
		}
		var want [][2]int32
		for pgRows.Next() {
			var id, v int32
			_ = pgRows.Scan(&id, &v)
			want = append(want, [2]int32{id, v})
		}
		pgRows.Close()

		airRes, err := rs.executeReaderQuery(ctx, ts, "SELECT id, v FROM mx_live ORDER BY id", nil)
		if err != nil {
			t.Fatalf("%s AIR query: %v", label, err)
		}
		if len(airRes.Rows) != len(want) {
			t.Fatalf("%s row count mismatch: AIR=%d PG=%d", label, len(airRes.Rows), len(want))
		}
		for i := range want {
			gotID := fmt.Sprintf("%d", want[i][0])
			gotV := fmt.Sprintf("%d", want[i][1])
			if airRes.Rows[i][0].Text != gotID || airRes.Rows[i][1].Text != gotV {
				t.Fatalf("%s row %d mismatch: AIR=(%s,%s) PG=(%s,%s)",
					label, i, airRes.Rows[i][0].Text, airRes.Rows[i][1].Text, gotID, gotV)
			}
		}
	}

	// Snapshot 1: tx2 on row 4 is still in-progress -> PG & AIR both see (4, 40).
	checkMatchesPG("in-progress-tx2")

	// Now commit tx2 on row 4, wait for WAL, and verify PG & AIR both see (4, 400).
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}
	_ = c1.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsnStr)
	_, _ = fmt.Sscanf(lsnStr, "%X/%X", &hi, &lo)
	targetLSN = (uint64(hi) << 32) | uint64(lo)
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && tl.LastLSN() < targetLSN-8 {
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("WAL catchup: tl.LastLSN=%#x targetLSN=%#x", tl.LastLSN(), targetLSN)
	checkMatchesPG("committed-tx2")

	dm := rs.DetailedMetrics()
	if dm.MultiXactFallbacks != 0 {
		t.Fatalf("expected 0 MultiXactFallbacks, got %d", dm.MultiXactFallbacks)
	}
}
