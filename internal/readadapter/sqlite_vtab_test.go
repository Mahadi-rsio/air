package readadapter

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

// buildPG17HeapPage constructs a valid 8 KiB PostgreSQL 17 heap page containing tuples.
func buildPG17HeapPage(lsn uint64, tuples [][]byte) []byte {
	page := make([]byte, PGPageSize)
	binary.LittleEndian.PutUint32(page[0:4], uint32(lsn>>32))
	binary.LittleEndian.PutUint32(page[4:8], uint32(lsn&0xFFFFFFFF))
	binary.LittleEndian.PutUint16(page[16:18], uint16(PGPageSize))           // pd_special
	binary.LittleEndian.PutUint16(page[18:20], uint16(PGExpectedPageSizeVer)) // pd_pagesize_version

	lower := uint16(PGPageHeaderSize)
	upper := uint16(PGPageSize)

	for _, tup := range tuples {
		aligned := uint16(alignUp(len(tup), 8))
		upper -= aligned
		copy(page[upper:upper+uint16(len(tup))], tup)

		itemID := uint32(upper&0x7FFF) | (uint32(LPNormal) << 15) | (uint32(len(tup)&0x7FFF) << 17)
		binary.LittleEndian.PutUint32(page[lower:lower+4], itemID)
		lower += 4
	}

	binary.LittleEndian.PutUint16(page[12:14], lower)
	binary.LittleEndian.PutUint16(page[14:16], upper)
	return page
}

func buildHeapTuple(xmin, xmax uint32, infomask uint16, payload []byte) []byte {
	hoff := uint8(24) // 8-byte aligned header (23B + 1B pad)
	tup := make([]byte, int(hoff)+len(payload))
	binary.LittleEndian.PutUint32(tup[0:4], xmin)
	binary.LittleEndian.PutUint32(tup[4:8], xmax)
	binary.LittleEndian.PutUint16(tup[20:22], infomask)
	tup[22] = hoff
	copy(tup[hoff:], payload)
	return tup
}

type tupleBuilder struct {
	buf []byte
}

func (b *tupleBuilder) align(n int) {
	for len(b.buf)%n != 0 {
		b.buf = append(b.buf, 0)
	}
}
func (b *tupleBuilder) addBool(v bool) {
	if v {
		b.buf = append(b.buf, 1)
	} else {
		b.buf = append(b.buf, 0)
	}
}
func (b *tupleBuilder) addChar(v byte) { b.buf = append(b.buf, v) }
func (b *tupleBuilder) addInt2(v int16) {
	b.align(2)
	var tmp [2]byte
	binary.LittleEndian.PutUint16(tmp[:], uint16(v))
	b.buf = append(b.buf, tmp[:]...)
}
func (b *tupleBuilder) addInt4(v int32) {
	b.align(4)
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(v))
	b.buf = append(b.buf, tmp[:]...)
}
func (b *tupleBuilder) addOID(v uint32) {
	b.align(4)
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	b.buf = append(b.buf, tmp[:]...)
}
func (b *tupleBuilder) addFloat4(v float32) {
	b.align(4)
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], math.Float32bits(v))
	b.buf = append(b.buf, tmp[:]...)
}
func (b *tupleBuilder) addFloat8(v float64) {
	b.align(8)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(v))
	b.buf = append(b.buf, tmp[:]...)
}
func (b *tupleBuilder) addName(s string) {
	var raw [64]byte
	copy(raw[:63], s)
	b.buf = append(b.buf, raw[:]...)
}
func (b *tupleBuilder) addText(s string) {
	// 1-byte short varlena header: ((len + 1) << 1) | 1
	b.buf = append(b.buf, byte(((len(s)+1)<<1)|1))
	b.buf = append(b.buf, []byte(s)...)
}

func TestNativePageserverVTab_ZeroSync_MVCC_And_CatalogDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := t.TempDir()
	sockPath := filepath.Join(tmpDir, "ps.sock")

	ps, err := pageserver.New(pageserver.Config{
		SocketPath: sockPath,
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	if err := ps.Start(ctx); err != nil {
		t.Fatalf("pageserver.Start: %v", err)
	}
	defer func() { _ = ps.Close() }()

	const (
		tenantID = "native_vtab_tenant"
		gen      = "gen-1"
		lsn      = uint64(0x2000000)
		dbOID    = uint32(16384)
		pubNsp   = uint32(2200)
		tblOID   = uint32(24576)
		tblNode  = uint32(24576)
	)
	ps.SetTenantGeneration(tenantID, gen)
	tl := ps.EnsureTenant(tenantID, 1)

	putRel := func(spc, db, rel uint32, page []byte) {
		if err := ps.PutPageBatch(ctx, tenantID, spc, db, rel, 0, 0, 1, lsn, page); err != nil {
			t.Fatalf("PutPageBatch(%d/%d/%d): %v", spc, db, rel, err)
		}
	}

	// 1. Seed pg_database (1664/0/1262) with database "postgres" -> dbOID
	var dbRow tupleBuilder
	dbRow.addOID(dbOID)
	dbRow.addName("postgres")
	dbRow.addOID(10)
	dbRow.addInt4(6)
	dbRow.addChar('c')
	dbRow.addBool(false)
	dbRow.addBool(true)
	dbRow.addBool(false)
	dbRow.addInt4(-1)
	dbRow.addOID(726)
	dbRow.addOID(1)
	dbRow.addOID(PGDefaultTablespaceOID)
	putRel(PGGlobalTablespaceOID, 0, PGDatabaseRelID, buildPG17HeapPage(lsn, [][]byte{
		buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen|HeapXmaxInvalid, dbRow.buf),
	}))

	// 2. Seed pg_namespace (1663/dbOID/2615) with "public" -> pubNsp
	var nsRow tupleBuilder
	nsRow.addOID(pubNsp)
	nsRow.addName("public")
	putRel(PGDefaultTablespaceOID, dbOID, PGNamespaceRelID, buildPG17HeapPage(lsn, [][]byte{
		buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen|HeapXmaxInvalid, nsRow.buf),
	}))

	// 3. Seed pg_class (1663/dbOID/1259) with table "products" -> tblOID/tblNode
	var clsRow tupleBuilder
	clsRow.addOID(tblOID)
	clsRow.addName("products")
	clsRow.addOID(pubNsp)
	clsRow.addOID(tblOID + 1)
	clsRow.addOID(0)
	clsRow.addOID(10)
	clsRow.addOID(2)
	clsRow.addOID(tblNode)
	clsRow.addOID(0) // default tablespace
	clsRow.addInt4(1)
	clsRow.addFloat4(2)
	clsRow.addInt4(0)
	clsRow.addOID(0)
	clsRow.addBool(false)
	clsRow.addBool(false)
	clsRow.addChar('p')
	clsRow.addChar('r')
	putRel(PGDefaultTablespaceOID, dbOID, PGClassRelID, buildPG17HeapPage(lsn, [][]byte{
		buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen|HeapXmaxInvalid, clsRow.buf),
	}))

	// 4. Seed pg_attribute (1663/dbOID/1249) for products(id int4, name text, price float8)
	makeAttr := func(name string, typOID uint32, attlen, attnum int16, align byte) []byte {
		var b tupleBuilder
		b.addOID(tblOID)
		b.addName(name)
		b.addOID(typOID)
		b.addInt2(attlen)
		b.addInt2(attnum)
		b.addInt4(-1)
		b.addInt4(-1)
		b.addInt2(0)
		b.addBool(attlen > 0)
		b.addChar(align)
		b.addChar('p')
		b.addChar(0)
		b.addBool(false)
		b.addBool(false)
		b.addBool(false)
		b.addChar(0)
		b.addChar(0)
		b.addBool(false)
		return buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen|HeapXmaxInvalid, b.buf)
	}
	putRel(PGDefaultTablespaceOID, dbOID, PGAttributeRelID, buildPG17HeapPage(lsn, [][]byte{
		makeAttr("id", 23, 4, 1, 'i'),
		makeAttr("name", 25, -1, 2, 'i'),
		makeAttr("price", 701, 8, 3, 'd'),
	}))

	// 5. Seed pg_xact SLRU page: xid 100 = Committed, xid 101 = Aborted, xid 102 = Committed (deleted)
	clogPage := make([]byte, PGPageSize)
	_ = SetCLOGStatus(clogPage, 100, XactStatusCommitted)
	_ = SetCLOGStatus(clogPage, 101, XactStatusAborted)
	_ = SetCLOGStatus(clogPage, 102, XactStatusCommitted)
	tl.ApplyWALRecord(&pageserver.DecodedWALRecord{
		LSN:    lsn,
		EndLSN: lsn,
		XID:    100,
		Rmid:   pageserver.RmXactID,
		Info:   pageserver.XLogXactCommit,
	})
	tl.ApplyWALRecord(&pageserver.DecodedWALRecord{
		LSN:    lsn,
		EndLSN: lsn,
		XID:    101,
		Rmid:   pageserver.RmXactID,
		Info:   pageserver.XLogXactAbort,
	})
	tl.ApplyWALRecord(&pageserver.DecodedWALRecord{
		LSN:    lsn,
		EndLSN: lsn,
		XID:    102,
		Rmid:   pageserver.RmXactID,
		Info:   pageserver.XLogXactCommit,
	})

	// 6. Seed products heap page with 3 tuples:
	//    - id=1 "alpha" 10.5 (xmin=100, no hint bits -> resolved via CLOG!)
	//    - id=2 "aborted" 99.9 (xmin=101 -> invisible!)
	//    - id=3 "beta" 20.0 (xmin=100, xmax=102 -> deleted, invisible!)
	//    - id=4 "gamma" 30.25 (xmin=Frozen -> visible!)
	makeProd := func(id int32, name string, price float64, xmin, xmax uint32, infomask uint16) []byte {
		var b tupleBuilder
		b.addInt4(id)
		b.addText(name)
		b.addFloat8(price)
		return buildHeapTuple(xmin, xmax, infomask, b.buf)
	}
	putRel(PGDefaultTablespaceOID, dbOID, tblNode, buildPG17HeapPage(lsn, [][]byte{
		makeProd(1, "alpha", 10.5, 100, 0, HeapXmaxInvalid),
		makeProd(2, "aborted", 99.9, 101, 0, HeapXmaxInvalid),
		makeProd(3, "beta", 20.0, 100, 102, 0),
		makeProd(4, "gamma", 30.25, FrozenTransactionID, 0, HeapXminFrozen|HeapXmaxInvalid),
	}))

	// 7. Start SQLiteReadServer in NATIVE storage mode (NO /sync, NO PrimaryConnInfo!)
	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost:       "127.0.0.1",
		PGPort:           0,
		DataDir:          filepath.Join(tmpDir, "re"),
		PageserverSocket: sockPath,
		CacheDir:         filepath.Join(tmpDir, "l2cache"),
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("srv.Start: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	// 8. Connect via pgx and run SELECT queries with ZERO /sync!
	connStr := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/%s?sslmode=disable", srv.Port(), tenantID)
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// Verify pg_last_wal_replay_lsn() reports Pageserver LSN
	var lsnStr string
	if err := conn.QueryRow(ctx, "SELECT pg_last_wal_replay_lsn()").Scan(&lsnStr); err != nil {
		t.Fatalf("pg_last_wal_replay_lsn: %v", err)
	}
	if ParsePGLSN(lsnStr) != lsn {
		t.Fatalf("expected LSN %s, got %s", FormatPGLSN(lsn), lsnStr)
	}

	// Verify MVCC-filtered rows from native virtual table
	rows, err := conn.Query(ctx, "SELECT id, name, price FROM products ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT products: %v", err)
	}
	defer rows.Close()

	type prod struct {
		id    int32
		name  string
		price float64
	}
	var got []prod
	for rows.Next() {
		var p prod
		if err := rows.Scan(&p.id, &p.name, &p.price); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, p)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := []prod{
		{id: 1, name: "alpha", price: 10.5},
		{id: 4, name: "gamma", price: 30.25},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d: got %+v, want %+v", i, got[i], want[i])
		}
	}

	// Verify aggregation works natively in SQLite over virtual table
	var cnt int64
	var sum float64
	if err := conn.QueryRow(ctx, "SELECT COUNT(*), SUM(price) FROM products").Scan(&cnt, &sum); err != nil {
		t.Fatalf("aggregate query: %v", err)
	}
	if cnt != 2 || sum != 40.75 {
		t.Fatalf("got count=%d sum=%v, want count=2 sum=40.75", cnt, sum)
	}

	// Verify writes are strictly rejected
	if _, err := conn.Exec(ctx, "INSERT INTO products VALUES (5, 'bad', 1.0)"); err == nil {
		t.Fatalf("expected INSERT to be rejected in read-only native mode")
	}
}

type advancingSnapProv struct {
	calls atomic.Uint64
	lsns  []uint64
}

func (p *advancingSnapProv) CurrentSnapshot(_ context.Context, _, _ string) (SnapshotInfo, error) {
	idx := int(p.calls.Add(1) - 1)
	if idx >= len(p.lsns) {
		idx = len(p.lsns) - 1
	}
	return SnapshotInfo{LSN: p.lsns[idx]}, nil
}

func TestQueryLevelSnapshotConsistency_JoinAndLeakCheck(t *testing.T) {
	tmpDir := t.TempDir()
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(tmpDir, "ps.sock"),
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const tenantID = "tenant-join-snap"
	const gen = "gen-1"
	const dbOID = uint32(16384)
	const usersNode = uint32(30001)
	const ordersNode = uint32(30002)
	const lsn1 = uint64(0x10000)
	const lsn2 = uint64(0x20000)

	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, gen)

	putAt := func(relNode uint32, lsn uint64, page []byte) {
		if err := ps.PutPageBatch(context.Background(), tenantID, PGDefaultTablespaceOID, dbOID, relNode, 0, 0, 1, lsn, page); err != nil {
			t.Fatalf("PutPageBatch(%d): %v", relNode, err)
		}
	}

	makeUser := func(id int32, name string) []byte {
		var b tupleBuilder
		b.addInt4(id)
		b.addText(name)
		return buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen|HeapXmaxInvalid, b.buf)
	}
	makeOrder := func(oid, uid, amt int32) []byte {
		var b tupleBuilder
		b.addInt4(oid)
		b.addInt4(uid)
		b.addInt4(amt)
		return buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen|HeapXmaxInvalid, b.buf)
	}

	// At LSN1: user 1 ("alice") has order 10 (amount 100).
	// At LSN2: user 2 ("bob") is added, and order 10 is updated to amount 999.
	putAt(usersNode, lsn1, buildPG17HeapPage(lsn1, [][]byte{makeUser(1, "alice")}))
	putAt(ordersNode, lsn1, buildPG17HeapPage(lsn1, [][]byte{makeOrder(10, 1, 100)}))

	putAt(usersNode, lsn2, buildPG17HeapPage(lsn2, [][]byte{makeUser(1, "alice"), makeUser(2, "bob")}))
	putAt(ordersNode, lsn2, buildPG17HeapPage(lsn2, [][]byte{makeOrder(10, 1, 999), makeOrder(20, 2, 200)}))

	snapProv := &advancingSnapProv{lsns: []uint64{lsn1, lsn2}}
	adapter := NewAdapter(ps, snapProv)
	clog := NewCLOGResolver(ps)

	conn, err := OpenSQLiteConn(":memory:", false, 4096)
	if err != nil {
		t.Fatalf("OpenSQLiteConn: %v", err)
	}
	defer conn.Close()
	if err := conn.RegisterAirPGModule(); err != nil {
		t.Fatalf("RegisterAirPGModule: %v", err)
	}

	usersTbl := DiscoveredTable{
		Schema: "public", Name: "users", RelOID: usersNode, SpcOID: PGDefaultTablespaceOID, DbOID: dbOID, RelNumber: usersNode,
		Columns: []DiscoveredColumn{
			{Name: "id", AttNum: 1, PGTypeOID: 23, ColType: ColInt4, SQLiteType: "INTEGER"},
			{Name: "name", AttNum: 2, PGTypeOID: 25, ColType: ColText, SQLiteType: "TEXT"},
		},
	}
	ordersTbl := DiscoveredTable{
		Schema: "public", Name: "orders", RelOID: ordersNode, SpcOID: PGDefaultTablespaceOID, DbOID: dbOID, RelNumber: ordersNode,
		Columns: []DiscoveredColumn{
			{Name: "id", AttNum: 1, PGTypeOID: 23, ColType: ColInt4, SQLiteType: "INTEGER"},
			{Name: "user_id", AttNum: 2, PGTypeOID: 23, ColType: ColInt4, SQLiteType: "INTEGER"},
			{Name: "amount", AttNum: 3, PGTypeOID: 23, ColType: ColInt4, SQLiteType: "INTEGER"},
		},
	}

	uid, err := conn.RegisterNativeTable(tenantID, gen, usersTbl, adapter, clog)
	if err != nil {
		t.Fatalf("RegisterNativeTable users: %v", err)
	}
	defer UnregisterNativeTable(uid)
	oid, err := conn.RegisterNativeTable(tenantID, gen, ordersTbl, adapter, clog)
	if err != nil {
		t.Fatalf("RegisterNativeTable orders: %v", err)
	}
	defer UnregisterNativeTable(oid)
	_ = conn.Exec("PRAGMA query_only=ON;")
	conn.SetReadOnlyAuthorizer(true)

	ctx := context.Background()
	snap, err := adapter.GetSnapshot(ctx, tenantID, gen)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	res, err := conn.QueryWithContext(ctx, snap, "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY u.id", nil)
	_ = adapter.ReleaseSnapshot(snap)
	if err != nil {
		t.Fatalf("JOIN QueryWithContext: %v", err)
	}

	// Must have called CurrentSnapshot ONLY ONCE (at query start), so both tables see LSN1 (alice, 100), never (alice, 999).
	if gotCalls := snapProv.calls.Load(); gotCalls != 1 {
		t.Fatalf("expected 1 snapshot acquisition for JOIN, got %d", gotCalls)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].Text != "alice" || res.Rows[0][1].Int64 != 100 {
		t.Fatalf("JOIN observed inconsistent state: %+v", res.Rows)
	}
	if active := adapter.ActiveSnapshotCount(); active != 0 {
		t.Fatalf("expected 0 active snapshots after query, got %d", active)
	}

	// Cancelled context must fail cleanly without leaking snapshots.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	snap2, err := adapter.GetSnapshot(ctx, tenantID, gen)
	if err != nil {
		t.Fatalf("GetSnapshot 2: %v", err)
	}
	_, err = conn.QueryWithContext(cctx, snap2, "SELECT * FROM users", nil)
	_ = adapter.ReleaseSnapshot(snap2)
	if err == nil {
		t.Fatalf("expected error on cancelled context")
	}
	if active := adapter.ActiveSnapshotCount(); active != 0 {
		t.Fatalf("expected 0 active snapshots after cancelled query, got %d", active)
	}
}

func TestVTabQueryOptimizationAndReadOnlyGuard(t *testing.T) {
	tmpDir := t.TempDir()
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(tmpDir, "ps.sock"),
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const tenantID = "tenant-opt"
	const gen = "gen-1"
	const dbOID = uint32(16384)
	const usersNode = uint32(30055)
	const lsn = uint64(0x10000)

	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, gen)

	// Build 3 users:
	// 1: id=100, age=25, name="alice", status="active", created_at="2026-05-01", email=NULL
	// 2: id=101, age=17, name="bob",   status="inactive", created_at="2025-01-01", email="bob@x.com"
	// 3: id=102, age=30, name="carol", status="active", created_at="2026-06-15", email="carol@x.com"
	makeUser := func(id, age int32, name, status, createdAt string, email *string) []byte {
		var b tupleBuilder
		b.addInt4(id)
		b.addInt4(age)
		b.addText(name)
		b.addText(status)
		b.addText(createdAt)
		if email != nil {
			b.addText(*email)
		}
		tup := make([]byte, 24+len(b.buf))
		binary.LittleEndian.PutUint32(tup[0:4], FrozenTransactionID)
		infomask := HeapXminFrozen | HeapXmaxInvalid
		if email == nil {
			infomask |= HeapHasNull
			tup[23] = 0x1F // cols 0..4 non-null, col 5 null
		}
		binary.LittleEndian.PutUint16(tup[20:22], infomask)
		tup[22] = 24
		copy(tup[24:], b.buf)
		return tup
	}
	sBob := "bob@x.com"
	sCarol := "carol@x.com"
	pg := buildPG17HeapPage(lsn, [][]byte{
		makeUser(100, 25, "alice", "active", "2026-05-01", nil),
		makeUser(101, 17, "bob", "inactive", "2025-01-01", &sBob),
		makeUser(102, 30, "carol", "active", "2026-06-15", &sCarol),
	})
	if err := ps.PutPageBatch(context.Background(), tenantID, PGDefaultTablespaceOID, dbOID, usersNode, 0, 0, 1, lsn, pg); err != nil {
		t.Fatalf("PutPageBatch: %v", err)
	}

	snapProv := NewStaticSnapshotProvider()
	snapProv.SetSnapshot(tenantID, gen, SnapshotInfo{LSN: lsn})
	adapter := NewAdapter(ps, snapProv)
	clog := NewCLOGResolver(ps)

	conn, err := OpenSQLiteConn(":memory:", false, 4096)
	if err != nil {
		t.Fatalf("OpenSQLiteConn: %v", err)
	}
	defer conn.Close()
	if err := conn.RegisterAirPGModule(); err != nil {
		t.Fatalf("RegisterAirPGModule: %v", err)
	}

	usersTbl := DiscoveredTable{
		Schema: "public", Name: "users", RelOID: usersNode, SpcOID: PGDefaultTablespaceOID, DbOID: dbOID, RelNumber: usersNode,
		Columns: []DiscoveredColumn{
			{Name: "id", AttNum: 1, PGTypeOID: 23, ColType: ColInt4, SQLiteType: "INTEGER"},
			{Name: "age", AttNum: 2, PGTypeOID: 23, ColType: ColInt4, SQLiteType: "INTEGER"},
			{Name: "name", AttNum: 3, PGTypeOID: 25, ColType: ColText, SQLiteType: "TEXT"},
			{Name: "status", AttNum: 4, PGTypeOID: 25, ColType: ColText, SQLiteType: "TEXT"},
			{Name: "created_at", AttNum: 5, PGTypeOID: 25, ColType: ColText, SQLiteType: "TEXT"},
			{Name: "email", AttNum: 6, PGTypeOID: 25, ColType: ColText, SQLiteType: "TEXT"},
		},
	}
	uid, err := conn.RegisterNativeTable(tenantID, gen, usersTbl, adapter, clog)
	if err != nil {
		t.Fatalf("RegisterNativeTable: %v", err)
	}
	defer UnregisterNativeTable(uid)
	_ = conn.Exec("PRAGMA query_only=ON;")
	conn.SetReadOnlyAuthorizer(true)

	// 1. Point lookup + projection pushdown measurement:
	_, beforeTup, beforeCols, beforeMatched := VTabStats()
	res, err := conn.Query("SELECT name FROM users WHERE id = 100", nil)
	if err != nil {
		t.Fatalf("point lookup: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].Text != "alice" {
		t.Fatalf("point lookup got %+v", res.Rows)
	}
	_, afterTup, afterCols, afterMatched := VTabStats()
	if afterTup-beforeTup != 3 {
		t.Fatalf("expected 3 tuples scanned, got %d", afterTup-beforeTup)
	}
	// Only 2 columns (id + name) out of 6 should be decoded per tuple -> 6 cols total instead of 18!
	if afterCols-beforeCols != 6 {
		t.Fatalf("expected 6 columns decoded with projection pushdown, got %d", afterCols-beforeCols)
	}
	// Only 1 row matched pushed-down filter!
	if afterMatched-beforeMatched != 1 {
		t.Fatalf("expected 1 row matched pushed-down filter, got %d", afterMatched-beforeMatched)
	}

	// 2. Range query: age > 18
	res, err = conn.Query("SELECT id FROM users WHERE age > 18 ORDER BY id", nil)
	if err != nil || len(res.Rows) != 2 || res.Rows[0][0].Int64 != 100 || res.Rows[1][0].Int64 != 102 {
		t.Fatalf("age > 18 got %+v err=%v", res, err)
	}

	// 3. Projection + string equality: status = 'active'
	res, err = conn.Query("SELECT name FROM users WHERE status = 'active' ORDER BY name", nil)
	if err != nil || len(res.Rows) != 2 || res.Rows[0][0].Text != "alice" || res.Rows[1][0].Text != "carol" {
		t.Fatalf("status = active got %+v err=%v", res, err)
	}

	// 4. BETWEEN
	res, err = conn.Query("SELECT id FROM users WHERE created_at BETWEEN '2026-01-01' AND '2026-12-31' ORDER BY id", nil)
	if err != nil || len(res.Rows) != 2 || res.Rows[0][0].Int64 != 100 || res.Rows[1][0].Int64 != 102 {
		t.Fatalf("BETWEEN got %+v err=%v", res, err)
	}

	// 5. IS NULL
	res, err = conn.Query("SELECT id FROM users WHERE email IS NULL", nil)
	if err != nil || len(res.Rows) != 1 || res.Rows[0][0].Int64 != 100 {
		t.Fatalf("IS NULL got %+v err=%v", res, err)
	}

	// 6. Lexical read-only guard tests
	badSQLs := []string{
		"/* leading comment */ INSERT INTO users VALUES (1,2,'a','b','c','d')",
		"WITH c AS(DELETE FROM users) SELECT * FROM c",
		"SELECT * FROM users FOR UPDATE",
		"SELECT 1; DROP TABLE users",
		"PRAGMA writable_schema = ON",
	}
	for _, bsql := range badSQLs {
		toks := tokenizeSQLWords(bsql)
		if len(toks) == 0 {
			t.Fatalf("expected tokens for %q", bsql)
		}
	}
}
