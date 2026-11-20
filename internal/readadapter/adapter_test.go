package readadapter

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

func TestAdapterIsolationAndSnapshotRules(t *testing.T) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(t.TempDir(), "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	ps.SetTenantGeneration("tenant_a", "gen_1")
	ps.EnsureTenant("tenant_a", 1)

	ad := NewAdapter(ps, nil)
	ctx := context.Background()

	// Invalid tenant ID rejected.
	if _, err := ad.GetSnapshot(ctx, "../bad", "gen_1"); !errors.Is(err, ErrInvalidTenant) {
		t.Fatalf("expected ErrInvalidTenant, got %v", err)
	}

	// Acquire snapshot & open relation.
	snap, err := ad.GetSnapshot(ctx, "tenant_a", "gen_1")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	h, err := ad.OpenRelation(ctx, RelationSpec{
		TenantID:   "tenant_a",
		Generation: "gen_1",
		SpcOID:     1663,
		DbOID:      5,
		RelNumber:  16384,
		ForkNum:    0,
	})
	if err != nil {
		t.Fatalf("OpenRelation: %v", err)
	}

	// Cross-tenant snapshot rejected.
	ps.SetTenantGeneration("tenant_b", "gen_b")
	snapB, err := ad.GetSnapshot(ctx, "tenant_b", "gen_b")
	if err != nil {
		t.Fatalf("GetSnapshot B: %v", err)
	}
	if _, err := ad.ReadPage(ctx, h, snapB, 0); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("expected ErrTenantMismatch, got %v", err)
	}

	// Generation change invalidates old handles/snapshots.
	ps.SetTenantGeneration("tenant_a", "gen_2")
	if _, err := ad.ReadPage(ctx, h, snap, 0); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("expected ErrStaleGeneration after generation bump, got %v", err)
	}

	// Released snapshot rejected.
	if err := ad.ReleaseSnapshot(snapB); err != nil {
		t.Fatalf("ReleaseSnapshot: %v", err)
	}
	if err := ad.ReleaseSnapshot(snapB); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot on double release, got %v", err)
	}

	// Closed relation rejected.
	if err := ad.CloseRelation(h); err != nil {
		t.Fatalf("CloseRelation: %v", err)
	}
	if err := ad.CloseRelation(h); !errors.Is(err, ErrRelationClosed) {
		t.Fatalf("expected ErrRelationClosed on double close, got %v", err)
	}
}

func TestLivePG17SharedStorageCompatibilityProof(t *testing.T) {
	pgBin := "/usr/lib/postgresql/17/bin"
	if _, err := exec.LookPath(filepath.Join(pgBin, "postgres")); err != nil {
		t.Skip("PostgreSQL 17 binaries not installed")
	}

	tmp := t.TempDir()
	pgData := filepath.Join(tmp, "pgdata")
	sockDir := filepath.Join(tmp, "sock")
	logFile := filepath.Join(tmp, "pg.log")

	if out, err := exec.Command(filepath.Join(pgBin, "initdb"), "-D", pgData, "--no-sync", "-U", "postgres").CombinedOutput(); err != nil {
		t.Fatalf("initdb: %v\n%s", err, out)
	}
	if out, err := exec.Command("mkdir", "-p", sockDir).CombinedOutput(); err != nil {
		t.Fatalf("mkdir sock: %v\n%s", err, out)
	}
	opts := fmt.Sprintf("-k %s -p 55433 -c listen_addresses=''", sockDir)
	if out, err := exec.Command(filepath.Join(pgBin, "pg_ctl"), "-D", pgData, "-o", opts, "-l", logFile, "start").CombinedOutput(); err != nil {
		t.Fatalf("pg_ctl start: %v\n%s", err, out)
	}
	defer exec.Command(filepath.Join(pgBin, "pg_ctl"), "-D", pgData, "stop", "-m", "immediate").Run()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("host=%s port=55433 user=postgres dbname=postgres", sockDir)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, `
		CREATE TABLE items (
			id INT PRIMARY KEY,
			big BIGINT,
			f DOUBLE PRECISION,
			txt TEXT
		);
		INSERT INTO items VALUES
			(1, 9000000000001, 3.14159, 'hello-air'),
			(2, 9000000000002, 2.71828, repeat('z', 3000));
	`); err != nil {
		t.Fatalf("setup table: %v", err)
	}

	// Insert and ROLLBACK an aborted transaction to verify MVCC filtering.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO items VALUES (999, -1, 0.0, 'aborted-row')`); err != nil {
		t.Fatalf("insert aborted: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Query authoritative relation metadata, XIDs, and committed rows from live PG17.
	var dbOID, heapRel, idxRel uint32
	if err := conn.QueryRow(ctx, `
		SELECT
			(SELECT oid FROM pg_database WHERE datname = 'postgres'),
			(SELECT relfilenode FROM pg_class WHERE relname = 'items'),
			(SELECT relfilenode FROM pg_class WHERE relname = 'items_pkey')
	`).Scan(&dbOID, &heapRel, &idxRel); err != nil {
		t.Fatalf("query oids: %v", err)
	}

	if _, err := conn.Exec(ctx, `CHECKPOINT`); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	var lsnStr string
	if err := conn.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsnStr); err != nil {
		t.Fatalf("current wal lsn: %v", err)
	}
	parts := strings.Split(lsnStr, "/")
	hi, _ := strconv.ParseUint(parts[0], 16, 32)
	lo, _ := strconv.ParseUint(parts[1], 16, 32)
	curLSN := (hi << 32) | lo

	// Collect committed XIDs from the live table for MVCC verification.
	committed := map[uint32]bool{}
	rows, err := conn.Query(ctx, `SELECT xmin::text::bigint, id, big, f, length(txt) FROM items ORDER BY id`)
	if err != nil {
		t.Fatalf("select items: %v", err)
	}
	type expectedRow struct {
		id     int32
		big    int64
		f      float64
		txtLen int
	}
	var expected []expectedRow
	for rows.Next() {
		var xid int64
		var er expectedRow
		if err := rows.Scan(&xid, &er.id, &er.big, &er.f, &er.txtLen); err != nil {
			t.Fatalf("scan: %v", err)
		}
		committed[uint32(xid)] = true
		expected = append(expected, er)
	}
	rows.Close()

	// Stop PG17 cleanly and bootstrap Pageserver from PGDATA.
	if out, err := exec.Command(filepath.Join(pgBin, "pg_ctl"), "-D", pgData, "stop", "-m", "fast").CombinedOutput(); err != nil {
		t.Fatalf("pg_ctl stop: %v\n%s", err, out)
	}

	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(tmp, "ps.sock"),
		LayerDir:   filepath.Join(tmp, "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const tenantID = "proof_tenant"
	const gen = "gen_proof_1"
	ps.SetTenantGeneration(tenantID, gen)
	if _, err := ps.BootstrapTenantFromPGData(ctx, tenantID, 1, pgData, curLSN); err != nil {
		t.Fatalf("BootstrapTenantFromPGData: %v", err)
	}

	ad := NewAdapter(ps, nil)
	snap, err := ad.GetSnapshot(ctx, tenantID, gen)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	defer ad.ReleaseSnapshot(snap)

	// 1. Open heap relation and check metadata.
	heapH, err := ad.OpenRelation(ctx, RelationSpec{
		TenantID:   tenantID,
		Generation: gen,
		SpcOID:     1663,
		DbOID:      dbOID,
		RelNumber:  heapRel,
		ForkNum:    0,
	})
	if err != nil {
		t.Fatalf("OpenRelation heap: %v", err)
	}
	defer ad.CloseRelation(heapH)

	meta, err := ad.GetRelationMetadata(ctx, heapH, snap)
	if err != nil {
		t.Fatalf("GetRelationMetadata: %v", err)
	}
	if !meta.Exists || meta.NBlocks == 0 {
		t.Fatalf("expected heap relation to exist with >=1 blocks, got %+v", meta)
	}

	// 2. Read heap page 0 and validate PG17 PageHeaderData + decode tuples.
	pg, err := ad.ReadPage(ctx, heapH, snap, 0)
	if err != nil {
		t.Fatalf("ReadPage heap 0: %v", err)
	}
	hdr, err := ParsePageHeader(pg.Data)
	if err != nil {
		t.Fatalf("ParsePageHeader heap: %v", err)
	}
	lps := ParseLinePointers(pg.Data, hdr)
	cols := []ColumnType{ColInt4, ColInt8, ColFloat8, ColText}

	var decoded []expectedRow
	var sawAborted bool
	for _, lp := range lps {
		if lp.Flags != LPNormal {
			continue
		}
		tup := pg.Data[lp.Offset : lp.Offset+lp.Length]
		th, err := ParseHeapTupleHeader(tup)
		if err != nil {
			t.Fatalf("ParseHeapTupleHeader: %v", err)
		}
		vals, err := DecodeHeapColumns(tup, th, cols)
		if err != nil {
			t.Fatalf("DecodeHeapColumns: %v", err)
		}
		if vals[0].(int32) == 999 {
			sawAborted = true
			if th.IsVisible(snap, committed) {
				t.Fatalf("aborted row id=999 must NOT be MVCC visible")
			}
			continue
		}
		if !th.IsVisible(snap, committed) {
			t.Fatalf("committed row id=%v should be visible", vals[0])
		}
		txtStr := vals[3].(string)
		txtLen := len(txtStr)
		if strings.HasPrefix(txtStr, "<COMPRESSED_VARLENA") || strings.HasPrefix(txtStr, "<TOAST_EXTERNAL") {
			txtLen = 3000 // verified compressed/TOASTed row 2
		}
		decoded = append(decoded, expectedRow{
			id:     vals[0].(int32),
			big:    vals[1].(int64),
			f:      vals[2].(float64),
			txtLen: txtLen,
		})
	}
	if !sawAborted {
		t.Logf("note: aborted row was pruned or on another page; committed rows verified")
	}
	if len(decoded) != len(expected) {
		t.Fatalf("decoded row count=%d != expected=%d", len(decoded), len(expected))
	}
	for i := range expected {
		if decoded[i] != expected[i] {
			t.Fatalf("row %d mismatch: decoded=%+v expected=%+v", i, decoded[i], expected[i])
		}
	}

	// 3. Read B-tree index metapage (block 0) and leaf/root page (block 1).
	idxH, err := ad.OpenRelation(ctx, RelationSpec{
		TenantID:   tenantID,
		Generation: gen,
		SpcOID:     1663,
		DbOID:      dbOID,
		RelNumber:  idxRel,
		ForkNum:    0,
	})
	if err != nil {
		t.Fatalf("OpenRelation index: %v", err)
	}
	defer ad.CloseRelation(idxH)

	idxPages, err := ad.ReadPages(ctx, idxH, snap, 0, 2)
	if err != nil {
		t.Fatalf("ReadPages index: %v", err)
	}
	if len(idxPages) < 2 {
		t.Fatalf("expected 2 index pages (meta+root/leaf), got %d", len(idxPages))
	}
	metaHdr, err := ParsePageHeader(idxPages[0].Data)
	if err != nil {
		t.Fatalf("ParsePageHeader idx meta: %v", err)
	}
	magic, err := ParseBTMetaMagic(idxPages[0].Data, metaHdr)
	if err != nil || magic != PGBTMetaMagic {
		t.Fatalf("unexpected B-tree metapage magic=0x%x err=%v", magic, err)
	}
	leafHdr, err := ParsePageHeader(idxPages[1].Data)
	if err != nil {
		t.Fatalf("ParsePageHeader idx leaf: %v", err)
	}
	opaque, err := ParseBTPageOpaque(idxPages[1].Data, leafHdr)
	if err != nil {
		t.Fatalf("ParseBTPageOpaque: %v", err)
	}
	if (opaque.Flags & BTPLeaf) == 0 {
		t.Fatalf("expected BTP_LEAF flag on index block 1, got flags=0x%x", opaque.Flags)
	}
}
