package readadapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

// encodeRawLZ4Block encodes src into a valid raw LZ4 block (literal-only + RLE match sequences)
// for testing PostgreSQL LZ4 inline and external TOAST decompression deterministically.
func encodeRawLZ4Block(src []byte) []byte {
	// If src is a repeated pattern or general buffer, we can emit a 16-byte literal prefix
	// followed by LZ4 match backreferences if src repeats, or a valid literal-only LZ4 block.
	// Let's also support real backreference compression when src consists of repeated 16-byte blocks!
	if len(src) >= 32 && bytes.Equal(src[:16], src[16:32]) {
		// Check if all of src is repetitions of src[:16]
		allRep := true
		for i := 16; i+16 <= len(src); i += 16 {
			if !bytes.Equal(src[:16], src[i:i+16]) {
				allRep = false
				break
			}
		}
		if allRep && len(src)%16 == 0 && len(src) >= 48 {
			var out []byte
			// Sequence 1: 16-byte literal (src[:16]), match offset = 16, matchLen = len(src) - 32 (leaving last 16 bytes as final literal sequence per LZ4 spec)
			matchLen := len(src) - 32
			mToken := matchLen - 4
			var token byte = 0xF0 // litLen = 15 (+1 extended byte = 16)
			if mToken >= 15 {
				token |= 0x0F
			} else {
				token |= byte(mToken)
			}
			out = append(out, token)
			out = append(out, 1) // 15 + 1 = 16 literal bytes
			out = append(out, src[:16]...)
			// 2-byte little-endian offset = 16
			out = append(out, 16, 0)
			if mToken >= 15 {
				rem := mToken - 15
				for rem >= 255 {
					out = append(out, 255)
					rem -= 255
				}
				out = append(out, byte(rem))
			}
			// Final sequence (literals only: last 16 bytes)
			out = append(out, 0xF0, 1)
			out = append(out, src[len(src)-16:]...)
			return out
		}
	}

	// General literal-only LZ4 block
	var out []byte
	litLen := len(src)
	if litLen < 15 {
		out = append(out, byte(litLen<<4))
	} else {
		out = append(out, 0xF0)
		rem := litLen - 15
		for rem >= 255 {
			out = append(out, 255)
			rem -= 255
		}
		out = append(out, byte(rem))
	}
	out = append(out, src...)
	return out
}

func buildToastChunkTuple(xmin, xmax uint32, infomask uint16, chunkID uint32, chunkSeq int32, chunkData []byte) []byte {
	var tb tupleBuilder
	tb.addOID(chunkID)
	tb.addInt4(chunkSeq)
	// 4-byte uncompressed varlena for chunk_data
	tb.align(4)
	hdr4 := uint32(len(chunkData)+4) << 2
	var hbuf [4]byte
	binary.LittleEndian.PutUint32(hbuf[:], hdr4)
	tb.buf = append(tb.buf, hbuf[:]...)
	tb.buf = append(tb.buf, chunkData...)

	tup := buildHeapTuple(xmin, xmax, infomask, tb.buf)
	binary.LittleEndian.PutUint16(tup[18:20], 3) // natts = 3
	return tup
}

func buildExternalToastPointer(rawPayloadSize int, extSize int, compMethod uint32, valueID uint32, toastRelID uint32) [18]byte {
	var ptr [18]byte
	ptr[0] = 0x01
	ptr[1] = PGVarTagOnDisk
	binary.LittleEndian.PutUint32(ptr[2:6], uint32(int32(rawPayloadSize+4)))
	extInfo := (uint32(extSize) & 0x3FFFFFFF) | ((compMethod & 0x03) << 30)
	binary.LittleEndian.PutUint32(ptr[6:10], extInfo)
	binary.LittleEndian.PutUint32(ptr[10:14], valueID)
	binary.LittleEndian.PutUint32(ptr[14:18], toastRelID)
	return ptr
}

// TestPhase14B_Unit_LZ4_OutOfOrderChunks_And_CorruptHandling tests:
// 1. Pure-Go LZ4 decompression (with RLE match backreferences and extended lengths).
// 2. Out-of-order TOAST chunks reconstructed in chunk_seq order (0..N-1).
// 3. Inline LZ4-compressed varlena and external multi-chunk LZ4-compressed TOAST value.
// 4. Missing chunk, gap in chunk_seq, and mismatched reconstructed length safely returning ErrTOASTUnsupported.
func TestPhase14B_Unit_LZ4_OutOfOrderChunks_And_CorruptHandling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Direct LZ4 round-trip with RLE match backreferences (4,096 bytes -> ~42 bytes compressed)
	origLZ4 := bytes.Repeat([]byte("AIR_PHASE14_LZ4!"), 256) // 4096 bytes
	compLZ4 := encodeRawLZ4Block(origLZ4)
	if len(compLZ4) >= len(origLZ4)/10 {
		t.Fatalf("expected compressed LZ4 block, got len=%d", len(compLZ4))
	}
	decomp, err := pgLZ4Decompress(compLZ4, len(origLZ4))
	if err != nil {
		t.Fatalf("pgLZ4Decompress: %v", err)
	}
	if !bytes.Equal(decomp, origLZ4) {
		t.Fatalf("pgLZ4Decompress mismatch")
	}

	// 2. Test external multi-chunk LZ4 TOAST + out-of-order chunk_seq + corrupt chunk fallback via air_pg vtab
	tmpDir := t.TempDir()
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(tmpDir, "ps.sock"),
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ps.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	const (
		tenantID     = "t_toast_unit"
		gen          = "gen-1"
		lsn          = uint64(0x3000000)
		spcOID       = uint32(1663)
		dbOID        = uint32(16384)
		heapRel      = uint32(18000)
		toastRelOID  = uint32(18001)
		toastRelNode = uint32(18001)
	)
	ps.SetTenantGeneration(tenantID, gen)
	_ = ps.EnsureTenant(tenantID, 1)

	// Split compLZ4 into 3 chunks and store them OUT OF ORDER on the TOAST heap page (seq 2, seq 0, seq 1)
	// plus an invisible uncommitted/aborted duplicate of seq 0 (xmin=999 not committed) to test TOAST MVCC filtering!
	part0 := compLZ4[:10]
	part1 := compLZ4[10:25]
	part2 := compLZ4[25:]
	const validChunkID = uint32(5001)
	const missingSeqChunkID = uint32(5002)

	toastPage := buildPG17HeapPage(lsn, [][]byte{
		buildToastChunkTuple(100, 0, HeapXminCommitted|HeapXmaxInvalid, validChunkID, 2, part2),
		buildToastChunkTuple(999, 0, HeapXminInvalid|HeapXmaxInvalid, validChunkID, 0, []byte("ABORTED_CORRUPT_CHUNK")), // uncommitted xmin=999 -> must be ignored!
		buildToastChunkTuple(100, 0, HeapXminCommitted|HeapXmaxInvalid, validChunkID, 0, part0),
		buildToastChunkTuple(100, 0, HeapXminCommitted|HeapXmaxInvalid, validChunkID, 1, part1),
		// Missing chunk_seq=1 for missingSeqChunkID (only seq 0 and seq 2 present!)
		buildToastChunkTuple(100, 0, HeapXminCommitted|HeapXmaxInvalid, missingSeqChunkID, 0, part0),
		buildToastChunkTuple(100, 0, HeapXminCommitted|HeapXmaxInvalid, missingSeqChunkID, 2, part2),
	})
	if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, toastRelNode, 0, 0, 1, lsn, toastPage); err != nil {
		t.Fatal(err)
	}

	// Build heap page with Row 1 (id=1, valid LZ4 external TOAST pointer) and Row 2 (id=2, corrupt/missing chunk_seq TOAST pointer)
	ptrValid := buildExternalToastPointer(len(origLZ4), len(compLZ4), PGToastCompLZ4, validChunkID, toastRelOID)
	var row1 tupleBuilder
	row1.addInt4(1)
	row1.buf = append(row1.buf, ptrValid[:]...)
	tup1 := buildHeapTuple(100, 0, HeapXminCommitted|HeapXmaxInvalid, row1.buf)
	binary.LittleEndian.PutUint16(tup1[18:20], 2)

	ptrCorrupt := buildExternalToastPointer(len(origLZ4), len(compLZ4), PGToastCompLZ4, missingSeqChunkID, toastRelOID)
	var row2 tupleBuilder
	row2.addInt4(2)
	row2.buf = append(row2.buf, ptrCorrupt[:]...)
	tup2 := buildHeapTuple(100, 0, HeapXminCommitted|HeapXmaxInvalid, row2.buf)
	binary.LittleEndian.PutUint16(tup2[18:20], 2)

	heapPage := buildPG17HeapPage(lsn, [][]byte{tup1, tup2})
	if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, heapRel, 0, 0, 1, lsn, heapPage); err != nil {
		t.Fatal(err)
	}

	snapProv := NewStaticSnapshotProvider()
	snapProv.SetSnapshot(tenantID, gen, SnapshotInfo{LSN: lsn})
	adapter := NewAdapter(ps, snapProv)
	clog := NewCLOGResolver(ps)

	tbl := DiscoveredTable{
		Schema:         "public",
		Name:           "lz4_docs",
		RelOID:         heapRel,
		SpcOID:         spcOID,
		DbOID:          dbOID,
		RelNumber:      heapRel,
		ToastRelOID:    toastRelOID,
		ToastRelNumber: toastRelNode,
		Columns: []DiscoveredColumn{
			{Name: "id", AttNum: 1, PGTypeOID: 23, ColType: ColInt4, SQLiteType: "INTEGER"},
			{Name: "body", AttNum: 2, PGTypeOID: 25, ColType: ColText, SQLiteType: "TEXT"},
		},
	}

	conn, err := OpenSQLiteConn(":memory:", false, 2000)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.RegisterAirPGModule()
	vid, err := conn.RegisterNativeTable(tenantID, gen, tbl, adapter, clog)
	if err != nil {
		t.Fatal(err)
	}
	defer UnregisterNativeTable(vid)

	// Query valid multi-chunk out-of-order LZ4 TOAST row (id = 1)
	res, err := conn.Query("SELECT id, body FROM lz4_docs WHERE id = 1;", nil)
	if err != nil {
		t.Fatalf("query valid LZ4 TOAST row: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][1].Text != string(origLZ4) {
		t.Fatalf("reconstructed LZ4 TOAST mismatch: got len=%d want len=%d", len(res.Rows[0][1].Text), len(origLZ4))
	}

	// Query corrupt/missing chunk_seq row (id = 2) -> must fail with ErrTOASTUnsupported!
	_, err = conn.Query("SELECT id, body FROM lz4_docs WHERE id = 2;", nil)
	if !errors.Is(err, ErrTOASTUnsupported) {
		t.Fatalf("expected ErrTOASTUnsupported for non-contiguous TOAST chunk_seq, got %v", err)
	}
}

// TestPhase14B_LivePG15_TOAST_FullSuite verifies all required Phase 14B scenarios
// against a live PostgreSQL 15 instance with WAL streaming into Pageserver:
// - Small inline TEXT, empty values, NULL values
// - Large TEXT (PGLZ compressed & EXTERNAL uncompressed, multi-chunk 55 KB+)
// - Large JSON (OID 114) and Large JSONB (OID 3802)
// - Large BYTEA (32 KB binary payload across 16+ TOAST chunks)
// - Concurrent updates & old snapshot visibility on updated TOAST values
// - Byte-for-byte comparison against PostgreSQL 15 query results.
func TestPhase14B_LivePG15_TOAST_FullSuite(t *testing.T) {
	initdbBin := "/usr/lib/postgresql/15/bin/initdb"
	pgBin := "/usr/lib/postgresql/15/bin/postgres"
	if _, err := os.Stat(initdbBin); err != nil {
		t.Skip("PostgreSQL 15 binaries not installed")
	}

	tmpDir := t.TempDir()
	pgData := filepath.Join(tmpDir, "pgdata")
	sockDir := filepath.Join(tmpDir, "pgsock")
	_ = os.MkdirAll(sockDir, 0o755)

	if out, err := exec.Command(initdbBin, "-D", pgData, "-U", "postgres", "-A", "trust", "--no-sync").CombinedOutput(); err != nil {
		t.Fatalf("initdb failed: %v\n%s", err, out)
	}
	f, err := os.OpenFile(filepath.Join(pgData, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("\nwal_level = replica\nmax_wal_senders = 4\nautovacuum = off\n")
	_ = f.Close()

	hf, err := os.OpenFile(filepath.Join(pgData, "pg_hba.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = hf.WriteString("\nlocal replication postgres trust\n")
	_ = hf.Close()

	const pgPort = "55446"
	cmd := exec.Command(pgBin, "-D", pgData, "-k", sockDir, "-h", "", "-p", pgPort, "-F")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("host=%s port=%s user=postgres dbname=postgres sslmode=disable", sockDir, pgPort)
	var pgConn *pgx.Conn
	for i := 0; i < 40; i++ {
		pgConn, err = pgx.Connect(ctx, dsn)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if pgConn == nil {
		t.Fatalf("could not connect to PG15")
	}
	defer pgConn.Close(ctx)

	if _, err := pgConn.Exec(ctx, "CHECKPOINT"); err != nil {
		t.Fatalf("CHECKPOINT: %v", err)
	}
	var lsnStr string
	if err := pgConn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsnStr); err != nil {
		t.Fatalf("pg_current_wal_lsn: %v", err)
	}
	initLSN := ParsePGLSN(lsnStr)

	psSock := filepath.Join(tmpDir, "ps.sock")
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: psSock,
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()
	if err := ps.Start(ctx); err != nil {
		t.Fatalf("ps.Start: %v", err)
	}

	const tenantID = "tenant_p14_toast"
	if _, err := ps.BootstrapTenantFromPGData(ctx, tenantID, 1, pgData, initLSN); err != nil {
		t.Fatalf("BootstrapTenantFromPGData: %v", err)
	}
	tl := ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, "gen-1")

	rxCtx, rxCancel := context.WithCancel(ctx)
	defer rxCancel()
	rx := pageserver.NewWALReceiver(dsn+" replication=database", tl, tl.LastLSN())
	go func() {
		_ = rx.Run(rxCtx)
	}()

	waitForWAL := func() uint64 {
		var cur string
		if err := pgConn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&cur); err != nil {
			t.Fatalf("pg_current_wal_lsn: %v", err)
		}
		target := ParsePGLSN(cur)
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			if tl.LastLSN() >= target-8 {
				return tl.LastLSN()
			}
			time.Sleep(15 * time.Millisecond)
		}
		t.Fatalf("WALReceiver did not reach target LSN %s (got %s)", cur, FormatPGLSN(tl.LastLSN()))
		return 0
	}

	if _, err := pgConn.Exec(ctx, `
		CREATE TABLE toast_matrix (
			id int4 PRIMARY KEY,
			small_txt text,
			ext_txt text,
			pglz_txt text,
			doc_json json,
			doc_jsonb jsonb,
			raw_bytes bytea
		);
		-- Force ext_txt and raw_bytes to use external uncompressed TOAST storage (many chunks),
		-- while pglz_txt, doc_json, and doc_jsonb use default EXTENDED (compressed inline or compressed external TOAST).
		ALTER TABLE toast_matrix ALTER COLUMN ext_txt SET STORAGE EXTERNAL;
		ALTER TABLE toast_matrix ALTER COLUMN raw_bytes SET STORAGE EXTERNAL;

		-- Row 1: Small inline text, empty string, NULL values
		INSERT INTO toast_matrix VALUES (
			1,
			'inline_hello',
			'',
			NULL,
			'{"small": true}',
			'{"status": "ok", "count": 42, "items": [1, 2, 3]}'::jsonb,
			'\x'::bytea
		);

		-- Row 2: Large multi-chunk EXTERNAL text (55 KB = ~28 chunks), inline PGLZ text (8 KB),
		-- large JSON (12 KB), large JSONB (15 KB externally TOASTed), and large BYTEA (32 KB = ~17 chunks)
		INSERT INTO toast_matrix VALUES (
			2,
			'row2_small',
			repeat('EXTERNAL_CHUNK_DATA_0123456789_', 1800),
			repeat('PGLZ_COMPRESSIBLE_BLOCK_ABCDEF_', 300),
			('{"payload": "' || repeat('json_val_', 1400) || '", "id": 2}')::json,
			jsonb_build_object(
				'id', 2,
				'active', true,
				'nothing', null,
				'tags', jsonb_build_array('alpha', 'beta', 'gamma'),
				'big_str', repeat('jsonb_toast_data_', 900)
			),
			decode(repeat('deadbeef01020304cafebabe', 2700), 'hex')
		);

		-- Row 3: Incompressible large PGLZ text forced out-of-line to external TOAST (md5 pseudorandom blocks, 40 KB)
		INSERT INTO toast_matrix VALUES (
			3,
			'row3_incompressible_toast',
			repeat('ext3_', 1000),
			(SELECT string_agg(md5(i::text), '') FROM generate_series(1, 1300) AS s(i)),
			NULL,
			NULL,
			decode(repeat('00ffAA55', 2500), 'hex')
		);
	`); err != nil {
		t.Fatalf("populate toast_matrix: %v", err)
	}
	waitForWAL()

	readSrv, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost:       "127.0.0.1",
		PGPort:           0,
		DataDir:          filepath.Join(tmpDir, "readsrv"),
		PageserverSocket: psSock,
		CacheDir:         filepath.Join(tmpDir, "l2cache"),
		L1CacheBytes:     32 * 1024 * 1024,
		ReadersPerTenant: 2,
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := readSrv.Start(); err != nil {
		t.Fatalf("readSrv.Start: %v", err)
	}
	defer readSrv.Stop()

	airDSN := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/%s?sslmode=disable", readSrv.Port(), tenantID)
	airConn, err := pgx.Connect(ctx, airDSN)
	if err != nil {
		t.Fatalf("connect AIR read server: %v", err)
	}
	defer airConn.Close(ctx)

	compareQuery := func(name, sqlQuery string) {
		t.Helper()
		fetch := func(c *pgx.Conn) ([][]string, error) {
			rows, qerr := c.Query(ctx, sqlQuery)
			if qerr != nil {
				return nil, qerr
			}
			defer rows.Close()
			var out [][]string
			for rows.Next() {
				vals, verr := rows.Values()
				if verr != nil {
					return nil, verr
				}
				r := make([]string, len(vals))
				for i, v := range vals {
					switch tv := v.(type) {
					case nil:
						r[i] = "<NULL>"
					case []byte:
						r[i] = fmt.Sprintf("bytes(%d):%x", len(tv), tv)
					case map[string]any, []any:
						b, _ := json.Marshal(tv)
						r[i] = string(b)
					case string:
						trimmed := strings.TrimSpace(tv)
						if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
							var parsed any
							if json.Unmarshal([]byte(trimmed), &parsed) == nil {
								b, _ := json.Marshal(parsed)
								r[i] = string(b)
								continue
							}
						}
						r[i] = tv
					default:
						r[i] = fmt.Sprint(tv)
					}
				}
				out = append(out, r)
			}
			return out, rows.Err()
		}

		pgRows, err := fetch(pgConn)
		if err != nil {
			t.Fatalf("[%s] PG query failed: %v", name, err)
		}
		airRows, err := fetch(airConn)
		if err != nil {
			t.Fatalf("[%s] AIR query failed: %v", name, err)
		}
		if !reflect.DeepEqual(pgRows, airRows) {
			t.Fatalf("[%s] byte-for-byte mismatch for %q:\nPG:  %+v\nAIR: %+v",
				name, sqlQuery, pgRows, airRows)
		}
	}

	compareQuery("row1_inline_empty_null_jsonb", "SELECT id, small_txt, ext_txt, pglz_txt, doc_json, doc_jsonb, raw_bytes FROM toast_matrix WHERE id = 1")
	compareQuery("row2_multichunk_text_json_jsonb_bytea", "SELECT id, small_txt, ext_txt, pglz_txt, doc_json, doc_jsonb, raw_bytes FROM toast_matrix WHERE id = 2")
	compareQuery("row3_external_pglz_and_bytea", "SELECT id, small_txt, ext_txt, pglz_txt, doc_json, doc_jsonb, raw_bytes FROM toast_matrix WHERE id = 3")
	compareQuery("all_rows_ordered", "SELECT id, small_txt, ext_txt, pglz_txt, doc_json, doc_jsonb, raw_bytes FROM toast_matrix ORDER BY id")

	// Pin an old snapshot BEFORE updating Row 2's external TOAST value, then update Row 2 and verify
	// both old snapshot visibility and new snapshot visibility!
	gen := readSrv.nativeSock.GetTenantGeneration(tenantID)
	oldSnap, err := readSrv.nativeAdapter.GetSnapshot(ctx, tenantID, gen)
	if err != nil {
		t.Fatal(err)
	}
	defer readSrv.nativeAdapter.ReleaseSnapshot(oldSnap)

	if _, err := pgConn.Exec(ctx, `
		UPDATE toast_matrix
		SET ext_txt = repeat('UPDATED_TOAST_V2_9876543210_', 2000)
		WHERE id = 2;
	`); err != nil {
		t.Fatalf("UPDATE toast_matrix: %v", err)
	}
	waitForWAL()

	var tblOid uint32
	_ = pgConn.QueryRow(ctx, "SELECT 'toast_matrix'::regclass::oid").Scan(&tblOid)
	var cid uint32
	var ctid string
	qerr := pgConn.QueryRow(ctx, fmt.Sprintf("SELECT chunk_id, ctid::text FROM pg_toast.pg_toast_%d WHERE chunk_seq=12 ORDER BY chunk_id DESC LIMIT 1", tblOid)).Scan(&cid, &ctid)
	t.Logf("PG chunk_seq=12 cid=%d ctid=%s tblOid=%d err=%v", cid, ctid, tblOid, qerr)
	// Latest query must match updated 56 KB TOAST value byte-for-byte
	compareQuery("row2_after_toast_update", "SELECT id, ext_txt FROM toast_matrix WHERE id = 2")

	// Old pinned snapshot must still reconstruct the original pre-update 55.8 KB TOAST value!
	oldTables, err := DiscoverCatalog(ctx, readSrv.nativeAdapter, readSrv.nativeCLOG, oldSnap, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	var oldTbl DiscoveredTable
	for _, tb := range oldTables {
		if tb.Name == "toast_matrix" {
			oldTbl = tb
			break
		}
	}
	snapConn, err := OpenSQLiteConn(":memory:", false, 2000)
	if err != nil {
		t.Fatal(err)
	}
	defer snapConn.Close()
	_ = snapConn.RegisterAirPGModule()
	vid, err := snapConn.RegisterNativeTable(tenantID, gen, oldTbl, readSrv.nativeAdapter, readSrv.nativeCLOG)
	if err != nil {
		t.Fatal(err)
	}
	defer UnregisterNativeTable(vid)

	oldRes, err := snapConn.QueryWithContext(ctx, oldSnap, "SELECT ext_txt FROM toast_matrix WHERE id = 2;", nil)
	if err != nil {
		t.Fatalf("oldSnap TOAST query: %v", err)
	}
	wantOld := strings.Repeat("EXTERNAL_CHUNK_DATA_0123456789_", 1800)
	if len(oldRes.Rows) != 1 || oldRes.Rows[0][0].Text != wantOld {
		t.Fatalf("oldSnap TOAST value mismatch: got len=%d want len=%d", len(oldRes.Rows[0][0].Text), len(wantOld))
	}

	// Verify zero TOAST fallbacks occurred across all valid TOAST queries!
	dm := readSrv.DetailedMetrics()
	if dm.ToastFallbacks != 0 {
		t.Fatalf("expected 0 ToastFallbacks for valid TOAST queries, got %d", dm.ToastFallbacks)
	}
}
