package readadapter

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

// buildPGNumericHeader builds an on-disk PostgreSQL NumericLong payload (little-endian).
func buildPGNumericHeader(ndigits uint16, weight int16, sign, dscale uint16, digits ...uint16) []byte {
	// NumericLong: uint16 n_sign_dscale (sign | dscale), int16 n_weight, digits...
	buf := make([]byte, 4+2*len(digits))
	binary.LittleEndian.PutUint16(buf[0:2], sign|(dscale&0x3FFF))
	binary.LittleEndian.PutUint16(buf[2:4], uint16(weight))
	for i, d := range digits {
		binary.LittleEndian.PutUint16(buf[4+2*i:6+2*i], d)
	}
	return buf
}

func TestPhase18A_NumericDecodingAndCompare(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
		want string
	}{
		{"zero", buildPGNumericHeader(0, 0, 0x0000, 0), "0"},
		{"zero_scale2", buildPGNumericHeader(0, 0, 0x0000, 2), "0.00"},
		{"pos_123_45", buildPGNumericHeader(2, 0, 0x0000, 2, 123, 4500), "123.45"},
		{"neg_123_45", buildPGNumericHeader(2, 0, 0x4000, 2, 123, 4500), "-123.45"},
		{"high_prec_30", buildPGNumericHeader(8, 3, 0x0000, 16, 1234, 5678, 9012, 3456, 7890, 1234, 5678, 9012), "1234567890123456.7890123456789012"},
		{"small_frac", buildPGNumericHeader(1, -2, 0x0000, 8, 5), "0.00000005"},
		{"nan", buildPGNumericHeader(0, 0, 0xC000, 0), "NaN"},
		{"pos_inf", buildPGNumericHeader(0, 0, 0xD000, 0), "Infinity"},
		{"neg_inf", buildPGNumericHeader(0, 0, 0xF000, 0), "-Infinity"},
	}
	for _, tc := range cases {
		got, err := DecodePGNumeric(tc.raw)
		if err != nil {
			t.Fatalf("%s: DecodePGNumeric error: %v", tc.name, err)
		}
		if got != tc.want {
			t.Fatalf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}

	// Ordering semantics
	ordered := []string{
		"-Infinity",
		"-999999999999999999999999999.99",
		"-123.45",
		"-0.00000005",
		"0",
		"0.00000005",
		"123.45",
		"1234567890123456.7890123456789012",
		"1234567890123456.7890123456789013",
		"Infinity",
		"NaN",
	}
	for i := 0; i < len(ordered); i++ {
		for j := i + 1; j < len(ordered); j++ {
			if cmp := ComparePGNumeric(ordered[i], ordered[j]); cmp >= 0 {
				t.Fatalf("expected ComparePGNumeric(%q, %q) < 0, got %d", ordered[i], ordered[j], cmp)
			}
			if cmp := ComparePGNumeric(ordered[j], ordered[i]); cmp <= 0 {
				t.Fatalf("expected ComparePGNumeric(%q, %q) > 0, got %d", ordered[j], ordered[i], cmp)
			}
		}
	}
	if ComparePGNumeric("123.4500", "123.45") != 0 {
		t.Fatalf("expected 123.4500 == 123.45")
	}
}

func TestPhase18B_InetCIDRDecodingAndCompare(t *testing.T) {
	// PG on-disk inet: family(2=AF_INET, 3=AF_INET6), bits, ipaddr...
	v4 := []byte{2, 24, 192, 168, 1, 10}
	s, err := DecodePGInet(v4, false)
	if err != nil || s != "192.168.1.10/24" {
		t.Fatalf("v4 inet: got %q, err=%v", s, err)
	}
	v4Host := []byte{2, 32, 10, 0, 0, 1}
	s, err = DecodePGInet(v4Host, false)
	if err != nil || s != "10.0.0.1" {
		t.Fatalf("v4 host inet: got %q, err=%v", s, err)
	}
	v4Cidr := []byte{2, 24, 192, 168, 1, 0}
	s, err = DecodePGInet(v4Cidr, true)
	if err != nil || s != "192.168.1.0/24" {
		t.Fatalf("v4 cidr: got %q, err=%v", s, err)
	}
	v6 := []byte{3, 64, 0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	s, err = DecodePGInet(v6, false)
	if err != nil || s != "2001:db8::1/64" {
		t.Fatalf("v6 inet: got %q, err=%v", s, err)
	}
	// IPv4-mapped IPv6 ::ffff:192.0.2.128
	v4Mapped := []byte{3, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 0, 2, 128}
	s, err = DecodePGInet(v4Mapped, false)
	if err != nil || s != "::ffff:192.0.2.128" {
		t.Fatalf("v4-mapped v6: got %q, err=%v", s, err)
	}

	// Ordering: 10.0.0.1 < 10.0.0.1/24 < 192.168.1.1 < ::1 < 2001:db8::1/64
	if ComparePGInet("10.0.0.1", "10.0.0.10") >= 0 {
		t.Fatalf("expected 10.0.0.1 < 10.0.0.10")
	}
	if ComparePGInet("192.168.1.1", "2001:db8::1") >= 0 {
		t.Fatalf("expected IPv4 < IPv6")
	}
}

func TestPhase18C_ArrayDecoding(t *testing.T) {
	// Empty int4[] (on-disk ArrayType header: ndim=0, dataoffset=0, elemtype=23, little-endian)
	empty := make([]byte, 12)
	binary.LittleEndian.PutUint32(empty[8:12], PGOIDInt4)
	got, err := DecodePGArray(empty, PGOIDInt4)
	if err != nil || got != "{}" {
		t.Fatalf("empty int4[]: got %q, err=%v", got, err)
	}

	// 1D int4[] with non-default lower bound [3:5]={10,NULL,30}
	// On-disk: ndim=1, dataoffset=28 (MAXALIGN(16 + 8 + 1) = 32? 25->32? Wait: 16+8+1=25, MAXALIGN(25)=32, raw starts at +4 so 28 in raw -> dataoffset=28+4=32? Test 28 in raw => dataoffset=28)
	buf := make([]byte, 0, 64)
	hdr := make([]byte, 20)
	binary.LittleEndian.PutUint32(hdr[0:4], 1)          // ndim
	binary.LittleEndian.PutUint32(hdr[4:8], 28)         // dataoffset (including 4-byte varlena header -> 24 in raw)
	binary.LittleEndian.PutUint32(hdr[8:12], PGOIDInt4) // elemOID
	binary.LittleEndian.PutUint32(hdr[12:16], 3)        // dim
	binary.LittleEndian.PutUint32(hdr[16:20], 3)        // lbound
	buf = append(buf, hdr...)
	// null bitmap (1 byte: bit 0=1 (10), bit 1=0 (NULL), bit 2=1 (30) -> 0b00000101 = 5) + 3 bytes alignment padding
	buf = append(buf, 0x05, 0, 0, 0)
	// elem 10 (int32 little-endian)
	tmp := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp, 10)
	buf = append(buf, tmp...)
	// elem 30 (int32 little-endian)
	binary.LittleEndian.PutUint32(tmp, 30)
	buf = append(buf, tmp...)

	got, err = DecodePGArray(buf, PGOIDInt4)
	if err != nil || got != "[3:5]={10,NULL,30}" {
		t.Fatalf("non-default lbound int4[]: got %q, err=%v", got, err)
	}

	// Hostile dimension guard
	hostile := make([]byte, 20)
	binary.LittleEndian.PutUint32(hostile[0:4], 1)
	binary.LittleEndian.PutUint32(hostile[8:12], PGOIDInt4)
	binary.LittleEndian.PutUint32(hostile[12:16], 100000000)
	binary.LittleEndian.PutUint32(hostile[16:20], 1)
	if _, err := DecodePGArray(hostile, PGOIDInt4); err == nil {
		t.Fatalf("expected error on hostile array dimension")
	}

	// OID mismatch guard
	if _, err := DecodePGArray(empty, PGOIDText); !errors.Is(err, ErrUnsupportedType) {
		t.Fatalf("expected ErrUnsupportedType on elem OID mismatch, got %v", err)
	}
}

func TestPhase18_UnsupportedOpsFallback(t *testing.T) {
	ext := map[string]ColumnType{
		"amt":  ColNumeric,
		"ip":   ColInet,
		"net":  ColCIDR,
		"tags": ColTextArray,
	}
	allowed := []string{
		"SELECT id, amt, ip, tags FROM t WHERE amt > '10.50' ORDER BY amt DESC",
		"SELECT amt, count(*), min(amt), max(amt) FROM t GROUP BY amt",
		"SELECT ip FROM t WHERE ip = '10.0.0.1' ORDER BY ip",
		"SELECT tags FROM t WHERE tags = '{a,b}'",
		"SELECT 'a+b', '10.0.0.0/8 << 1' FROM t",
	}
	for _, q := range allowed {
		if err := CheckUnsupportedExtOps(q, ext); err != nil {
			t.Fatalf("expected allowed query %q, got err: %v", q, err)
		}
	}

	rejected := []string{
		"SELECT sum(amt) FROM t",
		"SELECT avg(amt) FROM t",
		"SELECT amt + 1 FROM t",
		"SELECT amt * 2 FROM t",
		"SELECT * FROM t WHERE ip << '10.0.0.0/8'",
		"SELECT host(ip) FROM t",
		"SELECT tags[1] FROM t",
		"SELECT * FROM t WHERE tags @> '{a}'",
		"SELECT * FROM t ORDER BY tags",
	}
	for _, q := range rejected {
		if err := CheckUnsupportedExtOps(q, ext); !errors.Is(err, ErrUnsupportedType) {
			t.Fatalf("expected ErrUnsupportedType for %q, got %v", q, err)
		}
	}
}

func TestPhase18_TypesDifferential_LivePG17(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live PostgreSQL 17 differential test in -short mode")
	}
	initdbMatches, _ := filepath.Glob("/usr/lib/postgresql/*/bin/initdb")
	if len(initdbMatches) == 0 {
		t.Skip("PostgreSQL server binaries not installed")
	}
	initdbBin := initdbMatches[len(initdbMatches)-1]
	pgBin := filepath.Join(filepath.Dir(initdbBin), "postgres")

	tmpDir := t.TempDir()
	pgData := filepath.Join(tmpDir, "pgdata")
	sockDir := filepath.Join(tmpDir, "pgsock")
	_ = os.MkdirAll(sockDir, 0o755)

	if out, err := exec.Command(initdbBin, "-D", pgData, "-U", "postgres", "-A", "trust", "--encoding=UTF8", "--locale=C", "--no-sync").CombinedOutput(); err != nil {
		t.Fatalf("initdb failed: %v\n%s", err, out)
	}
	f, err := os.OpenFile(filepath.Join(pgData, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("\nwal_level = replica\nmax_wal_senders = 4\n")
	_ = f.Close()

	hf, err := os.OpenFile(filepath.Join(pgData, "pg_hba.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = hf.WriteString("\nlocal replication postgres trust\n")
	_ = hf.Close()

	pgCmd := exec.Command(pgBin, "-D", pgData, "-k", sockDir, "-h", "", "-p", "55452", "-F")
	if err := pgCmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = pgCmd.Process.Signal(os.Interrupt)
		_ = pgCmd.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pgDSN := fmt.Sprintf("host=%s port=55452 user=postgres dbname=postgres sslmode=disable", sockDir)
	var pgConn *pgx.Conn
	for i := 0; i < 50; i++ {
		pgConn, err = pgx.Connect(ctx, pgDSN)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if pgConn == nil {
		t.Fatal("failed to connect to PostgreSQL 17")
	}
	defer pgConn.Close(ctx)

	_, _ = pgConn.Exec(ctx, "CHECKPOINT")
	var lsnStr string
	_ = pgConn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsnStr)
	var hi, lo uint32
	_, _ = fmt.Sscanf(lsnStr, "%X/%X", &hi, &lo)
	initLSN := (uint64(hi) << 32) | uint64(lo)

	tenantID := "tenant_p18_types"
	layerDir := filepath.Join(tmpDir, "layers")
	psSock := filepath.Join(tmpDir, "ps.sock")
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: psSock,
		LayerDir:   layerDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ps.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	if _, err := ps.BootstrapTenantFromPGData(ctx, tenantID, 1, pgData, initLSN); err != nil {
		t.Fatal(err)
	}
	tl := ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, "gen-1")

	rxCtx, rxCancel := context.WithCancel(ctx)
	defer rxCancel()
	rx := pageserver.NewWALReceiver(pgDSN+" replication=database", tl, tl.LastLSN())
	go func() { _ = rx.Run(rxCtx) }()

	if _, err := pgConn.Exec(ctx, `
		CREATE TABLE num_items (
			id int4 PRIMARY KEY,
			grp text NOT NULL,
			amt10 numeric(10,2),
			amt_hp numeric
		);
		INSERT INTO num_items VALUES
			(1, 'a', 123.45, '12345678901234567890123456.123456789012'),
			(2, 'a', -123.45, '-0.000000000001'),
			(3, 'b', 0.00, '0'),
			(4, 'b', 99999999.99, '12345678901234567890123456.123456789013'),
			(5, 'b', NULL, 'NaN'),
			(6, 'c', 123.45, 'Infinity'),
			(7, 'c', -0.01, '-Infinity');

		CREATE TABLE net_items (
			id int4 PRIMARY KEY,
			ip inet,
			net cidr
		);
		INSERT INTO net_items VALUES
			(1, '10.0.0.1', '10.0.0.0/8'),
			(2, '10.0.0.10/24', '10.0.0.0/24'),
			(3, '192.168.1.5', '192.168.1.0/24'),
			(4, '::1', '::1/128'),
			(5, '2001:db8::1/64', '2001:db8::/32'),
			(6, '::ffff:192.0.2.1', '::ffff:192.0.2.0/120'),
			(7, NULL, NULL);

		CREATE TABLE arr_items (
			id int4 PRIMARY KEY,
			i2 int2[],
			i4 int4[],
			i8 int8[],
			txt text[],
			b bool[],
			f4 float4[],
			f8 float8[]
		);
		INSERT INTO arr_items VALUES
			(1, '{1,2,3}', '{10,20,30}', '{10000000000,20000000000}', '{"alpha","beta",""}', '{true,false,true}', '{1.5,2.5}', '{3.14159,-2.5}'),
			(2, '{}', '{}', '{}', '{}', '{}', '{}', '{}'),
			(3, '{1,NULL,3}', '{{1,2},{3,4}}', '[2:3]={7,8}', '{"a,b","NULL",NULL,"x\"y"}', '{true,NULL,false}', '{NULL,0.25}', '{NULL,1e10}'),
			(4, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
		CHECKPOINT;
	`); err != nil {
		t.Fatalf("PG17 setup failed: %v", err)
	}
	waitForWALSync(ctx, pgConn, tl, 10*time.Second)

	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		DataDir:          filepath.Join(tmpDir, "sqlite_srv"),
		PGPort:           0,
		PageserverSocket: psSock,
		L1CacheBytes:     16 * 1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	airConn, err := pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=%s sslmode=disable", srv.Port(), tenantID))
	if err != nil {
		t.Fatal(err)
	}
	defer airConn.Close(ctx)

	// 18A: Numeric differential queries (use simple protocol + text scan so pgx compares exact text output e.g. "0.00")
	assertSimpleMatch := func(label, q string) {
		t.Helper()
		scanTextRows := func(conn *pgx.Conn) ([][]string, error) {
			rows, err := conn.Query(ctx, q, pgx.QueryExecModeSimpleProtocol)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			fds := rows.FieldDescriptions()
			var out [][]string
			for rows.Next() {
				ptrs := make([]*string, len(fds))
				dests := make([]any, len(fds))
				for i := range ptrs {
					dests[i] = &ptrs[i]
				}
				if err := rows.Scan(dests...); err != nil {
					return nil, err
				}
				rec := make([]string, len(fds))
				for i, p := range ptrs {
					if p == nil {
						rec[i] = "<NULL>"
					} else {
						rec[i] = *p
					}
				}
				out = append(out, rec)
			}
			return out, rows.Err()
		}
		pgRows, err := scanTextRows(pgConn)
		if err != nil {
			t.Fatalf("[%s] PG query %q failed: %v", label, q, err)
		}
		airRows, err := scanTextRows(airConn)
		if err != nil {
			t.Fatalf("[%s] AIR query %q failed: %v", label, q, err)
		}
		if len(pgRows) != len(airRows) {
			t.Fatalf("[%s] row count mismatch for %q: PG=%d AIR=%d", label, q, len(pgRows), len(airRows))
		}
		for r := range pgRows {
			for c := range pgRows[r] {
				if pgRows[r][c] != airRows[r][c] {
					t.Fatalf("[%s] value mismatch at row %d col %d for %q: PG=%q AIR=%q", label, r, c, q, pgRows[r][c], airRows[r][c])
				}
			}
		}
	}
	numQueries := []string{
		"SELECT id, amt10, amt_hp FROM num_items ORDER BY id",
		"SELECT id, amt_hp FROM num_items ORDER BY amt_hp ASC, id ASC",
		"SELECT id, amt10 FROM num_items WHERE amt10 > '0' ORDER BY id",
		"SELECT id, amt_hp FROM num_items WHERE amt_hp = '12345678901234567890123456.123456789012'",
		"SELECT grp, count(*), min(amt10), max(amt10) FROM num_items GROUP BY grp ORDER BY grp",
		"SELECT a.id, b.id FROM num_items a JOIN num_items b ON a.amt10 = b.amt10 WHERE a.id < b.id ORDER BY a.id, b.id",
		"SELECT sum(amt10) FROM num_items",
		"SELECT amt10 + 1 FROM num_items ORDER BY id",
	}
	for i, q := range numQueries {
		assertSimpleMatch(fmt.Sprintf("18a_num_%d", i), q)
	}

	// 18B: INET / CIDR differential queries
	netQueries := []string{
		"SELECT id, ip, net FROM net_items ORDER BY id",
		"SELECT id, ip FROM net_items WHERE ip IS NOT NULL ORDER BY ip ASC, id ASC",
		"SELECT id, net FROM net_items WHERE net IS NOT NULL ORDER BY net ASC, id ASC",
		"SELECT id FROM net_items WHERE ip = '10.0.0.1'",
		"SELECT id FROM net_items WHERE ip > '10.0.0.1' ORDER BY id",
	}
	for i, q := range netQueries {
		assertSimpleMatch(fmt.Sprintf("18b_net_%d", i), q)
	}

	// 18C: Array differential queries
	arrQueries := []string{
		"SELECT id, i2, i4, i8, txt, b, f4, f8 FROM arr_items ORDER BY id",
		"SELECT id FROM arr_items WHERE i4 = '{10,20,30}'",
		"SELECT id FROM arr_items WHERE txt = '{}'",
		"SELECT id FROM arr_items WHERE i4 IS NULL",
	}
	for i, q := range arrQueries {
		assertSimpleMatch(fmt.Sprintf("18c_arr_%d", i), q)
	}

	// Verify remaining unsupported operations safely reject so gateway falls back to PG
	fallbackQueries := []string{
		"SELECT id FROM net_items WHERE ip << '10.0.0.0/8'",
		"SELECT host(ip) FROM net_items",
		"SELECT i4[1] FROM arr_items",
		"SELECT id FROM arr_items WHERE i4 @> '{10}'",
		"SELECT id FROM arr_items ORDER BY i4",
	}
	for _, q := range fallbackQueries {
		if _, err := queryRowsNormalized(ctx, airConn, q); err == nil {
			t.Fatalf("expected fallback error for unsupported query %q, got nil", q)
		}
	}

	_ = strings.TrimSpace("")
}
