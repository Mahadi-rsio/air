package readadapter

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func pickFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen 127.0.0.1:0: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestLivePG17AirRoExtensionEnforcement(t *testing.T) {
	pgBinDir := "/usr/lib/postgresql/17/bin"
	if _, err := os.Stat(filepath.Join(pgBinDir, "postgres")); err != nil {
		t.Skipf("PG 17 binary not found at %s: %v", pgBinDir, err)
	}

	// Build air_ro.so if not already present or up to date.
	extDir, err := filepath.Abs(filepath.Join("..", "..", "extensions", "air_ro"))
	if err != nil {
		t.Fatalf("abs extDir: %v", err)
	}
	if out, err := exec.Command("make", "-C", extDir, "all").CombinedOutput(); err != nil {
		t.Fatalf("make air_ro failed: %v\n%s", err, string(out))
	}
	soPath := filepath.Join(extDir, "air_ro.so")
	if _, err := os.Stat(soPath); err != nil {
		t.Fatalf("air_ro.so missing at %s: %v", soPath, err)
	}

	// Install extension control/SQL into a temporary PGSHAREDIR?
	// Even simpler: use shared_preload_libraries = '<soPath>' and call
	// CREATE FUNCTION ... directly in the seed phase, OR test hooks directly
	// without needing root write access to /usr/share/postgresql/17/extension.
	rootDir := t.TempDir()
	dataDir := filepath.Join(rootDir, "pgdata")
	sockDir := filepath.Join(rootDir, "sock")
	if err := os.MkdirAll(sockDir, 0o700); err != nil {
		t.Fatalf("mkdir sock: %v", err)
	}

	runAsPG := func(name string, args ...string) string {
		t.Helper()
		cmd := exec.Command(filepath.Join(pgBinDir, name), args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v failed: %v\n%s", name, args, err, string(out))
		}
		return string(out)
	}

	runAsPG("initdb", "-D", dataDir, "-U", "postgres", "--auth=trust", "--no-sync")

	port := pickFreePort(t)
	confPath := filepath.Join(dataDir, "postgresql.conf")

	// Phase A: Seed table & air_ro_status() function with air_ro.enabled = off.
	appendConf := func(extra string) {
		t.Helper()
		f, err := os.OpenFile(confPath, os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatalf("open postgresql.conf: %v", err)
		}
		defer f.Close()
		if _, err := f.WriteString(extra); err != nil {
			t.Fatalf("write postgresql.conf: %v", err)
		}
	}

	appendConf(fmt.Sprintf(`
port = %d
listen_addresses = '127.0.0.1'
unix_socket_directories = '%s'
fsync = off
shared_preload_libraries = '%s'
air_ro.enabled = off
`, port, sockDir, soPath))

	pgCtl := func(args ...string) {
		t.Helper()
		runAsPG("pg_ctl", append([]string{"-D", dataDir, "-w"}, args...)...)
	}

	pgCtl("-l", filepath.Join(rootDir, "pg.log"), "start")
	stopped := false
	defer func() {
		if !stopped {
			_ = exec.Command(filepath.Join(pgBinDir, "pg_ctl"), "-D", dataDir, "-m", "immediate", "stop").Run()
		}
	}()

	psql := func(sql string) (string, error) {
		cmd := exec.Command(filepath.Join(pgBinDir, "psql"),
			"-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", "-d", "postgres",
			"-v", "ON_ERROR_STOP=1", "-At", "-c", sql)
		out, err := cmd.CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}

	seedSQL := fmt.Sprintf(`
CREATE TABLE items (id int primary key, val text);
INSERT INTO items VALUES (1, 'alpha'), (2, 'beta');
CREATE FUNCTION air_ro_status(
    OUT enabled bool,
    OUT queries_allowed int8,
    OUT writes_blocked int8,
    OUT allow_temp_tables bool
)
RETURNS record
AS '%s', 'air_ro_status'
LANGUAGE C STRICT VOLATILE;
`, soPath)
	if out, err := psql(seedSQL); err != nil {
		t.Fatalf("seed failed: %v\n%s", err, out)
	}

	// Phase B: Restart with air_ro.enabled = on.
	pgCtl("stop")
	appendConf("\nair_ro.enabled = on\n")
	pgCtl("-l", filepath.Join(rootDir, "pg.log"), "start")

	// 1. Read-only queries MUST succeed.
	allowed := []struct {
		name string
		sql  string
		want string
	}{
		{"select_count", "SELECT count(*) FROM items;", "2"},
		{"select_filter", "SELECT val FROM items WHERE id = 1;", "alpha"},
		{"explain_analyze", "EXPLAIN (ANALYZE, COSTS OFF) SELECT * FROM items WHERE id = 2;", ""},
		{"prepare_exec", "PREPARE p1 (int) AS SELECT val FROM items WHERE id = $1; EXECUTE p1(2);", "PREPARE\nbeta"},
		{"tx_readonly", "BEGIN READ ONLY; SELECT count(*) FROM items; COMMIT;", "BEGIN\n2\nCOMMIT"},
		{"copy_to", "COPY (SELECT id, val FROM items ORDER BY id) TO STDOUT;", "1\talpha\n2\tbeta"},
		{"set_safe_guc", "SET work_mem = '8MB'; SHOW work_mem;", "SET\n8MB"},
	}
	for _, tc := range allowed {
		out, err := psql(tc.sql)
		if err != nil {
			t.Fatalf("allowed query %s (%q) failed: %v\n%s", tc.name, tc.sql, err, out)
		}
		if tc.want != "" && out != tc.want {
			t.Fatalf("allowed query %s: got %q, want %q", tc.name, out, tc.want)
		}
	}

	// 2. Write / mutating / lock queries MUST fail.
	blocked := []struct {
		name string
		sql  string
	}{
		{"insert", "INSERT INTO items VALUES (3, 'gamma');"},
		{"update", "UPDATE items SET val = 'x' WHERE id = 1;"},
		{"delete", "DELETE FROM items WHERE id = 1;"},
		{"merge", "MERGE INTO items t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN DELETE;"},
		{"modifying_cte", "WITH d AS (DELETE FROM items WHERE id = 1 RETURNING id) SELECT * FROM d;"},
		{"for_update", "SELECT * FROM items WHERE id = 1 FOR UPDATE;"},
		{"for_share", "SELECT * FROM items WHERE id = 1 FOR SHARE;"},
		{"create_table", "CREATE TABLE evil (id int);"},
		{"create_temp_table", "CREATE TEMP TABLE tmp_evil (id int);"},
		{"select_into", "SELECT * INTO items_copy FROM items;"},
		{"alter_table", "ALTER TABLE items ADD COLUMN extra int;"},
		{"drop_table", "DROP TABLE items;"},
		{"truncate", "TRUNCATE items;"},
		{"copy_from", "COPY items FROM STDIN;"},
		{"do_block", "DO $$ BEGIN INSERT INTO items VALUES (9, 'do'); END $$;"},
		{"begin_read_write", "BEGIN READ WRITE; SELECT 1; COMMIT;"},
		{"set_ro_off", "SET transaction_read_only = off;"},
		{"set_default_ro_off", "SET default_transaction_read_only = off;"},
	}
	for _, tc := range blocked {
		out, err := psql(tc.sql)
		if err == nil {
			t.Fatalf("blocked query %s (%q) unexpectedly SUCCEEDED with output: %s", tc.name, tc.sql, out)
		}
	}

	// 3. Verify air_ro_status() metrics and data integrity.
	statusOut, err := psql("SELECT enabled, queries_allowed, writes_blocked, allow_temp_tables FROM air_ro_status();")
	if err != nil {
		t.Fatalf("air_ro_status failed: %v\n%s", err, statusOut)
	}
	parts := strings.Split(statusOut, "|")
	if len(parts) != 4 {
		t.Fatalf("unexpected air_ro_status output: %q", statusOut)
	}
	if parts[0] != "t" || parts[3] != "f" {
		t.Fatalf("unexpected flags in air_ro_status: %q", statusOut)
	}
	qAllowed, _ := strconv.ParseInt(parts[1], 10, 64)
	wBlocked, _ := strconv.ParseInt(parts[2], 10, 64)
	if qAllowed < 6 {
		t.Fatalf("expected queries_allowed >= 6, got %d", qAllowed)
	}
	if wBlocked < int64(len(blocked)) {
		t.Fatalf("expected writes_blocked >= %d, got %d", len(blocked), wBlocked)
	}

	// Confirm table contents are untouched.
	countOut, err := psql("SELECT count(*) FROM items;")
	if err != nil || countOut != "2" {
		t.Fatalf("expected 2 intact rows after blocked writes, got %q (err=%v)", countOut, err)
	}

	pgCtl("stop")
	stopped = true
}
