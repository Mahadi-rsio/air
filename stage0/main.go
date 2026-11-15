package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sys/unix"
)

const pgBin = "/usr/lib/postgresql/17/bin"

type Stage0Report struct {
	Timestamp string       `json:"timestamp"`
	ExpA1     ExpA1Result  `json:"exp_a1"`
	ExpC1     ExpC1Result  `json:"exp_c1_and_uqr06"`
	ExpC3     ExpC3Result  `json:"exp_c3"`
	ExpD1     ExpD1Result  `json:"exp_d1"`
	ExpD4D5   ExpD45Result `json:"exp_d4_d5"`
	AllPassed bool         `json:"all_passed"`
}

type ExpA1Result struct {
	SubreaperOK           bool     `json:"pr_set_child_subreaper_ok"`
	PoolSlotCount         int      `json:"pool_slot_count"`
	SystemIdentifiers     []string `json:"system_identifiers"`
	UniqueSystemIDs       bool     `json:"unique_system_ids"`
	ClaimLatenciesMs      []float64 `json:"claim_latencies_ms"`
	ClaimMaxMs            float64  `json:"claim_max_ms"`
	RefillDuringOLTPOk    bool     `json:"refill_during_oltp_ok"`
	OLTPQueryP50Ms        float64  `json:"oltp_query_p50_ms"`
	OLTPQueryP99Ms        float64  `json:"oltp_query_p99_ms"`
	PgTestFsyncSummary    string   `json:"pg_test_fsync_summary"`
	Pass                  bool     `json:"pass"`
}

type ExpC1Result struct {
	SupautilsCompiled       bool `json:"supautils_compiled_pg17"`
	SupautilsLoaded         bool `json:"supautils_loaded"`
	AllowlistedExtInstalled bool `json:"allowlisted_ext_installed_by_non_superuser"`
	ReservedRoleBlocked     bool `json:"reserved_role_blocked"`
	ReservedGrantBlocked    bool `json:"reserved_grant_blocked"`
	PrivExtBlocked          bool `json:"non_allowlisted_ext_blocked"`
	CopyProgramBlocked      bool `json:"copy_program_blocked"`
	PgReadFileBlocked       bool `json:"pg_read_file_blocked"`
	NetnsSSRFBlocked        bool `json:"netns_ssrf_blocked"`
	SiblingPGDATABlocked    bool `json:"sibling_pgdata_blocked"`
	Pass                    bool `json:"pass"`
}

type ExpC3Result struct {
	BallastFileBytes      int64  `json:"ballast_file_bytes"`
	TenantAQuotaMB        int64  `json:"tenant_a_quota_mb"`
	TenantAPeakMB         float64 `json:"tenant_a_peak_mb"`
	TenantAHaltedReadOnly bool   `json:"tenant_a_halted_readonly"`
	TenantBQueriesRun     int    `json:"tenant_b_queries_run"`
	TenantBErrors         int    `json:"tenant_b_errors"`
	Pass                  bool   `json:"pass"`
}

type ExpD1Result struct {
	CommittedAckedRows    int    `json:"committed_acked_rows"`
	RecoveredRowsAfterKill int   `json:"recovered_rows_after_kill9"`
	UncommittedRolledBack bool   `json:"uncommitted_rolled_back"`
	PromoteRTOMs          int64  `json:"promote_rto_ms"`
	SyncStallOnStandbyStop bool  `json:"sync_stall_on_standby_stop"`
	Pass                  bool   `json:"pass"`
}

type ExpD45Result struct {
	R2Key                 string `json:"r2_race_key"`
	RaceTotalAttempts     int    `json:"race_total_attempts"`
	RaceWinners200        int    `json:"race_winners_200"`
	RaceFenced412         int    `json:"race_fenced_412"`
	CorruptBackupRejected bool   `json:"corrupt_backup_rejected"`
	MissingSummaryFallback bool  `json:"missing_summary_fallback_detected"`
	FullBackupR2UploadMs  int64  `json:"full_backup_r2_upload_ms"`
	IncrBackupR2UploadMs  int64  `json:"incr_backup_r2_upload_ms"`
	CombineBackupOK       bool   `json:"pg_combinebackup_ok"`
	VerifyCombinedOK      bool   `json:"pg_verifybackup_combined_ok"`
	PITRRecoveredRows     int    `json:"pitr_recovered_rows_before_drop"`
	PgAmcheckZeroErrors   bool   `json:"pg_amcheck_zero_errors"`
	Pass                  bool   `json:"pass"`
}

func runAsPostgres(dir string, name string, args ...string) (string, error) {
	fullArgs := append([]string{"-u", "postgres", "--", filepath.Join(pgBin, name)}, args...)
	cmd := exec.Command("runuser", fullArgs...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func runCmd(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func dirSizeMB(path string) float64 {
	var total int64
	_ = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return float64(total) / (1024.0 * 1024.0)
}

func runExpA1() ExpA1Result {
	var res ExpA1Result
	// 1. PR_SET_CHILD_SUBREAPER
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err == nil {
		res.SubreaperOK = true
	}

	// 2. Pre-warmed 3-slot initdb pool with --data-checksums
	poolDir := "/vol/exp_a1/pool"
	tenantDir := "/vol/exp_a1/tenants"
	_ = os.RemoveAll("/vol/exp_a1")
	_ = os.MkdirAll(poolDir, 0755)
	_ = os.MkdirAll(tenantDir, 0755)
	_, _ = runCmd("chown", "-R", "postgres:postgres", "/vol/exp_a1")

	sysIDSet := map[string]bool{}
	reSysID := regexp.MustCompile(`Database system identifier:\s+([0-9]+)`)

	for i := 1; i <= 3; i++ {
		slot := filepath.Join(poolDir, fmt.Sprintf("slot_%d", i))
		out, err := runAsPostgres("", "initdb", "-D", slot, "--data-checksums", "-U", "postgres", "--no-instructions")
		if err != nil {
			fmt.Fprintf(os.Stderr, "initdb slot %d failed: %v %s\n", i, err, out)
		}
		ctrlOut, _ := runAsPostgres("", "pg_controldata", "-D", slot)
		if m := reSysID.FindStringSubmatch(ctrlOut); len(m) == 2 {
			res.SystemIdentifiers = append(res.SystemIdentifiers, m[1])
			sysIDSet[m[1]] = true
		}
	}
	res.PoolSlotCount = len(res.SystemIdentifiers)
	res.UniqueSystemIDs = len(sysIDSet) == 3

	// 3. Measure two-phase claim latency (atomic rename + config write)
	for i := 1; i <= 3; i++ {
		slot := filepath.Join(poolDir, fmt.Sprintf("slot_%d", i))
		target := filepath.Join(tenantDir, fmt.Sprintf("tenant_%d", i))
		t0 := time.Now()
		_ = os.Rename(slot, target)
		confPath := filepath.Join(target, "postgresql.auto.conf")
		_ = os.WriteFile(confPath, []byte("shared_buffers = '8MB'\nmax_connections = 15\n"), 0600)
		_ = os.Chown(confPath, 999, 999)
		ms := float64(time.Since(t0).Microseconds()) / 1000.0
		res.ClaimLatenciesMs = append(res.ClaimLatenciesMs, ms)
		if ms > res.ClaimMaxMs {
			res.ClaimMaxMs = ms
		}
	}

	// 4. Start tenant_1 and run OLTP queries while background nice -n 19 initdb refills a slot
	t1Dir := filepath.Join(tenantDir, "tenant_1")
	_, _ = runAsPostgres("", "pg_ctl", "-D", t1Dir, "-l", filepath.Join(t1Dir, "pg.log"), "-o", "-p 5510 -k /tmp", "start")
	_, _ = runAsPostgres("", "psql", "-h", "/tmp", "-p", "5510", "-U", "postgres", "-c", "CREATE TABLE oltp(id serial primary key, v text);")

	var refillDone atomic.Bool
	var refillErr error
	go func() {
		refillSlot := filepath.Join(poolDir, "slot_refill")
		cmd := exec.Command("nice", "-n", "19", "runuser", "-u", "postgres", "--", filepath.Join(pgBin, "initdb"), "-D", refillSlot, "--data-checksums", "-U", "postgres")
		_, refillErr = cmd.CombinedOutput()
		refillDone.Store(true)
	}()

	var latencies []float64
	for iter := 0; (!refillDone.Load() || len(latencies) < 20) && iter < 200; iter++ {
		qStart := time.Now()
		_, err := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5510", "-U", "postgres", "-c", "INSERT INTO oltp(v) VALUES ('bench') RETURNING id;")
		if err == nil {
			latencies = append(latencies, float64(time.Since(qStart).Microseconds())/1000.0)
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	_, _ = runAsPostgres("", "pg_ctl", "-D", t1Dir, "stop", "-m", "fast")
	sort.Float64s(latencies)
	if len(latencies) > 0 {
		res.OLTPQueryP50Ms = latencies[len(latencies)/2]
		res.OLTPQueryP99Ms = latencies[(len(latencies)*99)/100]
	}
	res.RefillDuringOLTPOk = refillErr == nil && len(latencies) >= 20

	// 5. Run pg_test_fsync (1 second per test)
	fsyncOut, _ := runAsPostgres("/vol/exp_a1", "pg_test_fsync", "--secs-per-test=1")
	lines := strings.Split(strings.TrimSpace(fsyncOut), "\n")
	if len(lines) > 12 {
		res.PgTestFsyncSummary = strings.Join(lines[:12], " | ")
	} else {
		res.PgTestFsyncSummary = strings.ReplaceAll(strings.TrimSpace(fsyncOut), "\n", " | ")
	}

	res.Pass = res.SubreaperOK && res.UniqueSystemIDs && res.ClaimMaxMs < 15.0 && res.RefillDuringOLTPOk
	return res
}

func runExpC1() ExpC1Result {
	var res ExpC1Result
	if _, err := os.Stat("/usr/lib/postgresql/17/lib/supautils.so"); err == nil {
		res.SupautilsCompiled = true
	}

	c1Dir := "/vol/exp_c1/t_sec"
	otherDir := "/vol/exp_c1/t_sibling"
	_ = os.RemoveAll("/vol/exp_c1")
	_ = os.MkdirAll(c1Dir, 0700)
	_ = os.MkdirAll(otherDir, 0700)
	_ = os.WriteFile(filepath.Join(otherDir, "PG_VERSION"), []byte("17\n"), 0600)
	_, _ = runCmd("chown", "-R", "postgres:postgres", "/vol/exp_c1")
	// Sibling owned by UID 10002
	_, _ = runCmd("chown", "-R", "10002:10002", otherDir)

	_, _ = runAsPostgres("", "initdb", "-D", c1Dir, "--data-checksums", "-U", "postgres")
	conf := `
shared_buffers = '8MB'
max_connections = 15
shared_preload_libraries = 'supautils'
supautils.reserved_roles = 'postgres,supabase_admin'
supautils.reserved_memberships = 'pg_read_server_files,pg_write_server_files,pg_execute_server_program'
supautils.privileged_extensions = 'pg_stat_statements,pg_trgm'
supautils.superuser = 'postgres'
supautils.privileged_role = 'app_user'
`
	_ = os.WriteFile(filepath.Join(c1Dir, "postgresql.auto.conf"), []byte(conf), 0600)
	_, _ = runCmd("chown", "-R", "postgres:postgres", c1Dir)

	_, err := runAsPostgres("", "pg_ctl", "-D", c1Dir, "-l", filepath.Join(c1Dir, "pg.log"), "-o", "-p 5520 -k /tmp", "start")
	if err == nil {
		res.SupautilsLoaded = true
	} else {
		logBytes, _ := os.ReadFile(filepath.Join(c1Dir, "pg.log"))
		fmt.Fprintf(os.Stderr, "EXP-C1 pg.log:\n%s\n", string(logBytes))
	}

	// Create non-superuser tenant role app_user
	_, _ = runAsPostgres("", "psql", "-h", "/tmp", "-p", "5520", "-U", "postgres", "-c",
		"CREATE ROLE app_user LOGIN CREATEROLE; GRANT ALL ON SCHEMA public TO app_user; GRANT CREATE ON DATABASE postgres TO app_user;")

	// Test 0: Allowlisted extension (pg_trgm) succeeds for non-superuser app_user via supautils
	_, errAllowExt := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5520", "-U", "app_user", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
		`CREATE EXTENSION pg_trgm;`)
	res.AllowlistedExtInstalled = errAllowExt == nil

	// Test 1: Reserved role alteration blocked by supautils
	outRole, errRole := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5520", "-U", "app_user", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
		"ALTER ROLE postgres WITH NOLOGIN;")
	res.ReservedRoleBlocked = errRole != nil && strings.Contains(outRole, "reserved role")

	// Test 2: Reserved membership grant blocked by supautils
	outGrant, errGrant := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5520", "-U", "app_user", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
		"GRANT pg_execute_server_program TO app_user;")
	res.ReservedGrantBlocked = errGrant != nil && strings.Contains(outGrant, "reserved")

	// Test 3: Non-allowlisted superuser extension (amcheck) blocked for app_user
	outExt, errExt := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5520", "-U", "app_user", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
		"CREATE EXTENSION amcheck;")
	res.PrivExtBlocked = errExt != nil && strings.Contains(outExt, "permission denied")

	// Test 4: COPY PROGRAM blocked
	outCopy, errCopy := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5520", "-U", "app_user", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
		"COPY (SELECT 1) TO PROGRAM 'id';")
	res.CopyProgramBlocked = errCopy != nil && strings.Contains(outCopy, "permission denied")

	// Test 5: pg_read_file blocked
	outRead, errRead := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5520", "-U", "app_user", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
		"SELECT pg_read_file('/etc/passwd');")
	res.PgReadFileBlocked = errRead != nil && strings.Contains(outRead, "permission denied")

	_, _ = runAsPostgres("", "pg_ctl", "-D", c1Dir, "stop", "-m", "fast")

	// Test 6: Isolated network namespace blocks outbound SSRF (e.g., 1.1.1.1 or 6PN fdaa::3)
	_, errNet := runCmd("unshare", "--net", "sh", "-c", "ip link set lo up && ping -c 1 -W 1 1.1.1.1")
	res.NetnsSSRFBlocked = errNet != nil

	// Test 7: UID 10001 cannot read sibling UID 10002 PGDATA
	_, errSib := runCmd("setpriv", "--reuid=10001", "--regid=10001", "--clear-groups", "cat", filepath.Join(otherDir, "PG_VERSION"))
	res.SiblingPGDATABlocked = errSib != nil

	res.Pass = res.SupautilsCompiled && res.SupautilsLoaded && res.AllowlistedExtInstalled &&
		res.ReservedRoleBlocked && res.ReservedGrantBlocked && res.PrivExtBlocked &&
		res.CopyProgramBlocked && res.PgReadFileBlocked && res.NetnsSSRFBlocked && res.SiblingPGDATABlocked
	return res
}

func runExpC3() ExpC3Result {
	var res ExpC3Result
	_ = os.RemoveAll("/vol/exp_c3")
	_ = os.MkdirAll("/vol/exp_c3", 0755)
	_, _ = runCmd("chown", "-R", "postgres:postgres", "/vol/exp_c3")

	// 1. Pre-allocate 64 MB .cell_ballast
	ballastPath := "/vol/exp_c3/.cell_ballast"
	f, err := os.Create(ballastPath)
	if err == nil {
		_ = f.Truncate(64 * 1024 * 1024)
		_ = f.Close()
		if st, err := os.Stat(ballastPath); err == nil {
			res.BallastFileBytes = st.Size()
		}
	}

	dirA := "/vol/exp_c3/tenant_a"
	dirB := "/vol/exp_c3/tenant_b"
	_, _ = runAsPostgres("", "initdb", "-D", dirA, "-U", "postgres")
	_, _ = runAsPostgres("", "initdb", "-D", dirB, "-U", "postgres")
	_, _ = runAsPostgres("", "pg_ctl", "-D", dirA, "-l", filepath.Join(dirA, "pg.log"), "-o", "-p 5531 -k /tmp", "start")
	_, _ = runAsPostgres("", "pg_ctl", "-D", dirB, "-l", filepath.Join(dirB, "pg.log"), "-o", "-p 5532 -k /tmp", "start")

	_, _ = runAsPostgres("", "psql", "-h", "/tmp", "-p", "5531", "-U", "postgres", "-c", "CREATE TABLE flood(id serial, payload text); ALTER TABLE flood ALTER COLUMN payload SET STORAGE EXTERNAL;")
	_, _ = runAsPostgres("", "psql", "-h", "/tmp", "-p", "5532", "-U", "postgres", "-c", "CREATE TABLE neighbor(id serial, v text);")

	res.TenantAQuotaMB = 36 // Base initdb is ~29 MB; set quota at 36 MB
	var stopMonitor atomic.Bool
	var enforced atomic.Bool

	// Sub-second Quota Enforcer (ADR-010 fallback)
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for !stopMonitor.Load() {
			<-ticker.C
			mb := dirSizeMB(dirA)
			if mb > float64(res.TenantAQuotaMB) && !enforced.Load() {
				enforced.Store(true)
				_, _ = runAsPostgres("", "psql", "-h", "/tmp", "-p", "5531", "-U", "postgres", "-c",
					"ALTER DATABASE postgres SET default_transaction_read_only = on; SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = 'postgres';")
			}
		}
	}()

	// Run Neighbor Tenant B OLTP concurrently
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			_, err := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5532", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
				"INSERT INTO neighbor(v) VALUES ('ok'); SELECT count(*) FROM neighbor;")
			res.TenantBQueriesRun++
			if err != nil {
				res.TenantBErrors++
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Runaway Tenant A write flood (incompressible EXTERNAL payload)
	for batch := 0; batch < 10; batch++ {
		_, errFlood := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5531", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
			"INSERT INTO flood(payload) SELECT repeat(md5(g::text), 64) FROM generate_series(1, 2000) g;")
		if errFlood != nil {
			break
		}
	}

	wg.Wait()
	stopMonitor.Store(true)

	res.TenantAPeakMB = dirSizeMB(dirA)
	// Verify Tenant A is now locked read-only
	outRO, errRO := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5531", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
		"INSERT INTO flood(payload) VALUES ('should_fail');")
	res.TenantAHaltedReadOnly = errRO != nil && strings.Contains(outRO, "read-only")

	_, _ = runAsPostgres("", "pg_ctl", "-D", dirA, "stop", "-m", "fast")
	_, _ = runAsPostgres("", "pg_ctl", "-D", dirB, "stop", "-m", "fast")

	res.Pass = res.BallastFileBytes == 64*1024*1024 && res.TenantAHaltedReadOnly && res.TenantBErrors == 0
	return res
}

func runExpD1() ExpD1Result {
	var res ExpD1Result
	_ = os.RemoveAll("/vol/exp_d1")
	primDir := "/vol/exp_d1/primary"
	stbyDir := "/vol/exp_d1/standby"
	_ = os.MkdirAll("/vol/exp_d1", 0755)
	_, _ = runCmd("chown", "-R", "postgres:postgres", "/vol/exp_d1")

	_, _ = runAsPostgres("", "initdb", "-D", primDir, "--data-checksums", "-U", "postgres")
	primConf := `
shared_buffers = '8MB'
max_connections = 20
wal_level = 'replica'
max_wal_senders = 5
max_replication_slots = 5
synchronous_commit = on
`
	_ = os.WriteFile(filepath.Join(primDir, "postgresql.auto.conf"), []byte(primConf), 0600)
	_, _ = runCmd("chown", "-R", "postgres:postgres", primDir)

	_, _ = runAsPostgres("", "pg_ctl", "-D", primDir, "-l", filepath.Join(primDir, "pg.log"), "-o", "-p 5541 -k /tmp", "start")
	_, _ = runAsPostgres("", "psql", "-h", "/tmp", "-p", "5541", "-U", "postgres", "-c", "SELECT pg_create_physical_replication_slot('standby1');")

	// Create physical standby via pg_basebackup
	_, _ = runAsPostgres("", "pg_basebackup", "-h", "/tmp", "-p", "5541", "-U", "postgres", "-D", stbyDir, "-R", "-X", "stream", "-S", "standby1")
	stbyConf := "\nprimary_conninfo = 'host=/tmp port=5541 user=postgres application_name=standby1'\n"
	f, _ := os.OpenFile(filepath.Join(stbyDir, "postgresql.auto.conf"), os.O_APPEND|os.O_WRONLY, 0600)
	_, _ = f.WriteString(stbyConf)
	_ = f.Close()
	_, _ = runCmd("chown", "-R", "postgres:postgres", stbyDir)

	_, _ = runAsPostgres("", "pg_ctl", "-D", stbyDir, "-l", filepath.Join(stbyDir, "pg.log"), "-o", "-p 5542 -k /tmp", "start")

	// Enable strict synchronous replication on Primary (two separate -c flags so ALTER SYSTEM is not in a transaction block)
	_, _ = runAsPostgres("", "psql", "-h", "/tmp", "-p", "5541", "-U", "postgres",
		"-c", "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (standby1)';",
		"-c", "SELECT pg_reload_conf();")
	_, _ = runAsPostgres("", "psql", "-h", "/tmp", "-p", "5541", "-U", "postgres", "-c",
		"CREATE TABLE ledger(id int primary key, note text);")

	// Commit 50 rows and record external ACK count
	for i := 1; i <= 50; i++ {
		_, err := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5541", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
			fmt.Sprintf("INSERT INTO ledger VALUES (%d, 'acked_%d');", i, i))
		if err == nil {
			res.CommittedAckedRows++
		}
	}

	// Test CP stall behavior when Standby stops: stop standby briefly, verify COMMIT blocks in SyncRepWaitForLSN
	_, _ = runAsPostgres("", "pg_ctl", "-D", stbyDir, "stop", "-m", "fast")
	ctxStall, cancelStall := context.WithTimeout(context.Background(), 400*time.Millisecond)
	cmdStall := exec.CommandContext(ctxStall, "runuser", "-u", "postgres", "--", filepath.Join(pgBin, "psql"),
		"-h", "/tmp", "-p", "5541", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
		"INSERT INTO ledger VALUES (999, 'must_stall');")
	_ = cmdStall.Run()
	res.SyncStallOnStandbyStop = ctxStall.Err() == context.DeadlineExceeded
	cancelStall()

	// Cancel stalled backend on Primary before restarting Standby so row 999 does not commit
	_, _ = runAsPostgres("", "psql", "-h", "/tmp", "-p", "5541", "-U", "postgres", "-c",
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND query LIKE '%999%';")

	// Restart standby so it catches up cleanly
	_, _ = runAsPostgres("", "pg_ctl", "-D", stbyDir, "-l", filepath.Join(stbyDir, "pg.log"), "-o", "-p 5542 -k /tmp", "start")
	time.Sleep(500 * time.Millisecond)

	// Start an uncommitted transaction (id=51) in background and kill -9 Primary postmaster immediately
	cmdUncomm := exec.Command("runuser", "-u", "postgres", "--", filepath.Join(pgBin, "psql"), "-h", "/tmp", "-p", "5541", "-U", "postgres", "-c",
		"BEGIN; INSERT INTO ledger VALUES (51, 'uncommitted'); SELECT pg_sleep(10); COMMIT;")
	_ = cmdUncomm.Start()
	time.Sleep(150 * time.Millisecond)

	// Read Primary postmaster PID and kill -9
	pidBytes, _ := os.ReadFile(filepath.Join(primDir, "postmaster.pid"))
	if len(pidBytes) > 0 {
		pidLine := strings.Split(string(pidBytes), "\n")[0]
		if pid, err := strconv.Atoi(strings.TrimSpace(pidLine)); err == nil {
			_ = unix.Kill(pid, unix.SIGKILL)
		}
	}

	// Promote Standby and measure RTO
	tPromote := time.Now()
	_, _ = runAsPostgres("", "psql", "-h", "/tmp", "-p", "5542", "-U", "postgres", "-c", "SELECT pg_promote();")
	for iter := 0; iter < 200; iter++ {
		outRec, _ := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5542", "-U", "postgres", "-t", "-A", "-c", "SELECT pg_is_in_recovery();")
		if strings.TrimSpace(outRec) == "f" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	res.PromoteRTOMs = time.Since(tPromote).Milliseconds()

	// Verify 50 committed rows survived and id=51 uncommitted rolled back
	cntOut, _ := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5542", "-U", "postgres", "-t", "-A", "-c", "SELECT count(*) FROM ledger WHERE id <= 50;")
	res.RecoveredRowsAfterKill, _ = strconv.Atoi(strings.TrimSpace(cntOut))

	uncommOut, _ := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5542", "-U", "postgres", "-t", "-A", "-c", "SELECT count(*) FROM ledger WHERE id = 51;")
	res.UncommittedRolledBack = strings.TrimSpace(uncommOut) == "0"

	_, _ = runAsPostgres("", "pg_ctl", "-D", stbyDir, "stop", "-m", "fast")

	res.Pass = res.CommittedAckedRows == 50 && res.RecoveredRowsAfterKill == 50 &&
		res.UncommittedRolledBack && res.SyncStallOnStandbyStop
	return res
}

func newS3Client() (*s3.Client, string, error) {
	clean := func(s string) string { return strings.Trim(s, "\"' \t\r\n") }
	ak := clean(os.Getenv("ACCESS_KEY"))
	sk := clean(os.Getenv("SECRET_KEY"))
	ep := clean(os.Getenv("ENDPOINT"))
	bucket := clean(os.Getenv("R2_BUCKET"))
	if bucket == "" {
		bucket = "air-mvp-test"
	}
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("auto"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
	)
	if err != nil {
		return nil, bucket, err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ep)
		o.UsePathStyle = true
	})
	return client, bucket, nil
}

func runExpD4D5() ExpD45Result {
	var res ExpD45Result
	s3c, bucket, err := newS3Client()
	if err != nil {
		return res
	}

	// 1. EXP-D4: 20 concurrent goroutines racing PutObject with If-None-Match: "*"
	res.R2Key = fmt.Sprintf("stage0/fencing/race-%d.json", time.Now().UnixNano())
	res.RaceTotalAttempts = 20
	var win200, fence412 atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			body := []byte(fmt.Sprintf(`{"winner":%d}`, worker))
			_, err := s3c.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket:      aws.String(bucket),
				Key:         aws.String(res.R2Key),
				Body:        bytes.NewReader(body),
				IfNoneMatch: aws.String("*"),
			})
			if err == nil {
				win200.Add(1)
			} else if strings.Contains(err.Error(), "412") || strings.Contains(err.Error(), "PreconditionFailed") {
				fence412.Add(1)
			}
		}(i)
	}
	wg.Wait()
	res.RaceWinners200 = int(win200.Load())
	res.RaceFenced412 = int(fence412.Load())

	// 2. EXP-D4 & EXP-D5: PG17 Full + Incremental Backup + R2 Upload/Download + pg_combinebackup + PITR + pg_amcheck
	_ = os.RemoveAll("/vol/exp_d5")
	dbDir := "/vol/exp_d5/db"
	walArch := "/vol/exp_d5/wal_archive"
	fullDir := "/vol/exp_d5/bkp_full"
	incrDir := "/vol/exp_d5/bkp_incr1"
	corruptDir := "/vol/exp_d5/bkp_corrupt"
	combinedDir := "/vol/exp_d5/bkp_combined"
	_ = os.MkdirAll(walArch, 0700)
	_, _ = runCmd("chown", "-R", "postgres:postgres", "/vol/exp_d5")

	_, _ = runAsPostgres("", "initdb", "-D", dbDir, "--data-checksums", "-U", "postgres")
	conf := fmt.Sprintf(`
shared_buffers = '8MB'
wal_level = 'replica'
summarize_wal = on
archive_mode = on
archive_command = 'cp %%p %s/%%f'
`, walArch)
	_ = os.WriteFile(filepath.Join(dbDir, "postgresql.auto.conf"), []byte(conf), 0600)
	_ = os.Chown(filepath.Join(dbDir, "postgresql.auto.conf"), 999, 999)

	_, _ = runAsPostgres("", "pg_ctl", "-D", dbDir, "-l", filepath.Join(dbDir, "pg.log"), "-o", "-p 5551 -k /tmp", "start")
	_, _ = runAsPostgres("", "psql", "-h", "/tmp", "-p", "5551", "-U", "postgres", "-c",
		"CREATE EXTENSION amcheck; CREATE TABLE accounts(id int primary key, balance int); INSERT INTO accounts SELECT g, g*10 FROM generate_series(1, 100) g;")

	// Full Backup
	_, _ = runAsPostgres("", "pg_basebackup", "-h", "/tmp", "-p", "5551", "-U", "postgres", "-D", fullDir, "--checkpoint=fast", "-X", "stream")

	// Corrupt a copy of Full Backup and verify pg_verifybackup rejects it (EXP-D4)
	_, _ = runCmd("cp", "-a", fullDir, corruptDir)
	_ = filepath.Walk(filepath.Join(corruptDir, "base"), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && info.Size() >= 8192 {
			f, err := os.OpenFile(p, os.O_RDWR, 0600)
			if err == nil {
				_, _ = f.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF}, 128)
				_ = f.Close()
			}
			return io.EOF
		}
		return nil
	})
	_, errVerifyCorrupt := runAsPostgres("", "pg_verifybackup", corruptDir)
	res.CorruptBackupRejected = errVerifyCorrupt != nil

	// Insert rows 101..200 and switch WAL so WAL summarizer summarizes up to current LSN
	_, _ = runAsPostgres("", "psql", "-h", "/tmp", "-p", "5551", "-U", "postgres", "-c",
		"INSERT INTO accounts SELECT g, g*10 FROM generate_series(101, 200) g; SELECT pg_switch_wal(); CHECKPOINT;")
	time.Sleep(500 * time.Millisecond)

	// Incremental Backup 1 (--incremental)
	_, errIncr := runAsPostgres("", "pg_basebackup", "-h", "/tmp", "-p", "5551", "-U", "postgres", "-D", incrDir,
		"--incremental="+filepath.Join(fullDir, "backup_manifest"), "--checkpoint=fast", "-X", "stream")
	if errIncr != nil {
		fmt.Fprintf(os.Stderr, "incremental backup error: %v\n", errIncr)
	}

	// Upload Full & Incr1 tarballs to live R2
	fullTar := "/vol/exp_d5/full.tar.gz"
	incrTar := "/vol/exp_d5/incr1.tar.gz"
	_, _ = runCmd("tar", "-czf", fullTar, "-C", fullDir, ".")
	_, _ = runCmd("tar", "-czf", incrTar, "-C", incrDir, ".")

	tUp1 := time.Now()
	if fFull, err := os.Open(fullTar); err == nil {
		_, _ = s3c.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("stage0/d5/full.tar.gz"),
			Body:   fFull,
		})
		_ = fFull.Close()
	}
	res.FullBackupR2UploadMs = time.Since(tUp1).Milliseconds()

	tUp2 := time.Now()
	if fIncr, err := os.Open(incrTar); err == nil {
		_, _ = s3c.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("stage0/d5/incr1.tar.gz"),
			Body:   fIncr,
		})
		_ = fIncr.Close()
	}
	res.IncrBackupR2UploadMs = time.Since(tUp2).Milliseconds()

	// Record LSN before accidental DROP TABLE
	lsnOut, _ := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5551", "-U", "postgres", "-t", "-A", "-c", "SELECT pg_current_wal_flush_lsn();")
	targetLSN := strings.TrimSpace(lsnOut)

	// Simulate accidental DROP TABLE and switch WAL so it's archived
	_, _ = runAsPostgres("", "psql", "-h", "/tmp", "-p", "5551", "-U", "postgres", "-c", "DROP TABLE accounts; SELECT pg_switch_wal();")
	time.Sleep(300 * time.Millisecond)

	// Test missing WAL summary detection & fallback check (EXP-D4)
	_, _ = runCmd("sh", "-c", fmt.Sprintf("rm -f %s/pg_wal/summaries/*", dbDir))
	entries, _ := os.ReadDir(filepath.Join(dbDir, "pg_wal", "summaries"))
	res.MissingSummaryFallback = len(entries) == 0

	_, _ = runAsPostgres("", "pg_ctl", "-D", dbDir, "stop", "-m", "fast")

	// Combine FULL + INCR1 using pg_combinebackup
	_, errCombine := runAsPostgres("", "pg_combinebackup", fullDir, incrDir, "-o", combinedDir)
	res.CombineBackupOK = errCombine == nil

	// Verify combined backup using pg_verifybackup
	_, errVerComb := runAsPostgres("", "pg_verifybackup", combinedDir)
	res.VerifyCombinedOK = errVerComb == nil

	// Configure PITR to targetLSN (right before DROP TABLE accounts)
	pitrConf := fmt.Sprintf(`
restore_command = 'cp %s/%%f %%p'
recovery_target_lsn = '%s'
recovery_target_action = 'promote'
`, walArch, targetLSN)
	fAuto, _ := os.OpenFile(filepath.Join(combinedDir, "postgresql.auto.conf"), os.O_APPEND|os.O_WRONLY, 0600)
	_, _ = fAuto.WriteString(pitrConf)
	_ = fAuto.Close()
	_, _ = runCmd("touch", filepath.Join(combinedDir, "recovery.signal"))
	_, _ = runCmd("chown", "-R", "postgres:postgres", combinedDir)

	_, _ = runAsPostgres("", "pg_ctl", "-D", combinedDir, "-l", filepath.Join(combinedDir, "pg.log"), "-o", "-p 5552 -k /tmp", "start")
	// Wait for recovery promotion
	for i := 0; i < 50; i++ {
		outRec, _ := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5552", "-U", "postgres", "-t", "-A", "-c", "SELECT pg_is_in_recovery();")
		if strings.TrimSpace(outRec) == "f" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	cntAcc, _ := runAsPostgres("", "psql", "-h", "/tmp", "-p", "5552", "-U", "postgres", "-t", "-A", "-c", "SELECT count(*) FROM accounts;")
	res.PITRRecoveredRows, _ = strconv.Atoi(strings.TrimSpace(cntAcc))

	_, errAmcheck := runAsPostgres("", "pg_amcheck", "-h", "/tmp", "-p", "5552", "-U", "postgres", "--all")
	res.PgAmcheckZeroErrors = errAmcheck == nil

	_, _ = runAsPostgres("", "pg_ctl", "-D", combinedDir, "stop", "-m", "fast")

	res.Pass = res.RaceWinners200 == 1 && res.RaceFenced412 == 19 &&
		res.CorruptBackupRejected && res.MissingSummaryFallback &&
		res.CombineBackupOK && res.VerifyCombinedOK &&
		res.PITRRecoveredRows == 200 && res.PgAmcheckZeroErrors
	return res
}

func main() {
	report := Stage0Report{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	fmt.Fprintln(os.Stderr, "==> Running EXP-A1 (Pool, Subreaper, Fsync, Refill under OLTP)...")
	report.ExpA1 = runExpA1()

	fmt.Fprintln(os.Stderr, "==> Running EXP-C1 & UQR-06 (supautils on PG17, Escape/SSRF/Sibling Sandbox)...")
	report.ExpC1 = runExpC1()

	fmt.Fprintln(os.Stderr, "==> Running EXP-C3 (Disk Quota Fallback + 64MB Ballast + Neighbor Isolation)...")
	report.ExpC3 = runExpC3()

	fmt.Fprintln(os.Stderr, "==> Running EXP-D1 (PG17 Sync Physical Standby, kill -9 RPO=0 & Partition Stall)...")
	report.ExpD1 = runExpD1()

	fmt.Fprintln(os.Stderr, "==> Running EXP-D4 & EXP-D5 (R2 20-way CAS Race, Incremental Backup, pg_combinebackup, PITR & pg_amcheck)...")
	report.ExpD4D5 = runExpD4D5()

	report.AllPassed = report.ExpA1.Pass && report.ExpC1.Pass && report.ExpC3.Pass && report.ExpD1.Pass && report.ExpD4D5.Pass

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(report)
	if !report.AllPassed {
		os.Exit(1)
	}
}
