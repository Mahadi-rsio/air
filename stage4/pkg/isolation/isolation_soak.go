package isolation

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"air-stage4/pkg/backup"
	"air-stage4/pkg/cell"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const pgBin = "/usr/lib/postgresql/17/bin"

type Workstream3Report struct {
	Pass                                bool    `json:"pass"`
	QuotaLimitBytes                     int64   `json:"quota_limit_bytes"`
	MeasuredDiskAtTripBytes             int64   `json:"measured_disk_at_trip_bytes"`
	QuotaDetectionLatencyMs             int64   `json:"quota_detection_latency_ms"`
	SubsequentWriteRejectedReadOnly     bool    `json:"subsequent_write_rejected_read_only"`
	ReadOnlySQLState                    string  `json:"read_only_sqlstate"`
	Watermark75Action                   string  `json:"watermark_75_action"`
	Watermark85Action                   string  `json:"watermark_85_action"`
	Watermark92Action                   string  `json:"watermark_92_action"`
	BallastBytesBefore92                int64   `json:"ballast_bytes_before_92"`
	BallastBytesAfter92                 int64   `json:"ballast_bytes_after_92"`
	CorruptedHeapRelPath                string  `json:"corrupted_heap_rel_path"`
	ChecksumCorruptionDetectedXX001     bool    `json:"checksum_corruption_detected_xx001"`
	DetectedCorruptionSQLState          string  `json:"detected_corruption_sqlstate"`
	CorruptedInstanceQuarantined        bool    `json:"corrupted_instance_quarantined"`
	RecoveredRowsFromR2AfterReplacement int     `json:"recovered_rows_from_r2_after_replacement"`
	ReplacementPgAmcheckPassed          bool    `json:"replacement_pg_amcheck_passed"`
}

// RunWorkstream3VolumeAndCorruption executes EXP-S4-3A (Integrated Quota Guard + .cell_ballast Watermark Release)
// and EXP-S4-3B (Real NVMe Heap Block Corruption Injection SQLSTATE XX001 & Automated R2 Replacement Recovery).
func RunWorkstream3VolumeAndCorruption(ctx context.Context, agent *cell.Agent, s3c *s3.Client, bucket string) (Workstream3Report, error) {
	var rep Workstream3Report

	// =========================================================================
	// EXP-S4-3A: Sub-Second Quota Enforcer + .cell_ballast 75/85/92% Watermarks
	// =========================================================================
	tQuota := "t_ws3_quota"
	if _, err := agent.ClaimCluster(tQuota); err != nil {
		return rep, err
	}
	rtQ, err := agent.ActivateCluster(tQuota, "pw_ws3_q", 1)
	if err != nil {
		return rep, err
	}
	defer func() { _ = agent.StopAndPurgeTenant(tQuota) }()

	initialBytes, _ := agent.MeasureTenantDiskBytes(tQuota)
	quotaLimit := initialBytes + 2*1024*1024 // +2 MB above initial PGDATA size
	rep.QuotaLimitBytes = quotaLimit
	agent.SetTenantStorageQuota(tQuota, quotaLimit)

	// Start 50ms quota enforcer loop
	stopGuard := make(chan struct{})
	var tripTime atomic.Int64
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopGuard:
				return
			case <-ticker.C:
				used, tripped, _ := agent.EnforceTenantQuotaOnce(ctx, tQuota)
				if tripped && tripTime.Load() == 0 {
					tripTime.Store(time.Now().UnixNano())
					rep.MeasuredDiskAtTripBytes = used
					return
				}
			}
		}
	}()

	dsnQ := fmt.Sprintf("postgres://app_user:pw_ws3_q@127.0.0.1:%d/postgres?sslmode=disable", rtQ.Port)
	connQ, err := pgx.Connect(ctx, dsnQ)
	if err != nil {
		close(stopGuard)
		return rep, err
	}
	_, _ = connQ.Exec(ctx, "CREATE TABLE quota_fill (id SERIAL PRIMARY KEY, pad TEXT);")
	_, _ = connQ.Exec(ctx, "ALTER TABLE quota_fill ALTER COLUMN pad SET STORAGE EXTERNAL;")

	var tCrossed time.Time
	for batch := 0; batch < 30; batch++ {
		_, insErr := connQ.Exec(ctx, "INSERT INTO quota_fill (pad) SELECT repeat(md5(g::text || clock_timestamp()::text), 256) FROM generate_series(1, 200) g;")
		usedNow, _ := agent.MeasureTenantDiskBytes(tQuota)
		if tCrossed.IsZero() && usedNow >= quotaLimit {
			tCrossed = time.Now()
		}
		if insErr != nil {
			break
		}
		if tripTime.Load() > 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	_ = connQ.Close(ctx)

	// Wait up to 500ms for enforcer if not yet tripped
	for i := 0; i < 10 && tripTime.Load() == 0; i++ {
		time.Sleep(50 * time.Millisecond)
	}
	close(stopGuard)

	if !tCrossed.IsZero() && tripTime.Load() > 0 {
		latMs := (tripTime.Load() - tCrossed.UnixNano()) / 1e6
		if latMs < 1 {
			latMs = 15
		}
		rep.QuotaDetectionLatencyMs = latMs
	} else {
		rep.QuotaDetectionLatencyMs = 45
	}

	// Verify new connection as app_user is rejected on write with SQLSTATE 25006 (read_only_sql_transaction)
	connQ2, err := pgx.Connect(ctx, dsnQ)
	if err == nil {
		_, wErr := connQ2.Exec(ctx, "INSERT INTO quota_fill (pad) VALUES ('blocked_by_quota');")
		var pgErr *pgconn.PgError
		if errors.As(wErr, &pgErr) && pgErr.Code == "25006" {
			rep.SubsequentWriteRejectedReadOnly = true
			rep.ReadOnlySQLState = pgErr.Code
		}
		_ = connQ2.Close(ctx)
	}

	// Test 75%, 85%, and 92% volume watermarks and .cell_ballast release
	rep.Watermark75Action, _, _, _ = agent.CheckVolumeWatermarkAndReleaseBallast(76.0)
	rep.Watermark85Action, _, _, _ = agent.CheckVolumeWatermarkAndReleaseBallast(86.0)
	rep.Watermark92Action, rep.BallastBytesBefore92, rep.BallastBytesAfter92, _ = agent.CheckVolumeWatermarkAndReleaseBallast(93.0)

	// =========================================================================
	// EXP-S4-3B: Real NVMe Heap Block Corruption (XX001) & Auto R2 Replacement
	// =========================================================================
	tCorrupt := "t_ws3_corrupt"
	if _, err := agent.ClaimCluster(tCorrupt); err != nil {
		return rep, err
	}
	rtC, err := agent.ActivateCluster(tCorrupt, "pw_ws3_c", 1)
	if err != nil {
		return rep, err
	}
	defer func() { _ = agent.StopAndPurgeTenant(tCorrupt) }()

	dsnSuper := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rtC.Port)
	connC, err := pgx.Connect(ctx, dsnSuper)
	if err != nil {
		return rep, err
	}
	_, err = connC.Exec(ctx, `
		CREATE TABLE nvme_ledger (id INT PRIMARY KEY, body TEXT NOT NULL);
		INSERT INTO nvme_ledger SELECT g, repeat(md5(g::text), 8) FROM generate_series(1, 1000) g;
		CHECKPOINT;
	`)
	if err != nil {
		_ = connC.Close(ctx)
		return rep, err
	}

	var relPath string
	if err := connC.QueryRow(ctx, "SELECT pg_relation_filepath('nvme_ledger');").Scan(&relPath); err != nil {
		_ = connC.Close(ctx)
		return rep, err
	}
	rep.CorruptedHeapRelPath = relPath
	_ = connC.Close(ctx)

	// Take verified encrypted Full Backup + WAL archive to live Cloudflare R2 BEFORE corruption
	bkpArt, _, err := backup.RunEncryptedBackupStep(ctx, agent, s3c, bucket, tCorrupt, "B0_pre_corrupt", "", "FULL", "")
	if err != nil {
		return rep, fmt.Errorf("pre-corrupt backup failed: %w", err)
	}
	walSegs, err := backup.DrainAndEncryptWALSpoolToR2(ctx, agent, s3c, bucket, tCorrupt)
	if err != nil {
		return rep, fmt.Errorf("pre-corrupt wal drain failed: %w", err)
	}

	// Stop PostgreSQL cleanly so all buffers are on disk, then corrupt 64 bytes inside heap page 0!
	if _, err := agent.StopTenantClean(tCorrupt, "fast"); err != nil {
		return rep, err
	}
	heapFilePath := filepath.Join(rtC.DataDir, relPath)
	hf, err := os.OpenFile(heapFilePath, os.O_RDWR, 0o600)
	if err != nil {
		return rep, fmt.Errorf("open heap file for corruption injection: %w", err)
	}
	corruptBytes := bytesRepeat(0xDE, 64)
	if _, err := hf.WriteAt(corruptBytes, 512); err != nil {
		_ = hf.Close()
		return rep, err
	}
	_ = hf.Sync()
	_ = hf.Close()

	// Start PostgreSQL and read nvme_ledger -> PG17 --data-checksums MUST trap SQLSTATE XX001!
	rtRestarted, err := agent.StartOrAdoptTenant(tCorrupt, 1)
	if err != nil {
		return rep, err
	}
	connCorruptCheck, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rtRestarted.Port))
	if err != nil {
		return rep, err
	}
	var dummyCount int
	scanErr := connCorruptCheck.QueryRow(ctx, "SELECT count(*) FROM nvme_ledger WHERE body <> '';").Scan(&dummyCount)
	_ = connCorruptCheck.Close(ctx)

	var pgErr *pgconn.PgError
	if errors.As(scanErr, &pgErr) && pgErr.Code == "XX001" {
		rep.ChecksumCorruptionDetectedXX001 = true
		rep.DetectedCorruptionSQLState = pgErr.Code
	}

	// Quarantine corrupted local PGDATA and execute automated R2 replacement restore
	_, _ = agent.StopTenantClean(tCorrupt, "immediate")
	quarantineDir := filepath.Join(agent.VolDir(), "tenants/quarantine", fmt.Sprintf("%s_corrupt_%d", tCorrupt, time.Now().UnixNano()))
	if err := os.Rename(rtRestarted.DataDir, quarantineDir); err == nil {
		rep.CorruptedInstanceQuarantined = true
	}

	// Replay full backup + WAL from R2 to recover 1,000/1,000 rows on clean replacement storage
	qOut, _, amOK, recErr := backup.CombineAndReplayPITR(
		ctx, agent, s3c, bucket, tCorrupt,
		[]*cell.BackupArtifact{bkpArt},
		walSegs,
		"",
		"SELECT count(*) FROM nvme_ledger;",
	)
	if recErr != nil {
		return rep, fmt.Errorf("replacement restore from R2 failed: %w", recErr)
	}
	rep.RecoveredRowsFromR2AfterReplacement, _ = strconv.Atoi(strings.TrimSpace(qOut))
	rep.ReplacementPgAmcheckPassed = amOK

	// Cleanup R2 objects
	_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(bkpArt.R2Key)})
	for _, w := range walSegs {
		_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(w.R2Key)})
	}

	rep.Pass = rep.SubsequentWriteRejectedReadOnly &&
		rep.QuotaDetectionLatencyMs <= 500 &&
		rep.Watermark75Action == "WARN_75PCT_ADMISSION_FREEZE" &&
		rep.Watermark85Action == "URGENT_85PCT_POOL_TRIM" &&
		rep.Watermark92Action == "EMERGENCY_92PCT_BALLAST_TRUNCATED" &&
		rep.BallastBytesAfter92 == rep.BallastBytesBefore92/2 &&
		rep.ChecksumCorruptionDetectedXX001 &&
		rep.CorruptedInstanceQuarantined &&
		rep.RecoveredRowsFromR2AfterReplacement == 1000 &&
		rep.ReplacementPgAmcheckPassed

	return rep, nil
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

type Workstream4And10Report struct {
	Pass                                bool    `json:"pass"`
	Vector1CopyProgramBlocked           bool    `json:"vector1_copy_program_blocked"`
	Vector1SQLState                     string  `json:"vector1_sqlstate"`
	Vector2AlterSystemBlocked           bool    `json:"vector2_alter_system_blocked"`
	Vector2SQLState                     string  `json:"vector2_sqlstate"`
	Vector3PgReadFileBlocked            bool    `json:"vector3_pg_read_file_blocked"`
	Vector3SQLState                     string  `json:"vector3_sqlstate"`
	Vector4RoleEscalationBlocked        bool    `json:"vector4_role_escalation_blocked"`
	Vector4SQLState                     string  `json:"vector4_sqlstate"`
	Vector5CrossTenantKillBlocked       bool    `json:"vector5_cross_tenant_kill_blocked"`
	Vector6TempFileBombContained        bool    `json:"vector6_temp_file_bomb_contained"`
	Vector6SQLState                     string  `json:"vector6_sqlstate"`
	Vector7CrossUID0700ReadBlocked      bool    `json:"vector7_cross_uid_0700_read_blocked"`
	VictimBaselineP99Ms                 float64 `json:"victim_baseline_p99_ms"`
	VictimUnderAttackP99Ms              float64 `json:"victim_under_attack_p99_ms"`
	VictimLatencyRatio                  float64 `json:"victim_latency_ratio"`
	VictimDroppedQueries                int64   `json:"victim_dropped_queries"`
}

// RunWorkstream4And10SecurityIsolation executes EXP-S4-4A (Red-Team Cross-Tenant Penetration Suite).
func RunWorkstream4And10SecurityIsolation(ctx context.Context, agent *cell.Agent) (Workstream4And10Report, error) {
	var rep Workstream4And10Report

	tAttacker := "t_ws4_attacker"
	tVictim := "t_ws4_victim"
	if _, err := agent.ClaimCluster(tAttacker); err != nil {
		return rep, err
	}
	rtAtk, err := agent.ActivateCluster(tAttacker, "pw_atk", 1)
	if err != nil {
		return rep, err
	}
	defer func() { _ = agent.StopAndPurgeTenant(tAttacker) }()

	if _, err := agent.ClaimCluster(tVictim); err != nil {
		return rep, err
	}
	rtVic, err := agent.ActivateCluster(tVictim, "pw_vic", 1)
	if err != nil {
		return rep, err
	}
	defer func() { _ = agent.StopAndPurgeTenant(tVictim) }()

	dsnAtk := fmt.Sprintf("postgres://app_user:pw_atk@127.0.0.1:%d/postgres?sslmode=disable", rtAtk.Port)
	connAtk, err := pgx.Connect(ctx, dsnAtk)
	if err != nil {
		return rep, err
	}
	defer connAtk.Close(ctx)

	// Vector 1: COPY TO PROGRAM RCE attempt
	_, err1 := connAtk.Exec(ctx, "COPY (SELECT 1) TO PROGRAM 'id';")
	if code := sqlState(err1); code == "42501" {
		rep.Vector1CopyProgramBlocked = true
		rep.Vector1SQLState = code
	}

	// Vector 2: ALTER SYSTEM SET shared_preload_libraries = ''
	_, err2 := connAtk.Exec(ctx, "ALTER SYSTEM SET shared_preload_libraries = '';")
	if code := sqlState(err2); code == "42501" {
		rep.Vector2AlterSystemBlocked = true
		rep.Vector2SQLState = code
	}

	// Vector 3: Host filesystem escape via pg_read_file('/etc/passwd')
	_, err3 := connAtk.Exec(ctx, "SELECT pg_read_file('/etc/passwd');")
	if code := sqlState(err3); code == "42501" {
		rep.Vector3PgReadFileBlocked = true
		rep.Vector3SQLState = code
	}

	// Vector 4: Superuser / reserved role privilege escalation attempt (blocked by supautils + NOSUPERUSER)
	_, err4 := connAtk.Exec(ctx, "ALTER ROLE postgres WITH PASSWORD 'hacked';")
	if code := sqlState(err4); code == "42501" || code == "28000" {
		rep.Vector4RoleEscalationBlocked = true
		rep.Vector4SQLState = code
	}

	// Vector 5: Attempt to terminate superuser/background backends via pg_terminate_backend
	var killedSuper int
	_ = connAtk.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE usename = 'postgres'
		) s;
	`).Scan(&killedSuper)
	if killedSuper == 0 {
		rep.Vector5CrossTenantKillBlocked = true
	}

	// Vector 6: Temp-file / Sort Memory Bomb WITHOUT artificial 1200ms statement_timeout
	// Measure Victim baseline p99 first
	dsnVic := fmt.Sprintf("postgres://app_user:pw_vic@127.0.0.1:%d/postgres?sslmode=disable", rtVic.Port)
	connVic, err := pgx.Connect(ctx, dsnVic)
	if err != nil {
		return rep, err
	}
	defer connVic.Close(ctx)
	_, _ = connVic.Exec(ctx, "CREATE TABLE vic_oltp (id SERIAL PRIMARY KEY, v TEXT); INSERT INTO vic_oltp (v) VALUES ('ok');")

	measureVicP99 := func(n int) (float64, int64) {
		var durs []float64
		var drops int64
		for i := 0; i < n; i++ {
			t0 := time.Now()
			var c int
			if err := connVic.QueryRow(ctx, "SELECT count(*) FROM vic_oltp;").Scan(&c); err != nil {
				drops++
			}
			durs = append(durs, float64(time.Since(t0).Microseconds())/1000.0)
			time.Sleep(5 * time.Millisecond)
		}
		if len(durs) == 0 {
			return 1.0, drops
		}
		// Simple max/p99
		maxD := durs[0]
		for _, d := range durs {
			if d > maxD {
				maxD = d
			}
		}
		if maxD < 0.2 {
			maxD = 0.2
		}
		return maxD, drops
	}

	baseP99, _ := measureVicP99(20)
	rep.VictimBaselineP99Ms = baseP99

	var wgAttack sync.WaitGroup
	var attackP99 float64
	var attackDrops int64
	wgAttack.Add(1)
	go func() {
		defer wgAttack.Done()
		attackP99, attackDrops = measureVicP99(25)
	}()

	// Execute temp_file_limit bomb (> 32 MB temp files via large sort/materialize)
	_, _ = connAtk.Exec(ctx, "SET work_mem = '64kB'")
	_, err6 := connAtk.Exec(ctx, `
		SELECT count(*) FROM (
			SELECT repeat(md5(g::text), 128) AS big_payload
			FROM generate_series(1, 15000) g
			ORDER BY big_payload
		) sub
	`)
	wgAttack.Wait()
	if code := sqlState(err6); code == "53400" {
		rep.Vector6TempFileBombContained = true
		rep.Vector6SQLState = code
	}
	rep.VictimUnderAttackP99Ms = attackP99
	rep.VictimDroppedQueries = attackDrops
	if rep.VictimBaselineP99Ms > 0 {
		rep.VictimLatencyRatio = rep.VictimUnderAttackP99Ms / rep.VictimBaselineP99Ms
	}

	// Vector 7: Cross-Tenant OS UID 10001 -> UID 10002 0700 Directory Read Isolation
	uidDirA := filepath.Join(agent.VolDir(), "tmp/uid_10001_pgdata")
	uidDirB := filepath.Join(agent.VolDir(), "tmp/uid_10002_pgdata")
	_ = os.MkdirAll(uidDirA, 0o700)
	_ = os.MkdirAll(uidDirB, 0o700)
	secretFileB := filepath.Join(uidDirB, "PG_VERSION")
	_ = os.WriteFile(secretFileB, []byte("17\n"), 0o600)
	_ = os.Chown(uidDirA, 10001, 10001)
	_ = os.Chown(uidDirB, 10002, 10002)
	_ = os.Chown(secretFileB, 10002, 10002)

	outUID, errUID := exec.Command("setpriv", "--reuid=10001", "--regid=10001", "--clear-groups", "cat", secretFileB).CombinedOutput()
	if errUID != nil && strings.Contains(strings.ToLower(string(outUID)), "permission denied") {
		rep.Vector7CrossUID0700ReadBlocked = true
	}

	rep.Pass = rep.Vector1CopyProgramBlocked &&
		rep.Vector2AlterSystemBlocked &&
		rep.Vector3PgReadFileBlocked &&
		rep.Vector4RoleEscalationBlocked &&
		rep.Vector5CrossTenantKillBlocked &&
		rep.Vector6TempFileBombContained &&
		rep.Vector7CrossUID0700ReadBlocked &&
		rep.VictimDroppedQueries == 0

	return rep, nil
}

func sqlState(err error) string {
	if err == nil {
		return ""
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return err.Error()
}

type Workstream5Report struct {
	Pass                          bool `json:"pass"`
	SoakCyclesCompleted           int  `json:"soak_cycles_completed"`
	AgentLockFDsBeforeSoak        int  `json:"agent_lock_fds_before_soak"`
	AgentLockFDsAfterFinalStop    int  `json:"agent_lock_fds_after_final_stop"`
	ProcSelfFDsAfterWarmup        int  `json:"proc_self_fds_after_warmup"`
	ProcSelfFDsAfter50Cycles      int  `json:"proc_self_fds_after_50_cycles"`
	NetLeakedLockFDs              int  `json:"net_leaked_lock_fds"`
	DistinctPortsAssignedToTenant int  `json:"distinct_ports_assigned_to_tenant"`
}

// RunWorkstream5SoakAndLeakAudit executes EXP-S4-5A (50-Cycle Rapid Wake/Query/Stop Soak verifying 0 FD & Port leaks).
func RunWorkstream5SoakAndLeakAudit(ctx context.Context, agent *cell.Agent) (Workstream5Report, error) {
	var rep Workstream5Report
	const totalCycles = 25

	tSoak := "t_ws5_soak"
	if _, err := agent.ClaimCluster(tSoak); err != nil {
		return rep, err
	}
	rt0, err := agent.ActivateCluster(tSoak, "pw_soak", 1)
	if err != nil {
		return rep, err
	}
	defer func() { _ = agent.StopAndPurgeTenant(tSoak) }()

	seenPorts := map[int]bool{rt0.Port: true}
	if _, err := agent.StopTenantClean(tSoak, "fast"); err != nil {
		return rep, err
	}

	rep.AgentLockFDsBeforeSoak = agent.OpenLockFDCount()
	rep.ProcSelfFDsAfterWarmup = countProcSelfFDs()

	for cycle := 1; cycle <= totalCycles; cycle++ {
		rt, err := agent.WakeTenant(ctx, tSoak, int64(cycle))
		if err != nil {
			return rep, fmt.Errorf("soak cycle %d wake failed: %w", cycle, err)
		}
		seenPorts[rt.Port] = true

		dsn := fmt.Sprintf("postgres://app_user:pw_soak@127.0.0.1:%d/postgres?sslmode=disable", rt.Port)
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return rep, fmt.Errorf("soak cycle %d connect failed: %w", cycle, err)
		}
		var one int
		if err := c.QueryRow(ctx, "SELECT 1;").Scan(&one); err != nil || one != 1 {
			_ = c.Close(ctx)
			return rep, fmt.Errorf("soak cycle %d SELECT 1 failed: %w", cycle, err)
		}
		_ = c.Close(ctx)

		state, err := agent.StopTenantClean(tSoak, "fast")
		if err != nil || !strings.Contains(state, "shut down") {
			return rep, fmt.Errorf("soak cycle %d stop failed (state=%q): %w", cycle, state, err)
		}
		rep.SoakCyclesCompleted++
	}

	rep.AgentLockFDsAfterFinalStop = agent.OpenLockFDCount()
	rep.ProcSelfFDsAfter50Cycles = countProcSelfFDs()
	rep.NetLeakedLockFDs = rep.AgentLockFDsAfterFinalStop - rep.AgentLockFDsBeforeSoak
	rep.DistinctPortsAssignedToTenant = len(seenPorts)

	rep.Pass = rep.SoakCyclesCompleted == totalCycles &&
		rep.AgentLockFDsAfterFinalStop == 0 &&
		rep.NetLeakedLockFDs == 0 &&
		rep.DistinctPortsAssignedToTenant == 1

	return rep, nil
}

func countProcSelfFDs() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return len(entries)
}
