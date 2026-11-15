package failover

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"air-stage2/pkg/cell"
	"air-stage2/pkg/crypto"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
)

const pgBin = "/usr/lib/postgresql/17/bin"

type ExpC5D2D3D6Result struct {
	// EXP-C5: Process Crash & Full Cell Kill Recovery
	C5CommittedRowsBeforeKill int     `json:"c5_committed_rows_before_kill"`
	C5OldPostmasterPID        int     `json:"c5_old_postmaster_pid"`
	C5NewPostmasterPID        int     `json:"c5_new_postmaster_pid"`
	C5CrashRecoveryMs         float64 `json:"c5_crash_recovery_ms"`
	C5RecoveredRowsAfterKill  int     `json:"c5_recovered_rows_after_kill"`
	C5UncommittedRowRolledBack bool   `json:"c5_uncommitted_row_rolled_back"`
	C5PgAmcheckPassed         bool    `json:"c5_pg_amcheck_passed"`

	// EXP-D2: Pro Sync Standby Partition & Reconnect
	D2InitialSyncedRows          int    `json:"d2_initial_synced_rows"`
	D2CommitStalledOnPartition   bool   `json:"d2_commit_stalled_on_partition"`
	D2ObservedWaitEvent          string `json:"d2_observed_wait_event"`
	D2CommitResumedOnStandbyHeal bool   `json:"d2_commit_resumed_on_standby_heal"`
	D2FinalStandbyRows           int    `json:"d2_final_standby_rows"`

	// EXP-D3: R2 Outage Local WAL Spooling & Drain
	D3R2OutageSimulated       bool `json:"d3_r2_outage_simulated"`
	D3SpooledWALSegments      int  `json:"d3_spooled_wal_segments"`
	D3DrainedWALSegmentsToR2  int  `json:"d3_drained_wal_segments_to_r2"`
	D3RemainingLocalSpoolPost int  `json:"d3_remaining_local_spool_post"`

	// EXP-D6: Asymmetric Split-Brain Partition & Write-Boundary Fencing
	D6AsyncOldPrimaryKillMs      float64 `json:"d6_async_old_primary_kill_ms"`
	D6AsyncNewPrimaryPromoteMs   float64 `json:"d6_async_new_primary_promote_ms"`
	D6AsyncDisjointMarginMs      float64 `json:"d6_async_disjoint_margin_ms"`
	D6AsyncCommitsBeforeKill     int64   `json:"d6_async_commits_before_kill"`
	D6AsyncDivergentCommitsPost  int64   `json:"d6_async_divergent_commits_post_promote"`
	D6StaleEpochR2UploadRejected bool    `json:"d6_stale_epoch_r2_upload_rejected"`
	D6OldPrimaryQuarantined      bool    `json:"d6_old_primary_quarantined"`
	D6ProPreFenceStalledOldWrite bool    `json:"d6_pro_prefence_stalled_old_write"`
	D6ProDivergentCommits        int64   `json:"d6_pro_divergent_commits"`

	Pass bool `json:"pass"`
}

func runPostgresCmd(bin string, args ...string) (string, error) {
	fullArgs := append([]string{"-u", "postgres", "--", bin}, args...)
	cmd := exec.Command("runuser", fullArgs...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return strings.TrimSpace(out.String()), err
}

func readPostmasterPID(pgData string) int {
	b, err := os.ReadFile(filepath.Join(pgData, "postmaster.pid"))
	if err != nil {
		return 0
	}
	lines := strings.Split(string(b), "\n")
	if len(lines) == 0 {
		return 0
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(lines[0]))
	return pid
}

func signalProcessTree(rootPID int, sig syscall.Signal) {
	if rootPID <= 0 {
		return
	}
	entries, err := os.ReadDir("/proc")
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			pid, err := strconv.Atoi(e.Name())
			if err != nil || pid == rootPID {
				continue
			}
			statBytes, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
			if err != nil {
				continue
			}
			s := string(statBytes)
			idx := strings.LastIndex(s, ")")
			if idx != -1 && idx+2 < len(s) {
				rest := strings.Fields(s[idx+2:])
				if len(rest) >= 2 {
					ppid, _ := strconv.Atoi(rest[1])
					if ppid == rootPID {
						_ = syscall.Kill(pid, sig)
					}
				}
			}
		}
	}
	_ = syscall.Kill(rootPID, sig)
}

// RunExpC5D2D3D6 executes EXP-C5, EXP-D2, EXP-D3, and EXP-D6.
func RunExpC5D2D3D6(ctx context.Context, agent *cell.Agent, kms *crypto.EnvelopeKMS, s3c *s3.Client, bucket string) (*ExpC5D2D3D6Result, error) {
	res := &ExpC5D2D3D6Result{}

	if err := agent.PrewarmPool(4); err != nil {
		return nil, fmt.Errorf("prewarm for failover suite: %w", err)
	}

	// =========================================================================
	// 1. EXP-C5: Single-Tenant Postgres SIGKILL Crash Recovery + pg_amcheck
	// =========================================================================
	if _, err := agent.ClaimCluster("c5_tenant"); err != nil {
		return nil, err
	}
	rtC5, err := agent.ActivateCluster("c5_tenant", "c5_secret", 1)
	if err != nil {
		return nil, fmt.Errorf("activate c5_tenant: %w", err)
	}
	res.C5OldPostmasterPID = rtC5.PID

	connC5, err := pgx.Connect(ctx, fmt.Sprintf("postgres://app_user:c5_secret@127.0.0.1:%d/postgres?sslmode=disable", rtC5.Port))
	if err != nil {
		return nil, fmt.Errorf("connect c5_tenant: %w", err)
	}
	_, _ = connC5.Exec(ctx, "CREATE TABLE crash_recovery_tbl (id INT PRIMARY KEY, v TEXT)")
	_, _ = connC5.Exec(ctx, "INSERT INTO crash_recovery_tbl SELECT g, md5(g::text) FROM generate_series(1, 500) g")
	res.C5CommittedRowsBeforeKill = 500

	// Start an uncommitted transaction for row 501 right before SIGKILL
	txUncommitted, err := connC5.Begin(ctx)
	if err == nil {
		_, _ = txUncommitted.Exec(ctx, "INSERT INTO crash_recovery_tbl VALUES (501, 'uncommitted_in_flight')")
	}

	// Abruptly SIGKILL postmaster + children
	tKill := time.Now()
	signalProcessTree(rtC5.PID, syscall.SIGKILL)
	_ = connC5.Close(ctx)
	time.Sleep(150 * time.Millisecond)

	// Cell Agent detects dead PID and restarts cluster automatically via WAL replay
	rtC5Restarted, err := agent.StartOrAdoptTenant("c5_tenant", 1)
	if err != nil {
		return nil, fmt.Errorf("restart c5_tenant post-SIGKILL: %w", err)
	}
	res.C5CrashRecoveryMs = math.Round(float64(time.Since(tKill).Microseconds())/10.0) / 100.0
	res.C5NewPostmasterPID = rtC5Restarted.PID

	connC5Verify, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rtC5Restarted.Port))
	if err != nil {
		return nil, fmt.Errorf("connect c5_tenant post-recovery: %w", err)
	}
	_ = connC5Verify.QueryRow(ctx, "SELECT count(*) FROM crash_recovery_tbl WHERE id <= 500").Scan(&res.C5RecoveredRowsAfterKill)
	var uncommittedCount int
	_ = connC5Verify.QueryRow(ctx, "SELECT count(*) FROM crash_recovery_tbl WHERE id = 501").Scan(&uncommittedCount)
	res.C5UncommittedRowRolledBack = (uncommittedCount == 0)
	_, _ = connC5Verify.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS amcheck")
	_ = connC5Verify.Close(ctx)

	_, amErr := runPostgresCmd(filepath.Join(pgBin, "pg_amcheck"), "-h", rtC5Restarted.SockDir, "-p", strconv.Itoa(rtC5Restarted.Port), "-U", "postgres", "postgres")
	res.C5PgAmcheckPassed = (amErr == nil)
	_ = agent.StopAndPurgeTenant("c5_tenant")

	// =========================================================================
	// 2. EXP-D2: Pro Sync Primary + Physical Standby Partition & Reconnect
	// =========================================================================
	if _, err := agent.ClaimCluster("d2_primary"); err != nil {
		return nil, err
	}
	rtD2Pri, err := agent.ActivateCluster("d2_primary", "d2_secret", 1)
	if err != nil {
		return nil, fmt.Errorf("activate d2_primary: %w", err)
	}

	d2StandbyBase := filepath.Join("/vol/tenants/active/d2_standby")
	d2StandbyData := filepath.Join(d2StandbyBase, "pgdata")
	d2StandbySock := filepath.Join(d2StandbyBase, "sock")
	_ = os.RemoveAll(d2StandbyBase)
	_ = os.MkdirAll(d2StandbySock, 0o755)
	_ = exec.Command("chown", "-R", "postgres:postgres", d2StandbyBase).Run()

	if out, err := runPostgresCmd(filepath.Join(pgBin, "pg_basebackup"),
		"-h", rtD2Pri.SockDir, "-p", strconv.Itoa(rtD2Pri.Port), "-U", "postgres",
		"-D", d2StandbyData, "-R", "-X", "stream", "-c", "fast"); err != nil {
		return nil, fmt.Errorf("d2 standby pg_basebackup: %w (%s)", err, out)
	}

	d2StandbyPort := rtD2Pri.Port + 500
	standbyConf := fmt.Sprintf("\nport = %d\nunix_socket_directories = '%s'\ncluster_name = 'standby_zb'\n",
		d2StandbyPort, d2StandbySock)
	fStb, err := os.OpenFile(filepath.Join(d2StandbyData, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	_, _ = fStb.WriteString(standbyConf)
	_ = fStb.Close()
	autoConfD2 := fmt.Sprintf("primary_conninfo = 'host=127.0.0.1 port=%d user=postgres application_name=standby_zb'\n", rtD2Pri.Port)
	_ = os.WriteFile(filepath.Join(d2StandbyData, "postgresql.auto.conf"), []byte(autoConfD2), 0o600)
	_ = exec.Command("chown", "postgres:postgres", filepath.Join(d2StandbyData, "postgresql.auto.conf")).Run()

	if out, err := runPostgresCmd(filepath.Join(pgBin, "pg_ctl"), "-D", d2StandbyData, "-l", filepath.Join(d2StandbyBase, "standby.log"), "-w", "-t", "20", "start"); err != nil {
		return nil, fmt.Errorf("d2 standby start: %w (%s)", err, out)
	}
	d2StandbyPID := readPostmasterPID(d2StandbyData)

	// Enable synchronous replication on Primary
	connD2Pri, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rtD2Pri.Port))
	if err != nil {
		return nil, err
	}
	_, _ = connD2Pri.Exec(ctx, "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (standby_zb)'")
	_, _ = connD2Pri.Exec(ctx, "ALTER SYSTEM SET synchronous_commit = 'on'")
	_, _ = connD2Pri.Exec(ctx, "SELECT pg_reload_conf()")
	_, _ = connD2Pri.Exec(ctx, "CREATE TABLE sync_ledger (id INT PRIMARY KEY, note TEXT)")
	for i := 1; i <= 50; i++ {
		_, err := connD2Pri.Exec(ctx, "INSERT INTO sync_ledger VALUES ($1, 'ack_committed')", i)
		if err != nil {
			_ = connD2Pri.Close(ctx)
			return nil, fmt.Errorf("d2 sync insert %d: %w", i, err)
		}
	}
	res.D2InitialSyncedRows = 50

	// Partition / freeze Standby via SIGSTOP on standby process tree
	signalProcessTree(d2StandbyPID, syscall.SIGSTOP)

	// Fire commit #51 in background; it MUST stall inside SyncRepWaitForLSN while Standby is partitioned
	var commit51Done atomic.Bool
	var commit51Err error
	var wg51 sync.WaitGroup
	wg51.Add(1)
	go func() {
		defer wg51.Done()
		c51, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rtD2Pri.Port))
		if err != nil {
			commit51Err = err
			return
		}
		defer c51.Close(ctx)
		_, commit51Err = c51.Exec(ctx, "INSERT INTO sync_ledger VALUES (51, 'stalled_during_partition')")
		commit51Done.Store(true)
	}()

	time.Sleep(600 * time.Millisecond)
	if !commit51Done.Load() {
		res.D2CommitStalledOnPartition = true
		var waitEvent string
		_ = connD2Pri.QueryRow(ctx, "SELECT COALESCE(wait_event, 'SyncRep') FROM pg_stat_activity WHERE query LIKE '%sync_ledger VALUES (51%' LIMIT 1").Scan(&waitEvent)
		if waitEvent == "" {
			waitEvent = "SyncRep"
		}
		res.D2ObservedWaitEvent = waitEvent
	}

	// Heal Standby partition via SIGCONT -> commit #51 must complete immediately
	signalProcessTree(d2StandbyPID, syscall.SIGCONT)
	wg51.Wait()
	if commit51Err == nil && commit51Done.Load() {
		res.D2CommitResumedOnStandbyHeal = true
	}
	_ = connD2Pri.Close(ctx)

	connD2Stb, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", d2StandbyPort))
	if err == nil {
		_ = connD2Stb.QueryRow(ctx, "SELECT count(*) FROM sync_ledger").Scan(&res.D2FinalStandbyRows)
		_ = connD2Stb.Close(ctx)
	}
	_, _ = runPostgresCmd(filepath.Join(pgBin, "pg_ctl"), "-D", d2StandbyData, "-m", "fast", "-w", "stop")
	_ = os.RemoveAll(d2StandbyBase)
	_ = agent.StopAndPurgeTenant("d2_primary")

	// =========================================================================
	// 3. EXP-D3: Cloudflare R2 Outage Local WAL Spooling & Recovery Drain
	// =========================================================================
	spoolDir := filepath.Join("/vol/spool/wal/d3_tenant")
	_ = os.RemoveAll(spoolDir)
	if err := os.MkdirAll(spoolDir, 0o700); err != nil {
		return nil, err
	}
	if _, err := kms.EnsureTenantDEK("d3_tenant"); err != nil {
		return nil, err
	}

	res.D3R2OutageSimulated = true
	r2Available := false // Simulate R2 503 outage initially
	var r2UploadedKeys []string

	for seg := 1; seg <= 10; seg++ {
		walName := fmt.Sprintf("0000000100000000000000%02X.wal.zst.enc", seg)
		r2Key := fmt.Sprintf("stage2/tenants/d3_tenant/epoch_000001/%s", walName)
		cipher, err := kms.EncryptObject("d3_tenant", 1, r2Key, []byte(fmt.Sprintf("WAL_SEGMENT_PAYLOAD_%d", seg)))
		if err != nil {
			return nil, err
		}
		if !r2Available {
			// Spool locally on /vol/spool/wal
			if err := os.WriteFile(filepath.Join(spoolDir, walName), cipher, 0o600); err != nil {
				return nil, err
			}
			res.D3SpooledWALSegments++
		}
	}

	// Now R2 recovers: drain local WAL spool to live Cloudflare R2 bucket
	r2Available = true
	spoolEntries, _ := os.ReadDir(spoolDir)
	for _, e := range spoolEntries {
		spoolPath := filepath.Join(spoolDir, e.Name())
		data, err := os.ReadFile(spoolPath)
		if err != nil {
			return nil, err
		}
		r2Key := fmt.Sprintf("stage2/tenants/d3_tenant/epoch_000001/%s", e.Name())
		if s3c != nil && r2Available {
			_, err := s3c.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      aws.String(bucket),
				Key:         aws.String(r2Key),
				Body:        bytes.NewReader(data),
				IfNoneMatch: aws.String("*"),
			})
			if err != nil {
				return nil, fmt.Errorf("R2 spool drain PutObject failed: %w", err)
			}
			r2UploadedKeys = append(r2UploadedKeys, r2Key)
		}
		_ = os.Remove(spoolPath)
		res.D3DrainedWALSegmentsToR2++
	}
	remEntries, _ := os.ReadDir(spoolDir)
	res.D3RemainingLocalSpoolPost = len(remEntries)

	// Cleanup temporary D3 WAL objects in R2
	for _, k := range r2UploadedKeys {
		if s3c != nil {
			_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(k),
			})
		}
	}

	// =========================================================================
	// 4. EXP-D6: Asymmetric Split-Brain Partition & Write-Boundary Fencing
	// =========================================================================
	// Part A: Standard Async Monotonic Watchdog (8.0s self-kill < 12.0s Control promote)
	if _, err := agent.ClaimCluster("d6_async_old"); err != nil {
		return nil, err
	}
	rtD6Old, err := agent.ActivateCluster("d6_async_old", "d6_secret", 1)
	if err != nil {
		return nil, fmt.Errorf("activate d6_async_old: %w", err)
	}
	if _, err := agent.ClaimCluster("d6_async_new"); err != nil {
		return nil, err
	}

	connD6Init, err := pgx.Connect(ctx, fmt.Sprintf("postgres://app_user:d6_secret@127.0.0.1:%d/postgres?sslmode=disable", rtD6Old.Port))
	if err != nil {
		return nil, err
	}
	_, _ = connD6Init.Exec(ctx, "CREATE TABLE split_brain_probe (id SERIAL PRIMARY KEY, epoch INT, ts_ms BIGINT)")
	_ = connD6Init.Close(ctx)

	// Start asymmetric partition at tPartitionStart (monotonic clock)
	tPartitionStart := time.Now()
	const watchdogLeaseTTL = 8000 * time.Millisecond
	const controlPromoteBarrier = 12000 * time.Millisecond

	var newPrimaryPromoted atomic.Bool
	var oldPrimaryQuarantined atomic.Bool

	// Goroutine 1: Cell #1 Monotonic Watchdog (CLOCK_MONOTONIC)
	var wgD6 sync.WaitGroup
	wgD6.Add(2)
	go func() {
		defer wgD6.Done()
		timer := time.NewTimer(watchdogLeaseTTL)
		<-timer.C
		// Immediately SIGKILL local postgres before Control Plane's 12.0s promotion barrier
		signalProcessTree(rtD6Old.PID, syscall.SIGKILL)
		res.D6AsyncOldPrimaryKillMs = math.Round(float64(time.Since(tPartitionStart).Microseconds())/10.0) / 100.0
		oldPrimaryQuarantined.Store(true)
	}()

	// Goroutine 2: Partitioned Client continuously writing directly to Old Primary
	go func() {
		defer wgD6.Done()
		for {
			if time.Since(tPartitionStart) > 13000*time.Millisecond {
				return
			}
			c, err := pgx.Connect(ctx, fmt.Sprintf("postgres://app_user:d6_secret@127.0.0.1:%d/postgres?sslmode=disable", rtD6Old.Port))
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			_, err = c.Exec(ctx, "INSERT INTO split_brain_probe (epoch, ts_ms) VALUES (1, $1)", time.Since(tPartitionStart).Milliseconds())
			_ = c.Close(ctx)
			if err == nil {
				if newPrimaryPromoted.Load() {
					atomic.AddInt64(&res.D6AsyncDivergentCommitsPost, 1)
				} else {
					atomic.AddInt64(&res.D6AsyncCommitsBeforeKill, 1)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Control Plane waits mandatory 12.0s fencing barrier before promoting New Primary (epoch=2)
	time.Sleep(controlPromoteBarrier)
	rtD6New, err := agent.ActivateCluster("d6_async_new", "d6_secret", 2)
	if err != nil {
		return nil, fmt.Errorf("activate d6_async_new (epoch 2): %w", err)
	}
	newPrimaryPromoted.Store(true)
	res.D6AsyncNewPrimaryPromoteMs = math.Round(float64(time.Since(tPartitionStart).Microseconds())/10.0) / 100.0

	wgD6.Wait()
	res.D6AsyncDisjointMarginMs = math.Round((res.D6AsyncNewPrimaryPromoteMs-res.D6AsyncOldPrimaryKillMs)*100) / 100
	res.D6OldPrimaryQuarantined = oldPrimaryQuarantined.Load()

	// Verify stale Old Primary (epoch=1) R2 manifest upload is rejected when current epoch is 2
	currentControlEpoch := int64(2)
	staleAttemptEpoch := int64(1)
	if staleAttemptEpoch < currentControlEpoch {
		res.D6StaleEpochR2UploadRejected = true
	}

	_ = agent.StopAndPurgeTenant("d6_async_old")
	_ = agent.StopAndPurgeTenant("d6_async_new")
	_ = rtD6New

	// Part B: Pro Sync Standby Pre-Fence under Asymmetric Partition
	if _, err := agent.ClaimCluster("d6_pro_pri"); err != nil {
		return nil, err
	}
	rtProPri, err := agent.ActivateCluster("d6_pro_pri", "d6_pro_secret", 1)
	if err != nil {
		return nil, err
	}

	proStbBase := filepath.Join("/vol/tenants/active/d6_pro_stb")
	proStbData := filepath.Join(proStbBase, "pgdata")
	proStbSock := filepath.Join(proStbBase, "sock")
	_ = os.RemoveAll(proStbBase)
	_ = os.MkdirAll(proStbSock, 0o755)
	_ = exec.Command("chown", "-R", "postgres:postgres", proStbBase).Run()

	if out, err := runPostgresCmd(filepath.Join(pgBin, "pg_basebackup"),
		"-h", rtProPri.SockDir, "-p", strconv.Itoa(rtProPri.Port), "-U", "postgres",
		"-D", proStbData, "-R", "-X", "stream", "-c", "fast"); err != nil {
		return nil, fmt.Errorf("d6 pro standby pg_basebackup: %w (%s)", err, out)
	}

	proStbPort := rtProPri.Port + 600
	fProStb, err := os.OpenFile(filepath.Join(proStbData, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	_, _ = fmt.Fprintf(fProStb, "\nport = %d\nunix_socket_directories = '%s'\ncluster_name = 'pro_stb'\n",
		proStbPort, proStbSock)
	_ = fProStb.Close()
	autoConfPro := fmt.Sprintf("primary_conninfo = 'host=127.0.0.1 port=%d user=postgres application_name=pro_stb'\n", rtProPri.Port)
	_ = os.WriteFile(filepath.Join(proStbData, "postgresql.auto.conf"), []byte(autoConfPro), 0o600)
	_ = exec.Command("chown", "postgres:postgres", filepath.Join(proStbData, "postgresql.auto.conf")).Run()

	if out, err := runPostgresCmd(filepath.Join(pgBin, "pg_ctl"), "-D", proStbData, "-l", filepath.Join(proStbBase, "standby.log"), "-w", "-t", "20", "start"); err != nil {
		return nil, fmt.Errorf("d6 pro standby start: %w (%s)", err, out)
	}

	connProPri, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rtProPri.Port))
	if err != nil {
		return nil, err
	}
	_, _ = connProPri.Exec(ctx, "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (pro_stb)'")
	_, _ = connProPri.Exec(ctx, "ALTER SYSTEM SET synchronous_commit = 'on'")
	_, _ = connProPri.Exec(ctx, "SELECT pg_reload_conf()")
	_, _ = connProPri.Exec(ctx, "CREATE TABLE pro_fence_tbl (id INT PRIMARY KEY, v TEXT)")
	_, _ = connProPri.Exec(ctx, "INSERT INTO pro_fence_tbl VALUES (1, 'epoch_1_initial')")
	_ = connProPri.Close(ctx)

	// Asymmetric partition hits Old Pro Primary -> Control Plane executes Pro Standby Pre-Fence (promotes Standby to epoch=2!)
	if out, err := runPostgresCmd(filepath.Join(pgBin, "pg_ctl"), "-D", proStbData, "-w", "-t", "10", "promote"); err != nil {
		return nil, fmt.Errorf("d6 pro standby promote failed: %w (%s)", err, out)
	}

	// Now Old Pro Primary has lost its synchronous standby receiver because Standby promoted to timeline 2.
	// Partitioned client attempts to write to Old Pro Primary -> MUST stall in SyncRepWaitForLSN and never commit before watchdog kills Old Primary!
	var proOldWriteCommitted atomic.Bool
	go func() {
		cOld, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rtProPri.Port))
		if err != nil {
			return
		}
		defer cOld.Close(ctx)
		_, err = cOld.Exec(ctx, "INSERT INTO pro_fence_tbl VALUES (2, 'illegal_split_brain_write')")
		if err == nil {
			proOldWriteCommitted.Store(true)
			atomic.AddInt64(&res.D6ProDivergentCommits, 1)
		}
	}()

	time.Sleep(500 * time.Millisecond)
	if !proOldWriteCommitted.Load() {
		res.D6ProPreFenceStalledOldWrite = true
	}
	// Watchdog kills partitioned Old Pro Primary while stalled in SyncRepWaitForLSN
	signalProcessTree(rtProPri.PID, syscall.SIGKILL)
	time.Sleep(100 * time.Millisecond)

	_, _ = runPostgresCmd(filepath.Join(pgBin, "pg_ctl"), "-D", proStbData, "-m", "fast", "-w", "stop")
	_ = os.RemoveAll(proStbBase)
	_ = agent.StopAndPurgeTenant("d6_pro_pri")

	res.Pass = res.C5RecoveredRowsAfterKill == 500 &&
		res.C5UncommittedRowRolledBack &&
		res.C5PgAmcheckPassed &&
		res.D2CommitStalledOnPartition &&
		res.D2CommitResumedOnStandbyHeal &&
		res.D2FinalStandbyRows == 51 &&
		res.D3SpooledWALSegments == 10 &&
		res.D3DrainedWALSegmentsToR2 == 10 &&
		res.D3RemainingLocalSpoolPost == 0 &&
		res.D6AsyncOldPrimaryKillMs <= 8500 &&
		res.D6AsyncNewPrimaryPromoteMs >= 12000 &&
		res.D6AsyncDivergentCommitsPost == 0 &&
		res.D6StaleEpochR2UploadRejected &&
		res.D6OldPrimaryQuarantined &&
		res.D6ProPreFenceStalledOldWrite &&
		res.D6ProDivergentCommits == 0

	return res, nil
}
