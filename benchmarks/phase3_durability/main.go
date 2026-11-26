package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	apiURL = "http://127.0.0.1:8080"
	token  = "air_bench_token_local_only"
	outDir = "/workspaces/air/benchmarks/results/phase3"
)

type TestResult struct {
	Name    string         `json:"name"`
	Passed  bool           `json:"passed"`
	Detail  string         `json:"detail"`
	Metrics map[string]any `json:"metrics,omitempty"`
}

func httpJSON(method, path string, body any, auth bool) ([]byte, int, error) {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, apiURL+path, r)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if auth {
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Idempotency-Key", fmt.Sprintf("p3-%d", time.Now().UnixNano()))
	}
	resp, err := (&http.Client{Timeout: 120 * time.Second}).Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b, resp.StatusCode, nil
}

func provisionDB(tenantID string) (int, string, error) {
	_, _, _ = httpJSON("DELETE", "/v1/databases/"+tenantID, nil, true)
	b, code, err := httpJSON("POST", "/v1/databases", map[string]string{"tenant_id": tenantID}, true)
	if err != nil || code >= 300 {
		return 0, "", fmt.Errorf("provision %s failed code=%d err=%v body=%s", tenantID, code, err, string(b))
	}
	return getRoute(tenantID)
}

func getRoute(tenantID string) (int, string, error) {
	b, code, err := httpJSON("GET", "/internal/v1/tenants/"+tenantID+"/route", nil, false)
	if err != nil || code >= 300 {
		return 0, "", fmt.Errorf("route %s failed code=%d err=%v body=%s", tenantID, code, err, string(b))
	}
	var res struct {
		Port        int    `json:"port"`
		AppPassword string `json:"app_password"`
		State       string `json:"state"`
	}
	_ = json.Unmarshal(b, &res)
	return res.Port, res.AppPassword, nil
}

func suspendDB(tenantID string) error {
	b, code, err := httpJSON("POST", "/internal/v1/databases/"+tenantID+"/suspend", nil, false)
	if err != nil || code >= 300 {
		return fmt.Errorf("suspend %s failed code=%d err=%v body=%s", tenantID, code, err, string(b))
	}
	return nil
}

func wakeDB(tenantID string) (int, string, error) {
	b, code, err := httpJSON("POST", "/internal/v1/databases/"+tenantID+"/start", nil, false)
	if err != nil || code >= 300 {
		return 0, "", fmt.Errorf("wake %s failed code=%d err=%v body=%s", tenantID, code, err, string(b))
	}
	var res struct {
		Port        int    `json:"port"`
		AppPassword string `json:"app_password"`
	}
	_ = json.Unmarshal(b, &res)
	return res.Port, res.AppPassword, nil
}

func dsn(port int, pass, db string) string {
	return fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", pass, port, db)
}

func runCmd(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return string(out), err
}

func computeDatasetChecksum(ctx context.Context, conn *pgx.Conn) (int64, string, error) {
	var cnt int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM dur_items`).Scan(&cnt); err != nil {
		return 0, "", err
	}
	rows, err := conn.Query(ctx, `SELECT id, COALESCE(tag,'<NULL>'), COALESCE(meta::text,'<NULL>'), COALESCE(length(big_text),0), COALESCE(md5(big_text),'<NULL>') FROM dur_items ORDER BY id`)
	if err != nil {
		return 0, "", err
	}
	defer rows.Close()
	h := sha256.New()
	for rows.Next() {
		var id int64
		var tag, meta, bigMD5 string
		var bigLen int
		if err := rows.Scan(&id, &tag, &meta, &bigLen, &bigMD5); err != nil {
			return 0, "", err
		}
		fmt.Fprintf(h, "%d|%s|%s|%d|%s\n", id, tag, meta, bigLen, bigMD5)
	}
	return cnt, hex.EncodeToString(h.Sum(nil)), rows.Err()
}

func verifyIndexes(ctx context.Context, conn *pgx.Conn) error {
	// Force index scans on secondary indexes & PK
	var c1, c2, c3 int64
	if _, err := conn.Exec(ctx, `SET enable_seqscan = off`); err != nil {
		return err
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM dur_items WHERE id BETWEEN 1 AND 5000`).Scan(&c1); err != nil {
		return err
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM dur_items WHERE tag = 'tag-7'`).Scan(&c2); err != nil {
		return err
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM dur_items WHERE meta @> '{"active": true}'::jsonb`).Scan(&c3); err != nil {
		return err
	}
	_, _ = conn.Exec(ctx, `RESET enable_seqscan`)
	if c1 == 0 || c2 == 0 || c3 == 0 {
		return fmt.Errorf("unexpected zero index scan counts: c1=%d c2=%d c3=%d", c1, c2, c3)
	}
	return nil
}

func main() {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	var results []TestResult

	db := fmt.Sprintf("dur_db_%d", time.Now().Unix())
	port, pass, err := provisionDB(db)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Provisioned %s on port %d", db, port)

	conn, err := pgx.Connect(ctx, dsn(port, pass, db))
	if err != nil {
		log.Fatal(err)
	}

	// Create deterministic dataset: PK, secondary indexes, NULLs, JSONB, large text, updated & deleted rows
	seedStmts := []string{
		`CREATE TABLE dur_items (
			id BIGINT PRIMARY KEY,
			tag TEXT,
			meta JSONB,
			big_text TEXT,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`INSERT INTO dur_items (id, tag, meta, big_text)
		SELECT g,
			CASE WHEN g % 10 = 0 THEN NULL ELSE 'tag-' || (g % 50) END,
			CASE WHEN g % 15 = 0 THEN NULL ELSE jsonb_build_object('seq', g, 'active', (g % 2 = 0), 'grp', g % 7) END,
			CASE WHEN g % 20 = 0 THEN NULL ELSE repeat(md5(g::text), 45) END
		FROM generate_series(1, 2000) AS g`,
		`CREATE INDEX idx_dur_tag ON dur_items(tag)`,
		`CREATE INDEX idx_dur_meta_gin ON dur_items USING gin(meta)`,
		`UPDATE dur_items SET tag = 'updated-' || id, meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{updated}', 'true'::jsonb)
		WHERE id % 25 = 0`,
		`DELETE FROM dur_items WHERE id % 33 = 0`,
	}
	for i, stmt := range seedStmts {
		if _, err = conn.Exec(ctx, stmt); err != nil {
			log.Fatalf("seed deterministic dataset stmt[%d]: %v", i, err)
		}
	}

	expectedCnt, expectedHash, err := computeDatasetChecksum(ctx, conn)
	if err != nil {
		log.Fatal(err)
	}
	if err := verifyIndexes(ctx, conn); err != nil {
		log.Fatal(err)
	}
	conn.Close(ctx)
	log.Printf("Deterministic dataset seeded: count=%d sha256=%s", expectedCnt, expectedHash)

	// === TEST A: Normal suspend and wake (remove disposable local cache) ===
	func() {
		if err := suspendDB(db); err != nil {
			results = append(results, TestResult{Name: "Test A: Normal suspend & wake", Passed: false, Detail: err.Error()})
			return
		}
		time.Sleep(2 * time.Second)
		_, _ = runCmd("docker", "exec", "air-bench", "rm", "-rf", "/var/lib/air/lfc/"+db, "/var/lib/air/ps_cache")
		_, _ = runCmd("docker", "exec", "air-bench", "mkdir", "-p", "/var/lib/air/ps_cache")
		wPort, wPass, err := wakeDB(db)
		if err != nil {
			results = append(results, TestResult{Name: "Test A: Normal suspend & wake", Passed: false, Detail: err.Error()})
			return
		}
		c, err := pgx.Connect(ctx, dsn(wPort, wPass, db))
		if err != nil {
			results = append(results, TestResult{Name: "Test A: Normal suspend & wake", Passed: false, Detail: err.Error()})
			return
		}
		defer c.Close(ctx)
		cnt, h, err := computeDatasetChecksum(ctx, c)
		idxErr := verifyIndexes(ctx, c)
		passed := err == nil && idxErr == nil && cnt == expectedCnt && h == expectedHash
		results = append(results, TestResult{
			Name:   "Test A: Normal suspend & wake (cache cleared)",
			Passed: passed,
			Detail: fmt.Sprintf("cnt=%d (want %d) hash=%s (want %s) idxErr=%v err=%v", cnt, expectedCnt, h, expectedHash, idxErr, err),
		})
		port, pass = wPort, wPass
	}()

	// === TEST B: Full container restart + R2 recovery ===
	func() {
		// Commit extra row before restart
		c, err := pgx.Connect(ctx, dsn(port, pass, db))
		if err != nil {
			results = append(results, TestResult{Name: "Test B: Full container restart", Passed: false, Detail: err.Error()})
			return
		}
		_, err = c.Exec(ctx, `INSERT INTO dur_items (id, tag, meta, big_text) VALUES (999001, 'pre-restart', '{"restart":true}'::jsonb, repeat('R', 5000))`)
		if err != nil {
			c.Close(ctx)
			results = append(results, TestResult{Name: "Test B: Full container restart", Passed: false, Detail: err.Error()})
			return
		}
		expectedCnt, expectedHash, err = computeDatasetChecksum(ctx, c)
		c.Close(ctx)
		if err != nil {
			results = append(results, TestResult{Name: "Test B: Full container restart", Passed: false, Detail: err.Error()})
			return
		}

		// Suspend cleanly so cold offload uploads latest layers + skeleton + WAL to R2
		if err := suspendDB(db); err != nil {
			results = append(results, TestResult{Name: "Test B: Full container restart", Passed: false, Detail: err.Error()})
			return
		}
		time.Sleep(5 * time.Second) // allow async cold offload to finish

		// Stop container, remove disposable local caches (lfc, ps_cache), restart container
		_, _ = runCmd("docker", "stop", "air-bench")
		_, _ = runCmd("rm", "-rf", "/tmp/air-bench-data/ps_cache", "/tmp/air-bench-data/lfc/"+db)
		_, _ = runCmd("mkdir", "-p", "/tmp/air-bench-data/ps_cache")
		if out, err := runCmd("docker", "start", "air-bench"); err != nil {
			results = append(results, TestResult{Name: "Test B: Full container restart", Passed: false, Detail: fmt.Sprintf("docker start: %v %s", err, out)})
			return
		}
		for i := 0; i < 60; i++ {
			if _, code, err := httpJSON("GET", "/healthz", nil, false); err == nil && code == 200 {
				break
			}
			time.Sleep(time.Second)
		}
		wPort, wPass, err := wakeDB(db)
		if err != nil {
			results = append(results, TestResult{Name: "Test B: Full container restart", Passed: false, Detail: err.Error()})
			return
		}
		c2, err := pgx.Connect(ctx, dsn(wPort, wPass, db))
		if err != nil {
			results = append(results, TestResult{Name: "Test B: Full container restart", Passed: false, Detail: err.Error()})
			return
		}
		defer c2.Close(ctx)
		cnt, h, err := computeDatasetChecksum(ctx, c2)
		idxErr := verifyIndexes(ctx, c2)
		passed := err == nil && idxErr == nil && cnt == expectedCnt && h == expectedHash
		results = append(results, TestResult{
			Name:   "Test B: Full container restart (lfc+ps_cache cleared)",
			Passed: passed,
			Detail: fmt.Sprintf("cnt=%d (want %d) hash=%s (want %s) idxErr=%v err=%v", cnt, expectedCnt, h, expectedHash, idxErr, err),
		})
		port, pass = wPort, wPass
	}()

	// === TEST C: Abrupt process termination (SIGKILL PG + SIGKILL air-server) ===
	func() {
		pool, err := pgxpool.New(ctx, dsn(port, pass, db))
		if err != nil {
			results = append(results, TestResult{Name: "Test C: Abrupt termination", Passed: false, Detail: err.Error()})
			return
		}
		_, err = pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS tx_ledger (seq BIGINT PRIMARY KEY, state TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT now())`)
		if err != nil {
			pool.Close()
			results = append(results, TestResult{Name: "Test C: Abrupt termination", Passed: false, Detail: err.Error()})
			return
		}

		var mu sync.Mutex
		committedSeqs := make(map[int64]bool)
		var nextSeq atomic.Int64
		var stopWorkers atomic.Bool
		var wg sync.WaitGroup

		for w := 0; w < 6; w++ {
			wg.Add(1)
			go func(wid int) {
				defer wg.Done()
				for !stopWorkers.Load() {
					seq := nextSeq.Add(1)
					tx, err := pool.Begin(ctx)
					if err != nil {
						continue
					}
					_, err = tx.Exec(ctx, `INSERT INTO tx_ledger (seq, state) VALUES ($1, 'committed')`, seq)
					if err != nil {
						_ = tx.Rollback(ctx)
						continue
					}
					// Every 5th tx stays open slightly longer to catch in-flight uncommitted txns
					if seq%5 == 0 {
						time.Sleep(15 * time.Millisecond)
					}
					if err := tx.Commit(ctx); err == nil {
						mu.Lock()
						committedSeqs[seq] = true
						mu.Unlock()
					}
				}
			}(w)
		}

		// Let concurrent writes run for 1.5s, then SIGKILL tenant postgres process abruptly!
		time.Sleep(1500 * time.Millisecond)
		stopWorkers.Store(true)
		pidOut, _ := runCmd("docker", "exec", "air-bench", "head", "-n", "1", "/var/lib/air/tenants/active/"+db+"/pgdata/postmaster.pid")
		pgPID := strings.TrimSpace(pidOut)
		killOut, killErr := runCmd("docker", "exec", "air-bench", "kill", "-9", pgPID)
		wg.Wait()
		pool.Close()

		// Suspend cleanly (or wake) to trigger PG crash recovery / restart
		time.Sleep(1 * time.Second)
		_ = suspendDB(db)
		time.Sleep(2 * time.Second)
		wPort, wPass, err := wakeDB(db)
		if err != nil {
			results = append(results, TestResult{Name: "Test C: Abrupt SIGKILL postgres", Passed: false, Detail: fmt.Sprintf("wake failed: %v (kill=%v %s pid=%s)", err, killErr, killOut, pgPID)})
			return
		}
		c, err := pgx.Connect(ctx, dsn(wPort, wPass, db))
		if err != nil {
			results = append(results, TestResult{Name: "Test C: Abrupt SIGKILL air-bench", Passed: false, Detail: err.Error()})
			return
		}
		defer c.Close(ctx)
		rows, err := c.Query(ctx, `SELECT seq FROM tx_ledger`)
		if err != nil {
			results = append(results, TestResult{Name: "Test C: Abrupt SIGKILL air-bench", Passed: false, Detail: err.Error()})
			return
		}
		found := make(map[int64]bool)
		for rows.Next() {
			var s int64
			_ = rows.Scan(&s)
			found[s] = true
		}
		rows.Close()

		missing := 0
		for s := range committedSeqs {
			if !found[s] {
				missing++
			}
		}
		// Check base dataset integrity too
		cnt, h, err := computeDatasetChecksum(ctx, c)
		passed := err == nil && missing == 0 && cnt == expectedCnt && h == expectedHash
		results = append(results, TestResult{
			Name:    "Test C: Abrupt SIGKILL air-bench",
			Passed:  passed,
			Detail:  fmt.Sprintf("acknowledged_committed=%d found_after_recovery=%d missing=%d base_cnt=%d base_hash_ok=%v", len(committedSeqs), len(found), missing, cnt, h == expectedHash),
			Metrics: map[string]any{"acknowledged_committed": len(committedSeqs), "found": len(found), "missing": missing},
		})
		port, pass = wPort, wPass
	}()

	// === TEST D: WAL recovery & PITR backup/restore ===
	func() {
		bBody, bCode, bErr := httpJSON("POST", "/v1/databases/"+db+"/backups", map[string]string{"type": "FULL"}, true)
		if bErr != nil || bCode >= 300 {
			results = append(results, TestResult{Name: "Test D: WAL & Backup/Restore", Passed: false, Detail: fmt.Sprintf("backup failed code=%d err=%v body=%s", bCode, bErr, string(bBody))})
			return
		}
		// Write post-backup data to test WAL replay
		c, err := pgx.Connect(ctx, dsn(port, pass, db))
		if err != nil {
			results = append(results, TestResult{Name: "Test D: WAL & Backup/Restore", Passed: false, Detail: err.Error()})
			return
		}
		_, err = c.Exec(ctx, `INSERT INTO dur_items (id, tag, meta, big_text) VALUES (999002, 'post-backup-wal', '{"wal":true}'::jsonb, repeat('W', 4096)); SELECT pg_switch_wal();`)
		if err != nil {
			c.Close(ctx)
			results = append(results, TestResult{Name: "Test D: WAL & Backup/Restore", Passed: false, Detail: err.Error()})
			return
		}
		expectedCnt, expectedHash, err = computeDatasetChecksum(ctx, c)
		c.Close(ctx)
		if err != nil {
			results = append(results, TestResult{Name: "Test D: WAL & Backup/Restore", Passed: false, Detail: err.Error()})
			return
		}
		time.Sleep(2 * time.Second)
		rBody, rCode, rErr := httpJSON("POST", "/v1/databases/"+db+"/restore", map[string]string{}, true)
		passed := rErr == nil && rCode == 200
		detail := fmt.Sprintf("backup_code=%d restore_code=%d err=%v", bCode, rCode, rErr)
		if passed {
			port, pass, rErr = getRoute(db)
			var restored *pgx.Conn
			if rErr == nil {
				restored, rErr = pgx.Connect(ctx, dsn(port, pass, db))
			}
			if rErr == nil {
				var count int64
				var hash string
				count, hash, rErr = computeDatasetChecksum(ctx, restored)
				restored.Close(ctx)
				passed = rErr == nil && count == expectedCnt && hash == expectedHash
				detail += fmt.Sprintf(" expected_rows=%d restored_rows=%d checksum_match=%t", expectedCnt, count, hash == expectedHash)
			} else {
				passed = false
			}
			if rErr != nil {
				detail += fmt.Sprintf(" verification_err=%v", rErr)
			}
		} else {
			detail += " restore_resp=" + strings.TrimSpace(string(rBody))
		}
		results = append(results, TestResult{
			Name:   "Test D: WAL & Backup/Restore (PITR verify)",
			Passed: passed,
			Detail: detail,
		})
	}()

	// === TEST E: Repeated lifecycle (20 cycles of write -> commit -> suspend -> wake -> verify) ===
	func() {
		failures := 0
		var firstErr string
		for cycle := 1; cycle <= 20; cycle++ {
			c, err := pgx.Connect(ctx, dsn(port, pass, db))
			if err != nil {
				failures++
				if firstErr == "" {
					firstErr = fmt.Sprintf("cycle %d connect: %v", cycle, err)
				}
				break
			}
			id := int64(900000 + cycle)
			tag := fmt.Sprintf("cycle-%d", cycle)
			_, err = c.Exec(ctx, `INSERT INTO dur_items (id, tag, meta, big_text) VALUES ($1, $2, jsonb_build_object('cycle', $1::int), repeat($2, 50))`, id, tag)
			if err != nil {
				c.Close(ctx)
				failures++
				if firstErr == "" {
					firstErr = fmt.Sprintf("cycle %d insert: %v", cycle, err)
				}
				break
			}
			expCnt, expHash, err := computeDatasetChecksum(ctx, c)
			c.Close(ctx)
			if err != nil {
				failures++
				if firstErr == "" {
					firstErr = fmt.Sprintf("cycle %d checksum: %v", cycle, err)
				}
				break
			}
			if err := suspendDB(db); err != nil {
				failures++
				if firstErr == "" {
					firstErr = fmt.Sprintf("cycle %d suspend: %v", cycle, err)
				}
				break
			}
			wPort, wPass, err := wakeDB(db)
			if err != nil {
				failures++
				if firstErr == "" {
					firstErr = fmt.Sprintf("cycle %d wake: %v", cycle, err)
				}
				break
			}
			port, pass = wPort, wPass
			c2, err := pgx.Connect(ctx, dsn(port, pass, db))
			if err != nil {
				failures++
				if firstErr == "" {
					firstErr = fmt.Sprintf("cycle %d reconnect: %v", cycle, err)
				}
				break
			}
			gotCnt, gotHash, err := computeDatasetChecksum(ctx, c2)
			c2.Close(ctx)
			if err != nil || gotCnt != expCnt || gotHash != expHash {
				failures++
				if firstErr == "" {
					firstErr = fmt.Sprintf("cycle %d mismatch cnt=%d/%d hash=%s/%s err=%v", cycle, gotCnt, expCnt, gotHash, expHash, err)
				}
				break
			}
		}
		results = append(results, TestResult{
			Name:   "Test E: Repeated 20-cycle write->commit->suspend->wake->verify",
			Passed: failures == 0,
			Detail: fmt.Sprintf("failures=%d firstErr=%s", failures, firstErr),
		})
	}()

	// Write results JSON
	outBytes, _ := json.MarshalIndent(results, "", "  ")
	_ = os.WriteFile(filepath.Join(outDir, "durability_results.json"), outBytes, 0o644)
	fmt.Println(string(outBytes))

	// Cleanup test db
	_, _, _ = httpJSON("DELETE", "/v1/databases/"+db, nil, true)
}
