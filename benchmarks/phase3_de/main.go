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
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	apiURL = "http://127.0.0.1:8080"
	token  = "air_bench_token_local_only"
)

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
	}
	if method == "POST" && path == "/v1/databases" {
		req.Header.Set("Idempotency-Key", fmt.Sprintf("p3de-%d", time.Now().UnixNano()))
	}
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b, resp.StatusCode, nil
}

func dsn(port int, pass, db string) string {
	return fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", pass, port, db)
}

func computeChecksum(ctx context.Context, conn *pgx.Conn) (int64, string, error) {
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

func main() {
	ctx := context.Background()
	db := fmt.Sprintf("dur_de_%d", time.Now().Unix())
	b, code, err := httpJSON("POST", "/v1/databases", map[string]string{"tenant_id": db}, true)
	if err != nil || code >= 300 {
		log.Fatalf("provision: code=%d err=%v body=%s", code, err, string(b))
	}
	rb, _, _ := httpJSON("GET", "/internal/v1/tenants/"+db+"/route", nil, false)
	var route struct {
		Port        int    `json:"port"`
		AppPassword string `json:"app_password"`
	}
	_ = json.Unmarshal(rb, &route)
	port, pass := route.Port, route.AppPassword
	log.Printf("Provisioned %s on port %d", db, port)

	c, err := pgx.Connect(ctx, dsn(port, pass, db))
	if err != nil {
		log.Fatal(err)
	}
	for _, s := range []string{
		`CREATE TABLE dur_items (id BIGINT PRIMARY KEY, tag TEXT, meta JSONB, big_text TEXT)`,
		`INSERT INTO dur_items SELECT g, 'tag-'||(g%10), jsonb_build_object('seq', g), repeat(md5(g::text), 30) FROM generate_series(1, 500) g`,
	} {
		if _, err := c.Exec(ctx, s); err != nil {
			log.Fatal(err)
		}
	}
	c.Close(ctx)

	// Test D: Backup + post-backup WAL + Restore
	bBody, bCode, bErr := httpJSON("POST", "/v1/databases/"+db+"/backups", map[string]string{"type": "FULL"}, true)
	log.Printf("Test D backup: code=%d err=%v body=%s", bCode, bErr, strings.TrimSpace(string(bBody)))
	if bErr == nil && bCode < 300 {
		c, err := pgx.Connect(ctx, dsn(port, pass, db))
		if err != nil {
			log.Fatal(err)
		}
		_, err = c.Exec(ctx, `INSERT INTO dur_items VALUES (999002, 'post-backup-wal', '{"wal":true}'::jsonb, repeat('W', 1024))`)
		if err != nil {
			log.Fatal(err)
		}
		_, _ = c.Exec(ctx, `SELECT pg_switch_wal()`)
		c.Close(ctx)
		time.Sleep(2 * time.Second)
		rBody, rCode, rErr := httpJSON("POST", "/v1/databases/"+db+"/restore", map[string]string{"verify_query": "SELECT count(*) FROM dur_items"}, true)
		log.Printf("Test D restore: code=%d err=%v body=%s", rCode, rErr, strings.TrimSpace(string(rBody)))
	}

	// Refresh route
	rb, _, _ = httpJSON("GET", "/internal/v1/tenants/"+db+"/route", nil, false)
	_ = json.Unmarshal(rb, &route)
	port, pass = route.Port, route.AppPassword

	// Test E: 20 cycles
	failures := 0
	var firstErr string
	for cycle := 1; cycle <= 20; cycle++ {
		c, err := pgx.Connect(ctx, dsn(port, pass, db))
		if err != nil {
			failures++
			firstErr = fmt.Sprintf("cycle %d connect: %v", cycle, err)
			break
		}
		id := int64(900000 + cycle)
		tag := fmt.Sprintf("cycle-%d", cycle)
		if _, err := c.Exec(ctx, `INSERT INTO dur_items (id, tag, meta, big_text) VALUES ($1, $2, jsonb_build_object('cycle', $1::bigint), repeat($2, 20))`, id, tag); err != nil {
			c.Close(ctx)
			failures++
			firstErr = fmt.Sprintf("cycle %d insert: %v", cycle, err)
			break
		}
		expCnt, expHash, err := computeChecksum(ctx, c)
		c.Close(ctx)
		if err != nil {
			failures++
			firstErr = fmt.Sprintf("cycle %d checksum: %v", cycle, err)
			break
		}
		_, sc, serr := httpJSON("POST", "/internal/v1/databases/"+db+"/suspend", nil, false)
		if serr != nil || sc >= 300 {
			failures++
			firstErr = fmt.Sprintf("cycle %d suspend: code=%d err=%v", cycle, sc, serr)
			break
		}
		// Wait until SUSPENDED
		for i := 0; i < 30; i++ {
			rb2, _, _ := httpJSON("GET", "/internal/v1/tenants/"+db+"/route", nil, false)
			var r2 struct {
				State string `json:"state"`
			}
			_ = json.Unmarshal(rb2, &r2)
			if r2.State == "SUSPENDED" {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		wb, wc, werr := httpJSON("POST", "/internal/v1/databases/"+db+"/start", nil, false)
		if werr != nil || wc >= 300 {
			// Retry once if cold offload lock was still finishing
			time.Sleep(2 * time.Second)
			wb, wc, werr = httpJSON("POST", "/internal/v1/databases/"+db+"/start", nil, false)
		}
		if werr != nil || wc >= 300 {
			failures++
			firstErr = fmt.Sprintf("cycle %d wake: code=%d err=%v body=%s", cycle, wc, werr, string(wb))
			break
		}
		var wr struct {
			Port        int    `json:"port"`
			AppPassword string `json:"app_password"`
		}
		_ = json.Unmarshal(wb, &wr)
		port, pass = wr.Port, wr.AppPassword

		c2, err := pgx.Connect(ctx, dsn(port, pass, db))
		if err != nil {
			failures++
			firstErr = fmt.Sprintf("cycle %d reconnect: %v", cycle, err)
			break
		}
		gotCnt, gotHash, err := computeChecksum(ctx, c2)
		c2.Close(ctx)
		if err != nil || gotCnt != expCnt || gotHash != expHash {
			failures++
			firstErr = fmt.Sprintf("cycle %d mismatch cnt=%d/%d hash=%s/%s err=%v", cycle, gotCnt, expCnt, gotHash, expHash, err)
			break
		}
		if cycle%5 == 0 {
			log.Printf("Test E cycle %d/20 OK (cnt=%d)", cycle, gotCnt)
		}
	}
	log.Printf("Test E summary: failures=%d firstErr=%s", failures, firstErr)

	_, _, _ = httpJSON("DELETE", "/v1/databases/"+db, nil, true)
	os.Exit(0)
}
