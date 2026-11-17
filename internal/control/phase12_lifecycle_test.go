package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
)

type mockR2 struct {
	mu          sync.RWMutex
	objects     map[string][]byte
	etags       map[string]string
	seq         atomic.Int64
	putDelay    atomic.Int64 // nanoseconds
	failNextPut atomic.Int32
	failKeySub  string
	putStarted  chan string
	srv         *httptest.Server
}

func newMockR2() *mockR2 {
	m := &mockR2{
		objects:    make(map[string][]byte),
		etags:      make(map[string]string),
		putStarted: make(chan string, 64),
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	return m
}

func (m *mockR2) Close() { m.srv.Close() }

func (m *mockR2) Client() *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint:     aws.String(m.srv.URL),
		UsePathStyle:     true,
		Region:           "auto",
		Credentials:      credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
		RetryMaxAttempts: 1,
	})
}

func (m *mockR2) handle(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}

	if r.Method == http.MethodGet && key == "" && r.URL.Query().Get("list-type") == "2" {
		prefix := r.URL.Query().Get("prefix")
		m.mu.RLock()
		var keys []string
		for k := range m.objects {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		b.WriteString(fmt.Sprintf("<Name>%s</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount><IsTruncated>false</IsTruncated>",
			parts[0], prefix, len(keys)))
		for _, k := range keys {
			b.WriteString(fmt.Sprintf("<Contents><Key>%s</Key><Size>%d</Size><ETag>%q</ETag></Contents>",
				k, len(m.objects[k]), m.etags[k]))
		}
		m.mu.RUnlock()
		b.WriteString(`</ListBucketResult>`)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(b.String()))
		return
	}

	switch r.Method {
	case http.MethodPut:
		select {
		case m.putStarted <- key:
		default:
		}
		if d := time.Duration(m.putDelay.Load()); d > 0 {
			select {
			case <-time.After(d):
			case <-r.Context().Done():
				return
			}
		}
		if m.failNextPut.Load() > 0 && (m.failKeySub == "" || strings.Contains(key, m.failKeySub)) {
			m.failNextPut.Add(-1)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`<Error><Code>InternalError</Code></Error>`))
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		etag := fmt.Sprintf("etag-%d", m.seq.Add(1))
		m.mu.Lock()
		m.objects[key] = body
		m.etags[key] = etag
		m.mu.Unlock()
		w.Header().Set("ETag", fmt.Sprintf("%q", etag))
		w.WriteHeader(http.StatusOK)
	case http.MethodGet, http.MethodHead:
		m.mu.RLock()
		data, ok := m.objects[key]
		etag := m.etags[key]
		m.mu.RUnlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
			return
		}
		w.Header().Set("ETag", fmt.Sprintf("%q", etag))
		if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
			spec := strings.TrimPrefix(rng, "bytes=")
			bounds := strings.SplitN(spec, "-", 2)
			start, _ := strconv.Atoi(bounds[0])
			end := len(data) - 1
			if bounds[1] != "" {
				end, _ = strconv.Atoi(bounds[1])
			}
			if start < 0 || start >= len(data) || end < start {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if end >= len(data) {
				end = len(data) - 1
			}
			slice := data[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			w.Header().Set("Content-Length", strconv.Itoa(len(slice)))
			w.WriteHeader(http.StatusPartialContent)
			if r.Method == http.MethodGet {
				_, _ = w.Write(slice)
			}
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write(data)
		}
	case http.MethodDelete:
		m.mu.Lock()
		delete(m.objects, key)
		delete(m.etags, key)
		m.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func setupPhase12Env(t *testing.T, r2 *mockR2, evictOnSuspend bool) (string, *crypto.EnvelopeKMS, *cell.Agent, *ControlPlane, *pageserver.Pageserver) {
	t.Helper()
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/postgres"); err != nil {
		t.Skip("PostgreSQL 17 binaries not installed")
	}
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("/tmp", "air-p12-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatalf("NewEnvelopeKMS: %v", err)
	}
	ag, err := cell.NewAgent(filepath.Join(tmpDir, "vol"), "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	t.Cleanup(func() { ag.Close() })

	cp, err := NewControlPlaneWithConfig(ctx, ControlPlaneConfig{
		MetaDir:        filepath.Join(tmpDir, "meta"),
		PGBinDir:       "/usr/lib/postgresql/17/bin",
		MetaPort:       freeTCPPort(t),
		Agent:          ag,
		KMS:            kms,
		EvictOnSuspend: evictOnSuspend,
		InternalToken:  "test-internal-tok",
	})
	if err != nil {
		t.Fatalf("NewControlPlaneWithConfig: %v", err)
	}
	t.Cleanup(func() { cp.Close() })

	var s3c *s3.Client
	var bucket string
	if r2 != nil {
		s3c = r2.Client()
		bucket = "air-bucket"
		cp.SetBackupStore(s3c, bucket)
	}

	ps, err := pageserver.New(pageserver.Config{
		SocketPath:    filepath.Join(tmpDir, "ps.sock"),
		LayerDir:      filepath.Join(tmpDir, "ps_layers"),
		CacheDir:      filepath.Join(tmpDir, "ps_cache"),
		CacheMaxBytes: 32 * 1024 * 1024,
		S3Client:      s3c,
		R2Bucket:      bucket,
		KMS:           kms,
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	if err := ps.Start(ctx); err != nil {
		t.Fatalf("ps.Start: %v", err)
	}
	t.Cleanup(func() { ps.Close() })
	cp.SetPageserver(ps)

	return tmpDir, kms, ag, cp, ps
}

func waitForOffloadState(t *testing.T, cp *ControlPlane, acct, tid string, wantStates ...string) *OffloadStatus {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		st, err := cp.GetLatestOffloadStatus(context.Background(), acct, tid)
		if err == nil && st != nil {
			for _, w := range wantStates {
				if st.State == w {
					return st
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	st, _ := cp.GetLatestOffloadStatus(context.Background(), acct, tid)
	t.Fatalf("timed out waiting for offload state %v for %s; last=%+v", wantStates, tid, st)
	return nil
}

func connectTenantPG(t *testing.T, port int, sockDir, user, pass, db string) *pgx.Conn {
	t.Helper()
	host := sockDir
	if host == "" {
		host = "127.0.0.1"
	}
	if user == "" {
		user = "postgres"
	}
	if db == "" {
		db = "postgres"
	}
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, pass, db)
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	return conn
}

func TestPhase12TenantIDReuseAndGenerationIsolation(t *testing.T) {
	r2 := newMockR2()
	defer r2.Close()

	tmpDir, _, ag, cp, ps := setupPhase12Env(t, r2, true)
	cp.coldRetryDelay = 10 * time.Millisecond
	cp.coldMaxAttempts = 2
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cp.StartColdOffloadWorker(ctx, 1)

	const acct = "acct_p12_iso"
	const tid = "t_reuse_iso"

	for cycle := 0; cycle < 3; cycle++ {
		provA, err := cp.ProvisionDatabase(ctx, acct, fmt.Sprintf("idem-a-%d", cycle), tid)
		if err != nil {
			t.Fatalf("cycle %d ProvisionDatabase A: %v", cycle, err)
		}
		if provA.Generation == "" {
			t.Fatalf("cycle %d expected non-empty generation", cycle)
		}

		// Write PG tables/indexes in generation A.
		sockA := filepath.Join(tmpDir, "vol", "tenants", "active", tid, "sock")
		connA := connectTenantPG(t, provA.DirectPort, sockA, provA.Username, provA.Password, provA.Database)
		if _, err := connA.Exec(ctx, fmt.Sprintf("CREATE TABLE stale_tbl_%d (id int primary key, v text)", cycle)); err != nil {
			t.Fatalf("create table: %v", err)
		}
		if _, err := connA.Exec(ctx, fmt.Sprintf("INSERT INTO stale_tbl_%d VALUES (1, 'secret-gen-a-%d')", cycle, cycle)); err != nil {
			t.Fatalf("insert: %v", err)
		}
		_ = connA.Close(ctx)

		// Also write a page into Pageserver under generation A.
		if err := ps.PutPageBatch(ctx, tid, 1663, 16385, 55001, 0, 0, 1, 100, make([]byte, pageserver.PageSize)); err != nil {
			t.Fatalf("ps.PutPageBatch A: %v", err)
		}
		if _, err := ps.FlushTenant(ctx, tid); err != nil {
			t.Fatalf("ps.FlushTenant A: %v", err)
		}

		// Suspend and wait for offload to succeed.
		if err := cp.SuspendTenant(ctx, tid, 2*time.Second, nil); err != nil {
			t.Fatalf("SuspendTenant A: %v", err)
		}
		waitForOffloadState(t, cp, acct, tid, "succeeded")

		// Delete tenant A.
		if err := cp.DeleteDatabase(ctx, acct, tid); err != nil {
			t.Fatalf("DeleteDatabase A: %v", err)
		}

		// Recreate the exact same tenant ID -> must receive a fresh generation B.
		provB, err := cp.ProvisionDatabase(ctx, acct, fmt.Sprintf("idem-b-%d", cycle), tid)
		if err != nil {
			t.Fatalf("cycle %d ProvisionDatabase B: %v", cycle, err)
		}
		if provB.Generation == "" || provB.Generation == provA.Generation {
			t.Fatalf("cycle %d expected new generation != %q, got %q", cycle, provA.Generation, provB.Generation)
		}

		// Verify PG database is completely clean (old relation must not exist).
		sockB := filepath.Join(tmpDir, "vol", "tenants", "active", tid, "sock")
		connB := connectTenantPG(t, provB.DirectPort, sockB, provB.Username, provB.Password, provB.Database)
		var tblCount int
		if err := connB.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'").Scan(&tblCount); err != nil {
			t.Fatalf("query pg_tables: %v", err)
		}
		if tblCount != 0 {
			t.Fatalf("cycle %d recreated tenant leaked %d tables from previous generation", cycle, tblCount)
		}

		// Verify generation B can independently write, suspend, wake, and recover.
		if _, err := connB.Exec(ctx, "CREATE TABLE fresh_tbl (id int primary key, v text)"); err != nil {
			t.Fatalf("create fresh_tbl: %v", err)
		}
		if _, err := connB.Exec(ctx, "INSERT INTO fresh_tbl VALUES (42, 'gen-b-ok')"); err != nil {
			t.Fatalf("insert fresh_tbl: %v", err)
		}
		_ = connB.Close(ctx)

		// Verify Pageserver cannot read generation A's page 55001 in generation B.
		if _, _, err := ps.GetPageBatch(ctx, tid, 1663, 16385, 55001, 0, 0, 1, 0); !errors.Is(err, pageserver.ErrNotFound) {
			t.Fatalf("cycle %d expected ErrNotFound for old genA page in genB, got %v", cycle, err)
		}

		// Simulate stale delayed background manifest publish from generation A -> must be rejected!
		if err := ps.Store().PublishLayerManifestGen(ctx, tid, provA.Generation, 1, nil, 1); !errors.Is(err, pageserver.ErrStaleGeneration) {
			t.Fatalf("cycle %d expected ErrStaleGeneration for stale genA manifest publish, got %v", cycle, err)
		}

		if !ag.HasAirSmgr() {
			cp.evictOnSuspend = false
		}
		if err := cp.SuspendTenant(ctx, tid, 2*time.Second, nil); err != nil {
			t.Fatalf("SuspendTenant B: %v", err)
		}
		if ag.HasAirSmgr() {
			waitForOffloadState(t, cp, acct, tid, "succeeded")
		}
		cp.evictOnSuspend = true

		portB, sockB, passB, err := cp.WakeTenant(ctx, tid)
		if err != nil {
			pgLog, _ := os.ReadFile(filepath.Join(tmpDir, "vol", "tenants", "active", tid, "postgresql.log"))
			t.Fatalf("cycle %d WakeTenant B: %v\n--- postgresql.log ---\n%s", cycle, err, string(pgLog))
		}
		connB2 := connectTenantPG(t, portB, sockB, provB.Username, passB, provB.Database)
		var val string
		if err := connB2.QueryRow(ctx, "SELECT v FROM fresh_tbl WHERE id = 42").Scan(&val); err != nil || val != "gen-b-ok" {
			t.Fatalf("cycle %d WakeTenant B read failed: val=%q err=%v", cycle, val, err)
		}
		_ = connB2.Close(ctx)

		if err := cp.DeleteDatabase(ctx, acct, tid); err != nil {
			t.Fatalf("DeleteDatabase B: %v", err)
		}
	}
}

func TestPhase12WakeTenantNotBlockedBySlowR2Offload(t *testing.T) {
	r2 := newMockR2()
	defer r2.Close()

	_, _, _, cp, _ := setupPhase12Env(t, r2, true)
	cp.coldRetryDelay = 10 * time.Millisecond
	cp.coldMaxAttempts = 1
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cp.StartColdOffloadWorker(ctx, 1)

	const acct = "acct_p12_wake"
	const tid = "t_slow_wake"
	provW, err := cp.ProvisionDatabase(ctx, acct, "idem-wake-1", tid)
	if err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}

	// Inject a 3-second delay on R2 PUTs.
	r2.putDelay.Store(int64(3 * time.Second))
	// Drain any earlier notifications.
	for len(r2.putStarted) > 0 {
		<-r2.putStarted
	}

	if err := cp.SuspendTenant(ctx, tid, 2*time.Second, nil); err != nil {
		t.Fatalf("SuspendTenant: %v", err)
	}
	// Duplicate suspend call while already suspended must be idempotent.
	if err := cp.SuspendTenant(ctx, tid, 2*time.Second, nil); err != nil {
		t.Fatalf("duplicate SuspendTenant: %v", err)
	}

	// Wait until the background offload actually starts an R2 PUT.
	select {
	case <-r2.putStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for offload R2 PUT to start")
	}

	// WakeTenant must NOT wait for the 3s R2 PUT to complete.
	t0 := time.Now()
	port, sock, pass, err := cp.WakeTenant(ctx, tid)
	wakeElapsed := time.Since(t0)
	if err != nil {
		t.Fatalf("WakeTenant during slow R2 offload: %v", err)
	}
	t.Logf("WakeTenant during 3s slow R2 offload completed in %v", wakeElapsed)
	if wakeElapsed >= 2500*time.Millisecond {
		t.Fatalf("WakeTenant blocked on slow R2 offload: took %v (want < 2.5s)", wakeElapsed)
	}

	conn := connectTenantPG(t, port, sock, provW.Username, pass, provW.Database)
	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("PG health query after wake: one=%d err=%v", one, err)
	}
	_ = conn.Close(ctx)

	// Offload should have been cancelled / superseded by wake, not succeeded after wake.
	st, err := cp.GetLatestOffloadStatus(ctx, acct, tid)
	if err == nil && st != nil && st.State == "succeeded" {
		t.Fatalf("expected cancelled/failed offload after WakeTenant, got %+v", st)
	}
}

func TestPhase12ConcurrentSuspendWakeAndDeleteOffload(t *testing.T) {
	r2 := newMockR2()
	defer r2.Close()

	_, _, _, cp, _ := setupPhase12Env(t, r2, true)
	cp.coldRetryDelay = 10 * time.Millisecond
	cp.coldMaxAttempts = 1
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cp.StartColdOffloadWorker(ctx, 2)

	const acct = "acct_p12_conc"
	const tid = "t_conc_swd"
	if _, err := cp.ProvisionDatabase(ctx, acct, "idem-conc-1", tid); err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}

	r2.putDelay.Store(int64(150 * time.Millisecond))

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = cp.SuspendTenant(ctx, tid, 1*time.Second, nil)
		}()
		go func() {
			defer wg.Done()
			_, _, _, _ = cp.WakeTenant(ctx, tid)
		}()
	}
	wg.Wait()

	// Concurrent Delete while offload might be running.
	_ = cp.SuspendTenant(ctx, tid, 1*time.Second, nil)
	if err := cp.DeleteDatabase(ctx, acct, tid); err != nil {
		t.Fatalf("DeleteDatabase after concurrent suspend/wake: %v", err)
	}
}

func TestPhase12OffloadReliabilityRetryAndHTTPStatus(t *testing.T) {
	r2 := newMockR2()
	defer r2.Close()

	// Part A: Sync-mode (no cold queue worker started) MUST surface R2 PUT error!
	_, _, _, cpSync, _ := setupPhase12Env(t, r2, true)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const acct = "acct_p12_rel"
	const tidSync = "t_sync_fail"
	if _, err := cpSync.ProvisionDatabase(ctx, acct, "idem-sync-1", tidSync); err != nil {
		t.Fatalf("ProvisionDatabase sync: %v", err)
	}
	r2.failNextPut.Store(50)
	if err := cpSync.SuspendTenant(ctx, tidSync, 2*time.Second, nil); err == nil {
		t.Fatal("expected sync-mode SuspendTenant to fail when R2 PUT fails (no false success)")
	}
	stSync, err := cpSync.GetLatestOffloadStatus(ctx, acct, tidSync)
	if err != nil {
		t.Fatalf("GetLatestOffloadStatus sync: %v", err)
	}
	if stSync.State != "failed" || stSync.ErrorDetails == "" {
		t.Fatalf("expected failed offload status with error details, got %+v", stSync)
	}
	r2.failNextPut.Store(0)

	// Part B: Async-mode bounded retry success and exhaustion + HTTP endpoints.
	_, _, _, cpAsync, _ := setupPhase12Env(t, r2, true)
	cpAsync.coldRetryDelay = 15 * time.Millisecond
	cpAsync.coldMaxAttempts = 3
	cpAsync.StartColdOffloadWorker(ctx, 1)

	router := cpAsync.HTTPHandler()
	apiKey, err := cpAsync.CreateAPIKey(ctx, acct, "p12-test")
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}

	// 1. Transient R2 failures (2 failures, 3rd attempt succeeds).
	const tidRetryOK = "t_retry_ok"
	if _, err := cpAsync.ProvisionDatabase(ctx, acct, "idem-retry-ok", tidRetryOK); err != nil {
		t.Fatalf("ProvisionDatabase retry-ok: %v", err)
	}
	r2.failKeySub = "skeleton"
	r2.failNextPut.Store(2)
	if err := cpAsync.SuspendTenant(ctx, tidRetryOK, 2*time.Second, nil); err != nil {
		t.Fatalf("SuspendTenant retry-ok: %v", err)
	}
	stOK := waitForOffloadState(t, cpAsync, acct, tidRetryOK, "succeeded")
	r2.failKeySub = ""
	if stOK.RetryCount < 1 {
		t.Fatalf("expected retry_count >= 1 after transient R2 failures, got %+v", stOK)
	}

	// Verify HTTP status endpoints.
	req := httptest.NewRequest(http.MethodGet, "/v1/databases/"+tidRetryOK+"/offload", nil)
	req.Header.Set("Authorization", "Bearer "+apiKey.Token)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/databases/%s/offload status=%d body=%s", tidRetryOK, rec.Code, rec.Body.String())
	}
	var gotHTTP OffloadStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &gotHTTP); err != nil {
		t.Fatalf("unmarshal offload status: %v", err)
	}
	if gotHTTP.OpID != stOK.OpID || gotHTTP.State != "succeeded" {
		t.Fatalf("HTTP offload status mismatch: %+v vs %+v", gotHTTP, stOK)
	}

	reqOp := httptest.NewRequest(http.MethodGet, "/internal/v1/offloads/"+stOK.OpID, nil)
	reqOp.Header.Set("X-Air-Internal-Token", "test-internal-tok")
	recOp := httptest.NewRecorder()
	router.ServeHTTP(recOp, reqOp)
	if recOp.Code != http.StatusOK {
		t.Fatalf("GET /internal/v1/offloads/%s status=%d", stOK.OpID, recOp.Code)
	}

	// 2. Permanent R2 failure -> retry exhaustion.
	const tidRetryFail = "t_retry_fail"
	if _, err := cpAsync.ProvisionDatabase(ctx, acct, "idem-retry-fail", tidRetryFail); err != nil {
		t.Fatalf("ProvisionDatabase retry-fail: %v", err)
	}
	r2.failNextPut.Store(100)
	if err := cpAsync.SuspendTenant(ctx, tidRetryFail, 2*time.Second, nil); err != nil {
		t.Fatalf("SuspendTenant retry-fail: %v", err)
	}
	stFail := waitForOffloadState(t, cpAsync, acct, tidRetryFail, "failed")
	if stFail.RetryCount != cpAsync.coldMaxAttempts-1 || stFail.ErrorDetails == "" {
		t.Fatalf("expected exhausted retry_count=%d with error_details, got %+v", cpAsync.coldMaxAttempts-1, stFail)
	}
	r2.failNextPut.Store(0)
}

func TestPhase12OffloadStatusAfterRestart(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/postgres"); err != nil {
		t.Skip("PostgreSQL 17 binaries not installed")
	}
	ctx := context.Background()
	r2 := newMockR2()
	defer r2.Close()
	r2.putDelay.Store(int64(4 * time.Second))

	tmpDir, err := os.MkdirTemp("/tmp", "air-p12-restart-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatalf("NewEnvelopeKMS: %v", err)
	}
	metaPort := freeTCPPort(t)
	newCP := func() (*cell.Agent, *ControlPlane, *pageserver.Pageserver) {
		ag, err := cell.NewAgent(filepath.Join(tmpDir, "vol"), "/usr/lib/postgresql/17/bin", kms)
		if err != nil {
			t.Fatalf("NewAgent: %v", err)
		}
		cp, err := NewControlPlaneWithConfig(ctx, ControlPlaneConfig{
			MetaDir:        filepath.Join(tmpDir, "meta"),
			PGBinDir:       "/usr/lib/postgresql/17/bin",
			MetaPort:       metaPort,
			Agent:          ag,
			KMS:            kms,
			EvictOnSuspend: true,
			InternalToken:  "test-internal-tok",
		})
		if err != nil {
			t.Fatalf("NewControlPlaneWithConfig: %v", err)
		}
		cp.SetBackupStore(r2.Client(), "air-bucket")
		ps, err := pageserver.New(pageserver.Config{
			SocketPath:    filepath.Join(tmpDir, "ps.sock"),
			LayerDir:      filepath.Join(tmpDir, "ps_layers"),
			CacheDir:      filepath.Join(tmpDir, "ps_cache"),
			CacheMaxBytes: 32 * 1024 * 1024,
			S3Client:      r2.Client(),
			R2Bucket:      "air-bucket",
			KMS:           kms,
		})
		if err != nil {
			t.Fatalf("pageserver.New: %v", err)
		}
		if err := ps.Start(ctx); err != nil {
			t.Fatalf("ps.Start: %v", err)
		}
		cp.SetPageserver(ps)
		return ag, cp, ps
	}

	ag1, cp1, ps1 := newCP()
	cp1.StartColdOffloadWorker(ctx, 1)
	const acct = "acct_p12_restart"
	const tid = "t_restart_offload"
	if _, err := cp1.ProvisionDatabase(ctx, acct, "idem-restart-offload", tid); err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}
	if err := cp1.SuspendTenant(ctx, tid, 2*time.Second, nil); err != nil {
		t.Fatalf("SuspendTenant: %v", err)
	}
	// Wait until offload op is recorded before simulating abrupt shutdown/restart.
	_ = waitForOffloadState(t, cp1, acct, tid, "pending", "running", "succeeded", "failed", "cancelled")

	r2.putDelay.Store(0)
	cp1.Close()
	ps1.Close()
	ag1.Close()

	ag2, cp2, ps2 := newCP()
	defer ag2.Close()
	defer ps2.Close()
	defer cp2.Close()

	st, err := cp2.GetLatestOffloadStatus(ctx, acct, tid)
	if err != nil {
		t.Fatalf("GetLatestOffloadStatus after restart: %v", err)
	}
	if st.State != "failed" && st.State != "cancelled" && st.State != "succeeded" {
		t.Fatalf("expected terminal offload state after restart, got %+v", st)
	}
	if (st.State == "failed" || st.State == "cancelled") && st.ErrorDetails == "" {
		t.Fatalf("expected non-empty error_details on interrupted offload after restart, got %+v", st)
	}
}
