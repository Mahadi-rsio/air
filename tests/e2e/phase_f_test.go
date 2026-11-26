package e2e_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Mahadi-rsio/air/internal/backup"
	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/control"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/fencing"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/klauspost/compress/zstd"
)

type mockS3Obj struct {
	data        []byte
	etag        string
	contentType string
	meta        map[string]string
}

type mockS3Server struct {
	mu      sync.Mutex
	objects map[string]mockS3Obj
}

func startMockS3(t *testing.T) (*httptest.Server, *s3.Client, *mockS3Server) {
	t.Helper()
	m := &mockS3Server{objects: make(map[string]mockS3Obj)}
	srv := httptest.NewServer(http.HandlerFunc(m.handle))
	cfg := aws.Config{
		Region:      "auto",
		Credentials: credentials.NewStaticCredentialsProvider("test-ak", "test-sk", ""),
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
	})
	return srv, client, m
}

func (m *mockS3Server) hasKey(bucket, key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objects[bucket+"/"+key]
	return ok
}

func (m *mockS3Server) handle(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	bucket := parts[0]
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}
	fullKey := bucket + "/" + key

	m.mu.Lock()
	defer m.mu.Unlock()

	if r.Method == http.MethodGet && key == "" && r.URL.Query().Get("list-type") == "2" {
		prefix := bucket + "/" + r.URL.Query().Get("prefix")
		type contents struct {
			Key  string `xml:"Key"`
			Size int    `xml:"Size"`
			ETag string `xml:"ETag"`
		}
		type listResp struct {
			XMLName  xml.Name   `xml:"ListBucketResult"`
			Name     string     `xml:"Name"`
			Contents []contents `xml:"Contents"`
		}
		resp := listResp{Name: bucket}
		var keys []string
		for k := range m.objects {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			obj := m.objects[k]
			resp.Contents = append(resp.Contents, contents{
				Key:  strings.TrimPrefix(k, bucket+"/"),
				Size: len(obj.data),
				ETag: obj.etag,
			})
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(resp)
		return
	}

	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		existing, exists := m.objects[fullKey]
		if r.Header.Get("If-None-Match") == "*" && exists {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
			return
		}
		if im := r.Header.Get("If-Match"); im != "" {
			if !exists || strings.Trim(im, `"`) != strings.Trim(existing.etag, `"`) {
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
				return
			}
		}
		sum := md5.Sum(body)
		etag := `"` + hex.EncodeToString(sum[:]) + `"`
		meta := make(map[string]string)
		for hk, hv := range r.Header {
			lower := strings.ToLower(hk)
			if strings.HasPrefix(lower, "x-amz-meta-") && len(hv) > 0 {
				meta[strings.TrimPrefix(lower, "x-amz-meta-")] = hv[0]
			}
		}
		m.objects[fullKey] = mockS3Obj{
			data:        body,
			etag:        etag,
			contentType: r.Header.Get("Content-Type"),
			meta:        meta,
		}
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		obj, exists := m.objects[fullKey]
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
			return
		}
		for mk, mv := range obj.meta {
			w.Header().Set("X-Amz-Meta-"+mk, mv)
		}
		w.Header().Set("ETag", obj.etag)
		if obj.contentType != "" {
			w.Header().Set("Content-Type", obj.contentType)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(obj.data)

	case http.MethodHead:
		obj, exists := m.objects[fullKey]
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		for mk, mv := range obj.meta {
			w.Header().Set("X-Amz-Meta-"+mk, mv)
		}
		w.Header().Set("ETag", obj.etag)
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		delete(m.objects, fullKey)
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestPhaseF_LivePG17_Backup_WAL_PITR_And_CASFencing(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/initdb"); err != nil {
		t.Fatalf("PostgreSQL 17 is required: %v", err)
	}

	ctx := context.Background()
	rootDir, err := os.MkdirTemp("/tmp", "air_phase_f_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	srv, s3c, mock := startMockS3(t)
	defer srv.Close()
	bucket := "air-phase-f"

	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(rootDir, "master.key"))
	if err != nil {
		t.Fatal(err)
	}
	ag, err := cell.NewAgent(rootDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatal(err)
	}
	defer ag.Close()

	tenantID := "t_phasef_pitr"
	if _, err := ag.ClaimCluster(tenantID); err != nil {
		t.Fatalf("ClaimCluster: %v", err)
	}
	rt, err := ag.ActivateCluster(tenantID, "pw_phasef", 1)
	if err != nil {
		t.Fatalf("ActivateCluster: %v", err)
	}
	defer func() { _ = ag.StopAndPurgeTenant(tenantID) }()

	dsn := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres", rt.SockDir, rt.Port)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer conn.Close(ctx)

	// 1. Insert 1,000 rows -> FULL backup B0
	if _, err := conn.Exec(ctx, `
		CREATE TABLE pitr_orders (id INT PRIMARY KEY, batch TEXT NOT NULL, payload TEXT NOT NULL);
		INSERT INTO pitr_orders SELECT g, 'B0_FULL', md5(g::text) FROM generate_series(1, 1000) g;
	`); err != nil {
		t.Fatalf("seed B0: %v", err)
	}

	b0Art, b0Manifest, err := backup.RunEncryptedBackupStep(ctx, ag, s3c, bucket, tenantID, "B0_full", "", "FULL", "")
	if err != nil {
		t.Fatalf("RunEncryptedBackupStep B0: %v", err)
	}

	// Verify atomic manifest commit after B0
	m0, etag0, err := fencing.ReadManifestWithETag(ctx, s3c, bucket, tenantID)
	if err != nil || len(m0.Backups) != 1 || m0.Backups[0].BackupID != "B0_full" || etag0 == "" {
		t.Fatalf("expected B0 in manifest, got m=%+v etag=%q err=%v", m0, etag0, err)
	}

	// 2. Insert 500 rows -> INCR backup I1
	if _, err := conn.Exec(ctx, `INSERT INTO pitr_orders SELECT g, 'I1_INCR', md5(g::text) FROM generate_series(1001, 1500) g;`); err != nil {
		t.Fatalf("seed I1: %v", err)
	}
	i1Art, _, err := backup.RunEncryptedBackupStep(ctx, ag, s3c, bucket, tenantID, "I1_incr", b0Art.BackupID, "INCR", b0Manifest)
	if err != nil {
		t.Fatalf("RunEncryptedBackupStep I1: %v", err)
	}
	if i1Art.Type != "INCR" || i1Art.ParentID != "B0_full" {
		t.Fatalf("expected INCR backup chained to B0_full, got %+v", i1Art)
	}

	m1, etag1, err := fencing.ReadManifestWithETag(ctx, s3c, bucket, tenantID)
	if err != nil || len(m1.Backups) != 2 || etag1 == etag0 {
		t.Fatalf("expected 2 backups and rotated ETag, got len=%d etag=%q err=%v", len(m1.Backups), etag1, err)
	}

	// 3. Insert 300 rows -> capture LSN -> DROP TABLE
	if _, err := conn.Exec(ctx, `INSERT INTO pitr_orders SELECT g, 'POST_I1_WAL', md5(g::text) FROM generate_series(1501, 1800) g;`); err != nil {
		t.Fatalf("seed post-I1: %v", err)
	}
	var targetLSN string
	if err := conn.QueryRow(ctx, `SELECT pg_current_wal_insert_lsn()::text;`).Scan(&targetLSN); err != nil {
		t.Fatalf("capture LSN: %v", err)
	}
	if _, err := conn.Exec(ctx, `DROP TABLE pitr_orders;`); err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}
	_ = conn.Close(ctx)

	// 4. Drain encrypted WAL spool to R2
	walSegs, err := backup.DrainAndEncryptWALSpoolToR2(ctx, ag, s3c, bucket, tenantID)
	if err != nil || len(walSegs) == 0 {
		t.Fatalf("DrainAndEncryptWALSpoolToR2: len=%d err=%v", len(walSegs), err)
	}

	// 5. Combine B0 + I1 + replay WAL up to targetLSN (recovering all 1,800 rows before DROP TABLE)
	qOut, headroomOK, amcheckOK, err := backup.CombineAndReplayPITR(
		ctx, ag, s3c, bucket, tenantID,
		[]*fencing.BackupArtifact{b0Art, i1Art},
		walSegs,
		targetLSN,
		"SELECT count(*) FROM pitr_orders;",
	)
	if err != nil {
		t.Fatalf("CombineAndReplayPITR: %v", err)
	}
	if qOut != "1800" || !headroomOK || !amcheckOK {
		t.Fatalf("unexpected PITR result: qOut=%q headroomOK=%v amcheckOK=%v", qOut, headroomOK, amcheckOK)
	}

	// 6. Verify stale epoch backup writer is rejected by CAS and cleans up
	m1.FencingEpoch = 2
	m1.HolderCell = "cell-new"
	if _, err := fencing.PutManifestCAS(ctx, s3c, bucket, m1, etag1); err != nil {
		t.Fatalf("promote manifest epoch 2: %v", err)
	}
	_, _, staleErr := backup.RunEncryptedBackupStep(ctx, ag, s3c, bucket, tenantID, "B_stale", "", "FULL", "")
	if !errors.Is(staleErr, fencing.ErrStaleFencingEpoch) {
		t.Fatalf("expected ErrStaleFencingEpoch for epoch-1 writer, got: %v", staleErr)
	}
	staleKey := backup.BackupBaseObjectKey(tenantID, "", "B_stale")
	if mock.hasKey(bucket, staleKey) {
		t.Fatalf("expected stale backup object %s to be absent/cleaned up", staleKey)
	}

	// 7. Verify crafted malicious tar archive (symlink & byte-cap) fails closed in CombineAndReplayPITRWithLimits
	var evilBuf bytes.Buffer
	zw, _ := zstd.NewWriter(&evilBuf)
	tw := tar.NewWriter(zw)
	_ = tw.WriteHeader(&tar.Header{Name: "evil_sym", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"})
	_ = tw.Close()
	_ = zw.Close()

	evilKey := "tenants/" + tenantID + "/epoch_000001/evil.tar.zst.enc"
	evilCipher, err := kms.EncryptObject(tenantID, 1, evilKey, evilBuf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(evilKey), Body: bytes.NewReader(evilCipher)})
	_, _, _, evilErr := backup.CombineAndReplayPITRWithLimits(
		ctx, ag, s3c, bucket, tenantID,
		[]*fencing.BackupArtifact{{BackupID: "evil", Type: "FULL", R2Key: evilKey, Epoch: 1, CipherBytes: len(evilCipher)}},
		nil, "", "SELECT 1;", backup.TarExtractLimits{MaxFileBytes: 1024 * 1024, MaxTotalBytes: 2 * 1024 * 1024},
	)
	if !errors.Is(evilErr, backup.ErrUnsafeTarEntry) {
		t.Fatalf("expected ErrUnsafeTarEntry for crafted symlink tar, got: %v", evilErr)
	}
}

func TestPhaseF_ControlPlane_BackupAndRestoreHTTPAPI(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/initdb"); err != nil {
		t.Fatalf("PostgreSQL 17 is required: %v", err)
	}

	ctx := context.Background()
	rootDir, err := os.MkdirTemp("/tmp", "air_phase_f_cp_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	srv, s3c, _ := startMockS3(t)
	defer srv.Close()
	bucket := "air-cp-backups"

	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(rootDir, "master.key"))
	if err != nil {
		t.Fatal(err)
	}
	ag, err := cell.NewAgent(rootDir, "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = ag.StopAndPurgeTenant("t_cp_bkp")
		ag.Close()
	}()

	cp, err := control.NewControlPlane(ctx, filepath.Join(rootDir, "meta"), "/usr/lib/postgresql/17/bin", freeTCPPort(t), ag, kms)
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	defer cp.Close()
	cp.SetBackupStore(s3c, bucket)

	keyA, err := cp.CreateAPIKey(ctx, "acct_a", "primary")
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := cp.CreateAPIKey(ctx, "acct_b", "other")
	if err != nil {
		t.Fatal(err)
	}

	httpSrv := httptest.NewServer(cp.HTTPHandler())
	defer httpSrv.Close()

	// Provision tenant t_cp_bkp under acct_a
	provReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/databases", strings.NewReader(`{"tenant_id":"t_cp_bkp"}`))
	provReq.Header.Set("Authorization", "Bearer "+keyA.Token)
	provReq.Header.Set("Idempotency-Key", "idem-cp-bkp-1")
	provResp, err := http.DefaultClient.Do(provReq)
	if err != nil || provResp.StatusCode != http.StatusCreated {
		t.Fatalf("provision failed: status=%v err=%v", provResp.StatusCode, err)
	}
	var prov control.ProvisionResponse
	_ = json.NewDecoder(provResp.Body).Decode(&prov)
	_ = provResp.Body.Close()

	// Seed 42 rows directly on tenant DB
	dsn := fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", prov.Username, prov.Password, prov.DirectPort, prov.Database)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect tenant: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE orders (id INT PRIMARY KEY); INSERT INTO orders SELECT generate_series(1, 42);`); err != nil {
		t.Fatalf("seed 42 rows: %v", err)
	}

	// Auth checks on POST /v1/databases/t_cp_bkp/backups
	unauthReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/databases/t_cp_bkp/backups", nil)
	unauthResp, _ := http.DefaultClient.Do(unauthReq)
	_ = unauthResp.Body.Close()
	if unauthResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", unauthResp.StatusCode)
	}

	forbidReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/databases/t_cp_bkp/backups", nil)
	forbidReq.Header.Set("Authorization", "Bearer "+keyB.Token)
	forbidResp, _ := http.DefaultClient.Do(forbidReq)
	_ = forbidResp.Body.Close()
	if forbidResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for acct_b, got %d", forbidResp.StatusCode)
	}

	// Trigger FULL backup via HTTP
	bkpReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/databases/t_cp_bkp/backups", strings.NewReader(`{"type":"FULL"}`))
	bkpReq.Header.Set("Authorization", "Bearer "+keyA.Token)
	bkpResp, err := http.DefaultClient.Do(bkpReq)
	if err != nil || bkpResp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(bkpResp.Body)
		t.Fatalf("FULL backup HTTP failed: status=%d err=%v body=%s", bkpResp.StatusCode, err, string(bodyBytes))
	}
	var bkpOut control.BackupResponse
	_ = json.NewDecoder(bkpResp.Body).Decode(&bkpOut)
	_ = bkpResp.Body.Close()
	if bkpOut.Type != "FULL" || bkpOut.CipherBytes <= 0 {
		t.Fatalf("unexpected FULL backup response: %+v", bkpOut)
	}

	// Insert 8 more rows (50 total) -> INCR backup via HTTP
	if _, err := conn.Exec(ctx, `INSERT INTO orders SELECT generate_series(43, 50);`); err != nil {
		t.Fatalf("insert 43..50: %v", err)
	}
	incrReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/databases/t_cp_bkp/backups", strings.NewReader(`{"type":"INCR"}`))
	incrReq.Header.Set("Authorization", "Bearer "+keyA.Token)
	incrResp, err := http.DefaultClient.Do(incrReq)
	if err != nil || incrResp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(incrResp.Body)
		t.Fatalf("INCR backup HTTP failed: status=%d err=%v body=%s", incrResp.StatusCode, err, string(bodyBytes))
	}
	var incrOut control.BackupResponse
	_ = json.NewDecoder(incrResp.Body).Decode(&incrOut)
	_ = incrResp.Body.Close()
	if incrOut.Type != "INCR" {
		t.Fatalf("unexpected INCR backup response: %+v", incrOut)
	}

	// Insert 2 more rows (52 total) AFTER I1, capture targetLSN, then DROP TABLE orders
	if _, err := conn.Exec(ctx, `INSERT INTO orders SELECT generate_series(51, 52);`); err != nil {
		t.Fatalf("insert 51..52: %v", err)
	}
	var targetLSN string
	if err := conn.QueryRow(ctx, `SELECT pg_current_wal_insert_lsn()::text;`).Scan(&targetLSN); err != nil {
		t.Fatalf("capture LSN: %v", err)
	}
	if _, err := conn.Exec(ctx, `DROP TABLE orders;`); err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}
	_ = conn.Close(ctx)

	// Drain WAL archive via HTTP
	walReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/databases/t_cp_bkp/backups", strings.NewReader(`{"type":"WAL"}`))
	walReq.Header.Set("Authorization", "Bearer "+keyA.Token)
	walResp, err := http.DefaultClient.Do(walReq)
	if err != nil || walResp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(walResp.Body)
		t.Fatalf("WAL drain HTTP failed: status=%d err=%v body=%s", walResp.StatusCode, err, string(bodyBytes))
	}
	var walOut control.BackupResponse
	_ = json.NewDecoder(walResp.Body).Decode(&walOut)
	_ = walResp.Body.Close()
	if walOut.WALSegmentsArchived == 0 {
		t.Fatalf("expected WAL segments archived > 0, got %+v", walOut)
	}

	// Trigger PITR restore via HTTP
	restoreBody := fmt.Sprintf(`{"target_lsn":%q}`, targetLSN)
	rstReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/databases/t_cp_bkp/restore", strings.NewReader(restoreBody))
	rstReq.Header.Set("Authorization", "Bearer "+keyA.Token)
	rstResp, err := http.DefaultClient.Do(rstReq)
	if err != nil || rstResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(rstResp.Body)
		t.Fatalf("restore HTTP failed: status=%d err=%v body=%s", rstResp.StatusCode, err, string(bodyBytes))
	}
	var rstOut control.RestoreResponse
	_ = json.NewDecoder(rstResp.Body).Decode(&rstOut)
	_ = rstResp.Body.Close()

	if rstOut.VerifyQueryOutput != "1" || !rstOut.StagingHeadroomOK || !rstOut.AmcheckOK || rstOut.RestoredBackups != 2 {
		t.Fatalf("unexpected restore response: %+v", rstOut)
	}
}
