package e2e

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
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
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/control"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
)

type e2eR2Object struct {
	data           []byte
	etag           string
	checksumSHA256 string
	metadata       map[string]string
}

type e2eMultipart struct {
	key      string
	metadata map[string]string
	parts    map[int][]byte
}

func startE2EMockR2(t *testing.T) (*s3.Client, map[string]*e2eR2Object, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	objects := make(map[string]*e2eR2Object)
	multiparts := make(map[string]*e2eMultipart)
	nextUpload := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trimmed := strings.TrimPrefix(r.URL.Path, "/")
		var key string
		if slash := strings.IndexByte(trimmed, '/'); slash >= 0 {
			key = trimmed[slash+1:]
		}
		q := r.URL.Query()

		switch r.Method {
		case http.MethodHead:
			mu.Lock()
			obj, ok := objects[key]
			mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			for mk, mv := range obj.metadata {
				w.Header().Set("X-Amz-Meta-"+mk, mv)
			}
			if obj.checksumSHA256 != "" {
				w.Header().Set("X-Amz-Checksum-Sha256", obj.checksumSHA256)
			}
			w.Header().Set("ETag", obj.etag)
			w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			if q.Get("list-type") == "2" {
				prefix := q.Get("prefix")
				type s3Obj struct {
					Key  string `xml:"Key"`
					Size int    `xml:"Size"`
					ETag string `xml:"ETag"`
				}
				type listResp struct {
					XMLName     xml.Name `xml:"ListBucketResult"`
					IsTruncated bool     `xml:"IsTruncated"`
					Contents    []s3Obj  `xml:"Contents"`
				}
				mu.Lock()
				var out listResp
				keys := make([]string, 0, len(objects))
				for k := range objects {
					if strings.HasPrefix(k, prefix) {
						keys = append(keys, k)
					}
				}
				sort.Strings(keys)
				for _, k := range keys {
					o := objects[k]
					out.Contents = append(out.Contents, s3Obj{Key: k, Size: len(o.data), ETag: o.etag})
				}
				mu.Unlock()
				w.Header().Set("Content-Type", "application/xml")
				_ = xml.NewEncoder(w).Encode(out)
				return
			}
			mu.Lock()
			obj, ok := objects[key]
			mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
				spec := strings.TrimPrefix(rng, "bytes=")
				parts := strings.SplitN(spec, "-", 2)
				start, _ := strconv.Atoi(parts[0])
				end, _ := strconv.Atoi(parts[1])
				if end >= len(obj.data) {
					end = len(obj.data) - 1
				}
				slice := obj.data[start : end+1]
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.data)))
				w.Header().Set("Content-Length", strconv.Itoa(len(slice)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(slice)
				return
			}
			w.Header().Set("ETag", obj.etag)
			w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(obj.data)

		case http.MethodPost:
			if q.Has("uploads") {
				mu.Lock()
				nextUpload++
				uid := fmt.Sprintf("up-%d", nextUpload)
				meta := make(map[string]string)
				for hk, hvals := range r.Header {
					lk := strings.ToLower(hk)
					if strings.HasPrefix(lk, "x-amz-meta-") && len(hvals) > 0 {
						meta[strings.TrimPrefix(lk, "x-amz-meta-")] = hvals[0]
					}
				}
				multiparts[uid] = &e2eMultipart{key: key, metadata: meta, parts: make(map[int][]byte)}
				mu.Unlock()
				type initResp struct {
					XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
					UploadId string   `xml:"UploadId"`
				}
				w.Header().Set("Content-Type", "application/xml")
				_ = xml.NewEncoder(w).Encode(initResp{UploadId: uid})
				return
			}
			if uid := q.Get("uploadId"); uid != "" {
				mu.Lock()
				if r.Header.Get("If-None-Match") == "*" {
					if _, exists := objects[key]; exists {
						mu.Unlock()
						w.WriteHeader(http.StatusPreconditionFailed)
						return
					}
				}
				mp, ok := multiparts[uid]
				if !ok {
					mu.Unlock()
					w.WriteHeader(http.StatusNotFound)
					return
				}
				partNums := make([]int, 0, len(mp.parts))
				for pn := range mp.parts {
					partNums = append(partNums, pn)
				}
				sort.Ints(partNums)
				var combined bytes.Buffer
				for _, pn := range partNums {
					combined.Write(mp.parts[pn])
				}
				delete(multiparts, uid)
				data := combined.Bytes()
				sum := md5.Sum(data)
				etag := fmt.Sprintf(`"%s-%d"`, hex.EncodeToString(sum[:]), len(partNums))
				sha := sha256.Sum256(data)
				objects[key] = &e2eR2Object{
					data:           data,
					etag:           etag,
					checksumSHA256: base64.StdEncoding.EncodeToString(sha[:]),
					metadata:       mp.metadata,
				}
				mu.Unlock()
				type compResp struct {
					XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
					ETag    string   `xml:"ETag"`
				}
				w.Header().Set("Content-Type", "application/xml")
				_ = xml.NewEncoder(w).Encode(compResp{ETag: etag})
				return
			}

		case http.MethodPut:
			if uid := q.Get("uploadId"); uid != "" {
				pn, _ := strconv.Atoi(q.Get("partNumber"))
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				if mp, ok := multiparts[uid]; ok {
					mp.parts[pn] = body
				}
				mu.Unlock()
				sum := md5.Sum(body)
				w.Header().Set("ETag", fmt.Sprintf(`"%s"`, hex.EncodeToString(sum[:])))
				w.WriteHeader(http.StatusOK)
				return
			}
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			defer mu.Unlock()
			if r.Header.Get("If-None-Match") == "*" {
				if _, exists := objects[key]; exists {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}
			if im := r.Header.Get("If-Match"); im != "" {
				cur, exists := objects[key]
				if !exists || cur.etag != im {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}
			sum := md5.Sum(body)
			etag := fmt.Sprintf(`"%s"`, hex.EncodeToString(sum[:]))
			sha := sha256.Sum256(body)
			meta := make(map[string]string)
			for hk, hvals := range r.Header {
				lk := strings.ToLower(hk)
				if strings.HasPrefix(lk, "x-amz-meta-") && len(hvals) > 0 {
					meta[strings.TrimPrefix(lk, "x-amz-meta-")] = hvals[0]
				}
			}
			objects[key] = &e2eR2Object{
				data:           body,
				etag:           etag,
				checksumSHA256: base64.StdEncoding.EncodeToString(sha[:]),
				metadata:       meta,
			}
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusOK)

		case http.MethodDelete:
			mu.Lock()
			delete(objects, key)
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(srv.Close)

	s3c := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("e2e-ak", "e2e-sk", ""),
	})
	return s3c, objects, &mu
}

func queryPageserverSock(t *testing.T, sockPath, tenantID string, op uint16, spc, db, rel uint32, fork uint8, blk, nblk uint32) (uint16, uint32, []byte) {
	t.Helper()
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("Dial %s: %v", sockPath, err)
	}
	defer conn.Close()

	var req [pageserver.ReqHeaderSize]byte
	binary.LittleEndian.PutUint32(req[0:4], pageserver.ProtocolMagic)
	binary.LittleEndian.PutUint16(req[4:6], pageserver.ProtocolVersion)
	binary.LittleEndian.PutUint16(req[6:8], op)
	copy(req[8:8+pageserver.TenantIDMaxLen], tenantID)
	binary.LittleEndian.PutUint32(req[72:76], spc)
	binary.LittleEndian.PutUint32(req[76:80], db)
	binary.LittleEndian.PutUint32(req[80:84], rel)
	req[84] = fork
	binary.LittleEndian.PutUint32(req[88:92], blk)
	binary.LittleEndian.PutUint32(req[92:96], nblk)

	if _, err := conn.Write(req[:]); err != nil {
		t.Fatalf("Write req: %v", err)
	}
	var resp [pageserver.RespHeaderSize]byte
	if _, err := io.ReadFull(conn, resp[:]); err != nil {
		t.Fatalf("ReadFull resp: %v", err)
	}
	status := binary.LittleEndian.Uint16(resp[6:8])
	pLen := binary.LittleEndian.Uint32(resp[8:12])
	val := binary.LittleEndian.Uint32(resp[12:16])
	var payload []byte
	if pLen > 0 {
		payload = make([]byte, pLen)
		if _, err := io.ReadFull(conn, payload); err != nil {
			t.Fatalf("ReadFull payload: %v", err)
		}
	}
	return status, val, payload
}

func TestPageserverCAU_LivePG17_LifecycleAndRestart(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/postgres"); err != nil {
		t.Skip("PostgreSQL 17 binaries not installed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir, err := os.MkdirTemp("/tmp", "air-cau-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	s3c, mockObjects, mockMu := startE2EMockR2(t)
	bucket := "air-cau-e2e"

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	ag, err := cell.NewAgent(filepath.Join(tmpDir, "vol"), "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatal(err)
	}
	defer ag.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metaPort := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	cp, err := control.NewControlPlane(ctx, filepath.Join(tmpDir, "meta"), "/usr/lib/postgresql/17/bin", metaPort, ag, kms)
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	cp.SetBackupStore(s3c, bucket)

	psSock := filepath.Join(tmpDir, "ps1.sock")
	ps, err := pageserver.New(pageserver.Config{
		SocketPath:    psSock,
		LayerDir:      filepath.Join(tmpDir, "ps_layers_1"),
		CacheDir:      filepath.Join(tmpDir, "ps_cache_1"),
		CacheMaxBytes: 32 * 1024 * 1024,
		S3Client:      s3c,
		R2Bucket:      bucket,
		KMS:           kms,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ps.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	cp.SetPageserver(ps)

	const accountID = "acct_cau_e2e"
	const tenantID = "t_cau_e2e"

	// 1. Provision live PG17 tenant -> bootstraps initial CAU Image layer + manifest in R2.
	resp, err := cp.ProvisionDatabase(ctx, accountID, "idem-cau-1", tenantID)
	if err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}

	// 2. Write data to live PG17 instance.
	dsn := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", resp.Password, resp.DirectPort, resp.Database)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		CREATE TABLE cau_items (id int PRIMARY KEY, payload text);
		INSERT INTO cau_items SELECT g, repeat('x', 200) FROM generate_series(1, 200) g;
	`); err != nil {
		conn.Close(ctx)
		t.Fatalf("create/insert cau_items: %v", err)
	}
	var dbOID, relNode uint32
	if err := conn.QueryRow(ctx, `
		SELECT (SELECT oid FROM pg_database WHERE datname = current_database())::int4,
		       pg_relation_filenode('cau_items')::int4
	`).Scan(&dbOID, &relNode); err != nil {
		conn.Close(ctx)
		t.Fatalf("query oid/relnode: %v", err)
	}
	conn.Close(ctx)

	// Wait briefly for WALReceiver to ingest the CREATE/INSERT WAL records.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		st, val, _ := queryPageserverSock(t, psSock, tenantID, pageserver.OpGetRelSize, 1663, dbOID, relNode, 0, 0, 0)
		if st == pageserver.StatusOK && val >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 3. Suspend tenant -> flushes L0 CAU layer, uploads + verifies in R2, publishes manifest.
	if err := cp.SuspendTenant(ctx, tenantID, 5*time.Second, nil); err != nil {
		t.Fatalf("SuspendTenant: %v", err)
	}

	// Verify manifest and CAU objects exist in R2.
	manifestKey := pageserver.LayerManifestR2Key(tenantID, resp.FencingEpoch)
	mockMu.Lock()
	_, hasManifest := mockObjects[manifestKey]
	var cauKeys []string
	for k := range mockObjects {
		if pageserver.IsContentAddressedLayerKey(k) {
			cauKeys = append(cauKeys, k)
		}
	}
	mockMu.Unlock()
	if !hasManifest {
		t.Fatalf("expected published manifest %s in R2", manifestKey)
	}
	if len(cauKeys) < 2 {
		t.Fatalf("expected at least 2 CAU layer objects (bootstrap + L0) in R2, got %d (%v)", len(cauKeys), cauKeys)
	}

	// 4. Simulate full Pageserver restart with empty local disk & cache.
	_ = ps.Close()
	psSock2 := filepath.Join(tmpDir, "ps2.sock")
	ps2, err := pageserver.New(pageserver.Config{
		SocketPath:    psSock2,
		LayerDir:      filepath.Join(tmpDir, "ps_layers_2"),
		CacheDir:      filepath.Join(tmpDir, "ps_cache_2"),
		CacheMaxBytes: 32 * 1024 * 1024,
		S3Client:      s3c,
		R2Bucket:      bucket,
		KMS:           kms,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ps2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ps2.Close()

	tl2 := ps2.EnsureTenant(tenantID, resp.FencingEpoch)
	if err := tl2.LoadLayersFromR2(ctx); err != nil {
		t.Fatalf("LoadLayersFromR2 after restart: %v", err)
	}
	if tl2.LayerCount() < 2 {
		t.Fatalf("expected >= 2 layers loaded after restart, got %d", tl2.LayerCount())
	}

	// Query restarted Pageserver over Unix socket -> must reconstruct page from encrypted CAU layers in R2!
	st, nblocks, _ := queryPageserverSock(t, psSock2, tenantID, pageserver.OpGetRelSize, 1663, dbOID, relNode, 0, 0, 0)
	if st != pageserver.StatusOK || nblocks < 1 {
		t.Fatalf("OpGetRelSize after restart failed: st=%d nblocks=%d", st, nblocks)
	}
	st, _, pageBytes := queryPageserverSock(t, psSock2, tenantID, pageserver.OpGetPageAtLSN, 1663, dbOID, relNode, 0, 0, 1)
	if st != pageserver.StatusOK || len(pageBytes) != pageserver.PageSize {
		t.Fatalf("OpGetPageAtLSN after restart failed: st=%d len=%d", st, len(pageBytes))
	}
}

func wipeR2Bucket(ctx context.Context, t *testing.T, s3c *s3.Client, bucket string) int {
	t.Helper()
	// Abort any incomplete multipart uploads first.
	mpOut, err := s3c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
	if err == nil {
		for _, up := range mpOut.Uploads {
			_, _ = s3c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      up.Key,
				UploadId: up.UploadId,
			})
		}
	}

	deleted := 0
	var token *string
	for {
		out, err := s3c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: token,
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 wipe %s: %v", bucket, err)
		}
		for _, obj := range out.Contents {
			if obj.Key == nil {
				continue
			}
			if _, err := s3c.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			}); err != nil {
				t.Fatalf("DeleteObject %s: %v", *obj.Key, err)
			}
			deleted++
		}
		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}
	return deleted
}

func TestPageserverCAU_RealR2_LivePG17_CreateDBAndInsert(t *testing.T) {
	ak := os.Getenv("ACCESS_KEY")
	sk := os.Getenv("SECRET_KEY")
	ep := os.Getenv("ENDPOINT")
	if ak == "" || sk == "" || ep == "" {
		t.Skip("R2 credentials (ACCESS_KEY, SECRET_KEY, ENDPOINT) not set")
	}
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/postgres"); err != nil {
		t.Skip("PostgreSQL 17 binaries not installed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	s3c := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(ep),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(ak, sk, ""),
	})

	bucket := os.Getenv("AIR_R2_BUCKET")
	if bucket == "" {
		lb, err := s3c.ListBuckets(ctx, &s3.ListBucketsInput{})
		if err != nil || len(lb.Buckets) == 0 {
			t.Fatalf("ListBuckets failed or empty: err=%v", err)
		}
		bucket = aws.ToString(lb.Buckets[0].Name)
	}

	// 1. Wipe all existing data from R2 bucket before starting.
	wiped := wipeR2Bucket(ctx, t, s3c, bucket)
	t.Logf("Wiped %d existing objects from R2 bucket %s", wiped, bucket)

	tmpDir, err := os.MkdirTemp("/tmp", "air-cau-real-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	ag, err := cell.NewAgent(filepath.Join(tmpDir, "vol"), "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatal(err)
	}
	defer ag.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metaPort := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	cp, err := control.NewControlPlane(ctx, filepath.Join(tmpDir, "meta"), "/usr/lib/postgresql/17/bin", metaPort, ag, kms)
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	cp.SetBackupStore(s3c, bucket)

	psSock := filepath.Join(tmpDir, "ps1.sock")
	layerDir1 := filepath.Join(tmpDir, "ps_layers_1")
	cacheDir1 := filepath.Join(tmpDir, "ps_cache_1")
	ps, err := pageserver.New(pageserver.Config{
		SocketPath:    psSock,
		LayerDir:      layerDir1,
		CacheDir:      cacheDir1,
		CacheMaxBytes: 64 * 1024 * 1024,
		S3Client:      s3c,
		R2Bucket:      bucket,
		KMS:           kms,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ps.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	cp.SetPageserver(ps)

	const accountID = "acct_real_r2"
	const tenantID = "t_real_r2"

	// 2. Create a database -> bootstraps initial Image layer via multipart CAU to real R2.
	prov, err := cp.ProvisionDatabase(ctx, accountID, "idem-real-r2-1", tenantID)
	if err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}

	// 3. Connect to live PG17 and insert data.
	dsn := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", prov.Password, prov.DirectPort, prov.Database)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		CREATE TABLE real_orders (id int PRIMARY KEY, customer text, amount int);
		INSERT INTO real_orders SELECT g, 'cust_' || g, g * 10 FROM generate_series(1, 500) g;
	`); err != nil {
		conn.Close(ctx)
		t.Fatalf("CREATE/INSERT real_orders: %v", err)
	}
	var rowCount int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM real_orders`).Scan(&rowCount); err != nil || rowCount != 500 {
		conn.Close(ctx)
		t.Fatalf("SELECT count(*) got=%d err=%v", rowCount, err)
	}
	var dbOID, relNode uint32
	if err := conn.QueryRow(ctx, `
		SELECT (SELECT oid FROM pg_database WHERE datname = current_database())::int4,
		       pg_relation_filenode('real_orders')::int4
	`).Scan(&dbOID, &relNode); err != nil {
		conn.Close(ctx)
		t.Fatalf("query dbOID/relNode: %v", err)
	}
	conn.Close(ctx)

	// Wait for WALReceiver to ingest the table creation & inserts.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		st, val, _ := queryPageserverSock(t, psSock, tenantID, pageserver.OpGetRelSize, 1663, dbOID, relNode, 0, 0, 0)
		if st == pageserver.StatusOK && val >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 4. Suspend tenant -> flushes L0 CAU layer and publishes manifest to real R2.
	if err := cp.SuspendTenant(ctx, tenantID, 5*time.Second, nil); err != nil {
		t.Fatalf("SuspendTenant: %v", err)
	}

	// 5. Inspect real R2 objects created.
	listOut, err := s3c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	var cauCount int
	var hasManifest bool
	for _, obj := range listOut.Contents {
		k := aws.ToString(obj.Key)
		t.Logf("R2 object: key=%s size=%d", k, aws.ToInt64(obj.Size))
		if pageserver.IsContentAddressedLayerKey(k) {
			cauCount++
		}
		if k == pageserver.LayerManifestR2Key(tenantID, prov.FencingEpoch) {
			hasManifest = true
		}
	}
	if !hasManifest || cauCount < 2 {
		t.Fatalf("expected manifest (%v) and >=2 CAU layers in R2, got cauCount=%d", hasManifest, cauCount)
	}

	// 6. Wipe local layer & cache dirs and restart a fresh Pageserver reading only from real R2.
	_ = ps.Close()
	_ = os.RemoveAll(layerDir1)
	_ = os.RemoveAll(cacheDir1)

	psSock2 := filepath.Join(tmpDir, "ps2.sock")
	ps2, err := pageserver.New(pageserver.Config{
		SocketPath:    psSock2,
		LayerDir:      filepath.Join(tmpDir, "ps_layers_2"),
		CacheDir:      filepath.Join(tmpDir, "ps_cache_2"),
		CacheMaxBytes: 64 * 1024 * 1024,
		S3Client:      s3c,
		R2Bucket:      bucket,
		KMS:           kms,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ps2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ps2.Close()

	tl2 := ps2.EnsureTenant(tenantID, prov.FencingEpoch)
	if err := tl2.LoadLayersFromR2(ctx); err != nil {
		t.Fatalf("LoadLayersFromR2 from real R2: %v", err)
	}
	if tl2.LayerCount() < 2 {
		t.Fatalf("expected >= 2 layers loaded from real R2, got %d", tl2.LayerCount())
	}

	st, nblocks, _ := queryPageserverSock(t, psSock2, tenantID, pageserver.OpGetRelSize, 1663, dbOID, relNode, 0, 0, 0)
	if st != pageserver.StatusOK || nblocks < 1 {
		t.Fatalf("OpGetRelSize from real R2 failed: st=%d nblocks=%d", st, nblocks)
	}
	st, _, pageBytes := queryPageserverSock(t, psSock2, tenantID, pageserver.OpGetPageAtLSN, 1663, dbOID, relNode, 0, 0, 1)
	if st != pageserver.StatusOK || len(pageBytes) != pageserver.PageSize {
		t.Fatalf("OpGetPageAtLSN from real R2 failed: st=%d len=%d", st, len(pageBytes))
	}
	t.Logf("Real R2 live PG17 E2E passed: bucket=%s cau_layers=%d rel_blocks=%d", bucket, cauCount, nblocks)
}
