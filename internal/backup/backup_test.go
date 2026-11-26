package backup

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/fencing"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/klauspost/compress/zstd"
)

func buildTarZst(t *testing.T, fn func(tw *tar.Writer)) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(zw)
	fn(tw)
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestExtractTarZstSafe_RejectsUnsafeEntriesAndEnforcesCaps(t *testing.T) {
	cases := []struct {
		name    string
		build   func(tw *tar.Writer)
		limits  TarExtractLimits
		wantErr error
	}{
		{
			name: "symlink rejected",
			build: func(tw *tar.Writer) {
				_ = tw.WriteHeader(&tar.Header{
					Name:     "evil_link",
					Typeflag: tar.TypeSymlink,
					Linkname: "/etc/passwd",
					Mode:     0o777,
				})
			},
			wantErr: ErrUnsafeTarEntry,
		},
		{
			name: "hardlink rejected",
			build: func(tw *tar.Writer) {
				_ = tw.WriteHeader(&tar.Header{
					Name:     "evil_hardlink",
					Typeflag: tar.TypeLink,
					Linkname: "/etc/shadow",
				})
			},
			wantErr: ErrUnsafeTarEntry,
		},
		{
			name: "char device rejected",
			build: func(tw *tar.Writer) {
				_ = tw.WriteHeader(&tar.Header{
					Name:     "dev_null",
					Typeflag: tar.TypeChar,
					Devmajor: 1,
					Devminor: 3,
				})
			},
			wantErr: ErrUnsafeTarEntry,
		},
		{
			name: "fifo rejected",
			build: func(tw *tar.Writer) {
				_ = tw.WriteHeader(&tar.Header{
					Name:     "pipe",
					Typeflag: tar.TypeFifo,
				})
			},
			wantErr: ErrUnsafeTarEntry,
		},
		{
			name: "dotdot traversal rejected",
			build: func(tw *tar.Writer) {
				payload := []byte("pwned")
				_ = tw.WriteHeader(&tar.Header{
					Name:     "sub/../../escape.txt",
					Typeflag: tar.TypeReg,
					Size:     int64(len(payload)),
					Mode:     0o644,
				})
				_, _ = tw.Write(payload)
			},
			wantErr: ErrUnsafeTarEntry,
		},
		{
			name: "absolute path rejected",
			build: func(tw *tar.Writer) {
				payload := []byte("pwned")
				_ = tw.WriteHeader(&tar.Header{
					Name:     "/etc/passwd",
					Typeflag: tar.TypeReg,
					Size:     int64(len(payload)),
					Mode:     0o644,
				})
				_, _ = tw.Write(payload)
			},
			wantErr: ErrUnsafeTarEntry,
		},
		{
			name: "per-file byte cap exceeded",
			build: func(tw *tar.Writer) {
				payload := bytes.Repeat([]byte("A"), 2048)
				_ = tw.WriteHeader(&tar.Header{
					Name:     "big.bin",
					Typeflag: tar.TypeReg,
					Size:     int64(len(payload)),
					Mode:     0o644,
				})
				_, _ = tw.Write(payload)
			},
			limits:  TarExtractLimits{MaxFileBytes: 1024, MaxTotalBytes: 8192},
			wantErr: ErrTarSizeLimitExceeded,
		},
		{
			name: "total byte cap exceeded across multiple files",
			build: func(tw *tar.Writer) {
				for _, fname := range []string{"a.bin", "b.bin", "c.bin"} {
					payload := bytes.Repeat([]byte("B"), 600)
					_ = tw.WriteHeader(&tar.Header{
						Name:     fname,
						Typeflag: tar.TypeReg,
						Size:     int64(len(payload)),
						Mode:     0o644,
					})
					_, _ = tw.Write(payload)
				}
			},
			limits:  TarExtractLimits{MaxFileBytes: 1024, MaxTotalBytes: 1500},
			wantErr: ErrTarSizeLimitExceeded,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := buildTarZst(t, tc.build)
			dest := t.TempDir()
			err := ExtractTarZstSafe(bytes.NewReader(raw), dest, tc.limits)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got: %v", tc.wantErr, err)
			}
		})
	}
}

func TestExtractTarZstSafe_EnforcesPermissions(t *testing.T) {
	raw := buildTarZst(t, func(tw *tar.Writer) {
		_ = tw.WriteHeader(&tar.Header{
			Name:     "subdir/",
			Typeflag: tar.TypeDir,
			Mode:     0o777,
		})
		payload := []byte("hello-air")
		_ = tw.WriteHeader(&tar.Header{
			Name:     "subdir/data.txt",
			Typeflag: tar.TypeReg,
			Size:     int64(len(payload)),
			Mode:     0o777,
		})
		_, _ = tw.Write(payload)
	})

	dest := filepath.Join(t.TempDir(), "restore")
	if err := ExtractTarZstSafe(bytes.NewReader(raw), dest, TarExtractLimits{}); err != nil {
		t.Fatalf("ExtractTarZstSafe failed: %v", err)
	}

	dInfo, err := os.Stat(filepath.Join(dest, "subdir"))
	if err != nil {
		t.Fatal(err)
	}
	if perm := dInfo.Mode().Perm(); perm != 0o700 {
		t.Fatalf("expected dir perm 0700, got %o", perm)
	}
	fInfo, err := os.Stat(filepath.Join(dest, "subdir", "data.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if perm := fInfo.Mode().Perm(); perm != 0o600 {
		t.Fatalf("expected file perm 0600, got %o", perm)
	}
}

func TestGarbageCollectBackupDAG(t *testing.T) {
	baseTime := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	nodes := []fencing.BackupArtifact{
		{BackupID: "B0", ParentID: "", Type: "FULL", StartLSN: "0/1000000", CreatedAt: baseTime.Add(24 * time.Hour)},
		{BackupID: "I1", ParentID: "B0", Type: "INCR", StartLSN: "0/2000000", CreatedAt: baseTime.Add(2 * 24 * time.Hour)},
		{BackupID: "I2", ParentID: "I1", Type: "INCR", StartLSN: "0/3000000", CreatedAt: baseTime.Add(5 * 24 * time.Hour)},
		{BackupID: "B1", ParentID: "", Type: "FULL", StartLSN: "0/5000000", CreatedAt: baseTime.Add(6 * 24 * time.Hour)},
		{BackupID: "I3", ParentID: "B1", Type: "INCR", StartLSN: "0/6000000", CreatedAt: baseTime.Add(7 * 24 * time.Hour)},
	}

	// Cutoff Day 4: I2 (Day 5) is retained, so its ancestors I1 and B0 must be preserved.
	resA := GarbageCollectBackupDAG(nodes, baseTime.Add(4*24*time.Hour))
	if len(resA.SweptBackupIDs) != 0 || len(resA.RetainedAncestorIDs) != 2 || resA.MinRetainedWALStartLSN != "0/1000000" {
		t.Fatalf("unexpected Case A GC result: %+v", resA)
	}

	// Cutoff Day 6: B0, I1, I2 have no retained descendants -> all 3 swept cleanly.
	resB := GarbageCollectBackupDAG(nodes, baseTime.Add(6*24*time.Hour))
	if len(resB.SweptBackupIDs) != 3 || len(resB.RetainedBackupIDs) != 2 || resB.MinRetainedWALStartLSN != "0/5000000" {
		t.Fatalf("unexpected Case B GC result: %+v", resB)
	}
}

type testR2Obj struct {
	data []byte
	etag string
	meta map[string]string
}

func newTestS3(t *testing.T) (*httptest.Server, *s3.Client) {
	t.Helper()
	var mu sync.Mutex
	objs := make(map[string]testR2Obj)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			if r.Header.Get("If-None-Match") == "*" {
				if _, exists := objs[key]; exists {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}
			if ifMatch := r.Header.Get("If-Match"); ifMatch != "" {
				cur, exists := objs[key]
				if !exists || cur.etag != ifMatch {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}
			b, _ := io.ReadAll(r.Body)
			sum := md5.Sum(b)
			etag := `"` + hex.EncodeToString(sum[:]) + `"`
			meta := make(map[string]string)
			for hk, hv := range r.Header {
				lower := strings.ToLower(hk)
				if strings.HasPrefix(lower, "x-amz-meta-") && len(hv) > 0 {
					meta[strings.TrimPrefix(lower, "x-amz-meta-")] = hv[0]
				}
			}
			objs[key] = testR2Obj{data: b, etag: etag, meta: meta}
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			o, ok := objs[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			for mk, mv := range o.meta {
				w.Header().Set("X-Amz-Meta-"+mk, mv)
			}
			w.Header().Set("ETag", o.etag)
			w.Header().Set("Content-Length", strconv.Itoa(len(o.data)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.Query().Get("list-type") == "2" {
				bucketPrefix := key
				if bucketPrefix != "" && !strings.HasSuffix(bucketPrefix, "/") {
					bucketPrefix += "/"
				}
				qPrefix := r.URL.Query().Get("prefix")
				fullPrefix := bucketPrefix + qPrefix
				var keys []string
				for k := range objs {
					if strings.HasPrefix(k, fullPrefix) {
						keys = append(keys, strings.TrimPrefix(k, bucketPrefix))
					}
				}
				var sb strings.Builder
				sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated>`)
				for _, k := range keys {
					sb.WriteString("<Contents><Key>")
					sb.WriteString(k)
					sb.WriteString("</Key></Contents>")
				}
				sb.WriteString("</ListBucketResult>")
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(sb.String()))
				return
			}
			o, ok := objs[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			for mk, mv := range o.meta {
				w.Header().Set("X-Amz-Meta-"+mk, mv)
			}
			w.Header().Set("ETag", o.etag)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(o.data)
		case http.MethodDelete:
			delete(objs, key)
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	cfg := aws.Config{
		Region:      "auto",
		Credentials: credentials.NewStaticCredentialsProvider("ak", "sk", ""),
	}
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
	})
	return srv, s3c
}

func TestDrainWALSpoolFaultInjectionAndResume(t *testing.T) {
	srv, s3c := newTestS3(t)
	defer srv.Close()

	ctx := context.Background()
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	tenantID := "t_wal_fault"
	if _, err := kms.EnsureTenantDEK(tenantID); err != nil {
		t.Fatal(err)
	}

	spoolDir := t.TempDir()
	segName := "000000010000000000000001"
	segPath := filepath.Join(spoolDir, segName)
	if err := os.WriteFile(segPath, bytes.Repeat([]byte("WALDATA"), 128), 0o600); err != nil {
		t.Fatal(err)
	}

	// Fail closed when R2 client is nil
	if _, err := drainWALSpoolDirWithFault(ctx, kms, nil, "bkt", tenantID, 1, spoolDir, nil); !errors.Is(err, ErrR2ClientRequired) {
		t.Fatalf("expected ErrR2ClientRequired, got %v", err)
	}

	// Fault after upload, before manifest commit -> local WAL must still exist
	errInjected := errors.New("crash after upload before manifest")
	_, err = drainWALSpoolDirWithFault(ctx, kms, s3c, "bkt", tenantID, 1, spoolDir, func(stage, _ string) error {
		if stage == "after_upload_before_manifest" {
			return errInjected
		}
		return nil
	})
	if !errors.Is(err, errInjected) {
		t.Fatalf("expected injected error, got %v", err)
	}
	if _, statErr := os.Stat(segPath); statErr != nil {
		t.Fatalf("local WAL segment should still exist after pre-manifest crash: %v", statErr)
	}

	// Fault after manifest commit, before local delete -> manifest has VERIFIED_IN_R2
	errInjected2 := errors.New("crash after manifest before delete")
	_, err = drainWALSpoolDirWithFault(ctx, kms, s3c, "bkt", tenantID, 1, spoolDir, func(stage, _ string) error {
		if stage == "after_manifest_before_delete" {
			return errInjected2
		}
		return nil
	})
	if !errors.Is(err, errInjected2) {
		t.Fatalf("expected injected error 2, got %v", err)
	}

	// Resume without fault -> verifies HeadObject and deletes local WAL
	uploaded, err := drainWALSpoolDirWithFault(ctx, kms, s3c, "bkt", tenantID, 1, spoolDir, nil)
	if err != nil {
		t.Fatalf("resume drain failed: %v", err)
	}
	if len(uploaded) != 1 || uploaded[0].Status != "LOCAL_DELETED" {
		t.Fatalf("unexpected resume result: %+v", uploaded)
	}
	if _, statErr := os.Stat(segPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected local WAL segment to be deleted after resume, statErr=%v", statErr)
	}
}

func TestLSNAndValidateWALContinuity(t *testing.T) {
	// Single segment covering [0/1000000, 0/2000000]
	single := []WALRef{
		{SegmentName: "000000010000000000000001", TimelineID: "00000001"},
	}
	if err := ValidateWALContinuity(single, "0/1000000", "0/1500000"); err != nil {
		t.Fatalf("expected single segment to cover 0/1500000, got: %v", err)
	}
	if err := ValidateWALContinuity(single, "0/1000000", "0/2000000"); err != nil {
		t.Fatalf("expected single segment to cover end boundary 0/2000000, got: %v", err)
	}
	if err := ValidateWALContinuity(single, "0/1000000", "0/2000001"); !errors.Is(err, ErrMissingWALSegment) {
		t.Fatalf("expected ErrMissingWALSegment past 0/2000000, got: %v", err)
	}

	// Multi-segment with auxiliary .history file and earlier retained segment
	multi := []WALRef{
		{SegmentName: "00000001.history"},
		{SegmentName: "000000010000000000000001", TimelineID: "00000001"},
		{SegmentName: "000000010000000000000002", TimelineID: "00000001"},
		{SegmentName: "000000010000000000000003", TimelineID: "00000001"},
	}
	if err := ValidateWALContinuity(multi, "0/2000000", "0/3800000"); err != nil {
		t.Fatalf("expected multi-segment with .history and earlier segment to pass, got: %v", err)
	}

	// Gap between segment 1 and 3
	gapped := []WALRef{
		{SegmentName: "000000010000000000000001", TimelineID: "00000001"},
		{SegmentName: "000000010000000000000003", TimelineID: "00000001"},
	}
	if err := ValidateWALContinuity(gapped, "0/1000000", "0/3500000"); !errors.Is(err, ErrMissingWALSegment) {
		t.Fatalf("expected ErrMissingWALSegment on gap, got: %v", err)
	}

	// Duplicate segment
	dup := []WALRef{
		{SegmentName: "000000010000000000000001", TimelineID: "00000001"},
		{SegmentName: "000000010000000000000001", TimelineID: "00000001"},
	}
	if err := ValidateWALContinuity(dup, "0/1000000", "0/1800000"); !errors.Is(err, ErrWALDiscontinuity) {
		t.Fatalf("expected ErrWALDiscontinuity on duplicate segment, got: %v", err)
	}

	// Mixed timeline IDs
	mixedTL := []WALRef{
		{SegmentName: "000000010000000000000001", TimelineID: "00000001"},
		{SegmentName: "000000020000000000000002", TimelineID: "00000002"},
	}
	if err := ValidateWALContinuity(mixedTL, "0/1000000", "0/2800000"); !errors.Is(err, ErrWALDiscontinuity) {
		t.Fatalf("expected ErrWALDiscontinuity on mixed timelines, got: %v", err)
	}
}

func TestManifestPublishVerifyAndChecksumSeparation(t *testing.T) {
	srv, s3c := newTestS3(t)
	defer srv.Close()

	ctx := context.Background()
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	tenantID := "t_manifest_ver"
	gen := "gen_001"
	if _, err := kms.EnsureTenantDEK(tenantID); err != nil {
		t.Fatal(err)
	}
	dekBundle, err := kms.ExportWrappedDEKBundle(tenantID)
	if err != nil {
		t.Fatal(err)
	}

	// Upload a real WAL segment via drainWALSpoolDirWithFaultGen
	spoolDir := t.TempDir()
	segName := "000000010000000000000001"
	if err := os.WriteFile(filepath.Join(spoolDir, segName), bytes.Repeat([]byte("W"), 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	walArts, err := drainWALSpoolDirWithFaultGen(ctx, kms, s3c, "bkt", tenantID, gen, 1, spoolDir, nil)
	if err != nil || len(walArts) != 1 {
		t.Fatalf("drainWALSpoolDirWithFaultGen failed: arts=%+v err=%v", walArts, err)
	}
	if walArts[0].PlainSHA256 == walArts[0].CipherSHA256 {
		t.Fatalf("PlainSHA256 and CipherSHA256 must differ")
	}

	// Upload base object
	bkpID := "bkp_100"
	baseKey := BackupBaseObjectKey(tenantID, gen, bkpID)
	baseCipher := []byte("encrypted-base-archive-payload")
	baseSum := sha256.Sum256(baseCipher)
	baseCipherSHA := hex.EncodeToString(baseSum[:])
	st := NewStore(s3c, "bkt")
	if _, err := st.PutImmutable(ctx, baseKey, baseCipher, "application/octet-stream"); err != nil {
		t.Fatalf("PutImmutable base failed: %v", err)
	}

	art := &fencing.BackupArtifact{
		BackupID:     bkpID,
		Type:         BackupTypeFull,
		R2Key:        baseKey,
		Epoch:        1,
		StartLSN:     "0/1000000",
		EndLSN:       "0/1500000",
		CipherBytes:  len(baseCipher),
		CipherSHA256: baseCipherSHA,
		CreatedAt:    time.Now().UTC(),
		Verified:     true,
		DEKBundle:    dekBundle,
	}

	m, err := PublishCompletedBackupManifest(ctx, s3c, "bkt", tenantID, gen, time.Now().UTC().Add(-time.Second), art, walArts, nil)
	if err != nil {
		t.Fatalf("PublishCompletedBackupManifest failed: %v", err)
	}

	// Idempotent re-publish of identical manifest must succeed
	if err := st.PublishManifest(ctx, m); err != nil {
		t.Fatalf("idempotent PublishManifest failed: %v", err)
	}

	// VerifyBackup with StreamHash=false (uses Head metadata SHA256; must compare CipherSHA256 against CipherSHA256)
	repFast, err := VerifyBackup(ctx, st, m, VerifyOptions{
		ExpectedTenantID:   tenantID,
		ExpectedGeneration: gen,
		StreamHash:         false,
	})
	if err != nil || !repFast.Valid || !repFast.ObjectsOK || !repFast.WALContinuityOK {
		t.Fatalf("VerifyBackup(StreamHash=false) failed: rep=%+v err=%v", repFast, err)
	}

	// VerifyBackup with StreamHash=true
	repStream, err := VerifyBackup(ctx, st, m, VerifyOptions{
		ExpectedTenantID:   tenantID,
		ExpectedGeneration: gen,
		StreamHash:         true,
	})
	if err != nil || !repStream.Valid {
		t.Fatalf("VerifyBackup(StreamHash=true) failed: rep=%+v err=%v", repStream, err)
	}

	// Generation mismatch must fail verification
	repWrongGen, _ := VerifyBackup(ctx, st, m, VerifyOptions{
		ExpectedTenantID:   tenantID,
		ExpectedGeneration: "gen_other",
	})
	if repWrongGen.Valid {
		t.Fatalf("expected VerifyBackup to fail on generation mismatch")
	}

	// Corrupt the base object in S3 -> VerifyBackup must fail
	_ = st.PutBytesOverwrite(ctx, baseKey, []byte("corrupted-bytes-of-same-len!!12"), "application/octet-stream")
	repCorrupt, _ := VerifyBackup(ctx, st, m, VerifyOptions{
		ExpectedTenantID:   tenantID,
		ExpectedGeneration: gen,
		StreamHash:         true,
	})
	if repCorrupt.Valid || repCorrupt.ObjectsOK {
		t.Fatalf("expected VerifyBackup to detect corrupted base object: %+v", repCorrupt)
	}
}

func TestRecoveryTargetValidation(t *testing.T) {
	if _, err := ParseRecoveryTarget(true, "0/1000000", ""); !errors.Is(err, ErrInvalidRecoveryTarget) {
		t.Fatalf("expected mutual exclusion error, got: %v", err)
	}
	m := &BackupManifest{
		ConsistencyLSN:  "0/1500000",
		WALStartLSN:     "0/1000000",
		WALEndLSN:       "0/3000000",
		ConsistencyTime: time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC),
	}
	tBefore, err := NewLSNRecoveryTarget("0/1200000")
	if err != nil {
		t.Fatal(err)
	}
	if err := tBefore.ValidateAgainstManifest(m); !errors.Is(err, ErrTargetOutOfRange) {
		t.Fatalf("expected ErrTargetOutOfRange before ConsistencyLSN, got: %v", err)
	}
	tAfter, err := NewLSNRecoveryTarget("0/4000000")
	if err != nil {
		t.Fatal(err)
	}
	if err := tAfter.ValidateAgainstManifest(m); !errors.Is(err, ErrTargetOutOfRange) {
		t.Fatalf("expected ErrTargetOutOfRange after WALEndLSN, got: %v", err)
	}
	tValid, err := NewLSNRecoveryTarget("0/2000000")
	if err != nil {
		t.Fatal(err)
	}
	if err := tValid.ValidateAgainstManifest(m); err != nil {
		t.Fatalf("expected valid LSN target to pass, got: %v", err)
	}
}

func TestStagePITRRestoreRejectsCustomVerification(t *testing.T) {
	_, err := StagePITRRestore(context.Background(), nil, nil, "", nil, nil, nil, NewLatestRecoveryTarget(),
		StagePITROptions{VerifyQuery: "SELECT current_user;"})
	if !errors.Is(err, ErrInvalidRecoveryTarget) {
		t.Fatalf("expected custom verification SQL to be rejected, got %v", err)
	}
}
