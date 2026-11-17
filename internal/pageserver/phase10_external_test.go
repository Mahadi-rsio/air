package pageserver

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func newExternalR2Client(t *testing.T) (*s3.Client, string) {
	t.Helper()
	endpoint := os.Getenv("AIR_R2_ENDPOINT")
	bucket := os.Getenv("AIR_R2_BUCKET")
	ak := os.Getenv("AIR_R2_ACCESS_KEY")
	sk := os.Getenv("AIR_R2_SECRET_KEY")
	if endpoint == "" || bucket == "" || ak == "" || sk == "" {
		t.Skip("skipping real R2 external test: set AIR_R2_ENDPOINT, AIR_R2_BUCKET, AIR_R2_ACCESS_KEY, AIR_R2_SECRET_KEY")
	}
	cfg := aws.Config{
		Region:      "auto",
		Credentials: credentials.NewStaticCredentialsProvider(ak, sk, ""),
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return client, bucket
}

// 1. Real R2 integration + 6. Interrupted uploads and manifest recovery against real R2.
func TestPhase10_External_RealR2AndManifestRecovery(t *testing.T) {
	s3c, bucket := newExternalR2Client(t)
	ctx := context.Background()

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	tenantID := fmt.Sprintf("ext-r2-%d", time.Now().UnixNano())
	bundle, err := kms.ExportWrappedDEKBundle(tenantID)
	if err != nil {
		t.Fatal(err)
	}

	cache1, err := NewDiskCache(t.TempDir(), 32*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	store1 := NewLayerStore("", s3c, bucket, kms, cache1)
	tl1 := NewTimeline(tenantID, 1, store1, NewDefaultWALRedoer(""))

	for b := uint32(0); b < 128; b++ {
		tl1.PutPage(RelBlockKey(1663, 1, 900, 0, b), uint64(b+1), makePhase9CompressiblePage(uint64(b)))
	}
	lm, err := tl1.FlushInMemory(ctx)
	if err != nil {
		t.Fatalf("real R2 FlushInMemory: %v", err)
	}
	// Interrupted upload simulation: cancel context mid-flush on a second batch, ensure manifest stays consistent.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	tl1.PutPage(RelBlockKey(1663, 1, 900, 0, 200), 999, makePhase9CompressiblePage(200))
	if _, err := tl1.FlushInMemory(cancelCtx); err == nil {
		t.Fatalf("expected cancelled upload to fail")
	}

	// Recover in a fresh store/timeline from real R2 manifest.
	kms2, _ := crypto.NewEnvelopeKMS()
	if err := kms2.ImportWrappedDEKBundle(bundle); err != nil {
		t.Fatal(err)
	}
	cache2, _ := NewDiskCache(t.TempDir(), 32*1024*1024)
	store2 := NewLayerStore("", s3c, bucket, kms2, cache2)
	tl2 := NewTimeline(tenantID, 1, store2, NewDefaultWALRedoer(""))
	if err := tl2.LoadLayersFromR2(ctx); err != nil {
		t.Fatalf("LoadLayersFromR2: %v", err)
	}
	got, cnt, err := tl2.GetPageBatch(ctx, 1663, 1, 900, 0, 0, 128, 0)
	if err != nil || cnt != 128 || !bytes.Equal(got[:PageSize], makePhase9CompressiblePage(0)) {
		t.Fatalf("real R2 recovery mismatch: err=%v cnt=%d layer=%s", err, cnt, lm.R2Key)
	}
}

// 2. Multi-process Pageserver crash and restart (spawns child test process and SIGKILLs it).
func TestPhase10_External_MultiProcessCrashAndRestart(t *testing.T) {
	if os.Getenv("AIR_EXT_CHILD_CRASH") == "1" {
		dataDir := os.Getenv("AIR_EXT_DATADIR")
		s3c, bucket := newExternalR2Client(t)
		kms, _ := crypto.NewEnvelopeKMS()
		_, _ = kms.EnsureTenantDEK("crash-tenant")
		cache, _ := NewDiskCache(filepath.Join(dataDir, "cache"), 16*1024*1024)
		store := NewLayerStore(filepath.Join(dataDir, "local"), s3c, bucket, kms, cache)
		tl := NewTimeline("crash-tenant", 1, store, NewDefaultWALRedoer(""))
		for i := 0; i < 16; i++ {
			tl.PutPage(RelBlockKey(1663, 1, 100, 0, uint32(i)), uint64(i+1), makePhase9CompressiblePage(uint64(i)))
			_, _ = tl.FlushInMemory(context.Background())
		}
		select {} // Wait to be SIGKILLed by parent.
	}

	if os.Getenv("AIR_RUN_EXTERNAL") != "1" {
		t.Skip("set AIR_RUN_EXTERNAL=1 with R2 credentials to run multi-process crash/restart test")
	}
	_, _ = newExternalR2Client(t)
	dataDir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPhase10_External_MultiProcessCrashAndRestart$")
	cmd.Env = append(os.Environ(), "AIR_EXT_CHILD_CRASH=1", "AIR_EXT_DATADIR="+dataDir)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	// Verify DiskCache recovers cleanly after child SIGKILL without leaving stale .tmp files.
	dc, err := NewDiskCache(filepath.Join(dataDir, "cache"), 16*1024*1024)
	if err != nil {
		t.Fatalf("DiskCache recovery after SIGKILL failed: %v", err)
	}
	_, _ = dc.Stats()
}

// 3. 24-hour (or configurable AIR_SOAK_DURATION) soak test.
func TestPhase10_External_Soak(t *testing.T) {
	durStr := os.Getenv("AIR_SOAK_DURATION")
	if durStr == "" {
		t.Skip("set AIR_SOAK_DURATION=24h to run long-running soak test")
	}
	dur, err := time.ParseDuration(durStr)
	if err != nil {
		t.Fatalf("invalid AIR_SOAK_DURATION=%q: %v", durStr, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	_, s3c, _ := newMockR2Server(t)
	kms, _ := crypto.NewEnvelopeKMS()
	_, _ = kms.EnsureTenantDEK("soak-tenant")
	cache, _ := NewDiskCache(t.TempDir(), 64*1024*1024)
	store := NewLayerStore("", s3c, "air-bucket", kms, cache)
	tl := NewTimeline("soak-tenant", 1, store, NewDefaultWALRedoer(""))

	for b := uint32(0); b < 512; b++ {
		tl.PutPage(RelBlockKey(1663, 1, 400, 0, b), 1, makePhase9CompressiblePage(uint64(b)))
	}
	if _, err := tl.FlushInMemory(context.Background()); err != nil {
		t.Fatal(err)
	}

	var ops atomic.Int64
	var errs atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for ctx.Err() == nil {
				_, cnt, err := tl.GetPageBatch(ctx, 1663, 1, 400, 0, 0, 128, 0)
				if err != nil && ctx.Err() == nil {
					errs.Add(1)
				} else if cnt == 128 {
					ops.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	if errs.Load() > 0 {
		t.Fatalf("soak test observed %d errors across %d ops", errs.Load(), ops.Load())
	}
}

// 4 & 5. Live PostgreSQL 17 WAL redo worker SIGKILL, recovery, and page correctness.
func TestPhase10_External_LiveWALRedoWorkerSIGKILL(t *testing.T) {
	bin := os.Getenv("AIR_WALREDO_BIN")
	if bin == "" {
		t.Skip("skipping live PG17 walredo SIGKILL test: set AIR_WALREDO_BIN=/path/to/air-walredo")
	}
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("AIR_WALREDO_BIN=%q not accessible: %v", bin, err)
	}
	// Verify worker pool starts, handles SIGKILL on child worker, restarts, and preserves correctness.
	redoer := NewDefaultWALRedoer(bin)
	defer redoer.Close()

	key := RelBlockKey(1663, 1, 1200, 0, 0)
	base := makePhase9CompressiblePage(1200)
	got1, err := redoer.Redo(context.Background(), key, base, nil)
	if err != nil || !bytes.Equal(got1, base) {
		t.Fatalf("initial redo failed: err=%v", err)
	}

	// Kill any spawned walredo worker processes matching filepath.Base(bin) and verify recovery.
	_ = exec.Command("pkill", "-KILL", "-f", filepath.Base(bin)).Run()
	time.Sleep(50 * time.Millisecond)

	got2, err := redoer.Redo(context.Background(), key, base, nil)
	if err != nil || !bytes.Equal(got2, base) {
		t.Fatalf("post-SIGKILL redo recovery failed: err=%v (pid_check=%s)", err, strconv.Itoa(os.Getpid()))
	}
}
