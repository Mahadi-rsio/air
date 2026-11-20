package readadapter

import (
	"bytes"
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
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

func TestHardeningConcurrentReadsWritesInvalidations(t *testing.T) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(t.TempDir(), "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const (
		tenantA   = "hardening-a"
		tenantB   = "hardening-b"
		gen       = "gen-1"
		spcOID    = uint32(1663)
		dbOID     = uint32(5)
		relNumber = uint32(50001)
		forkNum   = uint8(0)
		numBlocks = uint32(8)
		numEpochs = 40
	)
	ps.EnsureTenant(tenantA, 1)
	ps.SetTenantGeneration(tenantA, gen)
	ps.EnsureTenant(tenantB, 1)
	ps.SetTenantGeneration(tenantB, gen)

	ctx := context.Background()
	for b := uint32(0); b < numBlocks; b++ {
		pgA := bytes.Repeat([]byte{byte(0x10 + b)}, PGPageSize)
		pgB := bytes.Repeat([]byte{byte(0x80 + b)}, PGPageSize)
		if err := ps.PutPageBatch(ctx, tenantA, spcOID, dbOID, relNumber, forkNum, b, 1, 0x1000, pgA); err != nil {
			t.Fatalf("init A: %v", err)
		}
		if err := ps.PutPageBatch(ctx, tenantB, spcOID, dbOID, relNumber, forkNum, b, 1, 0x1000, pgB); err != nil {
			t.Fatalf("init B: %v", err)
		}
	}

	cache, err := NewTieredCacheBackend(ps, CacheConfig{
		L1MaxBytes: 4 * PGPageSize,
		L2Dir:      filepath.Join(t.TempDir(), "l2"),
		L2MaxBytes: 12 * PGPageSize,
	})
	if err != nil {
		t.Fatalf("NewTieredCacheBackend: %v", err)
	}

	coords := NewSnapshotCoordinator()
	pub, err := NewCommitPublisher(coords, cache)
	if err != nil {
		t.Fatalf("NewCommitPublisher: %v", err)
	}
	_ = pub.PublishCommit(CommitEvent{TenantID: tenantA, Generation: gen, CommitLSN: 0x1000, Xmin: 1, Xmax: 2})
	_ = pub.PublishCommit(CommitEvent{TenantID: tenantB, Generation: gen, CommitLSN: 0x1000, Xmin: 1, Xmax: 2})

	adapter := NewAdapter(cache, coords)

	var wg sync.WaitGroup
	var errCount atomic.Int64

	// Concurrent writers advancing LSNs and invalidating changed blocks.
	for _, tid := range []string{tenantA, tenantB} {
		tid := tid
		wg.Add(1)
		go func() {
			defer wg.Done()
			base := byte(0x10)
			if tid == tenantB {
				base = 0x80
			}
			for ep := 1; ep <= numEpochs; ep++ {
				lsn := uint64(0x1000 + ep*0x100)
				blk := uint32(ep) % numBlocks
				pg := bytes.Repeat([]byte{base + byte(blk)}, PGPageSize)
				if err := ps.PutPageBatch(ctx, tid, spcOID, dbOID, relNumber, forkNum, blk, 1, lsn, pg); err != nil {
					errCount.Add(1)
					return
				}
				if err := pub.PublishCommit(CommitEvent{
					TenantID:   tid,
					Generation: gen,
					CommitLSN:  lsn,
					Xmin:       1,
					Xmax:       uint32(2 + ep),
					ChangedBlocks: []BlockCoord{
						{SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum, BlockNum: blk},
					},
				}); err != nil {
					errCount.Add(1)
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// Concurrent readers verifying tenant isolation and exact page contents.
	for r := 0; r < 12; r++ {
		tid := tenantA
		base := byte(0x10)
		if r%2 == 1 {
			tid = tenantB
			base = 0x80
		}
		wg.Add(1)
		go func(id int, tenant string, expectedBase byte) {
			defer wg.Done()
			rel, err := adapter.OpenRelation(ctx, RelationSpec{
				TenantID:   tenant,
				Generation: gen,
				SpcOID:     spcOID,
				DbOID:      dbOID,
				RelNumber:  relNumber,
				ForkNum:    forkNum,
			})
			if err != nil {
				errCount.Add(1)
				return
			}
			defer adapter.CloseRelation(rel)

			for iter := 0; iter < 30; iter++ {
				snap, err := adapter.GetSnapshot(ctx, tenant, gen)
				if err != nil {
					errCount.Add(1)
					return
				}
				blk := uint32((id + iter) % int(numBlocks))
				pg, err := adapter.ReadPage(ctx, rel, snap, blk)
				_ = adapter.ReleaseSnapshot(snap)
				if err != nil {
					errCount.Add(1)
					return
				}
				wantByte := expectedBase + byte(blk)
				if len(pg.Data) != PGPageSize || pg.Data[0] != wantByte || pg.Data[PGPageSize-1] != wantByte {
					errCount.Add(1)
					return
				}
			}
		}(r, tid, base)
	}

	wg.Wait()
	if errCount.Load() != 0 {
		t.Fatalf("observed %d errors during concurrent storm", errCount.Load())
	}
	if adapter.ActiveSnapshotCount() != 0 {
		t.Fatalf("expected 0 leaked snapshots, got %d", adapter.ActiveSnapshotCount())
	}
}

func TestHardeningGenerationFenceAndL2CrashRecovery(t *testing.T) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(t.TempDir(), "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const (
		tenantID  = "hardening-fence"
		gen1      = "gen-1"
		gen2      = "gen-2"
		spcOID    = uint32(1663)
		dbOID     = uint32(5)
		relNumber = uint32(50002)
		forkNum   = uint8(0)
		lsn1      = uint64(0x10000)
		lsn2      = uint64(0x20000)
	)
	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, gen1)

	pg1 := bytes.Repeat([]byte{0x41}, PGPageSize)
	pg2 := bytes.Repeat([]byte{0x42}, PGPageSize)
	ctx := context.Background()
	if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, 0, 1, lsn1, pg1); err != nil {
		t.Fatalf("PutPageBatch 1: %v", err)
	}

	l2Dir := filepath.Join(t.TempDir(), "l2-crash")
	cache, err := NewTieredCacheBackend(ps, CacheConfig{
		L1MaxBytes: PGPageSize,
		L2Dir:      l2Dir,
		L2MaxBytes: 8 * PGPageSize,
	})
	if err != nil {
		t.Fatalf("NewTieredCacheBackend: %v", err)
	}
	coords := NewSnapshotCoordinator()
	pub, err := NewCommitPublisher(coords, cache)
	if err != nil {
		t.Fatalf("NewCommitPublisher: %v", err)
	}
	_ = pub.PublishCommit(CommitEvent{TenantID: tenantID, Generation: gen1, CommitLSN: lsn1, Xmin: 1, Xmax: 2})

	adapter := NewAdapter(cache, coords)
	snap1, err := adapter.GetSnapshot(ctx, tenantID, gen1)
	if err != nil {
		t.Fatalf("GetSnapshot gen1: %v", err)
	}
	rel1, err := adapter.OpenRelation(ctx, RelationSpec{
		TenantID: tenantID, Generation: gen1, SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum,
	})
	if err != nil {
		t.Fatalf("OpenRelation gen1: %v", err)
	}
	if _, err := adapter.ReadPage(ctx, rel1, snap1, 0); err != nil {
		t.Fatalf("ReadPage gen1: %v", err)
	}

	// Simulate primary failover / fencing gen1 -> gen2.
	ps.SetTenantGeneration(tenantID, gen2)
	ps.EnsureTenant(tenantID, 2)
	if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, 0, 1, lsn2, pg2); err != nil {
		t.Fatalf("PutPageBatch 2: %v", err)
	}
	pub.OnGenerationFence(tenantID, gen1, gen2, SnapshotInfo{LSN: lsn2, Xmin: 1, Xmax: 3})

	// Stale gen1 handle must fail with ErrStaleGeneration.
	if _, err := adapter.ReadPage(ctx, rel1, snap1, 0); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("expected ErrStaleGeneration for fenced gen1 read, got %v", err)
	}
	_ = adapter.ReleaseSnapshot(snap1)
	_ = adapter.CloseRelation(rel1)

	// New gen2 handle succeeds and reads pg2.
	snap2, err := adapter.GetSnapshot(ctx, tenantID, gen2)
	if err != nil {
		t.Fatalf("GetSnapshot gen2: %v", err)
	}
	defer adapter.ReleaseSnapshot(snap2)
	rel2, err := adapter.OpenRelation(ctx, RelationSpec{
		TenantID: tenantID, Generation: gen2, SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum,
	})
	if err != nil {
		t.Fatalf("OpenRelation gen2: %v", err)
	}
	defer adapter.CloseRelation(rel2)

	got, err := adapter.ReadPage(ctx, rel2, snap2, 0)
	if err != nil || !bytes.Equal(got.Data, pg2) {
		t.Fatalf("gen2 read failed: err=%v", err)
	}

	// Simulate L2 NVMe crash truncation on gen2's L2 file + evict L1 so L2 is hit.
	ck := PageCacheKey{
		TenantID: tenantID, Generation: gen2,
		SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum, BlockNum: 0, LSN: lsn2,
	}
	l2Path := cache.l2PathFor(ck)
	if err := os.Truncate(l2Path, 128); err != nil {
		t.Fatalf("truncate L2: %v", err)
	}
	// Evict from L1 by putting another dummy key into L1 directly.
	cache.putL1(PageCacheKey{TenantID: "evict", BlockNum: 99}, make([]byte, PGPageSize))

	recovered, err := adapter.ReadPage(ctx, rel2, snap2, 0)
	if err != nil || !bytes.Equal(recovered.Data, pg2) {
		t.Fatalf("expected automatic recovery after L2 truncation, err=%v", err)
	}
	if cache.Stats().L2Corrupt < 1 {
		t.Fatalf("expected L2Corrupt >= 1 after truncation, got %+v", cache.Stats())
	}
}

func TestHardeningLivePG17AirRoConcurrentMix(t *testing.T) {
	pgBinDir := "/usr/lib/postgresql/17/bin"
	if _, err := os.Stat(filepath.Join(pgBinDir, "postgres")); err != nil {
		t.Skipf("PG 17 binary not found at %s: %v", pgBinDir, err)
	}

	extDir, err := filepath.Abs(filepath.Join("..", "..", "extensions", "air_ro"))
	if err != nil {
		t.Fatalf("abs extDir: %v", err)
	}
	soPath := filepath.Join(extDir, "air_ro.so")
	if _, err := os.Stat(soPath); err != nil {
		if out, err := exec.Command("make", "-C", extDir, "all").CombinedOutput(); err != nil {
			t.Fatalf("make air_ro: %v\n%s", err, string(out))
		}
	}

	rootDir := t.TempDir()
	dataDir := filepath.Join(rootDir, "pgdata")
	sockDir := filepath.Join(rootDir, "sock")
	if err := os.MkdirAll(sockDir, 0o700); err != nil {
		t.Fatalf("mkdir sock: %v", err)
	}

	runPG := func(name string, args ...string) {
		t.Helper()
		if out, err := exec.Command(filepath.Join(pgBinDir, name), args...).CombinedOutput(); err != nil {
			t.Fatalf("%s %v: %v\n%s", name, args, err, string(out))
		}
	}

	runPG("initdb", "-D", dataDir, "-U", "postgres", "--auth=trust", "--no-sync")
	port := pickFreePort(t)
	confPath := filepath.Join(dataDir, "postgresql.conf")
	appendConf := func(s string) {
		f, err := os.OpenFile(confPath, os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatalf("open conf: %v", err)
		}
		defer f.Close()
		_, _ = f.WriteString(s)
	}
	appendConf(fmt.Sprintf(`
port = %d
listen_addresses = '127.0.0.1'
unix_socket_directories = '%s'
fsync = off
shared_preload_libraries = '%s'
air_ro.enabled = off
`, port, sockDir, soPath))

	runPG("pg_ctl", "-D", dataDir, "-w", "-l", filepath.Join(rootDir, "pg.log"), "start")
	defer func() {
		_ = exec.Command(filepath.Join(pgBinDir, "pg_ctl"), "-D", dataDir, "-m", "immediate", "stop").Run()
	}()

	psql := func(sql string) (string, error) {
		cmd := exec.Command(filepath.Join(pgBinDir, "psql"),
			"-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", "-d", "postgres",
			"-v", "ON_ERROR_STOP=1", "-At", "-c", sql)
		out, err := cmd.CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}

	if out, err := psql(fmt.Sprintf(`
CREATE TABLE accounts (id int primary key, bal int);
INSERT INTO accounts SELECT g, g * 10 FROM generate_series(1, 50) g;
CREATE FUNCTION air_ro_status(
    OUT enabled bool,
    OUT queries_allowed int8,
    OUT writes_blocked int8,
    OUT allow_temp_tables bool
) RETURNS record AS '%s', 'air_ro_status' LANGUAGE C STRICT VOLATILE;
`, soPath)); err != nil {
		t.Fatalf("seed failed: %v\n%s", err, out)
	}

	runPG("pg_ctl", "-D", dataDir, "-w", "stop")
	appendConf("\nair_ro.enabled = on\n")
	runPG("pg_ctl", "-D", dataDir, "-w", "-l", filepath.Join(rootDir, "pg.log"), "start")

	var wg sync.WaitGroup
	var readFails atomic.Int64
	var writeLeaks atomic.Int64

	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				if out, err := psql("SELECT sum(bal) FROM accounts;"); err != nil || out != "12750" {
					readFails.Add(1)
				}
				if _, err := psql(fmt.Sprintf("INSERT INTO accounts VALUES (%d, 999);", 1000+id*10+i)); err == nil {
					writeLeaks.Add(1)
				}
				if _, err := psql("UPDATE accounts SET bal = 0 WHERE id = 1;"); err == nil {
					writeLeaks.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()

	if readFails.Load() != 0 {
		t.Fatalf("observed %d read failures under concurrent mix", readFails.Load())
	}
	if writeLeaks.Load() != 0 {
		t.Fatalf("observed %d write leaks under air_ro", writeLeaks.Load())
	}

	sumOut, err := psql("SELECT sum(bal) FROM accounts;")
	if err != nil || sumOut != "12750" {
		t.Fatalf("expected intact sum 12750, got %q (err=%v)", sumOut, err)
	}
}
