package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Mahadi-rsio/air/internal/control"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/pageserver"
)

type ProcStatus struct {
	VmPeakKB int64 `json:"vm_peak_kb"`
	VmSizeKB int64 `json:"vm_size_kb"`
	VmHWMKB  int64 `json:"vm_hwm_kb"`
	VmRSSKB  int64 `json:"vm_rss_kb"`
	RssAnonKB int64 `json:"rss_anon_kb"`
	RssFileKB int64 `json:"rss_file_kb"`
	Threads  int64 `json:"threads"`
}

func readProcStatus() ProcStatus {
	var ps ProcStatus
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return ps
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, _ := strconv.ParseInt(fields[1], 10, 64)
		switch fields[0] {
		case "VmPeak:":
			ps.VmPeakKB = v
		case "VmSize:":
			ps.VmSizeKB = v
		case "VmHWM:":
			ps.VmHWMKB = v
		case "VmRSS:":
			ps.VmRSSKB = v
		case "RssAnon:":
			ps.RssAnonKB = v
		case "RssFile:":
			ps.RssFileKB = v
		case "Threads:":
			ps.Threads = v
		}
	}
	return ps
}

type CgroupMem struct {
	CurrentBytes int64 `json:"current_bytes"`
	PeakBytes    int64 `json:"peak_bytes"`
	AnonBytes    int64 `json:"anon_bytes"`
	FileBytes    int64 `json:"file_bytes"`
	HighEvents   int64 `json:"high_events"`
	OOMEvents    int64 `json:"oom_events"`
	OOMKills     int64 `json:"oom_kills"`
}

func readCgroupMem() CgroupMem {
	var cm CgroupMem
	readInt := func(p string) int64 {
		b, err := os.ReadFile(p)
		if err != nil {
			return -1
		}
		v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		if err != nil {
			return -1
		}
		return v
	}
	cm.CurrentBytes = readInt("/sys/fs/cgroup/memory.current")
	cm.PeakBytes = readInt("/sys/fs/cgroup/memory.peak")
	if raw, err := os.ReadFile("/sys/fs/cgroup/memory.stat"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			f := strings.Fields(line)
			if len(f) == 2 {
				v, _ := strconv.ParseInt(f[1], 10, 64)
				switch f[0] {
				case "anon":
					cm.AnonBytes = v
				case "file":
					cm.FileBytes = v
				}
			}
		}
	}
	if raw, err := os.ReadFile("/sys/fs/cgroup/memory.events"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			f := strings.Fields(line)
			if len(f) == 2 {
				v, _ := strconv.ParseInt(f[1], 10, 64)
				switch f[0] {
				case "high":
					cm.HighEvents = v
				case "oom":
					cm.OOMEvents = v
				case "oom_kill":
					cm.OOMKills = v
				}
			}
		}
	}
	return cm
}

type StageSnapshot struct {
	Stage             string         `json:"stage"`
	ActiveTenants     int            `json:"active_tenants"`
	Goroutines        int            `json:"goroutines"`
	HeapAllocMB       float64        `json:"heap_alloc_mb"`
	HeapInuseMB       float64        `json:"heap_inuse_mb"`
	HeapIdleMB        float64        `json:"heap_idle_mb"`
	HeapReleasedMB    float64        `json:"heap_released_mb"`
	HeapSysMB         float64        `json:"heap_sys_mb"`
	TotalAllocMB      float64        `json:"total_alloc_mb"`
	SysMB             float64        `json:"sys_mb"`
	GCCPUMemMB        float64        `json:"gc_sys_mb"`
	StackInuseMB      float64        `json:"stack_inuse_mb"`
	NumGC             uint32         `json:"num_gc"`
	PauseTotalMs      float64        `json:"pause_total_ms"`
	Proc              ProcStatus     `json:"proc"`
	Cgroup            CgroupMem      `json:"cgroup"`
	DecodedCacheBytes int64          `json:"decoded_cache_bytes"`
}

func captureSnapshot(stage string, ps *pageserver.Pageserver, outDir string, forceGC bool) StageSnapshot {
	if forceGC {
		runtime.GC()
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	proc := readProcStatus()
	cg := readCgroupMem()
	var dcBytes int64
	if ps != nil && ps.Store() != nil && ps.Store().DecodedChunkCache() != nil {
		dcBytes = ps.Store().DecodedChunkCache().Stats().Bytes
	}
	active := 0
	if ps != nil {
		active = ps.ActiveTenantCount()
	}
	snap := StageSnapshot{
		Stage:             stage,
		ActiveTenants:     active,
		Goroutines:        runtime.NumGoroutine(),
		HeapAllocMB:       float64(ms.HeapAlloc) / (1024 * 1024),
		HeapInuseMB:       float64(ms.HeapInuse) / (1024 * 1024),
		HeapIdleMB:        float64(ms.HeapIdle) / (1024 * 1024),
		HeapReleasedMB:    float64(ms.HeapReleased) / (1024 * 1024),
		HeapSysMB:         float64(ms.HeapSys) / (1024 * 1024),
		TotalAllocMB:      float64(ms.TotalAlloc) / (1024 * 1024),
		SysMB:             float64(ms.Sys) / (1024 * 1024),
		GCCPUMemMB:        float64(ms.GCSys) / (1024 * 1024),
		StackInuseMB:      float64(ms.StackInuse) / (1024 * 1024),
		NumGC:             ms.NumGC,
		PauseTotalMs:      float64(ms.PauseTotalNs) / 1e6,
		Proc:              proc,
		Cgroup:            cg,
		DecodedCacheBytes: dcBytes,
	}
	if outDir != "" {
		_ = os.MkdirAll(outDir, 0o755)
		if hf, err := os.Create(filepath.Join(outDir, fmt.Sprintf("heap_%s.pb.gz", stage))); err == nil {
			_ = pprof.WriteHeapProfile(hf)
			_ = hf.Close()
		}
		if af, err := os.Create(filepath.Join(outDir, fmt.Sprintf("allocs_%s.pb.gz", stage))); err == nil {
			if p := pprof.Lookup("allocs"); p != nil {
				_ = p.WriteTo(af, 0)
			}
			_ = af.Close()
		}
	}
	log.Printf("[FORENSICS][%s] tenants=%d goroutines=%d | HeapAlloc=%.2fMB HeapInuse=%.2fMB HeapIdle=%.2fMB HeapRel=%.2fMB GCSys=%.2fMB TotalAlloc=%.1fMB NumGC=%d | VmRSS=%.2fMB(Anon=%.2fMB File=%.2fMB HWM=%.2fMB) | CgroupCur=%.2fMB(Anon=%.2fMB File=%.2fMB Peak=%.2fMB) | DecodedCache=%.2fMB",
		stage, snap.ActiveTenants, snap.Goroutines,
		snap.HeapAllocMB, snap.HeapInuseMB, snap.HeapIdleMB, snap.HeapReleasedMB, snap.GCCPUMemMB, snap.TotalAllocMB, snap.NumGC,
		float64(proc.VmRSSKB)/1024.0, float64(proc.RssAnonKB)/1024.0, float64(proc.RssFileKB)/1024.0, float64(proc.VmHWMKB)/1024.0,
		float64(cg.CurrentBytes)/(1024*1024), float64(cg.AnonBytes)/(1024*1024), float64(cg.FileBytes)/(1024*1024), float64(cg.PeakBytes)/(1024*1024),
		float64(dcBytes)/(1024*1024))
	return snap
}

// buildSyntheticPGData creates a realistic freshly-initialized PG17 PGDATA directory
// (~910 blocks / ~7.3 MB across 300+ catalog relation files + SLRU segments).
func buildSyntheticPGData(dir string) error {
	globalDir := filepath.Join(dir, "global")
	baseDir := filepath.Join(dir, "base", "16384")
	xactDir := filepath.Join(dir, "pg_xact")
	subDir := filepath.Join(dir, "pg_subtrans")
	mxOffDir := filepath.Join(dir, "pg_multixact", "offsets")
	mxMemDir := filepath.Join(dir, "pg_multixact", "members")
	for _, d := range []string{globalDir, baseDir, xactDir, subDir, mxOffDir, mxMemDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	_ = os.WriteFile(filepath.Join(dir, "PG_VERSION"), []byte("17\n"), 0o600)
	_ = os.WriteFile(filepath.Join(globalDir, "pg_control"), make([]byte, 8192), 0o600)

	// Realistic PG17 initdb catalog layout: ~40 global files (~80 blocks), ~260 base/16384 files (~830 blocks)
	rng := rand.New(rand.NewSource(42))
	writeRel := func(folder string, relNum int, blocks int) error {
		buf := make([]byte, blocks*pageserver.PageSize)
		for b := 0; b < blocks; b++ {
			off := b * pageserver.PageSize
			// Semi-compressible catalog page content
			for i := 0; i < 2048; i++ {
				buf[off+i] = byte((relNum + b + i) & 0xFF)
			}
			for i := 2048; i < 2560; i++ {
				buf[off+i] = byte(rng.Intn(256))
			}
		}
		return os.WriteFile(filepath.Join(folder, strconv.Itoa(relNum)), buf, 0o600)
	}
	for i := 0; i < 40; i++ {
		if err := writeRel(globalDir, 1200+i, 2); err != nil {
			return err
		}
	}
	// pg_proc (85 blocks), pg_depend (60 blocks), pg_attribute (45 blocks), pg_description (45 blocks), etc.
	heavyRels := []struct{ rel, blks int }{
		{1255, 85}, {2608, 60}, {1249, 45}, {2609, 45}, {2690, 35}, {2691, 35}, {1247, 25}, {2662, 20},
	}
	usedBlocks := 0
	for _, hr := range heavyRels {
		if err := writeRel(baseDir, hr.rel, hr.blks); err != nil {
			return err
		}
		usedBlocks += hr.blks
	}
	for i := 0; i < 240; i++ {
		blks := 2
		if i%3 == 0 {
			blks = 3
		}
		if err := writeRel(baseDir, 3000+i, blks); err != nil {
			return err
		}
		usedBlocks += blks
	}
	// SLRU segments (1 page each for pg_xact, pg_subtrans, multixact)
	for _, sd := range []string{xactDir, subDir, mxOffDir, mxMemDir} {
		if err := os.WriteFile(filepath.Join(sd, "0000"), make([]byte, pageserver.PageSize), 0o600); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	outDir := flag.String("out", "/tmp/air_p22_forensics", "Output directory for pprof and JSON reports")
	numTenants := flag.Int("tenants", 10, "Number of tenants to bootstrap and drive")
	concBootstrap := flag.Bool("conc-bootstrap", false, "Bootstrap tenants concurrently")
	flag.Parse()

	_ = os.RemoveAll(*outDir)
	_ = os.MkdirAll(*outDir, 0o755)

	ctx := context.Background()
	dataDir := filepath.Join(*outDir, "ps_data")
	pgTemplateDir := filepath.Join(*outDir, "pgdata_template")
	if err := buildSyntheticPGData(pgTemplateDir); err != nil {
		log.Fatalf("buildSyntheticPGData: %v", err)
	}

	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(dataDir, "master.key"))
	if err != nil {
		log.Fatalf("init kms: %v", err)
	}
	ps, err := pageserver.New(pageserver.Config{
		DisableUnixSocket: true,
		LayerDir:          filepath.Join(dataDir, "ps_layers"),
		CacheDir:          filepath.Join(dataDir, "ps_cache"),
		CacheMaxBytes:     256 * 1024 * 1024,
		KMS:               kms,
	})
	if err != nil {
		log.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const token = "forensics-token"
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcSrv := pageserver.NewGRPCServer(ps, token)
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	client, err := control.NewGRPCPageserverClient(lis.Addr().String(), token)
	if err != nil {
		log.Fatalf("NewGRPCPageserverClient: %v", err)
	}
	defer client.Close()

	var snaps []StageSnapshot
	snaps = append(snaps, captureSnapshot("00_init", ps, *outDir, true))

	// Stage 1: Bootstrap tenants via gRPC (exact remote path used by air-server -> air-pageserver)
	bootstrapOne := func(i int) error {
		tid := fmt.Sprintf("tenant_%02d", i)
		gen := fmt.Sprintf("gen_%02d", i)
		wdek, err := kms.EnsureTenantDEK(tid)
		if err != nil {
			return err
		}
		if _, err := client.EnsureTenant(ctx, tid, 1, gen, false, wdek); err != nil {
			return err
		}
		if _, err := client.BootstrapTenantFromPGData(ctx, tid, 1, pgTemplateDir, 0x1500000); err != nil {
			return err
		}
		if err := client.SaveMetadataSkeleton(ctx, tid, 1, pgTemplateDir); err != nil {
			return err
		}
		return nil
	}

	if *concBootstrap {
		var wg sync.WaitGroup
		for i := 0; i < *numTenants; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				if err := bootstrapOne(idx); err != nil {
					log.Printf("bootstrap %d failed: %v", idx, err)
				}
			}(i)
		}
		wg.Wait()
		snaps = append(snaps, captureSnapshot("01_after_conc_bootstrap_no_gc", ps, *outDir, false))
		snaps = append(snaps, captureSnapshot("01_after_conc_bootstrap_post_gc", ps, *outDir, true))
	} else {
		for i := 0; i < *numTenants; i++ {
			if err := bootstrapOne(i); err != nil {
				log.Fatalf("bootstrap %d: %v", i, err)
			}
			snaps = append(snaps, captureSnapshot(fmt.Sprintf("01_boot_t%02d_no_gc", i+1), ps, *outDir, false))
			snaps = append(snaps, captureSnapshot(fmt.Sprintf("01_boot_t%02d_post_gc", i+1), ps, *outDir, true))
		}
	}

	// Stage 2: Drive active writes (PutPageBatch via air_smgr + IngestWALRecords via WALReceiver) up to ~15 MB per tenant (just below 16 MB flush threshold!)
	log.Printf("=== Driving active writes & WAL ingestion across %d tenants ===", *numTenants)
	pagePayload := bytes.Repeat([]byte{0x41}, pageserver.PageSize)
	rawWAL := make([]byte, 256)
	var writeWG sync.WaitGroup
	for i := 0; i < *numTenants; i++ {
		writeWG.Add(1)
		go func(idx int) {
			defer writeWG.Done()
			tid := fmt.Sprintf("tenant_%02d", idx)
			gen := fmt.Sprintf("gen_%02d", idx)
			// Write 1,500 pages (~12.3 MB) via PutPageBatch + 500 FPI WAL records (~4.1 MB)
			batch16 := bytes.Repeat(pagePayload, 16)
			for b := uint32(0); b < 90; b++ {
				lsn := uint64(0x2000000 + b*0x1000)
				_ = client.PutPageBatch(ctx, tid, 1663, 16384, 16385, 0, b*16, 16, lsn, batch16)
			}
			// Also simulate WAL ingestion with FPIs
			for w := 0; w < 20; w++ {
				recs := make([]*pageserver.DecodedWALRecord, 0, 16)
				for r := 0; r < 16; r++ {
					lsn := uint64(0x3000000 + (w*16+r)*0x400)
					recs = append(recs, &pageserver.DecodedWALRecord{
						LSN:       lsn,
						EndLSN:    lsn + 256,
						TotalLen:  256,
						XID:       uint32(1000 + w*16 + r),
						Rmid:      pageserver.RmHeapID,
						RawRecord: rawWAL,
						Blocks: []pageserver.WALBlockRef{{
							BlockID:      0,
							Rel:          pageserver.RelFileLocator{SpcOID: 1663, DbOID: 16384, RelNumber: 16385},
							ForkNum:      0,
							BlockNum:     uint32(w*16 + r),
							HasImage:     true,
							ImagePayload: pagePayload,
						}},
					})
				}
				_, _, _ = ps.IngestWALRecords(ctx, tid, 1, gen, recs, true)
			}
		}(i)
	}
	writeWG.Wait()
	snaps = append(snaps, captureSnapshot("02_after_sustained_writes_no_gc", ps, *outDir, false))
	snaps = append(snaps, captureSnapshot("02_after_sustained_writes_post_gc", ps, *outDir, true))

	// Stage 3: Read workload (populating DecodedChunkCache and IndexCache)
	log.Printf("=== Driving concurrent GetPageBatch reads across %d tenants ===", *numTenants)
	for i := 0; i < *numTenants; i++ {
		tid := fmt.Sprintf("tenant_%02d", i)
		for b := uint32(0); b < 80; b += 16 {
			_, _, _ = client.GetPageBatch(ctx, tid, 1663, 16384, 1255, 0, b, 16, 0)
			_, _, _ = client.GetPageBatch(ctx, tid, 1663, 16384, 16385, 0, b, 16, 0)
		}
	}
	snaps = append(snaps, captureSnapshot("03_after_reads_post_gc", ps, *outDir, true))

	// Stage 4: Flush all tenants (simulating tenant suspension)
	log.Printf("=== Flushing all tenants (simulating tenant suspension) ===")
	for i := 0; i < *numTenants; i++ {
		tid := fmt.Sprintf("tenant_%02d", i)
		_, _ = client.FlushTenant(ctx, tid)
	}
	time.Sleep(100 * time.Millisecond)
	snaps = append(snaps, captureSnapshot("04_after_suspend_flush_post_gc", ps, *outDir, true))

	reportBytes, _ := json.MarshalIndent(snaps, "", "  ")
	_ = os.WriteFile(filepath.Join(*outDir, "forensics_snapshots.json"), reportBytes, 0o644)
	log.Printf("Saved forensics report to %s/forensics_snapshots.json", *outDir)
}
