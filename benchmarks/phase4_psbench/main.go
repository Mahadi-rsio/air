package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

type BenchRow struct {
	Scenario   string  `json:"scenario"`
	Readers    int     `json:"readers"`
	PagesPerS  float64 `json:"pages_per_sec"`
	TotalPages int64   `json:"total_pages"`
	Errors     int64   `json:"errors"`
	P50Us      float64 `json:"p50_us"`
	P95Us      float64 `json:"p95_us"`
	P99Us      float64 `json:"p99_us"`
	MemMiB     float64 `json:"mem_mib"`
}

func encodeReq(tenantID string, op uint16, spcOID, dbOID, relNum uint32, fork uint8, blk uint32, nblks uint32, lsn uint64) []byte {
	buf := make([]byte, pageserver.ReqHeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], pageserver.ProtocolMagic)
	binary.LittleEndian.PutUint16(buf[4:6], pageserver.ProtocolVersion)
	binary.LittleEndian.PutUint16(buf[6:8], op)
	copy(buf[8:8+pageserver.TenantIDMaxLen], tenantID)
	binary.LittleEndian.PutUint32(buf[72:76], spcOID)
	binary.LittleEndian.PutUint32(buf[76:80], dbOID)
	binary.LittleEndian.PutUint32(buf[80:84], relNum)
	buf[84] = fork
	binary.LittleEndian.PutUint32(buf[88:92], blk)
	binary.LittleEndian.PutUint32(buf[92:96], nblks)
	binary.LittleEndian.PutUint64(buf[96:104], lsn)
	return buf
}

func getPage(conn net.Conn, reqBuf, respHdr, pageBuf []byte) (uint16, error) {
	if _, err := conn.Write(reqBuf); err != nil {
		return 0, err
	}
	if _, err := io.ReadFull(conn, respHdr); err != nil {
		return 0, err
	}
	status := binary.LittleEndian.Uint16(respHdr[6:8])
	plen := binary.LittleEndian.Uint32(respHdr[8:12])
	if plen > 0 {
		if int(plen) > len(pageBuf) {
			tmp := make([]byte, plen)
			if _, err := io.ReadFull(conn, tmp); err != nil {
				return status, err
			}
		} else {
			if _, err := io.ReadFull(conn, pageBuf[:plen]); err != nil {
				return status, err
			}
		}
	}
	return status, nil
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	k := float64(len(sorted)-1) * (p / 100.0)
	f := int(math.Floor(k))
	c := int(math.Ceil(k))
	if f == c {
		return sorted[f]
	}
	return sorted[f]*(float64(c)-k) + sorted[c]*(k-float64(f))
}

func containerMemMiB() float64 {
	out, err := exec.Command("docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", "air-bench").Output()
	if err != nil {
		return 0
	}
	parts := strings.Fields(string(out))
	if len(parts) == 0 {
		return 0
	}
	s := parts[0]
	if strings.HasSuffix(s, "GiB") {
		v, _ := strconv.ParseFloat(strings.TrimSuffix(s, "GiB"), 64)
		return v * 1024
	}
	if strings.HasSuffix(s, "MiB") {
		v, _ := strconv.ParseFloat(strings.TrimSuffix(s, "MiB"), 64)
		return v
	}
	return 0
}

func runReaders(sock, tenant string, spc, db, rel, maxBlk uint32, readers int, dur time.Duration, sameBlock bool, scenario string) BenchRow {
	var okCnt, errCnt atomic.Int64
	var latMu sync.Mutex
	var lats []float64
	var wg sync.WaitGroup
	stopAt := time.Now().Add(dur)

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(rid int) {
			defer wg.Done()
			conn, err := net.Dial("unix", sock)
			if err != nil {
				errCnt.Add(1)
				return
			}
			defer conn.Close()
			rng := rand.New(rand.NewSource(int64(rid+1) * 1337))
			respHdr := make([]byte, pageserver.RespHeaderSize)
			pageBuf := make([]byte, pageserver.PageSize)
			var localLats []float64
			for time.Now().Before(stopAt) {
				blk := uint32(0)
				if !sameBlock && maxBlk > 0 {
					blk = uint32(rng.Intn(int(maxBlk)))
				}
				req := encodeReq(tenant, pageserver.OpGetPageAtLSN, spc, db, rel, 0, blk, 1, 0)
				t0 := time.Now()
				st, err := getPage(conn, req, respHdr, pageBuf)
				us := float64(time.Since(t0).Nanoseconds()) / 1000.0
				if err != nil || st == pageserver.StatusError {
					errCnt.Add(1)
					continue
				}
				okCnt.Add(1)
				localLats = append(localLats, us)
			}
			latMu.Lock()
			lats = append(lats, localLats...)
			latMu.Unlock()
		}(r)
	}
	wg.Wait()
	sort.Float64s(lats)
	return BenchRow{
		Scenario:   scenario,
		Readers:    readers,
		PagesPerS:  math.Round(float64(okCnt.Load())/dur.Seconds()*100) / 100,
		TotalPages: okCnt.Load(),
		Errors:     errCnt.Load(),
		P50Us:      math.Round(pct(lats, 50)*100) / 100,
		P95Us:      math.Round(pct(lats, 95)*100) / 100,
		P99Us:      math.Round(pct(lats, 99)*100) / 100,
		MemMiB:     containerMemMiB(),
	}
}

func main() {
	sock := flag.String("sock", "/tmp/air-bench-data/pageserver.sock", "Pageserver Unix socket")
	tenant := flag.String("tenant", "", "Tenant ID")
	spc := flag.Uint("spc", 1663, "Tablespace OID")
	db := flag.Uint("db", 0, "Database OID")
	rel := flag.Uint("rel", 0, "Relation filenode")
	maxBlk := flag.Uint("blocks", 1000, "Max block count")
	outFile := flag.String("out", "/workspaces/air/benchmarks/results/phase4/psbench_results.json", "Output JSON")
	flag.Parse()

	if *tenant == "" || *db == 0 || *rel == 0 {
		log.Fatal("must provide -tenant, -db, -rel")
	}
	_ = os.MkdirAll(filepath.Dir(*outFile), 0o755)
	_ = context.Background()

	var rows []BenchRow
	// Warm-up
	_ = runReaders(*sock, *tenant, uint32(*spc), uint32(*db), uint32(*rel), uint32(*maxBlk), 4, 2*time.Second, false, "warmup")

	for _, r := range []int{1, 8, 32, 64, 128} {
		row := runReaders(*sock, *tenant, uint32(*spc), uint32(*db), uint32(*rel), uint32(*maxBlk), r, 8*time.Second, false, "warm_random_reads")
		log.Printf("%+v", row)
		rows = append(rows, row)
	}

	// Concurrent repeated reads of same page (64 readers)
	rowSame := runReaders(*sock, *tenant, uint32(*spc), uint32(*db), uint32(*rel), uint32(*maxBlk), 64, 5*time.Second, true, "same_page_64_readers")
	log.Printf("%+v", rowSame)
	rows = append(rows, rowSame)

	b, _ := json.MarshalIndent(rows, "", "  ")
	_ = os.WriteFile(*outFile, b, 0o644)
	fmt.Println(string(b))
}
