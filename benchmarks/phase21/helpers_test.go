package main

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestReadProcRSSMBFromRoot_MockAndLiveProc(t *testing.T) {
	// 1. Mock /proc tree with two postgres processes and one unrelated process
	tmp := t.TempDir()
	mkProc := func(pid, comm, cmdline, status string) {
		p := filepath.Join(tmp, pid)
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
		_ = os.WriteFile(filepath.Join(p, "comm"), []byte(comm+"\n"), 0o644)
		_ = os.WriteFile(filepath.Join(p, "cmdline"), []byte(cmdline), 0o644)
		_ = os.WriteFile(filepath.Join(p, "status"), []byte(status), 0o644)
	}
	mkProc("101", "postgres", "/usr/lib/postgresql/17/bin/postgres\x00-D\x00/data\x00", "Name:\tpostgres\nVmRSS:\t   20480 kB\n")
	mkProc("102", "postmaster", "/usr/lib/postgresql/17/bin/postgres\x00-D\x00/data\x00", "Name:\tpostmaster\nVmRSS:\t   10240 kB\n")
	mkProc("201", "air-server", "/usr/local/bin/air-server\x00", "Name:\tair-server\nVmRSS:\t   30720 kB\n")

	pgRSS, err := readProcRSSMBFromRoot(tmp, "postgres")
	if err != nil {
		t.Fatalf("readProcRSSMBFromRoot(postgres): %v", err)
	}
	if math.Abs(pgRSS-30.0) > 0.01 {
		t.Fatalf("expected 30.0 MB for postgres processes, got %.2f", pgRSS)
	}

	// Missing process must return NaN + error, never 0.00
	missingRSS, err := readProcRSSMBFromRoot(tmp, "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing process")
	}
	if !math.IsNaN(missingRSS) || fmtMetric(missingRSS) != "NA" {
		t.Fatalf("expected NaN/NA for missing process, got %v (%s)", missingRSS, fmtMetric(missingRSS))
	}

	// 2. Verify against live Linux /proc for the current test process
	selfCommRaw, err := os.ReadFile("/proc/self/comm")
	if err != nil {
		t.Skipf("cannot read /proc/self/comm: %v", err)
	}
	selfComm := string(selfCommRaw)
	if len(selfComm) > 0 && selfComm[len(selfComm)-1] == '\n' {
		selfComm = selfComm[:len(selfComm)-1]
	}
	liveRSS, err := readProcRSSMBFromRoot("/proc", selfComm)
	if err != nil {
		t.Fatalf("readProcRSSMBFromRoot(/proc, %q): %v", selfComm, err)
	}
	if math.IsNaN(liveRSS) || liveRSS <= 0 {
		t.Fatalf("expected positive live RSS for %q, got %v", selfComm, liveRSS)
	}
}

func TestParseCgroupSampleOutput_UnavailableNotZero(t *testing.T) {
	raw := "104857600\n134217728\n536870912\n41943040 52428800\n0 0 0\n0.12\n12.34\n0.05\n9876543\nNA\n"
	s := parseCgroupSampleOutput(raw)
	if math.Abs(s.MemCurrentMB-100.0) > 0.01 {
		t.Fatalf("MemCurrentMB = %v, want 100", s.MemCurrentMB)
	}
	if math.Abs(s.MemPeakMB-128.0) > 0.01 {
		t.Fatalf("MemPeakMB = %v, want 128", s.MemPeakMB)
	}
	if math.Abs(s.AnonMB-40.0) > 0.01 {
		t.Fatalf("AnonMB = %v, want 40", s.AnonMB)
	}
	if !s.CPUUsageAvail || s.CPUUsageUsec != 9876543 {
		t.Fatalf("CPUUsage = (%v, %d), want (true, 9876543)", s.CPUUsageAvail, s.CPUUsageUsec)
	}
	if !math.IsNaN(s.PGRSSMB) || fmtMetric(s.PGRSSMB) != "NA" {
		t.Fatalf("expected PGRSSMB=NA when unavailable, got %v (%s)", s.PGRSSMB, fmtMetric(s.PGRSSMB))
	}
}
