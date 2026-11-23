package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
)

func writeJSON(path string, v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	_ = os.WriteFile(path, b, 0o644)
}

func main() {
	suite := flag.String("suite", "all", "Suite to run: workloads, sweep, memory512, multitenant, correctness, sustained, all")
	durSec := flag.Int("duration", 8, "Per-test duration in seconds for workloads/sweep/memory/fairness")
	sustainedSec := flag.Int("sustained-sec", 60, "Duration in seconds for sustained load test")
	sustainedClients := flag.Int("sustained-clients", 8, "Concurrent clients for sustained load test")
	gatewayBin := flag.String("gateway-bin", "/workspaces/air/benchmarks/phase20.9/bin/air-gateway", "Path to air-gateway binary")
	outDir := flag.String("out-dir", "/workspaces/air/benchmarks/phase20.9/results", "Output directory for JSON results")
	workDir := flag.String("work-dir", "/tmp/air-phase209-work", "Temporary PGDATA/work directory")
	flag.Parse()

	ctx := context.Background()
	dur := time.Duration(*durSec) * time.Second
	_ = os.MkdirAll(*outDir, 0o755)

	fmt.Printf("=== AIR Phase 20.9 Real PostgreSQL 17 Validation Harness ===\n")
	fmt.Printf("Suite=%s Duration=%s Sustained=%ds GatewayBin=%s OutDir=%s\n", *suite, dur, *sustainedSec, *gatewayBin, *outDir)

	if *suite == "workloads" || *suite == "all" {
		fmt.Println("\n--- STAGE: Workload Suite (INSERT, UPDATE, UPSERT, DELETE, COPY, TX) ---")
		limits := cell.CgroupLimits{
			MemoryHigh: strconv.Itoa(435 * 1024 * 1024),
			MemoryMax:  strconv.Itoa(512 * 1024 * 1024),
			SwapMax:    "0",
			CPUMax:     "max 100000",
			PIDsMax:    "256",
		}
		env, err := NewBenchEnv(filepath.Join(*workDir, "workloads"), *gatewayBin, defaultCgroupRoot, limits)
		if err != nil {
			fmt.Fprintf(os.Stderr, "workloads env error: %v\n", err)
			os.Exit(1)
		}
		if _, err := env.ProvisionTenant("t_work"); err != nil {
			env.Cleanup()
			fmt.Fprintf(os.Stderr, "provision t_work: %v\n", err)
			os.Exit(1)
		}
		if err := env.StartGateway(ProfileFullPhase20(), "t_work"); err != nil {
			env.Cleanup()
			fmt.Fprintf(os.Stderr, "start gateway: %v\n", err)
			os.Exit(1)
		}
		time.Sleep(1100 * time.Millisecond)
		res, err := runWorkloadSuite(ctx, env, "t_work", dur)
		env.Cleanup()
		if err != nil {
			fmt.Fprintf(os.Stderr, "workload suite error: %v\n", err)
			os.Exit(1)
		}
		writeJSON(filepath.Join(*outDir, "workloads.json"), res)
	}

	if *suite == "sweep" || *suite == "all" {
		fmt.Println("\n--- STAGE: Concurrency Sweep (1,2,4,8,16,32 across 5 Phase 20 Configs) ---")
		limits := cell.CgroupLimits{
			MemoryHigh: strconv.Itoa(435 * 1024 * 1024),
			MemoryMax:  strconv.Itoa(512 * 1024 * 1024),
			SwapMax:    "0",
			CPUMax:     "max 100000",
			PIDsMax:    "256",
		}
		env, err := NewBenchEnv(filepath.Join(*workDir, "sweep"), *gatewayBin, defaultCgroupRoot, limits)
		if err != nil {
			fmt.Fprintf(os.Stderr, "sweep env error: %v\n", err)
			os.Exit(1)
		}
		if _, err := env.ProvisionTenant("t_sweep"); err != nil {
			env.Cleanup()
			fmt.Fprintf(os.Stderr, "provision t_sweep: %v\n", err)
			os.Exit(1)
		}
		pts, err := runConcurrencySweep(ctx, env, "t_sweep", dur)
		env.Cleanup()
		if err != nil {
			fmt.Fprintf(os.Stderr, "sweep error: %v\n", err)
			os.Exit(1)
		}
		writeJSON(filepath.Join(*outDir, "sweep.json"), pts)
	}

	if *suite == "memory512" || *suite == "all" {
		fmt.Println("\n--- STAGE: 512MB / 256MB / 1GB Cgroup v2 Memory Validation & AIMD ---")
		pts, trans, err := runMemory512Suite(ctx, *gatewayBin, *workDir, dur)
		if err != nil {
			fmt.Fprintf(os.Stderr, "memory512 error: %v\n", err)
			os.Exit(1)
		}
		writeJSON(filepath.Join(*outDir, "memory512.json"), pts)
		writeJSON(filepath.Join(*outDir, "adaptive_transitions.json"), trans)
	}

	if *suite == "multitenant" || *suite == "all" {
		fmt.Println("\n--- STAGE: 4-Tenant Fairness Validation (DRR) ---")
		rows, err := runMultiTenantFairnessSuite(ctx, *gatewayBin, *workDir, dur)
		if err != nil {
			fmt.Fprintf(os.Stderr, "multitenant error: %v\n", err)
			os.Exit(1)
		}
		writeJSON(filepath.Join(*outDir, "multitenant.json"), rows)
	}

	if *suite == "correctness" || *suite == "all" {
		fmt.Println("\n--- STAGE: PostgreSQL 17 Correctness, Crash Recovery & Lazy LSN ---")
		res, err := runCorrectnessSuite(ctx, *gatewayBin, *workDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "correctness error: %v\n", err)
			os.Exit(1)
		}
		writeJSON(filepath.Join(*outDir, "correctness.json"), res)
		for _, r := range res {
			if !r.Passed {
				fmt.Fprintf(os.Stderr, "CRITICAL: correctness check %q FAILED: %s\n", r.Name, r.Detail)
				os.Exit(1)
			}
		}
	}

	if *suite == "sustained" || *suite == "all" {
		fmt.Printf("\n--- STAGE: Sustained Load Stability Test (%ds, %d clients) ---\n", *sustainedSec, *sustainedClients)
		samples, err := runSustainedSuite(ctx, *gatewayBin, *workDir, time.Duration(*sustainedSec)*time.Second, *sustainedClients)
		if err != nil {
			fmt.Fprintf(os.Stderr, "sustained error: %v\n", err)
			os.Exit(1)
		}
		writeJSON(filepath.Join(*outDir, fmt.Sprintf("sustained_%ds.json", *sustainedSec)), samples)
	}

	fmt.Println("\n=== Phase 20.9 Harness Completed ===")
}
