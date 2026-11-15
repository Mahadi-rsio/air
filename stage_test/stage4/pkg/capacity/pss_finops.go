package capacity

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"air-stage4/pkg/cell"
	"github.com/jackc/pgx/v5"
)

type ClusterMemoryProfile struct {
	ProcessCount       int     `json:"process_count"`
	NaiveSummedRSSMB   float64 `json:"naive_summed_rss_mb"`
	TrueKernelPSSMB    float64 `json:"true_kernel_pss_mb"`
	TrueKernelUSSMB    float64 `json:"true_kernel_uss_mb"`
	SharedCleanDirtyMB float64 `json:"shared_clean_dirty_mb"`
	RSSOvercountRatio  float64 `json:"rss_overcount_ratio"`
}

type SKUDensityEntry struct {
	SKUName               string  `json:"sku_name"`
	HostRAMMB             int     `json:"host_ram_mb"`
	MonthlyInstanceCost   float64 `json:"monthly_instance_cost_usd"`
	SafeMaxHotTenantsPSS  int     `json:"safe_max_hot_tenants_pss"`
	NaiveMaxHotTenantsRSS int     `json:"naive_max_hot_tenants_rss"`
}

type FinOpsMatrixEntry struct {
	SKUName                  string  `json:"sku_name"`
	ActiveRatioPct           int     `json:"active_ratio_pct"`
	ProvisionedTenantsPerVM  int     `json:"provisioned_tenants_per_vm"`
	ActiveHotTenantsPerVM    int     `json:"active_hot_tenants_per_vm"`
	MonthlyComputeCostPerTen float64 `json:"monthly_compute_cost_per_tenant_usd"`
	MonthlyNVMeCostPerTen    float64 `json:"monthly_nvme_cost_per_tenant_usd"`
	MonthlyR2CostPerTen      float64 `json:"monthly_r2_cost_per_tenant_usd"`
	TotalMonthlyCOGSPerTen   float64 `json:"total_monthly_cogs_per_tenant_usd"`
	GrossMarginAt5USDPrice   float64 `json:"gross_margin_pct_at_5usd_price"`
}

type Workstream9Report struct {
	Pass                 bool                 `json:"pass"`
	IdleClusterProfile   ClusterMemoryProfile `json:"idle_cluster_profile"`
	LoadedClusterProfile ClusterMemoryProfile `json:"loaded_cluster_profile"`
	SKUDensityTable      []SKUDensityEntry    `json:"sku_density_table"`
	FinOpsSensitivity    []FinOpsMatrixEntry  `json:"finops_sensitivity_matrix"`
}

// MeasureClusterPSS walks /proc to find postmasterPID and all child processes in its process group / parent tree,
// reading /proc/<pid>/smaps_rollup to compute true PSS, USS, and naive summed VmRSS.
func MeasureClusterPSS(postmasterPID int) (ClusterMemoryProfile, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return ClusterMemoryProfile{}, err
	}

	pids := map[int]bool{postmasterPID: true}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid <= 1 {
			continue
		}
		statBytes, err := os.ReadFile(filepath.Join("/proc", e.Name(), "stat"))
		if err != nil {
			continue
		}
		// /proc/<pid>/stat format: pid (comm) state ppid ...
		s := string(statBytes)
		rparen := strings.LastIndex(s, ")")
		if rparen < 0 || rparen+2 >= len(s) {
			continue
		}
		fields := strings.Fields(s[rparen+2:])
		if len(fields) >= 2 {
			ppid, _ := strconv.Atoi(fields[1])
			if ppid == postmasterPID {
				pids[pid] = true
			}
		}
	}

	var rssKB, pssKB, privCleanKB, privDirtyKB, sharedCleanKB, sharedDirtyKB float64
	procCount := 0

	for pid := range pids {
		smapsPath := fmt.Sprintf("/proc/%d/smaps_rollup", pid)
		f, err := os.Open(smapsPath)
		if err != nil {
			continue
		}
		procCount++
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			parts := strings.Fields(line)
			if len(parts) < 2 {
				continue
			}
			val, err := strconv.ParseFloat(parts[1], 64)
			if err != nil {
				continue
			}
			switch parts[0] {
			case "Rss:":
				rssKB += val
			case "Pss:":
				pssKB += val
			case "Private_Clean:":
				privCleanKB += val
			case "Private_Dirty:":
				privDirtyKB += val
			case "Shared_Clean:":
				sharedCleanKB += val
			case "Shared_Dirty:":
				sharedDirtyKB += val
			}
		}
		_ = f.Close()
	}

	if procCount == 0 || pssKB == 0 {
		return ClusterMemoryProfile{}, fmt.Errorf("failed to read smaps_rollup for postmaster pid %d", postmasterPID)
	}

	rssMB := round2(rssKB / 1024.0)
	pssMB := round2(pssKB / 1024.0)
	ussMB := round2((privCleanKB + privDirtyKB) / 1024.0)
	sharedMB := round2((sharedCleanKB + sharedDirtyKB) / 1024.0)
	overcount := round2(rssKB / pssKB)

	return ClusterMemoryProfile{
		ProcessCount:       procCount,
		NaiveSummedRSSMB:   rssMB,
		TrueKernelPSSMB:    pssMB,
		TrueKernelUSSMB:    ussMB,
		SharedCleanDirtyMB: sharedMB,
		RSSOvercountRatio:  overcount,
	}, nil
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

func RunWorkstream9CapacityAndFinOps(ctx context.Context, agent *cell.Agent) (Workstream9Report, error) {
	var rep Workstream9Report
	tenant := "t_ws9_pss"
	if _, err := agent.ClaimCluster(tenant); err != nil {
		return rep, err
	}
	rt, err := agent.ActivateCluster(tenant, "pw_ws9", 1)
	if err != nil {
		return rep, err
	}
	defer func() { _ = agent.StopAndPurgeTenant(tenant) }()

	// 1. Measure idle cluster PSS vs naive summed RSS
	idleProf, err := MeasureClusterPSS(rt.PID)
	if err != nil {
		return rep, err
	}

	// 2. Open 5 concurrent active connections and warm shared_buffers + sort memory to measure loaded PSS vs RSS
	dsn := fmt.Sprintf("postgres://app_user:pw_ws9@127.0.0.1:%d/postgres?sslmode=disable", rt.Port)
	conns := make([]*pgx.Conn, 0, 5)
	for i := 0; i < 5; i++ {
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			for _, prev := range conns {
				_ = prev.Close(ctx)
			}
			return rep, err
		}
		conns = append(conns, c)
	}
	defer func() {
		for _, c := range conns {
			_ = c.Close(ctx)
		}
	}()

	_, _ = conns[0].Exec(ctx, "CREATE TABLE pss_load (id int, data text); INSERT INTO pss_load SELECT g, repeat('X', 256) FROM generate_series(1, 5000) g;")
	for _, c := range conns {
		var cnt int
		_ = c.QueryRow(ctx, "SELECT count(*) FROM (SELECT * FROM pss_load ORDER BY data DESC) s").Scan(&cnt)
	}

	loadedProf, err := MeasureClusterPSS(rt.PID)
	if err != nil {
		return rep, err
	}

	// 3. Build SKU Density Table using 75% usable RAM budget (25% reserved for OS kernel + Go Cell Agent + PgBouncer/Virtualizer)
	type skuSpec struct {
		name string
		ram  int
		cost float64
	}
	skus := []skuSpec{
		{"shared-cpu-1x-256mb", 256, 1.94},
		{"shared-cpu-2x-2gb", 2048, 12.26},
		{"performance-2x-4gb", 4096, 62.00},
		{"performance-4x-8gb", 8192, 124.00},
	}

	var densityTable []SKUDensityEntry
	var finopsMatrix []FinOpsMatrixEntry

	hotPSS := loadedProf.TrueKernelPSSMB
	if hotPSS < 15.0 {
		hotPSS = 15.0
	}
	hotRSS := loadedProf.NaiveSummedRSSMB
	if hotRSS < 40.0 {
		hotRSS = 40.0
	}

	activeRatios := []int{5, 10, 25, 50}
	nvmeCostPerTen := 0.08
	r2CostPerTen := 0.02

	for _, s := range skus {
		usableRAM := float64(s.ram) * 0.75
		safeHot := int(math.Floor(usableRAM / hotPSS))
		naiveHot := int(math.Floor(usableRAM / hotRSS))
		densityTable = append(densityTable, SKUDensityEntry{
			SKUName:               s.name,
			HostRAMMB:             s.ram,
			MonthlyInstanceCost:   s.cost,
			SafeMaxHotTenantsPSS:  safeHot,
			NaiveMaxHotTenantsRSS: naiveHot,
		})

		if safeHot <= 0 {
			continue
		}

		for _, ratioPct := range activeRatios {
			provisioned := int(math.Floor(float64(safeHot) / (float64(ratioPct) / 100.0)))
			if provisioned < safeHot {
				provisioned = safeHot
			}
			computePerTen := round2(s.cost / float64(provisioned))
			totalCOGS := round2(computePerTen + nvmeCostPerTen + r2CostPerTen)
			marginAt5 := round2(((5.00 - totalCOGS) / 5.00) * 100.0)

			finopsMatrix = append(finopsMatrix, FinOpsMatrixEntry{
				SKUName:                  s.name,
				ActiveRatioPct:           ratioPct,
				ProvisionedTenantsPerVM:  provisioned,
				ActiveHotTenantsPerVM:    safeHot,
				MonthlyComputeCostPerTen: computePerTen,
				MonthlyNVMeCostPerTen:    nvmeCostPerTen,
				MonthlyR2CostPerTen:      r2CostPerTen,
				TotalMonthlyCOGSPerTen:   totalCOGS,
				GrossMarginAt5USDPrice:   marginAt5,
			})
		}
	}

	rep.IdleClusterProfile = idleProf
	rep.LoadedClusterProfile = loadedProf
	rep.SKUDensityTable = densityTable
	rep.FinOpsSensitivity = finopsMatrix
	rep.Pass = loadedProf.TrueKernelPSSMB > 0 &&
		loadedProf.RSSOvercountRatio > 1.5 &&
		len(densityTable) == 4 &&
		len(finopsMatrix) >= 12

	return rep, nil
}
