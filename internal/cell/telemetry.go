package cell

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Mahadi-rsio/air/internal/tenant"
)

// ResourceSnapshot captures point-in-time cgroup v2 and Linux PSI resource telemetry
// for a tenant or cell host.
type ResourceSnapshot struct {
	Timestamp          time.Time `json:"timestamp"`
	TenantID           string    `json:"tenant_id,omitempty"`
	MemoryCurrentBytes uint64    `json:"memory_current_bytes"`
	MemoryHighBytes    uint64    `json:"memory_high_bytes"`
	MemoryMaxBytes     uint64    `json:"memory_max_bytes"`
	MemoryHighEvents   uint64    `json:"memory_high_events"`
	MemoryMaxEvents    uint64    `json:"memory_max_events"`
	MemoryOOMEvents    uint64    `json:"memory_oom_events"`
	MemoryPSISomeAvg10 float64   `json:"memory_psi_some_avg10"`
	MemoryPSIFullAvg10 float64   `json:"memory_psi_full_avg10"`
	CPUPSISomeAvg10    float64   `json:"cpu_psi_some_avg10"`
	IOPSIMaxAvg10      float64   `json:"io_psi_max_avg10"`
	CPUUsageUsec       uint64    `json:"cpu_usage_usec"`
	CPUThrottledUsec   uint64    `json:"cpu_throttled_usec"`
	PGActiveConns      int       `json:"pg_active_conns,omitempty"`
	PGActiveQueries    int       `json:"pg_active_queries,omitempty"`
	Source             string    `json:"source"`
}

func readCgroupUint64(path string) (uint64, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "max" {
		return 0, true
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func parseKVUint64(raw string) map[string]uint64 {
	out := make(map[string]uint64)
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			if v, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
				out[fields[0]] = v
			}
		}
	}
	return out
}

// parsePSI parses Linux PSI format:
//
//	some avg10=0.00 avg60=0.00 avg300=0.00 total=0
//	full avg10=0.00 avg60=0.00 avg300=0.00 total=0
func parsePSI(raw string) (someAvg10, fullAvg10 float64, ok bool) {
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		kind := fields[0]
		for _, f := range fields[1:] {
			if strings.HasPrefix(f, "avg10=") {
				v, err := strconv.ParseFloat(strings.TrimPrefix(f, "avg10="), 64)
				if err == nil {
					ok = true
					if kind == "some" {
						someAvg10 = v
					} else if kind == "full" {
						fullAvg10 = v
					}
				}
			}
		}
	}
	return someAvg10, fullAvg10, ok
}

func readPSIFile(path string) (someAvg10, fullAvg10 float64, ok bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, false
	}
	return parsePSI(string(raw))
}

func (a *Agent) collectFromCgroupDir(cgDir, tenantID string) ResourceSnapshot {
	snap := ResourceSnapshot{
		Timestamp: time.Now().UTC(),
		TenantID:  tenantID,
		Source:    "unavailable",
	}
	if cgDir == "" {
		return snap
	}

	hasCgroup := false
	if v, ok := readCgroupUint64(filepath.Join(cgDir, "memory.current")); ok {
		snap.MemoryCurrentBytes = v
		hasCgroup = true
	}
	if v, ok := readCgroupUint64(filepath.Join(cgDir, "memory.high")); ok {
		snap.MemoryHighBytes = v
	}
	if v, ok := readCgroupUint64(filepath.Join(cgDir, "memory.max")); ok {
		snap.MemoryMaxBytes = v
	}
	if raw, err := os.ReadFile(filepath.Join(cgDir, "memory.events")); err == nil {
		ev := parseKVUint64(string(raw))
		snap.MemoryHighEvents = ev["high"]
		snap.MemoryMaxEvents = ev["max"]
		snap.MemoryOOMEvents = ev["oom"] + ev["oom_kill"]
		hasCgroup = true
	}
	if raw, err := os.ReadFile(filepath.Join(cgDir, "cpu.stat")); err == nil {
		st := parseKVUint64(string(raw))
		snap.CPUUsageUsec = st["usage_usec"]
		snap.CPUThrottledUsec = st["throttled_usec"]
		hasCgroup = true
	}

	if s, f, ok := readPSIFile(filepath.Join(cgDir, "memory.pressure")); ok {
		snap.MemoryPSISomeAvg10 = s
		snap.MemoryPSIFullAvg10 = f
		hasCgroup = true
	} else if s, f, ok := readPSIFile("/proc/pressure/memory"); ok {
		snap.MemoryPSISomeAvg10 = s
		snap.MemoryPSIFullAvg10 = f
	}

	if s, _, ok := readPSIFile(filepath.Join(cgDir, "cpu.pressure")); ok {
		snap.CPUPSISomeAvg10 = s
	} else if s, _, ok := readPSIFile("/proc/pressure/cpu"); ok {
		snap.CPUPSISomeAvg10 = s
	}

	if s, f, ok := readPSIFile(filepath.Join(cgDir, "io.pressure")); ok {
		if f > s {
			snap.IOPSIMaxAvg10 = f
		} else {
			snap.IOPSIMaxAvg10 = s
		}
	} else if s, f, ok := readPSIFile("/proc/pressure/io"); ok {
		if f > s {
			snap.IOPSIMaxAvg10 = f
		} else {
			snap.IOPSIMaxAvg10 = s
		}
	}

	if hasCgroup {
		snap.Source = "cgroup_v2"
	} else if snap.MemoryPSISomeAvg10 > 0 || snap.CPUPSISomeAvg10 > 0 || snap.IOPSIMaxAvg10 > 0 {
		snap.Source = "proc_psi"
	}
	return snap
}

// CollectTenantTelemetry reads cgroup v2 and PSI telemetry for a single tenant.
func (a *Agent) CollectTenantTelemetry(tenantID string) ResourceSnapshot {
	if tenant.ValidateID(tenantID) != nil {
		return ResourceSnapshot{Timestamp: time.Now().UTC(), TenantID: tenantID, Source: "unavailable"}
	}
	a.tenantMu.RLock()
	root := a.cgroupRoot
	a.tenantMu.RUnlock()
	if root == "" {
		return ResourceSnapshot{Timestamp: time.Now().UTC(), TenantID: tenantID, Source: "unavailable"}
	}
	return a.collectFromCgroupDir(filepath.Join(root, "air_tenant_"+tenantID), tenantID)
}

// CollectCellTelemetry reads cell-wide cgroup v2 and PSI telemetry from cgroupRoot.
func (a *Agent) CollectCellTelemetry() ResourceSnapshot {
	a.tenantMu.RLock()
	root := a.cgroupRoot
	a.tenantMu.RUnlock()
	return a.collectFromCgroupDir(root, "")
}
