package cell

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCgroupV2SetupAndTeardown_TempRoot(t *testing.T) {
	volDir := t.TempDir()
	cgRoot := t.TempDir()

	ag, err := NewAgent(volDir, "", nil)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer ag.Close()
	ag.SetCgroupRoot(cgRoot)

	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	pid := cmd.Process.Pid
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	tenantID := "t_cg_unit_01"
	cgPath, ok := ag.setupTenantCgroupV2(tenantID, pid)
	if !ok {
		t.Fatalf("expected setupTenantCgroupV2 to succeed on writable temp root")
	}
	expectedDir := filepath.Join(cgRoot, "air_tenant_"+tenantID)
	if cgPath != expectedDir {
		t.Fatalf("expected cgPath %q, got %q", expectedDir, cgPath)
	}

	limits := DefaultCgroupLimits()
	checks := map[string]string{
		"memory.high":     limits.MemoryHigh,
		"memory.max":      limits.MemoryMax,
		"memory.swap.max": limits.SwapMax,
		"cpu.max":         limits.CPUMax,
		"pids.max":        limits.PIDsMax,
		"cgroup.procs":    strconv.Itoa(pid),
	}
	for file, want := range checks {
		gotBytes, err := os.ReadFile(filepath.Join(cgPath, file))
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		if got := strings.TrimSpace(string(gotBytes)); got != want {
			t.Fatalf("%s: want %q, got %q", file, want, got)
		}
	}

	if err := ag.killAndRemoveTenantCgroupV2(tenantID, pid); err != nil {
		t.Fatalf("killAndRemoveTenantCgroupV2: %v", err)
	}
	_ = cmd.Wait()

	if isProcessAlive(pid) {
		t.Fatalf("expected pid %d to be dead after killAndRemoveTenantCgroupV2", pid)
	}
	killBytes, err := os.ReadFile(filepath.Join(cgPath, "cgroup.kill"))
	if err != nil {
		t.Fatalf("read cgroup.kill: %v", err)
	}
	if strings.TrimSpace(string(killBytes)) != "1" {
		t.Fatalf("expected cgroup.kill=1, got %q", strings.TrimSpace(string(killBytes)))
	}
}

func TestCgroupV2GracefulDegradation_AndProcessTreeKill(t *testing.T) {
	volDir := t.TempDir()
	unwritableParent := filepath.Join(t.TempDir(), "ro")
	if err := os.MkdirAll(unwritableParent, 0o555); err != nil {
		t.Fatalf("mkdir ro: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(unwritableParent, 0o755) })

	ag, err := NewAgent(volDir, "", nil)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer ag.Close()
	ag.SetCgroupRoot(filepath.Join(unwritableParent, "cgroup"))

	// Spawn a parent shell with a background child process to verify recursive /proc tree reaping.
	cmd := exec.Command("sh", "-c", "sleep 60 & wait")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start process tree: %v", err)
	}
	rootPID := cmd.Process.Pid
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	var tree []int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		tree = collectProcessTree(rootPID)
		if len(tree) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(tree) < 2 {
		t.Fatalf("expected process tree with >=2 PIDs, got %v", tree)
	}

	// Must degrade gracefully when cgroupfs is unwritable.
	if cgPath, ok := ag.setupTenantCgroupV2("t_cg_degrade", rootPID); ok || cgPath != "" {
		t.Fatalf("expected graceful degradation on unwritable cgroup root, got (%q, %v)", cgPath, ok)
	}

	// Teardown must still kill the entire descendant tree via /proc + SIGKILL + waitForPIDsExit.
	if err := ag.killAndRemoveTenantCgroupV2("t_cg_degrade", rootPID); err != nil {
		t.Fatalf("killAndRemoveTenantCgroupV2: %v", err)
	}
	_ = cmd.Wait()

	for _, p := range tree {
		if isProcessAlive(p) {
			t.Fatalf("descendant pid %d survived killAndRemoveTenantCgroupV2", p)
		}
	}
}
