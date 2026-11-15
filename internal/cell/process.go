package cell

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func readProcStartTime(pid int) (string, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	s := string(raw)
	idx := strings.LastIndex(s, ")")
	if idx < 0 || idx+2 >= len(s) {
		return "", fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	fields := strings.Fields(s[idx+2:])
	// Field 22 in /proc/<pid>/stat is index 19 after comm (field 2) + ')'
	if len(fields) <= 19 {
		return "", fmt.Errorf("short /proc/%d/stat", pid)
	}
	return fields[19], nil
}

func verifyPostgresProcessIdentity(pid int, expectedPGData string) error {
	if pid <= 1 {
		return fmt.Errorf("invalid pid %d", pid)
	}
	if err := syscall.Kill(pid, 0); err != nil {
		return fmt.Errorf("pid %d not alive: %w", pid, err)
	}
	exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return fmt.Errorf("cannot read exe for pid %d: %w", pid, err)
	}
	baseExe := filepath.Base(strings.TrimSuffix(exe, " (deleted)"))
	if baseExe != "postgres" && baseExe != "postmaster" {
		return fmt.Errorf("pid %d exe %q is not postgres", pid, exe)
	}
	if expectedPGData != "" {
		cmdRaw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil {
			return fmt.Errorf("cannot read cmdline for pid %d: %w", pid, err)
		}
		cmd := strings.ReplaceAll(string(cmdRaw), "\x00", " ")
		if !strings.Contains(cmd, expectedPGData) {
			// Also check /proc/<pid>/cwd as fallback when postgres was started with relative args
			cwd, cwdErr := os.Readlink(fmt.Sprintf("/proc/%d/cwd", pid))
			if cwdErr != nil || cwd != expectedPGData {
				return fmt.Errorf("pid %d does not own pgdata %s", pid, expectedPGData)
			}
		}
	}
	return nil
}

func readPostmasterDetails(pgData string) (pid int, port int, pgDataDir string) {
	raw, err := os.ReadFile(filepath.Join(pgData, "postmaster.pid"))
	if err != nil {
		return 0, 0, ""
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) >= 1 {
		pid, _ = strconv.Atoi(strings.TrimSpace(lines[0]))
	}
	if len(lines) >= 2 {
		pgDataDir = strings.TrimSpace(lines[1])
	}
	if len(lines) >= 4 {
		port, _ = strconv.Atoi(strings.TrimSpace(lines[3]))
	}
	return pid, port, pgDataDir
}

func waitProcessGone(pid int, timeout time.Duration) bool {
	if pid <= 1 {
		return true
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); err != nil {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return syscall.Kill(pid, 0) != nil
}
