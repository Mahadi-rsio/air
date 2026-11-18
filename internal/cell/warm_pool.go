package cell

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const defaultWarmPoolSize = 2

func parseWarmPoolSize() int {
	raw := strings.TrimSpace(os.Getenv("AIR_WARM_POOL_SIZE"))
	if raw == "" {
		return defaultWarmPoolSize
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return defaultWarmPoolSize
	}
	return n
}

func (a *Agent) initWarmPool() {
	target := parseWarmPoolSize()
	a.warmPoolMu.Lock()
	a.warmPoolTarget = target
	a.warmPoolNotify = make(chan struct{}, 1)
	a.warmPoolMu.Unlock()

	if target <= 0 {
		return
	}
	// Only start background initdb workers if the initdb binary actually exists.
	initdbBin := filepath.Join(a.pgBin, "initdb")
	if _, err := os.Stat(initdbBin); err != nil {
		return
	}

	poolDir := filepath.Join(a.volDir, "warm_pool")
	_ = os.RemoveAll(poolDir)
	if err := os.MkdirAll(poolDir, 0o755); err != nil {
		return
	}

	a.warmPoolWG.Add(1)
	go a.warmPoolLoop(poolDir, initdbBin)
	select {
	case a.warmPoolNotify <- struct{}{}:
	default:
	}
}

func (a *Agent) warmPoolLoop(poolDir, initdbBin string) {
	defer a.warmPoolWG.Done()
	for {
		a.warmPoolMu.Lock()
		need := len(a.warmPoolSlots) < a.warmPoolTarget
		a.warmPoolMu.Unlock()

		if need {
			select {
			case <-a.stopCh:
				return
			default:
			}
			seq := a.warmPoolSeq.Add(1)
			slotDir := filepath.Join(poolDir, fmt.Sprintf("slot_%d", seq))
			pgData := filepath.Join(slotDir, "pgdata")
			if err := os.MkdirAll(pgData, 0o700); err == nil {
				if _, err := a.runPostgresCmd(
					initdbBin,
					"-D", pgData, "-U", "postgres",
					"--auth-local=trust", "--auth-host=scram-sha-256",
					"--data-checksums", "--no-instructions",
				); err == nil {
					select {
					case <-a.stopCh:
						_ = os.RemoveAll(slotDir)
						return
					default:
					}
					a.warmPoolMu.Lock()
					a.warmPoolSlots = append(a.warmPoolSlots, slotDir)
					a.warmPoolMu.Unlock()
					continue
				}
			}
			_ = os.RemoveAll(slotDir)
		}

		select {
		case <-a.stopCh:
			return
		case <-a.warmPoolNotify:
		}
	}
}

// claimWarmPGData moves a pre-initialized PGDATA directory from the warm pool to dstPGData.
// Returns true on hit; returns false if the warm pool is disabled or currently empty.
func (a *Agent) claimWarmPGData(dstPGData string) bool {
	a.warmPoolMu.Lock()
	if len(a.warmPoolSlots) == 0 {
		a.warmPoolMu.Unlock()
		return false
	}
	slotDir := a.warmPoolSlots[0]
	a.warmPoolSlots = a.warmPoolSlots[1:]
	notifyCh := a.warmPoolNotify
	a.warmPoolMu.Unlock()

	if notifyCh != nil {
		select {
		case notifyCh <- struct{}{}:
		default:
		}
	}

	srcPGData := filepath.Join(slotDir, "pgdata")
	if err := os.MkdirAll(filepath.Dir(dstPGData), 0o700); err != nil {
		_ = os.RemoveAll(slotDir)
		return false
	}
	if err := os.Rename(srcPGData, dstPGData); err != nil {
		_ = os.RemoveAll(slotDir)
		return false
	}
	_ = os.RemoveAll(slotDir)
	return true
}
