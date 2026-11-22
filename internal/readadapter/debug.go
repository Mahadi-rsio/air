package readadapter

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
)

var debugLoggingEnabled atomic.Bool

// SetDebugLogging enables or disables verbose read-engine debug logging.
func SetDebugLogging(enabled bool) {
	debugLoggingEnabled.Store(enabled)
}

// DebugLoggingEnabled reports whether verbose read-engine debug logging is active.
func DebugLoggingEnabled() bool {
	return debugLoggingEnabled.Load()
}

func debugf(component, format string, args ...any) {
	if !debugLoggingEnabled.Load() {
		return
	}
	log.Printf("[air-read-engine][DEBUG][%s] "+format, append([]any{component}, args...)...)
}

func formatLSNHex(lsn uint64) string {
	if lsn == ^uint64(0) {
		return "MAX"
	}
	return fmt.Sprintf("%X/%08X", uint32(lsn>>32), uint32(lsn))
}

func truncateSQL(sql string, maxLen int) string {
	s := strings.Join(strings.Fields(strings.TrimSpace(sql)), " ")
	if maxLen > 0 && len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}
