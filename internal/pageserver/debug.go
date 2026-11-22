package pageserver

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
)

var debugLoggingEnabled atomic.Bool

// SetDebugLogging enables or disables verbose pageserver debug logging.
func SetDebugLogging(enabled bool) {
	debugLoggingEnabled.Store(enabled)
}

// DebugLoggingEnabled reports whether verbose pageserver debug logging is active.
func DebugLoggingEnabled() bool {
	return debugLoggingEnabled.Load()
}

func debugf(component, format string, args ...any) {
	if !debugLoggingEnabled.Load() {
		return
	}
	log.Printf("[air-pageserver][DEBUG][%s] "+format, append([]any{component}, args...)...)
}

func formatLSNHex(lsn uint64) string {
	return fmt.Sprintf("%X/%08X", uint32(lsn>>32), uint32(lsn))
}

func shortHash(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

func rmidName(rmid uint8) string {
	switch rmid {
	case 0:
		return "XLOG"
	case 1:
		return "Transaction"
	case 2:
		return "Storage"
	case 3:
		return "CLOG"
	case 4:
		return "Database"
	case 5:
		return "Tablespace"
	case 6:
		return "MultiXact"
	case 7:
		return "RelMap"
	case 8:
		return "Standby"
	case 9:
		return "Heap2"
	case 10:
		return "Heap"
	case 11:
		return "Btree"
	case 12:
		return "Hash"
	case 13:
		return "Gin"
	case 14:
		return "Gist"
	case 15:
		return "Sequence"
	case 16:
		return "SPGist"
	case 17:
		return "BRIN"
	case 18:
		return "CommitTs"
	case 19:
		return "ReplOrigin"
	case 20:
		return "Generic"
	case 21:
		return "LogicalMsg"
	default:
		return fmt.Sprintf("RMID(%d)", rmid)
	}
}

func summarizeRMIDs(recs []*DecodedWALRecord) string {
	if len(recs) == 0 {
		return "none"
	}
	counts := make(map[uint8]int, 8)
	var order []uint8
	for _, r := range recs {
		if _, ok := counts[r.Rmid]; !ok {
			order = append(order, r.Rmid)
		}
		counts[r.Rmid]++
	}
	var b strings.Builder
	for i, rmid := range order {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s:%d", rmidName(rmid), counts[rmid])
	}
	return b.String()
}
