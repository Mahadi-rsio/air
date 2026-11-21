package gateway

import "regexp"

var (
	reSetStmt       = regexp.MustCompile(`(?is)^\s*SET\s+([A-Za-z_][A-Za-z0-9_\.]*)\s*(?:=|TO)\s*([\s\S]+?)\s*;?\s*$`)
	reResetStmt     = regexp.MustCompile(`(?i)^\s*RESET\s+([A-Za-z_][A-Za-z0-9_\.]*|ALL)\s*;?\s*$`)
	rePrepareStmt   = regexp.MustCompile(`(?is)^\s*PREPARE\s+([A-Za-z_][A-Za-z0-9_]*)\s*(\([^)]*\))?\s+AS\s+([\s\S]+?)\s*;?\s*$`)
	reDeallocStmt   = regexp.MustCompile(`(?i)^\s*DEALLOCATE\s+(?:PREPARE\s+)?(ALL|[A-Za-z_][A-Za-z0-9_]*)\s*;?\s*$`)
	reExecuteStmt   = regexp.MustCompile(`(?is)^(\s*EXECUTE\s+)([A-Za-z_][A-Za-z0-9_]*)(\s*(?:\([\s\S]*\))?\s*;?\s*)$`)
	reListenStmt    = regexp.MustCompile(`(?i)^\s*(LISTEN|UNLISTEN)\b`)
	reTempTableStmt = regexp.MustCompile(`(?i)^\s*CREATE\s+(TEMP|TEMPORARY)\s+TABLE\b`)
	reAdvLockStmt   = regexp.MustCompile(`(?i)pg_advisory_lock`)
)

// IsSessionPinningSQL returns true if sql creates session-scoped state (LISTEN, TEMP TABLE, advisory locks)
// that requires pinning the backend connection to this client until disconnect.
func IsSessionPinningSQL(sql string) bool {
	return reListenStmt.MatchString(sql) ||
		reTempTableStmt.MatchString(sql) ||
		reAdvLockStmt.MatchString(sql)
}
