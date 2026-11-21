package gateway

import (
	"strings"
	"sync/atomic"
	"unicode"
)

type RouteTarget int

const (
	RouteTargetReadEngine RouteTarget = iota
	RouteTargetWriteEngine
)

type RouteReason int

const (
	RouteReasonReadOnlySelect RouteReason = iota
	RouteReasonReadOnlyUtility
	RouteReasonWriteDML
	RouteReasonWriteDDL
	RouteReasonModifyingCTE
	RouteReasonRowLocking
	RouteReasonSelectInto
	RouteReasonMutatingFunction
	RouteReasonTransactionWriteState
	RouteReasonSessionPinned
	RouteReasonReadEngineUnavailableFallback
	RouteReasonCausalLagFallback
	RouteReasonReadErrorFallback
)

type RouteDecision struct {
	Target RouteTarget
	Reason RouteReason
}

type SessionRouteContext struct {
	InTransaction     bool
	TxHasWrites       bool
	SessionPinned     bool
	ReadEngineHealthy bool
	SessionWriteLSN   uint64
	ReadEngineLSN     uint64
}

type RoutingMetrics struct {
	ReadRouted        atomic.Uint64
	WriteRouted       atomic.Uint64
	FallbacksToWrite  atomic.Uint64
	ReadErrorsRetried atomic.Uint64
}

type tokKind int

const (
	tokWord tokKind = iota
	tokSymbol
)

type sqlToken struct {
	kind  tokKind
	upper string
}

func isIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '$'
}

func isIdentChar(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

func tokenizeStatements(sql string) [][]sqlToken {
	stmts := [][]sqlToken{{}}
	n := len(sql)
	i := 0

	for i < n {
		c := sql[i]

		if unicode.IsSpace(rune(c)) {
			i++
			continue
		}

		// Line comment -- ...
		if c == '-' && i+1 < n && sql[i+1] == '-' {
			i += 2
			for i < n && sql[i] != '\n' {
				i++
			}
			continue
		}

		// Block comment /* ... */ (supports nesting)
		if c == '/' && i+1 < n && sql[i+1] == '*' {
			i += 2
			depth := 1
			for i < n && depth > 0 {
				if sql[i] == '/' && i+1 < n && sql[i+1] == '*' {
					depth++
					i += 2
				} else if sql[i] == '*' && i+1 < n && sql[i+1] == '/' {
					depth--
					i += 2
				} else {
					i++
				}
			}
			continue
		}

		// Single-quoted string literal
		if c == '\'' {
			i++
			for i < n {
				if sql[i] == '\'' {
					if i+1 < n && sql[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			stmts[len(stmts)-1] = append(stmts[len(stmts)-1], sqlToken{kind: tokSymbol, upper: "'"})
			continue
		}

		// Double-quoted identifier
		if c == '"' {
			i++
			var ident strings.Builder
			for i < n {
				if sql[i] == '"' {
					if i+1 < n && sql[i+1] == '"' {
						ident.WriteByte('"')
						i += 2
						continue
					}
					i++
					break
				}
				ident.WriteByte(sql[i])
				i++
			}
			stmts[len(stmts)-1] = append(stmts[len(stmts)-1], sqlToken{kind: tokWord, upper: strings.ToUpper(ident.String())})
			continue
		}

		// Dollar-quoted string literal $tag$...$tag$
		if c == '$' {
			tagEnd := i + 1
			for tagEnd < n && ((sql[tagEnd] >= 'a' && sql[tagEnd] <= 'z') ||
				(sql[tagEnd] >= 'A' && sql[tagEnd] <= 'Z') ||
				(sql[tagEnd] >= '0' && sql[tagEnd] <= '9') ||
				sql[tagEnd] == '_') {
				tagEnd++
			}
			if tagEnd < n && sql[tagEnd] == '$' {
				tag := sql[i : tagEnd+1]
				if closeIdx := strings.Index(sql[tagEnd+1:], tag); closeIdx >= 0 {
					i = tagEnd + 1 + closeIdx + len(tag)
				} else {
					i = n
				}
				stmts[len(stmts)-1] = append(stmts[len(stmts)-1], sqlToken{kind: tokSymbol, upper: "$$"})
				continue
			}
		}

		if c == ';' {
			if len(stmts[len(stmts)-1]) > 0 {
				stmts = append(stmts, nil)
			}
			i++
			continue
		}

		if isIdentStart(c) {
			start := i
			i++
			for i < n && isIdentChar(sql[i]) {
				i++
			}
			stmts[len(stmts)-1] = append(stmts[len(stmts)-1], sqlToken{
				kind:  tokWord,
				upper: strings.ToUpper(sql[start:i]),
			})
			continue
		}

		stmts[len(stmts)-1] = append(stmts[len(stmts)-1], sqlToken{kind: tokSymbol, upper: string(c)})
		i++
	}

	if len(stmts) > 1 && len(stmts[len(stmts)-1]) == 0 {
		stmts = stmts[:len(stmts)-1]
	}
	return stmts
}

func isWriteKeyword(w string) bool {
	switch w {
	case "INSERT", "UPDATE", "DELETE", "MERGE":
		return true
	}
	return false
}

func isDDLOrWriteHead(w string) bool {
	if isWriteKeyword(w) {
		return true
	}
	switch w {
	case "CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE", "VACUUM",
		"ANALYZE", "CLUSTER", "REINDEX", "COMMENT", "REFRESH", "DO", "CALL",
		"LOCK", "CHECKPOINT", "IMPORT", "REASSIGN", "SECURITY", "NOTIFY",
		"LISTEN", "UNLISTEN":
		return true
	}
	return false
}

func isMutatingFunction(w string) bool {
	switch w {
	case "NEXTVAL", "SETVAL",
		"PG_ADVISORY_LOCK", "PG_ADVISORY_LOCK_SHARED",
		"PG_TRY_ADVISORY_LOCK", "PG_TRY_ADVISORY_LOCK_SHARED",
		"PG_ADVISORY_XACT_LOCK", "PG_ADVISORY_XACT_LOCK_SHARED",
		"PG_ADVISORY_UNLOCK", "PG_ADVISORY_UNLOCK_ALL",
		"LO_CREATE", "LO_IMPORT", "LO_UNLINK",
		"PG_RELOAD_CONF", "PG_ROTATE_LOGFILE",
		"PG_TERMINATE_BACKEND", "PG_CANCEL_BACKEND":
		return true
	}
	return false
}

func classifySingleStatement(toks []sqlToken) RouteDecision {
	if len(toks) == 0 {
		return RouteDecision{RouteTargetReadEngine, RouteReasonReadOnlyUtility}
	}

	head := toks[0].upper

	if isWriteKeyword(head) {
		return RouteDecision{RouteTargetWriteEngine, RouteReasonWriteDML}
	}
	if isDDLOrWriteHead(head) {
		return RouteDecision{RouteTargetWriteEngine, RouteReasonWriteDDL}
	}

	// COPY ... FROM -> WriteEngine; COPY (...) TO -> ReadEngine
	if head == "COPY" {
		parenDepth := 0
		for i := 1; i < len(toks); i++ {
			w := toks[i].upper
			switch w {
			case "(":
				parenDepth++
				continue
			case ")":
				if parenDepth > 0 {
					parenDepth--
				}
				continue
			}
			if parenDepth == 0 {
				if w == "FROM" {
					return RouteDecision{RouteTargetWriteEngine, RouteReasonWriteDML}
				}
				if w == "TO" {
					return RouteDecision{RouteTargetReadEngine, RouteReasonReadOnlyUtility}
				}
			} else {
				if isWriteKeyword(w) {
					return RouteDecision{RouteTargetWriteEngine, RouteReasonModifyingCTE}
				}
				if isMutatingFunction(w) && i+1 < len(toks) && toks[i+1].upper == "(" {
					return RouteDecision{RouteTargetWriteEngine, RouteReasonMutatingFunction}
				}
			}
		}
		return RouteDecision{RouteTargetWriteEngine, RouteReasonWriteDML}
	}

	// Transaction boundaries must execute on WriteEngine so multi-statement
	// transactions remain atomic and post-commit LSNs are captured accurately.
	if head == "BEGIN" || head == "START" || head == "COMMIT" || head == "END" || head == "ROLLBACK" {
		return RouteDecision{RouteTargetWriteEngine, RouteReasonTransactionWriteState}
	}

	switch head {
	case "SHOW", "DISCARD", "DEALLOCATE":
		return RouteDecision{RouteTargetReadEngine, RouteReasonReadOnlyUtility}
	case "SET", "RESET":
		for i := 1; i < len(toks); i++ {
			if toks[i].upper == "TRANSACTION_READ_ONLY" || toks[i].upper == "DEFAULT_TRANSACTION_READ_ONLY" {
				return RouteDecision{RouteTargetWriteEngine, RouteReasonSessionPinned}
			}
		}
		return RouteDecision{RouteTargetReadEngine, RouteReasonReadOnlyUtility}
	}

	for i := 0; i < len(toks); i++ {
		w := toks[i].upper
		if i > 0 && isWriteKeyword(w) {
			return RouteDecision{RouteTargetWriteEngine, RouteReasonModifyingCTE}
		}
		if w == "INTO" && head == "SELECT" {
			return RouteDecision{RouteTargetWriteEngine, RouteReasonSelectInto}
		}
		if w == "FOR" && i+1 < len(toks) {
			next := toks[i+1].upper
			if next == "UPDATE" || next == "SHARE" || next == "NO" || next == "KEY" {
				return RouteDecision{RouteTargetWriteEngine, RouteReasonRowLocking}
			}
		}
		if isMutatingFunction(w) && i+1 < len(toks) && toks[i+1].upper == "(" {
			return RouteDecision{RouteTargetWriteEngine, RouteReasonMutatingFunction}
		}
	}

	switch head {
	case "SELECT", "WITH", "VALUES", "TABLE", "EXPLAIN":
		return RouteDecision{RouteTargetReadEngine, RouteReasonReadOnlySelect}
	}

	return RouteDecision{RouteTargetWriteEngine, RouteReasonWriteDDL}
}

// ClassifySQLForRouting classifies a SQL string (single or multi-statement) for ReadEngine vs WriteEngine routing.
func ClassifySQLForRouting(sql string) RouteDecision {
	stmts := tokenizeStatements(sql)
	overall := RouteDecision{RouteTargetReadEngine, RouteReasonReadOnlySelect}
	for _, toks := range stmts {
		if len(toks) == 0 {
			continue
		}
		d := classifySingleStatement(toks)
		if d.Target == RouteTargetWriteEngine {
			return d
		}
		overall = d
	}
	return overall
}

// RouteQuery decides the final route for a query given session transaction/pinning/LSN state.
func RouteQuery(sql string, ctx SessionRouteContext, metrics *RoutingMetrics) RouteDecision {
	d := ClassifySQLForRouting(sql)
	if d.Target == RouteTargetReadEngine {
		if ctx.SessionPinned {
			d = RouteDecision{RouteTargetWriteEngine, RouteReasonSessionPinned}
		} else if ctx.InTransaction && ctx.TxHasWrites {
			d = RouteDecision{RouteTargetWriteEngine, RouteReasonTransactionWriteState}
		} else if !ctx.ReadEngineHealthy {
			d = RouteDecision{RouteTargetWriteEngine, RouteReasonReadEngineUnavailableFallback}
			if metrics != nil {
				metrics.FallbacksToWrite.Add(1)
			}
		} else if ctx.SessionWriteLSN > 0 && ctx.ReadEngineLSN < ctx.SessionWriteLSN {
			d = RouteDecision{RouteTargetWriteEngine, RouteReasonCausalLagFallback}
			if metrics != nil {
				metrics.FallbacksToWrite.Add(1)
			}
		}
	}

	if metrics != nil {
		if d.Target == RouteTargetReadEngine {
			metrics.ReadRouted.Add(1)
		} else {
			metrics.WriteRouted.Add(1)
		}
	}
	return d
}

// RecordReadFallback records a fallback from ReadEngine to WriteEngine after a transient/read-only error.
func RecordReadFallback(metrics *RoutingMetrics) {
	if metrics != nil {
		metrics.FallbacksToWrite.Add(1)
		metrics.ReadErrorsRetried.Add(1)
	}
}
