package readadapter

import (
	"strconv"
	"strings"
)

// replaceDollarParamsTokenAware replaces PostgreSQL positional parameters ($1, $2, ...)
// with SQLite positional parameters (?1, ?2, ...) while preserving string literals,
// comments, and quoted identifiers.
func replaceDollarParamsTokenAware(sql string) (string, int) {
	toks := tokenizeSQLForRewrite(sql)
	maxParam := 0
	var b strings.Builder
	b.Grow(len(sql))
	for _, t := range toks {
		if t.kind == rwWord && len(t.raw) >= 2 && t.raw[0] == '$' {
			if n, err := strconv.Atoi(t.raw[1:]); err == nil && n > 0 {
				if n > maxParam {
					maxParam = n
				}
				b.WriteByte('?')
				b.WriteString(t.raw[1:])
				continue
			}
		}
		b.WriteString(t.raw)
	}
	return b.String(), maxParam
}

// mapPGTypeToSQLiteCast maps a PostgreSQL type name to a SQLite cast expression
// or helper function name.
func mapPGTypeToSQLiteCast(pgType string) (sqliteType string, helperFn string) {
	u := strings.ToUpper(strings.TrimSpace(pgType))
	switch u {
	case "INT", "INT2", "INT4", "INT8", "SMALLINT", "INTEGER", "BIGINT", "TINYINT", "MEDIUMINT":
		return "INTEGER", ""
	case "REAL", "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION":
		return "REAL", ""
	case "TEXT", "VARCHAR", "CHAR", "BPCHAR", "CHARACTER", "CHARACTER VARYING", "NVARCHAR",
		"UUID", "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ",
		"TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITHOUT TIME ZONE",
		"JSON", "JSONB":
		return "TEXT", ""
	case "BYTEA", "BLOB":
		return "BLOB", ""
	case "BOOL", "BOOLEAN":
		return "", "air_pg_cast_bool"
	case "NUMERIC", "DECIMAL":
		return "", "air_pg_cast_numeric"
	default:
		// Unknown or complex type: preserve as TEXT cast rather than silently stripping.
		return "TEXT", ""
	}
}

func isPGTypeBoundary(u string) bool {
	switch u {
	case "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET",
		"UNION", "INTERSECT", "EXCEPT", "AS", "ON", "AND", "OR", "IN",
		"IS", "NOT", "THEN", "ELSE", "END", "WHEN", "JOIN", "LEFT", "RIGHT",
		"INNER", "OUTER", "CROSS", "FULL", "INTO", "FOR", "ASC", "DESC", "NULLS",
		"OVER", "FILTER", "BETWEEN", "LIKE", "ILIKE":
		return true
	}
	return false
}

// parsePGTypeSpec parses a PostgreSQL type name starting at idx in toks,
// returning the normalized type name and the index of the last consumed token.
func parsePGTypeSpec(toks []rwTok, idx int) (string, int, bool) {
	if idx < 0 || idx >= len(toks) || toks[idx].kind != rwWord {
		return "", idx, false
	}
	var parts []string
	cur := idx
	lastConsumed := idx
	for cur < len(toks) {
		if toks[cur].kind == rwSpace || toks[cur].kind == rwComment {
			cur++
			continue
		}
		if toks[cur].kind != rwWord || isPGTypeBoundary(toks[cur].upper) {
			break
		}
		u := toks[cur].upper
		parts = append(parts, u)
		lastConsumed = cur
		cur++
		// Multi-word types: DOUBLE PRECISION, CHARACTER VARYING, TIMESTAMP WITH/WITHOUT TIME ZONE
		if u != "DOUBLE" && u != "CHARACTER" && u != "TIMESTAMP" && u != "TIME" && u != "WITH" && u != "WITHOUT" {
			break
		}
	}
	if len(parts) == 0 {
		return "", idx, false
	}
	// Skip optional precision/scale like (10, 2)
	ns := nextSigTok(toks, lastConsumed+1)
	if ns >= 0 && toks[ns].raw == "(" {
		depth := 1
		j := ns + 1
		for j < len(toks) && depth > 0 {
			if toks[j].raw == "(" {
				depth++
			} else if toks[j].raw == ")" {
				depth--
			}
			j++
		}
		if depth == 0 {
			lastConsumed = j - 1
		}
	}
	// Skip optional [] array suffix
	ns2 := nextSigTok(toks, lastConsumed+1)
	if ns2 >= 0 && toks[ns2].raw == "[" {
		ns3 := nextSigTok(toks, ns2+1)
		if ns3 >= 0 && toks[ns3].raw == "]" {
			lastConsumed = ns3
		}
	}
	return strings.Join(parts, " "), lastConsumed, true
}

// rewritePGCasts replaces PostgreSQL `expr::type` and `CAST(expr AS pg_type)`
// with semantic-preserving SQLite CAST or helper calls, without touching string literals.
func rewritePGCasts(sql string) string {
	toks := tokenizeSQLForRewrite(sql)
	var out []rwTok

	for i := 0; i < len(toks); i++ {
		// Handle :: operator (tokenized as two ':' rwOp tokens)
		if toks[i].kind == rwOp && toks[i].raw == ":" &&
			i+1 < len(toks) && toks[i+1].kind == rwOp && toks[i+1].raw == ":" {
			typeStart := nextSigTok(toks, i+2)
			if pgType, lastIdx, ok := parsePGTypeSpec(toks, typeStart); ok {
				if lhsStart, okLHS := prevPrimaryBounds(out); okLHS {
					lhs := strings.TrimSpace(joinTokens(out[lhsStart:]))
					out = out[:lhsStart]
					sqliteType, helperFn := mapPGTypeToSQLiteCast(pgType)
					var expr string
					if helperFn != "" {
						expr = helperFn + "(" + lhs + ")"
					} else {
						expr = "CAST(" + lhs + " AS " + sqliteType + ")"
					}
					out = append(out, rwTok{kind: rwWord, raw: expr, upper: strings.ToUpper(expr)})
					i = lastIdx
					continue
				}
			}
		}

		// Handle CAST(expr AS pg_type)
		if toks[i].kind == rwWord && toks[i].upper == "CAST" {
			lp := nextSigTok(toks, i+1)
			if lp >= 0 && toks[lp].raw == "(" {
				depth := 1
				asIdx := -1
				rp := -1
				for j := lp + 1; j < len(toks); j++ {
					if toks[j].raw == "(" {
						depth++
					} else if toks[j].raw == ")" {
						depth--
						if depth == 0 {
							rp = j
							break
						}
					} else if depth == 1 && toks[j].kind == rwWord && toks[j].upper == "AS" {
						asIdx = j
					}
				}
				if rp > 0 && asIdx > lp {
					innerExpr := strings.TrimSpace(joinTokens(toks[lp+1 : asIdx]))
					// Recursively rewrite nested casts inside innerExpr
					innerExpr = rewritePGCasts(innerExpr)
					typeStart := nextSigTok(toks, asIdx+1)
					if pgType, _, ok := parsePGTypeSpec(toks, typeStart); ok {
						sqliteType, helperFn := mapPGTypeToSQLiteCast(pgType)
						var expr string
						if helperFn != "" {
							expr = helperFn + "(" + innerExpr + ")"
						} else {
							expr = "CAST(" + innerExpr + " AS " + sqliteType + ")"
						}
						out = append(out, rwTok{kind: rwWord, raw: expr, upper: strings.ToUpper(expr)})
						i = rp
						continue
					}
				}
			}
		}

		out = append(out, toks[i])
	}

	return joinTokens(out)
}

// rewritePGSpecialSyntax rewrites PostgreSQL EXTRACT(field FROM expr),
// POSITION(sub IN str), and SUBSTRING(s FROM start [FOR len]) into SQLite-compatible calls.
func rewritePGSpecialSyntax(sql string) string {
	toks := tokenizeSQLForRewrite(sql)
	var out []rwTok

	for i := 0; i < len(toks); i++ {
		if toks[i].kind == rwWord && (toks[i].upper == "EXTRACT" || toks[i].upper == "POSITION" || toks[i].upper == "SUBSTRING") {
			lp := nextSigTok(toks, i+1)
			if lp >= 0 && toks[lp].raw == "(" {
				depth := 1
				rp := -1
				fromIdx := -1
				inIdx := -1
				forIdx := -1
				for j := lp + 1; j < len(toks); j++ {
					if toks[j].raw == "(" {
						depth++
					} else if toks[j].raw == ")" {
						depth--
						if depth == 0 {
							rp = j
							break
						}
					} else if depth == 1 && toks[j].kind == rwWord {
						switch toks[j].upper {
						case "FROM":
							if fromIdx < 0 {
								fromIdx = j
							}
						case "IN":
							if inIdx < 0 {
								inIdx = j
							}
						case "FOR":
							if forIdx < 0 {
								forIdx = j
							}
						}
					}
				}
				if rp > 0 {
					switch toks[i].upper {
					case "EXTRACT":
						if fromIdx > lp {
							field := strings.TrimSpace(joinTokens(toks[lp+1 : fromIdx]))
							field = strings.Trim(field, "'\"")
							expr := strings.TrimSpace(joinTokens(toks[fromIdx+1 : rp]))
							call := "air_pg_date_part('" + strings.ToLower(field) + "', " + expr + ")"
							out = append(out, rwTok{kind: rwWord, raw: call, upper: strings.ToUpper(call)})
							i = rp
							continue
						}
					case "POSITION":
						if inIdx > lp {
							sub := strings.TrimSpace(joinTokens(toks[lp+1 : inIdx]))
							str := strings.TrimSpace(joinTokens(toks[inIdx+1 : rp]))
							call := "INSTR(" + str + ", " + sub + ")"
							out = append(out, rwTok{kind: rwWord, raw: call, upper: strings.ToUpper(call)})
							i = rp
							continue
						}
					case "SUBSTRING":
						if fromIdx > lp {
							str := strings.TrimSpace(joinTokens(toks[lp+1 : fromIdx]))
							var startExpr, lenExpr string
							if forIdx > fromIdx {
								startExpr = strings.TrimSpace(joinTokens(toks[fromIdx+1 : forIdx]))
								lenExpr = strings.TrimSpace(joinTokens(toks[forIdx+1 : rp]))
								call := "SUBSTR(" + str + ", " + startExpr + ", " + lenExpr + ")"
								out = append(out, rwTok{kind: rwWord, raw: call, upper: strings.ToUpper(call)})
							} else {
								startExpr = strings.TrimSpace(joinTokens(toks[fromIdx+1 : rp]))
								call := "SUBSTR(" + str + ", " + startExpr + ")"
								out = append(out, rwTok{kind: rwWord, raw: call, upper: strings.ToUpper(call)})
							}
							i = rp
							continue
						}
					}
				}
			}
		}
		out = append(out, toks[i])
	}
	return joinTokens(out)
}

func isOrderByTerminator(u string) bool {
	switch u {
	case "LIMIT", "OFFSET", "FETCH", "UNION", "INTERSECT", "EXCEPT", "FOR", "WINDOW", "ROWS", "RANGE", "GROUPS":
		return true
	}
	return false
}

// rewriteOrderByNulls appends PostgreSQL default NULL ordering (ASC -> NULLS LAST,
// DESC -> NULLS FIRST) to ORDER BY items that do not explicitly specify NULLS FIRST/LAST.
func rewriteOrderByNulls(sql string) string {
	toks := tokenizeSQLForRewrite(sql)
	var out []rwTok

	for i := 0; i < len(toks); i++ {
		out = append(out, toks[i])
		if toks[i].kind == rwWord && toks[i].upper == "ORDER" {
			byIdx := nextSigTok(toks, i+1)
			if byIdx >= 0 && toks[byIdx].kind == rwWord && toks[byIdx].upper == "BY" {
				// Emit everything up to BY
				for k := i + 1; k <= byIdx; k++ {
					out = append(out, toks[k])
				}
				// Process ORDER BY items at current paren depth
				cur := byIdx + 1
				depth := 0
				hasDir := false
				isDesc := false
				hasNulls := false

				flushNullsIfNeeded := func() {
					// Trim trailing whitespace/comments in out before inserting NULLS FIRST/LAST
					if !hasNulls {
						if isDesc {
							out = append(out, rwTok{kind: rwSpace, raw: " "}, rwTok{kind: rwWord, raw: "NULLS FIRST", upper: "NULLS FIRST"})
						} else if hasDir || true {
							out = append(out, rwTok{kind: rwSpace, raw: " "}, rwTok{kind: rwWord, raw: "NULLS LAST", upper: "NULLS LAST"})
						}
					}
					hasDir = false
					isDesc = false
					hasNulls = false
				}

				// Helper to check if we have seen at least one significant token in current item
				itemHasTok := false
				for cur < len(toks) {
					t := toks[cur]
					if t.raw == "(" {
						depth++
						itemHasTok = true
						out = append(out, t)
						cur++
						continue
					}
					if t.raw == ")" {
						if depth == 0 {
							if itemHasTok {
								flushNullsIfNeeded()
							}
							break
						}
						depth--
						itemHasTok = true
						out = append(out, t)
						cur++
						continue
					}
					if depth == 0 {
						if t.raw == ";" || (t.kind == rwWord && isOrderByTerminator(t.upper)) {
							if itemHasTok {
								flushNullsIfNeeded()
							}
							break
						}
						if t.raw == "," {
							if itemHasTok {
								flushNullsIfNeeded()
							}
							itemHasTok = false
							out = append(out, t)
							cur++
							continue
						}
						if t.kind == rwWord {
							switch t.upper {
							case "ASC":
								hasDir = true
								isDesc = false
							case "DESC":
								hasDir = true
								isDesc = true
							case "NULLS":
								hasNulls = true
							}
						}
					}
					if t.kind != rwSpace && t.kind != rwComment {
						itemHasTok = true
					}
					out = append(out, t)
					cur++
				}
				if cur == len(toks) && itemHasTok {
					flushNullsIfNeeded()
				}
				i = cur - 1
			}
		}
	}
	return joinTokens(out)
}
