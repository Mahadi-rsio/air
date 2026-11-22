package readadapter

import (
	"strings"
	"unicode"
)

type rwTokKind int

const (
	rwWord rwTokKind = iota
	rwStr
	rwOp
	rwSpace
	rwComment
)

type rwTok struct {
	kind  rwTokKind
	raw   string
	upper string
}

func tokenizeSQLForRewrite(sql string) []rwTok {
	var toks []rwTok
	n := len(sql)
	i := 0
	for i < n {
		c := sql[i]
		if unicode.IsSpace(rune(c)) {
			start := i
			i++
			for i < n && unicode.IsSpace(rune(sql[i])) {
				i++
			}
			toks = append(toks, rwTok{kind: rwSpace, raw: sql[start:i]})
			continue
		}
		if c == '-' && i+1 < n && sql[i+1] == '-' {
			start := i
			i += 2
			for i < n && sql[i] != '\n' {
				i++
			}
			toks = append(toks, rwTok{kind: rwComment, raw: sql[start:i]})
			continue
		}
		if c == '/' && i+1 < n && sql[i+1] == '*' {
			start := i
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
			toks = append(toks, rwTok{kind: rwComment, raw: sql[start:i]})
			continue
		}
		if c == '\'' {
			start := i
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
			raw := sql[start:i]
			toks = append(toks, rwTok{kind: rwStr, raw: raw, upper: raw})
			continue
		}
		if c == '"' {
			start := i
			i++
			for i < n {
				if sql[i] == '"' {
					if i+1 < n && sql[i+1] == '"' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			raw := sql[start:i]
			toks = append(toks, rwTok{kind: rwWord, raw: raw, upper: strings.ToUpper(raw)})
			continue
		}
		// Multi-char regex operators: !~*, !~, ~*
		if c == '!' && i+2 < n && sql[i+1] == '~' && sql[i+2] == '*' {
			toks = append(toks, rwTok{kind: rwOp, raw: "!~*", upper: "!~*"})
			i += 3
			continue
		}
		if c == '!' && i+1 < n && sql[i+1] == '~' {
			toks = append(toks, rwTok{kind: rwOp, raw: "!~", upper: "!~"})
			i += 2
			continue
		}
		if c == '~' && i+1 < n && sql[i+1] == '*' {
			toks = append(toks, rwTok{kind: rwOp, raw: "~*", upper: "~*"})
			i += 2
			continue
		}
		if c == '~' {
			toks = append(toks, rwTok{kind: rwOp, raw: "~", upper: "~"})
			i++
			continue
		}
		// Placeholders ?1, $1, identifiers, numbers
		if isSQLIdentStart(c) || (c >= '0' && c <= '9') || c == '?' || c == '$' {
			start := i
			i++
			for i < n && (isSQLIdentChar(sql[i]) || sql[i] == '.') {
				i++
			}
			raw := sql[start:i]
			toks = append(toks, rwTok{kind: rwWord, raw: raw, upper: strings.ToUpper(raw)})
			continue
		}
		toks = append(toks, rwTok{kind: rwOp, raw: sql[i : i+1], upper: sql[i : i+1]})
		i++
	}
	return toks
}

func isSQLIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isSQLIdentChar(c byte) bool {
	return isSQLIdentStart(c) || (c >= '0' && c <= '9') || c == '$'
}

func joinTokens(toks []rwTok) string {
	var b strings.Builder
	for _, t := range toks {
		b.WriteString(t.raw)
	}
	return b.String()
}

// skipNonSpaceIdx finds the next non-space/non-comment token index >= start.
func nextSigTok(toks []rwTok, start int) int {
	for i := start; i < len(toks); i++ {
		if toks[i].kind != rwSpace && toks[i].kind != rwComment {
			return i
		}
	}
	return -1
}

// prevPrimaryBounds finds the start index in outToks of the primary expression immediately preceding end.
func prevPrimaryBounds(out []rwTok) (int, bool) {
	i := len(out) - 1
	for i >= 0 && (out[i].kind == rwSpace || out[i].kind == rwComment) {
		i--
	}
	if i < 0 {
		return 0, false
	}
	if out[i].raw == ")" {
		depth := 1
		j := i - 1
		for j >= 0 && depth > 0 {
			if out[j].raw == ")" {
				depth++
			} else if out[j].raw == "(" {
				depth--
			}
			j--
		}
		if depth != 0 {
			return 0, false
		}
		// Check if preceded by a function name
		k := j
		for k >= 0 && (out[k].kind == rwSpace || out[k].kind == rwComment) {
			k--
		}
		if k >= 0 && out[k].kind == rwWord && !isSQLClauseKeyword(out[k].upper) {
			return k, true
		}
		return j + 1, true
	}
	if out[i].kind == rwWord || out[i].kind == rwStr {
		// Handle dotted chains like t.col
		start := i
		for start >= 2 && out[start-1].raw == "." && out[start-2].kind == rwWord {
			start -= 2
		}
		return start, true
	}
	return 0, false
}

// nextPrimaryBounds finds the inclusive end index in toks of the primary expression starting at start.
func nextPrimaryBounds(toks []rwTok, start int) (int, int, bool) {
	p := nextSigTok(toks, start)
	if p < 0 {
		return 0, 0, false
	}
	if toks[p].raw == "(" {
		depth := 1
		j := p + 1
		for j < len(toks) && depth > 0 {
			if toks[j].raw == "(" {
				depth++
			} else if toks[j].raw == ")" {
				depth--
			}
			j++
		}
		if depth != 0 {
			return 0, 0, false
		}
		return p, j - 1, true
	}
	if toks[p].kind == rwWord || toks[p].kind == rwStr {
		end := p
		// Handle dotted chain or function call
		for end+2 < len(toks) && toks[end+1].raw == "." && toks[end+2].kind == rwWord {
			end += 2
		}
		ns := nextSigTok(toks, end+1)
		if ns >= 0 && toks[ns].raw == "(" && toks[p].kind == rwWord {
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
			if depth != 0 {
				return 0, 0, false
			}
			return p, j - 1, true
		}
		return p, end, true
	}
	return 0, 0, false
}

func isSQLClauseKeyword(u string) bool {
	switch u {
	case "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "ON", "WHEN", "THEN", "ELSE", "CASE", "HAVING", "GROUP", "ORDER", "BY", "LIMIT", "OFFSET", "AS":
		return true
	}
	return false
}

// rewriteILikeAndRegex rewrites PostgreSQL ILIKE, NOT ILIKE, ~, ~*, !~, !~*
// into calls to AIR's registered SQLite functions air_pg_ilike and air_pg_regex.
func rewriteILikeAndRegex(sql string) string {
	if !containsASCIIFold(sql, "ilike") && !strings.Contains(sql, "~") {
		return sql
	}
	toks := tokenizeSQLForRewrite(sql)
	var out []rwTok
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		// Check NOT ILIKE
		if t.kind == rwWord && t.upper == "NOT" {
			ns := nextSigTok(toks, i+1)
			if ns >= 0 && toks[ns].kind == rwWord && toks[ns].upper == "ILIKE" {
				if lhsStart, ok := prevPrimaryBounds(out); ok {
					if rhsStart, rhsEnd, ok2 := nextPrimaryBounds(toks, ns+1); ok2 {
						lhs := strings.TrimSpace(joinTokens(out[lhsStart:]))
						rhs := strings.TrimSpace(joinTokens(toks[rhsStart : rhsEnd+1]))
						out = out[:lhsStart]
						nextIdx := rhsEnd
						call := "NOT air_pg_ilike(" + lhs + ", " + rhs
						if escPos := nextSigTok(toks, rhsEnd+1); escPos >= 0 && toks[escPos].upper == "ESCAPE" {
							if eStart, eEnd, ok3 := nextPrimaryBounds(toks, escPos+1); ok3 {
								esc := strings.TrimSpace(joinTokens(toks[eStart : eEnd+1]))
								call += ", " + esc
								nextIdx = eEnd
							}
						}
						call += ")"
						out = append(out, rwTok{kind: rwWord, raw: call, upper: strings.ToUpper(call)})
						i = nextIdx
						continue
					}
				}
			}
		}
		// Check ILIKE
		if t.kind == rwWord && t.upper == "ILIKE" {
			if lhsStart, ok := prevPrimaryBounds(out); ok {
				if rhsStart, rhsEnd, ok2 := nextPrimaryBounds(toks, i+1); ok2 {
					lhs := strings.TrimSpace(joinTokens(out[lhsStart:]))
					rhs := strings.TrimSpace(joinTokens(toks[rhsStart : rhsEnd+1]))
					out = out[:lhsStart]
					nextIdx := rhsEnd
					call := "air_pg_ilike(" + lhs + ", " + rhs
					if escPos := nextSigTok(toks, rhsEnd+1); escPos >= 0 && toks[escPos].upper == "ESCAPE" {
						if eStart, eEnd, ok3 := nextPrimaryBounds(toks, escPos+1); ok3 {
							esc := strings.TrimSpace(joinTokens(toks[eStart : eEnd+1]))
							call += ", " + esc
							nextIdx = eEnd
						}
					}
					call += ")"
					out = append(out, rwTok{kind: rwWord, raw: call, upper: strings.ToUpper(call)})
					i = nextIdx
					continue
				}
			}
		}
		// Check regex operators: ~, ~*, !~, !~*
		if t.kind == rwOp && (t.raw == "~" || t.raw == "~*" || t.raw == "!~" || t.raw == "!~*") {
			if lhsStart, ok := prevPrimaryBounds(out); ok {
				if rhsStart, rhsEnd, ok2 := nextPrimaryBounds(toks, i+1); ok2 {
					lhs := strings.TrimSpace(joinTokens(out[lhsStart:]))
					rhs := strings.TrimSpace(joinTokens(toks[rhsStart : rhsEnd+1]))
					out = out[:lhsStart]
					neg := strings.HasPrefix(t.raw, "!")
					flag := "''"
					if strings.HasSuffix(t.raw, "*") {
						flag = "'i'"
					}
					call := "air_pg_regex(" + lhs + ", " + rhs + ", " + flag + ")"
					if neg {
						call = "NOT " + call
					}
					out = append(out, rwTok{kind: rwWord, raw: call, upper: strings.ToUpper(call)})
					i = rhsEnd
					continue
				}
			}
		}
		out = append(out, t)
	}
	return joinTokens(out)
}

// rewriteDistinctOn rewrites single-block SELECT DISTINCT ON (k1, ...) ... ORDER BY k1, ...
// to ROW_NUMBER() OVER (PARTITION BY k1, ... ORDER BY ...) = 1 when semantics are provably identical.
func rewriteDistinctOn(sql string) string {
	if !containsASCIIFold(sql, "distinct on") {
		return sql
	}
	// Reject complex multi-block / aggregated / window / locking queries conservatively.
	for _, kw := range []string{" group by ", " having ", " window ", " over ", " over(", " union ", " intersect ", " except ", " for update", " for share"} {
		if containsASCIIFold(sql, kw) {
			return sql
		}
	}

	toks := tokenizeSQLForRewrite(sql)
	// Find top-level SELECT DISTINCT ON ( ... )
	selIdx := nextSigTok(toks, 0)
	if selIdx < 0 || toks[selIdx].upper != "SELECT" {
		return sql
	}
	distIdx := nextSigTok(toks, selIdx+1)
	if distIdx < 0 || toks[distIdx].upper != "DISTINCT" {
		return sql
	}
	onIdx := nextSigTok(toks, distIdx+1)
	if onIdx < 0 || toks[onIdx].upper != "ON" {
		return sql
	}
	lparenIdx := nextSigTok(toks, onIdx+1)
	if lparenIdx < 0 || toks[lparenIdx].raw != "(" {
		return sql
	}
	depth := 1
	rparenIdx := lparenIdx + 1
	for rparenIdx < len(toks) && depth > 0 {
		if toks[rparenIdx].raw == "(" {
			depth++
		} else if toks[rparenIdx].raw == ")" {
			depth--
		}
		if depth > 0 {
			rparenIdx++
		}
	}
	if depth != 0 {
		return sql
	}
	distinctKeysRaw := strings.TrimSpace(joinTokens(toks[lparenIdx+1 : rparenIdx]))
	if distinctKeysRaw == "" {
		return sql
	}

	// Scan top-level clauses: FROM, ORDER BY, LIMIT, OFFSET
	fromIdx := -1
	orderIdx := -1
	limitIdx := -1
	offsetIdx := -1
	depth = 0
	for i := rparenIdx + 1; i < len(toks); i++ {
		if toks[i].raw == "(" {
			depth++
			continue
		}
		if toks[i].raw == ")" {
			depth--
			continue
		}
		if depth != 0 || toks[i].kind != rwWord {
			continue
		}
		switch toks[i].upper {
		case "FROM":
			if fromIdx < 0 {
				fromIdx = i
			}
		case "ORDER":
			if ns := nextSigTok(toks, i+1); ns >= 0 && toks[ns].upper == "BY" && orderIdx < 0 {
				orderIdx = i
			}
		case "LIMIT":
			if limitIdx < 0 {
				limitIdx = i
			}
		case "OFFSET":
			if offsetIdx < 0 {
				offsetIdx = i
			}
		}
	}

	// Require FROM and ORDER BY so row selection is deterministic and matches PG.
	if fromIdx < 0 || orderIdx < 0 || orderIdx < fromIdx {
		return sql
	}

	selectListRaw := strings.TrimSpace(joinTokens(toks[rparenIdx+1 : fromIdx]))
	outerCols, ok := extractSimpleOutputColumns(selectListRaw)
	if !ok {
		return sql
	}

	fromWhereRaw := strings.TrimSpace(joinTokens(toks[fromIdx:orderIdx]))

	orderByStart := nextSigTok(toks, orderIdx+1) // "BY"
	orderByEnd := len(toks)
	if limitIdx >= 0 && limitIdx < orderByEnd {
		orderByEnd = limitIdx
	}
	if offsetIdx >= 0 && offsetIdx < orderByEnd {
		orderByEnd = offsetIdx
	}
	orderByExprs := strings.TrimSpace(joinTokens(toks[orderByStart+1 : orderByEnd]))
	if orderByExprs == "" {
		return sql
	}

	// Verify ORDER BY leading expressions match DISTINCT ON keys (PG requires this).
	if !orderByPrefixMatchesDistinctKeys(distinctKeysRaw, orderByExprs) {
		return sql
	}

	tailClause := ""
	if orderByEnd < len(toks) {
		tailClause = " " + strings.TrimSpace(joinTokens(toks[orderByEnd:]))
	}
	trailingSemi := ""
	if strings.HasSuffix(strings.TrimSpace(tailClause), ";") {
		tailClause = strings.TrimSuffix(strings.TrimSpace(tailClause), ";")
		if tailClause != "" {
			tailClause = " " + tailClause
		}
		trailingSemi = ";"
	} else if strings.HasSuffix(strings.TrimSpace(orderByExprs), ";") {
		orderByExprs = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(orderByExprs), ";"))
		trailingSemi = ";"
	}

	return "SELECT " + strings.Join(outerCols, ", ") +
		" FROM (SELECT " + selectListRaw +
		", ROW_NUMBER() OVER (PARTITION BY " + distinctKeysRaw + " ORDER BY " + orderByExprs + ") AS __air_rn " +
		fromWhereRaw + ") WHERE __air_rn = 1 ORDER BY " + orderByExprs + tailClause + trailingSemi
}

func splitTopLevelComma(s string) []string {
	var parts []string
	depth := 0
	inStr := false
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\'' {
			inStr = !inStr
			continue
		}
		if inStr {
			continue
		}
		switch c {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(s[start:]))
	return parts
}

func extractSimpleOutputColumns(selectList string) ([]string, bool) {
	items := splitTopLevelComma(selectList)
	if len(items) == 0 {
		return nil, false
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item == "" || item == "*" || strings.HasSuffix(item, ".*") {
			return nil, false
		}
		toks := tokenizeSQLForRewrite(item)
		// Check for explicit AS <alias> at top level
		depth := 0
		asIdx := -1
		for i := 0; i < len(toks); i++ {
			if toks[i].raw == "(" {
				depth++
			} else if toks[i].raw == ")" {
				depth--
			} else if depth == 0 && toks[i].kind == rwWord && toks[i].upper == "AS" {
				asIdx = i
			}
		}
		if asIdx >= 0 {
			ns := nextSigTok(toks, asIdx+1)
			if ns < 0 || toks[ns].kind != rwWord {
				return nil, false
			}
			out = append(out, toks[ns].raw)
			continue
		}
		// Simple identifier or t.col
		sigCount := 0
		lastTok := ""
		onlyIdent := true
		for _, t := range toks {
			if t.kind == rwSpace || t.kind == rwComment {
				continue
			}
			sigCount++
			if t.kind != rwWord && t.raw != "." {
				onlyIdent = false
			}
			lastTok = t.raw
		}
		if !onlyIdent || sigCount == 0 {
			return nil, false
		}
		// For t.col, SQLite result column name is col (strip table qualifier)
		if dot := strings.LastIndex(lastTok, "."); dot >= 0 {
			lastTok = lastTok[dot+1:]
		}
		out = append(out, lastTok)
	}
	return out, true
}

func orderByPrefixMatchesDistinctKeys(distinctKeysRaw, orderByExprs string) bool {
	dKeys := splitTopLevelComma(distinctKeysRaw)
	oKeys := splitTopLevelComma(orderByExprs)
	if len(oKeys) < len(dKeys) || len(dKeys) == 0 {
		return false
	}
	for i, dk := range dKeys {
		ok := oKeys[i]
		// Strip trailing ASC/DESC/NULLS FIRST/NULLS LAST from order key
		fields := strings.Fields(ok)
		for len(fields) > 1 {
			last := strings.ToUpper(fields[len(fields)-1])
			if last == "ASC" || last == "DESC" || last == "FIRST" || last == "LAST" || last == "NULLS" {
				fields = fields[:len(fields)-1]
			} else {
				break
			}
		}
		normO := strings.ToLower(strings.Join(fields, " "))
		normD := strings.ToLower(strings.Join(strings.Fields(dk), " "))
		if normO != normD {
			return false
		}
	}
	return true
}

// rewriteNumericOps rewrites SUM(num_col), AVG(num_col), and binary +, -, *, /
// involving numeric columns into exact decimal calls (air_pg_num_*).
func rewriteNumericOps(sql string, extCols map[string]ColumnType) string {
	if len(extCols) == 0 {
		return sql
	}
	hasNumSchema := false
	for _, ct := range extCols {
		if ct == ColNumeric {
			hasNumSchema = true
			break
		}
	}
	if !hasNumSchema {
		return sql
	}

	toks := tokenizeSQLForRewrite(sql)
	isNumIdent := func(tok rwTok) bool {
		if tok.kind != rwWord {
			return false
		}
		name := strings.ToLower(tok.raw)
		if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
			name = name[dot+1:]
		}
		name = strings.Trim(name, `"`)
		return extCols[name] == ColNumeric
	}

	hasNumQuery := false
	for _, t := range toks {
		if isNumIdent(t) {
			hasNumQuery = true
			break
		}
	}
	if !hasNumQuery {
		return sql
	}

	// Pass 1: Rewrite SUM(expr) and AVG(expr) when expr contains a numeric column and no DISTINCT.
	var p1 []rwTok
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		if t.kind == rwWord && (t.upper == "SUM" || t.upper == "AVG") {
			lp := nextSigTok(toks, i+1)
			if lp >= 0 && toks[lp].raw == "(" {
				depth := 1
				rp := lp + 1
				hasNumInner := false
				hasDistinct := false
				for rp < len(toks) && depth > 0 {
					if toks[rp].raw == "(" {
						depth++
					} else if toks[rp].raw == ")" {
						depth--
						if depth == 0 {
							break
						}
					} else if toks[rp].kind == rwWord {
						if toks[rp].upper == "DISTINCT" {
							hasDistinct = true
						}
						if isNumIdent(toks[rp]) {
							hasNumInner = true
						}
					}
					rp++
				}
				if depth == 0 && hasNumInner && !hasDistinct {
					fn := "air_pg_num_sum"
					if t.upper == "AVG" {
						fn = "air_pg_num_avg"
					}
					p1 = append(p1, rwTok{kind: rwWord, raw: fn, upper: strings.ToUpper(fn)})
					p1 = append(p1, toks[lp:rp+1]...)
					i = rp
					continue
				}
			}
		}
		p1 = append(p1, t)
	}

	// Pass 2: Rewrite binary *, / then +, - when either side is a numeric column or air_pg_num_* result.
	rewriteBinPass := func(in []rwTok, opA, opB string) []rwTok {
		var out []rwTok
		for i := 0; i < len(in); i++ {
			t := in[i]
			if t.kind == rwOp && (t.raw == opA || t.raw == opB) {
				if lhsStart, ok := prevPrimaryBounds(out); ok {
					if rhsStart, rhsEnd, ok2 := nextPrimaryBounds(in, i+1); ok2 {
						lhs := strings.TrimSpace(joinTokens(out[lhsStart:]))
						rhs := strings.TrimSpace(joinTokens(in[rhsStart : rhsEnd+1]))
						lhsNum := containsNumExpr(out[lhsStart:], isNumIdent)
						rhsNum := containsNumExpr(in[rhsStart:rhsEnd+1], isNumIdent)
						if lhsNum || rhsNum {
							fn := ""
							switch t.raw {
							case "+":
								fn = "air_pg_num_add"
							case "-":
								fn = "air_pg_num_sub"
							case "*":
								fn = "air_pg_num_mul"
							case "/":
								fn = "air_pg_num_div"
							}
							out = out[:lhsStart]
							call := fn + "(" + lhs + ", " + rhs + ")"
							out = append(out, rwTok{kind: rwWord, raw: call, upper: strings.ToUpper(call)})
							i = rhsEnd
							continue
						}
					}
				}
			}
			out = append(out, t)
		}
		return out
	}

	p2 := rewriteBinPass(p1, "*", "/")
	p3 := rewriteBinPass(p2, "+", "-")
	return joinTokens(p3)
}

func containsNumExpr(toks []rwTok, isNumIdent func(rwTok) bool) bool {
	for _, t := range toks {
		if isNumIdent(t) || strings.Contains(t.upper, "AIR_PG_NUM_") {
			return true
		}
	}
	return false
}
