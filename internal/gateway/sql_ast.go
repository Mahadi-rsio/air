package gateway

import (
	"strings"
	"unicode"
)

// ASTTokenKind represents a PostgreSQL lexical token category.
type ASTTokenKind uint8

const (
	TokWord ASTTokenKind = iota
	TokNumber
	TokString
	TokParam
	TokOp
	TokPunct
)

// ASTToken is a single PostgreSQL lexical token.
type ASTToken struct {
	Kind  ASTTokenKind
	Raw   string
	Upper string
	Pos   int
}

// StmtKind identifies the top-level PostgreSQL statement type.
type StmtKind uint8

const (
	StmtEmpty StmtKind = iota
	StmtSelect
	StmtWith
	StmtValues
	StmtTable
	StmtExplain
	StmtCopy
	StmtInsert
	StmtUpdate
	StmtDelete
	StmtMerge
	StmtDDL
	StmtTx
	StmtSet
	StmtReset
	StmtShow
	StmtDiscard
	StmtDeallocate
	StmtPrepare
	StmtExecute
	StmtOther
)

// CTEClause represents a Common Table Expression in a WITH clause.
type CTEClause struct {
	Name      string
	Recursive bool
	IsWrite   bool
}

// SQLStmtAST holds the structural parse result for a single SQL statement.
type SQLStmtAST struct {
	Kind          StmtKind
	HeadKeyword   string
	Tokens        []ASTToken
	CTEs          []CTEClause
	Joins         []string
	SetOps        []string
	Functions     []string
	Operators     []string
	Casts         []string
	Tables        []string
	Features      []FeatureRef
	HasSubquery   bool
	HasWindowFunc bool
	HasFilter     bool
	HasDistinct   bool
	HasDistinctOn bool
	HasGroupBy    bool
	HasHaving     bool
	HasOrderBy    bool
	HasNullsOrder bool
	HasLimit      bool
	HasOffset     bool
	HasForLocking bool
	HasSelectInto bool
	HasModCTE     bool
	HasSysCatalog bool
	CopyToStdout  bool
	CopyFrom      bool
	SetReadOnly   bool
	MutatingFunc  string
}

// SQLScriptAST holds all parsed statements in a SQL request.
type SQLScriptAST struct {
	Statements []*SQLStmtAST
}

func isASTIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isASTIdentChar(c byte) bool {
	return isASTIdentStart(c) || (c >= '0' && c <= '9') || c == '$'
}

// LexPostgreSQL tokenizes a PostgreSQL SQL string into statement token slices.
func LexPostgreSQL(sql string) [][]ASTToken {
	stmts := [][]ASTToken{{}}
	n := len(sql)
	i := 0

	appendTok := func(tok ASTToken) {
		stmts[len(stmts)-1] = append(stmts[len(stmts)-1], tok)
	}

	for i < n {
		c := sql[i]

		if unicode.IsSpace(rune(c)) {
			i++
			continue
		}

		// Line comment: -- ...
		if c == '-' && i+1 < n && sql[i+1] == '-' {
			i += 2
			for i < n && sql[i] != '\n' {
				i++
			}
			continue
		}

		// Nested block comment: /* ... */
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

		// Escape/bit/hex/national string literal: E'...', B'...', X'...', N'...'
		if (c == 'E' || c == 'e' || c == 'B' || c == 'b' || c == 'X' || c == 'x' || c == 'N' || c == 'n') &&
			i+1 < n && sql[i+1] == '\'' {
			start := i
			isEsc := c == 'E' || c == 'e'
			i += 2
			for i < n {
				if isEsc && sql[i] == '\\' && i+1 < n {
					i += 2
					continue
				}
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
			appendTok(ASTToken{Kind: TokString, Raw: sql[start:i], Upper: "'", Pos: start})
			continue
		}

		// Standard string literal: '...'
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
			appendTok(ASTToken{Kind: TokString, Raw: sql[start:i], Upper: "'", Pos: start})
			continue
		}

		// Double-quoted identifier: "..."
		if c == '"' {
			start := i
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
			s := ident.String()
			appendTok(ASTToken{Kind: TokWord, Raw: s, Upper: strings.ToUpper(s), Pos: start})
			continue
		}

		// Dollar-quoted string ($tag$...$tag$) or positional parameter ($1)
		if c == '$' {
			if i+1 < n && sql[i+1] >= '0' && sql[i+1] <= '9' {
				start := i
				i++
				for i < n && sql[i] >= '0' && sql[i] <= '9' {
					i++
				}
				raw := sql[start:i]
				appendTok(ASTToken{Kind: TokParam, Raw: raw, Upper: raw, Pos: start})
				continue
			}
			tagEnd := i + 1
			for tagEnd < n && ((sql[tagEnd] >= 'a' && sql[tagEnd] <= 'z') ||
				(sql[tagEnd] >= 'A' && sql[tagEnd] <= 'Z') ||
				(sql[tagEnd] >= '0' && sql[tagEnd] <= '9') ||
				sql[tagEnd] == '_') {
				tagEnd++
			}
			if tagEnd < n && sql[tagEnd] == '$' {
				start := i
				tag := sql[i : tagEnd+1]
				if closeIdx := strings.Index(sql[tagEnd+1:], tag); closeIdx >= 0 {
					i = tagEnd + 1 + closeIdx + len(tag)
				} else {
					i = n
				}
				appendTok(ASTToken{Kind: TokString, Raw: sql[start:i], Upper: "$$", Pos: start})
				continue
			}
		}

		// Statement separator
		if c == ';' {
			if len(stmts[len(stmts)-1]) > 0 {
				stmts = append(stmts, nil)
			}
			i++
			continue
		}

		// Numbers
		if c >= '0' && c <= '9' {
			start := i
			i++
			for i < n && ((sql[i] >= '0' && sql[i] <= '9') || sql[i] == '.' || sql[i] == 'e' || sql[i] == 'E' || sql[i] == 'x' || sql[i] == 'X' || (sql[i] >= 'a' && sql[i] <= 'f') || (sql[i] >= 'A' && sql[i] <= 'F') || sql[i] == '_') {
				if (sql[i] == 'e' || sql[i] == 'E') && i+1 < n && (sql[i+1] == '+' || sql[i+1] == '-') {
					i += 2
					continue
				}
				i++
			}
			raw := sql[start:i]
			appendTok(ASTToken{Kind: TokNumber, Raw: raw, Upper: raw, Pos: start})
			continue
		}

		// Identifiers / Keywords
		if isASTIdentStart(c) {
			start := i
			i++
			for i < n && isASTIdentChar(sql[i]) {
				i++
			}
			raw := sql[start:i]
			appendTok(ASTToken{Kind: TokWord, Raw: raw, Upper: strings.ToUpper(raw), Pos: start})
			continue
		}

		// Multi-character operators
		if i+2 < n {
			tri := sql[i : i+3]
			if tri == "!~*" || tri == "#>>" || tri == "->>" {
				appendTok(ASTToken{Kind: TokOp, Raw: tri, Upper: tri, Pos: i})
				i += 3
				continue
			}
		}
		if i+1 < n {
			di := sql[i : i+2]
			switch di {
			case "::", "->", "#>", "@>", "<@", "?|", "?&", "!~", "~*", "||", "<=", ">=", "<>", "!=":
				appendTok(ASTToken{Kind: TokOp, Raw: di, Upper: di, Pos: i})
				i += 2
				continue
			}
		}

		// Punctuation vs single-char operator
		switch c {
		case '(', ')', '[', ']', ',', '.':
			appendTok(ASTToken{Kind: TokPunct, Raw: sql[i : i+1], Upper: sql[i : i+1], Pos: i})
		default:
			appendTok(ASTToken{Kind: TokOp, Raw: sql[i : i+1], Upper: sql[i : i+1], Pos: i})
		}
		i++
	}

	if len(stmts) > 1 && len(stmts[len(stmts)-1]) == 0 {
		stmts = stmts[:len(stmts)-1]
	}
	return stmts
}

// ParseSQLAST parses a PostgreSQL SQL string into a structured SQLScriptAST.
func ParseSQLAST(sql string) *SQLScriptAST {
	rawStmts := LexPostgreSQL(sql)
	script := &SQLScriptAST{
		Statements: make([]*SQLStmtAST, 0, len(rawStmts)),
	}
	for _, toks := range rawStmts {
		if len(toks) == 0 {
			continue
		}
		script.Statements = append(script.Statements, parseSingleStmtAST(toks))
	}
	if len(script.Statements) == 0 {
		script.Statements = append(script.Statements, &SQLStmtAST{
			Kind:     StmtEmpty,
			Features: []FeatureRef{{Category: CatStatement, Name: "EMPTY"}},
		})
	}
	return script
}

func parseSingleStmtAST(toks []ASTToken) *SQLStmtAST {
	ast := &SQLStmtAST{
		Tokens:      toks,
		HeadKeyword: toks[0].Upper,
	}

	head := toks[0].Upper
	switch head {
	case "SELECT":
		ast.Kind = StmtSelect
		ast.addFeature(CatStatement, "SELECT")
	case "WITH":
		ast.Kind = StmtWith
		ast.addFeature(CatStatement, "WITH")
	case "VALUES":
		ast.Kind = StmtValues
		ast.addFeature(CatStatement, "VALUES")
	case "TABLE":
		ast.Kind = StmtTable
		ast.addFeature(CatStatement, "TABLE")
	case "EXPLAIN":
		ast.Kind = StmtExplain
		ast.addFeature(CatStatement, "EXPLAIN")
	case "COPY":
		ast.Kind = StmtCopy
	case "INSERT":
		ast.Kind = StmtInsert
		ast.addFeature(CatStatement, "INSERT")
	case "UPDATE":
		ast.Kind = StmtUpdate
		ast.addFeature(CatStatement, "UPDATE")
	case "DELETE":
		ast.Kind = StmtDelete
		ast.addFeature(CatStatement, "DELETE")
	case "MERGE":
		ast.Kind = StmtMerge
		ast.addFeature(CatStatement, "MERGE")
	case "BEGIN", "START", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE", "ABORT":
		ast.Kind = StmtTx
		ast.addFeature(CatStatement, head)
	case "SET":
		ast.Kind = StmtSet
		for i := 1; i < len(toks); i++ {
			if toks[i].Upper == "TRANSACTION_READ_ONLY" || toks[i].Upper == "DEFAULT_TRANSACTION_READ_ONLY" {
				ast.SetReadOnly = true
				ast.addFeature(CatStatement, "SET")
				break
			}
		}
	case "RESET":
		ast.Kind = StmtReset
		for i := 1; i < len(toks); i++ {
			if toks[i].Upper == "TRANSACTION_READ_ONLY" || toks[i].Upper == "DEFAULT_TRANSACTION_READ_ONLY" {
				ast.SetReadOnly = true
				ast.addFeature(CatStatement, "RESET")
				break
			}
		}
	case "SHOW":
		ast.Kind = StmtShow
		ast.addFeature(CatStatement, "SHOW")
	case "DISCARD":
		ast.Kind = StmtDiscard
		ast.addFeature(CatStatement, "DISCARD")
	case "DEALLOCATE":
		ast.Kind = StmtDeallocate
		ast.addFeature(CatStatement, "DEALLOCATE")
	case "PREPARE":
		ast.Kind = StmtPrepare
		ast.addFeature(CatStatement, "PREPARE")
	case "EXECUTE":
		ast.Kind = StmtExecute
		ast.addFeature(CatStatement, "EXECUTE")
	default:
		if isDDLOrWriteHead(head) {
			ast.Kind = StmtDDL
			ast.addFeature(CatStatement, head)
		} else {
			ast.Kind = StmtOther
			ast.addFeature(CatStatement, head)
		}
	}

	// Early return for purely write/DDL/tx statements
	switch ast.Kind {
	case StmtInsert, StmtUpdate, StmtDelete, StmtMerge, StmtDDL, StmtTx, StmtPrepare, StmtExecute, StmtShow, StmtDiscard, StmtDeallocate:
		return ast
	case StmtSet, StmtReset:
		return ast
	}

	if ast.Kind == StmtCopy {
		analyzeCopyStmt(ast, toks)
		return ast
	}

	analyzeQueryBody(ast, toks)
	return ast
}

func (ast *SQLStmtAST) addFeature(cat CapabilityCategory, name string) {
	ast.Features = append(ast.Features, FeatureRef{Category: cat, Name: name})
}

func analyzeCopyStmt(ast *SQLStmtAST, toks []ASTToken) {
	parenDepth := 0
	for i := 1; i < len(toks); i++ {
		w := toks[i].Upper
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
				ast.CopyFrom = true
				ast.addFeature(CatStatement, "COPY_FROM")
				return
			}
			if w == "TO" {
				ast.CopyToStdout = true
				ast.addFeature(CatStatement, "COPY_TO")
				return
			}
		} else {
			if isWriteKeyword(w) {
				ast.HasModCTE = true
				ast.addFeature(CatClause, "MODIFYING_CTE")
			}
			if isMutatingFunction(w) && i+1 < len(toks) && toks[i+1].Upper == "(" {
				ast.MutatingFunc = w
				ast.addFeature(CatFunction, w)
			}
		}
	}
	ast.CopyFrom = true
	ast.addFeature(CatStatement, "COPY_FROM")
}

func analyzeQueryBody(ast *SQLStmtAST, toks []ASTToken) {
	parenDepth := 0
	inSelectHead := ast.HeadKeyword == "SELECT"

	for i := 0; i < len(toks); i++ {
		tok := toks[i]
		w := tok.Upper

		if w == "(" {
			parenDepth++
			// Subquery detection: '(' followed by SELECT/WITH/VALUES/TABLE
			if i+1 < len(toks) {
				nw := toks[i+1].Upper
				if nw == "SELECT" || nw == "WITH" || nw == "VALUES" || nw == "TABLE" {
					ast.HasSubquery = true
				}
			}
			continue
		}
		if w == ")" {
			if parenDepth > 0 {
				parenDepth--
			}
			continue
		}

		// Row locking: FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE / FOR KEY SHARE
		if w == "FOR" && i+1 < len(toks) {
			next := toks[i+1].Upper
			if next == "UPDATE" || next == "SHARE" || next == "NO" || next == "KEY" {
				ast.HasForLocking = true
				ast.addFeature(CatClause, "FOR_UPDATE")
				if next == "UPDATE" {
					i++
				}
				continue
			}
		}

		// Modifying CTE or nested DML anywhere in query
		if i > 0 && isWriteKeyword(w) {
			ast.HasModCTE = true
			ast.addFeature(CatClause, "MODIFYING_CTE")
		}

		// System catalog detection
		if isSystemCatalogToken(w) || w == "INFORMATION_SCHEMA" {
			ast.HasSysCatalog = true
		}

		// CTE analysis
		if w == "WITH" && i+1 < len(toks) && toks[i+1].Upper == "RECURSIVE" {
			ast.CTEs = append(ast.CTEs, CTEClause{Recursive: true})
			ast.addFeature(CatClause, "RECURSIVE_CTE")
		}

		// SELECT INTO
		if w == "INTO" && inSelectHead && parenDepth == 0 {
			ast.HasSelectInto = true
			ast.addFeature(CatClause, "SELECT_INTO")
		}

		// DISTINCT / DISTINCT ON
		if w == "DISTINCT" {
			if i+1 < len(toks) && toks[i+1].Upper == "ON" {
				ast.HasDistinctOn = true
				ast.addFeature(CatClause, "DISTINCT_ON")
			} else if i > 0 && toks[i-1].Upper == "IS" {
				// IS DISTINCT FROM
				ast.Operators = append(ast.Operators, "IS_DISTINCT_FROM")
				ast.addFeature(CatOperator, "IS_DISTINCT_FROM")
			} else {
				ast.HasDistinct = true
				ast.addFeature(CatClause, "DISTINCT")
			}
		}

		// JOINs
		if w == "JOIN" {
			joinType := "INNER"
			if i > 0 {
				prev := toks[i-1].Upper
				switch prev {
				case "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL", "INNER":
					joinType = prev
				case "OUTER":
					if i >= 2 {
						prev2 := toks[i-2].Upper
						if prev2 == "LEFT" || prev2 == "RIGHT" || prev2 == "FULL" {
							joinType = prev2
						}
					}
				}
			}
			ast.Joins = append(ast.Joins, joinType)
			ast.addFeature(CatJoin, joinType)
		}
		if w == "LATERAL" {
			ast.Joins = append(ast.Joins, "LATERAL")
			ast.addFeature(CatJoin, "LATERAL")
			ast.addFeature(CatClause, "LATERAL")
		}

		// GROUP BY / HAVING / GROUPING SETS / ROLLUP / CUBE
		if w == "GROUP" && i+1 < len(toks) && toks[i+1].Upper == "BY" {
			ast.HasGroupBy = true
			ast.addFeature(CatClause, "GROUP_BY")
		}
		if w == "GROUPING" && i+1 < len(toks) && toks[i+1].Upper == "SETS" {
			ast.addFeature(CatClause, "GROUPING_SETS")
		}
		if (w == "ROLLUP" || w == "CUBE") && i+1 < len(toks) && toks[i+1].Upper == "(" {
			ast.addFeature(CatClause, w)
		}
		if w == "HAVING" {
			ast.HasHaving = true
			ast.addFeature(CatClause, "HAVING")
		}

		// ORDER BY / NULLS FIRST / NULLS LAST / LIMIT / OFFSET
		if w == "ORDER" && i+1 < len(toks) && toks[i+1].Upper == "BY" {
			ast.HasOrderBy = true
			ast.addFeature(CatClause, "ORDER_BY")
		}
		if w == "NULLS" && i+1 < len(toks) {
			next := toks[i+1].Upper
			if next == "FIRST" {
				ast.HasNullsOrder = true
				ast.addFeature(CatClause, "NULLS_FIRST")
			} else if next == "LAST" {
				ast.HasNullsOrder = true
				ast.addFeature(CatClause, "NULLS_LAST")
			}
		}
		if w == "LIMIT" {
			ast.HasLimit = true
			ast.addFeature(CatClause, "LIMIT")
		}
		if w == "OFFSET" {
			ast.HasOffset = true
			ast.addFeature(CatClause, "OFFSET")
		}

		// Set operations
		if w == "UNION" || w == "INTERSECT" || w == "EXCEPT" {
			ast.SetOps = append(ast.SetOps, w)
			ast.addFeature(CatClause, w)
		}

		// FILTER (WHERE ...)
		if w == "FILTER" && i+1 < len(toks) && toks[i+1].Upper == "(" {
			ast.HasFilter = true
			ast.addFeature(CatClause, "FILTER")
		}

		// Window OVER (...)
		if w == "OVER" && i+1 < len(toks) && toks[i+1].Upper == "(" {
			ast.HasWindowFunc = true
			// Check for GROUPS frame which SQLite < 3.28 or some dialects handle differently
			for j := i + 2; j < len(toks) && toks[j].Upper != ")"; j++ {
				if toks[j].Upper == "GROUPS" {
					ast.addFeature(CatClause, "WINDOW_GROUPS")
					break
				}
			}
		}

		// ON CONFLICT / RETURNING / TABLESAMPLE
		if w == "ON" && i+1 < len(toks) && toks[i+1].Upper == "CONFLICT" {
			ast.addFeature(CatClause, "ON_CONFLICT")
		}
		if w == "RETURNING" {
			ast.addFeature(CatClause, "RETURNING")
		}
		if w == "TABLESAMPLE" {
			ast.addFeature(CatClause, "TABLESAMPLE")
		}

		// Operators
		if tok.Kind == TokOp {
			switch w {
			case "::":
				typeName, advance := parseCastTypeName(toks, i+1)
				if typeName != "" {
					ast.Casts = append(ast.Casts, typeName)
					ast.addFeature(CatCast, typeName)
					i += advance
				}
			case "~", "~*", "!~", "!~*", "->", "->>", "#>", "#>>", "@>", "<@", "?", "?|", "?&", "@@", "@?":
				ast.Operators = append(ast.Operators, w)
				ast.addFeature(CatOperator, w)
			}
		}
		if w == "ILIKE" {
			opName := "ILIKE"
			if i > 0 && toks[i-1].Upper == "NOT" {
				opName = "NOT_ILIKE"
			}
			ast.Operators = append(ast.Operators, opName)
			ast.addFeature(CatOperator, opName)
		}
		if w == "IS" {
			if i+2 < len(toks) && toks[i+1].Upper == "DISTINCT" && toks[i+2].Upper == "FROM" {
				ast.Operators = append(ast.Operators, "IS_DISTINCT_FROM")
				ast.addFeature(CatOperator, "IS_DISTINCT_FROM")
			} else if i+3 < len(toks) && toks[i+1].Upper == "NOT" && toks[i+2].Upper == "DISTINCT" && toks[i+3].Upper == "FROM" {
				ast.Operators = append(ast.Operators, "IS_NOT_DISTINCT_FROM")
				ast.addFeature(CatOperator, "IS_NOT_DISTINCT_FROM")
			}
		}
		if w == "SIMILAR" && i+1 < len(toks) && toks[i+1].Upper == "TO" {
			ast.Operators = append(ast.Operators, "SIMILAR_TO")
			ast.addFeature(CatOperator, "SIMILAR_TO")
		}
		if (w == "ANY" || w == "ALL" || w == "SOME") && i+1 < len(toks) && toks[i+1].Upper == "(" {
			// Scalar subquery ANY/ALL vs array ANY/ALL
			if i+2 < len(toks) && toks[i+2].Upper != "SELECT" && toks[i+2].Upper != "VALUES" {
				ast.Operators = append(ast.Operators, w)
				ast.addFeature(CatOperator, w)
			}
		}
		if w == "OVERLAPS" {
			ast.Operators = append(ast.Operators, "OVERLAPS")
			ast.addFeature(CatOperator, "OVERLAPS")
		}

		if w == "CURRENT_USER" || w == "SESSION_USER" || w == "CURRENT_ROLE" {
			ast.Functions = append(ast.Functions, w)
			ast.addFeature(CatFunction, w)
		}

		// Function calls: word followed by '(' (excluding SQL keywords and CTE column lists)
		if tok.Kind == TokWord && i+1 < len(toks) && toks[i+1].Upper == "(" && !isNonFuncKeyword(w) && !isCTEColumnList(toks, i+1) {
			if w == "CAST" {
				// CAST(expr AS type)
				typeName := parseSQLCastTarget(toks, i+2)
				if typeName != "" {
					ast.Casts = append(ast.Casts, typeName)
					ast.addFeature(CatCast, typeName)
				}
			} else {
				// Preserve schema-qualified function calls (e.g. my_schema.fn(...)) so custom
				// or unknown schema UDFs never masquerade as built-in SQLite functions.
				fnName := w
				if i >= 2 && toks[i-1].Raw == "." && toks[i-2].Kind == TokWord {
					schema := toks[i-2].Upper
					fnName = schema + "." + w
					if schema == "PG_CATALOG" {
						if isMutatingFunction(w) || isSystemCatalogToken(w) {
							ast.HasSysCatalog = true
						}
					}
				}
				ast.Functions = append(ast.Functions, fnName)
				if isMutatingFunction(w) {
					ast.MutatingFunc = fnName
				}
				ast.addFeature(CatFunction, fnName)
			}
		}

		// Collect table names after FROM / JOIN
		if (w == "FROM" || w == "JOIN") && i+1 < len(toks) && toks[i+1].Kind == TokWord {
			tname := toks[i+1].Upper
			if i+3 < len(toks) && toks[i+2].Raw == "." && toks[i+3].Kind == TokWord {
				tname = tname + "." + toks[i+3].Upper
			}
			ast.Tables = append(ast.Tables, tname)
		}
	}
}

func parseCastTypeName(toks []ASTToken, start int) (string, int) {
	if start >= len(toks) {
		return "", 0
	}
	// Handle schema-qualified types like my_schema.my_enum or pg_catalog.int4
	if start+2 < len(toks) && toks[start].Kind == TokWord && toks[start+1].Raw == "." && toks[start+2].Kind == TokWord {
		qualified := toks[start].Upper + "." + toks[start+2].Upper
		i := start + 3
		if i < len(toks) && toks[i].Raw == "(" {
			depth := 1
			i++
			for i < len(toks) && depth > 0 {
				if toks[i].Raw == "(" {
					depth++
				} else if toks[i].Raw == ")" {
					depth--
				}
				i++
			}
		}
		if i+1 < len(toks) && toks[i].Raw == "[" && toks[i+1].Raw == "]" {
			return "ARRAY", (i + 2) - start
		}
		return qualified, i - start
	}
	var parts []string
	i := start
	for i < len(toks) && toks[i].Kind == TokWord {
		u := toks[i].Upper
		// Stop at clause keywords
		if isClauseBoundary(u) {
			break
		}
		parts = append(parts, u)
		i++
		// Only multi-word types like DOUBLE PRECISION, CHARACTER VARYING, TIMESTAMP WITH TIME ZONE
		if u != "DOUBLE" && u != "CHARACTER" && u != "TIMESTAMP" && u != "TIME" && u != "WITH" && u != "WITHOUT" {
			break
		}
	}
	// Check for array suffix []
	if i+1 < len(toks) && toks[i].Raw == "[" && toks[i+1].Raw == "]" {
		return "ARRAY", (i + 2) - start
	}
	// Skip type precision (10,2)
	if i < len(toks) && toks[i].Raw == "(" {
		depth := 1
		i++
		for i < len(toks) && depth > 0 {
			if toks[i].Raw == "(" {
				depth++
			} else if toks[i].Raw == ")" {
				depth--
			}
			i++
		}
		if i+1 < len(toks) && toks[i].Raw == "[" && toks[i+1].Raw == "]" {
			return "ARRAY", (i + 2) - start
		}
	}
	if len(parts) == 0 {
		return "", 0
	}
	return strings.Join(parts, "_"), i - start
}

func parseSQLCastTarget(toks []ASTToken, start int) string {
	depth := 1
	for i := start; i < len(toks) && depth > 0; i++ {
		if toks[i].Raw == "(" {
			depth++
		} else if toks[i].Raw == ")" {
			depth--
		} else if depth == 1 && toks[i].Upper == "AS" && i+1 < len(toks) {
			name, _ := parseCastTypeName(toks, i+1)
			return name
		}
	}
	return ""
}

func isCTEColumnList(toks []ASTToken, openParenIdx int) bool {
	depth := 0
	for j := openParenIdx; j < len(toks); j++ {
		if toks[j].Raw == "(" {
			depth++
		} else if toks[j].Raw == ")" {
			depth--
			if depth == 0 {
				return j+2 < len(toks) && toks[j+1].Upper == "AS" && toks[j+2].Raw == "("
			}
		}
	}
	return false
}

func isClauseBoundary(u string) bool {
	switch u {
	case "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET",
		"UNION", "INTERSECT", "EXCEPT", "AS", "ON", "AND", "OR", "IN",
		"IS", "NOT", "THEN", "ELSE", "END", "WHEN", "JOIN", "LEFT", "RIGHT",
		"INNER", "OUTER", "CROSS", "FULL", "INTO", "FOR", "ASC", "DESC", "NULLS":
		return true
	}
	return false
}

func isNonFuncKeyword(w string) bool {
	switch w {
	case "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "EXISTS",
		"ANY", "ALL", "SOME", "CASE", "WHEN", "THEN", "ELSE", "END",
		"AS", "ON", "USING", "VALUES", "TABLE", "WITH", "OVER", "FILTER",
		"BETWEEN", "LIKE", "ILIKE", "SIMILAR", "GROUP", "ORDER", "HAVING",
		"UNION", "INTERSECT", "EXCEPT", "ROLLUP", "CUBE", "SETS", "DISTINCT",
		"CHECK", "PRIMARY", "UNIQUE", "REFERENCES", "INTERVAL":
		return true
	}
	return false
}
