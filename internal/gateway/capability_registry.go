package gateway

import "strings"

// CapabilitySupport classifies how well AIR's SQLite Read Engine supports a SQL feature.
// PARTIAL_SUPPORT must NEVER be treated as FULL_SUPPORT for read-engine routing.
type CapabilitySupport uint8

const (
	CapUnknown CapabilitySupport = iota
	CapFullSupport
	CapPartialSupport
	CapUnsupported
)

func (c CapabilitySupport) String() string {
	switch c {
	case CapFullSupport:
		return "FULL_SUPPORT"
	case CapPartialSupport:
		return "PARTIAL_SUPPORT"
	case CapUnsupported:
		return "UNSUPPORTED"
	default:
		return "UNKNOWN"
	}
}

// IsReadEngineEligible returns true only when support is FULL_SUPPORT.
func (c CapabilitySupport) IsReadEngineEligible() bool {
	return c == CapFullSupport
}

// CapabilityCategory identifies the domain of a SQL feature in CapabilityRegistry.
type CapabilityCategory uint8

const (
	CatStatement CapabilityCategory = iota
	CatClause
	CatJoin
	CatWindow
	CatAggregate
	CatFunction
	CatOperator
	CatDataType
	CatCast
)

// FeatureRef references a single SQL feature observed in a parsed AST.
type FeatureRef struct {
	Category CapabilityCategory
	Name     string
}

// CapabilityEntry describes the support level and rationale for a SQL feature.
type CapabilityEntry struct {
	Category CapabilityCategory
	Name     string
	Support  CapabilitySupport
	Reason   string
}

// CapabilityEvaluation summarizes the combined capability check for a query AST.
type CapabilityEvaluation struct {
	Support CapabilitySupport
	Reason  string
	Blocker *FeatureRef
}

type capKey struct {
	cat  CapabilityCategory
	name string
}

// CapabilityRegistry centralizes feature support metadata for the SQL router.
type CapabilityRegistry struct {
	entries map[capKey]CapabilityEntry
}

// NewDefaultCapabilityRegistry builds the default capability matrix for AIR's SQLite Read Engine.
func NewDefaultCapabilityRegistry() *CapabilityRegistry {
	r := &CapabilityRegistry{
		entries: make(map[capKey]CapabilityEntry, 256),
	}

	reg := func(cat CapabilityCategory, sup CapabilitySupport, reason string, names ...string) {
		for _, n := range names {
			upper := strings.ToUpper(strings.TrimSpace(n))
			r.entries[capKey{cat: cat, name: upper}] = CapabilityEntry{
				Category: cat,
				Name:     upper,
				Support:  sup,
				Reason:   reason,
			}
		}
	}

	// Statements
	reg(CatStatement, CapFullSupport, "read-only query supported on ReadEngine",
		"SELECT", "WITH", "VALUES", "TABLE", "EXPLAIN", "COPY_TO", "EMPTY")
	reg(CatStatement, CapUnsupported, "mutating or stateful statement requires WriteEngine",
		"INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE",
		"COPY_FROM", "BEGIN", "START", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE", "ABORT",
		"DO", "CALL", "VACUUM", "ANALYZE", "CLUSTER", "REINDEX", "COMMENT", "REFRESH",
		"GRANT", "REVOKE", "LOCK", "CHECKPOINT", "IMPORT", "REASSIGN", "SECURITY",
		"NOTIFY", "LISTEN", "UNLISTEN", "PREPARE", "EXECUTE", "SET", "RESET",
		"SHOW", "DISCARD", "DEALLOCATE")

	// Clauses
	reg(CatClause, CapFullSupport, "clause supported on ReadEngine",
		"DISTINCT", "DISTINCT_ON", "WHERE", "GROUP_BY", "HAVING", "ORDER_BY",
		"NULLS_FIRST", "NULLS_LAST", "LIMIT", "OFFSET", "FETCH",
		"UNION", "UNION_ALL", "INTERSECT", "EXCEPT", "FILTER", "RECURSIVE_CTE")
	reg(CatClause, CapUnsupported, "locking, DDL-like, or unsupported PG clause requires WriteEngine",
		"FOR_UPDATE", "FOR_SHARE", "FOR_NO_KEY_UPDATE", "FOR_KEY_SHARE",
		"SELECT_INTO", "MODIFYING_CTE", "LATERAL", "GROUPING_SETS", "ROLLUP", "CUBE",
		"TABLESAMPLE", "ON_CONFLICT", "RETURNING", "WINDOW_GROUPS")

	// Joins
	reg(CatJoin, CapFullSupport, "join type supported on ReadEngine",
		"INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL", "JOIN")
	reg(CatJoin, CapUnsupported, "LATERAL join requires WriteEngine",
		"LATERAL")

	// Window functions
	reg(CatWindow, CapFullSupport, "window function supported on ReadEngine",
		"ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "LAG", "LEAD",
		"FIRST_VALUE", "LAST_VALUE", "NTH_VALUE", "PERCENT_RANK", "CUME_DIST",
		"COUNT", "SUM", "AVG", "MIN", "MAX", "TOTAL")

	// Aggregates
	reg(CatAggregate, CapFullSupport, "aggregate supported on ReadEngine",
		"COUNT", "SUM", "AVG", "MIN", "MAX", "TOTAL",
		"GROUP_CONCAT", "STRING_AGG", "BOOL_AND", "BOOL_OR", "EVERY")
	reg(CatAggregate, CapPartialSupport, "PG-specific aggregate falls back to WriteEngine",
		"ARRAY_AGG", "JSON_AGG", "JSONB_AGG")
	reg(CatAggregate, CapUnsupported, "statistical/ordered-set aggregate requires WriteEngine",
		"PERCENTILE_CONT", "PERCENTILE_DISC", "MODE", "STDDEV", "STDDEV_POP", "STDDEV_SAMP",
		"VARIANCE", "VAR_POP", "VAR_SAMP", "CORR", "COVAR_POP", "COVAR_SAMP", "XMLAGG")

	// Operators
	reg(CatOperator, CapFullSupport, "operator supported on ReadEngine",
		"=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "||",
		"AND", "OR", "NOT", "IS_NULL", "IS_NOT_NULL",
		"IN", "NOT_IN", "BETWEEN", "NOT_BETWEEN", "LIKE", "NOT_LIKE", "ILIKE", "NOT_ILIKE",
		"~", "~*", "!~", "!~*", "->", "->>", "::")
	reg(CatOperator, CapPartialSupport, "PG-specific operator falls back to WriteEngine",
		"@>", "<@", "?", "?|", "?&", "@@", "@?", "SIMILAR_TO", "ANY", "ALL", "SOME", "OVERLAPS",
		"IS_DISTINCT_FROM", "IS_NOT_DISTINCT_FROM", "#>", "#>>")

	// Scalar Functions
	reg(CatFunction, CapFullSupport, "scalar function supported on ReadEngine",
		// Null/conditional
		"COALESCE", "NULLIF", "IFNULL", "IIF", "GREATEST", "LEAST",
		// String
		"LOWER", "UPPER", "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH", "OCTET_LENGTH",
		"SUBSTR", "SUBSTRING", "TRIM", "LTRIM", "RTRIM", "REPLACE",
		"CONCAT", "CONCAT_WS", "LEFT", "RIGHT", "SPLIT_PART", "STRPOS", "POSITION",
		"REPEAT", "REVERSE", "LPAD", "RPAD", "INITCAP", "INSTR", "PRINTF", "FORMAT",
		"CHAR", "UNICODE", "HEX", "UNHEX", "QUOTE",
		"MD5", "REGEXP_REPLACE", "TRANSLATE",
		// Math
		"ABS", "ROUND", "CEIL", "CEILING", "FLOOR", "TRUNC", "MOD", "POWER", "POW",
		"SQRT", "EXP", "LN", "LOG", "LOG10", "SIGN", "RANDOM", "PI",
		"SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "ATAN2", "DEGREES", "RADIANS",
		// Date/Time
		"NOW", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME",
		"DATE", "TIME", "DATETIME", "JULIANDAY", "UNIXEPOCH", "STRFTIME",
		"DATE_TRUNC", "DATE_PART", "EXTRACT", "TO_CHAR",
		// JSON
		"JSON", "JSON_ARRAY", "JSON_OBJECT", "JSON_EXTRACT", "JSON_TYPE", "JSON_VALID",
		"JSON_ARRAY_LENGTH", "JSON_GROUP_ARRAY", "JSON_GROUP_OBJECT",
		// Misc / Type / Utility
		"TYPEOF", "ZEROBLOB", "CHANGES", "TOTAL_CHANGES", "LAST_INSERT_ROWID",
		"GEN_RANDOM_UUID", "UUID_GENERATE_V4", "CURRENT_SCHEMA",
		// Window/Aggregate names that may also appear as function calls
		"ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "LAG", "LEAD",
		"FIRST_VALUE", "LAST_VALUE", "NTH_VALUE", "PERCENT_RANK", "CUME_DIST",
		"COUNT", "SUM", "AVG", "MIN", "MAX", "TOTAL",
		"GROUP_CONCAT", "STRING_AGG", "BOOL_AND", "BOOL_OR", "EVERY")
	reg(CatFunction, CapPartialSupport, "PG-specific function falls back to WriteEngine",
		"BTRIM", "ASCII", "CHR", "QUOTE_IDENT", "QUOTE_LITERAL", "ENCODE", "DECODE",
		"REGEXP_MATCH", "TO_DATE", "TO_TIMESTAMP", "AGE",
		"JSONB_EXTRACT_PATH", "JSONB_EXTRACT_PATH_TEXT", "JSONB_TYPEOF", "JSON_TYPEOF",
		"ARRAY_AGG", "JSON_AGG", "JSONB_AGG",
		"VERSION", "CURRENT_DATABASE", "CURRENT_USER", "SESSION_USER", "USER")
	reg(CatFunction, CapUnsupported, "mutating or backend-stateful function requires WriteEngine",
		"NEXTVAL", "SETVAL", "CURRVAL", "LASTVAL",
		"PG_ADVISORY_LOCK", "PG_ADVISORY_LOCK_SHARED",
		"PG_TRY_ADVISORY_LOCK", "PG_TRY_ADVISORY_LOCK_SHARED",
		"PG_ADVISORY_XACT_LOCK", "PG_ADVISORY_XACT_LOCK_SHARED",
		"PG_ADVISORY_UNLOCK", "PG_ADVISORY_UNLOCK_ALL",
		"LO_CREATE", "LO_IMPORT", "LO_UNLINK", "LO_EXPORT",
		"PG_RELOAD_CONF", "PG_ROTATE_LOGFILE", "PG_TERMINATE_BACKEND", "PG_CANCEL_BACKEND",
		"SET_CONFIG", "PG_SLEEP", "TXID_CURRENT", "PG_CURRENT_XACT_ID",
		"PG_CURRENT_WAL_LSN", "PG_GET_USERBYID", "PG_TABLE_IS_VISIBLE")

	// Data types & casts
	reg(CatDataType, CapFullSupport, "data type supported on ReadEngine",
		"INT", "INT2", "INT4", "INT8", "SMALLINT", "INTEGER", "BIGINT", "TINYINT", "MEDIUMINT",
		"NUMERIC", "DECIMAL", "REAL", "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE_PRECISION",
		"TEXT", "VARCHAR", "CHAR", "BPCHAR", "CHARACTER", "CHARACTER_VARYING", "NVARCHAR",
		"BOOL", "BOOLEAN", "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ",
		"TIMESTAMP_WITH_TIME_ZONE", "TIMESTAMP_WITHOUT_TIME_ZONE",
		"UUID", "JSON", "JSONB", "BYTEA", "BLOB")
	reg(CatDataType, CapPartialSupport, "complex PG type falls back to WriteEngine",
		"INTERVAL", "ENUM", "DOMAIN", "CIDR", "INET", "MACADDR", "XML", "MONEY",
		"BIT", "VARBIT", "TSVECTOR", "TSQUERY", "GEOMETRY", "POINT", "POLYGON", "ARRAY")

	reg(CatCast, CapFullSupport, "type cast supported on ReadEngine",
		"INT", "INT2", "INT4", "INT8", "SMALLINT", "INTEGER", "BIGINT",
		"NUMERIC", "DECIMAL", "REAL", "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE_PRECISION",
		"TEXT", "VARCHAR", "CHAR", "BPCHAR", "CHARACTER", "CHARACTER_VARYING",
		"BOOL", "BOOLEAN", "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ",
		"TIMESTAMP_WITH_TIME_ZONE", "TIMESTAMP_WITHOUT_TIME_ZONE",
		"UUID", "JSON", "JSONB", "BYTEA")
	reg(CatCast, CapPartialSupport, "complex PG cast falls back to WriteEngine",
		"INTERVAL", "CIDR", "INET", "MACADDR", "XML", "MONEY", "REGCLASS", "REGTYPE",
		"TSVECTOR", "TSQUERY", "ARRAY")

	return r
}

// Lookup returns the CapabilitySupport for a feature, or CapUnknown if unregistered.
func (r *CapabilityRegistry) Lookup(cat CapabilityCategory, name string) CapabilitySupport {
	if r == nil {
		return CapUnknown
	}
	upper := strings.ToUpper(strings.TrimSpace(name))
	if ent, ok := r.entries[capKey{cat: cat, name: upper}]; ok {
		return ent.Support
	}
	return CapUnknown
}

// LookupEntry returns the full CapabilityEntry if registered.
func (r *CapabilityRegistry) LookupEntry(cat CapabilityCategory, name string) (CapabilityEntry, bool) {
	if r == nil {
		return CapabilityEntry{}, false
	}
	upper := strings.ToUpper(strings.TrimSpace(name))
	ent, ok := r.entries[capKey{cat: cat, name: upper}]
	return ent, ok
}

// Evaluate checks all observed features against the registry.
// Priority of blockers: UNSUPPORTED > UNKNOWN > PARTIAL_SUPPORT > FULL_SUPPORT.
func (r *CapabilityRegistry) Evaluate(features []FeatureRef) CapabilityEvaluation {
	if r == nil {
		return CapabilityEvaluation{Support: CapUnknown, Reason: "nil capability registry"}
	}
	var firstPartial *FeatureRef
	var firstUnknown *FeatureRef
	var partialReason string

	for i := range features {
		f := features[i]
		ent, ok := r.entries[capKey{cat: f.Category, name: strings.ToUpper(strings.TrimSpace(f.Name))}]
		if !ok {
			if firstUnknown == nil {
				cp := f
				firstUnknown = &cp
			}
			continue
		}
		switch ent.Support {
		case CapUnsupported:
			cp := f
			return CapabilityEvaluation{
				Support: CapUnsupported,
				Reason:  ent.Reason,
				Blocker: &cp,
			}
		case CapUnknown:
			if firstUnknown == nil {
				cp := f
				firstUnknown = &cp
			}
		case CapPartialSupport:
			if firstPartial == nil {
				cp := f
				firstPartial = &cp
				partialReason = ent.Reason
			}
		}
	}

	if firstUnknown != nil {
		return CapabilityEvaluation{
			Support: CapUnknown,
			Reason:  "unregistered feature: " + firstUnknown.Name,
			Blocker: firstUnknown,
		}
	}
	if firstPartial != nil {
		return CapabilityEvaluation{
			Support: CapPartialSupport,
			Reason:  partialReason,
			Blocker: firstPartial,
		}
	}
	return CapabilityEvaluation{Support: CapFullSupport, Reason: "all features have FULL_SUPPORT"}
}

var defaultCapabilityRegistry = NewDefaultCapabilityRegistry()

// DefaultCapabilityRegistry returns the singleton default CapabilityRegistry.
func DefaultCapabilityRegistry() *CapabilityRegistry {
	return defaultCapabilityRegistry
}
