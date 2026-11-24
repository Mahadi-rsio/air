package gateway

import "testing"

func TestPhase31_SessionAndRoleRoutingSafety(t *testing.T) {
	sessionQueries := []string{
		"SET search_path = app, public",
		"SET TIME ZONE 'UTC'",
		"SET ROLE restricted_user",
		"SET SESSION AUTHORIZATION bob",
		"RESET work_mem",
		"RESET ALL",
		"SHOW search_path",
		"SHOW TimeZone",
		"DISCARD ALL",
		"DEALLOCATE stmt1",
		"PREPARE p1 AS SELECT 1",
		"EXECUTE p1",
		"SET search_path = app; SELECT id FROM items",
		"SELECT 1; DISCARD ALL",
	}
	for _, q := range sessionQueries {
		d, _ := ClassifySQLWithHint(q)
		if d.Target != RouteTargetWriteEngine {
			t.Fatalf("expected WriteEngine for session/utility query %q, got %v (reason=%v)", q, d.Target, d.Reason)
		}
	}

	// Role/auth changes must pin the session so subsequent reads never bypass PG RLS/GRANTs.
	pinning := []string{
		"SET ROLE app_reader",
		"SET LOCAL ROLE app_reader",
		"SET SESSION AUTHORIZATION app_user",
		"RESET ROLE",
		"RESET SESSION AUTHORIZATION",
	}
	for _, q := range pinning {
		if !IsSessionPinningSQL(q) {
			t.Fatalf("expected IsSessionPinningSQL(%q) == true", q)
		}
	}
}

func TestPhase31_UnsupportedTypesCastsAndFunctionsFallback(t *testing.T) {
	fallbackQueries := []string{
		// Unknown / custom / domain / enum / schema-qualified casts
		"SELECT 'active'::status_enum FROM users",
		"SELECT '123'::public.pos_int FROM items",
		"SELECT val::my_schema.custom_domain FROM items",
		"SELECT '{1,2,3}'::int[] FROM items",
		"SELECT id::pg_catalog.int4 FROM items",
		// Schema-qualified UDFs must not masquerade as built-ins
		"SELECT my_schema.lower(name) FROM users",
		// Unimplemented PG functions moved out of FULL_SUPPORT
		"SELECT btrim(name) FROM users",
		"SELECT array_agg(id) FROM items",
		"SELECT json_agg(id) FROM items",
		"SELECT jsonb_agg(id) FROM items",
		"SELECT regexp_match(name, 'a+') FROM users",
		"SELECT to_date('2026-01-01', 'YYYY-MM-DD')",
		"SELECT version()",
		"SELECT current_user",
		// Unsupported PG operators
		"SELECT a IS DISTINCT FROM b FROM items",
		"SELECT a IS NOT DISTINCT FROM b FROM items",
		"SELECT payload #> '{a,b}' FROM items",
		"SELECT payload #>> '{a,b}' FROM items",
	}
	for _, q := range fallbackQueries {
		d, _ := ClassifySQLWithHint(q)
		if d.Target != RouteTargetWriteEngine {
			t.Fatalf("expected WriteEngine fallback for %q, got %v (reason=%v)", q, d.Target, d.Reason)
		}
	}
}
