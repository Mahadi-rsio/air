#include "config.hpp"
#include "discovery.hpp"
#include "metrics.hpp"
#include "pg_wire.hpp"
#include "pooler.hpp"

#include <cassert>
#include <iostream>
#include <regex>

int main() {
    using namespace air::gw;

    // 1. Config parsing
    {
        char arg0[] = "air-gateway";
        char arg1[] = "--gateway-addr";
        char arg2[] = "0.0.0.0:6432";
        char arg3[] = "--pool-max-backends";
        char arg4[] = "32";
        char arg5[] = "--max-client-conns";
        char arg6[] = "256";
        char arg7[] = "--max-tenant-client-conns";
        char arg8[] = "64";
        char arg9[] = "--max-user-client-conns";
        char arg10[] = "16";
        char* argv[] = {arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10};
        Config cfg = Config::load(11, argv);
        assert(cfg.gateway_addr == "0.0.0.0:6432");
        assert(cfg.pool_max_backends == 32);
        assert(cfg.max_client_conns == 256);
        assert(cfg.max_tenant_client_conns == 64);
        assert(cfg.max_user_client_conns == 16);
        assert(!cfg.meta_dsn.empty());
    }

    // 2. Tenant ID & SNI extraction
    {
        assert(validate_tenant_id("tenant_123-abc"));
        assert(!validate_tenant_id(""));
        assert(!validate_tenant_id("bad;drop"));
        assert(parse_sni_tenant("t_demo.air.local") == "t_demo");
        std::unordered_map<std::string, std::string> params{{"database", "postgres"}, {"user", "app_user"}};
        assert(extract_tenant("t_demo.air.local", params) == "t_demo");
        params["database"] = "t_explicit";
        assert(extract_tenant("t_demo.air.local", params) == "t_explicit");
    }

    // 3. Extended query statement rewriting
    {
        std::vector<uint8_t> parse_body;
        append_cstr(parse_body, "stmt1");
        append_cstr(parse_body, "SELECT $1::int");
        append_be16(parse_body, 1);
        append_be32(parse_body, 23);

        std::string cname, query;
        std::vector<uint32_t> oids;
        auto rewritten = sqlparse::rewrite_parse_body(parse_body, "_air_s_1_1", cname, query, oids);
        assert(cname == "stmt1");
        assert(query == "SELECT $1::int");
        assert(oids.size() == 1 && oids[0] == 23);

        size_t off = 0;
        assert(sqlparse::read_cstr(rewritten, off) == "_air_s_1_1");

        std::unordered_map<std::string, PreparedDef> stmts;
        stmts["stmt1"] = PreparedDef{"stmt1", "_air_s_1_1", "", query, oids, false};

        std::vector<uint8_t> bind_body;
        append_cstr(bind_body, "");
        append_cstr(bind_body, "stmt1");
        append_be16(bind_body, 0);
        auto rbind = sqlparse::rewrite_bind_body(bind_body, stmts);
        off = 0;
        assert(sqlparse::read_cstr(rbind, off) == "");
        assert(sqlparse::read_cstr(rbind, off) == "_air_s_1_1");

        std::vector<uint8_t> close_body{'S'};
        append_cstr(close_body, "stmt1");
        std::string erased;
        auto rclose = sqlparse::rewrite_named_op_body(close_body, stmts, true, erased);
        assert(erased == "stmt1");
        assert(rclose[0] == 'S');
        off = 1;
        assert(sqlparse::read_cstr(rclose, off) == "_air_s_1_1");
    }

    // 4. SQL regexes (SET, PREPARE, EXECUTE, DEALLOCATE, pinning)
    {
        std::smatch sm;
        std::string s1 = "SET search_path = public, app;";
        assert(std::regex_match(s1, sm, sqlparse::re_set_stmt()));
        assert(sm[1].str() == "search_path");

        std::string s2 = "PREPARE p1 (int) AS SELECT $1 + 1;";
        assert(std::regex_match(s2, sm, sqlparse::re_prepare_stmt()));
        assert(sm[1].str() == "p1");

        std::string s3 = "EXECUTE p1(41);";
        assert(std::regex_match(s3, sm, sqlparse::re_execute_stmt()));
        assert(sm[2].str() == "p1");

        assert(sqlparse::is_session_pinning_sql("LISTEN my_chan;"));
        assert(sqlparse::is_session_pinning_sql("CREATE TEMP TABLE t (x int);"));
        assert(sqlparse::is_session_pinning_sql("SELECT pg_advisory_lock(42);"));
        assert(!sqlparse::is_session_pinning_sql("SELECT 1;"));
    }

    // 5. PoolManager drain & TLSContext self-signed cert
    {
        PoolManager pm(8);
        TenantRoute r{"t_demo", "ACTIVE", "127.0.0.1", 15444, "", "pw"};
        auto pool = pm.get_or_create(r, nullptr);
        assert(pool != nullptr);
        std::string err;
        assert(pm.request_drain("t_demo", 100, err));

        TLSContext tls;
        std::string tls_err;
        assert(tls.init("", "", tls_err));
    }

    // 6. JSON route parsing
    {
        std::string js = R"({"tenant_id":"t_1","state":"ACTIVE","host":"127.0.0.1","port":15445,"sock_dir":"/tmp/s","app_password":"secret"})";
        TenantRoute r;
        assert(detail::parse_route_json(js, r));
        assert(r.tenant_id == "t_1");
        assert(r.state == "ACTIVE");
        assert(r.port == 15445);
        assert(r.app_password == "secret");
    }

    // 7. Connection limits & query performance metrics
    {
        MetricsRegistry reg(3, 2, 1);
        std::string err;

        // Global + tenant + user limits
        assert(reg.try_acquire_global());
        assert(reg.try_acquire_tenant_user("t_a", "alice", err));

        // Second connection for alice on t_a hits max_user_conns=1
        assert(reg.try_acquire_global());
        assert(!reg.try_acquire_tenant_user("t_a", "alice", err));
        assert(err.find("user \"alice\"") != std::string::npos);

        // bob on t_a succeeds (t_a now has 2 connections: alice + bob)
        assert(reg.try_acquire_tenant_user("t_a", "bob", err));

        // charlie on t_a hits max_tenant_conns=2
        assert(reg.try_acquire_global());
        assert(!reg.try_acquire_tenant_user("t_a", "charlie", err));
        assert(err.find("tenant \"t_a\"") != std::string::npos);

        // 4th global connection hits max_global_conns=3
        assert(!reg.try_acquire_global());

        // Dynamic per-tenant limit override
        reg.set_tenant_client_limit("t_a", 5);
        assert(reg.get_tenant_client_limit("t_a") == 5);
        assert(reg.try_acquire_tenant_user("t_a", "charlie", err));

        // Record queries (time, count, data size, pool wait, errors)
        reg.record_query("t_a", "alice", 1200, 150, 64, 512, 10, 400, false, 1);
        reg.record_query("t_a", "alice", 4800, 50, 128, 1024, 20, 800, true, 1);
        reg.record_query("t_a", "bob", 600, 0, 32, 128, 1, 64, false, 1);

        PoolManager pm(8);
        TenantRoute r{"t_a", "ACTIVE", "127.0.0.1", 15444, "", "pw"};
        pm.get_or_create(r, nullptr);
        pm.set_tenant_max_backends("t_a", 24);
        auto snaps = pm.snapshots();
        assert(snaps.size() == 1 && snaps[0].max_backends == 24);

        std::string js = reg.to_json(snaps);
        assert(js.find("\"query_count\":3") != std::string::npos);
        assert(js.find("\"error_count\":1") != std::string::npos);
        assert(js.find("\"bytes_in\":224") != std::string::npos);
        assert(js.find("\"bytes_out\":1664") != std::string::npos);
        assert(js.find("\"rows_returned\":31") != std::string::npos);
        assert(js.find("\"rejected_connections\":3") != std::string::npos);

        std::string prom = reg.to_prometheus(snaps);
        assert(prom.find("air_gateway_queries_total{tenant=\"t_a\",user=\"alice\"} 2") != std::string::npos);
        assert(prom.find("air_gateway_bytes_out_total{tenant=\"t_a\",user=\"bob\"} 128") != std::string::npos);

        reg.release_tenant_user("t_a", "alice");
        reg.release_tenant_user("t_a", "bob");
        reg.release_tenant_user("t_a", "charlie");
        reg.release_global();
        reg.release_global();
        reg.release_global();
    }

    std::cout << "All C++ air-gateway tests passed!" << std::endl;
    return 0;
}
