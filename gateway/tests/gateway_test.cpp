#include "config.hpp"
#include "discovery.hpp"
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
        char* argv[] = {arg0, arg1, arg2, arg3, arg4};
        Config cfg = Config::load(5, argv);
        assert(cfg.gateway_addr == "0.0.0.0:6432");
        assert(cfg.pool_max_backends == 32);
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

    std::cout << "All C++ air-gateway tests passed!" << std::endl;
    return 0;
}
