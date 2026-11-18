#include "admin_grpc.hpp"
#include "admin_http.hpp"
#include "config.hpp"
#include "discovery.hpp"
#include "metrics.hpp"
#include "pg_wire.hpp"
#include "pooler.hpp"

#include <arpa/inet.h>
#include <cassert>
#include <chrono>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <iostream>
#include <netinet/in.h>
#include <regex>
#include <sys/socket.h>
#include <thread>

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

    // 2. Tenant ID, SNI extraction, constant-time auth & CancelRequest
    {
        assert(validate_tenant_id("tenant_123-abc"));
        assert(!validate_tenant_id(""));
        assert(!validate_tenant_id("bad;drop"));
        assert(parse_sni_tenant("t_demo.air.local") == "t_demo");
        assert(parse_sni_tenant("localhost") == "");
        assert(parse_sni_tenant("127.0.0.1") == "");
        std::unordered_map<std::string, std::string> params{{"database", "postgres"}, {"user", "app_user"}};
        assert(extract_tenant("t_demo.air.local", params) == "t_demo");
        params["database"] = "t_explicit";
        assert(extract_tenant("t_demo.air.local", params) == "t_explicit");

        assert(constant_time_equals("secret123", "secret123"));
        assert(!constant_time_equals("secret123", "secret124"));
        assert(!constant_time_equals("secret123", "secret12"));

        auto creq = build_cancel_request(1234, 5678);
        assert(creq.size() == 16);
        assert(read_be32(creq.data()) == 16);
        assert(read_be32(creq.data() + 4) == kCancelRequestCode);
        assert(read_be32(creq.data() + 8) == 1234);
        assert(read_be32(creq.data() + 12) == 5678);

        CancelRegistry creg;
        creg.register_target(100, 200, CancelTarget{"127.0.0.1", "", 15432, 9001, 42});
        CancelTarget ct;
        assert(creg.lookup(100, 200, ct) && ct.backend_pid == 9001 && ct.backend_secret == 42);
        creg.unregister_target(100, 200);
        assert(!creg.lookup(100, 200, ct));
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
        (void)off;

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

        std::string s4 = "RESET work_mem;";
        assert(std::regex_match(s4, sm, sqlparse::re_reset_stmt()));
        assert(sm[1].str() == "work_mem");
        std::string s5 = "RESET ALL;";
        assert(std::regex_match(s5, sm, sqlparse::re_reset_stmt()));
        assert(sm[1].str() == "ALL");
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

    // 7. TenantDiscovery gRPC ListDatabases + WatchTenantChanges + BatchTouchActivity + shared_mutex cache
    {
        class MockControlService final : public air::db::v1::AirDBControl::Service {
        public:
            std::atomic<int> get_route_calls{0};
            std::mutex touch_mu;
            std::vector<std::string> batch_touched;

            grpc::Status ListDatabases(grpc::ServerContext*,
                                       const air::db::v1::ListDatabasesRequest*,
                                       air::db::v1::ListDatabasesResponse* resp) override {
                auto* d = resp->add_databases();
                d->set_tenant_id("t_init");
                d->set_state("ACTIVE");
                d->set_direct_port(15433);
                resp->set_snapshot_version(10);
                return grpc::Status::OK;
            }

            grpc::Status WatchTenantChanges(grpc::ServerContext* ctx,
                                            const air::db::v1::WatchTenantsRequest* req,
                                            grpc::ServerWriter<air::db::v1::TenantChangeEvent>* writer) override {
                if (req->after_version() != 10) {
                    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "unexpected after_version");
                }
                air::db::v1::TenantChangeEvent ev1;
                ev1.set_version(11);
                ev1.set_type(air::db::v1::TENANT_EVENT_UPSERT);
                ev1.mutable_route()->set_tenant_id("t_stream");
                ev1.mutable_route()->set_state("ACTIVE");
                ev1.mutable_route()->set_host("127.0.0.1");
                ev1.mutable_route()->set_port(15434);
                writer->Write(ev1);

                air::db::v1::TenantChangeEvent ev2;
                ev2.set_version(12);
                ev2.set_type(air::db::v1::TENANT_EVENT_DELETE);
                ev2.mutable_route()->set_tenant_id("t_init");
                writer->Write(ev2);

                while (!ctx->IsCancelled()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
                return grpc::Status::OK;
            }

            grpc::Status GetTenantRoute(grpc::ServerContext*,
                                        const air::db::v1::TenantRequest* req,
                                        air::db::v1::TenantRouteResponse* resp) override {
                get_route_calls.fetch_add(1);
                resp->set_tenant_id(req->tenant_id());
                resp->set_state("ACTIVE");
                resp->set_host("127.0.0.1");
                resp->set_port(15434);
                resp->set_app_password("secret_pw");
                return grpc::Status::OK;
            }

            grpc::Status BatchTouchActivity(grpc::ServerContext*,
                                            const air::db::v1::BatchTouchActivityRequest* req,
                                            air::db::v1::Empty*) override {
                std::lock_guard<std::mutex> lk(touch_mu);
                for (const auto& id : req->tenant_ids()) {
                    batch_touched.push_back(id);
                }
                return grpc::Status::OK;
            }
        };

        MockControlService svc;
        int bound_port = 0;
        grpc::ServerBuilder builder;
        builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MIN_POLLERS, 4);
        builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MAX_POLLERS, 8);
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &bound_port);
        builder.RegisterService(&svc);
        auto server = builder.BuildAndStart();
        auto require = [](bool cond, const char* msg) {
            if (!cond) {
                std::cerr << "FAIL: " << msg << std::endl;
                std::abort();
            }
        };
        require(server && bound_port > 0, "mock gRPC server started");

        Config cfg;
        cfg.grpc_addr = "127.0.0.1:" + std::to_string(bound_port);
        cfg.meta_dsn = "";
        cfg.backend_host = "127.0.0.1";

        TenantDiscovery disc(cfg);
        disc.start();

        // Wait for WatchTenantChanges to apply t_stream upsert and t_init delete.
        bool stream_applied = false;
        for (int i = 0; i < 200; ++i) {
            auto cached = disc.list_cached();
            if (cached.size() == 1 && cached[0].tenant_id == "t_stream" && cached[0].port == 15434) {
                stream_applied = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        require(stream_applied, "WatchTenantChanges applied upsert and delete");

        // First resolve fetches app_password via GetTenantRoute; second hits shared_mutex cache.
        TenantRoute r1, r2;
        std::string err;
        require(disc.resolve_tenant("t_stream", r1, err), "first resolve_tenant");
        require(r1.app_password == "secret_pw", "app_password populated");
        require(disc.resolve_tenant("t_stream", r2, err), "second resolve_tenant");
        require(r2.app_password == "secret_pw", "cached app_password");
        require(svc.get_route_calls.load() == 1, "second resolve_tenant used cache without RPC");

        // BatchTouchActivity delivers batched touches.
        disc.touch_activity("t_stream");
        disc.touch_activity("t_other");
        bool touched_ok = false;
        for (int i = 0; i < 200; ++i) {
            {
                std::lock_guard<std::mutex> lk(svc.touch_mu);
                if (svc.batch_touched.size() >= 2) {
                    touched_ok = true;
                    break;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (!touched_ok) {
            std::lock_guard<std::mutex> lk(svc.touch_mu);
            std::cerr << "batch_touched.size()=" << svc.batch_touched.size() << std::endl;
        }
        require(touched_ok, "BatchTouchActivity delivered batched tenant_ids");

        disc.stop();
        server->Shutdown();
    }

    // 8. AdminGRPCServer (DrainTenant + SetTenantLimits + x-internal-token auth)
    {
        auto require = [](bool cond, const char* msg) {
            if (!cond) {
                std::cerr << "FAIL: " << msg << std::endl;
                std::abort();
            }
        };
        PoolManager pools(8);
        pools.upsert_discovered(TenantRoute{"t_admin", "127.0.0.1", "", 15432, "pw", "ACTIVE"});
        MetricsRegistry metrics(100, 10, 5);
        AdminGRPCServer admin_grpc("127.0.0.1:0", "adm-tok", pools, metrics);
        int port = 0;
        require(admin_grpc.start(&port) && port > 0, "AdminGRPCServer started");

        auto ch = grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials());
        auto stub = ::air::db::v1::GatewayAdmin::NewStub(ch);

        // Unauthorized call fails with PERMISSION_DENIED
        {
            grpc::ClientContext ctx;
            ::air::db::v1::DrainTenantRequest req;
            req.set_tenant_id("t_admin");
            req.set_timeout_ms(500);
            ::air::db::v1::StatusResponse resp;
            auto st = stub->DrainTenant(&ctx, req, &resp);
            require(st.error_code() == grpc::StatusCode::PERMISSION_DENIED, "missing token denied");
        }

        // Authorized DrainTenant succeeds
        {
            grpc::ClientContext ctx;
            ctx.AddMetadata("x-internal-token", "adm-tok");
            ::air::db::v1::DrainTenantRequest req;
            req.set_tenant_id("t_admin");
            req.set_timeout_ms(500);
            ::air::db::v1::StatusResponse resp;
            auto st = stub->DrainTenant(&ctx, req, &resp);
            require(st.ok() && resp.status() == "drained" && resp.tenant_id() == "t_admin", "DrainTenant ok");
        }

        // Authorized SetTenantLimits updates metrics
        {
            grpc::ClientContext ctx;
            ctx.AddMetadata("x-internal-token", "adm-tok");
            ::air::db::v1::SetTenantLimitsRequest req;
            req.set_tenant_id("t_admin");
            req.set_max_client_conns(42);
            req.set_max_backends(6);
            ::air::db::v1::StatusResponse resp;
            auto st = stub->SetTenantLimits(&ctx, req, &resp);
            require(st.ok() && resp.status() == "updated", "SetTenantLimits ok");
            require(metrics.get_tenant_client_limit("t_admin") == 42, "tenant client limit updated");
        }

        admin_grpc.stop();
    }

    // 9. Protocol regression tests: fail-closed auth, unknown msg rejection, Flush ('H'), COPY FROM STDIN, and isolation
    {
        auto require = [](bool cond, const char* msg) {
            if (!cond) {
                std::cerr << "FAIL: " << msg << std::endl;
                std::abort();
            }
        };

        // 9a. Fail-closed when expected_pass is empty
        {
            int sv[2];
            require(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0, "socketpair 9a");
            TenantPool pool("t_auth", "127.0.0.1", "", 1, "", 4);
            MetricsRegistry reg(10, 5, 5);
            Stream srv(sv[0]);
            Stream cli(sv[1]);
            std::thread t([&]() {
                pool.serve_client(srv, "app_user", "", nullptr, &reg);
            });
            char mtype = 0;
            std::vector<uint8_t> mbody;
            require(read_pg_packet(cli, mtype, mbody) && mtype == 'E', "empty password rejected with ErrorResponse");
            cli.close();
            t.join();
            require(reg.to_json({}).find("\"auth_failures\":1") != std::string::npos, "auth_failures incremented");
        }

        // 9b. Wrong password rejected
        {
            int sv[2];
            require(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0, "socketpair 9b");
            TenantPool pool("t_auth", "127.0.0.1", "", 1, "correct_pw", 4);
            MetricsRegistry reg(10, 5, 5);
            Stream srv(sv[0]);
            Stream cli(sv[1]);
            std::thread t([&]() {
                pool.serve_client(srv, "app_user", "correct_pw", nullptr, &reg);
            });
            char mtype = 0;
            std::vector<uint8_t> mbody;
            require(read_pg_packet(cli, mtype, mbody) && mtype == 'R' && read_be32(mbody.data()) == 3, "cleartext auth requested");
            require(cli.write_all(build_password_message("wrong_pw")), "send wrong password");
            require(read_pg_packet(cli, mtype, mbody) && mtype == 'E', "wrong password rejected with ErrorResponse");
            cli.close();
            t.join();
            require(reg.to_json({}).find("\"auth_failures\":1") != std::string::npos, "wrong password incremented auth_failures");
        }

        // 9c. Full mock PG backend protocol test: Flush ('H'), COPY FROM STDIN, and unsupported message rejection
        {
            int lfd = listen_tcp_addr("127.0.0.1:0");
            require(lfd >= 0, "listen mock pg backend");
            struct sockaddr_in sin{};
            socklen_t slen = sizeof(sin);
            require(::getsockname(lfd, reinterpret_cast<sockaddr*>(&sin), &slen) == 0, "getsockname");
            int pg_port = ntohs(sin.sin_port);

            std::atomic<bool> backend_ok{true};
            std::thread pg_backend([&]() {
                int bfd = ::accept(lfd, nullptr, nullptr);
                if (bfd < 0) { backend_ok = false; return; }
                Stream bs(bfd);
                // Read startup
                uint8_t hdr[8];
                if (!bs.read_exact(hdr, 8)) { backend_ok = false; return; }
                uint32_t mlen = read_be32(hdr);
                std::vector<uint8_t> sbody(mlen - 8);
                if (!bs.read_exact(sbody.data(), sbody.size())) { backend_ok = false; return; }
                // Send AuthOk + BackendKeyData + ReadyForQuery
                bs.write_all(build_auth_ok());
                bs.write_all(build_backend_key_data(4321, 8765));
                bs.write_all(build_ready_for_query('I'));

                // 1) Expect Parse + Describe + Flush
                char t = 0;
                std::vector<uint8_t> b;
                if (!read_pg_packet(bs, t, b) || t != 'P') { backend_ok = false; return; }
                if (!read_pg_packet(bs, t, b) || t != 'D') { backend_ok = false; return; }
                if (!read_pg_packet(bs, t, b) || t != 'H') { backend_ok = false; return; }
                // Respond to Flush immediately without ReadyForQuery
                bs.write_all(build_pg_message('1', {})); // ParseComplete
                bs.write_all(build_pg_message('t', {0, 0})); // ParameterDescription (0 params)
                bs.write_all(build_pg_message('n', {})); // NoData

                // 2) Expect Sync ('S')
                if (!read_pg_packet(bs, t, b) || t != 'S') { backend_ok = false; return; }
                bs.write_all(build_ready_for_query('I'));

                // 2b) Pooler liveness ping Q("") on re-acquire for same client_id
                if (!read_pg_packet(bs, t, b) || t != 'Q') { backend_ok = false; return; }
                bs.write_all(build_pg_message('I', {}));
                bs.write_all(build_ready_for_query('I'));

                // 3) Expect SimpleQuery COPY FROM STDIN
                if (!read_pg_packet(bs, t, b) || t != 'Q') { backend_ok = false; return; }
                bs.write_all(build_pg_message('G', {0, 0, 1, 0, 0})); // CopyInResponse
                if (!read_pg_packet(bs, t, b) || t != 'd') { backend_ok = false; return; }
                if (!read_pg_packet(bs, t, b) || t != 'c') { backend_ok = false; return; }
                std::vector<uint8_t> cc{'C','O','P','Y',' ','1',0};
                bs.write_all(build_pg_message('C', cc));
                bs.write_all(build_ready_for_query('I'));
                ::close(lfd);
            });

            int sv[2];
            require(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0, "socketpair 9c");
            TenantPool pool("t_proto", "127.0.0.1", "", pg_port, "pw123", 4);
            MetricsRegistry reg(10, 5, 5);
            CancelRegistry creg;
            Stream srv(sv[0]);
            Stream cli(sv[1]);

            std::thread gw([&]() {
                pool.serve_client(srv, "app_user", "pw123", nullptr, &reg, &creg, 0);
            });

            char t = 0;
            std::vector<uint8_t> b;
            require(read_pg_packet(cli, t, b) && t == 'R', "auth req");
            require(cli.write_all(build_password_message("pw123")), "send password");
            // Drain startup messages until ReadyForQuery
            while (read_pg_packet(cli, t, b)) {
                if (t == 'Z') break;
            }
            require(t == 'Z', "client ReadyForQuery");

            // Unsupported message type ('?') returns ERROR 08P01 + ReadyForQuery
            require(cli.write_all(build_pg_message('?', {1, 2})), "send unsupported msg");
            require(read_pg_packet(cli, t, b) && t == 'E', "unsupported msg returns ErrorResponse");
            require(read_pg_packet(cli, t, b) && t == 'Z', "unsupported msg returns ReadyForQuery");

            // Parse + Describe + Flush ('H') must NOT deadlock
            require(cli.write_all(build_parse_message("s1", "SELECT 1", {})), "send Parse");
            std::vector<uint8_t> desc_body{'S'};
            append_cstr(desc_body, "s1");
            require(cli.write_all(build_pg_message('D', desc_body)), "send Describe");
            require(cli.write_all(build_pg_message('H', {})), "send Flush");

            require(read_pg_packet(cli, t, b) && t == '1', "Flush relayed ParseComplete");
            require(read_pg_packet(cli, t, b) && t == 't', "Flush relayed ParameterDescription");
            require(read_pg_packet(cli, t, b) && t == 'n', "Flush relayed NoData");

            // Sync
            require(cli.write_all(build_sync_message()), "send Sync");
            require(read_pg_packet(cli, t, b) && t == 'Z', "Sync relayed ReadyForQuery");

            // COPY FROM STDIN
            require(cli.write_all(build_simple_query("COPY t FROM STDIN")), "send COPY");
            require(read_pg_packet(cli, t, b) && t == 'G', "relayed CopyInResponse");
            std::vector<uint8_t> row_data{'4','2','\n'};
            require(cli.write_all(build_pg_message('d', row_data)), "send CopyData");
            require(cli.write_all(build_pg_message('c', {})), "send CopyDone");
            require(read_pg_packet(cli, t, b) && t == 'C', "relayed COPY CommandComplete");
            require(read_pg_packet(cli, t, b) && t == 'Z', "relayed COPY ReadyForQuery");

            require(cli.write_all(build_pg_message('X', {})), "send Terminate");
            cli.close();
            gw.join();
            pg_backend.join();
            require(backend_ok.load(), "mock pg backend protocol sequence ok");
            require(reg.to_json({}).find("\"protocol_errors\":1") != std::string::npos, "protocol_errors recorded");
        }
    }

    std::cout << "All C++ air-gateway tests passed!" << std::endl;
    return 0;
}
