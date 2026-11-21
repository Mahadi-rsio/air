#include "pooler.hpp"

#include <iostream>
#include <openssl/rand.h>
#include <thread>

namespace air::gw {

namespace sqlparse {

std::vector<uint8_t> make_parse_body(const std::string& stmt_name,
                                     const std::string& query,
                                     const std::vector<uint32_t>& oids) {
    std::vector<uint8_t> out;
    append_cstr(out, stmt_name);
    append_cstr(out, query);
    append_be16(out, static_cast<uint16_t>(oids.size()));
    for (uint32_t oid : oids) append_be32(out, oid);
    return out;
}

std::vector<uint8_t> rewrite_parse_body(const std::vector<uint8_t>& body,
                                        const std::string& global_name,
                                        std::string& client_name_out,
                                        std::string& query_out,
                                        std::vector<uint32_t>& oids_out) {
    size_t off = 0;
    client_name_out = read_cstr(body, off);
    query_out = read_cstr(body, off);
    oids_out.clear();
    if (off + 2 <= body.size()) {
        uint16_t n = read_be16(body.data() + off);
        off += 2;
        for (uint16_t i = 0; i < n && off + 4 <= body.size(); ++i) {
            oids_out.push_back(read_be32(body.data() + off));
            off += 4;
        }
    }
    return make_parse_body(global_name, query_out, oids_out);
}

std::vector<uint8_t> rewrite_bind_body(const std::vector<uint8_t>& body,
                                       const std::unordered_map<std::string, PreparedDef>& stmts) {
    size_t off = 0;
    std::string portal = read_cstr(body, off);
    std::string stmt_name = read_cstr(body, off);
    size_t tail_off = off;

    auto it = stmts.find(stmt_name);
    std::string target = (it != stmts.end()) ? it->second.global_name : stmt_name;

    std::vector<uint8_t> out;
    append_cstr(out, portal);
    append_cstr(out, target);
    if (tail_off < body.size()) {
        out.insert(out.end(), body.begin() + static_cast<ptrdiff_t>(tail_off), body.end());
    }
    return out;
}

std::vector<uint8_t> rewrite_named_op_body(const std::vector<uint8_t>& body,
                                           const std::unordered_map<std::string, PreparedDef>& stmts,
                                           bool is_close,
                                           std::string& erased_client_name_out) {
    if (body.empty()) return body;
    char kind = static_cast<char>(body[0]);
    if (kind != 'S') return body;
    size_t off = 1;
    std::string name = read_cstr(body, off);
    size_t tail_off = off;

    auto it = stmts.find(name);
    std::string target = (it != stmts.end()) ? it->second.global_name : name;
    if (is_close && it != stmts.end()) {
        erased_client_name_out = name;
    }

    std::vector<uint8_t> out;
    out.push_back('S');
    append_cstr(out, target);
    if (tail_off < body.size()) {
        out.insert(out.end(), body.begin() + static_cast<ptrdiff_t>(tail_off), body.end());
    }
    return out;
}

} // namespace sqlparse

struct TenantPool::ClientSessionState {
    Stream& client;
    std::string startup_user;
    uint64_t client_id = 0;
    uint32_t gw_pid = 0;
    uint32_t gw_secret = 0;
    MetricsRegistry* metrics = nullptr;
    CancelRegistry* cancel_reg = nullptr;

    std::unordered_map<std::string, std::string> session_vars;
    std::unordered_map<std::string, PreparedDef> client_stmts;
    std::unique_ptr<BackendConn> current_backend;
    char tx_status = 'I';
    bool in_tx_counted = false;
    bool session_pinned = false;
    bool backend_in_flight = false;
    uint64_t session_write_lsn = 0;
};

TenantPool::TenantPool(std::string tenant_id, std::string host, std::string sock_dir,
                       int port, std::string app_password, int max_backends,
                       int idle_timeout_sec)
    : tenant_id_(std::move(tenant_id)),
      max_backends_(max_backends > 0 ? max_backends : 16),
      idle_timeout_sec_(idle_timeout_sec > 0 ? idle_timeout_sec : 300),
      host_(std::move(host)),
      sock_dir_(std::move(sock_dir)),
      port_(port),
      app_password_(std::move(app_password)) {}

TenantPool::~TenantPool() { close_all_idle(); }

void TenantPool::set_idle_timeout_sec(int sec) {
    if (sec > 0) idle_timeout_sec_.store(sec, std::memory_order_relaxed);
}

size_t TenantPool::evict_expired_idle() {
    auto now = std::chrono::steady_clock::now();
    int timeout_s = idle_timeout_sec_.load(std::memory_order_relaxed);
    std::vector<std::unique_ptr<BackendConn>> to_close;
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto it = idle_pool_.begin(); it != idle_pool_.end();) {
            if (!*it || std::chrono::duration_cast<std::chrono::seconds>(now - (*it)->last_used).count() >= timeout_s) {
                to_close.push_back(std::move(*it));
                it = idle_pool_.erase(it);
            } else {
                ++it;
            }
        }
    }
    for (auto& b : to_close) {
        if (b) b->close();
    }
    return to_close.size();
}

void TenantPool::set_wake_fn(WakeFn fn) {
    std::lock_guard<std::mutex> lk(mu_);
    wake_fn_ = std::move(fn);
}

void TenantPool::update_endpoint(const std::string& host, const std::string& sock_dir,
                                 int port, const std::string& app_password) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!app_password.empty()) {
        app_password_ = app_password;
    }
    if (port > 0 && (host_ != host || port_ != port || sock_dir_ != sock_dir)) {
        for (auto& b : idle_pool_) {
            if (b) b->close();
        }
        idle_pool_.clear();
        host_ = host;
        sock_dir_ = sock_dir;
        port_ = port;
    }
}

void TenantPool::set_max_backends(int max_backends) {
    if (max_backends <= 0) return;
    {
        std::lock_guard<std::mutex> lk(sem_mu_);
        max_backends_ = max_backends;
    }
    sem_cv_.notify_all();
}

PoolTenantSnapshot TenantPool::snapshot() {
    PoolTenantSnapshot snap;
    snap.tenant_id = tenant_id_;
    {
        std::lock_guard<std::mutex> lk(sem_mu_);
        snap.active_backends = active_backends_;
        snap.max_backends = max_backends_;
    }
    {
        std::lock_guard<std::mutex> lk(mu_);
        snap.idle_backends = idle_pool_.size();
    }
    snap.pinned_clients = pinned_clients_.load();
    snap.active_transactions = active_tx_count_.load();
    return snap;
}

void TenantPool::close_all_idle() {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& b : idle_pool_) {
        if (b) b->close();
    }
    idle_pool_.clear();
}

bool TenantPool::request_drain(int timeout_ms, std::string& err_out) {
    if (pinned_clients_.load() > 0) {
        err_out = "drain vetoed: " + std::to_string(pinned_clients_.load()) + " pinned client session(s)";
        return false;
    }
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (active_tx_count_.load() > 0) {
        if (std::chrono::steady_clock::now() >= deadline) {
            err_out = "drain vetoed: " + std::to_string(active_tx_count_.load()) + " active transaction(s) exceeded drain timeout";
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(15));
    }
    if (pinned_clients_.load() > 0) {
        err_out = "drain vetoed: " + std::to_string(pinned_clients_.load()) + " pinned client session(s)";
        return false;
    }
    close_all_idle();
    return true;
}

bool TenantPool::authenticate_client(Stream& client,
                                     const std::string& startup_user,
                                     const std::string& expected_pass,
                                     uint32_t gw_pid,
                                     uint32_t gw_secret,
                                     MetricsRegistry* metrics) {
    if (expected_pass.empty()) {
        if (metrics) metrics->record_auth_failure();
        std::cerr << "[air-gateway] auth rejected: missing credentials for tenant=" << tenant_id_ << std::endl;
        client.write_all(build_error_response("FATAL", "28000", "tenant credentials unavailable"));
        return false;
    }

    if (!client.write_all(build_auth_cleartext())) return false;

    char mtype = 0;
    std::vector<uint8_t> mbody;
    if (!read_pg_packet(client, mtype, mbody) || mtype != 'p') {
        if (metrics) metrics->record_auth_failure();
        client.write_all(build_error_response("FATAL", "28P01", "password authentication failed"));
        return false;
    }
    size_t off = 0;
    std::string got_pass = sqlparse::read_cstr(mbody, off);
    if (!constant_time_equals(got_pass, expected_pass)) {
        if (metrics) metrics->record_auth_failure();
        std::cerr << "[air-gateway] auth failed for tenant=" << tenant_id_
                  << " user=" << startup_user << std::endl;
        client.write_all(build_error_response(
            "FATAL", "28P01", "password authentication failed for user \"" + startup_user + "\""));
        return false;
    }

    return client.write_all(build_auth_ok()) &&
           client.write_all(build_param_status("server_version", "17.12")) &&
           client.write_all(build_param_status("client_encoding", "UTF8")) &&
           client.write_all(build_param_status("DateStyle", "ISO, MDY")) &&
           client.write_all(build_backend_key_data(gw_pid, gw_secret)) &&
           client.write_all(build_ready_for_query('I'));
}

void TenantPool::release_current(ClientSessionState& st, bool dirty) {
    if (st.cancel_reg) st.cancel_reg->unregister_target(st.gw_pid, st.gw_secret);
    st.backend_in_flight = false;
    if (st.current_backend) {
        release_backend(std::move(st.current_backend), dirty);
    }
}

void TenantPool::update_tx_count(ClientSessionState& st) {
    if (st.tx_status != 'I' && !st.in_tx_counted) {
        st.in_tx_counted = true;
        active_tx_count_.fetch_add(1);
    } else if (st.tx_status == 'I' && st.in_tx_counted) {
        st.in_tx_counted = false;
        active_tx_count_.fetch_sub(1);
    }
}

void TenantPool::record_query_metrics(ClientSessionState& st,
                                      std::chrono::steady_clock::time_point q_start,
                                      uint64_t pool_wait_us,
                                      uint64_t bytes_in,
                                      const RelayStats& rstats,
                                      bool had_err,
                                      uint64_t count) {
    if (!st.metrics) return;
    uint64_t dur_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - q_start)
            .count());
    st.metrics->record_query(tenant_id_, st.startup_user, dur_us, pool_wait_us,
                             bytes_in, rstats.bytes_out, rstats.rows_returned,
                             rstats.data_row_bytes, had_err, count);
}

bool TenantPool::ensure_session_and_stmts(BackendConn* b, ClientSessionState& st) {
    if (!st.session_vars.empty()) {
        std::string sql;
        for (const auto& [k, v] : st.session_vars) {
            sql += "SET " + k + " = " + v + "; ";
        }
        if (!exec_simple_backend(b, sql)) return false;
    }
    for (const auto& [_, def] : st.client_stmts) {
        if (b->prepared_stmts.count(def.global_name)) continue;
        if (def.is_sql_prep) {
            std::string prep_sql = "PREPARE " + def.global_name + def.type_list + " AS " + def.query;
            if (!exec_simple_backend(b, prep_sql)) return false;
        } else {
            if (!b->stream.write_all(build_parse_message(def.global_name, def.query, def.param_oids))) return false;
            if (!b->stream.write_all(build_sync_message())) return false;
            if (!drain_until_ready_backend(b)) return false;
        }
        b->prepared_stmts.insert(def.global_name);
    }
    return true;
}

bool TenantPool::ensure_backend(ClientSessionState& st, uint64_t& pool_wait_us_out) {
    pool_wait_us_out = 0;
    if (st.current_backend) return true;

    auto wait_start = std::chrono::steady_clock::now();
    std::string acq_err;
    st.current_backend = acquire_backend(st.client_id, acq_err);
    pool_wait_us_out = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - wait_start)
            .count());
    if (!st.current_backend) {
        if (st.metrics) st.metrics->record_pool_exhausted();
        st.client.write_all(build_error_response("ERROR", "53300", acq_err));
        st.client.write_all(build_ready_for_query(st.tx_status));
        return false;
    }
    if (!ensure_session_and_stmts(st.current_backend.get(), st)) {
        release_current(st, true);
        st.client.write_all(build_error_response("ERROR", "08006", "failed to prepare pooled backend session state"));
        st.client.write_all(build_ready_for_query(st.tx_status));
        return false;
    }
    if (st.cancel_reg && st.current_backend->backend_pid != 0) {
        std::lock_guard<std::mutex> lk(mu_);
        st.cancel_reg->register_target(st.gw_pid, st.gw_secret, CancelTarget{
            host_, sock_dir_, port_, st.current_backend->backend_pid, st.current_backend->backend_secret});
    }
    return true;
}

bool TenantPool::execute_simple_and_relay(ClientSessionState& st,
                                          const std::vector<uint8_t>& wire_msg,
                                          uint64_t bytes_in,
                                          std::chrono::steady_clock::time_point q_start,
                                          bool& had_err_out,
                                          bool& io_ok_out) {
    had_err_out = false;
    io_ok_out = true;
    uint64_t pool_wait_us = 0;
    if (!ensure_backend(st, pool_wait_us)) {
        record_query_metrics(st, q_start, pool_wait_us, bytes_in, {}, true, 1);
        return false;
    }
    st.backend_in_flight = true;
    if (!st.current_backend->stream.write_all(wire_msg)) {
        release_current(st, true);
        io_ok_out = false;
        return false;
    }
    RelayStats rstats;
    if (!relay_until_ready(st.current_backend.get(), st.client, st.tx_status, had_err_out, &rstats)) {
        release_current(st, true);
        io_ok_out = false;
        return false;
    }
    st.backend_in_flight = false;
    record_query_metrics(st, q_start, pool_wait_us, bytes_in, rstats, had_err_out, 1);
    update_tx_count(st);
    return true;
}

bool TenantPool::try_execute_on_read_engine(ClientSessionState& st,
                                            const std::vector<uint8_t>& wire_msg,
                                            uint64_t bytes_in,
                                            std::chrono::steady_clock::time_point q_start,
                                            bool& executed_out,
                                            bool& io_ok_out) {
    executed_out = false;
    io_ok_out = true;
    auto wait_start = std::chrono::steady_clock::now();
    std::string err;
    auto rb = acquire_read_backend(st.client_id, err);
    uint64_t pool_wait_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - wait_start)
            .count());
    if (!rb) {
        return true; // Fall back to WriteEngine.
    }
    if (!ensure_session_and_stmts(rb.get(), st)) {
        release_read_backend(std::move(rb), true);
        return true; // Fall back to WriteEngine.
    }
    if (!rb->stream.write_all(wire_msg)) {
        release_read_backend(std::move(rb), true);
        return true; // Fall back to WriteEngine.
    }
    bool had_err = false;
    RelayStats rstats;
    if (!relay_until_ready(rb.get(), st.client, st.tx_status, had_err, &rstats)) {
        release_read_backend(std::move(rb), true);
        io_ok_out = false;
        return false;
    }
    release_read_backend(std::move(rb), had_err || st.tx_status != 'I');
    record_query_metrics(st, q_start, pool_wait_us, bytes_in, rstats, had_err, 1);
    executed_out = true;
    return true;
}

bool TenantPool::handle_simple_query(ClientSessionState& st, const std::vector<uint8_t>& mbody) {
    auto q_start = std::chrono::steady_clock::now();
    uint64_t bytes_in = 5 + mbody.size();

    size_t off = 0;
    std::string q = sqlparse::read_cstr(mbody, off);
    std::string trimmed = trim_ascii(q);

    if (sqlparse::is_session_pinning_sql(trimmed) && !st.session_pinned) {
        st.session_pinned = true;
        pinned_clients_.fetch_add(1);
    }

    std::smatch sm;
    bool had_err = false;
    bool io_ok = true;

    if (std::regex_match(trimmed, sm, sqlparse::re_set_stmt())) {
        std::string key = sm[1].str();
        std::string val = sm[2].str();
        if (!execute_simple_and_relay(st, build_pg_message('Q', mbody), bytes_in, q_start, had_err, io_ok)) {
            return io_ok;
        }
        if (!had_err) st.session_vars[key] = val;
    } else if (std::regex_match(trimmed, sm, sqlparse::re_reset_stmt())) {
        std::string key = sm[1].str();
        if (!execute_simple_and_relay(st, build_pg_message('Q', mbody), bytes_in, q_start, had_err, io_ok)) {
            return io_ok;
        }
        if (!had_err) {
            if (iequals(key, "ALL")) st.session_vars.clear();
            else st.session_vars.erase(key);
        }
    } else if (std::regex_match(trimmed, sm, sqlparse::re_prepare_stmt())) {
        std::string client_name = sm[1].str();
        std::string type_list = sm[2].str();
        std::string inner_q = sm[3].str();
        std::string global_name = "_air_s_" + std::to_string(st.client_id) + "_" +
                                  std::to_string(stmt_seq_.fetch_add(1) + 1);
        std::string rewritten = "PREPARE " + global_name + type_list + " AS " + inner_q;
        if (!execute_simple_and_relay(st, build_simple_query(rewritten), bytes_in, q_start, had_err, io_ok)) {
            return io_ok;
        }
        if (!had_err) {
            PreparedDef def;
            def.client_name = client_name;
            def.global_name = global_name;
            def.type_list = type_list;
            def.query = inner_q;
            def.is_sql_prep = true;
            st.client_stmts[client_name] = std::move(def);
            st.current_backend->prepared_stmts.insert(global_name);
        }
    } else if (std::regex_match(trimmed, sm, sqlparse::re_dealloc_stmt())) {
        std::string cname = sm[1].str();
        std::string dealloc_sql;
        std::string erased_global;
        bool dealloc_all = iequals(cname, "ALL");
        if (dealloc_all) {
            dealloc_sql = "DEALLOCATE ALL";
        } else if (auto it = st.client_stmts.find(cname); it != st.client_stmts.end()) {
            erased_global = it->second.global_name;
            dealloc_sql = "DEALLOCATE " + erased_global;
        } else {
            // Forward unknown name so PostgreSQL returns SQLSTATE 26000 and updates tx state.
            dealloc_sql = "DEALLOCATE " + cname;
        }
        if (!execute_simple_and_relay(st, build_simple_query(dealloc_sql), bytes_in, q_start, had_err, io_ok)) {
            return io_ok;
        }
        if (!had_err) {
            if (dealloc_all) {
                st.client_stmts.clear();
                st.current_backend->prepared_stmts.clear();
            } else if (!erased_global.empty()) {
                st.client_stmts.erase(cname);
                st.current_backend->prepared_stmts.erase(erased_global);
            }
        }
    } else {
        std::vector<uint8_t> out_body = mbody;
        bool is_exec_stmt = false;
        if (std::regex_match(trimmed, sm, sqlparse::re_execute_stmt())) {
            is_exec_stmt = true;
            auto it = st.client_stmts.find(sm[2].str());
            if (it != st.client_stmts.end()) {
                std::string rewritten = sm[1].str() + it->second.global_name + sm[3].str();
                out_body.clear();
                append_cstr(out_body, rewritten);
            }
        }
        bool routed_to_read = false;
        int rport = 0;
        {
            std::lock_guard<std::mutex> lk(mu_);
            rport = read_port_;
        }
        if (!is_exec_stmt && !st.session_pinned && st.tx_status == 'I' && !st.current_backend && rport > 0) {
            uint64_t rlsn = read_engine_lsn_.load(std::memory_order_relaxed);
            if (st.session_write_lsn > 0 && rlsn < st.session_write_lsn) {
                std::string lerr;
                if (auto rb = acquire_read_backend(st.client_id, lerr)) {
                    uint64_t fresh = 0;
                    if (query_lsn_backend(rb.get(),
                            "SELECT COALESCE(pg_last_wal_replay_lsn(), pg_current_wal_lsn())::text", fresh)) {
                        rlsn = fresh;
                        read_engine_lsn_.store(fresh, std::memory_order_relaxed);
                    }
                    read_engine_healthy_.store(true, std::memory_order_relaxed);
                    release_read_backend(std::move(rb), false);
                } else {
                    read_engine_healthy_.store(false, std::memory_order_relaxed);
                }
            }
            RoutingContext rctx;
            rctx.session_pinned = st.session_pinned;
            rctx.in_transaction = (st.tx_status != 'I');
            rctx.read_engine_healthy = read_engine_healthy_.load(std::memory_order_relaxed);
            rctx.session_write_lsn = st.session_write_lsn;
            rctx.read_engine_lsn = rlsn;
            RouteDecision rdec = route_query(trimmed, rctx, &routing_metrics_);
            if (rdec.target == RouteTarget::ReadEngine) {
                if (!try_execute_on_read_engine(st, build_pg_message('Q', out_body),
                                                bytes_in, q_start, routed_to_read, io_ok)) {
                    return io_ok;
                }
                if (!routed_to_read) {
                    record_fallback_to_write(&routing_metrics_);
                }
            }
        }
        if (!routed_to_read) {
            if (!execute_simple_and_relay(st, build_pg_message('Q', out_body), bytes_in, q_start, had_err, io_ok)) {
                return io_ok;
            }
            if (rport > 0 && !had_err && st.tx_status == 'I' && st.current_backend &&
                classify_sql_for_routing(trimmed).target == RouteTarget::WriteEngine) {
                uint64_t wlsn = 0;
                if (query_lsn_backend(st.current_backend.get(), "SELECT pg_current_wal_lsn()::text", wlsn) &&
                    wlsn > st.session_write_lsn) {
                    st.session_write_lsn = wlsn;
                }
            }
        }
    }

    if (st.tx_status == 'I' && !st.session_pinned) {
        release_current(st, false);
    }
    return true;
}

bool TenantPool::handle_extended_batch(ClientSessionState& st,
                                       char terminator_type,
                                       std::vector<uint8_t> terminator_body,
                                       std::vector<std::pair<char, std::vector<uint8_t>>>& ext_batch,
                                       size_t& ext_batch_bytes) {
    auto q_start = std::chrono::steady_clock::now();
    ext_batch.emplace_back(terminator_type, std::move(terminator_body));
    uint64_t bytes_in = 0;
    uint64_t exec_count = 0;
    for (const auto& [bt, bb] : ext_batch) {
        bytes_in += 5 + bb.size();
        if (bt == 'E') ++exec_count;
    }
    if (exec_count == 0) exec_count = 1;

    int rport = 0;
    {
        std::lock_guard<std::mutex> lk(mu_);
        rport = read_port_;
    }

    // Check if this Sync batch is purely read-only and eligible for the Shared Read Engine.
    bool batch_has_write = false;
    bool try_read_engine = (rport > 0 && terminator_type == 'S' && !st.session_pinned &&
                            st.tx_status == 'I' && !st.current_backend);
    bool saw_routable = false;
    if (rport > 0) {
        uint64_t rlsn = read_engine_lsn_.load(std::memory_order_relaxed);
        if (try_read_engine && st.session_write_lsn > 0 && rlsn < st.session_write_lsn) {
            std::string lerr;
            if (auto rb = acquire_read_backend(st.client_id, lerr)) {
                uint64_t fresh = 0;
                if (query_lsn_backend(rb.get(),
                        "SELECT COALESCE(pg_last_wal_replay_lsn(), pg_current_wal_lsn())::text", fresh)) {
                    rlsn = fresh;
                    read_engine_lsn_.store(fresh, std::memory_order_relaxed);
                }
                read_engine_healthy_.store(true, std::memory_order_relaxed);
                release_read_backend(std::move(rb), false);
            } else {
                read_engine_healthy_.store(false, std::memory_order_relaxed);
            }
        }
        RoutingContext rctx;
        rctx.session_pinned = st.session_pinned;
        rctx.in_transaction = (st.tx_status != 'I');
        rctx.read_engine_healthy = read_engine_healthy_.load(std::memory_order_relaxed);
        rctx.session_write_lsn = st.session_write_lsn;
        rctx.read_engine_lsn = rlsn;

        for (const auto& [bt, bb] : ext_batch) {
            if (bt == 'P') {
                size_t off = 0;
                (void)sqlparse::read_cstr(bb, off); // stmt name
                std::string pq = sqlparse::read_cstr(bb, off);
                if (sqlparse::is_session_pinning_sql(pq)) {
                    try_read_engine = false;
                }
                if (classify_sql_for_routing(pq).target == RouteTarget::WriteEngine) {
                    batch_has_write = true;
                    try_read_engine = false;
                } else if (try_read_engine) {
                    saw_routable = true;
                    if (route_query(pq, rctx, &routing_metrics_).target != RouteTarget::ReadEngine) {
                        try_read_engine = false;
                    }
                }
            } else if (bt == 'B') {
                size_t off = 0;
                (void)sqlparse::read_cstr(bb, off); // portal
                std::string sname = sqlparse::read_cstr(bb, off);
                if (auto it = st.client_stmts.find(sname); it != st.client_stmts.end()) {
                    if (classify_sql_for_routing(it->second.query).target == RouteTarget::WriteEngine) {
                        batch_has_write = true;
                        try_read_engine = false;
                    } else if (try_read_engine) {
                        saw_routable = true;
                        if (route_query(it->second.query, rctx, &routing_metrics_).target != RouteTarget::ReadEngine) {
                            try_read_engine = false;
                        }
                    }
                }
            }
        }
        if (!saw_routable) try_read_engine = false;
    }

    auto run_batch_on_backend = [&](BackendConn* target_b, bool is_read_backend, uint64_t wait_us) -> int {
        // Returns: 1 = handled (success or client error), 0 = backend failed before client output (fallback), -1 = fatal IO error
        struct UndoParse {
            std::string cname;
            std::string gname;
            bool had_prev = false;
            PreparedDef prev;
        };
        std::vector<UndoParse> undo_parses;
        std::vector<std::pair<char, std::vector<uint8_t>>> prepared_msgs;
        prepared_msgs.reserve(ext_batch.size());

        for (const auto& [bt, orig_bb] : ext_batch) {
            std::vector<uint8_t> bb = orig_bb;
            if (bt == 'P') {
                std::string cname, query;
                std::vector<uint32_t> oids;
                std::string gname = "_air_s_" + std::to_string(st.client_id) + "_" +
                                    std::to_string(stmt_seq_.fetch_add(1) + 1);
                auto rewritten = sqlparse::rewrite_parse_body(bb, gname, cname, query, oids);
                std::smatch psm;
                std::string trimmed_q = trim_ascii(query);

                if (std::regex_match(trimmed_q, psm, sqlparse::re_prepare_stmt())) {
                    std::string sql_cname = psm[1].str();
                    std::string sql_tlist = psm[2].str();
                    std::string sql_inner = psm[3].str();
                    std::string sql_gname = "_air_s_" + std::to_string(st.client_id) + "_" +
                                            std::to_string(stmt_seq_.fetch_add(1) + 1);
                    query = "PREPARE " + sql_gname + sql_tlist + " AS " + sql_inner;
                    rewritten = sqlparse::make_parse_body(gname, query, oids);
                    PreparedDef sdef;
                    sdef.client_name = sql_cname;
                    sdef.global_name = sql_gname;
                    sdef.type_list = sql_tlist;
                    sdef.query = sql_inner;
                    sdef.is_sql_prep = true;
                    st.client_stmts[sql_cname] = std::move(sdef);
                    target_b->prepared_stmts.insert(sql_gname);
                } else if (std::regex_match(trimmed_q, psm, sqlparse::re_execute_stmt())) {
                    if (auto it = st.client_stmts.find(psm[2].str()); it != st.client_stmts.end()) {
                        query = psm[1].str() + it->second.global_name + psm[3].str();
                        rewritten = sqlparse::make_parse_body(gname, query, oids);
                    }
                } else if (std::regex_match(trimmed_q, psm, sqlparse::re_dealloc_stmt())) {
                    std::string sql_cname = psm[1].str();
                    if (iequals(sql_cname, "ALL")) {
                        st.client_stmts.clear();
                        target_b->prepared_stmts.clear();
                    } else if (auto it = st.client_stmts.find(sql_cname); it != st.client_stmts.end()) {
                        query = "DEALLOCATE " + it->second.global_name;
                        rewritten = sqlparse::make_parse_body(gname, query, oids);
                        target_b->prepared_stmts.erase(it->second.global_name);
                        st.client_stmts.erase(it);
                    }
                }

                if (sqlparse::is_session_pinning_sql(query) && !st.session_pinned) {
                    st.session_pinned = true;
                    pinned_clients_.fetch_add(1);
                }
                UndoParse u;
                u.cname = cname;
                u.gname = gname;
                if (auto prev_it = st.client_stmts.find(cname); prev_it != st.client_stmts.end()) {
                    u.had_prev = true;
                    u.prev = prev_it->second;
                }
                undo_parses.push_back(std::move(u));

                PreparedDef def;
                def.client_name = cname;
                def.global_name = gname;
                def.query = query;
                def.param_oids = std::move(oids);
                st.client_stmts[cname] = std::move(def);
                target_b->prepared_stmts.insert(gname);
                bb = std::move(rewritten);
            } else if (bt == 'B') {
                bb = sqlparse::rewrite_bind_body(bb, st.client_stmts);
            } else if (bt == 'D') {
                std::string unused;
                bb = sqlparse::rewrite_named_op_body(bb, st.client_stmts, false, unused);
            } else if (bt == 'C') {
                std::string erased;
                bb = sqlparse::rewrite_named_op_body(bb, st.client_stmts, true, erased);
                if (!erased.empty()) {
                    if (auto it = st.client_stmts.find(erased); it != st.client_stmts.end()) {
                        target_b->prepared_stmts.erase(it->second.global_name);
                        st.client_stmts.erase(it);
                    }
                }
            }
            if (!target_b->stream.write_all(build_pg_message(bt, bb))) {
                for (auto it = undo_parses.rbegin(); it != undo_parses.rend(); ++it) {
                    target_b->prepared_stmts.erase(it->gname);
                    if (it->had_prev) st.client_stmts[it->cname] = it->prev;
                    else st.client_stmts.erase(it->cname);
                }
                return is_read_backend ? 0 : -1;
            }
            prepared_msgs.emplace_back(bt, std::move(bb));
        }

        auto rollback_parses = [&]() {
            for (auto it = undo_parses.rbegin(); it != undo_parses.rend(); ++it) {
                target_b->prepared_stmts.erase(it->gname);
                if (it->had_prev) st.client_stmts[it->cname] = it->prev;
                else st.client_stmts.erase(it->cname);
            }
        };

        if (terminator_type == 'S') {
            bool had_err = false;
            RelayStats rstats;
            if (!relay_until_ready(target_b, st.client, st.tx_status, had_err, &rstats)) {
                return -1;
            }
            if (had_err) rollback_parses();
            record_query_metrics(st, q_start, wait_us, bytes_in, rstats, had_err, exec_count);
            if (!is_read_backend && rport > 0 && !had_err && st.tx_status == 'I' && batch_has_write) {
                uint64_t wlsn = 0;
                if (query_lsn_backend(target_b, "SELECT pg_current_wal_lsn()::text", wlsn) &&
                    wlsn > st.session_write_lsn) {
                    st.session_write_lsn = wlsn;
                }
            }
            return 1;
        } else {
            bool had_err = false;
            RelayStats rstats;
            bool ok = relay_flush_batch(target_b, st.client, prepared_msgs, had_err, &rstats);
            if (!ok) return -1;
            if (had_err) rollback_parses();
            record_query_metrics(st, q_start, wait_us, bytes_in, rstats, had_err, exec_count);
            return 1;
        }
    };

    if (try_read_engine) {
        auto wait_start = std::chrono::steady_clock::now();
        std::string rerr;
        if (auto rb = acquire_read_backend(st.client_id, rerr)) {
            uint64_t rwait_us = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - wait_start)
                    .count());
            read_engine_healthy_.store(true, std::memory_order_relaxed);
            if (ensure_session_and_stmts(rb.get(), st)) {
                int rc = run_batch_on_backend(rb.get(), true, rwait_us);
                if (rc == 1) {
                    ext_batch.clear();
                    ext_batch_bytes = 0;
                    release_read_backend(std::move(rb), st.tx_status != 'I');
                    return true;
                }
                if (rc == -1) {
                    ext_batch.clear();
                    ext_batch_bytes = 0;
                    release_read_backend(std::move(rb), true);
                    return false;
                }
            }
            release_read_backend(std::move(rb), true);
        } else {
            read_engine_healthy_.store(false, std::memory_order_relaxed);
        }
        record_fallback_to_write(&routing_metrics_);
    }

    uint64_t pool_wait_us = 0;
    if (!ensure_backend(st, pool_wait_us)) {
        ext_batch.clear();
        ext_batch_bytes = 0;
        record_query_metrics(st, q_start, pool_wait_us, bytes_in, {}, true, exec_count);
        return true;
    }

    st.backend_in_flight = true;
    int rc = run_batch_on_backend(st.current_backend.get(), false, pool_wait_us);
    ext_batch.clear();
    ext_batch_bytes = 0;
    st.backend_in_flight = false;
    if (rc == -1) {
        release_current(st, true);
        return false;
    }
    if (terminator_type == 'S') {
        update_tx_count(st);
        if (st.tx_status == 'I' && !st.session_pinned) {
            release_current(st, false);
        }
    }
    return true;
}

void TenantPool::serve_client(Stream& client,
                              const std::string& startup_user,
                              const std::string& expected_pass,
                              const std::function<void()>& touch_activity,
                              MetricsRegistry* metrics,
                              CancelRegistry* cancel_reg,
                              int client_idle_timeout_ms) {
    uint64_t client_id = client_seq_.fetch_add(1) + 1;
    uint32_t gw_pid = static_cast<uint32_t>(10000 + (client_id % 50000));
    uint32_t gw_secret = 0;
    if (RAND_bytes(reinterpret_cast<uint8_t*>(&gw_secret), sizeof(gw_secret)) != 1 || gw_secret == 0) {
        gw_secret = static_cast<uint32_t>(0x9e3779b9u ^ (client_id * 2654435761u));
    }

    if (!authenticate_client(client, startup_user, expected_pass, gw_pid, gw_secret, metrics)) {
        return;
    }

    // Transition from auth timeout to configured idle/query timeout.
    client.set_timeouts(client_idle_timeout_ms, 30000);

    ClientSessionState st{client, startup_user, client_id, gw_pid, gw_secret, metrics, cancel_reg,
                          {}, {}, nullptr, 'I', false, false, false};
    auto cleanup = [&]() {
        if (st.session_pinned) pinned_clients_.fetch_sub(1);
        if (st.in_tx_counted) active_tx_count_.fetch_sub(1);
        release_current(st, st.tx_status != 'I' || st.session_pinned || st.backend_in_flight);
    };

    static constexpr size_t kMaxExtBatchMsgs = 4096;
    static constexpr size_t kMaxExtBatchBytes = 32 * 1024 * 1024;
    std::vector<std::pair<char, std::vector<uint8_t>>> ext_batch;
    size_t ext_batch_bytes = 0;

    while (true) {
        char mtype = 0;
        std::vector<uint8_t> mbody;
        if (!read_pg_packet(client, mtype, mbody)) {
            cleanup();
            return;
        }
        if (touch_activity) touch_activity();

        if (mtype == 'X') { // Terminate
            cleanup();
            return;
        }

        if (mtype == 'Q') {
            if (!handle_simple_query(st, mbody)) {
                cleanup();
                return;
            }
            continue;
        }

        if (mtype == 'P' || mtype == 'B' || mtype == 'D' || mtype == 'E' || mtype == 'C') {
            if (ext_batch.size() >= kMaxExtBatchMsgs ||
                ext_batch_bytes + 5 + mbody.size() > kMaxExtBatchBytes) {
                ext_batch.clear();
                ext_batch_bytes = 0;
                if (metrics) metrics->record_protocol_error();
                client.write_all(build_error_response("FATAL", "54000", "extended query pipeline buffer limit exceeded"));
                cleanup();
                return;
            }
            ext_batch_bytes += 5 + mbody.size();
            ext_batch.emplace_back(mtype, std::move(mbody));
            continue;
        }

        if (mtype == 'S' || mtype == 'H') {
            if (!handle_extended_batch(st, mtype, std::move(mbody), ext_batch, ext_batch_bytes)) {
                cleanup();
                return;
            }
            continue;
        }

        if (metrics) metrics->record_protocol_error();
        if (!client.write_all(build_error_response("ERROR", "08P01", "unsupported frontend message type")) ||
            !client.write_all(build_ready_for_query(st.tx_status))) {
            cleanup();
            return;
        }
    }
}

bool TenantPool::acquire_slot(std::string& err_out) {
    std::unique_lock<std::mutex> lk(sem_mu_);
    bool ok = sem_cv_.wait_for(lk, std::chrono::seconds(10), [this]() {
        return active_backends_ < max_backends_;
    });
    if (!ok) {
        err_out = "backend connection pool saturated for tenant " + tenant_id_;
        return false;
    }
    ++active_backends_;
    return true;
}

void TenantPool::release_slot() {
    {
        std::lock_guard<std::mutex> lk(sem_mu_);
        if (active_backends_ > 0) --active_backends_;
    }
    sem_cv_.notify_one();
}

std::unique_ptr<BackendConn> TenantPool::acquire_backend(uint64_t client_id, std::string& err_out) {
    if (!acquire_slot(err_out)) return nullptr;

    auto now = std::chrono::steady_clock::now();
    int timeout_s = idle_timeout_sec_.load(std::memory_order_relaxed);
    while (true) {
        std::unique_ptr<BackendConn> b;
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (!idle_pool_.empty()) {
                b = std::move(idle_pool_.back());
                idle_pool_.pop_back();
            }
        }
        if (b) {
            if (std::chrono::duration_cast<std::chrono::seconds>(now - b->last_used).count() >= timeout_s) {
                b->close();
                continue;
            }
            if (b->last_client_id != client_id) {
                if (!exec_simple_backend(b.get(), "DISCARD ALL;")) {
                    b->close();
                    continue;
                }
                b->last_client_id = client_id;
                b->prepared_stmts.clear();
            } else if (!exec_simple_backend(b.get(), "")) {
                b->close();
                continue;
            }
            return b;
        }
        break;
    }

    std::string host, sock_dir, pass;
    int port = 0;
    WakeFn wake;
    {
        std::lock_guard<std::mutex> lk(mu_);
        host = host_;
        sock_dir = sock_dir_;
        port = port_;
        pass = app_password_;
        wake = wake_fn_;
    }

    auto b = dial_and_startup(host, sock_dir, port, pass, client_id, err_out);
    if (!b && wake) {
        TenantRoute wroute;
        std::string werr;
        if (wake(wroute, werr) && wroute.port > 0) {
            update_endpoint(wroute.host, wroute.sock_dir, wroute.port, wroute.app_password);
            {
                std::lock_guard<std::mutex> lk(mu_);
                host = host_;
                sock_dir = sock_dir_;
                port = port_;
                pass = app_password_;
            }
            b = dial_and_startup(host, sock_dir, port, pass, client_id, err_out);
        }
    }
    if (!b) {
        release_slot();
        return nullptr;
    }
    return b;
}

void TenantPool::release_backend(std::unique_ptr<BackendConn> b, bool dirty) {
    if (!b) return;
    if (dirty) {
        b->close();
        release_slot();
        return;
    }
    b->last_used = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (static_cast<int>(idle_pool_.size()) < max_backends_) {
            idle_pool_.push_back(std::move(b));
            release_slot();
            return;
        }
    }
    b->close();
    release_slot();
}

std::unique_ptr<BackendConn> TenantPool::dial_and_startup(const std::string& host,
                                                          const std::string& sock_dir,
                                                          int port,
                                                          const std::string& app_pass,
                                                          uint64_t client_id,
                                                          std::string& err_out) {
    int fd = dial_backend_socket(host, sock_dir, port);
    if (fd < 0) {
        err_out = "failed to connect to tenant backend on port " + std::to_string(port);
        return nullptr;
    }
    auto b = std::make_unique<BackendConn>();
    b->stream = Stream(fd);
    b->last_client_id = client_id;

    auto startup = build_startup_message({
        {"user", "app_user"},
        {"database", tenant_id_},
        {"application_name", "air-pooler"},
    });
    if (!b->stream.write_all(startup)) {
        err_out = "failed to write startup to backend";
        return nullptr;
    }

    while (true) {
        char t = 0;
        std::vector<uint8_t> body;
        if (!read_pg_packet(b->stream, t, body)) {
            err_out = "backend closed during startup";
            return nullptr;
        }
        if (t == 'R' && body.size() >= 4) {
            uint32_t code = read_be32(body.data());
            if (code == 0) {
                continue; // AuthOk
            } else if (code == 3) {
                if (!b->stream.write_all(build_password_message(app_pass))) {
                    err_out = "failed to send cleartext password to backend";
                    return nullptr;
                }
            } else if (code == 10) {
                if (!scram_sha256_client_auth(b->stream, "app_user", app_pass, err_out)) {
                    return nullptr;
                }
            } else {
                err_out = "unsupported backend auth method: " + std::to_string(code);
                return nullptr;
            }
        } else if (t == 'K' && body.size() >= 8) {
            b->backend_pid = read_be32(body.data());
            b->backend_secret = read_be32(body.data() + 4);
        } else if (t == 'Z' && !body.empty()) {
            return b;
        } else if (t == 'E') {
            err_out = "backend startup error";
            return nullptr;
        }
    }
}

uint64_t TenantPool::parse_pg_lsn(const std::string& s) {
    auto slash = s.find('/');
    if (slash == std::string::npos || slash == 0 || slash + 1 >= s.size()) return 0;
    try {
        uint64_t hi = std::stoull(s.substr(0, slash), nullptr, 16);
        uint64_t lo = std::stoull(s.substr(slash + 1), nullptr, 16);
        return (hi << 32) | (lo & 0xffffffffULL);
    } catch (...) {
        return 0;
    }
}

bool TenantPool::query_lsn_backend(BackendConn* b, const char* sql, uint64_t& lsn_out) {
    lsn_out = 0;
    if (!b || !b->stream.write_all(build_simple_query(sql))) return false;
    bool ok = false;
    while (true) {
        char t = 0;
        std::vector<uint8_t> body;
        if (!read_pg_packet(b->stream, t, body)) return false;
        if (t == 'D' && body.size() >= 6) {
            uint16_t ncols = read_be16(body.data());
            if (ncols >= 1) {
                int32_t flen = static_cast<int32_t>(read_be32(body.data() + 2));
                if (flen > 0 && static_cast<size_t>(6 + flen) <= body.size()) {
                    std::string val(reinterpret_cast<const char*>(body.data() + 6),
                                    static_cast<size_t>(flen));
                    lsn_out = parse_pg_lsn(val);
                    ok = (lsn_out > 0);
                }
            }
        } else if (t == 'E') {
            ok = false;
        } else if (t == 'Z') {
            return ok;
        }
    }
}

bool TenantPool::exec_simple_backend(BackendConn* b, const std::string& sql) {
    if (!b->stream.write_all(build_simple_query(sql))) return false;
    return drain_until_ready_backend(b);
}

bool TenantPool::drain_until_ready_backend(BackendConn* b) {
    while (true) {
        char t = 0;
        std::vector<uint8_t> body;
        if (!read_pg_packet(b->stream, t, body)) return false;
        if (t == 'E') return false;
        if (t == 'Z') return true;
    }
}

bool TenantPool::relay_copy_in(BackendConn* b, Stream& client) {
    while (true) {
        char ct = 0;
        std::vector<uint8_t> cbody;
        if (!read_pg_packet(client, ct, cbody)) return false;
        if (ct != 'd' && ct != 'c' && ct != 'f') {
            return false;
        }
        if (!b->stream.write_all(build_pg_message(ct, cbody))) return false;
        if (ct == 'c' || ct == 'f') return true;
    }
}

bool TenantPool::relay_until_ready(BackendConn* b, Stream& client, char& tx_status,
                                   bool& had_err, RelayStats* stats) {
    had_err = false;
    while (true) {
        char t = 0;
        std::vector<uint8_t> body;
        if (!read_pg_packet(b->stream, t, body)) return false;
        if (stats) {
            stats->bytes_out += 5 + body.size();
            if (t == 'D') {
                ++stats->rows_returned;
                stats->data_row_bytes += body.size();
            }
        }
        if (t == 'E') had_err = true;
        if (!client.write_all(build_pg_message(t, body))) return false;
        if (t == 'G') {
            if (!relay_copy_in(b, client)) return false;
            continue;
        }
        if (t == 'Z' && !body.empty()) {
            tx_status = static_cast<char>(body[0]);
            return true;
        }
    }
}

bool TenantPool::relay_flush_batch(BackendConn* b, Stream& client,
                                   const std::vector<std::pair<char, std::vector<uint8_t>>>& batch,
                                   bool& had_err, RelayStats* stats) {
    had_err = false;
    auto forward_pkt = [&](char& t, std::vector<uint8_t>& body) -> bool {
        while (true) {
            if (!read_pg_packet(b->stream, t, body)) return false;
            if (stats) {
                stats->bytes_out += 5 + body.size();
                if (t == 'D') {
                    ++stats->rows_returned;
                    stats->data_row_bytes += body.size();
                }
            }
            if (t == 'E') had_err = true;
            if (!client.write_all(build_pg_message(t, body))) return false;
            if (t == 'N' || t == 'S' || t == 'A') continue;
            return true;
        }
    };

    for (const auto& [bt, _] : batch) {
        if (bt == 'H') break;
        if (had_err) break;
        char t = 0;
        std::vector<uint8_t> body;
        if (bt == 'P' || bt == 'B' || bt == 'C') {
            if (!forward_pkt(t, body)) return false;
        } else if (bt == 'D') {
            if (!forward_pkt(t, body)) return false;
            if (t == 't') {
                if (!forward_pkt(t, body)) return false;
            }
        } else if (bt == 'E') {
            while (true) {
                if (!forward_pkt(t, body)) return false;
                if (t == 'G') {
                    if (!relay_copy_in(b, client)) return false;
                    continue;
                }
                if (t == 'C' || t == 'I' || t == 's' || t == 'E') break;
            }
        }
    }
    return true;
}

PoolManager::PoolManager(int max_backends, int idle_timeout_sec)
    : max_backends_(max_backends),
      idle_timeout_sec_(idle_timeout_sec > 0 ? idle_timeout_sec : 300) {}

void PoolManager::set_read_engine_endpoint(const std::string& host, const std::string& sock_dir, int port) {
    std::lock_guard<std::mutex> lk(mu_);
    read_host_ = host;
    read_sock_dir_ = sock_dir;
    read_port_ = port;
    for (auto& [_, p] : pools_) {
        if (p) p->set_read_engine_endpoint(host, sock_dir, port);
    }
}

std::shared_ptr<TenantPool> PoolManager::get_or_create(const TenantRoute& route,
                                                       TenantPool::WakeFn wake_fn) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = pools_.find(route.tenant_id);
    if (it != pools_.end()) {
        it->second->update_endpoint(route.host, route.sock_dir, route.port, route.app_password);
        return it->second;
    }
    auto p = std::make_shared<TenantPool>(
        route.tenant_id, route.host, route.sock_dir, route.port, route.app_password,
        max_backends_, idle_timeout_sec_);
    if (read_port_ > 0 || !read_sock_dir_.empty()) {
        p->set_read_engine_endpoint(read_host_, read_sock_dir_, read_port_);
    }
    p->set_wake_fn(std::move(wake_fn));
    pools_[route.tenant_id] = p;
    return p;
}

size_t PoolManager::evict_expired_idle() {
    std::vector<std::shared_ptr<TenantPool>> list;
    {
        std::lock_guard<std::mutex> lk(mu_);
        list.reserve(pools_.size());
        for (const auto& [_, p] : pools_) {
            if (p) list.push_back(p);
        }
    }
    size_t evicted = 0;
    for (const auto& p : list) evicted += p->evict_expired_idle();
    return evicted;
}

void PoolManager::upsert_discovered(const TenantRoute& route) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = pools_.find(route.tenant_id);
    if (it != pools_.end()) {
        if (route.state == "SUSPENDED") {
            it->second->close_all_idle();
        } else {
            it->second->update_endpoint(route.host, route.sock_dir, route.port, route.app_password);
        }
    }
}

void PoolManager::remove_tenant(const std::string& tenant_id) {
    std::shared_ptr<TenantPool> p;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pools_.find(tenant_id);
        if (it != pools_.end()) {
            p = std::move(it->second);
            pools_.erase(it);
        }
    }
    if (p) p->close_all_idle();
}

bool PoolManager::request_drain(const std::string& tenant_id, int timeout_ms, std::string& err_out) {
    std::shared_ptr<TenantPool> p;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pools_.find(tenant_id);
        if (it != pools_.end()) p = it->second;
    }
    if (!p) return true;
    return p->request_drain(timeout_ms, err_out);
}

void PoolManager::set_tenant_max_backends(const std::string& tenant_id, int max_backends) {
    std::shared_ptr<TenantPool> p;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pools_.find(tenant_id);
        if (it != pools_.end()) p = it->second;
    }
    if (p) p->set_max_backends(max_backends);
}

std::vector<PoolTenantSnapshot> PoolManager::snapshots() const {
    std::vector<std::shared_ptr<TenantPool>> list;
    {
        std::lock_guard<std::mutex> lk(mu_);
        list.reserve(pools_.size());
        for (const auto& [_, p] : pools_) {
            if (p) list.push_back(p);
        }
    }
    std::vector<PoolTenantSnapshot> out;
    out.reserve(list.size());
    for (const auto& p : list) {
        out.push_back(p->snapshot());
    }
    return out;
}

void PoolManager::close_all() {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& [_, p] : pools_) {
        if (p) p->close_all_idle();
    }
}

} // namespace air::gw
