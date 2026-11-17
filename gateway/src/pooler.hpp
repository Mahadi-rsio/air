#pragma once

#include "discovery.hpp"
#include "metrics.hpp"
#include "pg_wire.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <regex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace air::gw {

struct PreparedDef {
    std::string client_name;
    std::string global_name;
    std::string type_list;
    std::string query;
    std::vector<uint32_t> param_oids;
    bool is_sql_prep = false;
};

struct BackendConn {
    Stream stream;
    uint64_t last_client_id = 0;
    std::unordered_set<std::string> prepared_stmts;

    void close() { stream.close(); }
};

namespace sqlparse {

inline const std::regex& re_set_stmt() {
    static const std::regex r(R"(^\s*SET\s+(?:SESSION\s+)?([a-zA-Z0-9_.]+)\s*(?:=|TO)\s*(.+?)\s*;?\s*$)",
                              std::regex::icase);
    return r;
}

inline const std::regex& re_prepare_stmt() {
    static const std::regex r(R"(^\s*PREPARE\s+([a-zA-Z0-9_]+)(\s*\([^)]*\))?\s+AS\s+(.+?)\s*;?\s*$)",
                              std::regex::icase);
    return r;
}

inline const std::regex& re_dealloc_stmt() {
    static const std::regex r(R"(^\s*DEALLOCATE\s+(?:PREPARE\s+)?([a-zA-Z0-9_]+)\s*;?\s*$)",
                              std::regex::icase);
    return r;
}

inline const std::regex& re_execute_stmt() {
    static const std::regex r(R"(^(\s*EXECUTE\s+)([a-zA-Z0-9_]+)(\s*(?:\([^)]*\))?\s*;?\s*)$)",
                              std::regex::icase);
    return r;
}

inline const std::regex& re_listen_stmt() {
    static const std::regex r(R"(^\s*LISTEN\b)", std::regex::icase);
    return r;
}

inline const std::regex& re_temp_table_stmt() {
    static const std::regex r(R"(^\s*CREATE\s+(?:TEMP|TEMPORARY)\s+TABLE\b)", std::regex::icase);
    return r;
}

inline const std::regex& re_adv_lock_stmt() {
    static const std::regex r(R"(\bpg_advisory_lock(?:_shared)?\s*\()", std::regex::icase);
    return r;
}

inline bool is_session_pinning_sql(const std::string& sql) {
    return std::regex_search(sql, re_listen_stmt()) ||
           std::regex_search(sql, re_temp_table_stmt()) ||
           std::regex_search(sql, re_adv_lock_stmt());
}

inline std::string read_cstr(const std::vector<uint8_t>& buf, size_t& off) {
    size_t start = off;
    while (off < buf.size() && buf[off] != 0) ++off;
    std::string s(reinterpret_cast<const char*>(buf.data() + start), off - start);
    if (off < buf.size()) ++off;
    return s;
}

inline std::vector<uint8_t> rewrite_parse_body(const std::vector<uint8_t>& body,
                                               const std::string& global_name,
                                               std::string& client_name_out,
                                               std::string& query_out,
                                               std::vector<uint32_t>& oids_out) {
    size_t off = 0;
    client_name_out = read_cstr(body, off);
    query_out = read_cstr(body, off);
    oids_out.clear();
    if (off + 2 <= body.size()) {
        uint16_t cnt = read_be16(body.data() + off);
        off += 2;
        for (uint16_t i = 0; i < cnt && off + 4 <= body.size(); ++i) {
            oids_out.push_back(read_be32(body.data() + off));
            off += 4;
        }
    }
    std::vector<uint8_t> out;
    append_cstr(out, global_name);
    append_cstr(out, query_out);
    append_be16(out, static_cast<uint16_t>(oids_out.size()));
    for (uint32_t oid : oids_out) append_be32(out, oid);
    return out;
}

inline std::vector<uint8_t> rewrite_bind_body(const std::vector<uint8_t>& body,
                                              const std::unordered_map<std::string, PreparedDef>& stmts) {
    size_t off = 0;
    std::string portal = read_cstr(body, off);
    std::string stmt_name = read_cstr(body, off);
    auto it = stmts.find(stmt_name);
    if (it == stmts.end()) return body;
    std::vector<uint8_t> out;
    append_cstr(out, portal);
    append_cstr(out, it->second.global_name);
    if (off < body.size()) {
        out.insert(out.end(), body.begin() + static_cast<std::ptrdiff_t>(off), body.end());
    }
    return out;
}

inline std::vector<uint8_t> rewrite_named_op_body(const std::vector<uint8_t>& body,
                                                  const std::unordered_map<std::string, PreparedDef>& stmts,
                                                  bool is_close,
                                                  std::string& erased_client_name) {
    if (body.empty()) return body;
    uint8_t kind = body[0];
    size_t off = 1;
    std::string name = read_cstr(body, off);
    if (kind != 'S') return body;
    auto it = stmts.find(name);
    if (it == stmts.end()) return body;
    if (is_close) erased_client_name = name;
    std::vector<uint8_t> out;
    out.push_back(kind);
    append_cstr(out, it->second.global_name);
    return out;
}

} // namespace sqlparse

class TenantPool {
public:
    using WakeFn = std::function<bool(TenantRoute& route_out, std::string& err_out)>;

    TenantPool(std::string tenant_id, std::string host, std::string sock_dir,
               int port, std::string app_password, int max_backends)
        : tenant_id_(std::move(tenant_id)),
          max_backends_(max_backends > 0 ? max_backends : 16),
          host_(std::move(host)),
          sock_dir_(std::move(sock_dir)),
          port_(port),
          app_password_(std::move(app_password)) {}

    ~TenantPool() { close_all_idle(); }

    void set_wake_fn(WakeFn fn) {
        std::lock_guard<std::mutex> lk(mu_);
        wake_fn_ = std::move(fn);
    }

    void update_endpoint(const std::string& host, const std::string& sock_dir,
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

    void set_max_backends(int max_backends) {
        if (max_backends <= 0) return;
        {
            std::lock_guard<std::mutex> lk(sem_mu_);
            max_backends_ = max_backends;
        }
        sem_cv_.notify_all();
    }

    PoolTenantSnapshot snapshot() {
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

    void close_all_idle() {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto& b : idle_pool_) {
            if (b) b->close();
        }
        idle_pool_.clear();
    }

    bool request_drain(int timeout_ms, std::string& err_out) {
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

    void serve_client(Stream& client, const std::string& startup_user,
                      const std::string& expected_pass,
                      const std::function<void()>& touch_activity,
                      MetricsRegistry* metrics = nullptr) {
        uint64_t client_id = client_seq_.fetch_add(1) + 1;

        if (!expected_pass.empty()) {
            if (!client.write_all(build_auth_cleartext())) return;
            char mtype = 0;
            std::vector<uint8_t> mbody;
            if (!read_pg_packet(client, mtype, mbody) || mtype != 'p') return;
            size_t off = 0;
            std::string got_pass = sqlparse::read_cstr(mbody, off);
            if (got_pass != expected_pass) {
                client.write_all(build_error_response(
                    "FATAL", "28P01", "password authentication failed for user \"" + startup_user + "\""));
                return;
            }
        }

        if (!client.write_all(build_auth_ok())) return;
        if (!client.write_all(build_param_status("server_version", "17.12"))) return;
        if (!client.write_all(build_param_status("client_encoding", "UTF8"))) return;
        if (!client.write_all(build_param_status("DateStyle", "ISO, MDY"))) return;
        if (!client.write_all(build_backend_key_data(static_cast<uint32_t>(10000 + (client_id % 50000)), 9999))) return;
        if (!client.write_all(build_ready_for_query('I'))) return;

        std::unordered_map<std::string, std::string> session_vars;
        std::unordered_map<std::string, PreparedDef> client_stmts;
        std::unique_ptr<BackendConn> current_backend;
        char tx_status = 'I';
        bool in_tx_counted = false;
        bool session_pinned = false;

        auto cleanup = [&]() {
            if (session_pinned) pinned_clients_.fetch_sub(1);
            if (in_tx_counted) active_tx_count_.fetch_sub(1);
            if (current_backend) {
                release_backend(std::move(current_backend), tx_status != 'I' || session_pinned);
            }
        };

        auto ensure_session_and_stmts = [&](BackendConn* b) -> bool {
            if (!session_vars.empty()) {
                std::string sql;
                for (const auto& [k, v] : session_vars) {
                    sql += "SET " + k + " = " + v + "; ";
                }
                if (!exec_simple_backend(b, sql)) return false;
            }
            for (const auto& [_, def] : client_stmts) {
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
        };

        auto ensure_backend = [&](uint64_t& pool_wait_us_out) -> bool {
            pool_wait_us_out = 0;
            if (current_backend) return true;
            auto wait_start = std::chrono::steady_clock::now();
            std::string acq_err;
            current_backend = acquire_backend(client_id, acq_err);
            pool_wait_us_out = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - wait_start)
                    .count());
            if (!current_backend) {
                client.write_all(build_error_response("ERROR", "53300", acq_err));
                client.write_all(build_ready_for_query(tx_status));
                return false;
            }
            if (!ensure_session_and_stmts(current_backend.get())) {
                release_backend(std::move(current_backend), true);
                client.write_all(build_error_response("ERROR", "08006", "failed to prepare pooled backend session state"));
                client.write_all(build_ready_for_query(tx_status));
                return false;
            }
            return true;
        };

        auto record_metrics = [&](std::chrono::steady_clock::time_point q_start,
                                  uint64_t pool_wait_us,
                                  uint64_t bytes_in,
                                  const RelayStats& rstats,
                                  bool had_err,
                                  uint64_t count) {
            if (!metrics) return;
            uint64_t dur_us = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - q_start)
                    .count());
            metrics->record_query(tenant_id_, startup_user, dur_us, pool_wait_us,
                                  bytes_in, rstats.bytes_out, rstats.rows_returned,
                                  rstats.data_row_bytes, had_err, count);
        };

        std::vector<std::pair<char, std::vector<uint8_t>>> ext_batch;

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

            if (mtype == 'Q') { // Simple Query
                auto q_start = std::chrono::steady_clock::now();
                uint64_t bytes_in = 5 + mbody.size();
                uint64_t pool_wait_us = 0;

                size_t off = 0;
                std::string q = sqlparse::read_cstr(mbody, off);
                std::string trimmed = trim_ascii(q);

                if (sqlparse::is_session_pinning_sql(trimmed) && !session_pinned) {
                    session_pinned = true;
                    pinned_clients_.fetch_add(1);
                }

                std::smatch sm;
                if (std::regex_match(trimmed, sm, sqlparse::re_set_stmt())) {
                    std::string key = sm[1].str();
                    std::string val = sm[2].str();
                    if (!ensure_backend(pool_wait_us)) {
                        record_metrics(q_start, pool_wait_us, bytes_in, {}, true, 1);
                        continue;
                    }
                    if (!current_backend->stream.write_all(build_pg_message('Q', mbody))) {
                        release_backend(std::move(current_backend), true);
                        cleanup();
                        return;
                    }
                    bool had_err = false;
                    RelayStats rstats;
                    if (!relay_until_ready(current_backend.get(), client, tx_status, had_err, &rstats)) {
                        release_backend(std::move(current_backend), true);
                        cleanup();
                        return;
                    }
                    record_metrics(q_start, pool_wait_us, bytes_in, rstats, had_err, 1);
                    if (!had_err) session_vars[key] = val;
                    if (tx_status == 'I' && !session_pinned) {
                        release_backend(std::move(current_backend), false);
                    }
                    continue;
                }

                if (std::regex_match(trimmed, sm, sqlparse::re_prepare_stmt())) {
                    std::string client_name = sm[1].str();
                    std::string type_list = sm[2].str();
                    std::string inner_q = sm[3].str();
                    std::string global_name = "_air_s_" + std::to_string(client_id) + "_" +
                                              std::to_string(stmt_seq_.fetch_add(1) + 1);
                    if (!ensure_backend(pool_wait_us)) {
                        record_metrics(q_start, pool_wait_us, bytes_in, {}, true, 1);
                        continue;
                    }
                    std::string rewritten = "PREPARE " + global_name + type_list + " AS " + inner_q;
                    if (!current_backend->stream.write_all(build_simple_query(rewritten))) {
                        release_backend(std::move(current_backend), true);
                        cleanup();
                        return;
                    }
                    bool had_err = false;
                    RelayStats rstats;
                    if (!relay_until_ready(current_backend.get(), client, tx_status, had_err, &rstats)) {
                        release_backend(std::move(current_backend), true);
                        cleanup();
                        return;
                    }
                    record_metrics(q_start, pool_wait_us, bytes_in, rstats, had_err, 1);
                    if (!had_err) {
                        PreparedDef def;
                        def.client_name = client_name;
                        def.global_name = global_name;
                        def.type_list = type_list;
                        def.query = inner_q;
                        def.is_sql_prep = true;
                        client_stmts[client_name] = std::move(def);
                        current_backend->prepared_stmts.insert(global_name);
                    }
                    if (tx_status == 'I' && !session_pinned) {
                        release_backend(std::move(current_backend), false);
                    }
                    continue;
                }

                if (std::regex_match(trimmed, sm, sqlparse::re_dealloc_stmt())) {
                    std::string cname = sm[1].str();
                    if (iequals(cname, "ALL")) {
                        client_stmts.clear();
                    } else {
                        client_stmts.erase(cname);
                    }
                    auto cmsg = build_pg_message('C', std::vector<uint8_t>{'D','E','A','L','L','O','C','A','T','E',0});
                    auto zmsg = build_ready_for_query(tx_status);
                    if (!client.write_all(cmsg) || !client.write_all(zmsg)) {
                        cleanup();
                        return;
                    }
                    RelayStats rstats;
                    rstats.bytes_out = cmsg.size() + zmsg.size();
                    record_metrics(q_start, 0, bytes_in, rstats, false, 1);
                    continue;
                }

                std::vector<uint8_t> out_body = mbody;
                if (std::regex_match(trimmed, sm, sqlparse::re_execute_stmt())) {
                    std::string cname = sm[2].str();
                    auto it = client_stmts.find(cname);
                    if (it != client_stmts.end()) {
                        std::string rewritten = sm[1].str() + it->second.global_name + sm[3].str();
                        out_body.clear();
                        append_cstr(out_body, rewritten);
                    }
                }

                if (!ensure_backend(pool_wait_us)) {
                    record_metrics(q_start, pool_wait_us, bytes_in, {}, true, 1);
                    continue;
                }
                if (!current_backend->stream.write_all(build_pg_message('Q', out_body))) {
                    release_backend(std::move(current_backend), true);
                    cleanup();
                    return;
                }
                bool had_err = false;
                RelayStats rstats;
                if (!relay_until_ready(current_backend.get(), client, tx_status, had_err, &rstats)) {
                    release_backend(std::move(current_backend), true);
                    cleanup();
                    return;
                }
                record_metrics(q_start, pool_wait_us, bytes_in, rstats, had_err, 1);
                if (tx_status != 'I' && !in_tx_counted) {
                    in_tx_counted = true;
                    active_tx_count_.fetch_add(1);
                } else if (tx_status == 'I' && in_tx_counted) {
                    in_tx_counted = false;
                    active_tx_count_.fetch_sub(1);
                }
                if (tx_status == 'I' && !session_pinned) {
                    release_backend(std::move(current_backend), false);
                }
                continue;
            }

            // Extended query protocol messages: buffer until Sync ('S') or Flush ('H').
            if (mtype == 'P' || mtype == 'B' || mtype == 'D' || mtype == 'E' || mtype == 'C') {
                ext_batch.emplace_back(mtype, std::move(mbody));
                continue;
            }

            if (mtype == 'S' || mtype == 'H') {
                auto q_start = std::chrono::steady_clock::now();
                ext_batch.emplace_back(mtype, std::move(mbody));
                uint64_t bytes_in = 0;
                uint64_t exec_count = 0;
                for (const auto& [bt, bb] : ext_batch) {
                    bytes_in += 5 + bb.size();
                    if (bt == 'E') ++exec_count;
                }
                if (exec_count == 0) exec_count = 1;

                uint64_t pool_wait_us = 0;
                if (!ensure_backend(pool_wait_us)) {
                    ext_batch.clear();
                    record_metrics(q_start, pool_wait_us, bytes_in, {}, true, exec_count);
                    continue;
                }
                for (auto& [bt, bb] : ext_batch) {
                    if (bt == 'P') {
                        std::string cname, query;
                        std::vector<uint32_t> oids;
                        std::string gname = "_air_s_" + std::to_string(client_id) + "_" +
                                            std::to_string(stmt_seq_.fetch_add(1) + 1);
                        auto rewritten = sqlparse::rewrite_parse_body(bb, gname, cname, query, oids);
                        if (sqlparse::is_session_pinning_sql(query) && !session_pinned) {
                            session_pinned = true;
                            pinned_clients_.fetch_add(1);
                        }
                        PreparedDef def;
                        def.client_name = cname;
                        def.global_name = gname;
                        def.query = query;
                        def.param_oids = std::move(oids);
                        client_stmts[cname] = std::move(def);
                        current_backend->prepared_stmts.insert(gname);
                        bb = std::move(rewritten);
                    } else if (bt == 'B') {
                        bb = sqlparse::rewrite_bind_body(bb, client_stmts);
                    } else if (bt == 'D') {
                        std::string unused;
                        bb = sqlparse::rewrite_named_op_body(bb, client_stmts, false, unused);
                    } else if (bt == 'C') {
                        std::string erased;
                        bb = sqlparse::rewrite_named_op_body(bb, client_stmts, true, erased);
                        if (!erased.empty()) client_stmts.erase(erased);
                    }
                    if (!current_backend->stream.write_all(build_pg_message(bt, bb))) {
                        release_backend(std::move(current_backend), true);
                        cleanup();
                        return;
                    }
                }
                ext_batch.clear();

                if (mtype == 'S') {
                    bool had_err = false;
                    RelayStats rstats;
                    if (!relay_until_ready(current_backend.get(), client, tx_status, had_err, &rstats)) {
                        release_backend(std::move(current_backend), true);
                        cleanup();
                        return;
                    }
                    record_metrics(q_start, pool_wait_us, bytes_in, rstats, had_err, exec_count);
                    if (tx_status != 'I' && !in_tx_counted) {
                        in_tx_counted = true;
                        active_tx_count_.fetch_add(1);
                    } else if (tx_status == 'I' && in_tx_counted) {
                        in_tx_counted = false;
                        active_tx_count_.fetch_sub(1);
                    }
                    if (tx_status == 'I' && !session_pinned) {
                        release_backend(std::move(current_backend), false);
                    }
                }
                continue;
            }

            // CopyData ('d'), CopyDone ('c'), CopyFail ('f'), etc.
            uint64_t unused_wait = 0;
            if (!ensure_backend(unused_wait)) continue;
            if (!current_backend->stream.write_all(build_pg_message(mtype, mbody))) {
                release_backend(std::move(current_backend), true);
                cleanup();
                return;
            }
        }
    }

private:
    std::string tenant_id_;
    int max_backends_;

    std::mutex mu_;
    std::string host_;
    std::string sock_dir_;
    int port_ = 0;
    std::string app_password_;
    std::vector<std::unique_ptr<BackendConn>> idle_pool_;
    WakeFn wake_fn_;

    std::mutex sem_mu_;
    std::condition_variable sem_cv_;
    int active_backends_ = 0;

    std::atomic<uint64_t> stmt_seq_{0};
    std::atomic<uint64_t> client_seq_{0};
    std::atomic<int64_t> pinned_clients_{0};
    std::atomic<int64_t> active_tx_count_{0};

    bool acquire_slot(std::string& err_out) {
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

    void release_slot() {
        {
            std::lock_guard<std::mutex> lk(sem_mu_);
            if (active_backends_ > 0) --active_backends_;
        }
        sem_cv_.notify_one();
    }

    std::unique_ptr<BackendConn> acquire_backend(uint64_t client_id, std::string& err_out) {
        if (!acquire_slot(err_out)) return nullptr;

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
                if (b->last_client_id != client_id) {
                    if (!exec_simple_backend(b.get(), "DISCARD ALL;")) {
                        b->close();
                        continue;
                    }
                    b->last_client_id = client_id;
                    b->prepared_stmts.clear();
                } else {
                    if (!exec_simple_backend(b.get(), "")) {
                        b->close();
                        continue;
                    }
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

    void release_backend(std::unique_ptr<BackendConn> b, bool dirty) {
        if (!b) return;
        if (dirty) {
            b->close();
            release_slot();
            return;
        }
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

    std::unique_ptr<BackendConn> dial_and_startup(const std::string& host,
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
            } else if (t == 'Z' && !body.empty()) {
                return b;
            } else if (t == 'E') {
                err_out = "backend startup error";
                return nullptr;
            }
        }
    }

    static bool exec_simple_backend(BackendConn* b, const std::string& sql) {
        if (!b->stream.write_all(build_simple_query(sql))) return false;
        return drain_until_ready_backend(b);
    }

    static bool drain_until_ready_backend(BackendConn* b) {
        while (true) {
            char t = 0;
            std::vector<uint8_t> body;
            if (!read_pg_packet(b->stream, t, body)) return false;
            if (t == 'E') return false;
            if (t == 'Z') return true;
        }
    }

    struct RelayStats {
        uint64_t bytes_out = 0;
        uint64_t rows_returned = 0;
        uint64_t data_row_bytes = 0;
    };

    static bool relay_until_ready(BackendConn* b, Stream& client, char& tx_status,
                                  bool& had_err, RelayStats* stats = nullptr) {
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
            if (t == 'Z' && !body.empty()) {
                tx_status = static_cast<char>(body[0]);
                return true;
            }
        }
    }
};

class PoolManager {
public:
    explicit PoolManager(int max_backends) : max_backends_(max_backends) {}

    std::shared_ptr<TenantPool> get_or_create(const TenantRoute& route, TenantPool::WakeFn wake_fn) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pools_.find(route.tenant_id);
        if (it != pools_.end()) {
            it->second->update_endpoint(route.host, route.sock_dir, route.port, route.app_password);
            return it->second;
        }
        auto p = std::make_shared<TenantPool>(
            route.tenant_id, route.host, route.sock_dir, route.port, route.app_password, max_backends_);
        p->set_wake_fn(std::move(wake_fn));
        pools_[route.tenant_id] = p;
        return p;
    }

    void upsert_discovered(const TenantRoute& route) {
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

    void remove_tenant(const std::string& tenant_id) {
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

    bool request_drain(const std::string& tenant_id, int timeout_ms, std::string& err_out) {
        std::shared_ptr<TenantPool> p;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = pools_.find(tenant_id);
            if (it != pools_.end()) p = it->second;
        }
        if (!p) return true;
        return p->request_drain(timeout_ms, err_out);
    }

    void set_tenant_max_backends(const std::string& tenant_id, int max_backends) {
        std::shared_ptr<TenantPool> p;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = pools_.find(tenant_id);
            if (it != pools_.end()) p = it->second;
        }
        if (p) p->set_max_backends(max_backends);
    }

    std::vector<PoolTenantSnapshot> snapshots() const {
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

    void close_all() {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto& [_, p] : pools_) {
            if (p) p->close_all_idle();
        }
    }

private:
    int max_backends_;
    mutable std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<TenantPool>> pools_;
};

} // namespace air::gw
