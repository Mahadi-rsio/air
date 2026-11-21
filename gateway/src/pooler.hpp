#pragma once

#include "discovery.hpp"
#include "metrics.hpp"
#include "pg/sql_inspect.hpp"
#include "pg_wire.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace air::gw {

/// Metadata for a client-prepared statement (SQL PREPARE or Extended Query Parse),
/// mapped from the client-local name to a globally unique backend statement name.
struct PreparedDef {
    std::string client_name;
    std::string global_name;
    std::string type_list;
    std::string query;
    std::vector<uint32_t> param_oids;
    bool is_sql_prep = false;
};

/// Pooled PostgreSQL backend connection and its prepared-statement set.
struct BackendConn {
    Stream stream;
    uint32_t backend_pid = 0;
    uint32_t backend_secret = 0;
    uint64_t last_client_id = 0;
    std::chrono::steady_clock::time_point last_used = std::chrono::steady_clock::now();
    std::unordered_set<std::string> prepared_stmts;

    void close() { stream.close(); }
};

namespace sqlparse {

using air::gw::is_session_pinning_sql;
using air::gw::re_adv_lock_stmt;
using air::gw::re_dealloc_stmt;
using air::gw::re_execute_stmt;
using air::gw::re_listen_stmt;
using air::gw::re_prepare_stmt;
using air::gw::re_reset_stmt;
using air::gw::re_set_stmt;
using air::gw::re_temp_table_stmt;
using air::gw::read_cstr;

std::vector<uint8_t> make_parse_body(const std::string& stmt_name,
                                     const std::string& query,
                                     const std::vector<uint32_t>& oids);

std::vector<uint8_t> rewrite_parse_body(const std::vector<uint8_t>& body,
                                        const std::string& global_name,
                                        std::string& client_name_out,
                                        std::string& query_out,
                                        std::vector<uint32_t>& oids_out);

std::vector<uint8_t> rewrite_bind_body(const std::vector<uint8_t>& body,
                                       const std::unordered_map<std::string, PreparedDef>& stmts);

std::vector<uint8_t> rewrite_named_op_body(const std::vector<uint8_t>& body,
                                           const std::unordered_map<std::string, PreparedDef>& stmts,
                                           bool is_close,
                                           std::string& erased_client_name_out);

} // namespace sqlparse

/**
 * Per-tenant transaction-mode PostgreSQL connection pooler.
 * Multiplexes client sessions onto a bounded set of backend connections,
 * replaying session SET variables and prepared statements on demand.
 */
class TenantPool {
public:
    using WakeFn = std::function<bool(TenantRoute& route_out, std::string& err_out)>;

    TenantPool(std::string tenant_id, std::string host, std::string sock_dir,
               int port, std::string app_password, int max_backends,
               int idle_timeout_sec = 300);
    ~TenantPool();

    void set_idle_timeout_sec(int sec);
    size_t evict_expired_idle();
    void set_wake_fn(WakeFn fn);
    void update_endpoint(const std::string& host, const std::string& sock_dir,
                         int port, const std::string& app_password);
    void set_read_engine_endpoint(const std::string& host, const std::string& sock_dir, int port);
    void set_max_backends(int max_backends);

    PoolTenantSnapshot snapshot();
    void close_all_idle();
    bool request_drain(int timeout_ms, std::string& err_out);

    /// Authenticate and serve a single PG client session until disconnect or fatal error.
    void serve_client(Stream& client,
                      const std::string& startup_user,
                      const std::string& expected_pass,
                      const std::function<void()>& touch_activity,
                      MetricsRegistry* metrics = nullptr,
                      CancelRegistry* cancel_reg = nullptr,
                      int client_idle_timeout_ms = 0);

private:
    struct RelayStats {
        uint64_t bytes_out = 0;
        uint64_t rows_returned = 0;
        uint64_t data_row_bytes = 0;
    };

    struct ClientSessionState;

    std::string tenant_id_;
    int max_backends_;
    std::atomic<int> idle_timeout_sec_{300};

    std::mutex mu_;
    std::string host_;
    std::string sock_dir_;
    int port_ = 0;
    std::string app_password_;
    std::vector<std::unique_ptr<BackendConn>> idle_pool_;
    std::string read_host_;
    std::string read_sock_dir_;
    int read_port_ = 0;
    std::vector<std::unique_ptr<BackendConn>> read_idle_pool_;
    RoutingMetrics routing_metrics_;
    WakeFn wake_fn_;

    std::mutex sem_mu_;
    std::condition_variable sem_cv_;
    int active_backends_ = 0;

    std::atomic<uint64_t> stmt_seq_{0};
    std::atomic<uint64_t> client_seq_{0};
    std::atomic<int64_t> pinned_clients_{0};
    std::atomic<int64_t> active_tx_count_{0};
    std::atomic<uint64_t> read_engine_lsn_{0};
    std::atomic<bool> read_engine_healthy_{true};

    bool acquire_slot(std::string& err_out);
    void release_slot();
    std::unique_ptr<BackendConn> acquire_backend(uint64_t client_id, std::string& err_out);
    void release_backend(std::unique_ptr<BackendConn> b, bool dirty);
    std::unique_ptr<BackendConn> acquire_read_backend(uint64_t client_id, std::string& err_out);
    void release_read_backend(std::unique_ptr<BackendConn> b, bool dirty);
    std::unique_ptr<BackendConn> dial_and_startup(const std::string& host,
                                                  const std::string& sock_dir,
                                                  int port,
                                                  const std::string& app_pass,
                                                  uint64_t client_id,
                                                  std::string& err_out);

    bool authenticate_client(Stream& client,
                             const std::string& startup_user,
                             const std::string& expected_pass,
                             uint32_t gw_pid,
                             uint32_t gw_secret,
                             MetricsRegistry* metrics);
    bool ensure_session_and_stmts(BackendConn* b, ClientSessionState& st);
    bool ensure_backend(ClientSessionState& st, uint64_t& pool_wait_us_out);
    void release_current(ClientSessionState& st, bool dirty);
    void update_tx_count(ClientSessionState& st);
    void record_query_metrics(ClientSessionState& st,
                              std::chrono::steady_clock::time_point q_start,
                              uint64_t pool_wait_us,
                              uint64_t bytes_in,
                              const RelayStats& rstats,
                              bool had_err,
                              uint64_t count);

    bool execute_simple_and_relay(ClientSessionState& st,
                                  const std::vector<uint8_t>& wire_msg,
                                  uint64_t bytes_in,
                                  std::chrono::steady_clock::time_point q_start,
                                  bool& had_err_out,
                                  bool& io_ok_out);
    bool try_execute_on_read_engine(ClientSessionState& st,
                                    const std::vector<uint8_t>& wire_msg,
                                    uint64_t bytes_in,
                                    std::chrono::steady_clock::time_point q_start,
                                    bool& executed_out,
                                    bool& io_ok_out);
    bool handle_simple_query(ClientSessionState& st, const std::vector<uint8_t>& mbody);
    bool handle_extended_batch(ClientSessionState& st,
                               char terminator_type,
                               std::vector<uint8_t> terminator_body,
                               std::vector<std::pair<char, std::vector<uint8_t>>>& ext_batch,
                               size_t& ext_batch_bytes);

    static uint64_t parse_pg_lsn(const std::string& s);
    static bool query_lsn_backend(BackendConn* b, const char* sql, uint64_t& lsn_out);
    static bool exec_simple_backend(BackendConn* b, const std::string& sql);
    static bool drain_until_ready_backend(BackendConn* b);
    static bool relay_copy_in(BackendConn* b, Stream& client);
    static bool relay_until_ready(BackendConn* b, Stream& client, char& tx_status,
                                  bool& had_err, RelayStats* stats = nullptr);
    static bool relay_flush_batch(BackendConn* b, Stream& client,
                                  const std::vector<std::pair<char, std::vector<uint8_t>>>& batch,
                                  bool& had_err, RelayStats* stats = nullptr);
};

/**
 * Thread-safe registry of per-tenant TenantPool instances.
 */
class PoolManager {
public:
    explicit PoolManager(int max_backends, int idle_timeout_sec = 300);

    void set_read_engine_endpoint(const std::string& host, const std::string& sock_dir, int port);
    std::shared_ptr<TenantPool> get_or_create(const TenantRoute& route, TenantPool::WakeFn wake_fn);
    size_t evict_expired_idle();
    void upsert_discovered(const TenantRoute& route);
    void remove_tenant(const std::string& tenant_id);
    bool request_drain(const std::string& tenant_id, int timeout_ms, std::string& err_out);
    void set_tenant_max_backends(const std::string& tenant_id, int max_backends);
    std::vector<PoolTenantSnapshot> snapshots() const;
    void close_all();

private:
    int max_backends_;
    int idle_timeout_sec_;
    mutable std::mutex mu_;
    std::string read_host_;
    std::string read_sock_dir_;
    int read_port_ = 0;
    std::unordered_map<std::string, std::shared_ptr<TenantPool>> pools_;
};

} // namespace air::gw
