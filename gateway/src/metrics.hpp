#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace air::gw {

/// Escape a string for safe embedding in JSON or Prometheus label values.
std::string json_escape(const std::string& s);

/// Return current wall-clock time in milliseconds since Unix epoch.
int64_t now_epoch_ms();

/**
 * Fixed-bucket microsecond latency histogram for p50/p95/p99 estimation.
 */
struct LatencyHistogram {
    static constexpr size_t kNumBuckets = 12;
    static constexpr std::array<uint64_t, kNumBuckets> kUpperBoundsUs{
        500, 1000, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000, 5000000,
        std::numeric_limits<uint64_t>::max()};

    std::array<uint64_t, kNumBuckets> buckets{};

    void record(uint64_t us);
    uint64_t percentile_us(double p) const;
};

/**
 * Aggregated query execution, byte/row volume, and latency statistics.
 */
struct QueryStats {
    uint64_t query_count = 0;
    uint64_t error_count = 0;
    uint64_t bytes_in = 0;
    uint64_t bytes_out = 0;
    uint64_t rows_returned = 0;
    uint64_t data_row_bytes = 0;
    uint64_t total_query_us = 0;
    uint64_t min_query_us = 0;
    uint64_t max_query_us = 0;
    uint64_t total_pool_wait_us = 0;
    LatencyHistogram hist;
    int64_t last_query_at_ms = 0;

    void record(uint64_t duration_us, uint64_t pool_wait_us, uint64_t in_bytes,
                uint64_t out_bytes, uint64_t rows, uint64_t row_bytes,
                bool had_err, uint64_t count, int64_t ts_ms);

    uint64_t avg_query_us() const {
        return query_count > 0 ? (total_query_us / query_count) : 0;
    }

    uint64_t avg_pool_wait_us() const {
        return query_count > 0 ? (total_pool_wait_us / query_count) : 0;
    }

    void append_json_fields(std::ostringstream& out) const;
};

struct PoolTenantSnapshot {
    std::string tenant_id;
    int active_backends = 0;
    int max_backends = 0;
    size_t idle_backends = 0;
    int64_t pinned_clients = 0;
    int64_t active_transactions = 0;
};

struct TenantStats {
    std::string tenant_id;
    int64_t active_connections = 0;
    uint64_t total_connections = 0;
    uint64_t rejected_connections = 0;
    QueryStats queries;
};

struct UserStats {
    std::string tenant_id;
    std::string user;
    int64_t active_connections = 0;
    uint64_t total_connections = 0;
    uint64_t rejected_connections = 0;
    QueryStats queries;
};

/**
 * Thread-safe registry for connection limits, error/timeout counters,
 * per-tenant/per-user query metrics, and JSON/Prometheus rendering.
 */
class MetricsRegistry {
public:
    MetricsRegistry(int max_global_conns = 1024,
                    int max_tenant_conns = 128,
                    int max_user_conns = 0);

    bool try_acquire_global();
    void release_global();

    void record_auth_failure() { auth_failures_.fetch_add(1, std::memory_order_relaxed); }
    void record_protocol_error() { protocol_errors_.fetch_add(1, std::memory_order_relaxed); }
    void record_rpc_error() { rpc_errors_.fetch_add(1, std::memory_order_relaxed); }
    void record_timeout() { timeouts_.fetch_add(1, std::memory_order_relaxed); }
    void record_pool_exhausted() { pool_exhausted_.fetch_add(1, std::memory_order_relaxed); }

    bool try_acquire_tenant_user(const std::string& tenant_id,
                                 const std::string& user,
                                 std::string& err_out);
    void release_tenant_user(const std::string& tenant_id, const std::string& user);

    void set_tenant_client_limit(const std::string& tenant_id, int max_conns);
    int get_tenant_client_limit(const std::string& tenant_id) const;

    void record_query(const std::string& tenant_id,
                      const std::string& user,
                      uint64_t duration_us,
                      uint64_t pool_wait_us,
                      uint64_t bytes_in,
                      uint64_t bytes_out,
                      uint64_t rows_returned,
                      uint64_t data_row_bytes,
                      bool had_err,
                      uint64_t count = 1);

    std::string to_json(const std::vector<PoolTenantSnapshot>& pool_snaps,
                        const std::string& tenant_filter = "",
                        const std::string& user_filter = "") const;
    std::string to_prometheus(const std::vector<PoolTenantSnapshot>& pool_snaps) const;

private:
    int effective_tenant_limit_locked(const std::string& tenant_id) const;

    int max_global_conns_;
    int max_tenant_conns_;
    int max_user_conns_;
    std::chrono::steady_clock::time_point started_at_;

    std::atomic<uint64_t> auth_failures_{0};
    std::atomic<uint64_t> protocol_errors_{0};
    std::atomic<uint64_t> rpc_errors_{0};
    std::atomic<uint64_t> timeouts_{0};
    std::atomic<uint64_t> pool_exhausted_{0};

    mutable std::mutex mu_;
    int64_t active_global_conns_ = 0;
    uint64_t total_global_conns_ = 0;
    uint64_t rejected_global_conns_ = 0;
    QueryStats global_queries_;
    std::unordered_map<std::string, TenantStats> tenants_;
    std::unordered_map<std::string, UserStats> users_;
    std::unordered_map<std::string, int> tenant_limit_overrides_;
};

} // namespace air::gw
