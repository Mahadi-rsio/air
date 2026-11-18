#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace air::gw {

inline std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += static_cast<char>(c);
                }
                break;
        }
    }
    return out;
}

inline int64_t now_epoch_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

struct LatencyHistogram {
    static constexpr size_t kNumBuckets = 12;
    static constexpr std::array<uint64_t, kNumBuckets> kUpperBoundsUs{
        500, 1000, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000, 5000000,
        std::numeric_limits<uint64_t>::max()};

    std::array<uint64_t, kNumBuckets> buckets{};

    void record(uint64_t us) {
        for (size_t i = 0; i < kNumBuckets; ++i) {
            if (us <= kUpperBoundsUs[i]) {
                ++buckets[i];
                return;
            }
        }
    }

    uint64_t percentile_us(double p) const {
        uint64_t total = 0;
        for (auto c : buckets) total += c;
        if (total == 0) return 0;
        uint64_t target = static_cast<uint64_t>(p * static_cast<double>(total));
        if (target == 0) target = 1;
        uint64_t cum = 0;
        for (size_t i = 0; i < kNumBuckets; ++i) {
            cum += buckets[i];
            if (cum >= target) {
                return (i == kNumBuckets - 1) ? 5000000 : kUpperBoundsUs[i];
            }
        }
        return 5000000;
    }
};

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
                bool had_err, uint64_t count, int64_t ts_ms) {
        uint64_t inc = count > 0 ? count : 1;
        query_count += inc;
        if (had_err) error_count += 1;
        bytes_in += in_bytes;
        bytes_out += out_bytes;
        rows_returned += rows;
        data_row_bytes += row_bytes;
        total_query_us += duration_us;
        total_pool_wait_us += pool_wait_us;
        if (query_count == inc || duration_us < min_query_us) {
            min_query_us = duration_us;
        }
        if (duration_us > max_query_us) {
            max_query_us = duration_us;
        }
        hist.record(duration_us);
        last_query_at_ms = ts_ms;
    }

    uint64_t avg_query_us() const {
        return query_count > 0 ? (total_query_us / query_count) : 0;
    }

    uint64_t avg_pool_wait_us() const {
        return query_count > 0 ? (total_pool_wait_us / query_count) : 0;
    }

    void append_json_fields(std::ostringstream& out) const {
        out << "\"query_count\":" << query_count
            << ",\"error_count\":" << error_count
            << ",\"bytes_in\":" << bytes_in
            << ",\"bytes_out\":" << bytes_out
            << ",\"rows_returned\":" << rows_returned
            << ",\"data_row_bytes\":" << data_row_bytes
            << ",\"total_query_us\":" << total_query_us
            << ",\"avg_query_us\":" << avg_query_us()
            << ",\"min_query_us\":" << min_query_us
            << ",\"max_query_us\":" << max_query_us
            << ",\"p50_query_us\":" << hist.percentile_us(0.50)
            << ",\"p95_query_us\":" << hist.percentile_us(0.95)
            << ",\"p99_query_us\":" << hist.percentile_us(0.99)
            << ",\"total_pool_wait_us\":" << total_pool_wait_us
            << ",\"avg_pool_wait_us\":" << avg_pool_wait_us()
            << ",\"last_query_at_ms\":" << last_query_at_ms;
    }
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

class MetricsRegistry {
public:
    MetricsRegistry(int max_global_conns = 1024,
                    int max_tenant_conns = 128,
                    int max_user_conns = 0)
        : max_global_conns_(max_global_conns),
          max_tenant_conns_(max_tenant_conns),
          max_user_conns_(max_user_conns),
          started_at_(std::chrono::steady_clock::now()) {}

    bool try_acquire_global() {
        std::lock_guard<std::mutex> lk(mu_);
        if (max_global_conns_ > 0 && active_global_conns_ >= max_global_conns_) {
            ++rejected_global_conns_;
            return false;
        }
        ++active_global_conns_;
        ++total_global_conns_;
        return true;
    }

    void release_global() {
        std::lock_guard<std::mutex> lk(mu_);
        if (active_global_conns_ > 0) --active_global_conns_;
    }

    void record_auth_failure() { auth_failures_.fetch_add(1, std::memory_order_relaxed); }
    void record_protocol_error() { protocol_errors_.fetch_add(1, std::memory_order_relaxed); }
    void record_rpc_error() { rpc_errors_.fetch_add(1, std::memory_order_relaxed); }
    void record_timeout() { timeouts_.fetch_add(1, std::memory_order_relaxed); }
    void record_pool_exhausted() { pool_exhausted_.fetch_add(1, std::memory_order_relaxed); }

    bool try_acquire_tenant_user(const std::string& tenant_id,
                                 const std::string& user,
                                 std::string& err_out) {
        std::lock_guard<std::mutex> lk(mu_);
        auto& t = tenants_[tenant_id];
        if (t.tenant_id.empty()) t.tenant_id = tenant_id;

        std::string ukey = tenant_id + '\0' + user;
        auto& u = users_[ukey];
        if (u.tenant_id.empty()) {
            u.tenant_id = tenant_id;
            u.user = user;
        }

        int t_limit = effective_tenant_limit_locked(tenant_id);
        if (t_limit > 0 && t.active_connections >= t_limit) {
            ++t.rejected_connections;
            ++u.rejected_connections;
            ++rejected_global_conns_;
            err_out = "too many client connections for tenant \"" + tenant_id +
                      "\" (limit=" + std::to_string(t_limit) + ")";
            return false;
        }

        if (max_user_conns_ > 0 && u.active_connections >= max_user_conns_) {
            ++t.rejected_connections;
            ++u.rejected_connections;
            ++rejected_global_conns_;
            err_out = "too many client connections for user \"" + user +
                      "\" on tenant \"" + tenant_id +
                      "\" (limit=" + std::to_string(max_user_conns_) + ")";
            return false;
        }

        ++t.active_connections;
        ++t.total_connections;
        ++u.active_connections;
        ++u.total_connections;
        return true;
    }

    void release_tenant_user(const std::string& tenant_id, const std::string& user) {
        std::lock_guard<std::mutex> lk(mu_);
        if (auto it = tenants_.find(tenant_id); it != tenants_.end()) {
            if (it->second.active_connections > 0) --it->second.active_connections;
        }
        std::string ukey = tenant_id + '\0' + user;
        if (auto it = users_.find(ukey); it != users_.end()) {
            if (it->second.active_connections > 0) --it->second.active_connections;
        }
    }

    void set_tenant_client_limit(const std::string& tenant_id, int max_conns) {
        std::lock_guard<std::mutex> lk(mu_);
        if (max_conns <= 0) {
            tenant_limit_overrides_.erase(tenant_id);
        } else {
            tenant_limit_overrides_[tenant_id] = max_conns;
        }
    }

    int get_tenant_client_limit(const std::string& tenant_id) const {
        std::lock_guard<std::mutex> lk(mu_);
        return effective_tenant_limit_locked(tenant_id);
    }

    void record_query(const std::string& tenant_id,
                      const std::string& user,
                      uint64_t duration_us,
                      uint64_t pool_wait_us,
                      uint64_t bytes_in,
                      uint64_t bytes_out,
                      uint64_t rows_returned,
                      uint64_t data_row_bytes,
                      bool had_err,
                      uint64_t count = 1) {
        int64_t ts = now_epoch_ms();
        std::lock_guard<std::mutex> lk(mu_);
        global_queries_.record(duration_us, pool_wait_us, bytes_in, bytes_out,
                               rows_returned, data_row_bytes, had_err, count, ts);

        auto& t = tenants_[tenant_id];
        if (t.tenant_id.empty()) t.tenant_id = tenant_id;
        t.queries.record(duration_us, pool_wait_us, bytes_in, bytes_out,
                         rows_returned, data_row_bytes, had_err, count, ts);

        std::string ukey = tenant_id + '\0' + user;
        auto& u = users_[ukey];
        if (u.tenant_id.empty()) {
            u.tenant_id = tenant_id;
            u.user = user;
        }
        u.queries.record(duration_us, pool_wait_us, bytes_in, bytes_out,
                         rows_returned, data_row_bytes, had_err, count, ts);
    }

    std::string to_json(const std::vector<PoolTenantSnapshot>& pool_snaps,
                        const std::string& tenant_filter = "",
                        const std::string& user_filter = "") const {
        std::unordered_map<std::string, PoolTenantSnapshot> pmap;
        for (const auto& ps : pool_snaps) pmap[ps.tenant_id] = ps;

        std::lock_guard<std::mutex> lk(mu_);
        int64_t uptime_s = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now() - started_at_)
                               .count();

        std::ostringstream out;
        out << "{\"uptime_seconds\":" << uptime_s
            << ",\"limits\":{\"max_client_conns\":" << max_global_conns_
            << ",\"max_tenant_client_conns\":" << max_tenant_conns_
            << ",\"max_user_client_conns\":" << max_user_conns_ << "}"
            << ",\"global\":{\"active_connections\":" << active_global_conns_
            << ",\"total_connections\":" << total_global_conns_
            << ",\"rejected_connections\":" << rejected_global_conns_
            << ",\"auth_failures\":" << auth_failures_.load(std::memory_order_relaxed)
            << ",\"protocol_errors\":" << protocol_errors_.load(std::memory_order_relaxed)
            << ",\"rpc_errors\":" << rpc_errors_.load(std::memory_order_relaxed)
            << ",\"timeouts\":" << timeouts_.load(std::memory_order_relaxed)
            << ",\"pool_exhausted\":" << pool_exhausted_.load(std::memory_order_relaxed) << ",";
        global_queries_.append_json_fields(out);
        out << "},\"tenants\":[";

        bool first_t = true;
        for (const auto& [tid, t] : tenants_) {
            if (!tenant_filter.empty() && tid != tenant_filter) continue;
            if (!first_t) out << ",";
            first_t = false;
            PoolTenantSnapshot ps{};
            if (auto it = pmap.find(tid); it != pmap.end()) ps = it->second;
            out << "{\"tenant_id\":\"" << json_escape(tid) << "\""
                << ",\"max_client_conns\":" << effective_tenant_limit_locked(tid)
                << ",\"active_connections\":" << t.active_connections
                << ",\"total_connections\":" << t.total_connections
                << ",\"rejected_connections\":" << t.rejected_connections
                << ",\"active_backends\":" << ps.active_backends
                << ",\"max_backends\":" << ps.max_backends
                << ",\"idle_backends\":" << ps.idle_backends
                << ",\"pinned_clients\":" << ps.pinned_clients
                << ",\"active_transactions\":" << ps.active_transactions << ",";
            t.queries.append_json_fields(out);
            out << "}";
        }
        out << "],\"users\":[";

        bool first_u = true;
        for (const auto& [_, u] : users_) {
            if (!tenant_filter.empty() && u.tenant_id != tenant_filter) continue;
            if (!user_filter.empty() && u.user != user_filter) continue;
            if (!first_u) out << ",";
            first_u = false;
            out << "{\"tenant_id\":\"" << json_escape(u.tenant_id) << "\""
                << ",\"user\":\"" << json_escape(u.user) << "\""
                << ",\"active_connections\":" << u.active_connections
                << ",\"total_connections\":" << u.total_connections
                << ",\"rejected_connections\":" << u.rejected_connections << ",";
            u.queries.append_json_fields(out);
            out << "}";
        }
        out << "]}";
        return out.str();
    }

    std::string to_prometheus(const std::vector<PoolTenantSnapshot>& pool_snaps) const {
        std::lock_guard<std::mutex> lk(mu_);
        std::ostringstream out;
        out << std::fixed << std::setprecision(6);

        out << "# HELP air_gateway_connections_active Active client connections\n"
            << "# TYPE air_gateway_connections_active gauge\n"
            << "air_gateway_connections_active " << active_global_conns_ << "\n";

        out << "# HELP air_gateway_connections_rejected_total Total rejected client connections\n"
            << "# TYPE air_gateway_connections_rejected_total counter\n"
            << "air_gateway_connections_rejected_total " << rejected_global_conns_ << "\n";

        out << "# HELP air_gateway_queries_total Total queries executed\n"
            << "# TYPE air_gateway_queries_total counter\n";
        for (const auto& [_, u] : users_) {
            out << "air_gateway_queries_total{tenant=\"" << json_escape(u.tenant_id)
                << "\",user=\"" << json_escape(u.user) << "\"} " << u.queries.query_count << "\n";
        }

        out << "# HELP air_gateway_query_errors_total Total failed queries\n"
            << "# TYPE air_gateway_query_errors_total counter\n";
        for (const auto& [_, u] : users_) {
            out << "air_gateway_query_errors_total{tenant=\"" << json_escape(u.tenant_id)
                << "\",user=\"" << json_escape(u.user) << "\"} " << u.queries.error_count << "\n";
        }

        out << "# HELP air_gateway_query_duration_seconds_sum Total query execution time in seconds\n"
            << "# TYPE air_gateway_query_duration_seconds_sum counter\n";
        for (const auto& [_, u] : users_) {
            out << "air_gateway_query_duration_seconds_sum{tenant=\"" << json_escape(u.tenant_id)
                << "\",user=\"" << json_escape(u.user) << "\"} "
                << (static_cast<double>(u.queries.total_query_us) / 1e6) << "\n";
        }

        out << "# HELP air_gateway_bytes_in_total Total query bytes received from clients\n"
            << "# TYPE air_gateway_bytes_in_total counter\n";
        for (const auto& [_, u] : users_) {
            out << "air_gateway_bytes_in_total{tenant=\"" << json_escape(u.tenant_id)
                << "\",user=\"" << json_escape(u.user) << "\"} " << u.queries.bytes_in << "\n";
        }

        out << "# HELP air_gateway_bytes_out_total Total response bytes sent to clients\n"
            << "# TYPE air_gateway_bytes_out_total counter\n";
        for (const auto& [_, u] : users_) {
            out << "air_gateway_bytes_out_total{tenant=\"" << json_escape(u.tenant_id)
                << "\",user=\"" << json_escape(u.user) << "\"} " << u.queries.bytes_out << "\n";
        }

        out << "# HELP air_gateway_rows_returned_total Total DataRow rows returned to clients\n"
            << "# TYPE air_gateway_rows_returned_total counter\n";
        for (const auto& [_, u] : users_) {
            out << "air_gateway_rows_returned_total{tenant=\"" << json_escape(u.tenant_id)
                << "\",user=\"" << json_escape(u.user) << "\"} " << u.queries.rows_returned << "\n";
        }

        out << "# HELP air_gateway_pool_backends_active Active pooled backend connections\n"
            << "# TYPE air_gateway_pool_backends_active gauge\n";
        for (const auto& ps : pool_snaps) {
            out << "air_gateway_pool_backends_active{tenant=\"" << json_escape(ps.tenant_id)
                << "\"} " << ps.active_backends << "\n";
        }

        out << "# HELP air_gateway_auth_failures_total Total authentication failures\n"
            << "# TYPE air_gateway_auth_failures_total counter\n"
            << "air_gateway_auth_failures_total " << auth_failures_.load(std::memory_order_relaxed) << "\n"
            << "# HELP air_gateway_protocol_errors_total Total PostgreSQL wire protocol errors\n"
            << "# TYPE air_gateway_protocol_errors_total counter\n"
            << "air_gateway_protocol_errors_total " << protocol_errors_.load(std::memory_order_relaxed) << "\n"
            << "# HELP air_gateway_rpc_errors_total Total Control Plane RPC failures\n"
            << "# TYPE air_gateway_rpc_errors_total counter\n"
            << "air_gateway_rpc_errors_total " << rpc_errors_.load(std::memory_order_relaxed) << "\n"
            << "# HELP air_gateway_timeouts_total Total gateway timeouts\n"
            << "# TYPE air_gateway_timeouts_total counter\n"
            << "air_gateway_timeouts_total " << timeouts_.load(std::memory_order_relaxed) << "\n"
            << "# HELP air_gateway_pool_exhausted_total Total backend pool saturation events\n"
            << "# TYPE air_gateway_pool_exhausted_total counter\n"
            << "air_gateway_pool_exhausted_total " << pool_exhausted_.load(std::memory_order_relaxed) << "\n";

        return out.str();
    }

private:
    int effective_tenant_limit_locked(const std::string& tenant_id) const {
        if (auto it = tenant_limit_overrides_.find(tenant_id); it != tenant_limit_overrides_.end()) {
            return it->second;
        }
        return max_tenant_conns_;
    }

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
