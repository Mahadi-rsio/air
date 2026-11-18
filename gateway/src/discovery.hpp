#pragma once

#include "config.hpp"
#include "metrics.hpp"
#include "pb/airdb.grpc.pb.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <grpcpp/grpcpp.h>
#include <libpq-fe.h>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace air::gw {

/**
 * Cached routing metadata and backend credentials for a single tenant database.
 */
struct TenantRoute {
    std::string tenant_id;
    std::string state;
    std::string host;
    int port = 0;
    std::string sock_dir;
    std::string app_password;
};

namespace detail {

std::string json_extract_string(const std::string& json, const std::string& key);
int json_extract_int(const std::string& json, const std::string& key);
bool parse_route_json(const std::string& body, TenantRoute& out);

} // namespace detail

/**
 * Maintains the live tenant routing table via Control Plane gRPC (ListDatabases,
 * WatchTenantChanges stream, GetTenantRoute, WakeTenant, BatchTouchActivity) with
 * optional PostgreSQL metadata-DB fallback polling.
 */
class TenantDiscovery {
public:
    using TenantCallback = std::function<void(const TenantRoute&)>;
    using TenantRemovedCallback = std::function<void(const std::string&)>;

    static std::shared_ptr<grpc::Channel> make_channel(const std::string& addr);

    explicit TenantDiscovery(const Config& cfg);
    ~TenantDiscovery();

    TenantDiscovery(const TenantDiscovery&) = delete;
    TenantDiscovery& operator=(const TenantDiscovery&) = delete;

    void set_metrics(MetricsRegistry* metrics) { metrics_ = metrics; }
    void set_callbacks(TenantCallback on_upsert, TenantRemovedCallback on_remove);

    void start();
    void stop();

    std::vector<TenantRoute> list_cached() const;

    /// Resolve a tenant route from cache or Control Plane gRPC, waking SUSPENDED tenants if needed.
    bool resolve_tenant(const std::string& tenant_id,
                        TenantRoute& route_out,
                        std::string& err_out,
                        grpc::StatusCode* grpc_code_out = nullptr);

    /// Explicitly wake a SUSPENDED tenant via Control Plane gRPC.
    bool wake_tenant(const std::string& tenant_id, TenantRoute& route_out, std::string& err_out);

    // --- Admin HTTP -> Control Plane gRPC proxy methods ---
    bool create_database(const std::string& tenant_id,
                         const std::string& account_id,
                         const std::string& idem_key,
                         int& http_code_out,
                         std::string& json_out);
    bool list_databases(const std::string& account_id, int& http_code_out, std::string& json_out);
    bool get_database(const std::string& tenant_id, int& http_code_out, std::string& json_out);
    bool delete_database(const std::string& tenant_id, int& http_code_out, std::string& json_out);
    bool suspend_database(const std::string& tenant_id, int& http_code_out, std::string& json_out);
    bool start_database(const std::string& tenant_id, int& http_code_out, std::string& json_out);
    bool lock_database(const std::string& tenant_id, int& http_code_out, std::string& json_out);
    bool unlock_database(const std::string& tenant_id, int& http_code_out, std::string& json_out);

    void set_cached_state(const std::string& tenant_id, const std::string& state);
    void evict_cache(const std::string& tenant_id) { remove_cache(tenant_id); }
    void upsert_route(const TenantRoute& r) { upsert_cache(r); }

    /// Debounce and enqueue an activity heartbeat for `tenant_id` (scale-to-zero keepalive).
    void touch_activity(const std::string& tenant_id);

private:
    Config cfg_;
    std::shared_ptr<grpc::Channel> channel_;
    std::shared_ptr<air::db::v1::AirDBControl::Stub> stub_;
    MetricsRegistry* metrics_ = nullptr;

    std::atomic<bool> running_{false};
    std::thread worker_;
    std::mutex cv_mu_;
    std::condition_variable cv_;

    std::mutex pq_mu_;
    PGconn* pg_conn_ = nullptr;

    mutable std::shared_mutex cache_mu_;
    std::unordered_map<std::string, TenantRoute> cache_;

    std::mutex watch_ctx_mu_;
    grpc::ClientContext* watch_ctx_ = nullptr;
    int64_t last_event_version_ = 0;

    std::mutex cb_mu_;
    TenantCallback on_upsert_;
    TenantRemovedCallback on_remove_;

    std::mutex touch_mu_;
    std::condition_variable touch_cv_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_touch_;
    std::unordered_set<std::string> pending_touches_;
    std::thread touch_worker_;

    void prepare_ctx(grpc::ClientContext& ctx, int timeout_sec) const;
    TenantRoute from_proto_route(const air::db::v1::TenantRouteResponse& resp) const;
    static int grpc_to_http(grpc::StatusCode c);
    static void append_db_info_json(std::ostringstream& oss, const air::db::v1::DatabaseInfo& d);

    template <typename RpcFn>
    bool call_tenant_status_rpc(const std::string& tenant_id,
                                int timeout_sec,
                                const char* new_cached_state,
                                RpcFn rpc_fn,
                                int& http_code_out,
                                std::string& json_out);

    void touch_loop();
    bool sync_databases_grpc();
    void watch_tenant_stream();
    void discovery_loop();
    void upsert_cache(const TenantRoute& r);
    void remove_cache(const std::string& tenant_id);
    void poll_metadata_once();
    void reconcile_snapshot(const std::vector<TenantRoute>& discovered);
};

} // namespace air::gw
