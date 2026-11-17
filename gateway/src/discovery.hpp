#pragma once

#include "config.hpp"
#include "metrics.hpp"
#include "pb/airdb.grpc.pb.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <libpq-fe.h>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace air::gw {

struct TenantRoute {
    std::string tenant_id;
    std::string state;
    std::string host;
    int port = 0;
    std::string sock_dir;
    std::string app_password;
};

namespace detail {

inline std::string json_extract_string(const std::string& json, const std::string& key) {
    std::string pat = "\"" + key + "\"";
    auto pos = json.find(pat);
    if (pos == std::string::npos) return "";
    pos = json.find(':', pos + pat.size());
    if (pos == std::string::npos) return "";
    ++pos;
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' || json[pos] == '\n' || json[pos] == '\r')) {
        ++pos;
    }
    if (pos >= json.size() || json[pos] != '"') return "";
    ++pos;
    std::string val;
    while (pos < json.size()) {
        char ch = json[pos++];
        if (ch == '\\' && pos < json.size()) {
            char esc = json[pos++];
            switch (esc) {
                case '"': val.push_back('"'); break;
                case '\\': val.push_back('\\'); break;
                case '/': val.push_back('/'); break;
                case 'n': val.push_back('\n'); break;
                case 'r': val.push_back('\r'); break;
                case 't': val.push_back('\t'); break;
                default: val.push_back(esc); break;
            }
        } else if (ch == '"') {
            break;
        } else {
            val.push_back(ch);
        }
    }
    return val;
}

inline int json_extract_int(const std::string& json, const std::string& key) {
    std::string pat = "\"" + key + "\"";
    auto pos = json.find(pat);
    if (pos == std::string::npos) return 0;
    pos = json.find(':', pos + pat.size());
    if (pos == std::string::npos) return 0;
    ++pos;
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) ++pos;
    size_t end = pos;
    if (end < json.size() && json[end] == '-') ++end;
    while (end < json.size() && std::isdigit(static_cast<unsigned char>(json[end]))) ++end;
    if (end == pos) return 0;
    try {
        return std::stoi(json.substr(pos, end - pos));
    } catch (...) {
        return 0;
    }
}

inline bool parse_route_json(const std::string& body, TenantRoute& out) {
    out.tenant_id = json_extract_string(body, "tenant_id");
    out.state = json_extract_string(body, "state");
    out.host = json_extract_string(body, "host");
    out.port = json_extract_int(body, "port");
    out.sock_dir = json_extract_string(body, "sock_dir");
    out.app_password = json_extract_string(body, "app_password");
    return !out.tenant_id.empty();
}

} // namespace detail

class TenantDiscovery {
public:
    using TenantCallback = std::function<void(const TenantRoute&)>;
    using TenantRemovedCallback = std::function<void(const std::string&)>;

    explicit TenantDiscovery(const Config& cfg)
        : cfg_(cfg),
          channel_(grpc::CreateChannel(cfg_.grpc_addr, grpc::InsecureChannelCredentials())),
          stub_(air::db::v1::AirDBControl::NewStub(channel_)) {}

    ~TenantDiscovery() {
        stop();
        if (pg_conn_) {
            PQfinish(pg_conn_);
            pg_conn_ = nullptr;
        }
    }

    TenantDiscovery(const TenantDiscovery&) = delete;
    TenantDiscovery& operator=(const TenantDiscovery&) = delete;

    void set_callbacks(TenantCallback on_upsert, TenantRemovedCallback on_remove) {
        std::lock_guard<std::mutex> lk(cb_mu_);
        on_upsert_ = std::move(on_upsert);
        on_remove_ = std::move(on_remove);
    }

    void start() {
        running_.store(true);
        // Initial synchronous discovery attempt (non-fatal if metadata PG is still booting).
        poll_metadata_once();
        worker_ = std::thread([this]() { discovery_loop(); });
    }

    void stop() {
        running_.store(false);
        cv_.notify_all();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    std::vector<TenantRoute> list_cached() const {
        std::lock_guard<std::mutex> lk(cache_mu_);
        std::vector<TenantRoute> out;
        out.reserve(cache_.size());
        for (const auto& [_, r] : cache_) {
            out.push_back(r);
        }
        return out;
    }

    bool resolve_tenant(const std::string& tenant_id, TenantRoute& route_out, std::string& err_out) {
        // Check metadata DB on cache miss so newly created DBs are discovered immediately.
        bool known = false;
        {
            std::lock_guard<std::mutex> lk(cache_mu_);
            known = cache_.find(tenant_id) != cache_.end();
        }
        if (!known) {
            poll_metadata_once();
        }

        air::db::v1::TenantRequest req;
        req.set_tenant_id(tenant_id);
        air::db::v1::TenantRouteResponse resp;
        grpc::ClientContext ctx;
        prepare_ctx(ctx, 10);
        grpc::Status st = stub_->GetTenantRoute(&ctx, req, &resp);
        if (!st.ok()) {
            err_out = st.error_message();
            return false;
        }
        route_out = from_proto_route(resp);
        if (route_out.state == "SUSPENDED") {
            if (!wake_tenant(tenant_id, route_out, err_out)) {
                return false;
            }
        }
        upsert_cache(route_out);
        return true;
    }

    bool wake_tenant(const std::string& tenant_id, TenantRoute& route_out, std::string& err_out) {
        air::db::v1::TenantRequest req;
        req.set_tenant_id(tenant_id);
        air::db::v1::TenantRouteResponse resp;
        grpc::ClientContext ctx;
        prepare_ctx(ctx, 35);
        grpc::Status st = stub_->WakeTenant(&ctx, req, &resp);
        if (!st.ok()) {
            err_out = st.error_message();
            return false;
        }
        route_out = from_proto_route(resp);
        if (route_out.state.empty()) route_out.state = "ACTIVE";
        upsert_cache(route_out);
        return true;
    }

    bool create_database(const std::string& tenant_id,
                         const std::string& account_id,
                         const std::string& idem_key,
                         int& http_code_out,
                         std::string& json_out) {
        air::db::v1::CreateDatabaseRequest req;
        req.set_tenant_id(tenant_id);
        req.set_account_id(account_id);
        req.set_idempotency_key(idem_key);
        air::db::v1::CreateDatabaseResponse resp;
        grpc::ClientContext ctx;
        prepare_ctx(ctx, 60);
        grpc::Status st = stub_->CreateDatabase(&ctx, req, &resp);
        if (!st.ok()) {
            http_code_out = grpc_to_http(st.error_code());
            json_out = std::string(R"({"error":")") + json_escape(st.error_message()) + R"("})";
            return false;
        }
        http_code_out = 201;
        std::ostringstream oss;
        oss << "{\"tenant_id\":\"" << json_escape(resp.tenant_id())
            << "\",\"state\":\"" << json_escape(resp.state())
            << "\",\"direct_port\":" << resp.direct_port()
            << ",\"host\":\"" << json_escape(resp.host())
            << "\",\"user\":\"" << json_escape(resp.user())
            << "\",\"password\":\"" << json_escape(resp.password()) << "\"}";
        json_out = oss.str();
        TenantRoute r;
        std::string rerr;
        resolve_tenant(resp.tenant_id(), r, rerr);
        return true;
    }

    bool list_databases(const std::string& account_id, int& http_code_out, std::string& json_out) {
        air::db::v1::ListDatabasesRequest req;
        req.set_account_id(account_id);
        air::db::v1::ListDatabasesResponse resp;
        grpc::ClientContext ctx;
        prepare_ctx(ctx, 15);
        grpc::Status st = stub_->ListDatabases(&ctx, req, &resp);
        if (!st.ok()) {
            http_code_out = grpc_to_http(st.error_code());
            json_out = std::string(R"({"error":")") + json_escape(st.error_message()) + R"("})";
            return false;
        }
        http_code_out = 200;
        std::ostringstream oss;
        oss << "{\"databases\":[";
        for (int i = 0; i < resp.databases_size(); ++i) {
            if (i > 0) oss << ",";
            append_db_info_json(oss, resp.databases(i));
        }
        oss << "]}";
        json_out = oss.str();
        return true;
    }

    bool get_database(const std::string& tenant_id, int& http_code_out, std::string& json_out) {
        air::db::v1::TenantRequest req;
        req.set_tenant_id(tenant_id);
        air::db::v1::DatabaseInfo resp;
        grpc::ClientContext ctx;
        prepare_ctx(ctx, 15);
        grpc::Status st = stub_->GetDatabase(&ctx, req, &resp);
        if (!st.ok()) {
            http_code_out = grpc_to_http(st.error_code());
            json_out = std::string(R"({"error":")") + json_escape(st.error_message()) + R"("})";
            return false;
        }
        http_code_out = 200;
        std::ostringstream oss;
        append_db_info_json(oss, resp);
        json_out = oss.str();
        return true;
    }

    bool delete_database(const std::string& tenant_id, int& http_code_out, std::string& json_out) {
        air::db::v1::TenantRequest req;
        req.set_tenant_id(tenant_id);
        air::db::v1::StatusResponse resp;
        grpc::ClientContext ctx;
        prepare_ctx(ctx, 30);
        grpc::Status st = stub_->DeleteDatabase(&ctx, req, &resp);
        if (!st.ok()) {
            http_code_out = grpc_to_http(st.error_code());
            json_out = std::string(R"({"error":")") + json_escape(st.error_message()) + R"("})";
            return false;
        }
        http_code_out = 200;
        json_out = std::string(R"({"status":")") + json_escape(resp.status()) +
                   R"(","tenant_id":")" + json_escape(resp.tenant_id()) + R"("})";
        return true;
    }

    bool suspend_database(const std::string& tenant_id, int& http_code_out, std::string& json_out) {
        air::db::v1::TenantRequest req;
        req.set_tenant_id(tenant_id);
        air::db::v1::StatusResponse resp;
        grpc::ClientContext ctx;
        prepare_ctx(ctx, 30);
        grpc::Status st = stub_->SuspendDatabase(&ctx, req, &resp);
        if (!st.ok()) {
            http_code_out = grpc_to_http(st.error_code());
            json_out = std::string(R"({"error":")") + json_escape(st.error_message()) + R"("})";
            return false;
        }
        http_code_out = 200;
        set_cached_state(tenant_id, "SUSPENDED");
        json_out = std::string(R"({"status":")") + json_escape(resp.status()) +
                   R"(","tenant_id":")" + json_escape(resp.tenant_id()) + R"("})";
        return true;
    }

    bool start_database(const std::string& tenant_id, int& http_code_out, std::string& json_out) {
        air::db::v1::TenantRequest req;
        req.set_tenant_id(tenant_id);
        air::db::v1::TenantRouteResponse resp;
        grpc::ClientContext ctx;
        prepare_ctx(ctx, 35);
        grpc::Status st = stub_->StartDatabase(&ctx, req, &resp);
        if (!st.ok()) {
            http_code_out = grpc_to_http(st.error_code());
            json_out = std::string(R"({"error":")") + json_escape(st.error_message()) + R"("})";
            return false;
        }
        TenantRoute r = from_proto_route(resp);
        if (r.state.empty()) r.state = "ACTIVE";
        upsert_cache(r);
        http_code_out = 200;
        std::ostringstream oss;
        oss << "{\"tenant_id\":\"" << json_escape(r.tenant_id)
            << "\",\"state\":\"" << json_escape(r.state)
            << "\",\"host\":\"" << json_escape(r.host)
            << "\",\"port\":" << r.port << "}";
        json_out = oss.str();
        return true;
    }

    bool lock_database(const std::string& tenant_id, int& http_code_out, std::string& json_out) {
        air::db::v1::TenantRequest req;
        req.set_tenant_id(tenant_id);
        air::db::v1::StatusResponse resp;
        grpc::ClientContext ctx;
        prepare_ctx(ctx, 30);
        grpc::Status st = stub_->LockDatabase(&ctx, req, &resp);
        if (!st.ok()) {
            http_code_out = grpc_to_http(st.error_code());
            json_out = std::string(R"({"error":")") + json_escape(st.error_message()) + R"("})";
            return false;
        }
        http_code_out = 200;
        set_cached_state(tenant_id, "LOCKED");
        json_out = std::string(R"({"status":")") + json_escape(resp.status()) +
                   R"(","tenant_id":")" + json_escape(resp.tenant_id()) + R"("})";
        return true;
    }

    bool unlock_database(const std::string& tenant_id, int& http_code_out, std::string& json_out) {
        air::db::v1::TenantRequest req;
        req.set_tenant_id(tenant_id);
        air::db::v1::StatusResponse resp;
        grpc::ClientContext ctx;
        prepare_ctx(ctx, 15);
        grpc::Status st = stub_->UnlockDatabase(&ctx, req, &resp);
        if (!st.ok()) {
            http_code_out = grpc_to_http(st.error_code());
            json_out = std::string(R"({"error":")") + json_escape(st.error_message()) + R"("})";
            return false;
        }
        http_code_out = 200;
        set_cached_state(tenant_id, "SUSPENDED");
        json_out = std::string(R"({"status":")") + json_escape(resp.status()) +
                   R"(","tenant_id":")" + json_escape(resp.tenant_id()) + R"("})";
        return true;
    }

    void set_cached_state(const std::string& tenant_id, const std::string& state) {
        TenantRoute updated;
        bool found = false;
        {
            std::lock_guard<std::mutex> lk(cache_mu_);
            auto it = cache_.find(tenant_id);
            if (it != cache_.end()) {
                it->second.state = state;
                if (state != "ACTIVE") it->second.port = 0;
                updated = it->second;
                found = true;
            }
        }
        if (found) {
            TenantCallback cb;
            {
                std::lock_guard<std::mutex> lk(cb_mu_);
                cb = on_upsert_;
            }
            if (cb) cb(updated);
        }
    }

    void evict_cache(const std::string& tenant_id) {
        {
            std::lock_guard<std::mutex> lk(cache_mu_);
            cache_.erase(tenant_id);
        }
        TenantRemovedCallback cb;
        {
            std::lock_guard<std::mutex> lk(cb_mu_);
            cb = on_remove_;
        }
        if (cb) cb(tenant_id);
    }

    void upsert_route(const TenantRoute& r) {
        upsert_cache(r);
    }

    void touch_activity(const std::string& tenant_id) {
        if (tenant_id.empty()) return;
        auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::mutex> lk(touch_mu_);
            auto it = last_touch_.find(tenant_id);
            if (it != last_touch_.end() &&
                std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count() < 5) {
                return;
            }
            last_touch_[tenant_id] = now;
        }

        auto stub = stub_;
        std::string token = cfg_.internal_token;
        std::thread([stub, token, tenant_id]() {
            air::db::v1::TenantRequest req;
            req.set_tenant_id(tenant_id);
            air::db::v1::Empty resp;
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
            if (!token.empty()) ctx.AddMetadata("x-air-internal-token", token);
            stub->TouchActivity(&ctx, req, &resp);
        }).detach();
    }

private:
    Config cfg_;
    std::shared_ptr<grpc::Channel> channel_;
    std::shared_ptr<air::db::v1::AirDBControl::Stub> stub_;

    void prepare_ctx(grpc::ClientContext& ctx, int timeout_sec) const {
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(timeout_sec));
        if (!cfg_.internal_token.empty()) {
            ctx.AddMetadata("x-air-internal-token", cfg_.internal_token);
        }
    }

    TenantRoute from_proto_route(const air::db::v1::TenantRouteResponse& resp) const {
        TenantRoute r;
        r.tenant_id = resp.tenant_id();
        r.state = resp.state();
        r.host = cfg_.backend_host.empty() ? resp.host() : cfg_.backend_host;
        r.port = resp.port();
        r.sock_dir = resp.sock_dir();
        r.app_password = resp.app_password();
        return r;
    }

    static int grpc_to_http(grpc::StatusCode c) {
        switch (c) {
            case grpc::StatusCode::NOT_FOUND: return 404;
            case grpc::StatusCode::PERMISSION_DENIED: return 403;
            case grpc::StatusCode::UNAUTHENTICATED: return 401;
            case grpc::StatusCode::ALREADY_EXISTS:
            case grpc::StatusCode::FAILED_PRECONDITION: return 409;
            case grpc::StatusCode::INVALID_ARGUMENT: return 400;
            case grpc::StatusCode::UNAVAILABLE: return 503;
            default: return 502;
        }
    }

    static void append_db_info_json(std::ostringstream& oss, const air::db::v1::DatabaseInfo& d) {
        oss << "{\"account_id\":\"" << json_escape(d.account_id())
            << "\",\"tenant_id\":\"" << json_escape(d.tenant_id())
            << "\",\"direct_port\":" << d.direct_port()
            << ",\"system_identifier\":\"" << json_escape(d.system_identifier())
            << "\",\"fencing_epoch\":" << d.fencing_epoch()
            << ",\"state\":\"" << json_escape(d.state()) << "\"}";
    }
    std::atomic<bool> running_{false};
    std::thread worker_;
    std::mutex cv_mu_;
    std::condition_variable cv_;

    std::mutex pq_mu_;
    PGconn* pg_conn_ = nullptr;

    mutable std::mutex cache_mu_;
    std::unordered_map<std::string, TenantRoute> cache_;

    std::mutex cb_mu_;
    TenantCallback on_upsert_;
    TenantRemovedCallback on_remove_;

    std::mutex touch_mu_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_touch_;

    void discovery_loop() {
        while (running_.load()) {
            poll_metadata_once();
            std::unique_lock<std::mutex> lk(cv_mu_);
            cv_.wait_for(lk, std::chrono::milliseconds(cfg_.discovery_interval_ms),
                         [this]() { return !running_.load(); });
        }
    }

    void upsert_cache(const TenantRoute& r) {
        {
            std::lock_guard<std::mutex> lk(cache_mu_);
            auto& cur = cache_[r.tenant_id];
            if (r.app_password.empty() && !cur.app_password.empty()) {
                TenantRoute merged = r;
                merged.app_password = cur.app_password;
                cur = merged;
            } else {
                cur = r;
            }
        }
        TenantCallback cb;
        {
            std::lock_guard<std::mutex> lk(cb_mu_);
            cb = on_upsert_;
        }
        if (cb) cb(r);
    }

    void poll_metadata_once() {
        std::vector<TenantRoute> discovered;
        {
            std::lock_guard<std::mutex> lk(pq_mu_);
            if (!pg_conn_ || PQstatus(pg_conn_) != CONNECTION_OK) {
                if (pg_conn_) {
                    PQfinish(pg_conn_);
                    pg_conn_ = nullptr;
                }
                pg_conn_ = PQconnectdb(cfg_.meta_dsn.c_str());
                if (!pg_conn_ || PQstatus(pg_conn_) != CONNECTION_OK) {
                    if (pg_conn_) {
                        PQfinish(pg_conn_);
                        pg_conn_ = nullptr;
                    }
                    return;
                }
            }

            PGresult* res = PQexec(pg_conn_, "SELECT tenant_id, state, direct_port FROM clusters");
            if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
                if (res) PQclear(res);
                PQfinish(pg_conn_);
                pg_conn_ = nullptr;
                return;
            }
            int rows = PQntuples(res);
            discovered.reserve(static_cast<size_t>(rows));
            for (int i = 0; i < rows; ++i) {
                TenantRoute r;
                r.tenant_id = PQgetvalue(res, i, 0);
                r.state = PQgetvalue(res, i, 1);
                r.port = std::atoi(PQgetvalue(res, i, 2));
                if (!cfg_.backend_host.empty()) {
                    r.host = cfg_.backend_host;
                }
                discovered.push_back(std::move(r));
            }
            PQclear(res);
        }

        std::unordered_set<std::string> seen;
        std::vector<TenantRoute> changed;
        std::vector<std::string> removed;

        {
            std::lock_guard<std::mutex> lk(cache_mu_);
            for (const auto& d : discovered) {
                seen.insert(d.tenant_id);
                auto it = cache_.find(d.tenant_id);
                if (it == cache_.end()) {
                    cache_[d.tenant_id] = d;
                    changed.push_back(d);
                    std::cout << "[air-gateway] discovered tenant db=" << d.tenant_id
                              << " state=" << d.state << " port=" << d.port << std::endl;
                } else if (it->second.state != d.state || it->second.port != d.port) {
                    it->second.state = d.state;
                    it->second.port = d.port;
                    if (!d.host.empty()) it->second.host = d.host;
                    changed.push_back(it->second);
                }
            }
            for (auto it = cache_.begin(); it != cache_.end();) {
                if (seen.find(it->first) == seen.end()) {
                    removed.push_back(it->first);
                    std::cout << "[air-gateway] removed deleted tenant db=" << it->first << std::endl;
                    it = cache_.erase(it);
                } else {
                    ++it;
                }
            }
        }

        TenantCallback upsert_cb;
        TenantRemovedCallback remove_cb;
        {
            std::lock_guard<std::mutex> lk(cb_mu_);
            upsert_cb = on_upsert_;
            remove_cb = on_remove_;
        }
        if (upsert_cb) {
            for (const auto& r : changed) upsert_cb(r);
        }
        if (remove_cb) {
            for (const auto& tid : removed) remove_cb(tid);
        }
    }
};

} // namespace air::gw
