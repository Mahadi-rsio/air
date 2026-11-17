#pragma once

#include "metrics.hpp"
#include "pb/airdb.grpc.pb.h"
#include "pooler.hpp"

#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <string>

namespace air::gw {

class GatewayAdminServiceImpl final : public air::db::v1::GatewayAdmin::Service {
public:
    GatewayAdminServiceImpl(std::string internal_token,
                            PoolManager& pools,
                            MetricsRegistry& metrics)
        : internal_token_(std::move(internal_token)),
          pools_(pools),
          metrics_(metrics) {}

    grpc::Status DrainTenant(grpc::ServerContext* context,
                             const air::db::v1::DrainTenantRequest* request,
                             air::db::v1::StatusResponse* response) override {
        if (auto st = authorize(context); !st.ok()) return st;

        const std::string& tenant_id = request->tenant_id();
        if (tenant_id.empty()) {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "tenant_id is required");
        }
        int timeout_ms = request->timeout_ms() > 0 ? request->timeout_ms() : 3000;
        std::string err;
        if (!pools_.request_drain(tenant_id, timeout_ms, err)) {
            return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, err);
        }
        response->set_status("drained");
        response->set_tenant_id(tenant_id);
        return grpc::Status::OK;
    }

    grpc::Status SetTenantLimits(grpc::ServerContext* context,
                                 const air::db::v1::SetTenantLimitsRequest* request,
                                 air::db::v1::StatusResponse* response) override {
        if (auto st = authorize(context); !st.ok()) return st;

        const std::string& tenant_id = request->tenant_id();
        if (tenant_id.empty()) {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "tenant_id is required");
        }
        if (request->max_client_conns() > 0) {
            metrics_.set_tenant_client_limit(tenant_id, request->max_client_conns());
        }
        if (request->max_backends() > 0) {
            pools_.set_tenant_max_backends(tenant_id, request->max_backends());
        }
        response->set_status("updated");
        response->set_tenant_id(tenant_id);
        return grpc::Status::OK;
    }

private:
    grpc::Status authorize(const grpc::ServerContext* context) const {
        if (internal_token_.empty()) return grpc::Status::OK;
        const auto& md = context->client_metadata();
        auto range = md.equal_range("x-internal-token");
        for (auto it = range.first; it != range.second; ++it) {
            if (std::string(it->second.data(), it->second.length()) == internal_token_) {
                return grpc::Status::OK;
            }
        }
        return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "unauthorized");
    }

    std::string internal_token_;
    PoolManager& pools_;
    MetricsRegistry& metrics_;
};

class AdminGRPCServer {
public:
    AdminGRPCServer(std::string listen_addr,
                    std::string internal_token,
                    PoolManager& pools,
                    MetricsRegistry& metrics)
        : listen_addr_(std::move(listen_addr)),
          service_(std::move(internal_token), pools, metrics) {}

    ~AdminGRPCServer() { stop(); }

    bool start(int* selected_port_out = nullptr) {
        if (listen_addr_.empty()) return false;
        grpc::ServerBuilder builder;
        int selected_port = 0;
        builder.AddListeningPort(listen_addr_, grpc::InsecureServerCredentials(), &selected_port);
        builder.RegisterService(&service_);
        server_ = builder.BuildAndStart();
        if (!server_ || selected_port == 0) {
            server_.reset();
            return false;
        }
        if (selected_port_out) *selected_port_out = selected_port;
        return true;
    }

    void stop() {
        if (server_) {
            server_->Shutdown();
            server_->Wait();
            server_.reset();
        }
    }

private:
    std::string listen_addr_;
    GatewayAdminServiceImpl service_;
    std::unique_ptr<grpc::Server> server_;
};

} // namespace air::gw
