#pragma once

#include "metrics.hpp"
#include "pb/airdb.grpc.pb.h"
#include "pooler.hpp"

#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>

namespace air::gw {

/**
 * gRPC GatewayAdmin service allowing the Control Plane to drain tenant pools
 * and update per-tenant connection/backend limits.
 */
class GatewayAdminServiceImpl final : public air::db::v1::GatewayAdmin::Service {
public:
    GatewayAdminServiceImpl(std::string internal_token,
                            PoolManager& pools,
                            MetricsRegistry& metrics);

    grpc::Status DrainTenant(grpc::ServerContext* context,
                             const air::db::v1::DrainTenantRequest* request,
                             air::db::v1::StatusResponse* response) override;

    grpc::Status SetTenantLimits(grpc::ServerContext* context,
                                 const air::db::v1::SetTenantLimitsRequest* request,
                                 air::db::v1::StatusResponse* response) override;

private:
    grpc::Status authorize(const grpc::ServerContext* context) const;

    std::string internal_token_;
    PoolManager& pools_;
    MetricsRegistry& metrics_;
};

/**
 * Lifecycle wrapper for the optional internal GatewayAdmin gRPC server.
 */
class AdminGRPCServer {
public:
    AdminGRPCServer(std::string listen_addr,
                    std::string internal_token,
                    PoolManager& pools,
                    MetricsRegistry& metrics);
    ~AdminGRPCServer();

    bool start(int* selected_port_out = nullptr);
    void stop();

private:
    std::string listen_addr_;
    GatewayAdminServiceImpl service_;
    std::unique_ptr<grpc::Server> server_;
};

} // namespace air::gw
