#pragma once

#include "config.hpp"
#include "discovery.hpp"
#include "metrics.hpp"
#include "net/socket.hpp"
#include "net/tls.hpp"
#include "pg/auth.hpp"
#include "pooler.hpp"

namespace air::gw {

/**
 * Handle an accepted PostgreSQL client socket end-to-end:
 *   1. Pre-startup GSSENCRequest / SSLRequest / CancelRequest handling
 *   2. StartupMessage validation, tenant extraction & SNI consistency check
 *   3. Per-tenant/per-user connection limit enforcement
 *   4. Control Plane tenant resolution & SQLSTATE error mapping
 *   5. Pooled serving (pool_mode=true) or direct bidirectional proxying
 */
void handle_client_connection(int cfd,
                              const Config& cfg,
                              TLSContext& tls_ctx,
                              TenantDiscovery& discovery,
                              PoolManager& pools,
                              MetricsRegistry& metrics,
                              CancelRegistry& cancel_reg);

} // namespace air::gw
