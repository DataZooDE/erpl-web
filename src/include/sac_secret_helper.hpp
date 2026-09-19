#pragma once

#include "duckdb.hpp"
#include "datazoo/oauth2/http_client.hpp"
#include <memory>
#include <string>

namespace erpl_web {

// Consolidated structure for SAC secret resolution
struct SacSecretData {
    std::string tenant;
    std::string region;
    // When set, addresses the service directly instead of deriving a URL from
    // tenant/region. This is the loopback hatch that makes SAC testable; it is gated with
    // RequireGatedServiceUrl where it is read, so it cannot redirect a real tenant's token.
    std::string base_url;
    std::shared_ptr<HttpAuthParams> auth_params;
};

// Helper function to resolve SAC authentication from DuckDB secret
// Consolidates duplicate secret resolution logic from multiple SAC modules
// Returns tenant, region, and auth params from the named secret
// Throws InvalidInputException if secret not found or malformed
SacSecretData ResolveSacSecretData(duckdb::ClientContext& context, const std::string& secret_name);

} // namespace erpl_web
