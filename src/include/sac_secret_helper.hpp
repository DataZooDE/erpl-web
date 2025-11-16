#pragma once

#include "duckdb.hpp"
#include "http_client.hpp"
#include <memory>
#include <string>

namespace erpl_web {

// Consolidated structure for SAC secret resolution
struct SacSecretData {
    std::string tenant;
    std::string region;
    std::shared_ptr<HttpAuthParams> auth_params;
};

// Helper function to resolve SAC authentication from DuckDB secret
// Consolidates duplicate secret resolution logic from multiple SAC modules
// Returns tenant, region, and auth params from the named secret
// Throws InvalidInputException if secret not found or malformed
SacSecretData ResolveSacSecretData(duckdb::ClientContext& context, const std::string& secret_name);

} // namespace erpl_web
