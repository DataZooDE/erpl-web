#pragma once

#include "duckdb.hpp"
#include "sac_secret_helper.hpp"
#include "sac_catalog.hpp"
#include <functional>
#include <vector>
#include <string>

namespace erpl_web {

/**
 * Helper for common SAC catalog table function bind patterns
 *
 * Consolidates repetitive bind function logic:
 * - Secret name parameter extraction (default: "sac")
 * - Secret resolution via ResolveSacSecretData()
 * - SacCatalogService creation
 * - Return type/column name setup
 * - Bind data population
 *
 * All SAC catalog functions follow identical pattern:
 * 1. Extract secret name from named parameters
 * 2. Resolve SAC credentials
 * 3. Create catalog service
 * 4. Call catalog method to fetch data
 * 5. Setup return types and column names
 * 6. Populate bind data
 * 7. Return bind data
 */
class SacCatalogBindHelper {
public:
    /**
     * Extract secret name from bind input named parameters
     * Defaults to "sac" if not provided
     */
    static std::string ExtractSecretName(const duckdb::TableFunctionBindInput &input);

    /**
     * Extract positional string parameter by index
     * Throws InvalidInputException if parameter missing
     */
    static std::string ExtractPositionalString(
        const duckdb::TableFunctionBindInput &input,
        size_t index,
        const std::string &param_name);

    /**
     * Resolve SAC secret and return credentials
     * Throws InvalidInputException if secret not found or invalid
     */
    static SacSecretData ResolveSacCredentials(
        duckdb::ClientContext &context,
        const std::string &secret_name);

    /**
     * Create and return SacCatalogService
     * Using resolved credentials from secret
     */
    static std::shared_ptr<SacCatalogService> CreateCatalogService(
        const SacSecretData &secret_data);

    /**
     * Helper to define VARCHAR return type columns
     * Used for all catalog functions (they return VARCHAR for all columns)
     */
    static duckdb::vector<duckdb::LogicalType> CreateVarcharReturnTypes(size_t column_count);

    /**
     * Helper to validate and extract optional named parameter string
     * Returns default value if parameter not found
     */
    static std::string GetOptionalNamedString(
        const duckdb::TableFunctionBindInput &input,
        const std::string &param_name,
        const std::string &default_value);
};

} // namespace erpl_web
