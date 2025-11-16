#pragma once

#include <string>
#include <vector>
#include <optional>
#include "duckdb.hpp"

namespace erpl_web {

/**
 * Parameter Validation Helper
 *
 * Provides consistent validation and error handling for function parameters.
 * Standardizes error messages across SAC, Datasphere, and OData modules.
 *
 * Usage:
 *   ParameterValidation::ValidateRequired("model_id", model_id);
 *   ParameterValidation::ValidateNonEmpty("space_name", space_name);
 *   ParameterValidation::ValidateRegion(region);
 */
class ParameterValidation {
public:
    /**
     * Validate that a required string parameter is provided (not empty)
     * Throws InvalidInputException if validation fails
     *
     * @param param_name The parameter name (for error message)
     * @param value The value to validate
     * @return The validated value (for convenience)
     *
     * @throws duckdb::InvalidInputException if value is empty
     *
     * Example:
     *   auto space_id = ParameterValidation::ValidateRequired("space_id", input);
     */
    static std::string ValidateRequired(
        const std::string& param_name,
        const std::string& value);

    /**
     * Validate that a string parameter is not empty
     * Alias for ValidateRequired with clearer intent
     *
     * @param param_name The parameter name (for error message)
     * @param value The value to validate
     * @return The validated value (for convenience)
     *
     * @throws duckdb::InvalidInputException if value is empty
     */
    static std::string ValidateNonEmpty(
        const std::string& param_name,
        const std::string& value);

    /**
     * Validate that a SAC/Datasphere region is valid
     * Checks against known cloud data centers
     *
     * Known regions:
     * - us10, us20, eu10, eu20, ap10, ap11, ap12, ap20, ap21
     *
     * @param region The region code to validate
     * @return The validated region (for convenience)
     *
     * @throws duckdb::InvalidInputException if region is not recognized
     *
     * Example:
     *   auto region = ParameterValidation::ValidateRegion("us10");
     */
    static std::string ValidateRegion(const std::string& region);

    /**
     * Validate that a required parameter exists (non-null/optional has value)
     * Throws InvalidInputException if parameter not provided
     *
     * @param param_name The parameter name (for error message)
     * @param value The optional value to validate
     * @return The unwrapped value
     *
     * @throws duckdb::InvalidInputException if optional is empty
     *
     * Example:
     *   auto model_opt = catalog->GetModel(model_id);
     *   auto model = ParameterValidation::ValidateExists("model", model_opt);
     */
    template <typename T>
    static T ValidateExists(
        const std::string& param_name,
        const std::optional<T>& value);

    /**
     * Validate that a count/size is within acceptable range
     * Throws InvalidInputException if out of range
     *
     * @param param_name The parameter name (for error message)
     * @param value The value to validate
     * @param min_value Minimum acceptable value (inclusive)
     * @param max_value Maximum acceptable value (inclusive)
     * @return The validated value (for convenience)
     *
     * @throws duckdb::InvalidInputException if out of range
     *
     * Example:
     *   auto limit = ParameterValidation::ValidateRange("limit", limit, 1, 10000);
     */
    static size_t ValidateRange(
        const std::string& param_name,
        size_t value,
        size_t min_value,
        size_t max_value);

    /**
     * Validate that a value is one of the allowed options
     * Throws InvalidInputException if value not in allowed list
     *
     * @param param_name The parameter name (for error message)
     * @param value The value to validate
     * @param allowed_values Vector of allowed values
     * @return The validated value (for convenience)
     *
     * @throws duckdb::InvalidInputException if not in allowed list
     *
     * Example:
     *   auto type = ParameterValidation::ValidateOneOf("type", type, {"PLANNING", "ANALYTICS"});
     */
    static std::string ValidateOneOf(
        const std::string& param_name,
        const std::string& value,
        const std::vector<std::string>& allowed_values);

    /**
     * Check if a region is known/valid
     *
     * @param region The region code to check
     * @return true if region is recognized, false otherwise
     */
    static bool IsValidRegion(const std::string& region);

private:
    static const std::vector<std::string> KNOWN_REGIONS;
};

// Template implementation
template <typename T>
inline T ParameterValidation::ValidateExists(
    const std::string& param_name,
    const std::optional<T>& value) {

    if (!value.has_value()) {
        throw duckdb::InvalidInputException(
            "Required parameter '" + param_name + "' not found or is empty");
    }
    return value.value();
}

} // namespace erpl_web
