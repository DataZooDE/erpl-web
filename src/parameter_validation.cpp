#include "parameter_validation.hpp"
#include "duckdb.hpp"
#include <algorithm>

namespace erpl_web {

// Known SAP cloud regions (data centers)
const std::vector<std::string> ParameterValidation::KNOWN_REGIONS = {
    "us10", "us20",   // United States
    "eu10", "eu20",   // Europe
    "ap10", "ap11", "ap12",  // Asia Pacific
    "ap20", "ap21",   // Asia Pacific (additional)
    "ca10",           // Canada
    "ch20",           // Switzerland
    "br10",           // Brazil
    "jp10", "jp20"    // Japan
};

std::string ParameterValidation::ValidateRequired(
    const std::string& param_name,
    const std::string& value) {

    if (value.empty()) {
        throw duckdb::InvalidInputException(
            "Required parameter '" + param_name + "' is missing or empty");
    }
    return value;
}

std::string ParameterValidation::ValidateNonEmpty(
    const std::string& param_name,
    const std::string& value) {

    return ValidateRequired(param_name, value);
}

std::string ParameterValidation::ValidateRegion(const std::string& region) {
    if (!IsValidRegion(region)) {
        std::string valid_regions_str;
        for (size_t i = 0; i < KNOWN_REGIONS.size(); ++i) {
            if (i > 0) valid_regions_str += ", ";
            valid_regions_str += KNOWN_REGIONS[i];
        }
        throw duckdb::InvalidInputException(
            "Invalid region '" + region + "'. Known regions: " + valid_regions_str);
    }
    return region;
}

size_t ParameterValidation::ValidateRange(
    const std::string& param_name,
    size_t value,
    size_t min_value,
    size_t max_value) {

    if (value < min_value || value > max_value) {
        throw duckdb::InvalidInputException(
            "Parameter '" + param_name + "' value " + std::to_string(value) +
            " is out of range [" + std::to_string(min_value) + ", " +
            std::to_string(max_value) + "]");
    }
    return value;
}

std::string ParameterValidation::ValidateOneOf(
    const std::string& param_name,
    const std::string& value,
    const std::vector<std::string>& allowed_values) {

    if (std::find(allowed_values.begin(), allowed_values.end(), value) ==
        allowed_values.end()) {

        std::string allowed_str;
        for (size_t i = 0; i < allowed_values.size(); ++i) {
            if (i > 0) allowed_str += ", ";
            allowed_str += "'" + allowed_values[i] + "'";
        }
        throw duckdb::InvalidInputException(
            "Parameter '" + param_name + "' value '" + value +
            "' is not valid. Allowed values: " + allowed_str);
    }
    return value;
}

bool ParameterValidation::IsValidRegion(const std::string& region) {
    return std::find(KNOWN_REGIONS.begin(), KNOWN_REGIONS.end(), region) !=
           KNOWN_REGIONS.end();
}

} // namespace erpl_web
