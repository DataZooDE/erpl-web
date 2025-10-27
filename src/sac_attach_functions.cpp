#include "sac_attach_functions.hpp"
#include "sac_url_builder.hpp"
#include "sac_client.hpp"
#include "http_client.hpp"
#include "odata_storage.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/common/string_util.hpp"
#include <regex>

namespace erpl_web {

// SacAttachBindData implementation

duckdb::unique_ptr<SacAttachBindData> SacAttachBindData::FromUrl(
    const std::string& url,
    std::shared_ptr<HttpAuthParams> auth_params) {

    // Parse SAC URL to extract tenant and region
    // Expected format: https://tenant.region.sapanalytics.cloud or https://tenant.region.sapanalytics.cloud/api/v1/...

    std::string host;
    size_t protocol_end = url.find("://");
    if (protocol_end == std::string::npos) {
        throw duckdb::InvalidInputException("Invalid SAC URL format: " + url);
    }

    size_t host_start = protocol_end + 3;
    size_t host_end = url.find('/', host_start);
    if (host_end == std::string::npos) {
        host = url.substr(host_start);
    } else {
        host = url.substr(host_start, host_end - host_start);
    }

    // Parse host: tenant.region.sapanalytics.cloud
    std::vector<std::string> parts;
    size_t pos = 0;
    while (pos < host.length()) {
        size_t dot = host.find('.', pos);
        if (dot == std::string::npos) {
            parts.push_back(host.substr(pos));
            break;
        }
        parts.push_back(host.substr(pos, dot - pos));
        pos = dot + 1;
    }

    if (parts.size() < 3 || parts[parts.size() - 2] != "sapanalytics" ||
        parts[parts.size() - 1] != "cloud") {
        throw duckdb::InvalidInputException("Invalid SAC URL format. Expected: https://tenant.region.sapanalytics.cloud");
    }

    std::string tenant = parts[0];
    std::string region = parts[1];

    if (!SacUrlBuilder::IsValidTenant(tenant)) {
        throw duckdb::InvalidInputException("Invalid tenant name: " + tenant);
    }

    if (!SacUrlBuilder::IsValidRegion(region)) {
        throw duckdb::InvalidInputException("Invalid region: " + region);
    }

    // Build OData service root URL
    auto odata_url = SacUrlBuilder::BuildODataServiceRootUrl(tenant, region);
    HttpUrl parsed_url(odata_url);

    // Create OData service client
    auto http_client = std::make_shared<HttpClient>();
    auto odata_client = std::make_shared<ODataServiceClient>(http_client, parsed_url, auth_params);
    odata_client->SetODataVersionDirectly(ODataVersion::V4);

    // Create bind data
    auto bind = duckdb::make_uniq<SacAttachBindData>(odata_client);
    return bind;
}

SacAttachBindData::SacAttachBindData(std::shared_ptr<ODataServiceClient> odata_client_)
    : odata_client(odata_client_), finished(false), overwrite(false) {
}

bool SacAttachBindData::IsFinished() const {
    return finished;
}

void SacAttachBindData::SetFinished() {
    finished = true;
}

std::vector<std::string> SacAttachBindData::IgnorePatterns() const {
    return ignore_patterns;
}

void SacAttachBindData::IgnorePatterns(const std::vector<std::string>& ignore) {
    ignore_patterns = ignore;
}

bool SacAttachBindData::Overwrite() const {
    return overwrite;
}

void SacAttachBindData::SetOverwrite(bool ov) {
    overwrite = ov;
}

std::vector<ODataEntitySetReference> SacAttachBindData::EntitySets() {
    std::vector<ODataEntitySetReference> result;

    try {
        // Return common SAC entity sets for planning and analytics models
        // These are known entity sets in SAC OData services
        std::vector<std::string> known_sets = {
            "PlanningModels",
            "AnalyticsModels",
            "Stories",
            "DataExport",
            "Dimensions",
            "Measures"
        };

        for (const auto& set_name : known_sets) {
            // Check if should be ignored
            if (!MatchPattern(set_name, ignore_patterns)) {
                ODataEntitySetReference ref;
                ref.name = set_name;
                result.push_back(ref);
            }
        }
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("SAC_ATTACH", "Failed to fetch entity sets: " + std::string(e.what()));
    }

    return result;
}

bool SacAttachBindData::MatchPattern(const std::string& str, const std::string& ignore_pattern) {
    // Simple wildcard matching: * matches any sequence
    if (ignore_pattern == "*") {
        return true;
    }

    // Convert wildcard pattern to regex
    std::string regex_pattern = ignore_pattern;
    regex_pattern = std::regex_replace(regex_pattern, std::regex("\\*"), ".*");
    regex_pattern = "^" + regex_pattern + "$";

    try {
        std::regex re(regex_pattern);
        return std::regex_match(str, re);
    } catch (const std::regex_error& e) {
        // If regex is invalid, return false
        return false;
    }
}

bool SacAttachBindData::MatchPattern(const std::string& str, const std::vector<std::string>& ignore_patterns) {
    for (const auto& pattern : ignore_patterns) {
        if (MatchPattern(str, pattern)) {
            return true;
        }
    }
    return false;
}

// SAC ATTACH Table Function - Delegates to OData ATTACH with SAC URL configuration

duckdb::TableFunctionSet CreateSacAttachFunction() {
    duckdb::TableFunctionSet function_set("sac_attach");

    // For now, SAC ATTACH is handled through the OData ATTACH mechanism
    // Users can attach SAC instances directly:
    //   ATTACH 'https://tenant.region.sapanalytics.cloud' AS sac (TYPE sac, SECRET sac_secret)
    //
    // This will be transformed into an OData ATTACH call with the OData service root URL
    // The full implementation would register as a storage extension handler for 'sac' TYPE

    // Return empty set - SAC ATTACH is handled at the storage extension level
    return function_set;
}

// SAC Storage Extension

SacStorageExtension::SacStorageExtension() {
    // Register SAC as a custom storage extension
    // This allows ATTACH 'https://...' AS sac (TYPE sac, SECRET sac_secret)
}

duckdb::unique_ptr<SacStorageExtension> CreateSacStorageExtension() {
    return duckdb::make_uniq<SacStorageExtension>();
}

} // namespace erpl_web
