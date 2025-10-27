#include "sac_url_builder.hpp"
#include "duckdb/common/string_util.hpp"
#include <algorithm>

namespace erpl_web {

using namespace duckdb;

std::string SacUrlBuilder::BuildBaseUrl(
    const std::string& tenant,
    const std::string& region) {
    return "https://" + tenant + "." + region + ".sapanalytics.cloud";
}

std::string SacUrlBuilder::BuildODataServiceRootUrl(
    const std::string& tenant,
    const std::string& region) {
    return BuildBaseUrl(tenant, region) + "/api/v1/odata/";
}

std::string SacUrlBuilder::BuildPlanningDataUrl(
    const std::string& tenant,
    const std::string& region,
    const std::string& model_id) {
    auto base = BuildODataServiceRootUrl(tenant, region);
    if (model_id.empty()) {
        return base + "v1/PlanningModels";
    }
    return base + "v1/PlanningModels('" + model_id + "')";
}

std::string SacUrlBuilder::BuildStoryServiceUrl(
    const std::string& tenant,
    const std::string& region) {
    return BuildODataServiceRootUrl(tenant, region) + "v1/Stories";
}

std::string SacUrlBuilder::BuildModelServiceUrl(
    const std::string& tenant,
    const std::string& region,
    const std::string& model_id) {
    auto base = BuildODataServiceRootUrl(tenant, region);
    if (model_id.empty()) {
        return base + "v1/Models";
    }
    return base + "v1/Models('" + model_id + "')";
}

std::string SacUrlBuilder::BuildDataExportUrl(
    const std::string& tenant,
    const std::string& region) {
    return BuildODataServiceRootUrl(tenant, region) + "v1/DataExport";
}

std::string SacUrlBuilder::BuildDataImportUrl(
    const std::string& tenant,
    const std::string& region) {
    return BuildBaseUrl(tenant, region) + "/api/v1/import/";
}

std::string SacUrlBuilder::BuildTenantConfigUrl(
    const std::string& tenant,
    const std::string& region) {
    return BuildBaseUrl(tenant, region) + "/api/v1/tenant/";
}

std::string SacUrlBuilder::BuildUserManagementUrl(
    const std::string& tenant,
    const std::string& region) {
    return BuildBaseUrl(tenant, region) + "/api/v1/users/";
}

std::string SacUrlBuilder::BuildContentManagementUrl(
    const std::string& tenant,
    const std::string& region) {
    return BuildBaseUrl(tenant, region) + "/api/v1/content/";
}

std::string SacUrlBuilder::NormalizeRegion(const std::string& region) {
    auto normalized = StringUtil::Lower(region);

    // Validate against known regions
    for (const auto& known_region : KNOWN_REGIONS) {
        if (normalized == known_region) {
            return normalized;
        }
    }

    // Return as-is if not in known list (for future-proofing)
    return normalized;
}

bool SacUrlBuilder::IsValidTenant(const std::string& tenant) {
    if (tenant.empty() || tenant.length() > 100) {
        return false;
    }

    // Tenant names are alphanumeric with hyphens and underscores
    for (char c : tenant) {
        if (!std::isalnum(c) && c != '-' && c != '_') {
            return false;
        }
    }

    return true;
}

bool SacUrlBuilder::IsValidRegion(const std::string& region) {
    auto normalized = NormalizeRegion(region);

    for (const auto& known_region : KNOWN_REGIONS) {
        if (normalized == known_region) {
            return true;
        }
    }

    return false;
}

} // namespace erpl_web
