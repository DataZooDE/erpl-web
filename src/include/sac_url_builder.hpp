#pragma once

#include <string>
#include <vector>

namespace erpl_web {

/**
 * SAP Analytics Cloud URL builder
 * Constructs URLs for different SAC APIs following the pattern:
 * https://{tenant}.{region}.sapanalytics.cloud/api/v1/{endpoint}
 *
 * Supported APIs:
 * - OData v4: Planning Data, Stories, Models, Data Export
 * - REST: Import, Tenant Management, User Management, Content Management
 */
class SacUrlBuilder {
public:
    // OData v4 Endpoints (primary integration path)

    /**
     * Build URL for Planning Data Service (OData v4)
     * Provides access to planning models and data
     */
    static std::string BuildPlanningDataUrl(
        const std::string& tenant,
        const std::string& region,
        const std::string& model_id = "");

    /**
     * Build URL for Story Service (OData v4)
     * Provides access to stories and dashboards
     */
    static std::string BuildStoryServiceUrl(
        const std::string& tenant,
        const std::string& region);

    /**
     * Build URL for Model Service (OData v4)
     * Provides access to analytics models and their metadata
     */
    static std::string BuildModelServiceUrl(
        const std::string& tenant,
        const std::string& region,
        const std::string& model_id = "");

    /**
     * Build URL for Data Export Service (OData v4)
     * Provides bulk data export capabilities
     */
    static std::string BuildDataExportUrl(
        const std::string& tenant,
        const std::string& region);

    /**
     * Build base URL for all SAC APIs
     * https://{tenant}.{region}.sapanalytics.cloud
     */
    static std::string BuildBaseUrl(
        const std::string& tenant,
        const std::string& region);

    /**
     * Build complete OData v4 service root URL
     * https://{tenant}.{region}.sapanalytics.cloud/api/v1/odata/
     */
    static std::string BuildODataServiceRootUrl(
        const std::string& tenant,
        const std::string& region);

    // REST Endpoints (complementary integration)

    /**
     * Build URL for Data Import API (REST)
     * Used for importing data into SAC
     */
    static std::string BuildDataImportUrl(
        const std::string& tenant,
        const std::string& region);

    /**
     * Build URL for Tenant Configuration API (REST)
     * Used for tenant management and configuration
     */
    static std::string BuildTenantConfigUrl(
        const std::string& tenant,
        const std::string& region);

    /**
     * Build URL for User Management API (REST)
     * Used for user and role management
     */
    static std::string BuildUserManagementUrl(
        const std::string& tenant,
        const std::string& region);

    /**
     * Build URL for Content Management API (REST)
     * Used for managing stories, models, and dashboards
     */
    static std::string BuildContentManagementUrl(
        const std::string& tenant,
        const std::string& region);

    // Utility methods

    /**
     * Normalize region identifier (e.g., 'eu10', 'us10', 'ap10')
     * Returns normalized region or original if already valid
     */
    static std::string NormalizeRegion(const std::string& region);

    /**
     * Validate SAC tenant name format
     */
    static bool IsValidTenant(const std::string& tenant);

    /**
     * Validate SAC region format
     */
    static bool IsValidRegion(const std::string& region);

private:
    // Private constructor - all methods are static
    SacUrlBuilder() = delete;

    // Known SAC regions
    static constexpr const char* const KNOWN_REGIONS[] = {
        "us10",  // US (US East)
        "eu10",  // EU
        "ap10",  // APAC
        "ca10",  // Canada
        "jp10",  // Japan
        "au10",  // Australia
        "br10",  // Brazil
        "ch10"   // Switzerland
    };
};

} // namespace erpl_web
