#pragma once

#include "odata_client.hpp"
#include "http_client.hpp"
#include "odata_edm.hpp"
#include "duckdb/function/table_function.hpp"
#include <memory>
#include <string>

namespace erpl_web {

// Forward declarations
class SacUrlBuilder;

// SAP Analytics Cloud authentication parameters
// Extends HttpAuthParams with SAC-specific fields
struct SacAuthParams : public HttpAuthParams {
    std::string tenant;
    std::string region;
};

// Service type enumeration for factory consolidation
// Identifies which SAC service endpoint to connect to
enum class SacServiceType {
    PlanningData,   // Planning Data Service (OData v4) - requires model_id
    StoryService,   // Story Service (OData v4)
    ModelService,   // Model Service (OData v4)
    DataExport,     // Data Export Service (OData v4)
    Catalog         // Catalog Service (OData v4)
};

// SAC client factory for creating appropriate OData clients
// Reuses existing ODataEntitySetClient and ODataServiceClient
class SacClientFactory {
public:
    /**
     * Create client for Planning Data Service (OData v4)
     * Provides access to planning models and data
     */
    static std::shared_ptr<ODataEntitySetClient> CreatePlanningDataClient(
        const std::string& tenant,
        const std::string& region,
        const std::string& model_id,
        std::shared_ptr<HttpAuthParams> auth_params);

    /**
     * Create client for Story Service (OData v4)
     * Provides access to stories and dashboards
     */
    static std::shared_ptr<ODataEntitySetClient> CreateStoryServiceClient(
        const std::string& tenant,
        const std::string& region,
        std::shared_ptr<HttpAuthParams> auth_params);

    /**
     * Create client for Model Service (OData v4)
     * Provides access to analytics models
     */
    static std::shared_ptr<ODataEntitySetClient> CreateModelServiceClient(
        const std::string& tenant,
        const std::string& region,
        std::shared_ptr<HttpAuthParams> auth_params);

    /**
     * Create client for Data Export Service (OData v4)
     * Provides bulk data export capabilities
     */
    static std::shared_ptr<ODataEntitySetClient> CreateDataExportClient(
        const std::string& tenant,
        const std::string& region,
        std::shared_ptr<HttpAuthParams> auth_params);

    /**
     * Create catalog service client for discovery
     * Used to list models, stories, and dimensions
     */
    static std::shared_ptr<ODataServiceClient> CreateCatalogClient(
        const std::string& tenant,
        const std::string& region,
        std::shared_ptr<HttpAuthParams> auth_params);

private:
    // Consolidated URL builder - replaces 5 separate BuildXxxUrl methods
    // Reduces duplication by using SacUrlBuilder directly in public methods
    // These methods are now thin wrappers to SacUrlBuilder

    static std::string BuildPlanningDataUrl(
        const std::string& tenant,
        const std::string& region,
        const std::string& model_id);

    static std::string BuildStoryServiceUrl(
        const std::string& tenant,
        const std::string& region);

    static std::string BuildModelServiceUrl(
        const std::string& tenant,
        const std::string& region);

    static std::string BuildDataExportUrl(
        const std::string& tenant,
        const std::string& region);

    static std::string BuildCatalogUrl(
        const std::string& tenant,
        const std::string& region);
};

} // namespace erpl_web
