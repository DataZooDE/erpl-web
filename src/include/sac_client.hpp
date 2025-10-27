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
    std::string client_id;
    std::string client_secret;
    std::string scope;

    // OAuth2 token management
    std::optional<std::string> access_token;
    std::optional<std::string> refresh_token;
    std::optional<std::chrono::time_point<std::chrono::system_clock>> token_expiry;

    bool IsTokenExpired() const;
    bool NeedsRefresh() const;

    // OAuth2 endpoints for SAC
    std::string GetAuthorizationUrl() const;
    std::string GetTokenUrl() const;
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

// SAC OData client wrapper for managing dual-endpoint pattern if needed
// Similar to DatasphereODataClient
class SacODataClient {
public:
    SacODataClient(
        const std::string& base_url,
        const std::string& access_token);

    // Get metadata from base URL
    std::unique_ptr<ODataEntitySetResponse> GetMetadata();

    // Get data from service
    std::unique_ptr<ODataEntitySetResponse> GetData();

private:
    std::string base_url_;
    std::string access_token_;
    std::unique_ptr<ODataEntitySetClient> metadata_client_;
    std::unique_ptr<ODataEntitySetClient> data_client_;
};

} // namespace erpl_web
