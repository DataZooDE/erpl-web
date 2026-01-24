#pragma once

#include "odata_client.hpp"
#include "http_client.hpp"
#include <string>
#include <memory>

namespace erpl_web {

// URL builder for Business Central endpoints
class BusinessCentralUrlBuilder {
public:
    // Base API URL for BC API v2.0
    static std::string BuildApiUrl(const std::string &tenant_id, const std::string &environment);

    // Company-scoped URL
    static std::string BuildCompanyUrl(const std::string &base_url, const std::string &company_id);

    // Entity set URL within a company
    static std::string BuildEntitySetUrl(const std::string &company_url, const std::string &entity_set);

    // Metadata URL for $metadata
    static std::string BuildMetadataUrl(const std::string &base_url);

    // Companies endpoint URL
    static std::string BuildCompaniesUrl(const std::string &base_url);

    // Resource URL for OAuth2 scope
    static std::string GetResourceUrl();
};

// Client factory for Business Central OData clients
// Reuses existing OData infrastructure
class BusinessCentralClientFactory {
public:
    // Create client for listing companies
    static std::shared_ptr<ODataEntitySetClient> CreateCompaniesClient(
        const std::string &tenant_id,
        const std::string &environment,
        std::shared_ptr<HttpAuthParams> auth_params);

    // Create client for entity set within a company
    static std::shared_ptr<ODataEntitySetClient> CreateEntitySetClient(
        const std::string &tenant_id,
        const std::string &environment,
        const std::string &company_id,
        const std::string &entity_set,
        std::shared_ptr<HttpAuthParams> auth_params);

    // Create catalog client for $metadata
    static std::shared_ptr<ODataServiceClient> CreateCatalogClient(
        const std::string &tenant_id,
        const std::string &environment,
        std::shared_ptr<HttpAuthParams> auth_params);
};

} // namespace erpl_web
