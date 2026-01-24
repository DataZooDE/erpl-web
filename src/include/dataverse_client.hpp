#pragma once

#include "odata_client.hpp"
#include "http_client.hpp"
#include <string>
#include <memory>

namespace erpl_web {

// URL builder for Dataverse Web API endpoints
class DataverseUrlBuilder {
public:
    // Build the Web API base URL
    // e.g., https://myorg.crm.dynamics.com/api/data/v9.2
    static std::string BuildApiUrl(const std::string &environment_url, const std::string &api_version = "v9.2");

    // Build entity set URL
    // e.g., https://myorg.crm.dynamics.com/api/data/v9.2/accounts
    static std::string BuildEntitySetUrl(const std::string &base_url, const std::string &entity_set);

    // Build metadata URL
    // e.g., https://myorg.crm.dynamics.com/api/data/v9.2/$metadata
    static std::string BuildMetadataUrl(const std::string &base_url);

    // Build entity definitions URL (for listing entities)
    // e.g., https://myorg.crm.dynamics.com/api/data/v9.2/EntityDefinitions
    static std::string BuildEntityDefinitionsUrl(const std::string &base_url);

    // Build single entity definition URL
    // e.g., https://myorg.crm.dynamics.com/api/data/v9.2/EntityDefinitions(LogicalName='account')
    static std::string BuildEntityDefinitionUrl(const std::string &base_url, const std::string &logical_name);

    // Build entity attributes URL
    // e.g., https://myorg.crm.dynamics.com/api/data/v9.2/EntityDefinitions(LogicalName='account')/Attributes
    static std::string BuildEntityAttributesUrl(const std::string &base_url, const std::string &logical_name);
};

// Client factory for Dataverse OData clients
// Reuses existing OData infrastructure
class DataverseClientFactory {
public:
    // Create client for listing entity definitions
    static std::shared_ptr<ODataEntitySetClient> CreateEntityDefinitionsClient(
        const std::string &environment_url,
        std::shared_ptr<HttpAuthParams> auth_params,
        const std::string &api_version = "v9.2");

    // Create client for reading entity attributes
    static std::shared_ptr<ODataEntitySetClient> CreateEntityAttributesClient(
        const std::string &environment_url,
        const std::string &logical_name,
        std::shared_ptr<HttpAuthParams> auth_params,
        const std::string &api_version = "v9.2");

    // Create client for reading entity data
    static std::shared_ptr<ODataEntitySetClient> CreateEntitySetClient(
        const std::string &environment_url,
        const std::string &entity_set,
        std::shared_ptr<HttpAuthParams> auth_params,
        const std::string &api_version = "v9.2");

    // Create service client for $metadata
    static std::shared_ptr<ODataServiceClient> CreateServiceClient(
        const std::string &environment_url,
        std::shared_ptr<HttpAuthParams> auth_params,
        const std::string &api_version = "v9.2");
};

} // namespace erpl_web
