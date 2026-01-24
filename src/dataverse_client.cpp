#include "dataverse_client.hpp"
#include "tracing.hpp"

namespace erpl_web {

// URL Builder implementation
std::string DataverseUrlBuilder::BuildApiUrl(const std::string &environment_url, const std::string &api_version) {
    std::string base = environment_url;
    // Remove trailing slash if present
    if (!base.empty() && base.back() == '/') {
        base.pop_back();
    }
    return base + "/api/data/" + api_version;
}

std::string DataverseUrlBuilder::BuildEntitySetUrl(const std::string &base_url, const std::string &entity_set) {
    return base_url + "/" + entity_set;
}

std::string DataverseUrlBuilder::BuildMetadataUrl(const std::string &base_url) {
    return base_url + "/$metadata";
}

std::string DataverseUrlBuilder::BuildEntityDefinitionsUrl(const std::string &base_url) {
    return base_url + "/EntityDefinitions";
}

std::string DataverseUrlBuilder::BuildEntityDefinitionUrl(const std::string &base_url, const std::string &logical_name) {
    return base_url + "/EntityDefinitions(LogicalName='" + logical_name + "')";
}

std::string DataverseUrlBuilder::BuildEntityAttributesUrl(const std::string &base_url, const std::string &logical_name) {
    return base_url + "/EntityDefinitions(LogicalName='" + logical_name + "')/Attributes";
}

// Helper to create HTTP client for OData
static std::shared_ptr<HttpClient> CreateODataHttpClient() {
    HttpParams http_params;
    http_params.url_encode = false;  // OData handles URL encoding
    return std::make_shared<HttpClient>(http_params);
}

// Client Factory implementation
std::shared_ptr<ODataEntitySetClient> DataverseClientFactory::CreateEntityDefinitionsClient(
    const std::string &environment_url,
    std::shared_ptr<HttpAuthParams> auth_params,
    const std::string &api_version) {

    ERPL_TRACE_DEBUG("DATAVERSE_CLIENT", "Creating entity definitions client for: " + environment_url);

    std::string base_url = DataverseUrlBuilder::BuildApiUrl(environment_url, api_version);
    std::string entity_defs_url = DataverseUrlBuilder::BuildEntityDefinitionsUrl(base_url);

    auto http_client = CreateODataHttpClient();
    HttpUrl url(entity_defs_url);

    auto client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
    client->SetODataVersionDirectly(ODataVersion::V4);

    ERPL_TRACE_INFO("DATAVERSE_CLIENT", "Created entity definitions client with URL: " + entity_defs_url);
    return client;
}

std::shared_ptr<ODataEntitySetClient> DataverseClientFactory::CreateEntityAttributesClient(
    const std::string &environment_url,
    const std::string &logical_name,
    std::shared_ptr<HttpAuthParams> auth_params,
    const std::string &api_version) {

    ERPL_TRACE_DEBUG("DATAVERSE_CLIENT", "Creating entity attributes client for: " + logical_name);

    std::string base_url = DataverseUrlBuilder::BuildApiUrl(environment_url, api_version);
    std::string attrs_url = DataverseUrlBuilder::BuildEntityAttributesUrl(base_url, logical_name);

    auto http_client = CreateODataHttpClient();
    HttpUrl url(attrs_url);

    auto client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
    client->SetODataVersionDirectly(ODataVersion::V4);

    ERPL_TRACE_INFO("DATAVERSE_CLIENT", "Created entity attributes client with URL: " + attrs_url);
    return client;
}

std::shared_ptr<ODataEntitySetClient> DataverseClientFactory::CreateEntitySetClient(
    const std::string &environment_url,
    const std::string &entity_set,
    std::shared_ptr<HttpAuthParams> auth_params,
    const std::string &api_version) {

    ERPL_TRACE_DEBUG("DATAVERSE_CLIENT", "Creating entity set client for: " + entity_set);

    std::string base_url = DataverseUrlBuilder::BuildApiUrl(environment_url, api_version);
    std::string entity_set_url = DataverseUrlBuilder::BuildEntitySetUrl(base_url, entity_set);

    auto http_client = CreateODataHttpClient();
    HttpUrl url(entity_set_url);

    auto client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
    client->SetODataVersionDirectly(ODataVersion::V4);

    ERPL_TRACE_INFO("DATAVERSE_CLIENT", "Created entity set client with URL: " + entity_set_url);
    return client;
}

std::shared_ptr<ODataServiceClient> DataverseClientFactory::CreateServiceClient(
    const std::string &environment_url,
    std::shared_ptr<HttpAuthParams> auth_params,
    const std::string &api_version) {

    ERPL_TRACE_DEBUG("DATAVERSE_CLIENT", "Creating service client for: " + environment_url);

    std::string base_url = DataverseUrlBuilder::BuildApiUrl(environment_url, api_version);

    auto http_client = CreateODataHttpClient();
    HttpUrl url(base_url);

    auto client = std::make_shared<ODataServiceClient>(http_client, url, auth_params);

    ERPL_TRACE_INFO("DATAVERSE_CLIENT", "Created service client with URL: " + base_url);
    return client;
}

} // namespace erpl_web
