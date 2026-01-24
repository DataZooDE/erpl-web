#include "business_central_client.hpp"
#include "tracing.hpp"

namespace erpl_web {

// URL Builder implementation
std::string BusinessCentralUrlBuilder::BuildApiUrl(const std::string &tenant_id, const std::string &environment) {
    return "https://api.businesscentral.dynamics.com/v2.0/" + tenant_id + "/" + environment + "/api/v2.0";
}

std::string BusinessCentralUrlBuilder::BuildCompanyUrl(const std::string &base_url, const std::string &company_id) {
    return base_url + "/companies(" + company_id + ")";
}

std::string BusinessCentralUrlBuilder::BuildEntitySetUrl(const std::string &company_url, const std::string &entity_set) {
    return company_url + "/" + entity_set;
}

std::string BusinessCentralUrlBuilder::BuildMetadataUrl(const std::string &base_url) {
    return base_url + "/$metadata";
}

std::string BusinessCentralUrlBuilder::BuildCompaniesUrl(const std::string &base_url) {
    return base_url + "/companies";
}

std::string BusinessCentralUrlBuilder::GetResourceUrl() {
    return "https://api.businesscentral.dynamics.com";
}

// Helper to create HTTP client for OData
static std::shared_ptr<HttpClient> CreateODataHttpClient() {
    HttpParams http_params;
    http_params.url_encode = false;  // OData handles URL encoding
    return std::make_shared<HttpClient>(http_params);
}

// Client Factory implementation
std::shared_ptr<ODataEntitySetClient> BusinessCentralClientFactory::CreateCompaniesClient(
    const std::string &tenant_id,
    const std::string &environment,
    std::shared_ptr<HttpAuthParams> auth_params) {

    ERPL_TRACE_DEBUG("BC_CLIENT", "Creating companies client for tenant: " + tenant_id + ", environment: " + environment);

    std::string base_url = BusinessCentralUrlBuilder::BuildApiUrl(tenant_id, environment);
    std::string companies_url = BusinessCentralUrlBuilder::BuildCompaniesUrl(base_url);

    auto http_client = CreateODataHttpClient();
    HttpUrl url(companies_url);

    auto client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
    client->SetODataVersionDirectly(ODataVersion::V4);

    ERPL_TRACE_INFO("BC_CLIENT", "Created companies client with URL: " + companies_url);
    return client;
}

std::shared_ptr<ODataEntitySetClient> BusinessCentralClientFactory::CreateEntitySetClient(
    const std::string &tenant_id,
    const std::string &environment,
    const std::string &company_id,
    const std::string &entity_set,
    std::shared_ptr<HttpAuthParams> auth_params) {

    ERPL_TRACE_DEBUG("BC_CLIENT", "Creating entity set client for: " + entity_set + " in company: " + company_id);

    std::string base_url = BusinessCentralUrlBuilder::BuildApiUrl(tenant_id, environment);
    std::string company_url = BusinessCentralUrlBuilder::BuildCompanyUrl(base_url, company_id);
    std::string entity_set_url = BusinessCentralUrlBuilder::BuildEntitySetUrl(company_url, entity_set);

    auto http_client = CreateODataHttpClient();
    HttpUrl url(entity_set_url);

    auto client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
    client->SetODataVersionDirectly(ODataVersion::V4);

    ERPL_TRACE_INFO("BC_CLIENT", "Created entity set client with URL: " + entity_set_url);
    return client;
}

std::shared_ptr<ODataServiceClient> BusinessCentralClientFactory::CreateCatalogClient(
    const std::string &tenant_id,
    const std::string &environment,
    std::shared_ptr<HttpAuthParams> auth_params) {

    ERPL_TRACE_DEBUG("BC_CLIENT", "Creating catalog client for tenant: " + tenant_id);

    std::string base_url = BusinessCentralUrlBuilder::BuildApiUrl(tenant_id, environment);

    auto http_client = CreateODataHttpClient();
    HttpUrl url(base_url);

    auto client = std::make_shared<ODataServiceClient>(http_client, url, auth_params);

    ERPL_TRACE_INFO("BC_CLIENT", "Created catalog client with URL: " + base_url);
    return client;
}

} // namespace erpl_web
