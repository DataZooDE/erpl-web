#include "sac_client.hpp"
#include "sac_url_builder.hpp"
#include "http_client.hpp"
#include <chrono>

namespace erpl_web {

// SacAuthParams implementation

bool SacAuthParams::IsTokenExpired() const {
    if (!token_expiry) {
        return false;
    }
    auto now = std::chrono::system_clock::now();
    return now >= *token_expiry;
}

bool SacAuthParams::NeedsRefresh() const {
    if (!token_expiry) {
        return false;
    }
    auto now = std::chrono::system_clock::now();
    auto time_to_expiry = *token_expiry - now;
    // Refresh if less than 5 minutes remaining
    return time_to_expiry < std::chrono::minutes(5);
}

std::string SacAuthParams::GetAuthorizationUrl() const {
    // SAC OAuth2 authorization endpoint
    // Format: https://tenant.region.sapanalytics.cloud/oauth2/authorize
    return SacUrlBuilder::BuildBaseUrl(tenant, region) + "/oauth2/authorize";
}

std::string SacAuthParams::GetTokenUrl() const {
    // SAC OAuth2 token endpoint
    // Format: https://tenant.region.sapanalytics.cloud/oauth2/token
    return SacUrlBuilder::BuildBaseUrl(tenant, region) + "/oauth2/token";
}

// SacClientFactory implementation

std::shared_ptr<ODataEntitySetClient> SacClientFactory::CreatePlanningDataClient(
    const std::string& tenant,
    const std::string& region,
    const std::string& model_id,
    std::shared_ptr<HttpAuthParams> auth_params) {

    auto url_str = BuildPlanningDataUrl(tenant, region, model_id);
    HttpUrl url(url_str);

    // Create shared HTTP client
    auto http_client = std::make_shared<HttpClient>();

    // Create OData client with automatic version detection (V4 for SAC)
    auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
    odata_client->SetODataVersionDirectly(ODataVersion::V4);

    return odata_client;
}

std::shared_ptr<ODataEntitySetClient> SacClientFactory::CreateStoryServiceClient(
    const std::string& tenant,
    const std::string& region,
    std::shared_ptr<HttpAuthParams> auth_params) {

    auto url_str = BuildStoryServiceUrl(tenant, region);
    HttpUrl url(url_str);

    auto http_client = std::make_shared<HttpClient>();
    auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
    odata_client->SetODataVersionDirectly(ODataVersion::V4);

    return odata_client;
}

std::shared_ptr<ODataEntitySetClient> SacClientFactory::CreateModelServiceClient(
    const std::string& tenant,
    const std::string& region,
    std::shared_ptr<HttpAuthParams> auth_params) {

    auto url_str = BuildModelServiceUrl(tenant, region);
    HttpUrl url(url_str);

    auto http_client = std::make_shared<HttpClient>();
    auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
    odata_client->SetODataVersionDirectly(ODataVersion::V4);

    return odata_client;
}

std::shared_ptr<ODataEntitySetClient> SacClientFactory::CreateDataExportClient(
    const std::string& tenant,
    const std::string& region,
    std::shared_ptr<HttpAuthParams> auth_params) {

    auto url_str = BuildDataExportUrl(tenant, region);
    HttpUrl url(url_str);

    auto http_client = std::make_shared<HttpClient>();
    auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
    odata_client->SetODataVersionDirectly(ODataVersion::V4);

    return odata_client;
}

std::shared_ptr<ODataServiceClient> SacClientFactory::CreateCatalogClient(
    const std::string& tenant,
    const std::string& region,
    std::shared_ptr<HttpAuthParams> auth_params) {

    auto url_str = BuildCatalogUrl(tenant, region);
    HttpUrl url(url_str);

    auto http_client = std::make_shared<HttpClient>();
    auto odata_service_client = std::make_shared<ODataServiceClient>(http_client, url, auth_params);
    odata_service_client->SetODataVersionDirectly(ODataVersion::V4);

    return odata_service_client;
}

std::string SacClientFactory::BuildPlanningDataUrl(
    const std::string& tenant,
    const std::string& region,
    const std::string& model_id) {
    return SacUrlBuilder::BuildPlanningDataUrl(tenant, region, model_id);
}

std::string SacClientFactory::BuildStoryServiceUrl(
    const std::string& tenant,
    const std::string& region) {
    return SacUrlBuilder::BuildStoryServiceUrl(tenant, region);
}

std::string SacClientFactory::BuildModelServiceUrl(
    const std::string& tenant,
    const std::string& region) {
    return SacUrlBuilder::BuildModelServiceUrl(tenant, region);
}

std::string SacClientFactory::BuildDataExportUrl(
    const std::string& tenant,
    const std::string& region) {
    return SacUrlBuilder::BuildDataExportUrl(tenant, region);
}

std::string SacClientFactory::BuildCatalogUrl(
    const std::string& tenant,
    const std::string& region) {
    return SacUrlBuilder::BuildODataServiceRootUrl(tenant, region);
}

// SacODataClient implementation

SacODataClient::SacODataClient(
    const std::string& base_url,
    const std::string& access_token)
    : base_url_(base_url), access_token_(access_token) {
    // Simplified implementation - actual clients are created on-demand
}

std::unique_ptr<ODataEntitySetResponse> SacODataClient::GetMetadata() {
    // Stub implementation - would fetch metadata from OData service
    return nullptr;
}

std::unique_ptr<ODataEntitySetResponse> SacODataClient::GetData() {
    // Stub implementation - would fetch data from OData service
    return nullptr;
}

} // namespace erpl_web
