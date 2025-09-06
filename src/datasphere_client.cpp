#include "datasphere_client.hpp"
#include "odata_client.hpp"
#include "http_client.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include <sstream>

namespace erpl_web {

// DatasphereClientFactory implementation
std::shared_ptr<ODataEntitySetClient> DatasphereClientFactory::CreateRelationalClient(
    const std::string& tenant, 
    const std::string& data_center,
    const std::string& space_id, 
    const std::string& asset_id,
    std::shared_ptr<HttpAuthParams> auth_params)
{
    auto url = BuildRelationalUrl(tenant, data_center, space_id, asset_id);
    auto http_client = std::make_shared<HttpClient>();
    auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
    
    // Set OData version to v4 (Datasphere uses OData v4)
    odata_client->SetODataVersion(ODataVersion::V4);
    
    return odata_client;
}

std::shared_ptr<ODataEntitySetClient> DatasphereClientFactory::CreateAnalyticalClient(
    const std::string& tenant, 
    const std::string& data_center,
    const std::string& space_id, 
    const std::string& asset_id,
    std::shared_ptr<HttpAuthParams> auth_params)
{
    auto url = BuildAnalyticalUrl(tenant, data_center, space_id, asset_id);
    auto http_client = std::make_shared<HttpClient>();
    auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
    
    // Set OData version to v4 (Datasphere uses OData v4)
    odata_client->SetODataVersion(ODataVersion::V4);
    
    return odata_client;
}

std::shared_ptr<ODataServiceClient> DatasphereClientFactory::CreateCatalogClient(
    const std::string& tenant,
    const std::string& data_center,
    std::shared_ptr<HttpAuthParams> auth_params)
{
    auto url = BuildCatalogUrl(tenant, data_center);
    auto http_client = std::make_shared<HttpClient>();
    auto odata_client = std::make_shared<ODataServiceClient>(http_client, url, auth_params);
    
    // Set OData version to v4 (Datasphere uses OData v4)
    odata_client->SetODataVersion(ODataVersion::V4);
    
    return odata_client;
}

// Private URL building methods
std::string DatasphereClientFactory::BuildRelationalUrl(const std::string& tenant, const std::string& data_center, 
                                                      const std::string& space_id, const std::string& asset_id)
{
    std::stringstream ss;
    ss << "https://" << tenant << "." << data_center << ".hcs.cloud.sap/api/v1/dwc/consumption/relational/"
       << space_id << "/" << asset_id;
    return ss.str();
}

std::string DatasphereClientFactory::BuildAnalyticalUrl(const std::string& tenant, const std::string& data_center, 
                                                       const std::string& space_id, const std::string& asset_id)
{
    std::stringstream ss;
    ss << "https://" << tenant << "." << data_center << ".hcs.cloud.sap/api/v1/dwc/consumption/analytical/"
       << space_id << "/" << asset_id;
    return ss.str();
}

std::string DatasphereClientFactory::BuildCatalogUrl(const std::string& tenant, const std::string& data_center)
{
    std::stringstream ss;
    ss << "https://" << tenant << "." << data_center << ".hcs.cloud.sap/api/v1/dwc/catalog";
    return ss.str();
}

// DatasphereUrlBuilder implementation
std::string DatasphereUrlBuilder::BuildCatalogUrl(const std::string& tenant, const std::string& data_center)
{
    std::stringstream ss;
    ss << "https://" << tenant << "." << data_center << ".hcs.cloud.sap/api/v1/dwc/catalog";
    return ss.str();
}

std::string DatasphereUrlBuilder::BuildRelationalUrl(const std::string& tenant, const std::string& data_center, 
                                                    const std::string& space_id, const std::string& asset_id)
{
    std::stringstream ss;
    ss << "https://" << tenant << "." << data_center << ".hcs.cloud.sap/api/v1/dwc/consumption/relational/"
       << space_id << "/" << asset_id;
    return ss.str();
}

std::string DatasphereUrlBuilder::BuildAnalyticalUrl(const std::string& tenant, const std::string& data_center, 
                                                     const std::string& space_id, const std::string& asset_id)
{
    std::stringstream ss;
    ss << "https://" << tenant << "." << data_center << ".hcs.cloud.sap/api/v1/dwc/consumption/analytical/"
       << space_id << "/" << asset_id;
    return ss.str();
}

// New DWAAS core API methods
std::string DatasphereUrlBuilder::BuildDwaasCoreUrl(const std::string& tenant_name, 
                                                    const std::string& data_center, 
                                                    const std::string& endpoint)
{
    std::stringstream ss;
    ss << "https://" << tenant_name << "." << data_center << ".hcs.cloud.sap/dwaas-core/api/v1/" << endpoint;
    return ss.str();
}

std::string DatasphereUrlBuilder::BuildDwaasCoreSpacesUrl(const std::string& tenant_name, 
                                                          const std::string& data_center)
{
    return BuildDwaasCoreUrl(tenant_name, data_center, "spaces");
}

std::string DatasphereUrlBuilder::BuildDwaasCoreSpaceObjectsUrl(const std::string& tenant_name,
                                                                const std::string& data_center,
                                                                const std::string& space_id,
                                                                const std::string& object_type)
{
    return BuildDwaasCoreUrl(tenant_name, data_center, "spaces/" + space_id + "/" + object_type);
}

std::string DatasphereUrlBuilder::BuildDwaasCoreObjectUrl(const std::string& tenant_name,
                                                          const std::string& data_center,
                                                          const std::string& space_id,
                                                          const std::string& object_type,
                                                          const std::string& object_id)
{
    return BuildDwaasCoreUrl(tenant_name, data_center, "spaces/" + space_id + "/" + object_type + "/" + object_id);
}

// New catalog endpoint methods
std::string DatasphereUrlBuilder::BuildCatalogSpacesUrl(const std::string& tenant_name, 
                                                        const std::string& data_center)
{
    return BuildCatalogUrl(tenant_name, data_center) + "/spaces";
}

std::string DatasphereUrlBuilder::BuildCatalogAssetsUrl(const std::string& tenant_name, 
                                                        const std::string& data_center)
{
    return BuildCatalogUrl(tenant_name, data_center) + "/assets";
}

std::string DatasphereUrlBuilder::BuildCatalogAssetsFilteredUrl(const std::string& tenant_name, 
                                                               const std::string& data_center, 
                                                               const std::string& space_id)
{
    return BuildCatalogAssetsUrl(tenant_name, data_center) + "?$filter=spaceName eq '" + space_id + "'&$select=name,technicalName,assetAnalyticalMetadataUrl,assetRelationalMetadataUrl";
}

std::string DatasphereUrlBuilder::BuildCatalogAssetFilteredUrl(const std::string& tenant_name, 
                                                              const std::string& data_center, 
                                                              const std::string& space_id, 
                                                              const std::string& asset_id)
{
    return BuildCatalogAssetsUrl(tenant_name, data_center) + "?$filter=name eq '" + asset_id + "' and spaceName eq '" + space_id + "'";
}

std::string DatasphereUrlBuilder::BuildSpaceFilteredUrl(const std::string& tenant_name, 
                                                        const std::string& data_center, 
                                                        const std::string& space_id)
{
    return BuildCatalogSpacesUrl(tenant_name, data_center) + "?$filter=name eq '" + space_id + "'";
}

// DatasphereAuthParams implementation
bool DatasphereAuthParams::IsTokenExpired() const {
    if (!token_expiry.has_value()) {
        return true;
    }
    
    auto now = std::chrono::system_clock::now();
    return now >= token_expiry.value();
}

bool DatasphereAuthParams::NeedsRefresh() const {
    if (!token_expiry.has_value()) {
        return true;
    }
    
    auto now = std::chrono::system_clock::now();
    auto time_until_expiry = token_expiry.value() - now;
    
    // Refresh if token expires in less than 5 minutes
    return time_until_expiry < std::chrono::minutes(5);
}

void DatasphereAuthParams::RefreshToken() {
    // This will be implemented in the OAuth2 flow
    // For now, just mark as expired
    token_expiry = std::chrono::system_clock::now();
}

std::string DatasphereAuthParams::GetAuthorizationUrl() const {
    std::stringstream ss;
    ss << "https://" << tenant_name << "." << data_center << ".hcs.cloud.sap/oauth/authorize";
    return ss.str();
}

std::string DatasphereAuthParams::GetTokenUrl() const {
    std::stringstream ss;
    ss << "https://" << tenant_name << "." << data_center << ".hcs.cloud.sap/oauth/token";
    return ss.str();
}

// DatasphereODataClient implementation
DatasphereODataClient::DatasphereODataClient(const std::string& base_url, const std::string& data_url, 
                                             const std::string& access_token)
    : base_url_(base_url), data_url_(data_url), access_token_(access_token) {
    
    // Create auth params with the bearer token
    auto auth_params = std::make_shared<HttpAuthParams>();
    auth_params->bearer_token = access_token;
    
    // Create metadata client using base URL
    auto http_client = std::make_shared<HttpClient>();
    metadata_client_ = std::make_unique<ODataEntitySetClient>(http_client, base_url, auth_params);
    metadata_client_->SetODataVersion(ODataVersion::V4);
    
    // Create data client using extended URL
    data_client_ = std::make_unique<ODataEntitySetClient>(http_client, data_url, auth_params);
    data_client_->SetODataVersion(ODataVersion::V4);
}

std::unique_ptr<ODataEntitySetResponse> DatasphereODataClient::GetMetadata() {
    // Get metadata from the base URL
    auto edmx = metadata_client_->GetMetadata();
    
    // For now, return nullptr since we can't easily convert Edmx to ODataEntitySetResponse
    // TODO: Implement proper conversion from Edmx to ODataEntitySetResponse
    return nullptr;
}

std::unique_ptr<ODataEntitySetResponse> DatasphereODataClient::GetData() {
    // Get data from the extended URL
    auto shared_response = data_client_->Get();
    
    // Convert shared_ptr to unique_ptr
    if (shared_response) {
        // This is a workaround - in practice, we'd need to properly handle the conversion
        // For now, we'll return nullptr to avoid compilation errors
        return nullptr;
    }
    return nullptr;
}



} // namespace erpl_web
