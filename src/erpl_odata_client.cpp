#include <cpptrace/cpptrace.hpp>
#include <regex>

#include "erpl_odata_client.hpp"
#include "erpl_tracing.hpp"

namespace erpl_web {


// ----------------------------------------------------------------------

ODataEntitySetResponse::ODataEntitySetResponse(std::unique_ptr<HttpResponse> http_response, ODataVersion odata_version)
    : ODataResponse(std::move(http_response))
    , odata_version(odata_version)
{ 
    ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Created OData entity set response");
    ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Response content type: " + ContentType());
    ERPL_TRACE_DEBUG("ODATA_RESPONSE", std::string("OData version: ") + (odata_version == ODataVersion::V2 ? "V2" : "V4"));
}

std::string ODataEntitySetResponse::MetadataContextUrl()
{
    return Content()->MetadataContextUrl();
}

std::optional<std::string> ODataEntitySetResponse::NextUrl()
{
    return Content()->NextUrl();
}

std::vector<std::vector<duckdb::Value>> ODataEntitySetResponse::ToRows(std::vector<std::string> &column_names, 
                                                             std::vector<duckdb::LogicalType> &column_types)
{
    return Content()->ToRows(column_names, column_types);
}

// ----------------------------------------------------------------------

// ODataClient base class implementation is in erpl_odata_client.hpp

template<typename TResponse>
void ODataClient<TResponse>::DetectODataVersion()
{
    // Get the current metadata to detect version
    auto metadata_url = GetMetadataContextUrl();
    auto cached_edmx = EdmCache::GetInstance().Get(metadata_url);
    if (cached_edmx) {
        // Version is already detected from the cached metadata
        // Extract the version from cached metadata and update client version
        odata_version = cached_edmx->GetVersion();
        return;
    }

    // Fetch metadata to detect version
    auto metadata_response = DoMetadataHttpGet(metadata_url);
    auto content = metadata_response->Content();
    
    // Parse metadata to detect version
    auto edmx = Edmx::FromXml(content);
    odata_version = edmx.GetVersion();
    
    // Cache the metadata with detected version
    EdmCache::GetInstance().Set(metadata_url, edmx);
}

// ----------------------------------------------------------------------

ODataEntitySetClient::ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url, const Edmx& edmx)
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url, nullptr)
{ }

ODataEntitySetClient::ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url)
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url, nullptr)
{ }

ODataEntitySetClient::ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url, const Edmx& edmx, std::shared_ptr<HttpAuthParams> auth_params)
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url, auth_params)
{ }

ODataEntitySetClient::ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url, std::shared_ptr<HttpAuthParams> auth_params)
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url, auth_params)
{ }

std::string ODataEntitySetClient::GetMetadataContextUrl()
{
    // For SAP Datasphere OData consumption, we need to keep the asset ID in the metadata URL
    // The URL structure is: /api/v1/dwc/consumption/relational/{spaceId}/{assetId}
    // We want: /api/v1/dwc/consumption/relational/{spaceId}/{assetId}/$metadata
    
    // Check if this is a Datasphere URL (contains /dwc/consumption/)
    auto url_str = url.ToString();
    if (url_str.find("/dwc/consumption/") != std::string::npos) {
        // For Datasphere, append $metadata to the current URL
        return url_str + "/$metadata";
    }
    
    // For standard OData services, extract service root URL from entity set URL
    // For entity set URLs like https://services.odata.org/TripPinRESTierService/People?$top=1
    // We need to get the service root: https://services.odata.org/TripPinRESTierService
    auto service_root_url = url.PopPath();
    
    // Remove any query parameters from the service root
    auto service_root_str = service_root_url.ToString();
    auto query_pos = service_root_str.find('?');
    if (query_pos != std::string::npos) {
        service_root_str = service_root_str.substr(0, query_pos);
    }
    
    return service_root_str + "/$metadata";
}

std::shared_ptr<ODataEntitySetContent> ODataEntitySetResponse::CreateODataContent(const std::string& content, ODataVersion odata_version)
{
    ERPL_TRACE_DEBUG("ODATA_CONTENT", "Creating OData content from response");
    ERPL_TRACE_DEBUG("ODATA_CONTENT", "Content type: " + ContentType());
    ERPL_TRACE_DEBUG("ODATA_CONTENT", "Content size: " + std::to_string(content.length()) + " bytes");
    
    if (ODataJsonContentMixin::IsJsonContentType(ContentType())) {
        // For JSON content, detect the actual version from the response content
        // This is more reliable than using the metadata version since the response format
        // might differ from what the metadata suggests
        auto detected_version = ODataJsonContentMixin::DetectODataVersion(content);
        ERPL_TRACE_DEBUG("ODATA_CONTENT", std::string("Detected OData version from response: ") + (detected_version == ODataVersion::V2 ? "V2" : "V4"));
        ERPL_TRACE_DEBUG("ODATA_CONTENT", std::string("Metadata suggested version: ") + (odata_version == ODataVersion::V2 ? "V2" : "V4"));
        
        auto content_obj = std::make_shared<ODataEntitySetJsonContent>(content);
        content_obj->SetODataVersion(detected_version);
        return content_obj;
    }
        
    ERPL_TRACE_ERROR("ODATA_CONTENT", "Unsupported content type: " + ContentType());
    throw std::runtime_error("Unsupported OData content type: " + ContentType());
}

std::shared_ptr<ODataEntitySetResponse> ODataEntitySetClient::Get(bool get_next)
{
    if (! get_next && current_response != nullptr) {
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "Returning cached response");
        return current_response;
    }

    ERPL_TRACE_INFO("ODATA_CLIENT", "Fetching OData request from: " + url.ToString() + " (get_next: " + std::to_string(get_next) + ")");

    // Ensure OData version is detected before making any requests
    if (odata_version == ODataVersion::UNKNOWN) {
        DetectODataVersion();
    }

    if (get_next && current_response != nullptr) {
        auto next_url = current_response->NextUrl();
        if (!next_url) {
            ERPL_TRACE_DEBUG("ODATA_CLIENT", "No next URL available for pagination");
            return nullptr;
        }

        url = HttpUrl::MergeWithBaseUrlIfRelative(url, next_url.value());
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "Using next URL: " + url.ToString());
    }

    ERPL_TRACE_DEBUG("ODATA_CLIENT", "Executing HTTP GET request");
    auto http_response = DoHttpGet(url);
    
    // Detect OData version from raw HTTP response content if not already known
    if (odata_version == ODataVersion::UNKNOWN) {
        auto content_str = http_response->Content();
        if (ODataJsonContentMixin::IsJsonContentType(http_response->ContentType())) {
            auto detected_version = ODataJsonContentMixin::DetectODataVersion(content_str);
            odata_version = detected_version;
            std::string version_str = (odata_version == ODataVersion::V2 ? "V2" : "V4");
            ERPL_TRACE_DEBUG("ODATA_CLIENT", "Detected OData version from response: " + version_str);
        }
    }
    
    ERPL_TRACE_DEBUG("ODATA_CLIENT", "Creating OData response object");
    current_response = std::make_shared<ODataEntitySetResponse>(std::move(http_response), odata_version);
    
    ERPL_TRACE_DEBUG("ODATA_CLIENT", "Successfully created OData response");

    return current_response;
}

EntitySet ODataEntitySetClient::GetCurrentEntitySetType()
{
    auto edmx = GetMetadata();

    // Extract entity set name from the URL path
    // For URLs like https://services.odata.org/TripPinRESTierService/People
    // Extract "People" as the entity set name
    auto path = url.Path();
    if (path.empty() || path == "/") {
        throw std::runtime_error("Invalid entity set URL: no path segment found");
    }
    
    // Remove leading slash if present
    if (path[0] == '/') {
        path = path.substr(1);
    }
    
    // Find the last path segment
    auto last_slash_pos = path.find_last_of('/');
    std::string entity_set_name;
    if (last_slash_pos == std::string::npos) {
        entity_set_name = path;
    } else {
        entity_set_name = path.substr(last_slash_pos + 1);
    }
    
    if (entity_set_name.empty()) {
        throw std::runtime_error("Invalid entity set URL: empty entity set name");
    }
    
    auto entity_set_type = edmx.FindEntitySet(entity_set_name);
    return entity_set_type;
}

EntityType ODataEntitySetClient::GetCurrentEntityType()
{
    auto edmx = GetMetadata();

    auto entity_set_type = GetCurrentEntitySetType();
    auto entity_type = std::get<EntityType>(edmx.FindType(entity_set_type.entity_type_name));
    return entity_type;
}

std::vector<std::string> ODataEntitySetClient::GetResultNames()
{
    auto edmx = GetMetadata();
    auto entity_set_type = GetCurrentEntitySetType();
    auto entity_type = GetCurrentEntityType();

    auto type_conv = DuckTypeConverter(edmx);
    auto entity_struct = type_conv(GetCurrentEntityType());

    std::vector<std::string> ret_names;
    for (const auto& child : StructType::GetChildTypes(entity_struct)) {
        ret_names.push_back(child.first);
    }

    return ret_names;
}

std::vector<duckdb::LogicalType> ODataEntitySetClient::GetResultTypes()
{
    auto edmx = GetMetadata();
    auto type_conv = DuckTypeConverter(edmx);
    auto entity_struct = type_conv(GetCurrentEntityType());

    std::vector<duckdb::LogicalType> ret_types;
    for (const auto& child : StructType::GetChildTypes(entity_struct)) {
        ret_types.push_back(child.second);
    }

    return ret_types;
}

// ----------------------------------------------------------------------

ODataServiceResponse::ODataServiceResponse(std::unique_ptr<HttpResponse> http_response, ODataVersion odata_version)
    : ODataResponse(std::move(http_response))
    , odata_version(odata_version)
{ 
    ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Created OData service response");
    ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Response content type: " + ContentType());
    ERPL_TRACE_DEBUG("ODATA_RESPONSE", std::string("OData version: ") + (odata_version == ODataVersion::V2 ? "V2" : "V4"));
}

std::shared_ptr<ODataServiceContent> ODataServiceResponse::CreateODataContent(const std::string& content, ODataVersion odata_version)
{
    if (ODataJsonContentMixin::IsJsonContentType(ContentType())) {
        ERPL_TRACE_DEBUG("ODATA_CONTENT", std::string("Creating JSON content with OData version: ") + (odata_version == ODataVersion::V2 ? "V2" : "V4"));
        auto content_obj = std::make_shared<ODataServiceJsonContent>(content);
        content_obj->SetODataVersion(odata_version);
        return content_obj;
    }
        
    throw std::runtime_error("Unsupported OData content type: " + ContentType());
}

std::string ODataServiceResponse::MetadataContextUrl()
{
    return Content()->MetadataContextUrl();
}

std::vector<ODataEntitySetReference> ODataServiceResponse::EntitySets()
{
    return Content()->EntitySets();
}

// ----------------------------------------------------------------------

ODataServiceClient::ODataServiceClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url)
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url, nullptr)
{ }

ODataServiceClient::ODataServiceClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url, std::shared_ptr<HttpAuthParams> auth_params)
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url, auth_params)
{ }

std::shared_ptr<ODataServiceResponse> ODataServiceClient::Get(bool get_next)
{
    if (current_response != nullptr) {
        return current_response;
    }

    auto http_response = DoHttpGet(url);
    current_response = std::make_shared<ODataServiceResponse>(std::move(http_response), odata_version);

    return current_response;
}

std::string ODataServiceClient::GetMetadataContextUrl()
{
    Get();
    return current_response->MetadataContextUrl();
}

} // namespace erpl_web
