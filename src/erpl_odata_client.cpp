#include <cpptrace/cpptrace.hpp>
#include <regex>

#include "erpl_odata_client.hpp"
#include "erpl_tracing.hpp"

namespace erpl_web {


// ----------------------------------------------------------------------

ODataEntitySetResponse::ODataEntitySetResponse(std::unique_ptr<HttpResponse> http_response)
    : ODataResponse(std::move(http_response))
{ 
    ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Created OData entity set response");
    ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Response content type: " + ContentType());
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
    // Extract service root URL from entity set URL
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

std::shared_ptr<ODataEntitySetContent> ODataEntitySetResponse::CreateODataContent(const std::string& content)
{
    ERPL_TRACE_DEBUG("ODATA_CONTENT", "Creating OData content from response");
    ERPL_TRACE_DEBUG("ODATA_CONTENT", "Content type: " + ContentType());
    ERPL_TRACE_DEBUG("ODATA_CONTENT", "Content size: " + std::to_string(content.length()) + " bytes");
    
    if (ODataJsonContentMixin::IsJsonContentType(ContentType())) {
        ERPL_TRACE_DEBUG("ODATA_CONTENT", "Creating JSON content");
        return std::make_shared<ODataEntitySetJsonContent>(content);
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
    
    ERPL_TRACE_DEBUG("ODATA_CLIENT", "Creating OData response object");
    current_response = std::make_shared<ODataEntitySetResponse>(std::move(http_response));
    
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

ODataServiceResponse::ODataServiceResponse(std::unique_ptr<HttpResponse> http_response)
    : ODataResponse(std::move(http_response))
{ }

std::shared_ptr<ODataServiceContent> ODataServiceResponse::CreateODataContent(const std::string& content)
{
    if (ODataJsonContentMixin::IsJsonContentType(ContentType())) {
        return std::make_shared<ODataServiceJsonContent>(content);
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
    current_response = std::make_shared<ODataServiceResponse>(std::move(http_response));

    return current_response;
}

std::string ODataServiceClient::GetMetadataContextUrl()
{
    Get();
    return current_response->MetadataContextUrl();
}

} // namespace erpl_web
