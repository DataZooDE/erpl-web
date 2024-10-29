#include <cpptrace/cpptrace.hpp>
#include <regex>

#include "erpl_odata_client.hpp"

namespace erpl_web {


// ----------------------------------------------------------------------

ODataEntitySetResponse::ODataEntitySetResponse(std::unique_ptr<HttpResponse> http_response)
    : ODataResponse(std::move(http_response))
{ }

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
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url)
{ }

ODataEntitySetClient::ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url)
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url)
{ }

std::string ODataEntitySetClient::GetMetadataContextUrl()
{
    Get();
    return current_response->MetadataContextUrl();
}

std::shared_ptr<ODataEntitySetContent> ODataEntitySetResponse::CreateODataContent(const std::string& content)
{
    if (ODataJsonContentMixin::IsJsonContentType(ContentType())) {
        return std::make_shared<ODataEntitySetJsonContent>(content);
    }
        
    throw std::runtime_error("Unsupported OData content type: " + ContentType());
}

std::shared_ptr<ODataEntitySetResponse> ODataEntitySetClient::Get(bool get_next)
{
    if (! get_next && current_response != nullptr) {
        return current_response;
    }

    //std::cout << "Fetching OData Request From: [" << (get_next ? "next" : "first") << "] "; 
    //std::cout << url.ToString() << std::endl;
    //cpptrace::generate_trace().print();

    if (get_next && current_response != nullptr) {
        auto next_url = current_response->NextUrl();
        if (!next_url) {
            std::cout << "Tried to get next URL: No next URL found" << std::endl;
            return nullptr;
        }

        url = HttpUrl::MergeWithBaseUrlIfRelative(url, next_url.value());
        std::cout << "Tried to get next URL: " << url.ToString() << std::endl;
    }

    auto http_response = DoHttpGet(url);
    current_response = std::make_shared<ODataEntitySetResponse>(std::move(http_response));

    return current_response;
}

EntitySet ODataEntitySetClient::GetCurrentEntitySetType()
{
    auto edmx = GetMetadata();

    auto metadata_url = current_response->MetadataContextUrl();
    auto entity_set_type = edmx.FindEntitySet(metadata_url);
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
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url)
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
