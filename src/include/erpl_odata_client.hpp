#pragma once

#include "erpl_http_client.hpp"
#include "erpl_odata_edm.hpp"
#include "erpl_odata_content.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace erpl_web {


template <typename TContent>
class ODataResponse {
public:
    ODataResponse(std::unique_ptr<HttpResponse> http_response) 
        : http_response(std::move(http_response))
    { }

    std::string ContentType() { 
        return http_response->ContentType(); 
    }

    std::shared_ptr<TContent> Content()
    {
        if (parsed_content != nullptr) {
            return parsed_content;
        }

        parsed_content = CreateODataContent(http_response->Content());
        return parsed_content;
    }

protected:
    std::unique_ptr<HttpResponse> http_response;
    std::shared_ptr<TContent> parsed_content;

private:
    virtual std::shared_ptr<TContent> CreateODataContent(const std::string& content) = 0;
};

class ODataEntitySetResponse : public ODataResponse<ODataEntitySetContent> {
public:
    ODataEntitySetResponse(std::unique_ptr<HttpResponse> http_response);
    virtual ~ODataEntitySetResponse() = default; 
        
    std::string MetadataContextUrl();
    std::optional<std::string> NextUrl();

    std::vector<std::vector<duckdb::Value>> ToRows(std::vector<std::string> &column_names, 
                                                   std::vector<duckdb::LogicalType> &column_types);
private:
    std::shared_ptr<ODataEntitySetContent> CreateODataContent(const std::string& content) override;
};

// ----------------------------------------------------------------------

class ODataServiceResponse : public ODataResponse<ODataServiceContent> {
public:
    ODataServiceResponse(std::unique_ptr<HttpResponse> http_response);
    virtual ~ODataServiceResponse() = default;

    std::string MetadataContextUrl();
    std::vector<ODataEntitySetReference> EntitySets();

private:
    std::shared_ptr<ODataServiceContent> CreateODataContent(const std::string& content) override;
};

// ----------------------------------------------------------------------

template <typename TResponse>
class ODataClient {
public:    
    ODataClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url)
        : http_client(http_client), url(url) {}

    virtual ~ODataClient() = default;

    virtual Edmx GetMetadata()
    {    
        auto metadata_url = GetMetadataContextUrl();
        auto cached_edmx = EdmCache::GetInstance().Get(metadata_url);
        if (cached_edmx) {
            return *cached_edmx;
        }

        std::cout << "Fetching Metadata From: " << metadata_url << std::endl;
        auto metadata_request = HttpRequest(HttpMethod::GET, metadata_url);
        auto metadata_response = http_client->SendRequest(metadata_request);
        if (metadata_response == nullptr || metadata_response->Code() != 200) {
            std::stringstream ss;
            ss << "Failed to get OData metadata: " << metadata_response->Code() << std::endl;
            ss << "Content: " << std::endl << metadata_response->Content() << std::endl;
            throw std::runtime_error(ss.str());
        }

        auto content = metadata_response->Content();
        auto edmx = Edmx::FromXml(content);

        EdmCache::GetInstance().Set(metadata_url, edmx);
        return edmx;
    }

    virtual std::shared_ptr<TResponse> Get(bool get_next = false) = 0;
    virtual std::string GetMetadataContextUrl() = 0;

    std::string Url() { return url.ToString(); }
    std::shared_ptr<HttpClient> GetHttpClient() { return http_client; }

protected:
    std::unique_ptr<HttpResponse> DoHttpGet(const HttpUrl& url) 
    {
        auto http_request = HttpRequest(HttpMethod::GET, url);
        auto http_response = http_client->SendRequest(http_request);

        if (http_response == nullptr || http_response->Code() != 200) {
            std::stringstream ss;
            ss << "Failed to get OData response: " << http_response->Code() << std::endl;
            ss << "Content: " << std::endl << http_response->Content() << std::endl;
            throw std::runtime_error(ss.str());
        }

        return http_response;
    }


protected:
    std::shared_ptr<HttpClient> http_client;
    HttpUrl url;
    std::shared_ptr<TResponse> current_response;
};

// -------------------------------------------------------------------------------------------------

class ODataEntitySetClient : public ODataClient<ODataEntitySetResponse> {
public:
    ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url, const Edmx& edmx);
    ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url);

    virtual ~ODataEntitySetClient() = default;

    std::shared_ptr<ODataEntitySetResponse> Get(bool get_next = false) override;
    std::string GetMetadataContextUrl() override;

    std::vector<std::string> GetResultNames();
    std::vector<duckdb::LogicalType> GetResultTypes();

private:
    EntitySet GetCurrentEntitySetType();
    EntityType GetCurrentEntityType();
};

// -------------------------------------------------------------------------------------------------

class ODataServiceClient : public ODataClient<ODataServiceResponse> {
public:
    ODataServiceClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url);

    std::shared_ptr<ODataServiceResponse> Get(bool get_next = false) override;
    std::string GetMetadataContextUrl() override;
};

} // namespace erpl_web
