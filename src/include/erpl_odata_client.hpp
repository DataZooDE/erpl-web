#pragma once
#include "cpptrace/cpptrace.hpp"
#include "yyjson.hpp"

#include "erpl_http_client.hpp"
#include "erpl_odata_edm.hpp"
#include "erpl_odata_content.hpp"

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
    ODataClient(std::shared_ptr<CachingHttpClient> http_client, const HttpUrl& url, std::shared_ptr<HttpAuthParams> auth_params)
        : http_client(http_client)
        , url(url) 
        , auth_params(auth_params)
    {}

    virtual ~ODataClient() = default;

    virtual Edmx GetMetadata()
    {    
        auto metadata_url = GetMetadataContextUrl();
        auto cached_edmx = EdmCache::GetInstance().Get(metadata_url);
        if (cached_edmx) {
            //std::cout << "Using cached Metadata for: " << metadata_url << std::endl;
            return *cached_edmx;
        }

        auto metadata_response = DoMetadataHttpGet(metadata_url);

        auto content = metadata_response->Content();
        auto edmx = Edmx::FromXml(content);

        EdmCache::GetInstance().Set(metadata_url, edmx);
        return edmx;
    }

    virtual std::shared_ptr<TResponse> Get(bool get_next = false) = 0;
    virtual std::string GetMetadataContextUrl() = 0;

    std::string Url() { return url.ToString(); }
    std::shared_ptr<HttpClient> GetHttpClient() { return http_client->GetHttpClient(); }
    std::shared_ptr<HttpAuthParams> AuthParams() { return auth_params; }
protected:
    std::shared_ptr<CachingHttpClient> http_client;
    HttpUrl url;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::shared_ptr<TResponse> current_response;

    std::unique_ptr<HttpResponse> DoHttpGet(const HttpUrl& url) {
        auto http_request = HttpRequest(HttpMethod::GET, url);
        if (auth_params != nullptr) {
            http_request.AuthHeadersFromParams(*auth_params);
        }

        auto http_response = http_client->SendRequest(http_request);

        if (http_response == nullptr || http_response->Code() != 200) {
            std::stringstream ss;
            ss << "Failed to get OData response: " << http_response->Code() << std::endl;
            ss << "Content: " << std::endl << http_response->Content() << std::endl;
            ss << cpptrace::generate_trace().to_string() << std::endl;
            throw std::runtime_error(ss.str());
        }

        return http_response;
    }

    std::unique_ptr<HttpResponse> DoMetadataHttpGet(const std::string& metadata_url_raw) 
    {
        //std::cout << "Fetching Metadata from: " << metadata_url_raw << std::endl;
        auto current_svc_url = url;
        auto metadata_request = HttpRequest(HttpMethod::GET, HttpUrl::MergeWithBaseUrlIfRelative(current_svc_url, metadata_url_raw));
        if (auth_params != nullptr) {
            metadata_request.AuthHeadersFromParams(*auth_params);
        }

        std::unique_ptr<HttpResponse> metadata_response;
        for (size_t num_retries = 3; num_retries > 0; --num_retries) {
            metadata_response = http_client->SendRequest(metadata_request);
            if (metadata_response != nullptr && metadata_response->Code() == 200) {
                return metadata_response;
            }

            // The OData v4 spec defines that the metadata document when given as a relative URL
            // can also be attachted to the root ULR of the service. However we don't have that
            // URL here. So what we can do is to try to pop the last segment of the URL path and 
            // try whether that is the service root URL.
            current_svc_url = current_svc_url.PopPath();
            metadata_request = HttpRequest(HttpMethod::GET, HttpUrl::MergeWithBaseUrlIfRelative(current_svc_url, metadata_url_raw));
            if (auth_params != nullptr) {
                metadata_request.AuthHeadersFromParams(*auth_params);
            }   
        }

        std::stringstream ss;
        ss << "Failed to get OData metadata from " << HttpUrl::MergeWithBaseUrlIfRelative(url, metadata_url_raw).ToString();
        if (metadata_response == nullptr) {
            ss << ": " << metadata_response->Code() << std::endl;
            ss << "Content: " << std::endl << metadata_response->Content() << std::endl;
        }
        throw std::runtime_error(ss.str());

        return metadata_response;
    }
};

// -------------------------------------------------------------------------------------------------

class ODataEntitySetClient : public ODataClient<ODataEntitySetResponse> {
public:
    ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url, const Edmx& edmx);
    ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url);
    ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url, const Edmx& edmx, std::shared_ptr<HttpAuthParams> auth_params);
    ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url, std::shared_ptr<HttpAuthParams> auth_params);

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
    ODataServiceClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url, std::shared_ptr<HttpAuthParams> auth_params);
    
    std::shared_ptr<ODataServiceResponse> Get(bool get_next = false) override;
    std::string GetMetadataContextUrl() override;
};

} // namespace erpl_web
