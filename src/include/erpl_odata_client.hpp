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
            ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Returning cached parsed content");
            return parsed_content;
        }

        ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Parsing HTTP response content");
        ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Content type: " + http_response->ContentType());
        ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Content size: " + std::to_string(http_response->Content().length()) + " bytes");
        
        // For now, use default v4 version - the derived classes will override this
        parsed_content = CreateODataContent(http_response->Content(), ODataVersion::V4);
        
        ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Successfully parsed content");
        return parsed_content;
    }

protected:
    std::unique_ptr<HttpResponse> http_response;
    std::shared_ptr<TContent> parsed_content;

private:
    virtual std::shared_ptr<TContent> CreateODataContent(const std::string& content, ODataVersion odata_version) = 0;
};

class ODataEntitySetResponse : public ODataResponse<ODataEntitySetContent> {
public:
    ODataEntitySetResponse(std::unique_ptr<HttpResponse> http_response, ODataVersion odata_version = ODataVersion::V4);
    virtual ~ODataEntitySetResponse() = default; 
        
    std::string MetadataContextUrl();
    std::optional<std::string> NextUrl();

    std::vector<std::vector<duckdb::Value>> ToRows(std::vector<std::string> &column_names, 
                                                   std::vector<duckdb::LogicalType> &column_types);
    
    ODataVersion GetODataVersion() const { return odata_version; }
private:
    std::shared_ptr<ODataEntitySetContent> CreateODataContent(const std::string& content, ODataVersion odata_version) override;
    
    ODataVersion odata_version;
};

// ----------------------------------------------------------------------

class ODataServiceResponse : public ODataResponse<ODataServiceContent> {
public:
    ODataServiceResponse(std::unique_ptr<HttpResponse> http_response, ODataVersion odata_version = ODataVersion::V4);
    virtual ~ODataServiceResponse() = default;

    std::string MetadataContextUrl();
    std::vector<ODataEntitySetReference> EntitySets();
    
    ODataVersion GetODataVersion() const { return odata_version; }
private:
    std::shared_ptr<ODataServiceContent> CreateODataContent(const std::string& content, ODataVersion odata_version) override;
    
    ODataVersion odata_version;
};

// ----------------------------------------------------------------------

template <typename TResponse>
class ODataClient {
public:    
    ODataClient(std::shared_ptr<CachingHttpClient> http_client, const HttpUrl& url, std::shared_ptr<HttpAuthParams> auth_params)
        : http_client(http_client)
        , url(url) 
        , auth_params(auth_params)
        , odata_version(ODataVersion::UNKNOWN) // Start with unknown version, will be detected from metadata
    {}

    virtual ~ODataClient() = default;

    // OData version support
    void SetODataVersion(ODataVersion version) { odata_version = version; }
    ODataVersion GetODataVersion() const { return odata_version; }
    
    // Auto-detect OData version from metadata
    void DetectODataVersion();

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
        
        // Auto-detect version from metadata if not already set
        if (odata_version == ODataVersion::UNKNOWN) {
            DetectODataVersion();
        }

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
    ODataVersion odata_version;

    std::unique_ptr<HttpResponse> DoHttpGet(const HttpUrl& url) {
        auto http_request = HttpRequest(HttpMethod::GET, url);
        
        // Set OData version and add appropriate headers
        http_request.SetODataVersion(odata_version);
        http_request.AddODataVersionHeaders();
        
        if (auth_params != nullptr) {
            http_request.AuthHeadersFromParams(*auth_params);
        }

        auto http_response = http_client->SendRequest(http_request);

        if (http_response == nullptr || http_response->Code() != 200) {
            std::stringstream ss;
            ss << "Failed to get OData response: " << http_response->Code() << std::endl;
            ss << "Content: " << std::endl << http_response->Content() << std::endl;
            ss << cpptrace::generate_trace(0, 10).to_string() << std::endl;
            throw std::runtime_error(ss.str());
        }

        return http_response;
    }

    std::unique_ptr<HttpResponse> DoMetadataHttpGet(const std::string& metadata_url_raw) 
    {
        //std::cout << "Fetching Metadata from: " << metadata_url_raw << std::endl;
        auto current_svc_url = url;
        auto metadata_request = HttpRequest(HttpMethod::GET, HttpUrl::MergeWithBaseUrlIfRelative(current_svc_url, metadata_url_raw));
        
        // For metadata requests, don't use version-specific headers since we don't know the version yet
        // This allows us to detect the version from the metadata response
        if (odata_version != ODataVersion::UNKNOWN) {
            // Only use version-specific headers if we already know the version
            metadata_request.SetODataVersion(odata_version);
            metadata_request.AddODataVersionHeaders();
        }
        
        // Override Accept header for metadata (XML is standard for both v2 and v4)
        metadata_request.headers["Accept"] = "application/xml";
        
        if (auth_params != nullptr) {
            metadata_request.AuthHeadersFromParams(*auth_params);
        }
        
        // Trace metadata request details
        std::stringstream request_trace;
        request_trace << "OData Metadata HTTP Request:" << std::endl;
        request_trace << "  URL: " << HttpUrl::MergeWithBaseUrlIfRelative(current_svc_url, metadata_url_raw).ToString() << std::endl;
        request_trace << "  Method: GET" << std::endl;
        request_trace << "  Headers:";
        for (const auto& header : metadata_request.headers) {
            request_trace << std::endl << "    " << header.first << ": " << header.second;
        }
        ERPL_TRACE_DEBUG("ODATA_CLIENT", request_trace.str());
        
        std::unique_ptr<HttpResponse> metadata_response;
        for (size_t num_retries = 3; num_retries > 0; --num_retries) {
            ERPL_TRACE_DEBUG("ODATA_CLIENT", "Metadata request attempt " + std::to_string(4 - num_retries) + " of 3");
            
            metadata_response = http_client->SendRequest(metadata_request);
            if (metadata_response != nullptr && metadata_response->Code() == 200) {
                // Trace successful metadata response
                std::stringstream response_trace;
                response_trace << "OData Metadata HTTP Success Response:" << std::endl;
                response_trace << "  Status Code: " << metadata_response->Code() << std::endl;
                response_trace << "  Content Type: " << metadata_response->ContentType() << std::endl;
                response_trace << "  Response Headers:";
                for (const auto& header : metadata_response->headers) {
                    response_trace << std::endl << "    " << header.first << ": " << header.second;
                }
                response_trace << std::endl << "  Response Body Size: " << metadata_response->Content().length() << " bytes";
                if (metadata_response->Content().length() > 0) {
                    response_trace << std::endl << "  Response Body Preview (first 500 chars): " << metadata_response->Content().substr(0, 500);
                }
                ERPL_TRACE_DEBUG("ODATA_CLIENT", response_trace.str());
                
                return metadata_response;
            }

            // Trace failed metadata response
            if (metadata_response != nullptr) {
                std::stringstream error_trace;
                error_trace << "OData Metadata HTTP Error Response (attempt " + std::to_string(4 - num_retries) + "):" << std::endl;
                error_trace << "  Status Code: " << metadata_response->Code() << std::endl;
                error_trace << "  Content Type: " << metadata_response->ContentType() << std::endl;
                error_trace << "  Response Headers:";
                for (const auto& header : metadata_response->headers) {
                    error_trace << std::endl << "    " << header.first << ": " << header.second;
                }
                error_trace << std::endl << "  Response Body (first 1000 chars): " << metadata_response->Content().substr(0, 1000);
                ERPL_TRACE_WARN("ODATA_CLIENT", error_trace.str());
            } else {
                ERPL_TRACE_WARN("ODATA_CLIENT", "Metadata request attempt " + std::to_string(4 - num_retries) + " failed: No response received");
            }

            // The OData v4 spec defines that the metadata document when given as a relative URL
            // can also be attachted to the root ULR of the service. However we don't have that
            // URL here. So what we can do is to try to pop the last segment of the URL path and 
            // try whether that is the service root URL.
            current_svc_url = current_svc_url.PopPath();
            metadata_request = HttpRequest(HttpMethod::GET, HttpUrl::MergeWithBaseUrlIfRelative(current_svc_url, metadata_url_raw));
            ERPL_TRACE_DEBUG("ODATA_CLIENT", "Retrying metadata request with popped URL: " + current_svc_url.ToString());
            
            if (auth_params != nullptr) {
                metadata_request.AuthHeadersFromParams(*auth_params);
            }   
        }

        // Trace final failure
        std::stringstream final_error;
        final_error << "All metadata request attempts failed. Final attempt details:" << std::endl;
        final_error << "  Final URL: " << HttpUrl::MergeWithBaseUrlIfRelative(url, metadata_url_raw).ToString() << std::endl;
        if (metadata_response != nullptr) {
            final_error << "  Final Status Code: " << metadata_response->Code() << std::endl;
            final_error << "  Final Content Type: " << metadata_response->ContentType() << std::endl;
            final_error << "  Final Response Body (first 1000 chars): " << metadata_response->Content().substr(0, 1000);
        } else {
            final_error << "  No response received on final attempt";
        }
        ERPL_TRACE_ERROR("ODATA_CLIENT", final_error.str());
        
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
