#pragma once
#include "cpptrace/cpptrace.hpp"
#include "yyjson.hpp"
#include <type_traits>

#include "http_client.hpp"
#include "odata_edm.hpp"
#include "odata_content.hpp"
#include "tracing.hpp"

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
    
    // Expose raw response content for downstream processing (e.g., expand extraction)
    std::string RawContent() const { return http_response->Content(); }
    
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
    
    // Set metadata context URL directly (for Datasphere dual-URL pattern)
    void SetMetadataContextUrl(const std::string& context_url) { metadata_context_url = context_url; }
    
    // Set OData version directly to skip metadata fetching
    void SetODataVersionDirectly(ODataVersion version) { odata_version = version; }

    virtual Edmx GetMetadata()
    {    
        // Always resolve metadata; for Datasphere parameterized reads, use @odata.context (without fragment)
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
    
    // Virtual method to add input parameters to URL (default implementation does nothing)
    virtual HttpUrl AddInputParametersToUrl(const HttpUrl& url) const { return url; }
    
    // Virtual method to check if input parameters are present (default implementation returns false)
    virtual bool HasInputParameters() const { return false; }

    std::string Url() const { return url.ToString(); }
    std::shared_ptr<HttpClient> GetHttpClient() const { return http_client->GetHttpClient(); }
    std::shared_ptr<HttpAuthParams> AuthParams() const { return auth_params; }
protected:
    std::shared_ptr<CachingHttpClient> http_client;
    HttpUrl url;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::shared_ptr<TResponse> current_response;
    ODataVersion odata_version;
    std::string metadata_context_url; // For Datasphere dual-URL pattern

    std::unique_ptr<HttpResponse> DoHttpGet(const HttpUrl& url) {
        // Create a copy of the URL to modify with input parameters
        HttpUrl modified_url = url;
        
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "DoHttpGet: calling AddInputParametersToUrl");
        
        // Add input parameters to the URL if they exist (virtual method call)
        modified_url = AddInputParametersToUrl(modified_url);
        
        // If OData v4, append $count=true (lenient) unless already present
        if (odata_version == ODataVersion::V4) {
            auto q = modified_url.Query();
            const bool has_q = !q.empty();
            // Avoid duplicate count when nextLink already contains it (may be URL-encoded as %24count=)
            const bool has_count_plain = (q.find("$count=") != std::string::npos);
            const bool has_count_encoded = (q.find("%24count=") != std::string::npos);
            if (!has_count_plain && !has_count_encoded) {
                std::string sep = has_q ? "&" : "?";
                modified_url.Query(q + sep + "$count=true");
                ERPL_TRACE_DEBUG("ODATA_CLIENT", "Appended $count=true to URL for progress: " + modified_url.ToString());
            } else {
                ERPL_TRACE_DEBUG("ODATA_CLIENT", "Skipping $count append; query already has count parameter: " + q);
            }
        }

        ERPL_TRACE_DEBUG("ODATA_CLIENT", "DoHttpGet: URL after input parameters: " + modified_url.ToString());
        
        auto http_request = HttpRequest(HttpMethod::GET, modified_url);
        
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
        // Sanitize: strip any query from a $metadata URL (e.g., remove "$format=json")
        std::string sanitized_raw = metadata_url_raw;
        {
            auto meta_pos = sanitized_raw.find("/$metadata");
            if (meta_pos != std::string::npos) {
                auto qpos = sanitized_raw.find('?', meta_pos);
                if (qpos != std::string::npos) {
                    sanitized_raw = sanitized_raw.substr(0, qpos);
                }
            }
        }

        //std::cout << "Fetching Metadata from: " << sanitized_raw << std::endl;
        auto current_svc_url = url;
        auto metadata_request = HttpRequest(HttpMethod::GET, HttpUrl::MergeWithBaseUrlIfRelative(current_svc_url, sanitized_raw));

        // For metadata requests, do NOT add version-specific headers.
        // Some SAP gateways respond 400 to metadata when OData-Version headers are present.

        // Ensure metadata Accept is XML and request is not kept-alive (ICM 400s observed otherwise)
        metadata_request.headers["Accept"] = "application/xml";
        metadata_request.headers["Connection"] = "close";
        
        if (auth_params != nullptr) {
            metadata_request.AuthHeadersFromParams(*auth_params);
        }
        
        // Trace metadata request details
        std::stringstream request_trace;
        request_trace << "OData Metadata HTTP Request:" << std::endl;
        request_trace << "  URL: " << HttpUrl::MergeWithBaseUrlIfRelative(current_svc_url, sanitized_raw).ToString() << std::endl;
        request_trace << "  Method: GET" << std::endl;
        request_trace << "  Headers:";
        for (const auto& header : metadata_request.headers) {
            request_trace << std::endl << "    " << header.first << ": " << header.second;
        }
        ERPL_TRACE_DEBUG("ODATA_CLIENT", request_trace.str());
        
        std::unique_ptr<HttpResponse> metadata_response;
        for (size_t num_retries = 3; num_retries > 0; --num_retries) {
            ERPL_TRACE_DEBUG("ODATA_CLIENT", "Metadata request attempt " + std::to_string(4 - num_retries) + " of 3");
            // Use a fresh non-cached HTTP client for metadata to mirror http_get behavior exactly
            HttpParams meta_params;
            meta_params.url_encode = false;
            meta_params.keep_alive = false;
            HttpClient meta_client(meta_params);
            metadata_response = meta_client.SendRequest(metadata_request);
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
                    response_trace << std::endl << "  Response Body Preview (first 4000 chars): " << metadata_response->Content().substr(0, 4000);
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
                error_trace << std::endl << "  Response Body (first 4000 chars): " << metadata_response->Content().substr(0, 4000);
                ERPL_TRACE_WARN("ODATA_CLIENT", error_trace.str());
            } else {
                ERPL_TRACE_WARN("ODATA_CLIENT", "Metadata request attempt " + std::to_string(4 - num_retries) + " failed: No response received");
            }

            // Pop one level and retry toward service-root $metadata
            current_svc_url = current_svc_url.PopPath();
            metadata_request = HttpRequest(HttpMethod::GET, HttpUrl::MergeWithBaseUrlIfRelative(current_svc_url, sanitized_raw));
            // Re-apply essential headers on each retry
            metadata_request.headers["Accept"] = "application/xml";
            metadata_request.headers["Connection"] = "close";
            ERPL_TRACE_DEBUG("ODATA_CLIENT", "Retrying metadata request with popped URL: " + current_svc_url.ToString());
            
            if (auth_params != nullptr) {
                metadata_request.AuthHeadersFromParams(*auth_params);
            }   
        }

        // Trace final failure
        std::stringstream final_error;
        final_error << "All metadata request attempts failed. Final attempt details:" << std::endl;
        final_error << "  Final URL: " << HttpUrl::MergeWithBaseUrlIfRelative(url, sanitized_raw).ToString() << std::endl;
        if (metadata_response != nullptr) {
            final_error << "  Final Status Code: " << metadata_response->Code() << std::endl;
            final_error << "  Final Content Type: " << metadata_response->ContentType() << std::endl;
            final_error << "  Final Response Body (first 4000 chars): " << metadata_response->Content().substr(0, 4000);
        } else {
            final_error << "  No response received on final attempt";
        }
        ERPL_TRACE_ERROR("ODATA_CLIENT", final_error.str());
        
        std::stringstream ss;
        ss << "Failed to get OData metadata from "
           << HttpUrl::MergeWithBaseUrlIfRelative(url, sanitized_raw).ToString();
        if (metadata_response != nullptr) {
            ss << " (HTTP " << metadata_response->Code() << ")";
            auto body = metadata_response->Content();
            if (!body.empty()) {
                ss << "\nResponse: " << body.substr(0, 4000);
            }
        } else {
            ss << ": no response received";
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
    
    // For Datasphere input parameters: set input parameters that will be included in requests
    void SetInputParameters(const std::map<std::string, std::string>& input_params);
    
    // Override to add input parameters to URL
    HttpUrl AddInputParametersToUrl(const HttpUrl& url) const override;
    
    // Override to check if input parameters are present
    bool HasInputParameters() const override { return !input_parameters.empty(); }

    // Explicitly set the current entity set name from an @odata.context value or fragment
    void SetEntitySetNameFromContextFragment(const std::string &context_or_fragment);
    // Explicitly set the current entity set name directly
    void SetEntitySetName(const std::string &entity_name) { current_entity_name_from_fragment = entity_name; }

    // Public access to entity type information for navigation property filtering
    EntityType GetCurrentEntityType();

private:
    EntitySet GetCurrentEntitySetType();
    
    // For Datasphere input parameters: storage for input parameters
    std::map<std::string, std::string> input_parameters;
    
    // For Datasphere input parameters: store entity name extracted from metadata context fragment
    std::string current_entity_name_from_fragment;
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
