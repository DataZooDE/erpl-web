#include "odp_http_request_factory.hpp"
#include "tracing.hpp"
#include <sstream>

namespace erpl_web {

OdpHttpRequestFactory::OdpHttpRequestFactory(std::shared_ptr<HttpAuthParams> auth_params)
    : auth_params_(std::move(auth_params)) {
    ERPL_TRACE_DEBUG("ODP_HTTP_FACTORY", "Created ODP HTTP request factory");
}

HttpRequest OdpHttpRequestFactory::CreateInitialLoadRequest(const std::string& url, std::optional<uint32_t> max_page_size) {
    ERPL_TRACE_INFO("ODP_HTTP_FACTORY", "Creating initial load request for URL: " + url);
    
    OdpRequestConfig config;
    config.enable_change_tracking = true;  // Essential for ODP initial load
    config.max_page_size = max_page_size.value_or(default_page_size_);
    config.request_json = true;
    config.odata_version = ODataVersion::V2;
    
    return CreateRequest(HttpMethod::GET, url, config);
}

HttpRequest OdpHttpRequestFactory::CreateDeltaFetchRequest(const std::string& delta_url, std::optional<uint32_t> max_page_size) {
    ERPL_TRACE_INFO("ODP_HTTP_FACTORY", "Creating delta fetch request for URL: " + delta_url);
    
    OdpRequestConfig config;
    config.enable_change_tracking = false; // No Prefer header needed for delta fetch
    config.max_page_size = max_page_size.value_or(default_page_size_);
    config.request_json = true;
    config.odata_version = ODataVersion::V2;
    
    return CreateRequest(HttpMethod::GET, delta_url, config);
}

HttpRequest OdpHttpRequestFactory::CreateMetadataRequest(const std::string& metadata_url) {
    ERPL_TRACE_INFO("ODP_HTTP_FACTORY", "Creating metadata request for URL: " + metadata_url);
    
    OdpRequestConfig config;
    config.enable_change_tracking = false;
    config.request_json = false; // Metadata is XML
    config.odata_version = ODataVersion::V2;
    
    return CreateRequest(HttpMethod::GET, metadata_url, config);
}

HttpRequest OdpHttpRequestFactory::CreateTerminationRequest(const std::string& termination_url) {
    ERPL_TRACE_INFO("ODP_HTTP_FACTORY", "Creating termination request for URL: " + termination_url);
    
    OdpRequestConfig config;
    config.enable_change_tracking = false;
    config.request_json = true;
    config.odata_version = ODataVersion::V2;
    
    return CreateRequest(HttpMethod::GET, termination_url, config);
}

HttpRequest OdpHttpRequestFactory::CreateDeltaTokenDiscoveryRequest(const std::string& delta_links_url) {
    ERPL_TRACE_INFO("ODP_HTTP_FACTORY", "Creating delta token discovery request for URL: " + delta_links_url);
    
    OdpRequestConfig config;
    config.enable_change_tracking = false;
    config.request_json = true;
    config.odata_version = ODataVersion::V2;
    
    return CreateRequest(HttpMethod::GET, delta_links_url, config);
}

HttpRequest OdpHttpRequestFactory::CreateRequest(HttpMethod method, const std::string& url, const OdpRequestConfig& config) {
    ERPL_TRACE_DEBUG("ODP_HTTP_FACTORY", "Creating HTTP request with method: " + method.ToString() + ", URL: " + url);
    
    // Create base HTTP request
    HttpRequest request(method, url);
    
    // Apply ODP-specific headers
    ApplyOdpHeaders(request, config);
    
    // Apply authentication if available
    if (auth_params_) {
        request.AuthHeadersFromParams(*auth_params_);
        ERPL_TRACE_DEBUG("ODP_HTTP_FACTORY", "Applied authentication headers");
    }
    
    // Log final request configuration
    std::stringstream trace_msg;
    trace_msg << "Created ODP HTTP request:" << std::endl;
    trace_msg << "  Method: " << method.ToString() << std::endl;
    trace_msg << "  URL: " << url << std::endl;
    trace_msg << "  Headers:";
    for (const auto& header : request.headers) {
        trace_msg << std::endl << "    " << header.first << ": " << header.second;
    }
    ERPL_TRACE_DEBUG("ODP_HTTP_FACTORY", trace_msg.str());
    
    return request;
}

void OdpHttpRequestFactory::SetDefaultPageSize(uint32_t page_size) {
    default_page_size_ = page_size;
    ERPL_TRACE_INFO("ODP_HTTP_FACTORY", "Set default page size to: " + std::to_string(page_size));
}

uint32_t OdpHttpRequestFactory::GetDefaultPageSize() const {
    return default_page_size_;
}

void OdpHttpRequestFactory::ApplyOdpHeaders(HttpRequest& request, const OdpRequestConfig& config) {
    ERPL_TRACE_DEBUG("ODP_HTTP_FACTORY", "Applying ODP headers");
    
    // Apply OData version headers
    ApplyODataVersionHeaders(request, config.odata_version, config.request_json);
    
    // Apply Prefer headers for ODP operations
    ApplyPreferHeaders(request, config.enable_change_tracking, config.max_page_size);
    
    // Add $format=json to URL if requesting JSON and not already present
    if (config.request_json) {
        std::string url_str = request.url.ToString();
        if (url_str.find("$format=") == std::string::npos) {
            // Add $format=json parameter
            char separator = (url_str.find('?') != std::string::npos) ? '&' : '?';
            url_str += separator + std::string("$format=json");
            request.url = HttpUrl(url_str);
            ERPL_TRACE_DEBUG("ODP_HTTP_FACTORY", "Added $format=json to URL: " + url_str);
        }
    }
}

void OdpHttpRequestFactory::ApplyODataVersionHeaders(HttpRequest& request, ODataVersion version, bool request_json) {
    if (version == ODataVersion::V2) {
        // OData v2 headers as specified in ODP protocol
        request.headers["DataServiceVersion"] = "2.0";
        request.headers["MaxDataServiceVersion"] = "2.0";
        
        if (request_json) {
            // OData v2 JSON format with verbose metadata
            request.headers["Accept"] = "application/json;odata=verbose";
        } else {
            // XML format for metadata
            request.headers["Accept"] = "application/xml";
        }
        
        ERPL_TRACE_DEBUG("ODP_HTTP_FACTORY", "Applied OData v2 headers");
    } else {
        // OData v4 headers (fallback, though ODP typically uses v2)
        request.headers["OData-Version"] = "4.0";
        request.headers["OData-MaxVersion"] = "4.0";
        
        if (request_json) {
            request.headers["Accept"] = "application/json;odata.metadata=minimal";
        } else {
            request.headers["Accept"] = "application/xml";
        }
        
        ERPL_TRACE_DEBUG("ODP_HTTP_FACTORY", "Applied OData v4 headers");
    }
}

void OdpHttpRequestFactory::ApplyPreferHeaders(HttpRequest& request, bool enable_change_tracking, std::optional<uint32_t> max_page_size) {
    std::vector<std::string> prefer_values;
    
    // Add change tracking preference for initial load
    if (enable_change_tracking) {
        prefer_values.push_back("odata.track-changes");
        ERPL_TRACE_DEBUG("ODP_HTTP_FACTORY", "Added odata.track-changes preference");
    }
    
    // Add page size preference
    if (max_page_size.has_value()) {
        prefer_values.push_back("odata.maxpagesize=" + std::to_string(max_page_size.value()));
        ERPL_TRACE_DEBUG("ODP_HTTP_FACTORY", "Added odata.maxpagesize=" + std::to_string(max_page_size.value()) + " preference");
    }
    
    // Combine all preferences into single Prefer header
    if (!prefer_values.empty()) {
        std::string prefer_header;
        for (size_t i = 0; i < prefer_values.size(); ++i) {
            if (i > 0) {
                prefer_header += ", ";
            }
            prefer_header += prefer_values[i];
        }
        request.headers["Prefer"] = prefer_header;
        ERPL_TRACE_DEBUG("ODP_HTTP_FACTORY", "Applied Prefer header: " + prefer_header);
    }
}

} // namespace erpl_web
