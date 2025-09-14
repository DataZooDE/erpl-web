#include "odp_client_integration.hpp"
#include "tracing.hpp"

namespace erpl_web {

OdpClientIntegration::OdpClientIntegration(std::shared_ptr<HttpAuthParams> auth_params)
    : auth_params_(std::move(auth_params)) {
    
    // Create the ODP HTTP request factory
    request_factory_ = std::make_unique<OdpHttpRequestFactory>(auth_params_);
    
    // Create HTTP client for executing requests
    http_client_ = std::make_shared<HttpClient>();
    
    ERPL_TRACE_INFO("ODP_CLIENT_INTEGRATION", "Created ODP client integration");
}

std::shared_ptr<HttpClient> OdpClientIntegration::CreateHttpClient() {
    return http_client_;
}

std::unique_ptr<HttpResponse> OdpClientIntegration::ExecuteInitialLoad(const std::string& url, std::optional<uint32_t> max_page_size) {
    ERPL_TRACE_INFO("ODP_CLIENT_INTEGRATION", "Executing ODP initial load for URL: " + url);
    
    auto request = request_factory_->CreateInitialLoadRequest(url, max_page_size);
    return ExecuteRequest(request);
}

std::unique_ptr<HttpResponse> OdpClientIntegration::ExecuteDeltaFetch(const std::string& delta_url, std::optional<uint32_t> max_page_size) {
    ERPL_TRACE_INFO("ODP_CLIENT_INTEGRATION", "Executing ODP delta fetch for URL: " + delta_url);
    
    auto request = request_factory_->CreateDeltaFetchRequest(delta_url, max_page_size);
    return ExecuteRequest(request);
}

std::unique_ptr<HttpResponse> OdpClientIntegration::ExecuteMetadataRequest(const std::string& metadata_url) {
    ERPL_TRACE_INFO("ODP_CLIENT_INTEGRATION", "Executing ODP metadata request for URL: " + metadata_url);
    
    auto request = request_factory_->CreateMetadataRequest(metadata_url);
    return ExecuteRequest(request);
}

std::unique_ptr<HttpResponse> OdpClientIntegration::ExecuteTerminationRequest(const std::string& termination_url) {
    ERPL_TRACE_INFO("ODP_CLIENT_INTEGRATION", "Executing ODP termination request for URL: " + termination_url);
    
    auto request = request_factory_->CreateTerminationRequest(termination_url);
    return ExecuteRequest(request);
}

std::unique_ptr<HttpResponse> OdpClientIntegration::ExecuteDeltaTokenDiscovery(const std::string& delta_links_url) {
    ERPL_TRACE_INFO("ODP_CLIENT_INTEGRATION", "Executing ODP delta token discovery for URL: " + delta_links_url);
    
    auto request = request_factory_->CreateDeltaTokenDiscoveryRequest(delta_links_url);
    return ExecuteRequest(request);
}

OdpHttpRequestFactory& OdpClientIntegration::GetRequestFactory() {
    return *request_factory_;
}

void OdpClientIntegration::SetDefaultPageSize(uint32_t page_size) {
    request_factory_->SetDefaultPageSize(page_size);
    ERPL_TRACE_INFO("ODP_CLIENT_INTEGRATION", "Set default page size to: " + std::to_string(page_size));
}

std::unique_ptr<HttpResponse> OdpClientIntegration::ExecuteRequest(HttpRequest& request) {
    ERPL_TRACE_DEBUG("ODP_CLIENT_INTEGRATION", "Executing HTTP request: " + request.method.ToString() + " " + request.url.ToString());
    
    auto response = http_client_->SendRequest(request);
    
    if (!response) {
        throw std::runtime_error("Failed to execute HTTP request - no response received");
    }
    
    ERPL_TRACE_DEBUG("ODP_CLIENT_INTEGRATION", "HTTP response received with status: " + std::to_string(response->Code()));
    
    // Log response headers for debugging
    std::stringstream trace_msg;
    trace_msg << "HTTP Response Headers:";
    for (const auto& header : response->headers) {
        trace_msg << std::endl << "  " << header.first << ": " << header.second;
    }
    ERPL_TRACE_DEBUG("ODP_CLIENT_INTEGRATION", trace_msg.str());
    
    return response;
}

} // namespace erpl_web
