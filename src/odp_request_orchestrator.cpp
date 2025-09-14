#include "odp_request_orchestrator.hpp"
#include "tracing.hpp"
#include "yyjson.hpp"
#include <regex>
#include <sstream>

namespace erpl_web {

OdpRequestOrchestrator::OdpRequestOrchestrator(std::shared_ptr<HttpAuthParams> auth_params,
                                             uint32_t default_page_size)
    : http_factory_(std::make_unique<OdpHttpRequestFactory>(auth_params))
    , auth_params_(auth_params)
    , default_page_size_(default_page_size)
{
    ERPL_TRACE_INFO("ODP_ORCHESTRATOR", duckdb::StringUtil::Format(
        "Initializing request orchestrator with default page size: %u", default_page_size_));
    
    // Create HTTP client with OData-friendly settings
    HttpParams http_params;
    http_params.url_encode = false; // OData URLs should not be encoded
    http_client_ = std::make_shared<HttpClient>(http_params);
    
    // Set default page size on factory
    http_factory_->SetDefaultPageSize(default_page_size_);
}

OdpRequestOrchestrator::OdpRequestResult OdpRequestOrchestrator::ExecuteInitialLoad(
    const std::string& url, std::optional<uint32_t> max_page_size) {
    
    ERPL_TRACE_INFO("ODP_ORCHESTRATOR", "Executing initial load for URL: " + url);
    
    // Create initial load request with change tracking
    HttpRequest request = http_factory_->CreateInitialLoadRequest(url, max_page_size);
    
    return ExecuteRequest(request, "initial_load");
}

OdpRequestOrchestrator::OdpRequestResult OdpRequestOrchestrator::ExecuteDeltaFetch(
    const std::string& url, const std::string& delta_token, std::optional<uint32_t> max_page_size) {
    
    ERPL_TRACE_INFO("ODP_ORCHESTRATOR", duckdb::StringUtil::Format(
        "Executing delta fetch for URL: %s, Token: %s", url, delta_token.substr(0, 20) + "..."));
    
    // Build delta URL
    std::string delta_url = BuildDeltaUrl(url, delta_token);
    
    // Create delta fetch request
    HttpRequest request = http_factory_->CreateDeltaFetchRequest(delta_url, max_page_size);
    
    return ExecuteRequest(request, "delta_fetch");
}

OdpRequestOrchestrator::OdpRequestResult OdpRequestOrchestrator::ExecuteNextPage(const std::string& next_url) {
    ERPL_TRACE_INFO("ODP_ORCHESTRATOR", "Executing next page request for URL: " + next_url);
    
    // Create simple GET request for next page (no special ODP headers needed)
    HttpRequest request(HttpMethod::GET, next_url);
    
    // Apply authentication if available
    if (auth_params_) {
        request.AuthHeadersFromParams(*auth_params_);
    }
    
    return ExecuteRequest(request, "next_page");
}

bool OdpRequestOrchestrator::ValidatePreferenceApplied(const HttpResponse& response) {
    ERPL_TRACE_DEBUG("ODP_ORCHESTRATOR", "Validating preference-applied header");
    
    // Look for preference-applied header
    auto it = response.headers.find("preference-applied");
    if (it == response.headers.end()) {
        ERPL_TRACE_WARN("ODP_ORCHESTRATOR", "No preference-applied header found in response");
        return false;
    }
    
    std::string preference_header = it->second;
    ERPL_TRACE_DEBUG("ODP_ORCHESTRATOR", "Found preference-applied header: " + preference_header);
    
    // Check if it contains odata.track-changes
    bool contains_track_changes = preference_header.find("odata.track-changes") != std::string::npos;
    
    ERPL_TRACE_INFO("ODP_ORCHESTRATOR", duckdb::StringUtil::Format(
        "Preference validation result: %s", contains_track_changes ? "VALID" : "INVALID"));
    
    return contains_track_changes;
}

std::string OdpRequestOrchestrator::ExtractDeltaToken(const ODataEntitySetResponse& response) {
    ERPL_TRACE_DEBUG("ODP_ORCHESTRATOR", "Extracting delta token from response");
    
    std::string response_content = response.RawContent();
    ODataVersion version = response.GetODataVersion();
    
    std::string delta_token;
    if (version == ODataVersion::V2) {
        delta_token = ExtractDeltaTokenFromV2Response(response_content);
    } else {
        delta_token = ExtractDeltaTokenFromV4Response(response_content);
    }
    
    if (!delta_token.empty()) {
        ERPL_TRACE_INFO("ODP_ORCHESTRATOR", "Extracted delta token: " + delta_token.substr(0, 20) + "...");
    } else {
        ERPL_TRACE_WARN("ODP_ORCHESTRATOR", "No delta token found in response");
    }
    
    return delta_token;
}

std::string OdpRequestOrchestrator::BuildDeltaUrl(const std::string& base_url, const std::string& delta_token) {
    ERPL_TRACE_DEBUG("ODP_ORCHESTRATOR", duckdb::StringUtil::Format(
        "Building delta URL from base: %s, token: %s", base_url, delta_token.substr(0, 20) + "..."));
    
    // For OData v2, delta tokens are appended with !deltatoken= format
    std::string delta_url = base_url;
    
    // Remove any existing query parameters for delta URL
    size_t query_pos = delta_url.find('?');
    if (query_pos != std::string::npos) {
        delta_url = delta_url.substr(0, query_pos);
    }
    
    // Append delta token in OData v2 format
    delta_url += "!deltatoken=" + delta_token;
    
    // Ensure JSON format
    delta_url = EnsureJsonFormat(delta_url);
    
    ERPL_TRACE_DEBUG("ODP_ORCHESTRATOR", "Built delta URL: " + delta_url);
    return delta_url;
}

void OdpRequestOrchestrator::SetDefaultPageSize(uint32_t page_size) {
    default_page_size_ = page_size;
    http_factory_->SetDefaultPageSize(page_size);
    ERPL_TRACE_INFO("ODP_ORCHESTRATOR", "Updated default page size to: " + std::to_string(page_size));
}

uint32_t OdpRequestOrchestrator::GetDefaultPageSize() const {
    return default_page_size_;
}

// ============================================================================
// Private Helper Methods
// ============================================================================

OdpRequestOrchestrator::OdpRequestResult OdpRequestOrchestrator::ExecuteRequest(
    const HttpRequest& request, const std::string& operation_type) {
    
    LogRequestDetails(request, operation_type);
    
    OdpRequestResult result;
    
    try {
        // Execute HTTP request
        auto http_response = http_client_->SendRequest(const_cast<HttpRequest&>(request));
        
        result.http_status_code = http_response->Code();
        result.response_size_bytes = http_response->Content().size();
        
        ERPL_TRACE_DEBUG("ODP_ORCHESTRATOR", duckdb::StringUtil::Format(
            "HTTP request completed - Status: %d, Size: %zu bytes", 
            result.http_status_code, result.response_size_bytes));
        
        // Check for HTTP errors
        if (result.http_status_code >= 400) {
            throw duckdb::IOException("ODP request failed with HTTP " + std::to_string(result.http_status_code) + ": " + http_response->content);
        }
        
        // Process OData response
        result.response = ProcessHttpResponse(std::move(http_response));
        
        // Extract delta token if present
        result.extracted_delta_token = ExtractDeltaToken(*result.response);
        
        // Check for next page
        auto next_url = result.response->NextUrl();
        result.has_more_pages = next_url.has_value() && !next_url->empty();
        
        // Validate preference applied for initial load
        if (operation_type == "initial_load") {
            // We need access to the original HTTP response for header validation
            // For now, we'll assume preference was applied if we got a delta token
            result.preference_applied = !result.extracted_delta_token.empty();
        }
        
        LogResponseDetails(result, operation_type);
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_ORCHESTRATOR", duckdb::StringUtil::Format(
            "Request failed for operation %s: %s", operation_type, e.what()));
        throw;
    }
    
    return result;
}

std::shared_ptr<ODataEntitySetResponse> OdpRequestOrchestrator::ProcessHttpResponse(
    std::unique_ptr<HttpResponse> http_response) {
    
    ERPL_TRACE_DEBUG("ODP_ORCHESTRATOR", "Processing HTTP response to OData response");
    
    // Determine OData version from response
    ODataVersion version = ODataVersion::V2; // ODP typically uses V2
    
    // Create OData response wrapper
    auto odata_response = std::make_shared<ODataEntitySetResponse>(std::move(http_response), version);
    
    return odata_response;
}

void OdpRequestOrchestrator::LogRequestDetails(const HttpRequest& request, const std::string& operation_type) const {
    std::stringstream log_msg;
    log_msg << "Executing " << operation_type << " request:" << std::endl;
    log_msg << "  Method: " << request.method.ToString() << std::endl;
    log_msg << "  URL: " << request.url.ToString() << std::endl;
    log_msg << "  Headers:";
    
    for (const auto& header : request.headers) {
        log_msg << std::endl << "    " << header.first << ": " << header.second;
    }
    
    ERPL_TRACE_DEBUG("ODP_ORCHESTRATOR", log_msg.str());
}

void OdpRequestOrchestrator::LogResponseDetails(const OdpRequestResult& result, const std::string& operation_type) const {
    std::stringstream log_msg;
    log_msg << "Completed " << operation_type << " request:" << std::endl;
    log_msg << "  HTTP Status: " << result.http_status_code << std::endl;
    log_msg << "  Response Size: " << result.response_size_bytes << " bytes" << std::endl;
    log_msg << "  Delta Token: " << (result.extracted_delta_token.empty() ? "NONE" : result.extracted_delta_token.substr(0, 20) + "...") << std::endl;
    log_msg << "  Has More Pages: " << (result.has_more_pages ? "YES" : "NO") << std::endl;
    log_msg << "  Preference Applied: " << (result.preference_applied ? "YES" : "NO");
    
    ERPL_TRACE_INFO("ODP_ORCHESTRATOR", log_msg.str());
}

std::string OdpRequestOrchestrator::ExtractDeltaTokenFromV2Response(const std::string& response_content) {
    ERPL_TRACE_DEBUG("ODP_ORCHESTRATOR", "Extracting delta token from OData v2 response");
    
    try {
        // Parse JSON response
        auto doc = duckdb_yyjson::yyjson_read(response_content.c_str(), response_content.length(), 0);
        if (!doc) {
            ERPL_TRACE_WARN("ODP_ORCHESTRATOR", "Failed to parse JSON response");
            return "";
        }
        
        auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
        if (!root) {
            duckdb_yyjson::yyjson_doc_free(doc);
            return "";
        }
        
        // Look for __delta property in OData v2 format
        auto d_obj = duckdb_yyjson::yyjson_obj_get(root, "d");
        if (d_obj) {
            auto delta_val = duckdb_yyjson::yyjson_obj_get(d_obj, "__delta");
            if (delta_val && duckdb_yyjson::yyjson_is_str(delta_val)) {
                std::string delta_url = duckdb_yyjson::yyjson_get_str(delta_val);
                duckdb_yyjson::yyjson_doc_free(doc);
                
                // Extract token from delta URL
                return ExtractTokenFromDeltaUrl(delta_url);
            }
        }
        
        duckdb_yyjson::yyjson_doc_free(doc);
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_ORCHESTRATOR", "Error parsing v2 response: " + std::string(e.what()));
    }
    
    return "";
}

std::string OdpRequestOrchestrator::ExtractDeltaTokenFromV4Response(const std::string& response_content) {
    ERPL_TRACE_DEBUG("ODP_ORCHESTRATOR", "Extracting delta token from OData v4 response");
    
    try {
        // Parse JSON response
        auto doc = duckdb_yyjson::yyjson_read(response_content.c_str(), response_content.length(), 0);
        if (!doc) {
            ERPL_TRACE_WARN("ODP_ORCHESTRATOR", "Failed to parse JSON response");
            return "";
        }
        
        auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
        if (!root) {
            duckdb_yyjson::yyjson_doc_free(doc);
            return "";
        }
        
        // Look for @odata.deltaLink property in OData v4 format
        auto delta_val = duckdb_yyjson::yyjson_obj_get(root, "@odata.deltaLink");
        if (delta_val && duckdb_yyjson::yyjson_is_str(delta_val)) {
            std::string delta_url = duckdb_yyjson::yyjson_get_str(delta_val);
            duckdb_yyjson::yyjson_doc_free(doc);
            
            // Extract token from delta URL
            return ExtractTokenFromDeltaUrl(delta_url);
        }
        
        duckdb_yyjson::yyjson_doc_free(doc);
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_ORCHESTRATOR", "Error parsing v4 response: " + std::string(e.what()));
    }
    
    return "";
}

std::string OdpRequestOrchestrator::ExtractTokenFromDeltaUrl(const std::string& delta_url) {
    ERPL_TRACE_DEBUG("ODP_ORCHESTRATOR", "Extracting token from delta URL: " + delta_url);
    
    // Look for !deltatoken= pattern (OData v2)
    std::regex v2_pattern(R"(!deltatoken=([^&]+))");
    std::smatch match;
    
    if (std::regex_search(delta_url, match, v2_pattern)) {
        std::string token = match[1].str();
        ERPL_TRACE_DEBUG("ODP_ORCHESTRATOR", "Extracted v2 delta token: " + token.substr(0, 20) + "...");
        return token;
    }
    
    // Look for $deltatoken= pattern (OData v4)
    std::regex v4_pattern(R"(\$deltatoken=([^&]+))");
    if (std::regex_search(delta_url, match, v4_pattern)) {
        std::string token = match[1].str();
        ERPL_TRACE_DEBUG("ODP_ORCHESTRATOR", "Extracted v4 delta token: " + token.substr(0, 20) + "...");
        return token;
    }
    
    ERPL_TRACE_WARN("ODP_ORCHESTRATOR", "No delta token pattern found in URL");
    return "";
}

std::string OdpRequestOrchestrator::EnsureJsonFormat(const std::string& url) {
    if (HasJsonFormat(url)) {
        return url;
    }
    
    // Add $format=json parameter
    char separator = (url.find('?') != std::string::npos) ? '&' : '?';
    return url + separator + "$format=json";
}

bool OdpRequestOrchestrator::HasJsonFormat(const std::string& url) {
    return url.find("$format=json") != std::string::npos;
}

} // namespace erpl_web
