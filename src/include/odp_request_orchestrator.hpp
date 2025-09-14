#pragma once

#include "odp_http_request_factory.hpp"
#include "odata_client.hpp"
#include "http_client.hpp"
#include "tracing.hpp"
#include <memory>
#include <string>
#include <optional>

namespace erpl_web {

/**
 * @brief Orchestrates ODP-specific HTTP requests and response processing
 * 
 * This class coordinates between the ODP HTTP factory, OData client, and response
 * processing to handle the complexities of ODP initial load and delta fetch operations.
 * It manages preference validation, delta token extraction, and error handling.
 */
class OdpRequestOrchestrator {
public:
    /**
     * @brief Result of an ODP request operation
     */
    struct OdpRequestResult {
        std::shared_ptr<ODataEntitySetResponse> response;
        std::string extracted_delta_token;
        bool preference_applied;
        bool has_more_pages;
        int http_status_code;
        size_t response_size_bytes;
        
        OdpRequestResult() 
            : preference_applied(false)
            , has_more_pages(false)
            , http_status_code(0)
            , response_size_bytes(0) 
        {}
    };

    /**
     * @brief Construct orchestrator with authentication parameters
     * @param auth_params HTTP authentication parameters
     * @param default_page_size Default page size for ODP requests
     */
    explicit OdpRequestOrchestrator(std::shared_ptr<HttpAuthParams> auth_params = nullptr,
                                   uint32_t default_page_size = 15000);

    ~OdpRequestOrchestrator() = default;

    // Non-copyable, non-movable
    OdpRequestOrchestrator(const OdpRequestOrchestrator&) = delete;
    OdpRequestOrchestrator& operator=(const OdpRequestOrchestrator&) = delete;
    OdpRequestOrchestrator(OdpRequestOrchestrator&&) = delete;
    OdpRequestOrchestrator& operator=(OdpRequestOrchestrator&&) = delete;

    /**
     * @brief Execute initial load request with change tracking
     * @param url Target entity set URL
     * @param max_page_size Optional page size override
     * @return Result containing response and extracted delta token
     */
    OdpRequestResult ExecuteInitialLoad(const std::string& url, 
                                       std::optional<uint32_t> max_page_size = std::nullopt);

    /**
     * @brief Execute delta fetch request using existing delta token
     * @param url Base entity set URL (delta token will be appended)
     * @param delta_token Delta token from previous request
     * @param max_page_size Optional page size override
     * @return Result containing response and new delta token
     */
    OdpRequestResult ExecuteDeltaFetch(const std::string& url, 
                                      const std::string& delta_token,
                                      std::optional<uint32_t> max_page_size = std::nullopt);

    /**
     * @brief Execute next page request (for pagination)
     * @param next_url Next page URL from previous response
     * @return Result containing response data
     */
    OdpRequestResult ExecuteNextPage(const std::string& next_url);


    /**
     * @brief Extract delta token from OData response
     * @param response OData entity set response
     * @return Extracted delta token or empty string if not found
     */
    static std::string ExtractDeltaToken(const ODataEntitySetResponse& response);

    /**
     * @brief Build delta URL by appending delta token to base URL
     * @param base_url Base entity set URL
     * @param delta_token Delta token to append
     * @return Complete delta URL
     */
    static std::string BuildDeltaUrl(const std::string& base_url, const std::string& delta_token);

    /**
     * @brief Set default page size for requests
     * @param page_size Default page size
     */
    void SetDefaultPageSize(uint32_t page_size);

    /**
     * @brief Get current default page size
     * @return Default page size
     */
    uint32_t GetDefaultPageSize() const;

private:
    // Core components
    std::unique_ptr<OdpHttpRequestFactory> http_factory_;
    std::shared_ptr<HttpClient> http_client_;
    std::shared_ptr<HttpAuthParams> auth_params_;
    uint32_t default_page_size_;

    // Helper methods
    OdpRequestResult ExecuteRequest(const HttpRequest& request, const std::string& operation_type);
    std::shared_ptr<ODataEntitySetResponse> ProcessHttpResponse(std::unique_ptr<HttpResponse> http_response);
    void LogRequestDetails(const HttpRequest& request, const std::string& operation_type) const;
    void LogResponseDetails(const OdpRequestResult& result, const std::string& operation_type) const;
    
public:
    // Static utility methods (public for testing)
    static std::string ExtractDeltaTokenFromV2Response(const std::string& response_content);
    static std::string ExtractDeltaTokenFromV4Response(const std::string& response_content);
    static std::string ExtractTokenFromDeltaUrl(const std::string& delta_url);
    static std::string EnsureJsonFormat(const std::string& url);
    static bool HasJsonFormat(const std::string& url);
    static bool ValidatePreferenceApplied(const HttpResponse& response);
};

} // namespace erpl_web
