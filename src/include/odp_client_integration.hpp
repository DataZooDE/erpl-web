#pragma once

#include "odp_http_request_factory.hpp"
#include "odata_client.hpp"
#include "http_client.hpp"
#include <memory>
#include <string>

namespace erpl_web {

/**
 * @brief Integration helper for using ODP HTTP request factory with existing OData client infrastructure.
 * 
 * This class demonstrates how to integrate the ODP HTTP request factory with the existing
 * OData client architecture without modifying the base HttpClient or ODataClient classes.
 * It provides a clean abstraction layer for ODP-specific operations.
 */
class OdpClientIntegration {
public:
    /**
     * @brief Construct ODP client integration with authentication
     * @param auth_params Authentication parameters for HTTP requests
     */
    explicit OdpClientIntegration(std::shared_ptr<HttpAuthParams> auth_params = nullptr);

    /**
     * @brief Create an HTTP client configured for ODP operations
     * @return Shared pointer to HTTP client
     */
    std::shared_ptr<HttpClient> CreateHttpClient();

    /**
     * @brief Execute an ODP initial load request and return the response
     * @param url OData entity set URL
     * @param max_page_size Optional page size limit
     * @return HTTP response from the initial load request
     */
    std::unique_ptr<HttpResponse> ExecuteInitialLoad(const std::string& url, std::optional<uint32_t> max_page_size = std::nullopt);

    /**
     * @brief Execute an ODP delta fetch request and return the response
     * @param delta_url URL with delta token
     * @param max_page_size Optional page size limit
     * @return HTTP response from the delta fetch request
     */
    std::unique_ptr<HttpResponse> ExecuteDeltaFetch(const std::string& delta_url, std::optional<uint32_t> max_page_size = std::nullopt);

    /**
     * @brief Execute an ODP metadata request and return the response
     * @param metadata_url Metadata endpoint URL
     * @return HTTP response containing metadata XML
     */
    std::unique_ptr<HttpResponse> ExecuteMetadataRequest(const std::string& metadata_url);

    /**
     * @brief Execute an ODP subscription termination request
     * @param termination_url Termination function URL
     * @return HTTP response from termination request
     */
    std::unique_ptr<HttpResponse> ExecuteTerminationRequest(const std::string& termination_url);

    /**
     * @brief Execute an ODP delta token discovery request
     * @param delta_links_url DeltaLinksOf<EntitySet> URL
     * @return HTTP response containing available delta tokens
     */
    std::unique_ptr<HttpResponse> ExecuteDeltaTokenDiscovery(const std::string& delta_links_url);

    /**
     * @brief Get the ODP HTTP request factory instance
     * @return Reference to the factory
     */
    OdpHttpRequestFactory& GetRequestFactory();

    /**
     * @brief Set default page size for all ODP requests
     * @param page_size Default page size
     */
    void SetDefaultPageSize(uint32_t page_size);

private:
    std::shared_ptr<HttpAuthParams> auth_params_;
    std::unique_ptr<OdpHttpRequestFactory> request_factory_;
    std::shared_ptr<HttpClient> http_client_;

    /**
     * @brief Execute a generic HTTP request using the configured client
     * @param request HTTP request to execute
     * @return HTTP response
     */
    std::unique_ptr<HttpResponse> ExecuteRequest(HttpRequest& request);
};

} // namespace erpl_web
