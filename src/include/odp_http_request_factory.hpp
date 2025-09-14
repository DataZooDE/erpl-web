#pragma once

#include "http_client.hpp"
#include "odata_client.hpp"
#include <memory>
#include <string>
#include <optional>

namespace erpl_web {

/**
 * @brief Factory class for creating HTTP requests configured for ODP (Open Data Provisioning) operations.
 * 
 * This factory provides a layer of abstraction above the existing HttpRequest class,
 * composing it with ODP-specific headers and configuration without modifying the base HTTP functionality.
 * 
 * ODP requires specific HTTP headers for different operations:
 * - Initial load: Prefer: odata.track-changes
 * - Page size control: Prefer: odata.maxpagesize=<size>
 * - OData v2 version headers: DataServiceVersion: 2.0, MaxDataServiceVersion: 2.0
 * - Proper Accept headers for JSON vs XML content
 */
class OdpHttpRequestFactory {
public:
    /**
     * @brief Configuration for ODP HTTP requests
     */
    struct OdpRequestConfig {
        bool enable_change_tracking = false;      // Add Prefer: odata.track-changes header
        std::optional<uint32_t> max_page_size;    // Add Prefer: odata.maxpagesize=<size> header
        bool request_json = true;                 // Use JSON Accept header vs XML
        ODataVersion odata_version = ODataVersion::V2; // ODP typically uses OData v2
    };

    /**
     * @brief Construct factory with HTTP authentication parameters
     * @param auth_params Authentication parameters for HTTP requests
     */
    explicit OdpHttpRequestFactory(std::shared_ptr<HttpAuthParams> auth_params = nullptr);

    /**
     * @brief Create HTTP request for ODP initial load with change tracking
     * @param url Target URL for the request
     * @param max_page_size Optional page size limit
     * @return Configured HttpRequest for initial load
     */
    HttpRequest CreateInitialLoadRequest(const std::string& url, std::optional<uint32_t> max_page_size = std::nullopt);

    /**
     * @brief Create HTTP request for ODP delta fetch (incremental changes)
     * @param delta_url URL with delta token for incremental fetch
     * @param max_page_size Optional page size limit
     * @return Configured HttpRequest for delta fetch
     */
    HttpRequest CreateDeltaFetchRequest(const std::string& delta_url, std::optional<uint32_t> max_page_size = std::nullopt);

    /**
     * @brief Create HTTP request for ODP metadata retrieval
     * @param metadata_url URL for $metadata endpoint
     * @return Configured HttpRequest for metadata
     */
    HttpRequest CreateMetadataRequest(const std::string& metadata_url);

    /**
     * @brief Create HTTP request for ODP subscription termination
     * @param termination_url URL for termination function
     * @return Configured HttpRequest for subscription termination
     */
    HttpRequest CreateTerminationRequest(const std::string& termination_url);

    /**
     * @brief Create HTTP request for delta token discovery
     * @param delta_links_url URL for DeltaLinksOf<EntitySet> endpoint
     * @return Configured HttpRequest for delta token discovery
     */
    HttpRequest CreateDeltaTokenDiscoveryRequest(const std::string& delta_links_url);

    /**
     * @brief Create generic ODP request with custom configuration
     * @param method HTTP method
     * @param url Target URL
     * @param config ODP-specific configuration
     * @return Configured HttpRequest
     */
    HttpRequest CreateRequest(HttpMethod method, const std::string& url, const OdpRequestConfig& config);

    /**
     * @brief Set default page size for all requests created by this factory
     * @param page_size Default page size (15000 is Theobald default)
     */
    void SetDefaultPageSize(uint32_t page_size);

    /**
     * @brief Get current default page size
     * @return Default page size
     */
    uint32_t GetDefaultPageSize() const;

private:
    std::shared_ptr<HttpAuthParams> auth_params_;
    uint32_t default_page_size_ = 15000; // Theobald default from ODP spec

    /**
     * @brief Apply ODP-specific headers to an HttpRequest
     * @param request Request to modify
     * @param config Configuration specifying which headers to add
     */
    void ApplyOdpHeaders(HttpRequest& request, const OdpRequestConfig& config);

    /**
     * @brief Apply OData version-specific headers
     * @param request Request to modify
     * @param version OData version
     * @param request_json Whether to request JSON format
     */
    void ApplyODataVersionHeaders(HttpRequest& request, ODataVersion version, bool request_json);

    /**
     * @brief Apply Prefer headers for ODP operations
     * @param request Request to modify
     * @param enable_change_tracking Whether to add track-changes preference
     * @param max_page_size Optional page size preference
     */
    void ApplyPreferHeaders(HttpRequest& request, bool enable_change_tracking, std::optional<uint32_t> max_page_size);
};

} // namespace erpl_web
