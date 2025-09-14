#pragma once

#include "odata_read_functions.hpp"
#include "odp_subscription_state_manager.hpp"
#include "odp_request_orchestrator.hpp"
#include "http_client.hpp"
#include "tracing.hpp"
#include <memory>
#include <string>

namespace erpl_web {

/**
 * @brief ODP-aware bind data class that extends OData functionality with delta replication
 * 
 * This class uses composition to delegate core OData operations to ODataReadBindData
 * while adding ODP-specific functionality for subscription management and delta token handling.
 * It coordinates between the state manager and request orchestrator to provide seamless
 * ODP delta replication capabilities.
 */
class OdpODataReadBindData : public duckdb::TableFunctionData {
public:
    /**
     * @brief Construct ODP bind data with subscription parameters
     * @param context DuckDB client context
     * @param entity_set_url Full ODP entity set URL
     * @param secret_name Secret name for authentication (default: "default")
     * @param force_full_load Force initial load even if subscription exists
     * @param import_delta_token Import existing delta token
     * @param max_page_size Optional page size override
     */
    OdpODataReadBindData(duckdb::ClientContext& context,
                        const std::string& entity_set_url,
                        const std::string& secret_name = "",
                        bool force_full_load = false,
                        const std::string& import_delta_token = "",
                        std::optional<uint32_t> max_page_size = std::nullopt);

    ~OdpODataReadBindData() = default;

    // Non-copyable, non-movable
    OdpODataReadBindData(const OdpODataReadBindData&) = delete;
    OdpODataReadBindData& operator=(const OdpODataReadBindData&) = delete;
    OdpODataReadBindData(OdpODataReadBindData&&) = delete;
    OdpODataReadBindData& operator=(OdpODataReadBindData&&) = delete;

    // ========================================================================
    // Core DuckDB Table Function Interface (delegated to ODataReadBindData)
    // ========================================================================

    /**
     * @brief Get result column names
     * @param all_columns Include all columns or just selected ones
     * @return Vector of column names
     */
    std::vector<std::string> GetResultNames(bool all_columns = false);

    /**
     * @brief Get result column types
     * @param all_columns Include all columns or just selected ones
     * @return Vector of logical types
     */
    std::vector<duckdb::LogicalType> GetResultTypes(bool all_columns = false);

    /**
     * @brief Check if more results are available
     * @return True if more data can be fetched
     */
    bool HasMoreResults();

    /**
     * @brief Fetch next batch of results
     * @param output Output data chunk to fill
     * @return Number of rows fetched
     */
    unsigned int FetchNextResult(duckdb::DataChunk &output);

    /**
     * @brief Activate specific columns for projection pushdown
     * @param column_ids Column indices to activate
     */
    void ActivateColumns(const std::vector<duckdb::column_t> &column_ids);

    /**
     * @brief Add filters for predicate pushdown
     * @param filters Table filter set
     */
    void AddFilters(const duckdb::optional_ptr<duckdb::TableFilterSet> &filters);

    /**
     * @brief Add result modifiers (ORDER BY, LIMIT, etc.)
     * @param modifiers Result modifier list
     */
    void AddResultModifiers(const std::vector<duckdb::unique_ptr<duckdb::BoundResultModifier>> &modifiers);

    /**
     * @brief Get progress fraction for query execution
     * @return Progress value between 0.0 and 1.0
     */
    double GetProgressFraction() const;

    // ========================================================================
    // ODP-Specific Interface
    // ========================================================================

    /**
     * @brief Get subscription ID
     * @return Current subscription ID
     */
    std::string GetSubscriptionId() const;

    /**
     * @brief Get current delta token
     * @return Current delta token or empty string
     */
    std::string GetCurrentDeltaToken() const;

    /**
     * @brief Check if subscription is active
     * @return True if subscription is active and functional
     */
    bool IsSubscriptionActive() const;

    /**
     * @brief Get current subscription phase
     * @return Current phase (INITIAL_LOAD, DELTA_FETCH, etc.)
     */
    OdpSubscriptionStateManager::SubscriptionPhase GetCurrentPhase() const;

    /**
     * @brief Force transition to initial load phase
     * This will clear the delta token and restart from full load
     */
    void ForceInitialLoad();

    /**
     * @brief Terminate the subscription
     * This will mark the subscription as terminated and clean up resources
     */
    void TerminateSubscription();

    /**
     * @brief Get audit history for this subscription
     * @param days_back Number of days to look back (default: 30)
     * @return Vector of audit entries
     */
    std::vector<OdpAuditEntry> GetAuditHistory(int days_back = 30) const;
    
    /**
     * @brief Get the underlying OData bind data for delegation
     * @return Reference to the composed ODataReadBindData
     */
    ODataReadBindData& GetODataBindData();
    const ODataReadBindData& GetODataBindData() const;
    
    /**
     * @brief Process scan results for ODP-specific logic
     * 
     * Called after each successful scan to handle delta token updates,
     * audit logging, and subscription state management.
     * 
     * @param output The data chunk that was just scanned
     */
    void ProcessScanResult(const duckdb::DataChunk& output);

    /**
     * @brief Initialize all components and establish subscription
     */
    void Initialize();

private:
    // ========================================================================
    // Core Components (Composition Pattern)
    // ========================================================================

    // Delegate to existing OData functionality
    std::unique_ptr<ODataReadBindData> odata_bind_data_;
    
    // ODP-specific components
    std::unique_ptr<OdpSubscriptionStateManager> state_manager_;
    std::unique_ptr<OdpRequestOrchestrator> request_orchestrator_;
    
    // Configuration
    duckdb::ClientContext& context_;
    std::string entity_set_url_;
    std::string secret_name_;
    std::shared_ptr<HttpAuthParams> auth_params_;
    std::optional<uint32_t> max_page_size_;
    
    // State tracking
    bool initialized_;
    bool first_fetch_completed_;
    int64_t current_audit_id_;

    // ========================================================================
    // Initialization and Setup
    // ========================================================================

    /**
     * @brief Setup authentication parameters from secret
     */
    void SetupAuthentication();

    /**
     * @brief Create and configure the underlying ODataReadBindData
     */
    void CreateODataBindData();

    /**
     * @brief Validate the entity set URL for ODP compatibility
     */
    void ValidateEntitySetUrl() const;

    // ========================================================================
    // ODP Request Handling
    // ========================================================================

    /**
     * @brief Handle initial load request with change tracking
     * @return True if successful, false otherwise
     */
    bool HandleInitialLoad();

    /**
     * @brief Handle delta fetch request using current token
     * @return True if successful, false otherwise
     */
    bool HandleDeltaFetch();

    /**
     * @brief Process request result and update subscription state
     * @param result Request result from orchestrator
     * @param operation_type Type of operation performed
     */
    void ProcessRequestResult(const OdpRequestOrchestrator::OdpRequestResult& result,
                             const std::string& operation_type);

    /**
     * @brief Update the underlying OData client with new URL/token
     * @param url New URL to use for requests
     */
    void UpdateODataClient(const std::string& url);

    // ========================================================================
    // Error Handling and Recovery
    // ========================================================================

    /**
     * @brief Handle request errors and determine recovery strategy
     * @param error Exception that occurred
     * @param operation_type Type of operation that failed
     */
    void HandleRequestError(const std::exception& error, const std::string& operation_type);

    /**
     * @brief Check if error indicates expired/invalid delta token
     * @param error Exception to analyze
     * @return True if token-related error
     */
    bool IsTokenError(const std::exception& error) const;

    // ========================================================================
    // Utility Methods
    // ========================================================================

    /**
     * @brief Log current state for debugging
     */
    void LogCurrentState() const;

    /**
     * @brief Extract entity set name from URL
     * @param url Full entity set URL
     * @return Entity set name
     */
    static std::string ExtractEntitySetName(const std::string& url);
};

} // namespace erpl_web
