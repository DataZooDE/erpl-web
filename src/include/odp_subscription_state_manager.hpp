#pragma once

#include "odp_subscription_repository.hpp"
#include "tracing.hpp"
#include <memory>
#include <string>
#include <chrono>

namespace erpl_web {

/**
 * @brief Manages the lifecycle and state transitions of ODP subscriptions
 * 
 * This class encapsulates all subscription state logic and coordinates with the
 * OdpSubscriptionRepository for persistence. It handles state transitions between
 * initial load, delta fetch, and error states while maintaining audit trails.
 * 
 * Thread safety is handled via DuckDB database isolation rather than explicit locking.
 */
class OdpSubscriptionStateManager {
public:
    /**
     * @brief Subscription lifecycle phases
     */
    enum class SubscriptionPhase {
        INITIAL_LOAD,      // First load with change tracking
        DELTA_FETCH,       // Incremental loads with delta tokens
        TERMINATED,        // Subscription ended
        ERROR_STATE        // Recovery needed
    };

    /**
     * @brief Construct state manager for a specific subscription
     * @param context DuckDB client context for database operations
     * @param service_url Full ODP service URL
     * @param entity_set_name Entity set name
     * @param secret_name Secret name for authentication
     * @param force_full_load Force initial load even if subscription exists
     * @param import_delta_token Import existing delta token
     */
    OdpSubscriptionStateManager(duckdb::ClientContext& context,
                               const std::string& service_url,
                               const std::string& entity_set_name,
                               const std::string& secret_name = "",
                               bool force_full_load = false,
                               const std::string& import_delta_token = "");

    ~OdpSubscriptionStateManager() = default;

    // Non-copyable, non-movable
    OdpSubscriptionStateManager(const OdpSubscriptionStateManager&) = delete;
    OdpSubscriptionStateManager& operator=(const OdpSubscriptionStateManager&) = delete;
    OdpSubscriptionStateManager(OdpSubscriptionStateManager&&) = delete;
    OdpSubscriptionStateManager& operator=(OdpSubscriptionStateManager&&) = delete;

    // State inquiry methods
    SubscriptionPhase GetCurrentPhase() const { return current_phase_; }
    bool ShouldPerformInitialLoad() const { return current_phase_ == SubscriptionPhase::INITIAL_LOAD; }
    bool ShouldPerformDeltaFetch() const { return current_phase_ == SubscriptionPhase::DELTA_FETCH; }
    std::string GetCurrentDeltaToken() const;
    std::string GetSubscriptionId() const { return current_subscription_.subscription_id; }
    bool IsSubscriptionActive() const;

    // State transition methods
    void TransitionToInitialLoad();
    void TransitionToDeltaFetch(const std::string& delta_token, bool preference_applied = true);
    void TransitionToTerminated();
    void TransitionToError(const std::string& error_msg);

    // Persistence operations
    void PersistSubscription();
    void UpdateDeltaToken(const std::string& token);
    void UpdateSubscriptionStatus(const std::string& status);

    // Audit operations
    int64_t CreateAuditEntry(const std::string& operation_type, const std::string& request_url = "");
    void UpdateAuditEntry(int64_t audit_id, 
                         const std::optional<int>& http_status_code = std::nullopt,
                         int64_t rows_fetched = 0,
                         int64_t package_size_bytes = 0,
                         const std::string& delta_token_after = "",
                         const std::string& error_message = "",
                         const std::optional<int64_t>& duration_ms = std::nullopt);

    // Utility methods
    static std::string PhaseToString(SubscriptionPhase phase);
    void LogCurrentState() const;

private:
    // Core components
    std::unique_ptr<OdpSubscriptionRepository> repository_;
    OdpSubscription current_subscription_;
    SubscriptionPhase current_phase_;
    
    // Configuration
    std::string service_url_;
    std::string entity_set_name_;
    std::string secret_name_;
    bool force_full_load_;
    std::string import_delta_token_;
    
    // State tracking
    int64_t current_audit_id_;
    std::chrono::system_clock::time_point operation_start_time_;

    // Initialization methods
    void InitializeSubscription();
    void LoadExistingSubscription();
    void CreateNewSubscription();
    void DetermineInitialPhase();
    
    // Helper methods
    void ValidateSubscriptionData() const;
    void UpdateLastModified();
};

} // namespace erpl_web
