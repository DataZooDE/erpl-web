#pragma once

#include "duckdb.hpp"
#include "tracing.hpp"
#include <string>
#include <vector>
#include <optional>
#include <chrono>

namespace erpl_web {

// Data structures for ODP subscription management
struct OdpSubscription {
    std::string subscription_id;
    std::string service_url;
    std::string entity_set_name;
    std::string secret_name;
    std::string delta_token;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point last_updated;
    std::string subscription_status; // active, terminated, expired, error
    bool preference_applied;
    
    OdpSubscription() = default;
    OdpSubscription(const std::string& service_url, const std::string& entity_set_name, 
                   const std::string& secret_name = "");
};

struct OdpAuditEntry {
    int64_t audit_id; // Auto-increment, set by database
    std::string subscription_id;
    std::string operation_type; // initial_load, delta_fetch, terminate
    std::chrono::system_clock::time_point request_timestamp;
    std::optional<std::chrono::system_clock::time_point> response_timestamp;
    std::string request_url;
    std::optional<int> http_status_code;
    int64_t rows_fetched;
    int64_t package_size_bytes;
    std::string delta_token_before;
    std::string delta_token_after;
    std::string error_message;
    std::optional<int64_t> duration_ms;
    
    OdpAuditEntry() : audit_id(0), rows_fetched(0), package_size_bytes(0) {}
    OdpAuditEntry(const std::string& subscription_id, const std::string& operation_type);
};

// Repository class for managing ODP subscriptions and audit data
class OdpSubscriptionRepository {
public:
    explicit OdpSubscriptionRepository(duckdb::ClientContext& context);
    ~OdpSubscriptionRepository() = default;
    
    // Non-copyable, non-movable
    OdpSubscriptionRepository(const OdpSubscriptionRepository&) = delete;
    OdpSubscriptionRepository& operator=(const OdpSubscriptionRepository&) = delete;
    OdpSubscriptionRepository(OdpSubscriptionRepository&&) = delete;
    OdpSubscriptionRepository& operator=(OdpSubscriptionRepository&&) = delete;
    
    // Schema management
    void EnsureSchemaExists();
    void EnsureTablesExist();
    
    // Subscription management
    std::string CreateSubscription(const std::string& service_url, 
                                 const std::string& entity_set_name,
                                 const std::string& secret_name = "");
    std::optional<OdpSubscription> GetSubscription(const std::string& subscription_id);
    std::optional<OdpSubscription> FindActiveSubscription(const std::string& service_url, 
                                                        const std::string& entity_set_name);
    std::vector<OdpSubscription> ListAllSubscriptions();
    std::vector<OdpSubscription> ListActiveSubscriptions();
    
    bool UpdateSubscription(const OdpSubscription& subscription);
    bool UpdateDeltaToken(const std::string& subscription_id, const std::string& delta_token);
    bool UpdateSubscriptionStatus(const std::string& subscription_id, const std::string& status);
    bool RemoveSubscription(const std::string& subscription_id);
    
    // Audit management
    int64_t CreateAuditEntry(const OdpAuditEntry& entry);
    bool UpdateAuditEntry(const OdpAuditEntry& entry);
    std::vector<OdpAuditEntry> GetAuditHistory(const std::string& subscription_id, 
                                              int days_back = 30);
    
    // Utility methods
    static std::string GenerateSubscriptionId(const std::string& service_url, 
                                             const std::string& entity_set_name);
    static std::string CleanUrlForId(const std::string& url);
    static bool IsValidOdpUrl(const std::string& url);
    
private:
    duckdb::ClientContext& context;
    bool schema_initialized;
    bool tables_initialized;
    
    // Helper methods
    void InitializeSchema();
    void InitializeTables();
    duckdb::unique_ptr<duckdb::MaterializedQueryResult> ExecuteQuery(const std::string& query);
    std::string TimestampToString(const std::chrono::system_clock::time_point& tp);
    std::chrono::system_clock::time_point StringToTimestamp(const std::string& str);
    
    // SQL query builders
    std::string BuildInsertSubscriptionQuery(const OdpSubscription& subscription);
    std::string BuildUpdateSubscriptionQuery(const OdpSubscription& subscription);
    std::string BuildInsertAuditQuery(const OdpAuditEntry& entry);
    std::string BuildUpdateAuditQuery(const OdpAuditEntry& entry);
};

} // namespace erpl_web
