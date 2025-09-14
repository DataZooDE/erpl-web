#include "odp_subscription_repository.hpp"
#include "tracing.hpp"
#include <sstream>
#include <iomanip>
#include <regex>
#include <algorithm>

namespace erpl_web {

// ============================================================================
// OdpSubscription Implementation
// ============================================================================

OdpSubscription::OdpSubscription(const std::string& service_url, 
                               const std::string& entity_set_name, 
                               const std::string& secret_name)
    : service_url(service_url)
    , entity_set_name(entity_set_name)
    , secret_name(secret_name.empty() ? "default" : secret_name)
    , created_at(std::chrono::system_clock::now())
    , last_updated(std::chrono::system_clock::now())
    , subscription_status("active")
    , preference_applied(false)
{
    subscription_id = OdpSubscriptionRepository::GenerateSubscriptionId(service_url, entity_set_name);
}

// ============================================================================
// OdpAuditEntry Implementation
// ============================================================================

OdpAuditEntry::OdpAuditEntry(const std::string& subscription_id, const std::string& operation_type)
    : audit_id(0) // Will be set by database auto-increment
    , subscription_id(subscription_id)
    , operation_type(operation_type)
    , request_timestamp(std::chrono::system_clock::now())
    , rows_fetched(0)
    , package_size_bytes(0)
{
}

// ============================================================================
// OdpSubscriptionRepository Implementation
// ============================================================================

OdpSubscriptionRepository::OdpSubscriptionRepository(duckdb::ClientContext& context)
    : context(context)
    , schema_initialized(false)
    , tables_initialized(false)
{
    ERPL_TRACE_INFO("ODP_REPOSITORY", "OdpSubscriptionRepository initialized");
}

void OdpSubscriptionRepository::EnsureSchemaExists() {
    if (schema_initialized) {
        return;
    }
    
    ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Ensuring erpl_web schema exists");
    
    try {
        InitializeSchema();
        schema_initialized = true;
        ERPL_TRACE_INFO("ODP_REPOSITORY", "Schema erpl_web initialized successfully");
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_REPOSITORY", "Failed to initialize schema: " + std::string(e.what()));
        throw;
    }
}

void OdpSubscriptionRepository::EnsureTablesExist() {
    if (tables_initialized) {
        return;
    }
    
    EnsureSchemaExists(); // Ensure schema exists first
    
    ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Ensuring ODP tables exist");
    
    try {
        InitializeTables();
        tables_initialized = true;
        ERPL_TRACE_INFO("ODP_REPOSITORY", "ODP tables initialized successfully");
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_REPOSITORY", "Failed to initialize tables: " + std::string(e.what()));
        throw;
    }
}

std::string OdpSubscriptionRepository::CreateSubscription(const std::string& service_url,
                                                        const std::string& entity_set_name,
                                                        const std::string& secret_name) {
    ERPL_TRACE_INFO("ODP_REPOSITORY", duckdb::StringUtil::Format(
        "Creating subscription for URL: %s, Entity: %s, Secret: %s", 
        service_url, entity_set_name, secret_name));
    
    EnsureTablesExist();
    
    // Validate URL
    if (!IsValidOdpUrl(service_url)) {
        throw duckdb::InvalidInputException("Invalid ODP URL: " + service_url);
    }
    
    // Check if subscription already exists
    auto existing = FindActiveSubscription(service_url, entity_set_name);
    if (existing.has_value()) {
        ERPL_TRACE_WARN("ODP_REPOSITORY", "Active subscription already exists: " + existing->subscription_id);
        return existing->subscription_id;
    }
    
    // Create new subscription
    OdpSubscription subscription(service_url, entity_set_name, secret_name);
    
    try {
        auto query = BuildInsertSubscriptionQuery(subscription);
        ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Executing insert query: " + query);
        
        auto result = ExecuteQuery(query);
        if (!result->HasError()) {
            ERPL_TRACE_INFO("ODP_REPOSITORY", "Subscription created successfully: " + subscription.subscription_id);
            return subscription.subscription_id;
        } else {
            throw duckdb::InternalException("Failed to create subscription: " + result->GetError());
        }
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_REPOSITORY", "Error creating subscription: " + std::string(e.what()));
        throw;
    }
}

std::optional<OdpSubscription> OdpSubscriptionRepository::GetSubscription(const std::string& subscription_id) {
    ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Getting subscription: " + subscription_id);
    
    EnsureTablesExist();
    
    std::string query = duckdb::StringUtil::Format(
        "SELECT subscription_id, service_url, entity_set_name, secret_name, delta_token, "
        "created_at, last_updated, subscription_status, preference_applied "
        "FROM erpl_web.odp_subscriptions WHERE subscription_id = '%s'",
        subscription_id);
    
    try {
        auto result = ExecuteQuery(query);
        if (result->HasError()) {
            ERPL_TRACE_ERROR("ODP_REPOSITORY", "Query error: " + result->GetError());
            return std::nullopt;
        }
        
        if (result->RowCount() == 0) {
            ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Subscription not found: " + subscription_id);
            return std::nullopt;
        }
        
        // Parse result into OdpSubscription
        OdpSubscription subscription;
        subscription.subscription_id = result->GetValue(0, 0).ToString();
        subscription.service_url = result->GetValue(1, 0).ToString();
        subscription.entity_set_name = result->GetValue(2, 0).ToString();
        subscription.secret_name = result->GetValue(3, 0).ToString();
        subscription.delta_token = result->GetValue(4, 0).ToString();
        subscription.created_at = StringToTimestamp(result->GetValue(5, 0).ToString());
        subscription.last_updated = StringToTimestamp(result->GetValue(6, 0).ToString());
        subscription.subscription_status = result->GetValue(7, 0).ToString();
        subscription.preference_applied = result->GetValue(8, 0).GetValue<bool>();
        
        ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Subscription found: " + subscription_id);
        return subscription;
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_REPOSITORY", "Error getting subscription: " + std::string(e.what()));
        return std::nullopt;
    }
}

std::optional<OdpSubscription> OdpSubscriptionRepository::FindActiveSubscription(
    const std::string& service_url, const std::string& entity_set_name) {
    
    ERPL_TRACE_DEBUG("ODP_REPOSITORY", duckdb::StringUtil::Format(
        "Finding active subscription for URL: %s, Entity: %s", service_url, entity_set_name));
    
    EnsureTablesExist();
    
    std::string query = duckdb::StringUtil::Format(
        "SELECT subscription_id, service_url, entity_set_name, secret_name, delta_token, "
        "created_at, last_updated, subscription_status, preference_applied "
        "FROM erpl_web.odp_subscriptions "
        "WHERE service_url = '%s' AND entity_set_name = '%s' AND subscription_status = 'active' "
        "ORDER BY created_at DESC LIMIT 1",
        service_url, entity_set_name);
    
    try {
        auto result = ExecuteQuery(query);
        if (result->HasError() || result->RowCount() == 0) {
            return std::nullopt;
        }
        
        // Parse result into OdpSubscription (same logic as GetSubscription)
        OdpSubscription subscription;
        subscription.subscription_id = result->GetValue(0, 0).ToString();
        subscription.service_url = result->GetValue(1, 0).ToString();
        subscription.entity_set_name = result->GetValue(2, 0).ToString();
        subscription.secret_name = result->GetValue(3, 0).ToString();
        subscription.delta_token = result->GetValue(4, 0).ToString();
        subscription.created_at = StringToTimestamp(result->GetValue(5, 0).ToString());
        subscription.last_updated = StringToTimestamp(result->GetValue(6, 0).ToString());
        subscription.subscription_status = result->GetValue(7, 0).ToString();
        subscription.preference_applied = result->GetValue(8, 0).GetValue<bool>();
        
        ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Active subscription found: " + subscription.subscription_id);
        return subscription;
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_REPOSITORY", "Error finding active subscription: " + std::string(e.what()));
        return std::nullopt;
    }
}

std::vector<OdpSubscription> OdpSubscriptionRepository::ListAllSubscriptions() {
    ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Listing all subscriptions");
    
    EnsureTablesExist();
    
    std::string query = 
        "SELECT subscription_id, service_url, entity_set_name, secret_name, delta_token, "
        "created_at, last_updated, subscription_status, preference_applied "
        "FROM erpl_web.odp_subscriptions "
        "ORDER BY created_at DESC";
    
    std::vector<OdpSubscription> subscriptions;
    
    try {
        auto result = ExecuteQuery(query);
        if (result->HasError()) {
            ERPL_TRACE_ERROR("ODP_REPOSITORY", "Query error: " + result->GetError());
            return subscriptions;
        }
        
        for (duckdb::idx_t i = 0; i < result->RowCount(); i++) {
            OdpSubscription subscription;
            subscription.subscription_id = result->GetValue(0, i).ToString();
            subscription.service_url = result->GetValue(1, i).ToString();
            subscription.entity_set_name = result->GetValue(2, i).ToString();
            subscription.secret_name = result->GetValue(3, i).ToString();
            subscription.delta_token = result->GetValue(4, i).ToString();
            subscription.created_at = StringToTimestamp(result->GetValue(5, i).ToString());
            subscription.last_updated = StringToTimestamp(result->GetValue(6, i).ToString());
            subscription.subscription_status = result->GetValue(7, i).ToString();
            subscription.preference_applied = result->GetValue(8, i).GetValue<bool>();
            
            subscriptions.push_back(subscription);
        }
        
        ERPL_TRACE_INFO("ODP_REPOSITORY", duckdb::StringUtil::Format("Found %d subscriptions", subscriptions.size()));
        return subscriptions;
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_REPOSITORY", "Error listing subscriptions: " + std::string(e.what()));
        return subscriptions;
    }
}

bool OdpSubscriptionRepository::UpdateDeltaToken(const std::string& subscription_id, 
                                                const std::string& delta_token) {
    ERPL_TRACE_DEBUG("ODP_REPOSITORY", duckdb::StringUtil::Format(
        "Updating delta token for subscription %s: %s", subscription_id, delta_token));
    
    EnsureTablesExist();
    
    // First check if subscription exists
    auto existing = GetSubscription(subscription_id);
    if (!existing.has_value()) {
        ERPL_TRACE_WARN("ODP_REPOSITORY", "No subscription found with ID: " + subscription_id);
        return false;
    }
    
    // Escape single quotes in delta token to prevent SQL injection
    auto escape_sql = [](const std::string &s) {
        std::string out;
        out.reserve(s.size() + 8);
        for (char c : s) {
            if (c == '\'') {
                out += "''";
            } else {
                out += c;
            }
        }
        return out;
    };
    
    std::string query = duckdb::StringUtil::Format(
        "UPDATE erpl_web.odp_subscriptions "
        "SET delta_token = '%s', last_updated = NOW() "
        "WHERE subscription_id = '%s'",
        escape_sql(delta_token), escape_sql(subscription_id));
    
    try {
        auto result = ExecuteQuery(query);
        if (result->HasError()) {
            ERPL_TRACE_ERROR("ODP_REPOSITORY", "Update error: " + result->GetError());
            return false;
        }
        
        ERPL_TRACE_INFO("ODP_REPOSITORY", "Delta token updated successfully");
        return true;
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_REPOSITORY", "Error updating delta token: " + std::string(e.what()));
        return false;
    }
}

bool OdpSubscriptionRepository::UpdateSubscription(const OdpSubscription& subscription) {
    ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Updating subscription: " + subscription.subscription_id);
    
    EnsureTablesExist();
    
    std::string query = duckdb::StringUtil::Format(
        "UPDATE erpl_web.odp_subscriptions "
        "SET service_url = '%s', entity_set_name = '%s', secret_name = '%s', "
        "delta_token = '%s', subscription_status = '%s', preference_applied = %s, "
        "last_updated = NOW() "
        "WHERE subscription_id = '%s'",
        subscription.service_url, subscription.entity_set_name, subscription.secret_name,
        subscription.delta_token, subscription.subscription_status, 
        subscription.preference_applied ? "true" : "false",
        subscription.subscription_id);
    
    try {
        auto result = ExecuteQuery(query);
        if (result->HasError()) {
            ERPL_TRACE_ERROR("ODP_REPOSITORY", "Update error: " + result->GetError());
            return false;
        }
        
        // Check if any rows were affected
        if (result->RowCount() == 0) {
            ERPL_TRACE_WARN("ODP_REPOSITORY", "No subscription found with ID: " + subscription.subscription_id);
            return false;
        }
        
        ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Subscription updated successfully");
        return true;
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_REPOSITORY", "Error updating subscription: " + std::string(e.what()));
        return false;
    }
}

bool OdpSubscriptionRepository::UpdateSubscriptionStatus(const std::string& subscription_id, 
                                                       const std::string& status) {
    ERPL_TRACE_DEBUG("ODP_REPOSITORY", duckdb::StringUtil::Format(
        "Updating subscription status for %s: %s", subscription_id, status));
    
    EnsureTablesExist();
    
    // First check if subscription exists
    auto existing = GetSubscription(subscription_id);
    if (!existing.has_value()) {
        ERPL_TRACE_WARN("ODP_REPOSITORY", "No subscription found with ID: " + subscription_id);
        return false;
    }
    
    std::string query = duckdb::StringUtil::Format(
        "UPDATE erpl_web.odp_subscriptions "
        "SET subscription_status = '%s', last_updated = NOW() "
        "WHERE subscription_id = '%s'",
        status, subscription_id);
    
    try {
        auto result = ExecuteQuery(query);
        if (result->HasError()) {
            ERPL_TRACE_ERROR("ODP_REPOSITORY", "Update error: " + result->GetError());
            return false;
        }
        
        ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Subscription status updated successfully");
        return true;
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_REPOSITORY", "Error updating subscription status: " + std::string(e.what()));
        return false;
    }
}

bool OdpSubscriptionRepository::RemoveSubscription(const std::string& subscription_id) {
    ERPL_TRACE_INFO("ODP_REPOSITORY", "Removing subscription: " + subscription_id);
    
    EnsureTablesExist();
    
    // First check if subscription exists
    auto existing = GetSubscription(subscription_id);
    if (!existing.has_value()) {
        ERPL_TRACE_WARN("ODP_REPOSITORY", "No subscription found with ID: " + subscription_id);
        return false;
    }
    
    std::string query = duckdb::StringUtil::Format(
        "DELETE FROM erpl_web.odp_subscriptions WHERE subscription_id = '%s'",
        subscription_id);
    
    try {
        auto result = ExecuteQuery(query);
        if (result->HasError()) {
            ERPL_TRACE_ERROR("ODP_REPOSITORY", "Delete error: " + result->GetError());
            return false;
        }
        
        ERPL_TRACE_INFO("ODP_REPOSITORY", "Subscription removed successfully");
        return true;
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_REPOSITORY", "Error removing subscription: " + std::string(e.what()));
        return false;
    }
}

int64_t OdpSubscriptionRepository::CreateAuditEntry(const OdpAuditEntry& entry) {
    ERPL_TRACE_DEBUG("ODP_REPOSITORY", duckdb::StringUtil::Format(
        "Creating audit entry for subscription %s, operation: %s", 
        entry.subscription_id, entry.operation_type));
    
    EnsureTablesExist();
    
    try {
        // Generate a unique audit_id using timestamp + random component
        auto now = std::chrono::system_clock::now();
        auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
        int64_t audit_id = timestamp;
        
        // Create a copy of the entry with the generated ID
        OdpAuditEntry entry_with_id = entry;
        entry_with_id.audit_id = audit_id;
        
        auto query = BuildInsertAuditQuery(entry_with_id);
        ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Executing audit insert query: " + query);
        
        auto result = ExecuteQuery(query);
        if (result->HasError()) {
            ERPL_TRACE_ERROR("ODP_REPOSITORY", "Audit insert error: " + result->GetError());
            return -1;
        }
        
        ERPL_TRACE_INFO("ODP_REPOSITORY", duckdb::StringUtil::Format("Audit entry created with ID: %lld", audit_id));
        return audit_id;
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_REPOSITORY", "Error creating audit entry: " + std::string(e.what()));
        return -1;
    }
}

bool OdpSubscriptionRepository::UpdateAuditEntry(const OdpAuditEntry& entry) {
    ERPL_TRACE_DEBUG("ODP_REPOSITORY", duckdb::StringUtil::Format(
        "Updating audit entry %lld for subscription %s", 
        entry.audit_id, entry.subscription_id));
    
    EnsureTablesExist();
    
    // Escape single quotes in string fields to prevent SQL injection
    auto escape_sql = [](const std::string &s) {
        std::string out;
        out.reserve(s.size() + 8);
        for (char c : s) {
            if (c == '\'') {
                out += "''";
            } else {
                out += c;
            }
        }
        return out;
    };
    
    std::string query = duckdb::StringUtil::Format(
        "UPDATE erpl_web.odp_subscription_audit "
        "SET response_timestamp = NOW(), "
        "http_status_code = %s, "
        "rows_fetched = %lld, "
        "package_size_bytes = %lld, "
        "delta_token_after = '%s', "
        "error_message = '%s', "
        "duration_ms = %s "
        "WHERE audit_id = %lld",
        entry.http_status_code.has_value() ? std::to_string(entry.http_status_code.value()) : "NULL",
        entry.rows_fetched,
        entry.package_size_bytes,
        escape_sql(entry.delta_token_after),
        escape_sql(entry.error_message),
        entry.duration_ms.has_value() ? std::to_string(entry.duration_ms.value()) : "NULL",
        entry.audit_id);
    
    try {
        auto result = ExecuteQuery(query);
        if (result->HasError()) {
            ERPL_TRACE_ERROR("ODP_REPOSITORY", "Audit update error: " + result->GetError());
            return false;
        }
        
        ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Audit entry updated successfully");
        return true;
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_REPOSITORY", "Error updating audit entry: " + std::string(e.what()));
        return false;
    }
}

// ============================================================================
// Static Utility Methods
// ============================================================================

std::string OdpSubscriptionRepository::GenerateSubscriptionId(const std::string& service_url,
                                                             const std::string& entity_set_name) {
    // Generate timestamp prefix: YYYYMMDD_HHMMSS
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto tm = *std::localtime(&time_t);
    
    std::ostringstream timestamp_stream;
    timestamp_stream << std::put_time(&tm, "%Y%m%d_%H%M%S");
    
    // Clean and combine URL and entity set name
    std::string cleaned_url = CleanUrlForId(service_url);
    std::string cleaned_entity = CleanUrlForId(entity_set_name);
    
    std::string subscription_id = timestamp_stream.str() + "_" + cleaned_url + "_" + cleaned_entity;
    
    ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Generated subscription ID: " + subscription_id);
    return subscription_id;
}

std::string OdpSubscriptionRepository::CleanUrlForId(const std::string& url) {
    // Remove protocol, replace special characters with underscores
    std::string cleaned = url;
    
    // Remove http:// or https://
    std::regex protocol_regex("^https?://");
    cleaned = std::regex_replace(cleaned, protocol_regex, "");
    
    // Replace special characters with underscores
    std::regex special_chars_regex("[^a-zA-Z0-9_]");
    cleaned = std::regex_replace(cleaned, special_chars_regex, "_");
    
    // Remove multiple consecutive underscores
    std::regex multiple_underscores_regex("_{2,}");
    cleaned = std::regex_replace(cleaned, multiple_underscores_regex, "_");
    
    // Remove leading/trailing underscores
    if (!cleaned.empty() && cleaned.front() == '_') {
        cleaned = cleaned.substr(1);
    }
    if (!cleaned.empty() && cleaned.back() == '_') {
        cleaned = cleaned.substr(0, cleaned.length() - 1);
    }
    
    // Limit length to reasonable size
    if (cleaned.length() > 100) {
        cleaned = cleaned.substr(0, 100);
    }
    
    return cleaned;
}

bool OdpSubscriptionRepository::IsValidOdpUrl(const std::string& url) {
    // Check for ODP naming patterns: EntityOf* or FactsOf*
    std::regex odp_pattern(".*/(EntityOf|FactsOf)[^/]*$", std::regex_constants::icase);
    bool is_odp = std::regex_search(url, odp_pattern);
    
    ERPL_TRACE_DEBUG("ODP_REPOSITORY", duckdb::StringUtil::Format(
        "URL validation for %s: %s", url, is_odp ? "VALID" : "INVALID"));
    
    return is_odp;
}

// ============================================================================
// Private Helper Methods
// ============================================================================

void OdpSubscriptionRepository::InitializeSchema() {
    std::string query = "CREATE SCHEMA IF NOT EXISTS erpl_web";
    auto result = ExecuteQuery(query);
    if (result->HasError()) {
        throw duckdb::InternalException("Failed to create schema: " + result->GetError());
    }
}

void OdpSubscriptionRepository::InitializeTables() {
    // Create subscriptions table
    std::string subscriptions_query = R"(
        CREATE TABLE IF NOT EXISTS erpl_web.odp_subscriptions (
            subscription_id VARCHAR PRIMARY KEY,
            service_url VARCHAR NOT NULL,
            entity_set_name VARCHAR NOT NULL,
            secret_name VARCHAR,
            delta_token VARCHAR,
            created_at TIMESTAMP DEFAULT NOW(),
            last_updated TIMESTAMP DEFAULT NOW(),
            subscription_status VARCHAR DEFAULT 'active',
            preference_applied BOOLEAN DEFAULT FALSE
        )
    )";
    
    auto result = ExecuteQuery(subscriptions_query);
    if (result->HasError()) {
        throw duckdb::InternalException("Failed to create subscriptions table: " + result->GetError());
    }
    
    // Create audit table
    std::string audit_query = R"(
        CREATE TABLE IF NOT EXISTS erpl_web.odp_subscription_audit (
            audit_id BIGINT PRIMARY KEY,
            subscription_id VARCHAR NOT NULL,
            operation_type VARCHAR NOT NULL,
            request_timestamp TIMESTAMP DEFAULT NOW(),
            response_timestamp TIMESTAMP,
            request_url VARCHAR,
            http_status_code INTEGER,
            rows_fetched BIGINT DEFAULT 0,
            package_size_bytes BIGINT DEFAULT 0,
            delta_token_before VARCHAR,
            delta_token_after VARCHAR,
            error_message VARCHAR,
            duration_ms BIGINT
        )
    )";
    
    result = ExecuteQuery(audit_query);
    if (result->HasError()) {
        throw duckdb::InternalException("Failed to create audit table: " + result->GetError());
    }
}

duckdb::unique_ptr<duckdb::MaterializedQueryResult> OdpSubscriptionRepository::ExecuteQuery(const std::string& query) {
    ERPL_TRACE_DEBUG("ODP_REPOSITORY", "Executing query: " + query);
    
    auto connection = duckdb::Connection(context.db->GetDatabase(context));
    return connection.Query(query);
}

std::string OdpSubscriptionRepository::BuildInsertSubscriptionQuery(const OdpSubscription& subscription) {
    // Escape single quotes in string fields to prevent SQL injection
    auto escape_sql = [](const std::string &s) {
        std::string out;
        out.reserve(s.size() + 8);
        for (char c : s) {
            if (c == '\'') {
                out += "''";
            } else {
                out += c;
            }
        }
        return out;
    };
    
    return duckdb::StringUtil::Format(
        "INSERT INTO erpl_web.odp_subscriptions "
        "(subscription_id, service_url, entity_set_name, secret_name, delta_token, "
        "created_at, last_updated, subscription_status, preference_applied) "
        "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s)",
        escape_sql(subscription.subscription_id),
        escape_sql(subscription.service_url),
        escape_sql(subscription.entity_set_name),
        escape_sql(subscription.secret_name),
        escape_sql(subscription.delta_token),
        TimestampToString(subscription.created_at),
        TimestampToString(subscription.last_updated),
        escape_sql(subscription.subscription_status),
        subscription.preference_applied ? "TRUE" : "FALSE"
    );
}

std::string OdpSubscriptionRepository::BuildInsertAuditQuery(const OdpAuditEntry& entry) {
    // Escape single quotes in string fields to prevent SQL injection
    auto escape_sql = [](const std::string &s) {
        std::string out;
        out.reserve(s.size() + 8);
        for (char c : s) {
            if (c == '\'') {
                out += "''";
            } else {
                out += c;
            }
        }
        return out;
    };
    
    std::string response_ts = entry.response_timestamp.has_value() 
        ? "'" + TimestampToString(entry.response_timestamp.value()) + "'"
        : "NULL";
    
    std::string http_code = entry.http_status_code.has_value() 
        ? std::to_string(entry.http_status_code.value())
        : "NULL";
    
    std::string duration = entry.duration_ms.has_value() 
        ? std::to_string(entry.duration_ms.value())
        : "NULL";
    
    return duckdb::StringUtil::Format(
        "INSERT INTO erpl_web.odp_subscription_audit "
        "(audit_id, subscription_id, operation_type, request_timestamp, response_timestamp, "
        "request_url, http_status_code, rows_fetched, package_size_bytes, "
        "delta_token_before, delta_token_after, error_message, duration_ms) "
        "VALUES (%lld, '%s', '%s', '%s', %s, '%s', %s, %lld, %lld, '%s', '%s', '%s', %s)",
        entry.audit_id,
        escape_sql(entry.subscription_id),
        escape_sql(entry.operation_type),
        TimestampToString(entry.request_timestamp),
        response_ts,
        escape_sql(entry.request_url),
        http_code,
        entry.rows_fetched,
        entry.package_size_bytes,
        escape_sql(entry.delta_token_before),
        escape_sql(entry.delta_token_after),
        escape_sql(entry.error_message),
        duration
    );
}

std::string OdpSubscriptionRepository::TimestampToString(const std::chrono::system_clock::time_point& tp) {
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    auto tm = *std::gmtime(&time_t);
    
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

std::chrono::system_clock::time_point OdpSubscriptionRepository::StringToTimestamp(const std::string& str) {
    std::tm tm = {};
    std::istringstream ss(str);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    
    auto time_t = std::mktime(&tm);
    return std::chrono::system_clock::from_time_t(time_t);
}

} // namespace erpl_web
