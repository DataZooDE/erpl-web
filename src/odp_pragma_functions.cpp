#include "odp_pragma_functions.hpp"
#include "odp_subscription_repository.hpp"
#include "tracing.hpp"
#include "duckdb_argument_helper.hpp"

namespace erpl_web {

// ============================================================================
// ODP List Subscriptions Table Function
// ============================================================================

duckdb::unique_ptr<duckdb::FunctionData> OdpListSubscriptionsBind(duckdb::ClientContext &context, 
                                                                  duckdb::TableFunctionBindInput &input,
                                                                  duckdb::vector<duckdb::LogicalType> &return_types,
                                                                  duckdb::vector<std::string> &names) {
    ERPL_TRACE_DEBUG("ODP_LIST_SUBSCRIPTIONS_BIND", "=== BINDING ODP_LIST_SUBSCRIPTIONS FUNCTION ===");
    
    // Set up return schema
    return_types = {
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),    // subscription_id
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),    // service_url
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),    // entity_set_name
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),    // secret_name
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),    // delta_token
        duckdb::LogicalType(duckdb::LogicalTypeId::TIMESTAMP),  // created_at
        duckdb::LogicalType(duckdb::LogicalTypeId::TIMESTAMP),  // last_updated
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),    // subscription_status
        duckdb::LogicalType(duckdb::LogicalTypeId::BOOLEAN)     // preference_applied
    };
    
    names = {
        "subscription_id", "service_url", "entity_set_name", "secret_name", 
        "delta_token", "created_at", "last_updated", "subscription_status", "preference_applied"
    };
    
    // Create bind data that holds the repository
    auto bind_data = duckdb::make_uniq<OdpListSubscriptionsBindData>(context);
    
    ERPL_TRACE_INFO("ODP_LIST_SUBSCRIPTIONS_BIND", "Bound function with 9 columns");
    return std::move(bind_data);
}

void OdpListSubscriptionsScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
    auto &bind_data = data.bind_data->CastNoConst<OdpListSubscriptionsBindData>();
    
    ERPL_TRACE_DEBUG("ODP_LIST_SUBSCRIPTIONS_SCAN", "Starting subscription list scan");
    
    try {
        // Load subscriptions if not already loaded
        if (!bind_data.data_loaded) {
            bind_data.LoadSubscriptions();
        }
        
        // Return subscriptions in batches
        const idx_t target = STANDARD_VECTOR_SIZE;
        idx_t start_idx = bind_data.next_index;
        idx_t end_idx = std::min(start_idx + target, static_cast<idx_t>(bind_data.subscriptions.size()));
        idx_t count = end_idx - start_idx;
        
        if (count == 0) {
            ERPL_TRACE_DEBUG("ODP_LIST_SUBSCRIPTIONS_SCAN", "No more subscriptions to return");
            output.SetCardinality(0);
            return;
        }
        
        ERPL_TRACE_DEBUG("ODP_LIST_SUBSCRIPTIONS_SCAN", duckdb::StringUtil::Format(
            "Returning %zu subscriptions (indices %zu to %zu)", count, start_idx, end_idx - 1));
        
        // Fill output chunk
        for (idx_t i = 0; i < count; i++) {
            idx_t row_idx = start_idx + i;
            const auto& subscription = bind_data.subscriptions[row_idx];
            
            output.SetValue(0, i, duckdb::Value(subscription.subscription_id));
            output.SetValue(1, i, duckdb::Value(subscription.service_url));
            output.SetValue(2, i, duckdb::Value(subscription.entity_set_name));
            output.SetValue(3, i, duckdb::Value(subscription.secret_name));
            output.SetValue(4, i, subscription.delta_token.empty() ? 
                          duckdb::Value() : duckdb::Value(subscription.delta_token));
            // Convert std::chrono::time_point to duckdb::timestamp_t
            auto created_time_t = std::chrono::system_clock::to_time_t(subscription.created_at);
            auto updated_time_t = std::chrono::system_clock::to_time_t(subscription.last_updated);
            
            output.SetValue(5, i, duckdb::Value::TIMESTAMP(duckdb::Timestamp::FromEpochSeconds(created_time_t)));
            output.SetValue(6, i, duckdb::Value::TIMESTAMP(duckdb::Timestamp::FromEpochSeconds(updated_time_t)));
            output.SetValue(7, i, duckdb::Value(subscription.subscription_status));
            output.SetValue(8, i, duckdb::Value(subscription.preference_applied));
        }
        
        output.SetCardinality(count);
        bind_data.next_index = end_idx;
        
        ERPL_TRACE_INFO("ODP_LIST_SUBSCRIPTIONS_SCAN", duckdb::StringUtil::Format("Returned %zu subscriptions", count));
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_LIST_SUBSCRIPTIONS_SCAN", "Scan failed: " + std::string(e.what()));
        throw;
    }
}

// ============================================================================
// ODP Remove Subscription Pragma Function
// ============================================================================

void OdpRemoveSubscriptionPragma(duckdb::ClientContext &context, const duckdb::FunctionParameters &parameters) {
    ERPL_TRACE_DEBUG("ODP_REMOVE_SUBSCRIPTION", "=== EXECUTING ODP_REMOVE_SUBSCRIPTION PRAGMA ===");
    
    if (parameters.values.empty()) {
        throw duckdb::InvalidInputException("odp_odata_remove_subscription requires at least one parameter: subscription_id");
    }
    
    // Extract subscription ID(s)
    std::vector<std::string> subscription_ids;
    if (parameters.values[0].type().id() == duckdb::LogicalTypeId::LIST) {
        // Handle list of subscription IDs
        auto list_value = parameters.values[0];
        auto list_children = duckdb::ListValue::GetChildren(list_value);
        for (const auto& child : list_children) {
            subscription_ids.push_back(child.ToString());
        }
    } else {
        // Single subscription ID
        subscription_ids.push_back(parameters.values[0].ToString());
    }
    
    // Extract keep_local_data parameter (default: false)
    bool keep_local_data = false;
    if (parameters.values.size() > 1) {
        keep_local_data = parameters.values[1].GetValue<bool>();
    }
    
    ERPL_TRACE_INFO("ODP_REMOVE_SUBSCRIPTION", duckdb::StringUtil::Format(
        "Removing %zu subscription(s), keep_local_data: %s", 
        subscription_ids.size(), keep_local_data ? "true" : "false"));
    
    try {
        OdpSubscriptionRepository repository(context);
        
        for (const auto& subscription_id : subscription_ids) {
            ERPL_TRACE_INFO("ODP_REMOVE_SUBSCRIPTION", "Processing subscription: " + subscription_id);
            
            // Get subscription details before removal
            auto subscription = repository.GetSubscription(subscription_id);
            if (!subscription.has_value()) {
                ERPL_TRACE_WARN("ODP_REMOVE_SUBSCRIPTION", "Subscription not found: " + subscription_id);
                continue;
            }
            
            if (!keep_local_data) {
                // Remove both remote and local data
                bool removed = repository.RemoveSubscription(subscription_id);
                if (removed) {
                    ERPL_TRACE_INFO("ODP_REMOVE_SUBSCRIPTION", "Successfully removed subscription: " + subscription_id);
                    
                    // Log audit entry for removal
                    OdpAuditEntry audit_entry;
                    audit_entry.subscription_id = subscription_id;
                    audit_entry.operation_type = "subscription_removed";
                    audit_entry.request_timestamp = std::chrono::system_clock::now();
                    audit_entry.http_status_code = 200;
                    audit_entry.delta_token_before = subscription->delta_token;
                    
                    repository.CreateAuditEntry(audit_entry);
                } else {
                    ERPL_TRACE_ERROR("ODP_REMOVE_SUBSCRIPTION", "Failed to remove subscription: " + subscription_id);
                }
            } else {
                // Only terminate remote subscription, keep local data
                // For now, just update status to 'terminated'
                bool updated = repository.UpdateSubscriptionStatus(subscription_id, "terminated");
                if (updated) {
                    ERPL_TRACE_INFO("ODP_REMOVE_SUBSCRIPTION", "Terminated remote subscription: " + subscription_id);
                    
                    // Log audit entry for termination
                    OdpAuditEntry audit_entry;
                    audit_entry.subscription_id = subscription_id;
                    audit_entry.operation_type = "subscription_terminated";
                    audit_entry.request_timestamp = std::chrono::system_clock::now();
                    audit_entry.http_status_code = 200;
                    audit_entry.delta_token_before = subscription->delta_token;
                    
                    repository.CreateAuditEntry(audit_entry);
                } else {
                    ERPL_TRACE_ERROR("ODP_REMOVE_SUBSCRIPTION", "Failed to terminate subscription: " + subscription_id);
                }
            }
        }
        
        ERPL_TRACE_INFO("ODP_REMOVE_SUBSCRIPTION", "Completed processing all subscriptions");
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_REMOVE_SUBSCRIPTION", "Error: " + std::string(e.what()));
        throw;
    }
}

// ============================================================================
// Bind Data Implementation
// ============================================================================

OdpListSubscriptionsBindData::OdpListSubscriptionsBindData(duckdb::ClientContext& context) 
    : context_(context), data_loaded(false), next_index(0) {
    ERPL_TRACE_DEBUG("ODP_LIST_SUBSCRIPTIONS_BIND_DATA", "Created bind data");
}

void OdpListSubscriptionsBindData::LoadSubscriptions() {
    ERPL_TRACE_DEBUG("ODP_LIST_SUBSCRIPTIONS_BIND_DATA", "Loading subscriptions from repository");
    
    try {
        OdpSubscriptionRepository repository(context_);
        subscriptions = repository.ListAllSubscriptions();
        data_loaded = true;
        
        ERPL_TRACE_INFO("ODP_LIST_SUBSCRIPTIONS_BIND_DATA", duckdb::StringUtil::Format(
            "Loaded %zu subscriptions", subscriptions.size()));
            
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_LIST_SUBSCRIPTIONS_BIND_DATA", "Failed to load subscriptions: " + std::string(e.what()));
        throw;
    }
}

// ============================================================================
// Function Registration
// ============================================================================

duckdb::TableFunctionSet CreateOdpListSubscriptionsFunction() {
    ERPL_TRACE_DEBUG("ODP_PRAGMA_REGISTRATION", "=== REGISTERING ODP_LIST_SUBSCRIPTIONS FUNCTION ===");
    
    duckdb::TableFunctionSet function_set("odp_odata_list_subscriptions");
    
    // Table function: odp_odata_list_subscriptions()
    duckdb::TableFunction list_function(
        {},  // No parameters
        OdpListSubscriptionsScan,
        OdpListSubscriptionsBind
    );
    
    function_set.AddFunction(list_function);
    
    ERPL_TRACE_INFO("ODP_PRAGMA_REGISTRATION", "ODP_LIST_SUBSCRIPTIONS function registered successfully");
    return function_set;
}

duckdb::PragmaFunction CreateOdpRemoveSubscriptionFunction() {
    ERPL_TRACE_DEBUG("ODP_PRAGMA_REGISTRATION", "=== REGISTERING ODP_REMOVE_SUBSCRIPTION PRAGMA ===");
    
    duckdb::PragmaFunction pragma_function = duckdb::PragmaFunction::PragmaCall(
        "odp_odata_remove_subscription", OdpRemoveSubscriptionPragma, {duckdb::LogicalType::ANY, duckdb::LogicalType::BOOLEAN}
    );
    
    ERPL_TRACE_INFO("ODP_PRAGMA_REGISTRATION", "ODP_REMOVE_SUBSCRIPTION pragma registered successfully");
    return pragma_function;
}

} // namespace erpl_web
