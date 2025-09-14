#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "odp_subscription_repository.hpp"

namespace erpl_web {

// ============================================================================
// ODP List Subscriptions Table Function
// ============================================================================

/**
 * @brief Bind data for odp_odata_list_subscriptions table function
 */
class OdpListSubscriptionsBindData : public duckdb::TableFunctionData {
public:
    explicit OdpListSubscriptionsBindData(duckdb::ClientContext& context);
    
    void LoadSubscriptions();
    
    duckdb::ClientContext& context_;
    std::vector<OdpSubscription> subscriptions;
    bool data_loaded;
    idx_t next_index;
};

/**
 * @brief Bind function for odp_odata_list_subscriptions table function
 * 
 * Returns all active ODP subscriptions with their current status, delta tokens,
 * and metadata. This function takes no parameters and returns a table with
 * subscription details.
 * 
 * @param context DuckDB client context
 * @param input Function binding input (no parameters expected)
 * @param return_types Output parameter for column types
 * @param names Output parameter for column names
 * @return Unique pointer to OdpListSubscriptionsBindData
 */
duckdb::unique_ptr<duckdb::FunctionData> OdpListSubscriptionsBind(duckdb::ClientContext &context, 
                                                                  duckdb::TableFunctionBindInput &input,
                                                                  duckdb::vector<duckdb::LogicalType> &return_types,
                                                                  duckdb::vector<std::string> &names);

/**
 * @brief Scan function for odp_odata_list_subscriptions table function
 * 
 * Retrieves and returns subscription data from the ODP subscription repository.
 * Returns data in batches for efficient processing.
 * 
 * @param context DuckDB client context
 * @param data Table function input with bind data
 * @param output Output data chunk to fill
 */
void OdpListSubscriptionsScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output);

// ============================================================================
// ODP Remove Subscription Pragma Function
// ============================================================================

/**
 * @brief Pragma function for removing ODP subscriptions
 * 
 * Removes one or more ODP subscriptions, with options for handling local data.
 * 
 * Usage:
 * - PRAGMA odp_odata_remove_subscription('subscription_id')
 * - PRAGMA odp_odata_remove_subscription(['id1', 'id2'], keep_local_data=true)
 * 
 * Parameters:
 * - subscription_id: Single ID (VARCHAR) or list of IDs (LIST)
 * - keep_local_data: Optional BOOLEAN (default: false)
 *   - false: Remove both remote subscription and local data
 *   - true: Only terminate remote subscription, keep local data
 * 
 * @param context DuckDB client context
 * @param parameters Function parameters (subscription_id(s), keep_local_data)
 */
void OdpRemoveSubscriptionPragma(duckdb::ClientContext &context, const duckdb::FunctionParameters &parameters);

// ============================================================================
// Function Registration
// ============================================================================

/**
 * @brief Create the odp_odata_list_subscriptions table function set
 * 
 * Registers the table function for listing ODP subscriptions.
 * Returns a table with columns:
 * - subscription_id (VARCHAR): Unique subscription identifier
 * - service_url (VARCHAR): OData service URL
 * - entity_set_name (VARCHAR): Entity set name
 * - secret_name (VARCHAR): Authentication secret name
 * - delta_token (VARCHAR): Current delta token (nullable)
 * - created_at (TIMESTAMP): Subscription creation time
 * - last_updated (TIMESTAMP): Last update time
 * - subscription_status (VARCHAR): Current status (active, terminated, error)
 * - preference_applied (BOOLEAN): Whether change tracking is enabled
 * 
 * @return TableFunctionSet for registration with DuckDB
 */
duckdb::TableFunctionSet CreateOdpListSubscriptionsFunction();

/**
 * @brief Create the odp_odata_remove_subscription pragma function
 * 
 * Registers the pragma function for removing ODP subscriptions.
 * Supports both single subscription removal and batch operations.
 * 
 * @return PragmaFunction for registration with DuckDB
 */
duckdb::PragmaFunction CreateOdpRemoveSubscriptionFunction();

} // namespace erpl_web
