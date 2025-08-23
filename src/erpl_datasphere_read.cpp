#include "erpl_datasphere_read.hpp"
#include "erpl_datasphere_client.hpp"
#include "erpl_odata_client.hpp"
#include "erpl_odata_content.hpp"
#include "erpl_oauth2_flow_v2.hpp"
#include "erpl_odata_functions.hpp"
#include "erpl_odata_read_functions.hpp"
#include "erpl_datasphere_secret.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/function/table_function.hpp"
#include "erpl_tracing.hpp"
#include <sstream>
#include <algorithm>

namespace erpl_web {


static duckdb::unique_ptr<duckdb::FunctionData> DatasphereReadRelationalBind(duckdb::ClientContext &context, 
                                                                           duckdb::TableFunctionBindInput &input,
                                                                           duckdb::vector<duckdb::LogicalType> &return_types,
                                                                           duckdb::vector<std::string> &names) {
    ERPL_TRACE_DEBUG("DATASPHERE_RELATIONAL_BIND", "=== DATASPHERE_RELATIONAL_BIND CALLED ===");
    
    // Extract parameters from input
    auto space_id = input.inputs[0].GetValue<std::string>();
    auto asset_id = input.inputs[1].GetValue<std::string>();
    
    // Extract secret_name from input or named parameters
    std::string secret_name = "datasphere"; // Default to "datasphere" secret
    if (input.inputs.size() > 2) {
        secret_name = input.inputs[2].GetValue<std::string>();
    } else if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters["secret"].GetValue<std::string>();
    }
    
    // Resolve tenant, data_center and token via centralized helper
    auto auth = ResolveDatasphereAuth(context, secret_name);
    auto tenant = auth.tenant_name;
    auto data_center = auth.data_center;
    auto access_token = auth.access_token;
    auto auth_params = auth.auth_params;

    ERPL_TRACE_INFO("DATASPHERE_RELATIONAL_BIND", "Using tenant: " + tenant + ", data_center: " + data_center +
                    ", space_id: " + space_id + ", asset_id: " + asset_id);
    ERPL_TRACE_DEBUG("DATASPHERE_RELATIONAL_BIND", "Retrieved access token from secret (length: " + std::to_string(access_token.length()) + ")");

    // Check if space_id is already a full URL (starts with http)
    std::string data_url;
    if (space_id.find("http") == 0) {
        // space_id is already a full URL, check if it already ends with the asset_id
        data_url = space_id;
        if (!space_id.empty() && space_id.back() != '/' && !asset_id.empty()) {
            // Check if the URL already ends with the asset_id to avoid duplication
            if (space_id.length() > asset_id.length() && 
                space_id.substr(space_id.length() - asset_id.length()) == asset_id) {
                // URL already ends with asset_id, don't append
                ERPL_TRACE_DEBUG("DATASPHERE_RELATIONAL_BIND", "URL already ends with asset_id, not appending");
            } else {
                // Append asset_id
                data_url += "/" + asset_id;
            }
        }
    } else {
        // space_id is just an ID, build the full URL
        const auto base_url = DatasphereUrlBuilder::BuildRelationalUrl(tenant, data_center, space_id, asset_id);
        data_url = base_url + "/" + asset_id;
    }

    ERPL_TRACE_INFO("DATASPHERE_RELATIONAL_BIND", "Data URL (entity set root): " + data_url);

    // Delegate to standard OData pipeline; client now derives metadata URL via @odata.context
    auto read_bind = ODataReadBindData::FromEntitySetRoot(data_url, auth_params);
    
    // Handle named parameters for LIMIT and OFFSET
    if (input.named_parameters.find("top") != input.named_parameters.end()) {
        auto limit_value = input.named_parameters["top"].GetValue<duckdb::idx_t>();
        ERPL_TRACE_DEBUG("DATASPHERE_RELATIONAL_BIND", duckdb::StringUtil::Format("Named parameter 'top' set to: %d", limit_value));
        read_bind->PredicatePushdownHelper()->ConsumeLimit(limit_value);
    }
    
    if (input.named_parameters.find("skip") != input.named_parameters.end()) {
        auto offset_value = input.named_parameters["skip"].GetValue<duckdb::idx_t>();
        ERPL_TRACE_DEBUG("DATASPHERE_RELATIONAL_BIND", duckdb::StringUtil::Format("Named parameter 'skip' set to: %d", offset_value));
        read_bind->PredicatePushdownHelper()->ConsumeOffset(offset_value);
    }
    
    names = read_bind->GetResultNames(false);
    return_types = read_bind->GetResultTypes(false);
    
    ERPL_TRACE_DEBUG("DATASPHERE_RELATIONAL_BIND", "=== DATASPHERE_RELATIONAL_BIND COMPLETED ===");
    return std::move(read_bind);
}


static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> DatasphereReadRelationalTableInitGlobalState(duckdb::ClientContext &context,
                                                                                                           duckdb::TableFunctionInitInput &input) 
{
    auto &bind_data = input.bind_data->CastNoConst<ODataReadBindData>();
    auto column_ids = input.column_ids;

    ERPL_TRACE_DEBUG("DATASPHERE_RELATIONAL_INIT", "Initializing datasphere read relational with " + std::to_string(column_ids.size()) + " columns");
    
    // Activate columns - this sets up the column mapping for predicate pushdown
    bind_data.ActivateColumns(column_ids);
    
    // Add filters - this processes WHERE clauses into OData $filter parameters
    bind_data.AddFilters(input.filters);
    
    // Update URL with predicate pushdown - this applies filters, limits, and column selection to the OData URL
    bind_data.UpdateUrlFromPredicatePushdown();

    return duckdb::make_uniq<duckdb::GlobalTableFunctionState>();
}


duckdb::TableFunctionSet CreateDatasphereReadRelationalFunction() {
    ERPL_TRACE_DEBUG("DATASPHERE_FUNCTION_REGISTRATION", "=== REGISTERING DATASPHERE_RELATIONAL FUNCTION ===");
    
    duckdb::TableFunctionSet set("datasphere_read_relational");
    
    // Version with space_id and asset_id (secret will use default)
    duckdb::TableFunction relational_function_2_params({duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR), duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)},
                                                    ODataReadScan, DatasphereReadRelationalBind, DatasphereReadRelationalTableInitGlobalState);
    relational_function_2_params.filter_pushdown = true;
    relational_function_2_params.projection_pushdown = true;
    relational_function_2_params.named_parameters["secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    relational_function_2_params.named_parameters["top"] = duckdb::LogicalType(duckdb::LogicalTypeId::UBIGINT);
    relational_function_2_params.named_parameters["skip"] = duckdb::LogicalType(duckdb::LogicalTypeId::UBIGINT);
    set.AddFunction(relational_function_2_params);
    
    ERPL_TRACE_DEBUG("DATASPHERE_FUNCTION_REGISTRATION", "Added 2-parameter function with DatasphereReadRelationalTableInitGlobalState");
    
    // Version with space_id, asset_id, and secret_name
    duckdb::TableFunction relational_function_3_params({duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR), duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR), duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)},
                                                    ODataReadScan, DatasphereReadRelationalBind, DatasphereReadRelationalTableInitGlobalState);
    relational_function_3_params.filter_pushdown = true;
    relational_function_3_params.projection_pushdown = true;
    relational_function_3_params.named_parameters["top"] = duckdb::LogicalType(duckdb::LogicalTypeId::UBIGINT);
    relational_function_3_params.named_parameters["skip"] = duckdb::LogicalType(duckdb::LogicalTypeId::UBIGINT);
    set.AddFunction(relational_function_3_params);
    
    ERPL_TRACE_DEBUG("DATASPHERE_FUNCTION_REGISTRATION", "Added 3-parameter function with DatasphereReadRelationalTableInitGlobalState");
    ERPL_TRACE_DEBUG("DATASPHERE_FUNCTION_REGISTRATION", "=== DATASPHERE_RELATIONAL FUNCTION REGISTRATION COMPLETED ===");
    
    return set;
}



} // namespace erpl_web
