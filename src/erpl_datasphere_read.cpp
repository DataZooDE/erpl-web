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
#include "erpl_tracing.hpp"
#include <sstream>
#include <algorithm>

namespace erpl_web {

namespace {
    // Helper function to extract input parameters from DuckDB MAP value
    std::map<std::string, std::string> ExtractInputParameters(const duckdb::Value& params_value) {
        std::map<std::string, std::string> input_params;
        
        if (params_value.type().id() != duckdb::LogicalTypeId::MAP) {
            ERPL_TRACE_ERROR("DATASPHERE_RELATIONAL_BIND", "Params parameter must be a MAP<VARCHAR, VARCHAR> type");
            return input_params;
        }
        
        auto map_entries = duckdb::MapValue::GetChildren(params_value);
        ERPL_TRACE_DEBUG("DATASPHERE_RELATIONAL_BIND", "Processing " + std::to_string(map_entries.size()) + " input parameters");
        
        // DuckDB MAPs are stored as a list of structs with 'key' and 'value' fields
        for (const auto& entry : map_entries) {
            if (entry.type().id() == duckdb::LogicalTypeId::STRUCT) {
                auto struct_entries = duckdb::StructValue::GetChildren(entry);
                auto struct_types = duckdb::StructType::GetChildTypes(entry.type());
                
                std::string key, value;
                for (size_t j = 0; j < struct_types.size() && j < struct_entries.size(); j++) {
                    if (struct_types[j].first == "key") {
                        key = struct_entries[j].ToString();
                    } else if (struct_types[j].first == "value") {
                        value = struct_entries[j].ToString();
                    }
                }
                
                if (!key.empty() && !value.empty()) {
                    input_params[key] = value;
                    ERPL_TRACE_DEBUG("DATASPHERE_RELATIONAL_BIND", "Added input parameter: " + key + " = " + value);
                }
            }
        }
        
        return input_params;
    }
    
    // Helper function to build the data URL
    std::string BuildDataUrl(const std::string& space_id, const std::string& asset_id, 
                            const std::string& tenant, const std::string& data_center) {
        if (space_id.find("http") == 0) {
            // space_id is already a full URL
            std::string data_url = space_id;
            if (!space_id.empty() && space_id.back() != '/' && !asset_id.empty()) {
                // Check if URL already ends with asset_id to avoid duplication
                if (space_id.length() <= asset_id.length() || 
                    space_id.substr(space_id.length() - asset_id.length()) != asset_id) {
                    data_url += "/" + asset_id;
                }
            }
            // For Datasphere: ensure double asset segment (/{asset}/{asset}) when no params
            if (data_url.find("hcs.cloud.sap") != std::string::npos) {
                // If not already double, append second asset segment
                if (data_url.rfind("/" + asset_id) != data_url.length() - (asset_id.length() + 1)) {
                    data_url += "/" + asset_id;
                }
            }
            return data_url;
        } else {
            // Build URL from components
            auto base = DatasphereUrlBuilder::BuildRelationalUrl(tenant, data_center, space_id, asset_id);
            // For Datasphere: include double asset segment
            if (base.find("hcs.cloud.sap") != std::string::npos) {
                return base + "/" + asset_id;
            }
            return base;
        }
    }
    
    // Helper function to apply named parameters to bind data
    void ApplyNamedParameters(ODataReadBindData* read_bind, 
                            const duckdb::TableFunctionBindInput& input) {
        // Handle LIMIT and OFFSET
        if (input.named_parameters.find("top") != input.named_parameters.end()) {
            auto limit_value = input.named_parameters["top"].GetValue<duckdb::idx_t>();
            read_bind->PredicatePushdownHelper()->ConsumeLimit(limit_value);
            ERPL_TRACE_DEBUG("DATASPHERE_RELATIONAL_BIND", "Set limit to: " + std::to_string(limit_value));
        }
        
        if (input.named_parameters.find("skip") != input.named_parameters.end()) {
            auto offset_value = input.named_parameters["skip"].GetValue<duckdb::idx_t>();
            read_bind->PredicatePushdownHelper()->ConsumeOffset(offset_value);
            ERPL_TRACE_DEBUG("DATASPHERE_RELATIONAL_BIND", "Set offset to: " + std::to_string(offset_value));
        }
    }
}

static duckdb::unique_ptr<duckdb::FunctionData> DatasphereReadRelationalBind(duckdb::ClientContext &context, 
                                                                           duckdb::TableFunctionBindInput &input,
                                                                           duckdb::vector<duckdb::LogicalType> &return_types,
                                                                           duckdb::vector<std::string> &names) {
    ERPL_TRACE_DEBUG("DATASPHERE_RELATIONAL_BIND", "=== DATASPHERE_RELATIONAL_BIND CALLED ===");
    
    // Extract basic parameters
    auto space_id = input.inputs[0].GetValue<std::string>();
    auto asset_id = input.inputs[1].GetValue<std::string>();
    
    // Extract secret name
    std::string secret_name = "datasphere";
    if (input.inputs.size() > 2) {
        secret_name = input.inputs[2].GetValue<std::string>();
    } else if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters["secret"].GetValue<std::string>();
    }
    
    // Resolve authentication
    auto auth = ResolveDatasphereAuth(context, secret_name);
    ERPL_TRACE_INFO("DATASPHERE_RELATIONAL_BIND", "Using tenant: " + auth.tenant_name + 
                    ", data_center: " + auth.data_center + ", space_id: " + space_id + 
                    ", asset_id: " + asset_id);
    
    // Build data URL
    auto data_url = BuildDataUrl(space_id, asset_id, auth.tenant_name, auth.data_center);
    ERPL_TRACE_INFO("DATASPHERE_RELATIONAL_BIND", "Data URL: " + data_url);
    
    // Create OData bind data
    auto read_bind = ODataReadBindData::FromEntitySetRoot(data_url, auth.auth_params);
    
    // Extract and apply input parameters BEFORE metadata extraction
    if (input.named_parameters.find("params") != input.named_parameters.end()) {
        auto input_params = ExtractInputParameters(input.named_parameters["params"]);
        
        if (!input_params.empty()) {
            // Store parameters in bind data
            read_bind->SetInputParameters(input_params);
            ERPL_TRACE_INFO("DATASPHERE_RELATIONAL_BIND", "Stored " + std::to_string(input_params.size()) + " input parameters");
            
            // Pass parameters to OData client immediately for proper metadata handling
            auto odata_client = read_bind->GetODataClient();
            if (odata_client) {
                odata_client->SetInputParameters(input_params);
                ERPL_TRACE_INFO("DATASPHERE_RELATIONAL_BIND", "Passed input parameters to OData client");
            } else {
                ERPL_TRACE_ERROR("DATASPHERE_RELATIONAL_BIND", "Failed to get OData client");
            }
        }
    }
    
    // Apply other named parameters
    ApplyNamedParameters(read_bind.get(), input);
    
    // Get result schema
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

    ERPL_TRACE_DEBUG("DATASPHERE_RELATIONAL_INIT", "Initializing with " + std::to_string(column_ids.size()) + " columns");
    
    // Activate columns and add filters
    bind_data.ActivateColumns(column_ids);
    bind_data.AddFilters(input.filters);
    
    // Ensure input parameters are still available in OData client
    auto input_params = bind_data.GetInputParameters();
    if (!input_params.empty()) {
        auto odata_client = bind_data.GetODataClient();
        if (odata_client) {
            odata_client->SetInputParameters(input_params);
            ERPL_TRACE_INFO("DATASPHERE_RELATIONAL_INIT", "Re-applied " + std::to_string(input_params.size()) + " input parameters");
        }
    }
    
    // Update URL with predicate pushdown
    bind_data.UpdateUrlFromPredicatePushdown();

    return duckdb::make_uniq<duckdb::GlobalTableFunctionState>();
}

duckdb::TableFunctionSet CreateDatasphereReadRelationalFunction() {
    ERPL_TRACE_DEBUG("DATASPHERE_FUNCTION_REGISTRATION", "=== REGISTERING DATASPHERE_RELATIONAL FUNCTION ===");
    
    duckdb::TableFunctionSet set("datasphere_read_relational");
    
    // 2-parameter version (space_id, asset_id)
    duckdb::TableFunction relational_function_2_params(
        {duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR), 
         duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)},
        ODataReadScan, DatasphereReadRelationalBind, DatasphereReadRelationalTableInitGlobalState);
    relational_function_2_params.filter_pushdown = true;
    relational_function_2_params.projection_pushdown = true;
    relational_function_2_params.table_scan_progress = ODataReadTableProgress;
    relational_function_2_params.named_parameters["top"] = duckdb::LogicalType(duckdb::LogicalTypeId::UBIGINT);
    relational_function_2_params.named_parameters["skip"] = duckdb::LogicalType(duckdb::LogicalTypeId::UBIGINT);
    relational_function_2_params.named_parameters["params"] = duckdb::LogicalType::MAP(duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR), 
                                                                                    duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    relational_function_2_params.named_parameters["secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    set.AddFunction(relational_function_2_params);
    
    // 3-parameter version (space_id, asset_id, secret_name)
    duckdb::TableFunction relational_function_3_params(
        {duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR), 
         duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),
         duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)},
        ODataReadScan, DatasphereReadRelationalBind, DatasphereReadRelationalTableInitGlobalState);
    relational_function_3_params.filter_pushdown = true;
    relational_function_3_params.projection_pushdown = true;
    relational_function_3_params.table_scan_progress = [](duckdb::ClientContext &context,
                                                          const duckdb::FunctionData *bind_data,
                                                          const duckdb::GlobalTableFunctionState *gstate) -> double {
        if (!bind_data) return -1.0;
        auto odata_bind = static_cast<const ODataReadBindData *>(bind_data);
        return odata_bind ? odata_bind->GetProgressFraction() : -1.0;
    };
    relational_function_3_params.named_parameters["top"] = duckdb::LogicalType(duckdb::LogicalTypeId::UBIGINT);
    relational_function_3_params.named_parameters["skip"] = duckdb::LogicalType(duckdb::LogicalTypeId::UBIGINT);
    relational_function_3_params.named_parameters["params"] = duckdb::LogicalType::MAP(duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR), 
                                                                                    duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    set.AddFunction(relational_function_3_params);
    
    ERPL_TRACE_DEBUG("DATASPHERE_FUNCTION_REGISTRATION", "=== DATASPHERE_RELATIONAL FUNCTION REGISTRATION COMPLETED ===");
    return set;
}

} // namespace erpl_web
