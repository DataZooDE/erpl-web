#include "odp_odata_read_functions.hpp"
#include "odp_odata_read_bind_data.hpp"
#include "odata_read_functions.hpp"
#include "tracing.hpp"
#include "duckdb_argument_helper.hpp"

namespace erpl_web {

// ============================================================================
// ODP OData Read Function Implementation
// ============================================================================

duckdb::unique_ptr<duckdb::FunctionData> OdpODataReadBind(duckdb::ClientContext &context, 
                                                          duckdb::TableFunctionBindInput &input,
                                                          duckdb::vector<duckdb::LogicalType> &return_types,
                                                          duckdb::vector<std::string> &names) {
    ERPL_TRACE_DEBUG("ODP_ODATA_READ_BIND", "=== BINDING ODP_ODATA_READ FUNCTION ===");
    
    // Extract required parameter: entity_set_url
    if (input.inputs.empty()) {
        throw duckdb::InvalidInputException("odp_odata_read requires at least one parameter: entity_set_url");
    }
    
    std::string entity_set_url = input.inputs[0].ToString();
    ERPL_TRACE_INFO("ODP_ODATA_READ_BIND", "Entity set URL: " + entity_set_url);
    
    // Extract optional named parameters
    std::string secret_name = "";
    bool force_full_load = false;
    std::string import_delta_token = "";
    std::optional<uint32_t> max_page_size = std::nullopt;
    
    // Process named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "secret") {
            secret_name = kv.second.ToString();
            ERPL_TRACE_DEBUG("ODP_ODATA_READ_BIND", "Using secret: " + secret_name);
        } else if (kv.first == "force_full_load") {
            force_full_load = kv.second.GetValue<bool>();
            ERPL_TRACE_DEBUG("ODP_ODATA_READ_BIND", "Force full load: " + std::string(force_full_load ? "true" : "false"));
        } else if (kv.first == "import_delta_token") {
            import_delta_token = kv.second.ToString();
            ERPL_TRACE_DEBUG("ODP_ODATA_READ_BIND", "Import delta token: " + import_delta_token);
        } else if (kv.first == "max_page_size") {
            max_page_size = kv.second.GetValue<uint32_t>();
            ERPL_TRACE_DEBUG("ODP_ODATA_READ_BIND", "Max page size: " + std::to_string(max_page_size.value()));
        }
    }
    
    try {
        // Create ODP bind data
        auto odp_bind_data = duckdb::make_uniq<OdpODataReadBindData>(
            context, entity_set_url, secret_name, force_full_load, import_delta_token, max_page_size);
        
        // Initialize the ODP components
        odp_bind_data->Initialize();
        
        // Get the underlying OData bind data for schema information
        auto& odata_bind_data = odp_bind_data->GetODataBindData();
        
        // Set up return types and column names from the OData schema
        return_types = odata_bind_data.GetResultTypes();
        names = odata_bind_data.GetResultNames();
        
        ERPL_TRACE_INFO("ODP_ODATA_READ_BIND", duckdb::StringUtil::Format(
            "Bound ODP function with %zu columns", names.size()));
        
        return std::move(odp_bind_data);
        
    } catch (const duckdb::InvalidInputException& e) {
        // Re-throw DuckDB exceptions as-is (these are already well-formatted)
        throw;
    } catch (const std::runtime_error& e) {
        // Use shared error handling utility for HTTP errors
        ERPL_TRACE_ERROR("ODP_ODATA_READ_BIND", "Binding failed: " + std::string(e.what()));
        throw ODataErrorHandling::ConvertHttpErrorToUserFriendly(e, entity_set_url, "ODP OData", "sap_odp_odata_show()");
    } catch (const std::exception& e) {
        // Handle other exceptions with context
        ERPL_TRACE_ERROR("ODP_ODATA_READ_BIND", "Binding failed: " + std::string(e.what()));
        throw duckdb::InvalidInputException("Failed to bind ODP OData function for URL: " + entity_set_url + 
            ". Error: " + std::string(e.what()));
    }
}

void OdpODataReadScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
    auto &bind_data = data.bind_data->CastNoConst<OdpODataReadBindData>();
    
    ERPL_TRACE_DEBUG("ODP_ODATA_READ_SCAN", "Starting ODP scan operation");
    
    try {
        // Check if we have more results using ODP logic
        if (!bind_data.HasMoreResults()) {
            ERPL_TRACE_DEBUG("ODP_ODATA_READ_SCAN", "No more results available");
            return;
        }
        
        // Use the ODP bind data's fetch method which handles delta replication
        ERPL_TRACE_DEBUG("ODP_ODATA_READ_SCAN", "Fetching next result set via ODP logic");
        unsigned int rows_fetched = bind_data.FetchNextResult(output);
        
        ERPL_TRACE_INFO("ODP_ODATA_READ_SCAN", duckdb::StringUtil::Format("Fetched %d rows", rows_fetched));
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_ODATA_READ_SCAN", "Scan failed: " + std::string(e.what()));
        throw;
    }
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> OdpODataReadInitGlobalState(duckdb::ClientContext &context,
                                                                                 duckdb::TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->CastNoConst<OdpODataReadBindData>();
    auto column_ids = input.column_ids;
    
    // Delegate to the underlying OData bind data for initialization
    auto& odata_bind_data = bind_data.GetODataBindData();
    
    odata_bind_data.ActivateColumns(column_ids);
    odata_bind_data.AddFilters(input.filters);
    odata_bind_data.UpdateUrlFromPredicatePushdown();
    
    // Prefetch first page after URL is finalized
    odata_bind_data.PrefetchFirstPage();
    
    return duckdb::make_uniq<duckdb::GlobalTableFunctionState>();
}

double OdpODataReadProgress(duckdb::ClientContext &context, const duckdb::FunctionData *func_data,
                           const duckdb::GlobalTableFunctionState *) {
    auto &bind_data = func_data->CastNoConst<OdpODataReadBindData>();
    auto& odata_bind_data = bind_data.GetODataBindData();
    return odata_bind_data.GetProgressFraction();
}

// ============================================================================
// Function Registration
// ============================================================================

duckdb::TableFunctionSet CreateOdpODataReadFunction() {
    ERPL_TRACE_DEBUG("ODP_FUNCTION_REGISTRATION", "=== REGISTERING ODP_ODATA_READ FUNCTION ===");
    
    duckdb::TableFunctionSet function_set("odp_odata_read");
    
    // Main function: odp_odata_read(entity_set_url)
    duckdb::TableFunction odp_read_function(
        {duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)},  // entity_set_url parameter
        OdpODataReadScan, 
        OdpODataReadBind,
        OdpODataReadInitGlobalState
    );
    
    // Enable pushdown capabilities
    odp_read_function.filter_pushdown = true;
    odp_read_function.projection_pushdown = true;
    odp_read_function.table_scan_progress = OdpODataReadProgress;
    
    // Add named parameters for ODP-specific functionality
    odp_read_function.named_parameters["secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    odp_read_function.named_parameters["force_full_load"] = duckdb::LogicalType(duckdb::LogicalTypeId::BOOLEAN);
    odp_read_function.named_parameters["import_delta_token"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    odp_read_function.named_parameters["max_page_size"] = duckdb::LogicalType(duckdb::LogicalTypeId::UINTEGER);
    
    function_set.AddFunction(odp_read_function);
    
    ERPL_TRACE_INFO("ODP_FUNCTION_REGISTRATION", "ODP_ODATA_READ function registered successfully");
    return function_set;
}

} // namespace erpl_web
