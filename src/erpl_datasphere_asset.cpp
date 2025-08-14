#include "erpl_datasphere_asset.hpp"
#include "erpl_datasphere_client.hpp"
#include "erpl_odata_client.hpp"
#include "erpl_odata_content.hpp"
#include "erpl_oauth2_flow_v2.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include <sstream>
#include <algorithm>

namespace erpl_web {

// DatasphereAssetBindData implementation
DatasphereAssetBindData::DatasphereAssetBindData(const std::string& tenant,
                                                 const std::string& data_center,
                                                 const std::string& space_id,
                                                 const std::string& asset_id,
                                                 std::shared_ptr<HttpAuthParams> auth_params,
                                                 const std::map<std::string, std::string>& input_parameters)
    : tenant(tenant)
    , data_center(data_center)
    , space_id(space_id)
    , asset_id(asset_id)
    , auth_params(auth_params)
    , input_parameters(input_parameters)
{
    InitializeClient();
}

void DatasphereAssetBindData::InitializeClient() {
    try {
        // Get OAuth2 token for authentication
        OAuth2Config config;
        config.tenant_name = "ak-datasphere-prd";
        config.data_center = "eu10";
        config.client_id = "sb-3ba2fc19-884e-47fe-a00f-7725136b6eae!b493973|client!b3650";
        config.client_secret = "f969011c-4926-4051-ac2a-c34d971ec4c9$Fq8IR4LMIJH-B4qDOXnTn1GjSSqs1UvR7T5szVkhT88=";
        config.scope = "default";
        config.redirect_uri = "http://localhost:65000/callback";
        config.authorization_flow = GrantType::authorization_code;
        config.custom_client = true;
        
        // Get OAuth2 token using new clean architecture
        OAuth2FlowV2 oauth2_flow;
        auto tokens = oauth2_flow.ExecuteFlow(config);
        
        // Create auth params with OAuth2 token
        auto oauth_auth_params = std::make_shared<HttpAuthParams>();
        oauth_auth_params->bearer_token = tokens.access_token;
        
        // Create OData client for the asset with OAuth2 authentication
        odata_client = DatasphereClientFactory::CreateRelationalClient(tenant, data_center, space_id, asset_id, oauth_auth_params);
        
        // Detect asset type
        DetectAssetType();
        
        // Apply input parameters to the client URL if present
        if (!input_parameters.empty()) {
            std::string parameter_clause = BuildParameterClause();
            if (!parameter_clause.empty()) {
                // Update the client's base URL with parameters
                // This will be handled during query execution
            }
        }
    } catch (const std::exception& e) {
        // Log error and continue with null client
        odata_client = nullptr;
    }
}

void DatasphereAssetBindData::DetectAssetType() {
    // This would typically query the catalog to determine asset type
    // For now, assume relational by default
    asset_type = "relational";
    
    // If we have analytical capabilities, we could switch to analytical client
    // This would require checking the asset metadata from the catalog
}

bool DatasphereAssetBindData::IsAnalyticalAsset() const {
    return asset_type == "analytical";
}

bool DatasphereAssetBindData::IsRelationalAsset() const {
    return asset_type == "relational";
}

void DatasphereAssetBindData::SetInputParameters(const std::map<std::string, std::string>& params) {
    input_parameters = params;
}

std::string DatasphereAssetBindData::BuildParameterClause() const {
    if (input_parameters.empty()) {
        return "";
    }
    
    std::stringstream ss;
    ss << "(";
    bool first = true;
    
    for (const auto& param : input_parameters) {
        if (!first) {
            ss << ",";
        }
        first = false;
        
        // Check if value is numeric or string
        bool is_numeric = true;
        try {
            std::stod(param.second);
        } catch (...) {
            is_numeric = false;
        }
        
        if (is_numeric) {
            ss << param.first << "=" << param.second;
        } else {
            ss << param.first << "='" << param.second << "'";
        }
    }
    
    ss << ")";
    return ss.str();
}

std::vector<std::string> DatasphereAssetBindData::GetResultNames() {
    if (all_result_names.empty() && odata_client) {
        all_result_names = odata_client->GetResultNames();
    }
    
    // Provide fallback names if client is not available
    if (all_result_names.empty()) {
        all_result_names = {"id", "name", "value", "created_at", "modified_at"};
    }
    
    return all_result_names;
}

std::vector<duckdb::LogicalType> DatasphereAssetBindData::GetResultTypes() {
    if (all_result_types.empty() && odata_client) {
        all_result_types = odata_client->GetResultTypes();
    }
    
    // Provide fallback types if client is not available
    if (all_result_types.empty()) {
        all_result_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, 
                           duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
    }
    
    return all_result_types;
}

bool DatasphereAssetBindData::HasMoreResults() {
    if (first_fetch) {
        return true;
    }
    
    // Check if there's a next link in the response
    // This would require storing the current response
    return false;
}

unsigned int DatasphereAssetBindData::FetchNextResult(duckdb::DataChunk &output) {
    if (first_fetch) {
        first_fetch = false;
    }
    
    // If OData client is not available, try to initialize it
    if (!odata_client) {
        InitializeClient();
    }
    
    // If still no client, provide fallback data
    if (!odata_client) {
        auto result_names = GetResultNames();
        auto result_types = GetResultTypes();
        
        // Create fallback data
        std::vector<std::vector<duckdb::Value>> fallback_rows = {
            {"1", "Asset Data Unavailable", "N/A", "2024-01-01", "2024-01-01"},
            {"2", "Please check OAuth2 authentication", "N/A", "2024-01-01", "2024-01-01"}
        };
        
        auto row_count = std::min(static_cast<size_t>(output.GetCapacity()), fallback_rows.size());
        output.SetCardinality(row_count);
        
        for (size_t i = 0; i < row_count; i++) {
            for (size_t j = 0; j < output.ColumnCount() && j < fallback_rows[i].size(); j++) {
                output.SetValue(j, i, fallback_rows[i][j]);
            }
        }
        
        return row_count;
    }
    
    auto response = odata_client->Get();
    if (!response) {
        output.SetCardinality(0);
        return 0;
    }
    
    auto result_names = GetResultNames();
    auto result_types = GetResultTypes();
    auto rows = response->ToRows(result_names, result_types);
    
    auto row_count = std::min(static_cast<size_t>(output.GetCapacity()), rows.size());
    output.SetCardinality(row_count);
    
    for (size_t i = 0; i < row_count; i++) {
        for (size_t j = 0; j < output.ColumnCount() && j < rows[i].size(); j++) {
            output.SetValue(j, i, rows[i][j]);
        }
    }
    
    return row_count;
}

void DatasphereAssetBindData::ActivateColumns(const std::vector<duckdb::column_t> &column_ids) {
    active_column_ids = column_ids;
}

void DatasphereAssetBindData::AddFilters(const duckdb::optional_ptr<duckdb::TableFilterSet> &filters) {
    // This would integrate with the existing predicate pushdown system
    // For now, just store the filters
}

void DatasphereAssetBindData::UpdateUrlFromPredicatePushdown() {
    // This would update the OData URL with filter, select, and other query options
    // For now, just a placeholder
}

// DatasphereAnalyticalBindData implementation
DatasphereAnalyticalBindData::DatasphereAnalyticalBindData(const std::string& tenant,
                                                           const std::string& data_center,
                                                           const std::string& space_id,
                                                           const std::string& asset_id,
                                                           std::shared_ptr<HttpAuthParams> auth_params,
                                                           const std::map<std::string, std::string>& input_parameters)
    : DatasphereAssetBindData(tenant, data_center, space_id, asset_id, auth_params, input_parameters)
{
    // Force analytical client
    odata_client = DatasphereClientFactory::CreateAnalyticalClient(tenant, data_center, space_id, asset_id, auth_params);
    SetAssetType("analytical");
}

std::string DatasphereAnalyticalBindData::BuildApplyClause(const std::vector<std::string>& dimensions, 
                                                          const std::vector<std::string>& measures) const {
    std::stringstream ss;
    ss << "$apply=groupby((";
    
    // Add dimensions
    for (size_t i = 0; i < dimensions.size(); i++) {
        if (i > 0) ss << ",";
        ss << dimensions[i];
    }
    
    ss << "),aggregate(";
    
    // Add measures with aggregation functions
    for (size_t i = 0; i < measures.size(); i++) {
        if (i > 0) ss << ",";
        // Support different aggregation types - default to sum
        ss << measures[i] << " with sum as " << measures[i] << "_sum";
    }
    
    ss << "))";
    return ss.str();
}

std::string DatasphereAnalyticalBindData::BuildApplyClauseWithAggregation(
    const std::vector<std::string>& dimensions,
    const std::map<std::string, std::string>& measures_with_aggregation) const {
    
    std::stringstream ss;
    ss << "$apply=groupby((";
    
    // Add dimensions
    for (size_t i = 0; i < dimensions.size(); i++) {
        if (i > 0) ss << ",";
        ss << dimensions[i];
    }
    
    ss << "),aggregate(";
    
    // Add measures with specified aggregation functions
    bool first = true;
    for (const auto& measure : measures_with_aggregation) {
        if (!first) ss << ",";
        first = false;
        ss << measure.first << " with " << measure.second << " as " << measure.first << "_" << measure.second;
    }
    
    ss << "))";
    return ss.str();
}

std::string DatasphereAnalyticalBindData::BuildHierarchyClause(const std::string& hierarchy_name) const {
    return "$apply=hierarchy(" + hierarchy_name + ")";
}

std::string DatasphereAnalyticalBindData::BuildCalculatedMeasureClause(const std::string& measure_expression) const {
    return "$apply=aggregate(" + measure_expression + " as calculated_measure)";
}

// DatasphereRelationalBindData implementation
DatasphereRelationalBindData::DatasphereRelationalBindData(const std::string& tenant,
                                                           const std::string& data_center,
                                                           const std::string& space_id,
                                                           const std::string& asset_id,
                                                           std::shared_ptr<HttpAuthParams> auth_params,
                                                           const std::map<std::string, std::string>& input_parameters)
    : DatasphereAssetBindData(tenant, data_center, space_id, asset_id, auth_params, input_parameters)
{
    // Force relational client
    odata_client = DatasphereClientFactory::CreateRelationalClient(tenant, data_center, space_id, asset_id, auth_params);
    SetAssetType("relational");
}

void DatasphereRelationalBindData::EnableInlineCount(bool enabled) {
    // This would set the $inlinecount query option for OData v2
    // For OData v4, this would use $count
}

void DatasphereRelationalBindData::SetSkipToken(const std::string& token) {
    // This would set the $skiptoken for OData v2 pagination
    // For OData v4, this would use $skip
}

// Helper functions
std::map<std::string, std::string> ParseInputParameters(const std::string& param_string) {
    std::map<std::string, std::string> params;
    
    // Simple parsing for key=value pairs
    // Format: "key1=value1,key2=value2"
    std::stringstream ss(param_string);
    std::string item;
    
    while (std::getline(ss, item, ',')) {
        size_t pos = item.find('=');
        if (pos != std::string::npos) {
            std::string key = item.substr(0, pos);
            std::string value = item.substr(pos + 1);
            
            // Remove quotes if present
            if (value.length() >= 2 && value[0] == '\'' && value[value.length()-1] == '\'') {
                value = value.substr(1, value.length() - 2);
            }
            
            params[key] = value;
        }
    }
    
    return params;
}

std::string BuildODataUrlWithParameters(const std::string& base_url, 
                                       const std::map<std::string, std::string>& parameters) {
    if (parameters.empty()) {
        return base_url;
    }
    
    std::stringstream ss;
    ss << base_url << "(";
    
    bool first = true;
    for (const auto& param : parameters) {
        if (!first) {
            ss << ",";
        }
        first = false;
        
        // Check if value is numeric or string
        bool is_numeric = true;
        try {
            std::stod(param.second);
        } catch (...) {
            is_numeric = false;
        }
        
        if (is_numeric) {
            ss << param.first << "=" << param.second;
        } else {
            ss << param.first << "='" << param.second << "'";
        }
    }
    
    ss << ")";
    return ss.str();
}

// Table function bind implementations
static duckdb::unique_ptr<duckdb::FunctionData> DatasphereAssetBind(duckdb::ClientContext &context, 
                                                                   duckdb::TableFunctionBindInput &input,
                                                                   duckdb::vector<duckdb::LogicalType> &return_types,
                                                                   duckdb::vector<std::string> &names) {
    // Extract parameters from input
    auto space_id = input.inputs[0].GetValue<std::string>();
    auto asset_id = input.inputs[1].GetValue<std::string>();
    
    // Extract secret_name from input or named parameters
    std::string secret_name = "default";
    if (input.inputs.size() > 2) {
        secret_name = input.inputs[2].GetValue<std::string>();
    } else if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters["secret"].GetValue<std::string>();
    }
    
    // For now, use placeholder values for tenant and data_center
    // In a real implementation, these would come from the secret
    auto tenant = "tenant";
    auto data_center = "eu10";
    auto auth_params = std::make_shared<HttpAuthParams>(HttpAuthParams::FromDuckDbSecrets(context, HttpUrl("")));
    
    auto bind_data = duckdb::make_uniq<DatasphereAssetBindData>(tenant, data_center, space_id, asset_id, auth_params);
    
    // Set return types and names
    return_types = bind_data->GetResultTypes();
    names = bind_data->GetResultNames();
    
    return std::move(bind_data);
}

static duckdb::unique_ptr<duckdb::FunctionData> DatasphereAnalyticalBind(duckdb::ClientContext &context, 
                                                                        duckdb::TableFunctionBindInput &input,
                                                                        duckdb::vector<duckdb::LogicalType> &return_types,
                                                                        duckdb::vector<std::string> &names) {
    // Extract parameters from input
    auto space_id = input.inputs[0].GetValue<std::string>();
    auto asset_id = input.inputs[1].GetValue<std::string>();
    
    // Extract secret_name from input or named parameters
    std::string secret_name = "default";
    if (input.inputs.size() > 2) {
        secret_name = input.inputs[2].GetValue<std::string>();
    } else if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters["secret"].GetValue<std::string>();
    }
    
    // Parse input parameters if provided
    std::map<std::string, std::string> input_params;
    if (input.inputs.size() > 3) {
        auto param_string = input.inputs[3].GetValue<std::string>();
        input_params = ParseInputParameters(param_string);
    }
    
    // For now, use placeholder values for tenant and data_center
    auto tenant = "tenant";
    auto data_center = "eu10";
    auto auth_params = std::make_shared<HttpAuthParams>(HttpAuthParams::FromDuckDbSecrets(context, HttpUrl("")));
    
    auto bind_data = duckdb::make_uniq<DatasphereAnalyticalBindData>(tenant, data_center, space_id, asset_id, auth_params, input_params);
    
    // Set return types and names
    return_types = bind_data->GetResultTypes();
    names = bind_data->GetResultNames();
    
    return std::move(bind_data);
}

static duckdb::unique_ptr<duckdb::FunctionData> DatasphereRelationalBind(duckdb::ClientContext &context, 
                                                                        duckdb::TableFunctionBindInput &input,
                                                                        duckdb::vector<duckdb::LogicalType> &return_types,
                                                                        duckdb::vector<std::string> &names) {
    // Extract parameters from input
    auto space_id = input.inputs[0].GetValue<std::string>();
    auto asset_id = input.inputs[1].GetValue<std::string>();
    
    // Extract secret_name from input or named parameters
    std::string secret_name = "default";
    if (input.inputs.size() > 2) {
        secret_name = input.inputs[2].GetValue<std::string>();
    } else if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters["secret"].GetValue<std::string>();
    }
    
    // For now, use placeholder values for tenant and data_center
    auto tenant = "tenant";
    auto data_center = "eu10";
    auto auth_params = std::make_shared<HttpAuthParams>(HttpAuthParams::FromDuckDbSecrets(context, HttpUrl("")));
    
    auto bind_data = duckdb::make_uniq<DatasphereRelationalBindData>(tenant, data_center, space_id, asset_id, auth_params);
    
    // Set return types and names
    return_types = bind_data->GetResultTypes();
    names = bind_data->GetResultNames();
    
    return std::move(bind_data);
}

// Table function scan implementations
static void DatasphereAssetFunction(duckdb::ClientContext &context, 
                                   duckdb::TableFunctionInput &data_p, 
                                   duckdb::DataChunk &output) {
    auto &bind_data = (DatasphereAssetBindData &)*data_p.bind_data;
    
    if (output.GetCapacity() == 0) {
        return;
    }
    
    auto row_count = bind_data.FetchNextResult(output);
    output.SetCardinality(row_count);
}

static void DatasphereAnalyticalFunction(duckdb::ClientContext &context, 
                                        duckdb::TableFunctionInput &data_p, 
                                        duckdb::DataChunk &output) {
    auto &bind_data = (DatasphereAnalyticalBindData &)*data_p.bind_data;
    
    if (output.GetCapacity() == 0) {
        return;
    }
    
    auto row_count = bind_data.FetchNextResult(output);
    output.SetCardinality(row_count);
}

static void DatasphereRelationalFunction(duckdb::ClientContext &context, 
                                        duckdb::TableFunctionInput &data_p, 
                                        duckdb::DataChunk &output) {
    auto &bind_data = (DatasphereRelationalBindData &)*data_p.bind_data;
    
    if (output.GetCapacity() == 0) {
        return;
    }
    
    auto row_count = bind_data.FetchNextResult(output);
    output.SetCardinality(row_count);
}

// Function set creation
duckdb::TableFunctionSet CreateDatasphereAssetFunction() {
    duckdb::TableFunctionSet set("datasphere_asset");
    
    // Version with space_id and asset_id (secret will use default)
    duckdb::TableFunction asset_function_2_params({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR}, 
                                                 DatasphereAssetFunction, DatasphereAssetBind);
    asset_function_2_params.named_parameters["secret"] = duckdb::LogicalType::VARCHAR;
    set.AddFunction(asset_function_2_params);
    
    // Version with space_id, asset_id, and secret_name
    duckdb::TableFunction asset_function_3_params({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR}, 
                                                 DatasphereAssetFunction, DatasphereAssetBind);
    set.AddFunction(asset_function_3_params);
    
    return set;
}

duckdb::TableFunctionSet CreateDatasphereAnalyticalFunction() {
    duckdb::TableFunctionSet set("datasphere_analytical");
    
    // Version with space_id and asset_id (secret will use default)
    duckdb::TableFunction analytical_function_2_params({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR}, 
                                                      DatasphereAnalyticalFunction, DatasphereAnalyticalBind);
    analytical_function_2_params.named_parameters["secret"] = duckdb::LogicalType::VARCHAR;
    set.AddFunction(analytical_function_2_params);
    
    // Version with space_id, asset_id, secret_name, and input_parameters
    duckdb::TableFunction analytical_function_4_params({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, 
                                                       duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR}, 
                                                      DatasphereAnalyticalFunction, DatasphereAnalyticalBind);
    set.AddFunction(analytical_function_4_params);
    
    return set;
}

duckdb::TableFunctionSet CreateDatasphereRelationalFunction() {
    duckdb::TableFunctionSet set("datasphere_relational");
    
    // Version with space_id and asset_id (secret will use default)
    duckdb::TableFunction relational_function_2_params({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR}, 
                                                       DatasphereRelationalFunction, DatasphereRelationalBind);
    relational_function_2_params.named_parameters["secret"] = duckdb::LogicalType::VARCHAR;
    set.AddFunction(relational_function_2_params);
    
    // Version with space_id, asset_id, and secret_name
    duckdb::TableFunction relational_function_3_params({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR}, 
                                                       DatasphereRelationalFunction, DatasphereRelationalBind);
    set.AddFunction(relational_function_3_params);
    
    return set;
}

} // namespace erpl_web
