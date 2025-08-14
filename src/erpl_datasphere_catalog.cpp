#include "erpl_datasphere_catalog.hpp"
#include "erpl_datasphere_client.hpp"
#include "erpl_odata_client.hpp"
#include "erpl_odata_content.hpp"
#include "erpl_oauth2_flow_v2.hpp"
#include "erpl_datasphere_secret.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/secret/secret.hpp"
#include <algorithm>

namespace erpl_web {

// DatasphereShowBindData implementation - now using standard OData pipeline
DatasphereShowBindData::DatasphereShowBindData(std::shared_ptr<ODataEntitySetClient> odata_client)
    : ODataReadBindData(odata_client)
    , resource_type("spaces") // Default to spaces
    , space_id("")
{
}

std::vector<std::string> DatasphereShowBindData::GetResultNames(bool all_columns) {
    // Return Datasphere-specific column names
    return {"name", "label", "description", "created_at", "modified_at"};
}

std::vector<duckdb::LogicalType> DatasphereShowBindData::GetResultTypes(bool all_columns) {
    // Return Datasphere-specific column types
    return {
        duckdb::LogicalType::VARCHAR, 
        duckdb::LogicalType::VARCHAR, 
        duckdb::LogicalType::VARCHAR, 
        duckdb::LogicalType::VARCHAR, 
        duckdb::LogicalType::VARCHAR
    };
}

// DatasphereDescribeBindData implementation
DatasphereDescribeBindData::DatasphereDescribeBindData(std::shared_ptr<ODataServiceClient> catalog_client, 
                                                       const std::string& resource_type,
                                                       const std::string& resource_id)
    : catalog_client(catalog_client)
    , resource_type(resource_type)
    , resource_id(resource_id)
{
}

void DatasphereDescribeBindData::LoadResourceDetails(duckdb::ClientContext &context) {
    try {
        // Get OAuth2 token from secret management system
        std::string access_token;
        OAuth2Config config;
        
        // Try to get the datasphere secret from the context
        auto &secret_manager = duckdb::SecretManager::Get(context);
        auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
        
        // Look for existing datasphere secret
        std::unique_ptr<duckdb::SecretEntry> secret_entry;
        try {
            secret_entry = secret_manager.GetSecretByName(transaction, "datasphere");
        } catch (...) {
            // Secret doesn't exist, we'll create it after OAuth2 flow
            secret_entry = nullptr;
        }
        
        if (secret_entry) {
            // Try to get a valid token from the existing secret
            auto kv_secret = dynamic_cast<const duckdb::KeyValueSecret*>(secret_entry->secret.get());
            if (kv_secret) {
                if (DatasphereTokenManager::IsTokenValid(kv_secret)) {
                    ERPL_TRACE_INFO("DATASPHERE_CATALOG", "Using cached valid token");
                    access_token = DatasphereTokenManager::GetToken(context, kv_secret);
                    
                    // Extract config from secret for URL construction
                    auto tenant_val = kv_secret->secret_map.find("tenant_name");
                    auto data_center_val = kv_secret->secret_map.find("data_center");
                    if (tenant_val != kv_secret->secret_map.end() && data_center_val != kv_secret->secret_map.end()) {
                        config.tenant_name = tenant_val->second.ToString();
                        config.data_center = data_center_val->second.ToString();
                    }
                } else {
                    ERPL_TRACE_INFO("DATASPHERE_CATALOG", "Cached token expired, refreshing");
                    DatasphereTokenManager::RefreshTokens(context, kv_secret);
                    access_token = DatasphereTokenManager::GetToken(context, kv_secret);
                    
                    // Extract config from secret for URL construction
                    auto tenant_val = kv_secret->secret_map.find("tenant_name");
                    auto data_center_val = kv_secret->secret_map.find("data_center");
                    if (tenant_val != kv_secret->secret_map.end() && data_center_val != kv_secret->secret_map.end()) {
                        config.tenant_name = tenant_val->second.ToString();
                        config.data_center = data_center_val->second.ToString();
                    }
                }
            }
        }
        
        // If we still don't have a token, perform the interactive OAuth2 flow
        if (access_token.empty()) {
            ERPL_TRACE_INFO("DATASPHERE_CATALOG", "No valid cached token, performing interactive OAuth2 flow");
            
            // Use the new clean OAuth2 flow
            config.tenant_name = "ak-datasphere-prd";
            config.data_center = "eu10";
            config.client_id = "sb-3ba2fc19-884e-47fe-a00f-7725136b6eae!b493973|client!b3650";
            config.client_secret = "f969011c-4926-4051-ac2a-c34d971ec4c9$Fq8IR4LMIJH-B4qDOXnTn1GjSSqs1UvR7T5szVkhT88=";
            config.scope = "default";
            config.redirect_uri = "http://localhost:65000";
            config.authorization_flow = GrantType::authorization_code;
            config.custom_client = true;
            
            // Get OAuth2 token using new clean architecture
            OAuth2FlowV2 oauth2_flow;
            auto tokens = oauth2_flow.ExecuteFlow(config);
            access_token = tokens.access_token;
            
            // Store the tokens in a new secret
            if (secret_entry) {
                // Update existing secret
                auto kv_secret = dynamic_cast<const duckdb::KeyValueSecret*>(secret_entry->secret.get());
                if (kv_secret) {
                    DatasphereTokenManager token_manager;
                    token_manager.UpdateSecretWithTokens(context, kv_secret, tokens);
                }
            } else {
                // Create new secret with tokens
                const duckdb::vector<duckdb::string> prefix_paths = {};
                duckdb::KeyValueSecret new_secret(prefix_paths, "datasphere", "oauth2", "datasphere");
                new_secret.secret_map["access_token"] = duckdb::Value(tokens.access_token);
                new_secret.secret_map["refresh_token"] = duckdb::Value(tokens.refresh_token);
                new_secret.secret_map["expires_at"] = duckdb::Value(std::to_string(tokens.expires_after));
                new_secret.secret_map["token_type"] = duckdb::Value(tokens.token_type);
                new_secret.secret_map["scope"] = duckdb::Value(tokens.scope);
                new_secret.secret_map["client_id"] = duckdb::Value(config.client_id);
                new_secret.secret_map["client_secret"] = duckdb::Value(config.client_secret);
                new_secret.secret_map["tenant_name"] = duckdb::Value(config.tenant_name);
                new_secret.secret_map["data_center"] = duckdb::Value(config.data_center);
                
                secret_manager.RegisterSecret(transaction, duckdb::make_uniq<duckdb::KeyValueSecret>(new_secret),
                                            duckdb::OnCreateConflict::REPLACE_ON_CONFLICT,
                                            duckdb::SecretPersistType::PERSISTENT,
                                            "local_file");
            }
        }
        
        // Create HTTP client
        auto http_client = std::make_shared<HttpClient>();
        
        // Create auth params with OAuth2 token
        auto auth_params = std::make_shared<HttpAuthParams>();
        auth_params->bearer_token = access_token;
        
        // Construct resource URL based on type
        std::string resource_url;
        if (resource_type == "space") {
            resource_url = "https://" + config.tenant_name + "." + config.data_center + ".hcs.cloud.sap/api/v1/dwc/catalog/spaces('" + resource_id + "')";
        } else if (resource_type == "asset") {
            resource_url = "https://" + config.tenant_name + "." + config.data_center + ".hcs.cloud.sap/api/v1/dwc/catalog/spaces('" + space_id + "')/assets('" + resource_id + "')";
        }
        
        ERPL_TRACE_DEBUG("DATASPHERE_CATALOG", "Calling resource endpoint: " + resource_url);
        
        // Create resource client with auth params
        auto resource_client = std::make_shared<ODataEntitySetClient>(
            http_client, 
            HttpUrl(resource_url),
            auth_params
        );
        
        auto resource_response = resource_client->Get();
        if (resource_response) {
            ERPL_TRACE_DEBUG("DATASPHERE_CATALOG", "Successfully got resource response");
            
            // Use the actual Datasphere resource schema
            std::vector<std::string> result_names = {"name", "label", "description", "created_at", "modified_at"};
            std::vector<duckdb::LogicalType> result_types = {
                duckdb::LogicalType::VARCHAR, 
                duckdb::LogicalType::VARCHAR, 
                duckdb::LogicalType::VARCHAR, 
                duckdb::LogicalType::VARCHAR, 
                duckdb::LogicalType::VARCHAR
            };
            
            ERPL_TRACE_DEBUG("DATASPHERE_CATALOG", "Using Datasphere resource schema: name, label, description, created_at, modified_at");
            
            // Convert response to rows using the actual schema
            try {
                ERPL_TRACE_DEBUG("DATASPHERE_CATALOG", "About to call ToRows with " + std::to_string(result_names.size()) + " columns");
                
                resource_data = resource_response->ToRows(result_names, result_types);
                ERPL_TRACE_DEBUG("DATASPHERE_CATALOG", "Converted to " + std::to_string(resource_data.size()) + " rows");
                
                // Log the first row data for debugging
                if (!resource_data.empty() && !resource_data[0].empty()) {
                    ERPL_TRACE_DEBUG("DATASPHERE_CATALOG", "First row has " + std::to_string(resource_data[0].size()) + " columns");
                    for (size_t i = 0; i < resource_data[0].size(); ++i) {
                        ERPL_TRACE_DEBUG("DATASPHERE_CATALOG", "Column " + std::to_string(i) + " value: " + resource_data[0][i].ToString());
                    }
                }
            } catch (const std::exception& e) {
                ERPL_TRACE_ERROR("DATASPHERE_CATALOG", "Failed to convert response to rows: " + std::string(e.what()));
                throw; // Re-throw to be caught by outer try-catch
            }
        } else {
            ERPL_TRACE_ERROR("DATASPHERE_CATALOG", "Failed to get resource response");
        }
        
        // If no data found, provide fallback
        if (resource_data.empty()) {
            resource_data = {
                {"error", "Error loading resource details: No data found"}
            };
        }
        
    } catch (const std::exception& e) {
        // Log error and provide fallback data
        resource_data = {
            {"error", "Error loading resource details: " + std::string(e.what())}
        };
    }
}

std::vector<std::string> DatasphereDescribeBindData::GetColumnNames() const {
    if (resource_type == "space") {
        return {"name", "label", "description", "created_at", "modified_at"};
    } else if (resource_type == "asset") {
        return {"name", "spaceName", "label", "description", "created_at", "modified_at"};
    }
    return {};
}

std::vector<duckdb::LogicalType> DatasphereDescribeBindData::GetColumnTypes() const {
    if (resource_type == "space") {
        return {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, 
                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, 
                duckdb::LogicalType::VARCHAR};
    } else if (resource_type == "asset") {
        return {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, 
                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, 
                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
    }
    return {};
}

// Table function implementations
static duckdb::unique_ptr<duckdb::FunctionData> DatasphereShowSpacesBind(duckdb::ClientContext &context, 
                                                                        duckdb::TableFunctionBindInput &input,
                                                                        duckdb::vector<duckdb::LogicalType> &return_types,
                                                                        duckdb::vector<std::string> &names) {
    // Get OAuth2 token and create OData client for spaces endpoint
    std::string access_token;
    OAuth2Config config;
    
    // Try to get the datasphere secret from the context
    auto &secret_manager = duckdb::SecretManager::Get(context);
    auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
    
    // Look for existing datasphere secret
    std::unique_ptr<duckdb::SecretEntry> secret_entry;
    try {
        secret_entry = secret_manager.GetSecretByName(transaction, "datasphere");
    } catch (...) {
        // Secret doesn't exist, we'll create it after OAuth2 flow
        secret_entry = nullptr;
    }
    
    if (secret_entry) {
        // Try to get a valid token from the existing secret
        auto kv_secret = dynamic_cast<const duckdb::KeyValueSecret*>(secret_entry->secret.get());
        if (kv_secret) {
            if (DatasphereTokenManager::IsTokenValid(kv_secret)) {
                ERPL_TRACE_INFO("DATASPHERE_CATALOG", "Using cached valid token");
                access_token = DatasphereTokenManager::GetToken(context, kv_secret);
                
                // Extract config from secret for URL construction
                auto tenant_val = kv_secret->secret_map.find("tenant_name");
                auto data_center_val = kv_secret->secret_map.find("data_center");
                if (tenant_val != kv_secret->secret_map.end() && data_center_val != kv_secret->secret_map.end()) {
                    config.tenant_name = tenant_val->second.ToString();
                    config.data_center = data_center_val->second.ToString();
                }
            } else {
                ERPL_TRACE_INFO("DATASPHERE_CATALOG", "Cached token expired, refreshing");
                DatasphereTokenManager::RefreshTokens(context, kv_secret);
                access_token = DatasphereTokenManager::GetToken(context, kv_secret);
                
                // Extract config from secret for URL construction
                auto tenant_val = kv_secret->secret_map.find("tenant_name");
                auto data_center_val = kv_secret->secret_map.find("data_center");
                if (tenant_val != kv_secret->secret_map.end() && data_center_val != kv_secret->secret_map.end()) {
                    config.tenant_name = tenant_val->second.ToString();
                    config.data_center = data_center_val->second.ToString();
                }
            }
        }
    }
    
    // If we still don't have a token, perform the interactive OAuth2 flow
    if (access_token.empty()) {
        ERPL_TRACE_INFO("DATASPHERE_CATALOG", "No valid cached token, performing interactive OAuth2 flow");
        
        // Use the new clean OAuth2 flow
        config.tenant_name = "ak-datasphere-prd";
        config.data_center = "eu10";
        config.client_id = "sb-3ba2fc19-884e-47fe-a00f-7725136b6eae!b493973|client!b3650";
        config.client_secret = "f969011c-4926-4051-ac2a-c34d971ec4c9$Fq8IR4LMIJH-B4qDOXnTn1GjSSqs1UvR7T5szVkhT88=";
        config.scope = "default";
        config.redirect_uri = "http://localhost:65000";
        config.authorization_flow = GrantType::authorization_code;
        config.custom_client = true;
        
        // Get OAuth2 token using new clean architecture
        OAuth2FlowV2 oauth2_flow;
        auto tokens = oauth2_flow.ExecuteFlow(config);
        access_token = tokens.access_token;
        
        // Store the tokens in a new secret
        if (secret_entry) {
            // Update existing secret
            auto kv_secret = dynamic_cast<const duckdb::KeyValueSecret*>(secret_entry->secret.get());
            if (kv_secret) {
                DatasphereTokenManager token_manager;
                token_manager.UpdateSecretWithTokens(context, kv_secret, tokens);
            }
        } else {
            // Create new secret with tokens
            const duckdb::vector<duckdb::string> prefix_paths = {};
            duckdb::KeyValueSecret new_secret(prefix_paths, "datasphere", "oauth2", "datasphere");
            new_secret.secret_map["access_token"] = duckdb::Value(tokens.access_token);
            new_secret.secret_map["refresh_token"] = duckdb::Value(tokens.refresh_token);
            new_secret.secret_map["expires_at"] = duckdb::Value(std::to_string(tokens.expires_after));
            new_secret.secret_map["token_type"] = duckdb::Value(tokens.token_type);
            new_secret.secret_map["scope"] = duckdb::Value(tokens.scope);
            new_secret.secret_map["client_id"] = duckdb::Value(config.client_id);
            new_secret.secret_map["client_secret"] = duckdb::Value(config.client_secret);
            new_secret.secret_map["tenant_name"] = duckdb::Value(config.tenant_name);
            new_secret.secret_map["data_center"] = duckdb::Value(config.data_center);
            
            secret_manager.RegisterSecret(transaction, duckdb::make_uniq<duckdb::KeyValueSecret>(new_secret),
                                        duckdb::OnCreateConflict::REPLACE_ON_CONFLICT,
                                        duckdb::SecretPersistType::PERSISTENT,
                                        "local_file");
        }
    }
    
    // Create HTTP client and OData client for spaces endpoint
    auto http_client = std::make_shared<HttpClient>();
    auto auth_params = std::make_shared<HttpAuthParams>();
    auth_params->bearer_token = access_token;
    
    // Construct spaces URL directly
    std::string spaces_url = "https://" + config.tenant_name + "." + config.data_center + ".hcs.cloud.sap/api/v1/dwc/catalog/spaces";
    
    ERPL_TRACE_DEBUG("DATASPHERE_CATALOG", "Creating OData client for spaces endpoint: " + spaces_url);
    
    auto spaces_client = std::make_shared<ODataEntitySetClient>(
        http_client, 
        HttpUrl(spaces_url),
        auth_params
    );
    
    // Create bind data using the standard OData pipeline
    auto bind_data = duckdb::make_uniq<DatasphereShowBindData>(spaces_client);
    bind_data->resource_type = "spaces";
    
    // Get column names and types from the bind data
    names = bind_data->GetResultNames();
    return_types = bind_data->GetResultTypes();
    
    ERPL_TRACE_INFO("DATASPHERE_CATALOG", "Bound function with " + std::to_string(return_types.size()) + " columns");
    
    return std::move(bind_data);
}

static duckdb::unique_ptr<duckdb::FunctionData> DatasphereShowAssetsBind(duckdb::ClientContext &context, 
                                                                        duckdb::TableFunctionBindInput &input,
                                                                        duckdb::vector<duckdb::LogicalType> &return_types,
                                                                        duckdb::vector<std::string> &names) {
    // Similar implementation for assets - create OData client for assets endpoint
    // This would follow the same pattern as spaces but with assets URL
    
    // For now, return a placeholder implementation
    return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, 
                    duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
    names = {"name", "spaceName", "label", "description", "created_at", "modified_at"};
    
    // Create real catalog client with OAuth2 authentication
    auto http_client = std::make_shared<HttpClient>();
    auto catalog_url = HttpUrl("https://ak-datasphere-prd.eu10.hcs.cloud.sap/api/v1/dwc/catalog");
    auto catalog_client = std::make_shared<ODataServiceClient>(http_client, catalog_url);
    
    auto bind_data = duckdb::make_uniq<DatasphereShowBindData>(nullptr); // Will be implemented later
    
    // Set space_id if provided for filtering
    if (input.inputs.size() > 0) {
        bind_data->space_id = input.inputs[0].GetValue<std::string>();
    }
    
    return std::move(bind_data);
}

static duckdb::unique_ptr<duckdb::FunctionData> DatasphereDescribeSpaceBind(duckdb::ClientContext &context, 
                                                                           duckdb::TableFunctionBindInput &input,
                                                                           duckdb::vector<duckdb::LogicalType> &return_types,
                                                                           duckdb::vector<std::string> &names) {
    // Extract space_id from input
    auto space_id = input.inputs[0].GetValue<std::string>();
    
    // Extract optional secret from named parameters
    std::string secret_name = "default";
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters["secret"].GetValue<std::string>();
    }
    
    // Set return types and names
    return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, 
                    duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
    names = {"name", "label", "description", "created_at", "modified_at"};
    
    // Create real catalog client with OAuth2 authentication
    auto http_client = std::make_shared<HttpClient>();
    auto catalog_url = HttpUrl("https://ak-datasphere-prd.eu10.hcs.cloud.sap/api/v1/dwc/catalog");
    auto catalog_client = std::make_shared<ODataServiceClient>(http_client, catalog_url);
    
    auto bind_data = duckdb::make_uniq<DatasphereDescribeBindData>(catalog_client, "space", space_id);
    
    // Data will be loaded via OAuth2 authentication in LoadResourceDetails()
    
    return std::move(bind_data);
}

static duckdb::unique_ptr<duckdb::FunctionData> DatasphereDescribeAssetBind(duckdb::ClientContext &context, 
                                                                           duckdb::TableFunctionBindInput &input,
                                                                           duckdb::vector<duckdb::LogicalType> &return_types,
                                                                           duckdb::vector<std::string> &names) {
    // Extract space_id and asset_id from input
    auto space_id = input.inputs[0].GetValue<std::string>();
    auto asset_id = input.inputs[1].GetValue<std::string>();
    
    // Extract optional secret from named parameters
    std::string secret_name = "default";
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters["secret"].GetValue<std::string>();
    }
    
    // Set return types and names
    return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, 
                    duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
    names = {"name", "spaceName", "label", "description", "created_at", "modified_at"};
    
    // Create real catalog client with OAuth2 authentication
    auto http_client = std::make_shared<HttpClient>();
    auto catalog_url = HttpUrl("https://ak-datasphere-prd.eu10.hcs.cloud.sap/api/v1/dwc/catalog");
    auto catalog_client = std::make_shared<ODataServiceClient>(http_client, catalog_url);
    
    auto bind_data = duckdb::make_uniq<DatasphereDescribeBindData>(catalog_client, "asset", asset_id);
    
    // Store space_id for later use in LoadResourceDetails
    bind_data->space_id = space_id;
    
    return std::move(bind_data);
}

// Table function scan implementations
static void DatasphereShowSpacesFunction(duckdb::ClientContext &context, 
                                        duckdb::TableFunctionInput &data_p, 
                                        duckdb::DataChunk &output) {
    auto &bind_data = (DatasphereShowBindData &)*data_p.bind_data;
    
    // Use the standard OData pipeline - delegate to ODataReadBindData
    if (!bind_data.HasMoreResults()) {
        ERPL_TRACE_DEBUG("DATASPHERE_CATALOG", "No more results available");
        return;
    }
    
    ERPL_TRACE_DEBUG("DATASPHERE_CATALOG", "Fetching next result set using standard OData pipeline");
    auto rows_fetched = bind_data.FetchNextResult(output);
    ERPL_TRACE_INFO("DATASPHERE_CATALOG", "Fetched " + std::to_string(rows_fetched) + " rows");
}

static void DatasphereShowAssetsFunction(duckdb::ClientContext &context, 
                                        duckdb::TableFunctionInput &data_p, 
                                        duckdb::DataChunk &output) {
    auto &bind_data = (DatasphereShowBindData &)*data_p.bind_data;
    
    // Use the standard OData pipeline - delegate to ODataReadBindData
    if (!bind_data.HasMoreResults()) {
        ERPL_TRACE_DEBUG("DATASPHERE_CATALOG", "No more results available");
        return;
    }
    
    ERPL_TRACE_DEBUG("DATASPHERE_CATALOG", "Fetching next result set using standard OData pipeline");
    auto rows_fetched = bind_data.FetchNextResult(output);
    ERPL_TRACE_INFO("DATASPHERE_CATALOG", "Fetched " + std::to_string(rows_fetched) + " rows");
}

static void DatasphereDescribeSpaceFunction(duckdb::ClientContext &context, 
                                           duckdb::TableFunctionInput &data_p, 
                                           duckdb::DataChunk &output) {
    auto &bind_data = (DatasphereDescribeBindData &)*data_p.bind_data;
    
    if (output.GetCapacity() == 0) {
        return;
    }
    
    // Load resource details if not already loaded
    if (bind_data.resource_data.empty()) {
        ERPL_TRACE_INFO("DATASPHERE_CATALOG", "Loading space details");
        bind_data.LoadResourceDetails(context);
        ERPL_TRACE_INFO("DATASPHERE_CATALOG", "Loaded space details");
    }
    
    // Fill output with space details
    auto row_count = std::min(static_cast<size_t>(output.GetCapacity()), bind_data.resource_data.size());
    output.SetCardinality(row_count);
    
    for (size_t i = 0; i < row_count; i++) {
        for (size_t j = 0; j < output.ColumnCount() && j < bind_data.resource_data[i].size(); j++) {
            output.SetValue(j, i, bind_data.resource_data[i][j]);
        }
    }
}

static void DatasphereDescribeAssetFunction(duckdb::ClientContext &context, 
                                           duckdb::TableFunctionInput &data_p, 
                                           duckdb::DataChunk &output) {
    auto &bind_data = (DatasphereDescribeBindData &)*data_p.bind_data;
    
    if (output.GetCapacity() == 0) {
        return;
    }
    
    // Load resource details if not already loaded
    if (bind_data.resource_data.empty()) {
        ERPL_TRACE_INFO("DATASPHERE_CATALOG", "Loading asset details");
        bind_data.LoadResourceDetails(context);
        ERPL_TRACE_INFO("DATASPHERE_CATALOG", "Loaded asset details");
    }
    
    // Fill output with asset details
    auto row_count = std::min(static_cast<size_t>(output.GetCapacity()), bind_data.resource_data.size());
    output.SetCardinality(row_count);
    
    for (size_t i = 0; i < row_count; i++) {
        for (size_t j = 0; j < output.ColumnCount() && j < bind_data.resource_data[i].size(); j++) {
            output.SetValue(j, i, bind_data.resource_data[i][j]);
        }
    }
}

// Table function creation functions
duckdb::TableFunctionSet CreateDatasphereShowSpacesFunction() {
    duckdb::TableFunctionSet function_set("datasphere_show_spaces");
    
    duckdb::TableFunction show_spaces({}, DatasphereShowSpacesFunction, DatasphereShowSpacesBind);
    
    function_set.AddFunction(show_spaces);
    return function_set;
}

duckdb::TableFunctionSet CreateDatasphereShowAssetsFunction() {
    duckdb::TableFunctionSet function_set("datasphere_show_assets");
    
    duckdb::TableFunction show_assets({duckdb::LogicalType::VARCHAR}, DatasphereShowAssetsFunction, DatasphereShowAssetsBind);
    
    function_set.AddFunction(show_assets);
    return function_set;
}

duckdb::TableFunctionSet CreateDatasphereDescribeSpaceFunction() {
    duckdb::TableFunctionSet function_set("datasphere_describe_space");
    
    duckdb::TableFunction describe_space({duckdb::LogicalType::VARCHAR}, DatasphereDescribeSpaceFunction, DatasphereDescribeSpaceBind);
    
    function_set.AddFunction(describe_space);
    return function_set;
}

duckdb::TableFunctionSet CreateDatasphereDescribeAssetFunction() {
    duckdb::TableFunctionSet function_set("datasphere_describe_asset");
    
    duckdb::TableFunction describe_asset({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR}, 
                                        DatasphereDescribeAssetFunction, DatasphereDescribeAssetBind);
    
    function_set.AddFunction(describe_asset);
    return function_set;
}

} // namespace erpl_web
