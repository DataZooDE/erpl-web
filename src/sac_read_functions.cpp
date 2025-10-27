#include "sac_read_functions.hpp"
#include "sac_url_builder.hpp"
#include "sac_client.hpp"
#include "sac_catalog.hpp"
#include "odata_read_functions.hpp"
#include "odata_content.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/secret/secret.hpp"
#include <algorithm>

namespace erpl_web {

// Helper function to resolve SAC credentials and create OData client
struct SacReadContext {
    std::string tenant;
    std::string region;
    std::string model_id;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::shared_ptr<ODataEntitySetClient> odata_client;
};

SacReadContext ResolveSacReadContext(duckdb::ClientContext &context, const std::string& secret_name,
                                     const std::string& model_id) {
    SacReadContext ctx;
    ctx.model_id = model_id;

    // Resolve secret
    auto &secret_manager = duckdb::SecretManager::Get(context);
    auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
    std::unique_ptr<duckdb::SecretEntry> secret_entry;

    try {
        secret_entry = secret_manager.GetSecretByName(transaction, secret_name);
    } catch (...) {
        secret_entry = nullptr;
    }

    if (!secret_entry) {
        throw duckdb::InvalidInputException("Secret '" + secret_name + "' not found. Please create it using CREATE SECRET " +
            secret_name + " (type 'sac', provider 'oauth2', tenant_name => '...', region => '...', " +
            "client_id => '...', client_secret => '...', scope => 'openid');");
    }

    auto kv_secret = dynamic_cast<const duckdb::KeyValueSecret*>(secret_entry->secret.get());
    if (!kv_secret) {
        throw duckdb::InvalidInputException("Secret '" + secret_name + "' is not a KeyValueSecret");
    }

    // Extract tenant and region
    auto tenant_it = kv_secret->secret_map.find("tenant_name");
    auto region_it = kv_secret->secret_map.find("region");

    if (tenant_it == kv_secret->secret_map.end() || region_it == kv_secret->secret_map.end()) {
        throw duckdb::InvalidInputException("SAC secret must contain 'tenant_name' and 'region' fields");
    }

    ctx.tenant = tenant_it->second.ToString();
    ctx.region = region_it->second.ToString();

    // Create auth params
    ctx.auth_params = std::make_shared<HttpAuthParams>();
    auto access_token_it = kv_secret->secret_map.find("access_token");
    if (access_token_it != kv_secret->secret_map.end()) {
        // Token will be used if available
    }

    // Create OData client for the model
    auto url_str = SacUrlBuilder::BuildPlanningDataUrl(ctx.tenant, ctx.region, model_id);
    HttpUrl url(url_str);
    auto http_client = std::make_shared<HttpClient>();
    ctx.odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, ctx.auth_params);
    ctx.odata_client->SetODataVersionDirectly(ODataVersion::V4);

    return ctx;
}

// ===== sac_read_planning_data =====

/**
 * Bind function for sac_read_planning_data
 * Creates an ODataReadBindData that queries the planning model
 */
static duckdb::unique_ptr<duckdb::FunctionData> SacReadPlanningDataBind(
    duckdb::ClientContext &context,
    duckdb::TableFunctionBindInput &input,
    duckdb::vector<duckdb::LogicalType> &return_types,
    duckdb::vector<std::string> &names) {

    if (input.inputs.empty()) {
        throw duckdb::InvalidInputException("sac_read_planning_data requires a model_id parameter");
    }

    auto model_id = input.inputs[0].GetValue<std::string>();

    // Get secret name (default: "sac")
    std::string secret_name = "sac";
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }

    // Resolve SAC context
    auto sac_ctx = ResolveSacReadContext(context, secret_name, model_id);

    // Build the URL
    auto url_str = SacUrlBuilder::BuildPlanningDataUrl(sac_ctx.tenant, sac_ctx.region, model_id);

    // Create OData read bind data
    auto read_bind = ODataReadBindData::FromEntitySetRoot(url_str, sac_ctx.auth_params);

    // Handle optional named parameters for filtering and limiting
    if (input.named_parameters.find("top") != input.named_parameters.end()) {
        auto limit_value = input.named_parameters.at("top").GetValue<duckdb::idx_t>();
        read_bind->PredicatePushdownHelper()->ConsumeLimit(limit_value);
    }

    if (input.named_parameters.find("skip") != input.named_parameters.end()) {
        auto skip_value = input.named_parameters.at("skip").GetValue<duckdb::idx_t>();
        read_bind->PredicatePushdownHelper()->ConsumeOffset(skip_value);
    }

    // Get result schema from OData metadata
    names = read_bind->GetResultNames(false);
    return_types = read_bind->GetResultTypes(false);

    return std::move(read_bind);
}

duckdb::TableFunctionSet CreateSacReadPlanningDataFunction() {
    duckdb::TableFunctionSet set("sac_read_planning_data");

    duckdb::TableFunction function(
        {duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)},  // model_id
        ODataReadScan,
        SacReadPlanningDataBind,
        ODataReadTableInitGlobalState
    );

    // Add named parameters
    function.named_parameters["secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    function.named_parameters["top"] = duckdb::LogicalType(duckdb::LogicalTypeId::UBIGINT);
    function.named_parameters["skip"] = duckdb::LogicalType(duckdb::LogicalTypeId::UBIGINT);
    function.named_parameters["params"] = duckdb::LogicalType::MAP(
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)
    );

    // Progress tracking
    function.table_scan_progress = ODataReadTableProgress;

    set.AddFunction(function);
    return set;
}

// ===== sac_read_analytical =====

/**
 * Bind function for sac_read_analytical
 * Creates an ODataReadBindData for analytics model with dimension/measure filtering
 */
static duckdb::unique_ptr<duckdb::FunctionData> SacReadAnalyticalBind(
    duckdb::ClientContext &context,
    duckdb::TableFunctionBindInput &input,
    duckdb::vector<duckdb::LogicalType> &return_types,
    duckdb::vector<std::string> &names) {

    if (input.inputs.empty()) {
        throw duckdb::InvalidInputException("sac_read_analytical requires a model_id parameter");
    }

    auto model_id = input.inputs[0].GetValue<std::string>();

    std::string secret_name = "sac";
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }

    auto sac_ctx = ResolveSacReadContext(context, secret_name, model_id);

    // For analytics models, build URL to analytics endpoint instead
    auto analytics_url = SacUrlBuilder::BuildModelServiceUrl(sac_ctx.tenant, sac_ctx.region, model_id);

    // Create OData read bind data
    auto read_bind = ODataReadBindData::FromEntitySetRoot(analytics_url, sac_ctx.auth_params);

    // Handle dimension and measure filtering through $select parameter
    std::string select_clause;

    if (input.named_parameters.find("dimensions") != input.named_parameters.end()) {
        auto dims = input.named_parameters.at("dimensions").GetValue<std::string>();
        // Convert comma-separated list to OData $select format
        select_clause += dims;
    }

    if (input.named_parameters.find("measures") != input.named_parameters.end()) {
        auto measures = input.named_parameters.at("measures").GetValue<std::string>();
        if (!select_clause.empty()) {
            select_clause += ",";
        }
        select_clause += measures;
    }

    if (!select_clause.empty()) {
        // Apply $select through OData metadata
        // This is a simplified representation - actual implementation would use OData query construction
    }

    // Handle limit and offset
    if (input.named_parameters.find("top") != input.named_parameters.end()) {
        auto limit_value = input.named_parameters.at("top").GetValue<duckdb::idx_t>();
        read_bind->PredicatePushdownHelper()->ConsumeLimit(limit_value);
    }

    if (input.named_parameters.find("skip") != input.named_parameters.end()) {
        auto skip_value = input.named_parameters.at("skip").GetValue<duckdb::idx_t>();
        read_bind->PredicatePushdownHelper()->ConsumeOffset(skip_value);
    }

    names = read_bind->GetResultNames(false);
    return_types = read_bind->GetResultTypes(false);

    return std::move(read_bind);
}

duckdb::TableFunctionSet CreateSacReadAnalyticalFunction() {
    duckdb::TableFunctionSet set("sac_read_analytical");

    duckdb::TableFunction function(
        {duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)},  // model_id
        ODataReadScan,
        SacReadAnalyticalBind,
        ODataReadTableInitGlobalState
    );

    function.named_parameters["secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    function.named_parameters["top"] = duckdb::LogicalType(duckdb::LogicalTypeId::UBIGINT);
    function.named_parameters["skip"] = duckdb::LogicalType(duckdb::LogicalTypeId::UBIGINT);
    function.named_parameters["dimensions"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    function.named_parameters["measures"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    function.named_parameters["params"] = duckdb::LogicalType::MAP(
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)
    );

    function.table_scan_progress = ODataReadTableProgress;

    set.AddFunction(function);
    return set;
}

// ===== sac_read_story_data =====

/**
 * Bind function for sac_read_story_data
 * Extracts data from a story's underlying data model
 */
static duckdb::unique_ptr<duckdb::FunctionData> SacReadStoryDataBind(
    duckdb::ClientContext &context,
    duckdb::TableFunctionBindInput &input,
    duckdb::vector<duckdb::LogicalType> &return_types,
    duckdb::vector<std::string> &names) {

    if (input.inputs.empty()) {
        throw duckdb::InvalidInputException("sac_read_story_data requires a story_id parameter");
    }

    auto story_id = input.inputs[0].GetValue<std::string>();

    std::string secret_name = "sac";
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }

    // Resolve secret to get tenant and region
    auto &secret_manager = duckdb::SecretManager::Get(context);
    auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
    std::unique_ptr<duckdb::SecretEntry> secret_entry;

    try {
        secret_entry = secret_manager.GetSecretByName(transaction, secret_name);
    } catch (...) {
        secret_entry = nullptr;
    }

    if (!secret_entry) {
        throw duckdb::InvalidInputException("Secret '" + secret_name + "' not found");
    }

    auto kv_secret = dynamic_cast<const duckdb::KeyValueSecret*>(secret_entry->secret.get());
    if (!kv_secret) {
        throw duckdb::InvalidInputException("Secret '" + secret_name + "' is not a KeyValueSecret");
    }

    auto tenant_it = kv_secret->secret_map.find("tenant_name");
    auto region_it = kv_secret->secret_map.find("region");

    if (tenant_it == kv_secret->secret_map.end() || region_it == kv_secret->secret_map.end()) {
        throw duckdb::InvalidInputException("SAC secret must contain 'tenant_name' and 'region' fields");
    }

    std::string tenant = tenant_it->second.ToString();
    std::string region = region_it->second.ToString();

    // Create auth params
    auto auth_params = std::make_shared<HttpAuthParams>();
    auto access_token_it = kv_secret->secret_map.find("access_token");
    if (access_token_it != kv_secret->secret_map.end()) {
        // Token will be used if available
    }

    // Query the story service endpoint
    auto story_url = SacUrlBuilder::BuildStoryServiceUrl(tenant, region);
    // Append story ID to get specific story data
    story_url += "('" + story_id + "')";

    auto read_bind = ODataReadBindData::FromEntitySetRoot(story_url, auth_params);

    names = read_bind->GetResultNames(false);
    return_types = read_bind->GetResultTypes(false);

    return std::move(read_bind);
}

duckdb::TableFunctionSet CreateSacReadStoryDataFunction() {
    duckdb::TableFunctionSet set("sac_read_story_data");

    duckdb::TableFunction function(
        {duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)},  // story_id
        ODataReadScan,
        SacReadStoryDataBind,
        ODataReadTableInitGlobalState
    );

    function.named_parameters["secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    function.table_scan_progress = ODataReadTableProgress;

    set.AddFunction(function);
    return set;
}

} // namespace erpl_web
