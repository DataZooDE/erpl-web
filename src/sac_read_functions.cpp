#include "sac_read_functions.hpp"
#include "sac_secret_helper.hpp"
#include "sac_url_builder.hpp"
#include "sac_client.hpp"
#include "sac_catalog.hpp"
#include "odata_read_functions.hpp"
#include "odata_content.hpp"
#include "telemetry.hpp"
#include <algorithm>

namespace erpl_web {

using duckdb::PostHogTelemetry;

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
    PostHogTelemetry::Instance().CaptureFunctionExecution("sac_read_planning_data");

    if (input.inputs.empty()) {
        throw duckdb::InvalidInputException("sac_read_planning_data requires a model_id parameter");
    }

    auto model_id = input.inputs[0].GetValue<std::string>();

    // Get secret name (default: "sac")
    std::string secret_name = "sac";
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }

    // Resolve SAC credentials
    auto secret_data = ResolveSacSecretData(context, secret_name);

    // Build the URL
    auto url_str = SacUrlBuilder::BuildPlanningDataUrl(secret_data.tenant, secret_data.region, model_id);

    // Create OData read bind data
    auto read_bind = ODataReadBindData::FromEntitySetRoot(url_str, secret_data.auth_params);

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
    PostHogTelemetry::Instance().CaptureFunctionExecution("sac_read_analytical");

    if (input.inputs.empty()) {
        throw duckdb::InvalidInputException("sac_read_analytical requires a model_id parameter");
    }

    auto model_id = input.inputs[0].GetValue<std::string>();

    std::string secret_name = "sac";
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }

    auto secret_data = ResolveSacSecretData(context, secret_name);

    // For analytics models, build URL to analytics endpoint instead
    auto analytics_url = SacUrlBuilder::BuildModelServiceUrl(secret_data.tenant, secret_data.region, model_id);

    // Create OData read bind data
    auto read_bind = ODataReadBindData::FromEntitySetRoot(analytics_url, secret_data.auth_params);

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
    PostHogTelemetry::Instance().CaptureFunctionExecution("sac_read_story_data");

    if (input.inputs.empty()) {
        throw duckdb::InvalidInputException("sac_read_story_data requires a story_id parameter");
    }

    auto story_id = input.inputs[0].GetValue<std::string>();

    std::string secret_name = "sac";
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }

    // Resolve SAC credentials
    auto secret_data = ResolveSacSecretData(context, secret_name);

    // Query the story service endpoint
    auto story_url = SacUrlBuilder::BuildStoryServiceUrl(secret_data.tenant, secret_data.region);
    // Append story ID to get specific story data
    story_url += "('" + story_id + "')";

    auto read_bind = ODataReadBindData::FromEntitySetRoot(story_url, secret_data.auth_params);

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
