#include "business_central_functions.hpp"
#include "business_central_secret.hpp"
#include "business_central_client.hpp"
#include "odata_read_functions.hpp"
#include "odata_edm.hpp"
#include "tracing.hpp"
#include "duckdb/common/exception.hpp"
#include <variant>

namespace erpl_web {

using namespace duckdb;

// ============================================================================
// bc_show_companies - List companies in the Business Central environment
// ============================================================================

struct BcShowCompaniesBindData : public TableFunctionData {
    std::unique_ptr<ODataReadBindData> odata_bind_data;
    bool finished = false;
};

static unique_ptr<FunctionData> BcShowCompaniesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    ERPL_TRACE_DEBUG("BC_FUNC", "Binding bc_show_companies");

    // Get secret parameter
    auto secret_name = input.named_parameters.at("secret").GetValue<string>();

    // Resolve auth
    auto auth_info = ResolveBusinessCentralAuth(context, secret_name);

    // Create OData client for companies endpoint
    auto client = BusinessCentralClientFactory::CreateCompaniesClient(
        auth_info.tenant_id,
        auth_info.environment,
        auth_info.auth_params
    );

    // Create bind data using OData infrastructure
    auto bind_data = make_uniq<BcShowCompaniesBindData>();
    bind_data->odata_bind_data = ODataReadBindData::FromEntitySetClient(client);

    // Get schema from OData
    names = bind_data->odata_bind_data->GetResultNames();
    return_types = bind_data->odata_bind_data->GetResultTypes();

    ERPL_TRACE_INFO("BC_FUNC", "bc_show_companies bound with " + std::to_string(names.size()) + " columns");
    return std::move(bind_data);
}

static void BcShowCompaniesScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->CastNoConst<BcShowCompaniesBindData>();

    if (bind_data.finished) {
        return;
    }

    auto rows_fetched = bind_data.odata_bind_data->FetchNextResult(output);
    if (!bind_data.odata_bind_data->HasMoreResults() && rows_fetched == 0) {
        bind_data.finished = true;
    }
}

TableFunctionSet CreateBcShowCompaniesFunction() {
    TableFunctionSet set("bc_show_companies");

    TableFunction func({}, BcShowCompaniesScan, BcShowCompaniesBind);
    func.named_parameters["secret"] = LogicalType::VARCHAR;

    set.AddFunction(func);
    return set;
}

// ============================================================================
// bc_show_entities - List available entity sets in the Business Central API
// ============================================================================

struct BcShowEntitiesBindData : public TableFunctionData {
    std::unique_ptr<ODataReadBindData> odata_bind_data;
    bool finished = false;
};

static unique_ptr<FunctionData> BcShowEntitiesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    ERPL_TRACE_DEBUG("BC_FUNC", "Binding bc_show_entities");

    // Get secret parameter
    auto secret_name = input.named_parameters.at("secret").GetValue<string>();

    // Resolve auth
    auto auth_info = ResolveBusinessCentralAuth(context, secret_name);

    // Create catalog client
    auto client = BusinessCentralClientFactory::CreateCatalogClient(
        auth_info.tenant_id,
        auth_info.environment,
        auth_info.auth_params
    );

    // Create bind data from service client (lists entity sets)
    auto bind_data = make_uniq<BcShowEntitiesBindData>();
    bind_data->odata_bind_data = ODataReadBindData::FromServiceClient(client);

    // Get schema
    names = bind_data->odata_bind_data->GetResultNames();
    return_types = bind_data->odata_bind_data->GetResultTypes();

    ERPL_TRACE_INFO("BC_FUNC", "bc_show_entities bound");
    return std::move(bind_data);
}

static void BcShowEntitiesScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->CastNoConst<BcShowEntitiesBindData>();

    if (bind_data.finished) {
        return;
    }

    auto rows_fetched = bind_data.odata_bind_data->FetchNextResult(output);
    if (!bind_data.odata_bind_data->HasMoreResults() && rows_fetched == 0) {
        bind_data.finished = true;
    }
}

TableFunctionSet CreateBcShowEntitiesFunction() {
    TableFunctionSet set("bc_show_entities");

    TableFunction func({}, BcShowEntitiesScan, BcShowEntitiesBind);
    func.named_parameters["secret"] = LogicalType::VARCHAR;

    set.AddFunction(func);
    return set;
}

// ============================================================================
// bc_describe - Describe schema of a Business Central entity
// ============================================================================

struct BcDescribeBindData : public TableFunctionData {
    std::vector<std::string> property_names;
    std::vector<std::string> property_types;
    std::vector<bool> is_nullable;
    std::vector<bool> is_key;
    idx_t current_row = 0;
};

static unique_ptr<FunctionData> BcDescribeBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    ERPL_TRACE_DEBUG("BC_FUNC", "Binding bc_describe");

    // Get entity name
    auto entity_name = input.inputs[0].GetValue<string>();

    // Get secret parameter
    auto secret_name = input.named_parameters.at("secret").GetValue<string>();

    // Optional company parameter
    string company_id;
    if (input.named_parameters.count("company")) {
        company_id = input.named_parameters.at("company").GetValue<string>();
    }

    // Resolve auth
    auto auth_info = ResolveBusinessCentralAuth(context, secret_name);

    // Create catalog client to get metadata
    auto client = BusinessCentralClientFactory::CreateCatalogClient(
        auth_info.tenant_id,
        auth_info.environment,
        auth_info.auth_params
    );

    // Get metadata
    auto metadata = client->GetMetadata();

    // Find the entity type
    auto bind_data = make_uniq<BcDescribeBindData>();
    bool found = false;

    for (const auto &entity_set : metadata.FindEntitySets()) {
        if (entity_set.name == entity_name) {
            // Resolve the entity type from the metadata
            try {
                auto type_variant = metadata.FindType(entity_set.entity_type_name);
                auto* entity_type = std::get_if<EntityType>(&type_variant);

                if (entity_type) {
                    // Get key property names
                    std::set<std::string> key_props;
                    for (const auto &key_ref : entity_type->key.property_refs) {
                        key_props.insert(key_ref.name);
                    }

                    // Collect properties
                    for (const auto &prop : entity_type->properties) {
                        bind_data->property_names.push_back(prop.name);
                        bind_data->property_types.push_back(prop.type_name);
                        bind_data->is_nullable.push_back(prop.nullable);
                        bind_data->is_key.push_back(key_props.count(prop.name) > 0);
                    }
                    found = true;
                }
            } catch (const std::runtime_error &e) {
                // Type not found, continue searching
            }
            break;
        }
    }

    if (!found) {
        throw InvalidInputException("Entity '" + entity_name + "' not found in Business Central API. Use bc_show_entities() to list available entities.");
    }

    // Set up return schema
    names = {"property_name", "property_type", "nullable", "is_key"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN};

    ERPL_TRACE_INFO("BC_FUNC", "bc_describe bound for entity: " + entity_name);
    return std::move(bind_data);
}

static void BcDescribeScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->CastNoConst<BcDescribeBindData>();

    idx_t count = 0;
    while (bind_data.current_row < bind_data.property_names.size() && count < STANDARD_VECTOR_SIZE) {
        output.SetValue(0, count, Value(bind_data.property_names[bind_data.current_row]));
        output.SetValue(1, count, Value(bind_data.property_types[bind_data.current_row]));
        output.SetValue(2, count, Value(bind_data.is_nullable[bind_data.current_row]));
        output.SetValue(3, count, Value(bind_data.is_key[bind_data.current_row]));
        bind_data.current_row++;
        count++;
    }

    output.SetCardinality(count);
}

TableFunctionSet CreateBcDescribeFunction() {
    TableFunctionSet set("bc_describe");

    TableFunction func({LogicalType::VARCHAR}, BcDescribeScan, BcDescribeBind);
    func.named_parameters["secret"] = LogicalType::VARCHAR;
    func.named_parameters["company"] = LogicalType::VARCHAR;

    set.AddFunction(func);
    return set;
}

// ============================================================================
// bc_read - Read data from a Business Central entity with predicate pushdown
// ============================================================================

struct BcReadBindData : public TableFunctionData {
    std::unique_ptr<ODataReadBindData> odata_bind_data;
    bool finished = false;
};

static unique_ptr<FunctionData> BcReadBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    ERPL_TRACE_DEBUG("BC_FUNC", "Binding bc_read");

    // Get entity name
    auto entity_name = input.inputs[0].GetValue<string>();

    // Get secret parameter
    auto secret_name = input.named_parameters.at("secret").GetValue<string>();

    // Get company parameter (required for most entity reads)
    string company_id;
    if (input.named_parameters.count("company")) {
        company_id = input.named_parameters.at("company").GetValue<string>();
    }

    // Resolve auth
    auto auth_info = ResolveBusinessCentralAuth(context, secret_name);

    // Create OData client
    auto client = BusinessCentralClientFactory::CreateEntitySetClient(
        auth_info.tenant_id,
        auth_info.environment,
        company_id,
        entity_name,
        auth_info.auth_params
    );

    // Create bind data using OData infrastructure
    auto bind_data = make_uniq<BcReadBindData>();
    bind_data->odata_bind_data = ODataReadBindData::FromEntitySetClient(client);

    // Handle expand parameter
    if (input.named_parameters.count("expand")) {
        auto expand_clause = input.named_parameters.at("expand").GetValue<string>();
        bind_data->odata_bind_data->SetExpandClause(expand_clause);
    }

    // Get schema from OData
    names = bind_data->odata_bind_data->GetResultNames();
    return_types = bind_data->odata_bind_data->GetResultTypes();

    ERPL_TRACE_INFO("BC_FUNC", "bc_read bound for entity: " + entity_name + " with " + std::to_string(names.size()) + " columns");
    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> BcReadInitGlobalState(
    ClientContext &context,
    TableFunctionInitInput &input) {

    auto &bind_data = input.bind_data->CastNoConst<BcReadBindData>();

    // Activate columns for projection pushdown
    bind_data.odata_bind_data->ActivateColumns(input.column_ids);

    // Add filters for predicate pushdown
    bind_data.odata_bind_data->AddFilters(input.filters);

    // Update URL with pushdown predicates
    bind_data.odata_bind_data->UpdateUrlFromPredicatePushdown();

    // Prefetch first page
    bind_data.odata_bind_data->PrefetchFirstPage();

    return nullptr;
}

static void BcReadScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->CastNoConst<BcReadBindData>();

    if (bind_data.finished) {
        return;
    }

    auto rows_fetched = bind_data.odata_bind_data->FetchNextResult(output);
    if (!bind_data.odata_bind_data->HasMoreResults() && rows_fetched == 0) {
        bind_data.finished = true;
    }
}

static double BcReadProgress(ClientContext &context, const FunctionData *bind_data_p, const GlobalTableFunctionState *) {
    auto &bind_data = bind_data_p->Cast<BcReadBindData>();
    return bind_data.odata_bind_data->GetProgressFraction();
}

TableFunctionSet CreateBcReadFunction() {
    TableFunctionSet set("bc_read");

    TableFunction func({LogicalType::VARCHAR}, BcReadScan, BcReadBind, BcReadInitGlobalState);
    func.named_parameters["secret"] = LogicalType::VARCHAR;
    func.named_parameters["company"] = LogicalType::VARCHAR;
    func.named_parameters["expand"] = LogicalType::VARCHAR;

    // Enable pushdown features
    func.filter_pushdown = true;
    func.filter_prune = true;
    func.projection_pushdown = true;

    // Progress reporting
    func.table_scan_progress = BcReadProgress;

    set.AddFunction(func);
    return set;
}

} // namespace erpl_web
