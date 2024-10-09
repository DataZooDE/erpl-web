
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "erpl_odata_functions.hpp"

#include "telemetry.hpp"

namespace erpl_web {

duckdb::unique_ptr<ODataReadBindData> ODataReadBindData::FromEntitySetRoot(const std::string& entity_set_url)
{
    auto http_client = std::make_shared<HttpClient>();
    auto odata_client = std::make_shared<ODataClient>(http_client, entity_set_url);
    
    return duckdb::make_uniq<ODataReadBindData>(odata_client);
}

ODataReadBindData::ODataReadBindData(std::shared_ptr<ODataClient> odata_client)
    : odata_client(odata_client)
{ 
    predicate_pushdown_helper = std::make_shared<ODataPredicatePushdownHelper>(odata_client->GetResultNames());
}


std::vector<std::string> ODataReadBindData::GetResultNames()
{
    if (all_result_names.empty()) {
        all_result_names = odata_client->GetResultNames();
    }

    return all_result_names;
}

std::vector<duckdb::LogicalType> ODataReadBindData::GetResultTypes()
{
    if (all_result_types.empty()) {
        all_result_types = odata_client->GetResultTypes();
    }

    return all_result_types;
}

unsigned int ODataReadBindData::FetchNextResult(duckdb::DataChunk &output)
{
    auto response = odata_client->Get();
    auto result_names = GetResultNames();
    auto result_types = GetResultTypes();
    auto rows = response->ToRows(result_names, result_types);

    for (idx_t i = 0; i < rows.size(); i++) {
        for (idx_t j = 0; j < result_names.size(); j++) {
            output.SetValue(j, i, rows[i][j]);
        }
    }
    
    output.SetCardinality(rows.size());
    return rows.size();
}

bool ODataReadBindData::HasMoreResults()
{
    if (first_fetch) {
        first_fetch = false;
        return true;
    }

    auto response = odata_client->Get(true);
    return response != nullptr;
}

void ODataReadBindData::ActivateColumns(const std::vector<duckdb::column_t> &column_ids)
{
    predicate_pushdown_helper->ConsumeColumnSelection(column_ids);
}

void ODataReadBindData::AddFilters(const duckdb::optional_ptr<duckdb::TableFilterSet> &filters)
{
    predicate_pushdown_helper->ConsumeFilters(filters);
}
// -------------------------------------------------------------------------------------------------

static duckdb::unique_ptr<FunctionData> ODataReadBind(ClientContext &context, 
                                                      TableFunctionBindInput &input, 
                                                      vector<LogicalType> &return_types, 
                                                      vector<string> &names) 
{
    auto url = input.inputs[0].GetValue<std::string>();
    auto bind_data = ODataReadBindData::FromEntitySetRoot(url);

    names = bind_data->GetResultNames();
    return_types = bind_data->GetResultTypes();
    
    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> ODataReadTableInitGlobalState(ClientContext &context,
                                                                          TableFunctionInitInput &input) 
{
    auto &bind_data = input.bind_data->CastNoConst<ODataReadBindData>();
    auto column_ids = input.column_ids;

    bind_data.ActivateColumns(column_ids);
    bind_data.AddFilters(input.filters);

    return duckdb::make_uniq<GlobalTableFunctionState>();
}

static void ODataReadScan(ClientContext &context, 
                          TableFunctionInput &data, 
                          DataChunk &output) 
{
    
    auto &bind_data = data.bind_data->CastNoConst<ODataReadBindData>();
    if (! bind_data.HasMoreResults()) {
        return;
    }
    
    bind_data.FetchNextResult(output);
}

TableFunctionSet CreateODataReadFunction() 
{
    TableFunctionSet function_set("odata_read");
    
    TableFunction read_entity_set({LogicalType::VARCHAR}, ODataReadScan, ODataReadBind, ODataReadTableInitGlobalState);
    read_entity_set.filter_pushdown = true;
    read_entity_set.projection_pushdown = true;

    function_set.AddFunction(read_entity_set);
    return function_set;
}

} // namespace erpl_web
