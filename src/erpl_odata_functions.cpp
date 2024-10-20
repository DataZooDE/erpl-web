
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "erpl_odata_functions.hpp"

#include "telemetry.hpp"

namespace erpl_web {

duckdb::unique_ptr<ODataReadBindData> ODataReadBindData::FromEntitySetRoot(const std::string& entity_set_url)
{
    auto http_client = std::make_shared<HttpClient>();
    auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, entity_set_url);
    
    return duckdb::make_uniq<ODataReadBindData>(odata_client);
}

ODataReadBindData::ODataReadBindData(std::shared_ptr<ODataEntitySetClient> odata_client)
    : odata_client(odata_client)
{ 
    predicate_pushdown_helper = std::make_shared<ODataPredicatePushdownHelper>(odata_client->GetResultNames());
}

std::vector<std::string> ODataReadBindData::GetResultNames(bool all_columns)
{
    if (all_result_names.empty()) {
        all_result_names = odata_client->GetResultNames();
    }

    if (all_columns || active_column_ids.empty()) {
        return all_result_names;
    }

    std::vector<std::string> active_result_names;
    for (auto &column_id : active_column_ids) {
        if (duckdb::IsRowIdColumnId(column_id)) {
            continue;
        }

        active_result_names.push_back(all_result_names[column_id]);
    }

    return active_result_names;
}

std::vector<duckdb::LogicalType> ODataReadBindData::GetResultTypes(bool all_columns)
{
    if (all_result_types.empty()) {
        all_result_types = odata_client->GetResultTypes();
    }

    if (all_columns || active_column_ids.empty()) {
        return all_result_types;
    }

    std::vector<duckdb::LogicalType> active_result_types;
    for (auto &column_id : active_column_ids) {
        if (duckdb::IsRowIdColumnId(column_id)) {
            continue;
        }

        active_result_types.push_back(all_result_types[column_id]);
    }

    return active_result_types;
}

unsigned int ODataReadBindData::FetchNextResult(duckdb::DataChunk &output)
{
    auto response = odata_client->Get();
    auto result_names = GetResultNames();
    auto result_types = GetResultTypes();
    auto rows = response->ToRows(result_names, result_types);
    auto null_value = duckdb::Value();

    for (idx_t i = 0; i < rows.size(); i++) {
        for (idx_t j = 0; j < output.ColumnCount(); j++) {
            if (j >= result_names.size()) {
                auto col_type = output.data[j].GetType();
                output.SetValue(j, i, null_value.DefaultCastAs(col_type));
            } else {
                output.SetValue(j, i, rows[i][j]);
            }
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
    active_column_ids = column_ids;
    predicate_pushdown_helper->ConsumeColumnSelection(column_ids);

    std::cout << "Select: " << predicate_pushdown_helper->SelectClause() << std::endl;
}

void ODataReadBindData::AddFilters(const duckdb::optional_ptr<duckdb::TableFilterSet> &filters)
{
    predicate_pushdown_helper->ConsumeFilters(filters);
}

void ODataReadBindData::UpdateUrlFromPredicatePushdown()
{
    auto http_client = odata_client->GetHttpClient();
    auto updated_url = predicate_pushdown_helper->ApplyFiltersToUrl(odata_client->Url());

    odata_client = std::make_shared<ODataEntitySetClient>(http_client, updated_url);
}

// -------------------------------------------------------------------------------------------------

duckdb::unique_ptr<ODataAttachBindData> ODataAttachBindData::FromUrl(const std::string& url)
{
    auto http_client = std::make_shared<HttpClient>();
    auto odata_client = std::make_shared<ODataServiceClient>(http_client, url);

    return duckdb::make_uniq<ODataAttachBindData>(odata_client);
}

ODataAttachBindData::ODataAttachBindData(std::shared_ptr<ODataServiceClient> odata_client)
    : odata_client(odata_client)
{ }


bool ODataAttachBindData::IsFinished() const
{
    return finished;
}

void ODataAttachBindData::SetFinished()
{
    finished = true;
}


bool ODataAttachBindData::Overwrite() const
{
    return overwrite;
}

void ODataAttachBindData::SetOverwrite(bool overwrite)
{
    this->overwrite = overwrite;
}

std::vector<ODataEntitySetReference> ODataAttachBindData::EntitySets()
{
    auto svc_response = odata_client->Get();
    auto svc_references = svc_response->EntitySets();

    for (auto &svc_reference : svc_references) {
        svc_reference.MergeWithBaseUrlIfRelative(odata_client->Url());
    }

    return svc_references;
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

    bind_data.UpdateUrlFromPredicatePushdown();

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

static unique_ptr<FunctionData> ODataAttachBind(ClientContext &context, 
                                               TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, 
                                               vector<string> &names) 
{
    auto url = input.inputs[0].GetValue<std::string>();
    auto bind_data = ODataAttachBindData::FromUrl(url);

    for (auto &kv : input.named_parameters) {
		if (kv.first == "overwrite") {
			bind_data->SetOverwrite(BooleanValue::Get(kv.second));
		}
	}

    return_types.emplace_back(LogicalType::BOOLEAN);
    names.emplace_back("Success");

    return std::move(bind_data);
}

static void ODataAttachScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) 
{
    auto &data = data_p.bind_data->CastNoConst<ODataAttachBindData>();
	if (data.IsFinished()) {
		return;
	}

    auto duck_conn = duckdb::Connection(context.db->GetDatabase(context));
    
    auto svc_references = data.EntitySets();
    for (auto &svc_reference : svc_references) {
        auto table_relation = duck_conn.TableFunction("odata_read", {Value(svc_reference.url)});
        auto table_view = table_relation->CreateView(svc_reference.name, data.Overwrite(), false);
    }
	
    data.SetFinished();
}

TableFunctionSet CreateODataAttachFunction()
{
    TableFunctionSet function_set("odata_attach");

    TableFunction attach_service({LogicalType::VARCHAR}, ODataAttachScan, ODataAttachBind);
    attach_service.named_parameters["overwrite"] = LogicalType::BOOLEAN;

    function_set.AddFunction(attach_service);
    return function_set;
}

} // namespace erpl_web
