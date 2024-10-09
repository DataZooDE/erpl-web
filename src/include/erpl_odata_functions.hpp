#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"

#include "erpl_odata_client.hpp"
#include "erpl_odata_edm.hpp"
#include "erpl_odata_predicate_pushdown_helper.hpp"

using namespace duckdb;


namespace erpl_web {

class ODataReadBindData : public TableFunctionData
{
public: 
    static duckdb::unique_ptr<ODataReadBindData> FromEntitySetRoot(const std::string& entity_set_url);
public:
    ODataReadBindData(std::shared_ptr<ODataClient> odata_client);

    std::vector<std::string> GetResultNames();
    std::vector<duckdb::LogicalType> GetResultTypes();
    
    bool HasMoreResults();
    unsigned int FetchNextResult(DataChunk &output);

    void ActivateColumns(const std::vector<duckdb::column_t> &column_ids);
    void AddFilters(const duckdb::optional_ptr<duckdb::TableFilterSet> &filters);

private:
    bool first_fetch = true;
    std::shared_ptr<ODataClient> odata_client;
    std::vector<std::string> all_result_names;
    std::vector<duckdb::column_t> active_column_names;
    std::vector<duckdb::LogicalType> all_result_types;

    std::shared_ptr<ODataPredicatePushdownHelper> predicate_pushdown_helper;
};

TableFunctionSet CreateODataReadFunction();


} // namespace erpl_web