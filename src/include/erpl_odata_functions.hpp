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
    static duckdb::unique_ptr<ODataReadBindData> FromEntitySetRoot(const std::string& entity_set_url, 
                                                                   std::shared_ptr<HttpAuthParams> auth_params);

public:
    ODataReadBindData(std::shared_ptr<ODataEntitySetClient> odata_client);

    std::vector<std::string> GetResultNames(bool all_columns = false);
    std::vector<duckdb::LogicalType> GetResultTypes(bool all_columns = false);
    
    bool HasMoreResults();
    unsigned int FetchNextResult(DataChunk &output);

    void ActivateColumns(const std::vector<duckdb::column_t> &column_ids);
    void AddFilters(const duckdb::optional_ptr<duckdb::TableFilterSet> &filters);

    void UpdateUrlFromPredicatePushdown();

private:
    bool first_fetch = true;
    std::shared_ptr<ODataEntitySetClient> odata_client;
    std::vector<std::string> all_result_names;
    std::vector<duckdb::column_t> active_column_ids;
    std::vector<duckdb::LogicalType> all_result_types;

    std::shared_ptr<ODataPredicatePushdownHelper> predicate_pushdown_helper;
};


// -------------------------------------------------------------------------------------------------

class ODataAttachBindData : public TableFunctionData
{
public:
    static duckdb::unique_ptr<ODataAttachBindData> FromUrl(const std::string& url, std::shared_ptr<HttpAuthParams> auth_params);

public:
    ODataAttachBindData(std::shared_ptr<ODataServiceClient> odata_client);

    bool IsFinished() const;
    void SetFinished();

    bool Overwrite() const;
    void SetOverwrite(bool overwrite);

    std::vector<ODataEntitySetReference> EntitySets();

private:
    bool finished = false;
    bool overwrite = false;
    std::shared_ptr<ODataServiceClient> odata_client;
};

// -------------------------------------------------------------------------------------------------

TableFunctionSet CreateODataReadFunction();
TableFunctionSet CreateODataAttachFunction();

} // namespace erpl_web