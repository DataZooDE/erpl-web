#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include <map>

#include "erpl_odata_client.hpp"
#include "erpl_odata_edm.hpp"
#include "erpl_odata_predicate_pushdown_helper.hpp"
#include <deque>

using namespace duckdb;

namespace erpl_web {

class ODataReadBindData : public TableFunctionData
{
public: 
    static duckdb::unique_ptr<ODataReadBindData> FromEntitySetRoot(const std::string& entity_set_url, 
                                                                   std::shared_ptr<HttpAuthParams> auth_params);

public:
    ODataReadBindData(std::shared_ptr<ODataEntitySetClient> odata_client);

    virtual std::vector<std::string> GetResultNames(bool all_columns = false);
    virtual std::vector<duckdb::LogicalType> GetResultTypes(bool all_columns = false);
    
    bool HasMoreResults();
    unsigned int FetchNextResult(DataChunk &output);

    void ActivateColumns(const std::vector<duckdb::column_t> &column_ids);
    void AddFilters(const duckdb::optional_ptr<duckdb::TableFilterSet> &filters);
    
    // Method to consume result modifiers (for LIMIT/OFFSET)
    void AddResultModifiers(const std::vector<duckdb::unique_ptr<duckdb::BoundResultModifier>> &modifiers);

    void UpdateUrlFromPredicatePushdown();
    std::shared_ptr<ODataPredicatePushdownHelper> PredicatePushdownHelper();
    // Report cumulative progress in [0,1], or <0 when unknown (to hide DuckDB progress)
    double GetProgressFraction() const;

    // Prefetch first page after predicate pushdown (called from init state)
    void PrefetchFirstPage();
    
    // For Datasphere dual-URL pattern: set column names extracted from first data row
    void SetExtractedColumnNames(const std::vector<std::string>& column_names);
    
    // Get the original column name for a given activated column index
    std::string GetOriginalColumnName(duckdb::column_t activated_column_index) const;
    
    // For Datasphere input parameters: set and get input parameters
    void SetInputParameters(const std::map<std::string, std::string>& input_params);
    const std::map<std::string, std::string>& GetInputParameters() const;
    
    // Get the underlying OData client for advanced operations
    std::shared_ptr<ODataEntitySetClient> GetODataClient() const;
    
private:
    bool first_fetch = true;
    std::shared_ptr<ODataEntitySetClient> odata_client;
    std::vector<std::string> all_result_names;
    std::vector<duckdb::column_t> active_column_ids;
    std::vector<duckdb::LogicalType> all_result_types;
    
    // For Datasphere dual-URL pattern: column names extracted from first data row
    std::vector<std::string> extracted_column_names;
    
    // Mapping from activated column index to original column name index
    std::vector<duckdb::column_t> activated_to_original_mapping;
    
    // For Datasphere input parameters: storage for input parameters
    std::map<std::string, std::string> input_parameters;
    
    std::shared_ptr<ODataPredicatePushdownHelper> predicate_pushdown_helper;

    // Progress reporting state
    uint64_t progress_rows_fetched_ = 0;
    uint64_t progress_total_count_ = 0;
    bool progress_has_total_ = false;

    // Buffered rows across pages to align with DuckDB vector size
    std::deque<std::vector<duckdb::Value>> row_buffer_;
    bool has_next_page_ = false;
    bool first_page_cached_ = false;
};


// Forward declarations for the scan and bind functions
void ODataReadScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

unique_ptr<GlobalTableFunctionState> ODataReadTableInitGlobalState(ClientContext &context, TableFunctionInitInput &input);
unique_ptr<FunctionData> ODataReadBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names);
double ODataReadTableProgress(ClientContext &, const FunctionData *func_data, const GlobalTableFunctionState *);

TableFunctionSet CreateODataReadFunction();


} // namespace erpl_web