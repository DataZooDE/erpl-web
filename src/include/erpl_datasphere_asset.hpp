#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "erpl_datasphere_client.hpp"
#include "erpl_odata_client.hpp"
#include <memory>
#include <string>
#include <map>
#include <vector>

namespace erpl_web {

// Bind data for asset consumption functions
class DatasphereAssetBindData : public duckdb::TableFunctionData {
public:
    DatasphereAssetBindData(const std::string& tenant,
                            const std::string& data_center,
                            const std::string& space_id,
                            const std::string& asset_id,
                            std::shared_ptr<HttpAuthParams> auth_params,
                            const std::map<std::string, std::string>& input_parameters = {});
    
    std::string tenant;
    std::string data_center;
    std::string space_id;
    std::string asset_id;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::map<std::string, std::string> input_parameters;
    
    // OData client for data access
    std::shared_ptr<ODataEntitySetClient> odata_client;
    
    // Asset type detection and client initialization
    void InitializeClient();
    bool IsAnalyticalAsset() const;
    bool IsRelationalAsset() const;
    
    // Input parameter handling
    void SetInputParameters(const std::map<std::string, std::string>& params);
    std::string BuildParameterClause() const;
    
    // Data access methods
    std::vector<std::string> GetResultNames();
    std::vector<duckdb::LogicalType> GetResultTypes();
    bool HasMoreResults();
    unsigned int FetchNextResult(duckdb::DataChunk &output);
    
    // Column activation and filtering
    void ActivateColumns(const std::vector<duckdb::column_t> &column_ids);
    void AddFilters(const duckdb::optional_ptr<duckdb::TableFilterSet> &filters);
    void UpdateUrlFromPredicatePushdown();
    
private:
    std::vector<std::string> all_result_names;
    std::vector<duckdb::LogicalType> all_result_types;
    std::vector<duckdb::column_t> active_column_ids;
    bool first_fetch = true;
    
    // Asset type detection
    void DetectAssetType();
    std::string asset_type; // "analytical" or "relational"
    
    // Make asset_type accessible to derived classes
protected:
    void SetAssetType(const std::string& type) { asset_type = type; }
};

// Bind data for analytical-specific functions
class DatasphereAnalyticalBindData : public DatasphereAssetBindData {
public:
    DatasphereAnalyticalBindData(const std::string& tenant,
                                 const std::string& data_center,
                                 const std::string& space_id,
                                 const std::string& asset_id,
                                 std::shared_ptr<HttpAuthParams> auth_params,
                                 const std::map<std::string, std::string>& input_parameters = {});
    
    // Analytical-specific functionality
    std::string BuildApplyClause(const std::vector<std::string>& dimensions, 
                                const std::vector<std::string>& measures) const;
    std::string BuildApplyClauseWithAggregation(const std::vector<std::string>& dimensions,
                                               const std::map<std::string, std::string>& measures_with_aggregation) const;
    std::string BuildHierarchyClause(const std::string& hierarchy_name) const;
    std::string BuildCalculatedMeasureClause(const std::string& measure_expression) const;
};

// Bind data for relational-specific functions
class DatasphereRelationalBindData : public DatasphereAssetBindData {
public:
    DatasphereRelationalBindData(const std::string& tenant,
                                 const std::string& data_center,
                                 const std::string& space_id,
                                 const std::string& asset_id,
                                 std::shared_ptr<HttpAuthParams> auth_params,
                                 const std::map<std::string, std::string>& input_parameters = {});
    
    // Relational-specific functionality
    void EnableInlineCount(bool enabled = true);
    void SetSkipToken(const std::string& token);
};

// Function declarations
duckdb::TableFunctionSet CreateDatasphereAssetFunction();
duckdb::TableFunctionSet CreateDatasphereAnalyticalFunction();
duckdb::TableFunctionSet CreateDatasphereRelationalFunction();

// Helper functions for parameter parsing
std::map<std::string, std::string> ParseInputParameters(const std::string& param_string);
std::string BuildODataUrlWithParameters(const std::string& base_url, 
                                       const std::map<std::string, std::string>& parameters);

} // namespace erpl_web
