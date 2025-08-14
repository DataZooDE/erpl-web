#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "erpl_datasphere_client.hpp"
#include "erpl_odata_functions.hpp"
#include <memory>
#include <string>
#include <vector>

namespace erpl_web {

// Bind data for show functions (list resources) - now using standard OData pipeline
class DatasphereShowBindData : public ODataReadBindData {
public:
    DatasphereShowBindData(std::shared_ptr<ODataEntitySetClient> odata_client);
    
    std::string resource_type; // "spaces" or "assets"
    std::string space_id; // for assets only
    
    // Override column names and types for Datasphere-specific schema
    std::vector<std::string> GetResultNames(bool all_columns = false) override;
    std::vector<duckdb::LogicalType> GetResultTypes(bool all_columns = false) override;
};

// Bind data for describe functions (detailed resource information)
class DatasphereDescribeBindData : public duckdb::TableFunctionData {
public:
    DatasphereDescribeBindData(std::shared_ptr<ODataServiceClient> catalog_client, 
                               const std::string& resource_type,
                               const std::string& resource_id);
    
    std::shared_ptr<ODataServiceClient> catalog_client;
    std::string resource_type; // "space" or "asset"
    std::string resource_id;
    std::string space_id; // for assets only
    
    // Load detailed resource information
    void LoadResourceDetails(duckdb::ClientContext &context);
    
    // Data storage
    std::vector<std::vector<duckdb::Value>> resource_data;
    
    // Column definitions
    std::vector<std::string> GetColumnNames() const;
    std::vector<duckdb::LogicalType> GetColumnTypes() const;
};

// Table function creation functions
duckdb::TableFunctionSet CreateDatasphereShowSpacesFunction();
duckdb::TableFunctionSet CreateDatasphereShowAssetsFunction();
duckdb::TableFunctionSet CreateDatasphereDescribeSpaceFunction();
duckdb::TableFunctionSet CreateDatasphereDescribeAssetFunction();

} // namespace erpl_web
