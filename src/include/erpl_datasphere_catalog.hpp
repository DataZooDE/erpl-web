#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "erpl_datasphere_client.hpp"
#include "erpl_odata_functions.hpp"
#include "erpl_oauth2_types.hpp"
#include <memory>
#include <string>
#include <vector>
#include <optional>

namespace erpl_web {



// Bind data for show functions (list resources) - now using standard OData pipeline
class DatasphereShowBindData : public ODataReadBindData {
public:
    DatasphereShowBindData(std::shared_ptr<ODataEntitySetClient> odata_client);
    
    std::string resource_type; // "spaces" or "assets"
    std::string space_id; // for assets only
    
    // Override column names and types for Datasphere-specific schema
    std::vector<std::string> GetResultNames(bool all_columns = false);
    std::vector<duckdb::LogicalType> GetResultTypes(bool all_columns = false);
};

// Simple bind data for listing spaces via DWAAS core API
class DatasphereSpacesListBindData : public duckdb::TableFunctionData {
public:
    std::vector<std::string> space_names;
    idx_t next_index = 0;
};

struct DatasphereSpaceObjectItem {
    std::string name;
    std::string technical_name;
    std::string object_type;
    std::string space_name;  // Added for the all-spaces version
};

class DatasphereSpaceObjectsBindData : public duckdb::TableFunctionData {
public:
    std::vector<DatasphereSpaceObjectItem> items;
    idx_t next_index = 0;
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
    
    // Track whether we've already returned the data
    bool data_returned = false;
    
    // DWAAS core API response storage for parsing
    std::string dwass_response_content;
    std::string dwass_endpoint_type;
    
    // Column definitions
    std::vector<std::string> GetColumnNames() const;
    std::vector<duckdb::LogicalType> GetColumnTypes() const;
    
    // Extended metadata methods
    std::vector<duckdb::Value> FetchAssetExtendedMetadata(duckdb::ClientContext &context, 
                                                         const OAuth2Config &config,
                                                         const std::shared_ptr<HttpAuthParams> &auth_params);
    std::string FetchMetadataSummary(const std::string &metadata_url, 
                                   const std::shared_ptr<HttpAuthParams> &auth_params,
                                   const std::string &metadata_type);
    duckdb::Value FetchDetailedAnalyticalSchema(const std::string &metadata_url, 
                                               const std::shared_ptr<HttpAuthParams> &auth_params);
    duckdb::Value ParseAnalyticalMetadata(const std::string &xml_content);
    
    // Parse DWAAS core API response to extract analytical schema
    std::optional<duckdb::Value> ParseDwaasAnalyticalSchema(const std::string &json_content);
    std::optional<duckdb::Value> ParseDwaasRelationalSchema(const std::string &json_content);
    
private:
    // Helper function to convert EDM types to DuckDB types
    std::string ConvertEdmTypeToDuckDbType(const std::string& edm_type);
};

// Table function creation functions
duckdb::TableFunctionSet CreateDatasphereShowSpacesFunction();
duckdb::TableFunctionSet CreateDatasphereShowAssetsFunction();
duckdb::TableFunctionSet CreateDatasphereDescribeSpaceFunction();
duckdb::TableFunctionSet CreateDatasphereDescribeAssetFunction();

} // namespace erpl_web
