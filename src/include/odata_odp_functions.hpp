#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "yyjson.hpp"
// Forward declarations
namespace erpl_web {
    class HttpClient;
    class HttpAuthParams;
    class ODataEntitySetResponse;
}
#include <memory>
#include <string>
#include <vector>

namespace erpl_web {

// Bind data for SAP OData service discovery
class SapODataShowBindData : public duckdb::TableFunctionData {
public:
    SapODataShowBindData(std::shared_ptr<HttpClient> http_client, 
                         std::shared_ptr<HttpAuthParams> auth_params,
                         const std::string& base_url);
    
    std::shared_ptr<HttpClient> http_client;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::string base_url;
    
    // Service discovery results
    std::vector<std::vector<duckdb::Value>> service_data;
    idx_t next_index = 0;
    bool data_loaded = false;
    
    void LoadServiceData();
    
private:
    void LoadV2Services();
    void LoadV4Services();
    void ParseODataV2CatalogFromResponse(std::shared_ptr<ODataEntitySetResponse> response);
    void ParseODataV4CatalogFromResponse(std::shared_ptr<ODataEntitySetResponse> response);
};

// Bind data for ODP service discovery
class OdpODataShowBindData : public duckdb::TableFunctionData {
public:
    OdpODataShowBindData(std::shared_ptr<HttpClient> http_client, 
                         std::shared_ptr<HttpAuthParams> auth_params,
                         const std::string& base_url);
    
    std::shared_ptr<HttpClient> http_client;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::string base_url;
    
    // ODP service discovery results
    std::vector<std::vector<duckdb::Value>> odp_service_data;
    idx_t next_index = 0;
    bool data_loaded = false;
    
    void LoadOdpServiceData();
    
private:
    void ParseOdpServicesFromCatalog(const std::string& content);
    void ParseOdpServicesFromV2Response(std::shared_ptr<ODataEntitySetResponse> response);
    void ExtractAndAddOdpEntitySets(duckdb_yyjson::yyjson_val* service_entry, 
                                   const std::string& service_id, 
                                   const std::string& service_description);
};

// Helper function declarations
std::string ExtractOdpEntitySetsFromJson(duckdb_yyjson::yyjson_val* service_entry);

// Function declarations
duckdb::TableFunctionSet CreateSapODataShowFunction();
duckdb::TableFunctionSet CreateOdpODataShowFunction();

} // namespace erpl_web
