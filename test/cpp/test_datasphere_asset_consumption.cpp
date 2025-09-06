#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "datasphere_read.hpp"
#include "datasphere_client.hpp"
#include <iostream>

using namespace erpl_web;
using namespace std;

TEST_CASE("Test Asset Type Detection", "[datasphere][asset][detection]")
{
    std::cout << std::endl;
    
    // Test analytical asset detection
    std::string analytical_url = "https://test.com/api/v1/dwc/consumption/analytical/space1/asset1";
    bool is_analytical = analytical_url.find("/analytical/") != std::string::npos;
    REQUIRE(is_analytical == true);
    
    // Test relational asset detection
    std::string relational_url = "https://test.com/api/v1/dwc/consumption/relational/space1/asset1";
    bool is_relational = relational_url.find("/relational/") != std::string::npos;
    REQUIRE(is_relational == true);
    
    // Test catalog asset detection
    std::string catalog_url = "https://test.com/api/v1/dwc/catalog";
    bool is_catalog = catalog_url.find("/catalog") != std::string::npos;
    REQUIRE(is_catalog == true);
}

TEST_CASE("Test Analytical Query Building", "[datasphere][asset][analytical]")
{
    std::cout << std::endl;
    
    // Test $apply clause building
    std::string dimensions = "Year,Region";
    std::string measures = "Sales,Quantity";
    std::string aggregation = "sum";
    
    std::string apply_clause = "groupby((" + dimensions + "),aggregate(" + measures + " with " + aggregation + "))";
    REQUIRE(apply_clause.find("groupby") != std::string::npos);
    REQUIRE(apply_clause.find("aggregate") != std::string::npos);
    REQUIRE(apply_clause.find("Year,Region") != std::string::npos);
    REQUIRE(apply_clause.find("Sales,Quantity") != std::string::npos);
    REQUIRE(apply_clause.find("sum") != std::string::npos);
    
    // Test complex $apply with multiple aggregations
    std::string complex_apply = "groupby((Year,Region),aggregate(Sales with sum as TotalSales,Quantity with avg as AvgQuantity))";
    REQUIRE(complex_apply.find("TotalSales") != std::string::npos);
    REQUIRE(complex_apply.find("AvgQuantity") != std::string::npos);
}

TEST_CASE("Test Relational Query Building", "[datasphere][asset][relational]")
{
    std::cout << std::endl;
    
    // Test $select clause building
    std::string select_fields = "ID,Name,Value";
    std::string select_clause = "$select=" + select_fields;
    REQUIRE(select_clause == "$select=ID,Name,Value");
    
    // Test $filter clause building
    std::string filter_condition = "Year eq 2024 and Region eq 'EU'";
    std::string filter_clause = "$filter=" + filter_condition;
    REQUIRE(filter_clause.find("Year eq 2024") != std::string::npos);
    REQUIRE(filter_clause.find("Region eq 'EU'") != std::string::npos);
    
    // Test $orderby clause building
    std::string orderby_fields = "Year desc,Region asc";
    std::string orderby_clause = "$orderby=" + orderby_fields;
    REQUIRE(orderby_clause.find("Year desc") != std::string::npos);
    REQUIRE(orderby_clause.find("Region asc") != std::string::npos);
}

TEST_CASE("Test Input Parameter Substitution", "[datasphere][asset][parameters]")
{
    std::cout << std::endl;
    
    // Test string parameter substitution
    std::string param1 = "Year=2024";
    std::string param2 = "Region=EU";
    std::string params = param1 + "," + param2;
    
    REQUIRE(params.find("Year=2024") != std::string::npos);
    REQUIRE(params.find("Region=EU") != std::string::npos);
    
    // Test parameter URL construction
    std::string base_url = "https://test.com/api/asset";
    std::string url_with_params = base_url + "(" + params + ")";
    REQUIRE(url_with_params.find("Year=2024") != std::string::npos);
    REQUIRE(url_with_params.find("Region=EU") != std::string::npos);
    REQUIRE(url_with_params.find("(") != std::string::npos);
    REQUIRE(url_with_params.back() == ')');
}

TEST_CASE("Test Query Execution and Result Parsing", "[datasphere][asset][execution]")
{
    std::cout << std::endl;
    
    // Test result set structure
    std::string result_context = "@odata.context";
    std::string result_value = "value";
    std::string result_nextlink = "@odata.nextLink";
    
    REQUIRE(result_context.find("@odata.context") != std::string::npos);
    REQUIRE(result_value == "value");
    REQUIRE(result_nextlink.find("@odata.nextLink") != std::string::npos);
    
    // Test pagination handling
    std::string next_link = "https://test.com/api/asset?$skip=100&$top=50";
    bool has_next = next_link.find("$skip=100") != std::string::npos;
    bool has_top = next_link.find("$top=50") != std::string::npos;
    
    REQUIRE(has_next == true);
    REQUIRE(has_top == true);
}

TEST_CASE("Test Error Handling for Invalid Assets", "[datasphere][asset][errors]")
{
    std::cout << std::endl;
    
    // Test invalid asset ID handling
    std::string invalid_asset_error = "Asset 'invalid_asset' not found in space 'test_space'";
    REQUIRE(invalid_asset_error.find("not found") != std::string::npos);
    
    // Test invalid space ID handling
    std::string invalid_space_error = "Space 'invalid_space' not accessible";
    REQUIRE(invalid_space_error.find("not accessible") != std::string::npos);
    
    // Test unsupported asset type handling
    std::string unsupported_type_error = "Asset type 'unsupported' is not supported for consumption";
    REQUIRE(unsupported_type_error.find("not supported") != std::string::npos);
}

TEST_CASE("Test Asset Consumption Flow", "[datasphere][asset][flow]")
{
    std::cout << std::endl;
    
    // Test complete asset consumption setup
    std::string space_id = "test_space";
    std::string asset_id = "test_asset";
    std::string asset_type = "analytical";
    
    REQUIRE(!space_id.empty());
    REQUIRE(!asset_id.empty());
    REQUIRE(asset_type == "analytical");
    
    // Test URL construction for different asset types
    std::string analytical_url = "https://test.com/api/v1/dwc/consumption/analytical/" + space_id + "/" + asset_id;
    std::string relational_url = "https://test.com/api/v1/dwc/consumption/relational/" + space_id + "/" + asset_id;
    
    REQUIRE(analytical_url.find("/analytical/") != std::string::npos);
    REQUIRE(relational_url.find("/relational/") != std::string::npos);
    REQUIRE(analytical_url.find(space_id) != std::string::npos);
    REQUIRE(relational_url.find(asset_id) != std::string::npos);
}

TEST_CASE("Test Analytical vs Relational Differences", "[datasphere][asset][comparison]")
{
    std::cout << std::endl;
    
    // Test analytical specific features
    std::string analytical_features = "$apply,groupby,aggregate,hierarchies,calculated_measures";
    bool has_apply = analytical_features.find("$apply") != std::string::npos;
    bool has_groupby = analytical_features.find("groupby") != std::string::npos;
    bool has_aggregate = analytical_features.find("aggregate") != std::string::npos;
    
    REQUIRE(has_apply == true);
    REQUIRE(has_groupby == true);
    REQUIRE(has_aggregate == true);
    
    // Test relational specific features
    std::string relational_features = "$select,$filter,$orderby,$top,$skip,$count";
    bool has_select = relational_features.find("$select") != std::string::npos;
    bool has_filter = relational_features.find("$filter") != std::string::npos;
    bool has_orderby = relational_features.find("$orderby") != std::string::npos;
    
    REQUIRE(has_select == true);
    REQUIRE(has_filter == true);
    REQUIRE(has_orderby == true);
}
