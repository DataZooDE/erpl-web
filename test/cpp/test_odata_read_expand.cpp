#include "catch.hpp"
#include "erpl_odata_read_functions.hpp"
#include "erpl_odata_client.hpp"
#include "erpl_http_client.hpp"
#include "erpl_http_auth.hpp"

using namespace erpl_web;

TEST_CASE("OData Read Functions Expand Tests") {
    SECTION("ODataReadBindData expand support") {
        // Create a mock OData client
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        
        // Test setting expand clause
        bind_data.SetExpandClause("Category,Orders");
        REQUIRE(bind_data.GetExpandClause() == "Category,Orders");
        
        // Test expand paths parsing
        std::vector<std::string> expand_paths = {"Category", "Orders"};
        bind_data.ProcessExpandedData(expand_paths);
        
        // Verify expand paths were set
        // Note: This would require adding getter methods to ODataReadBindData
    }
    
    SECTION("Expand parameter in function binding") {
        // This test would require mocking the DuckDB binding context
        // For now, we'll test the parameter handling logic separately
        
        std::string expand_value = "Category,Orders";
        
        // Test that expand value can be parsed
        auto paths = ODataExpandParser::ParseExpandClause(expand_value);
        REQUIRE(paths.size() == 2);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[1].navigation_property == "Orders");
    }
    
    SECTION("Expand with URL parsing") {
        // Test parsing expand from URL
        std::string url_with_expand = "http://host/service/Customers?$expand=Category,Orders";
        
        // Extract expand parameter from URL
        size_t expand_pos = url_with_expand.find("$expand=");
        REQUIRE(expand_pos != std::string::npos);
        
        size_t start_pos = expand_pos + 9; // length of "$expand="
        size_t end_pos = url_with_expand.find('&', start_pos);
        if (end_pos == std::string::npos) {
            end_pos = url_with_expand.length();
        }
        
        std::string expand_clause = url_with_expand.substr(start_pos, end_pos - start_pos);
        REQUIRE(expand_clause == "Category,Orders");
        
        // Parse the expand clause
        auto paths = ODataExpandParser::ParseExpandClause(expand_clause);
        REQUIRE(paths.size() == 2);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[1].navigation_property == "Orders");
    }
    
    SECTION("Complex expand syntax parsing") {
        std::string complex_expand = "Products($filter=DiscontinuedDate eq null;$select=Name,Price),Category($select=Name)";
        
        auto paths = ODataExpandParser::ParseExpandClause(complex_expand);
        REQUIRE(paths.size() == 2);
        
        // First path: Products with filter and select
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].filter_clause == "DiscontinuedDate eq null");
        REQUIRE(paths[0].select_clause == "Name,Price");
        
        // Second path: Category with select
        REQUIRE(paths[1].navigation_property == "Category");
        REQUIRE(paths[1].select_clause == "Name");
    }
    
    SECTION("Nested expand path parsing") {
        std::string nested_expand = "Category/Products/Supplier";
        
        auto paths = ODataExpandParser::ParseExpandClause(nested_expand);
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[0].sub_expands.size() == 2);
        REQUIRE(paths[0].sub_expands[0] == "Products");
        REQUIRE(paths[0].sub_expands[1] == "Supplier");
    }
}
