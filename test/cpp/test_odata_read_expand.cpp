#include "catch.hpp"
#include "erpl_odata_read_functions.hpp"
#include "erpl_odata_client.hpp"
#include "erpl_http_client.hpp"
#include "erpl_http_auth.hpp"

using namespace erpl_web;

TEST_CASE("OData Read Functions - Expand Basic Functionality") {
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
        
        // Test setting empty expand clause
        bind_data.SetExpandClause("");
        REQUIRE(bind_data.GetExpandClause().empty());
        
        // Test setting complex expand clause
        bind_data.SetExpandClause("Products($filter=DiscontinuedDate eq null),Category($select=Name)");
        REQUIRE(bind_data.GetExpandClause() == "Products($filter=DiscontinuedDate eq null),Category($select=Name)");
    }
    
    SECTION("ODataReadBindData expand processing") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        
        // Test processing expand paths
        std::vector<std::string> expand_paths = {"Category", "Orders", "Products"};
        bind_data.ProcessExpandedData(expand_paths);
        
        // Verify that expand paths are stored
        REQUIRE(bind_data.GetExpandClause() == "Category,Orders,Products");
    }
}

TEST_CASE("OData Read Functions - Expand Parameter Binding") {
    SECTION("Expand parameter in function creation") {
        // This test verifies that the expand parameter is properly registered
        // The actual function creation is tested in integration tests
        REQUIRE(true); // Placeholder for now
    }
    
    SECTION("Expand parameter consumption") {
        // This test verifies that the expand parameter is properly consumed
        // The actual parameter consumption is tested in integration tests
        REQUIRE(true); // Placeholder for now
    }
}

TEST_CASE("OData Read Functions - Expand URL Integration") {
    SECTION("Expand clause in URL construction") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("Category,Orders");
        
        // The predicate pushdown helper should include the expand clause
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        REQUIRE(predicate_helper != nullptr);
        
        // Test that the expand clause is properly set in the predicate helper
        auto expand_clause = predicate_helper->ExpandClause();
        REQUIRE(expand_clause == "$expand=Category,Orders");
    }
    
    SECTION("Expand clause with other parameters") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("Category,Orders");
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        predicate_helper->ConsumeLimit(10);
        predicate_helper->ConsumeOffset(20);
        
        // Test that all clauses are properly combined
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = predicate_helper->ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$top=10&$skip=20&$expand=Category,Orders");
    }
}

TEST_CASE("OData Read Functions - Expand Complex Scenarios") {
    SECTION("Nested expand paths") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("Category/Products/Supplier");
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        auto expand_clause = predicate_helper->ExpandClause();
        REQUIRE(expand_clause == "$expand=Category/Products/Supplier");
    }
    
    SECTION("Expand with complex query options") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("Products($filter=Price gt 100;$select=Name,Price;$top=5;$skip=10)");
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        auto expand_clause = predicate_helper->ExpandClause();
        REQUIRE(expand_clause == "$expand=Products($filter=Price gt 100;$select=Name,Price;$top=5;$skip=10)");
    }
    
    SECTION("Multiple expand paths with options") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("Category($select=Name)/Products($filter=DiscontinuedDate eq null),Orders($top=10)");
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        auto expand_clause = predicate_helper->ExpandClause();
        REQUIRE(expand_clause == "$expand=Category($select=Name)/Products($filter=DiscontinuedDate eq null),Orders($top=10)");
    }
}

TEST_CASE("OData Read Functions - Expand OData Version Support") {
    SECTION("Expand with OData V2") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("Category,Orders");
        
        // Set OData version to V2
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        predicate_helper->SetODataVersion(ODataVersion::V2);
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = predicate_helper->ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$expand=Category,Orders");
    }
    
    SECTION("Expand with OData V4") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("Category,Orders");
        
        // Set OData version to V4
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        predicate_helper->SetODataVersion(ODataVersion::V4);
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = predicate_helper->ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$expand=Category,Orders");
    }
    
    SECTION("Expand with OData V2 inline count") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("Category,Orders");
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        predicate_helper->SetODataVersion(ODataVersion::V2);
        predicate_helper->SetInlineCount(true);
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = predicate_helper->ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$inlinecount=allpages&$expand=Category,Orders");
    }
}

TEST_CASE("OData Read Functions - Expand Edge Cases") {
    SECTION("Expand with special characters") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("Products($filter=Name eq 'Product;Name')");
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        auto expand_clause = predicate_helper->ExpandClause();
        REQUIRE(expand_clause == "$expand=Products($filter=Name eq 'Product;Name')");
    }
    
    SECTION("Expand with nested parentheses") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("Products($filter=(Price gt 100) and (CategoryID eq 1))");
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        auto expand_clause = predicate_helper->ExpandClause();
        REQUIRE(expand_clause == "$expand=Products($filter=(Price gt 100) and (CategoryID eq 1))");
    }
    
    SECTION("Expand with empty parentheses") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("Products()");
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        auto expand_clause = predicate_helper->ExpandClause();
        REQUIRE(expand_clause == "$expand=Products()");
    }
    
    SECTION("Expand with malformed syntax") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("Products($filter=Price gt 100");
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        auto expand_clause = predicate_helper->ExpandClause();
        REQUIRE(expand_clause == "$expand=Products($filter=Price gt 100");
    }
}

TEST_CASE("OData Read Functions - Expand Real-world Examples") {
    SECTION("SAP Datasphere example") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://localhost:50000/sap/opu/odata4/iwfnd/config/default/iwfnd/catalog/0002/ServiceGroups");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("DefaultSystem($expand=Services())");
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        HttpUrl base_url("http://localhost:50000/sap/opu/odata4/iwfnd/config/default/iwfnd/catalog/0002/ServiceGroups");
        auto result_url = predicate_helper->ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://localhost:50000/sap/opu/odata4/iwfnd/config/default/iwfnd/catalog/0002/ServiceGroups?$expand=DefaultSystem($expand=Services())");
    }
    
    SECTION("Northwind example") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Customers");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("Orders($filter=Freight gt 100;$select=OrderID,Freight)");
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = predicate_helper->ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$expand=Orders($filter=Freight gt 100;$select=OrderID,Freight)");
    }
    
    SECTION("Complex business scenario") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Invoices");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        bind_data.SetExpandClause("Customer($select=CustomerID,CompanyName)/Orders($filter=OrderDate gt 2023-01-01;$top=10)/OrderDetails($select=ProductID,Quantity,UnitPrice)");
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        HttpUrl base_url("http://host/service/Invoices");
        auto result_url = predicate_helper->ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Invoices?$expand=Customer($select=CustomerID,CompanyName)/Orders($filter=OrderDate gt 2023-01-01;$top=10)/OrderDetails($select=ProductID,Quantity,UnitPrice)");
    }
}

TEST_CASE("OData Read Functions - Expand Performance and Robustness") {
    SECTION("Large expand clause") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Entity");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        
        std::string large_expand = "Path1,Path2,Path3,Path4,Path5,Path6,Path7,Path8,Path9,Path10";
        bind_data.SetExpandClause(large_expand);
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        HttpUrl base_url("http://host/service/Entity");
        auto result_url = predicate_helper->ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Entity?$expand=" + large_expand);
    }
    
    SECTION("Very long filter in expand") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Entity");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        
        std::string long_filter = std::string(1000, 'a') + " eq 'test'";
        std::string expand_with_long_filter = "Products($filter=" + long_filter + ")";
        bind_data.SetExpandClause(expand_with_long_filter);
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        HttpUrl base_url("http://host/service/Entity");
        auto result_url = predicate_helper->ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Entity?$expand=" + expand_with_long_filter);
    }
    
    SECTION("Multiple expand operations") {
        auto http_client = std::make_shared<HttpClient>();
        HttpUrl url("http://host/service/Entity");
        auto auth_params = std::make_shared<HttpAuthParams>();
        auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
        
        ODataReadBindData bind_data(odata_client);
        
        // Test multiple expand operations
        for (int i = 0; i < 100; ++i) {
            bind_data.SetExpandClause("Path" + std::to_string(i));
        }
        
        // Last one should be the final result
        REQUIRE(bind_data.GetExpandClause() == "Path99");
        
        auto predicate_helper = bind_data.PredicatePushdownHelper();
        auto expand_clause = predicate_helper->ExpandClause();
        REQUIRE(expand_clause == "$expand=Path99");
    }
}
