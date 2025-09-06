#include "catch.hpp"
#include "odata_predicate_pushdown_helper.hpp"
#include "http_client.hpp"

using namespace erpl_web;

TEST_CASE("OData Predicate Pushdown Helper - Expand Basic Functionality") {
    std::vector<std::string> column_names = {"ID", "Name", "CategoryID"};
    ODataPredicatePushdownHelper helper(column_names);
    
    SECTION("Consume expand clause") {
        helper.ConsumeExpand("Category,Orders");
        
        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause == "$expand=Category,Orders");
    }
    
    SECTION("Consume empty expand clause") {
        helper.ConsumeExpand("");
        
        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause.empty());
    }
    
    SECTION("Consume expand clause with whitespace") {
        helper.ConsumeExpand(" Category , Orders ");
        
        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause == "$expand=Category,Orders");
    }
    
    SECTION("Consume expand clause multiple times") {
        helper.ConsumeExpand("Category");
        helper.ConsumeExpand("Orders");
        
        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause == "$expand=Orders"); // Last one wins
    }
}

TEST_CASE("OData Predicate Pushdown Helper - Expand URL Construction") {
    std::vector<std::string> column_names = {"ID", "Name", "CategoryID"};
    ODataPredicatePushdownHelper helper(column_names);
    
    SECTION("Apply expand to URL") {
        helper.ConsumeExpand("Category,Orders");
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$expand=Category,Orders");
    }
    
    SECTION("Apply expand with existing query parameters") {
        helper.ConsumeExpand("Category,Orders");
        
        HttpUrl base_url("http://host/service/Customers?$select=ID,Name");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$select=ID,Name&$expand=Category,Orders");
    }
    
    SECTION("Apply expand with other clauses") {
        helper.ConsumeExpand("Category,Orders");
        helper.ConsumeLimit(10);
        helper.ConsumeOffset(20);
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$top=10&$skip=20&$expand=Category,Orders");
    }
    
    SECTION("Apply expand with complex syntax") {
        helper.ConsumeExpand("Products($filter=DiscontinuedDate eq null),Category($select=Name)");
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$expand=Products($filter=DiscontinuedDate eq null),Category($select=Name)");
    }
}

TEST_CASE("OData Predicate Pushdown Helper - Expand with OData Versions") {
    std::vector<std::string> column_names = {"ID", "Name", "CategoryID"};
    ODataPredicatePushdownHelper helper(column_names);
    
    SECTION("Expand clause with OData V2") {
        helper.SetODataVersion(ODataVersion::V2);
        helper.ConsumeExpand("Category,Orders");
        
        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause == "$expand=Category,Orders");
    }
    
    SECTION("Expand clause with OData V4") {
        helper.SetODataVersion(ODataVersion::V4);
        helper.ConsumeExpand("Category,Orders");
        
        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause == "$expand=Category,Orders");
    }
    
    SECTION("Expand clause with OData V2 and inline count") {
        helper.SetODataVersion(ODataVersion::V2);
        helper.SetInlineCount(true);
        helper.ConsumeExpand("Category,Orders");
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$inlinecount=allpages&$expand=Category,Orders");
    }
}

TEST_CASE("OData Predicate Pushdown Helper - Expand Complex Scenarios") {
    std::vector<std::string> column_names = {"ID", "Name", "CategoryID"};
    ODataPredicatePushdownHelper helper(column_names);
    
    SECTION("Expand with nested paths") {
        helper.ConsumeExpand("Category/Products/Supplier");
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$expand=Category/Products/Supplier");
    }
    
    SECTION("Expand with complex query options") {
        helper.ConsumeExpand("Products($filter=Price gt 100;$select=Name,Price;$top=5;$skip=10)");
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$expand=Products($filter=Price gt 100;$select=Name,Price;$top=5;$skip=10)");
    }
    
    SECTION("Expand with multiple complex paths") {
        helper.ConsumeExpand("Category($select=Name)/Products($filter=DiscontinuedDate eq null),Orders($top=10)");
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$expand=Category($select=Name)/Products($filter=DiscontinuedDate eq null),Orders($top=10)");
    }
}

TEST_CASE("OData Predicate Pushdown Helper - Expand Integration with Other Clauses") {
    std::vector<std::string> column_names = {"ID", "Name", "CategoryID", "Address", "City"};
    ODataPredicatePushdownHelper helper(column_names);
    
    SECTION("Expand with select clause") {
        helper.ConsumeColumnSelection({1, 2}); // Name, CategoryID
        helper.ConsumeExpand("Category,Orders");
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$select=Name,CategoryID&$expand=Category,Orders");
    }
    
    SECTION("Expand with filter clause") {
        helper.ConsumeExpand("Category,Orders");
        // Note: filter clause would be set by ConsumeFilters, but we'll test the expand integration
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$expand=Category,Orders");
    }
    
    SECTION("Expand with all clause types") {
        helper.ConsumeColumnSelection({0, 1}); // ID, Name
        helper.ConsumeExpand("Category,Orders");
        helper.ConsumeLimit(25);
        helper.ConsumeOffset(50);
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$select=ID,Name&$top=25&$skip=50&$expand=Category,Orders");
    }
}

TEST_CASE("OData Predicate Pushdown Helper - Expand Edge Cases") {
    std::vector<std::string> column_names = {"ID", "Name"};
    ODataPredicatePushdownHelper helper(column_names);
    
    SECTION("Expand with special characters in filter") {
        helper.ConsumeExpand("Products($filter=Name eq 'Product;Name')");
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$expand=Products($filter=Name eq 'Product;Name')");
    }
    
    SECTION("Expand with nested parentheses") {
        helper.ConsumeExpand("Products($filter=(Price gt 100) and (CategoryID eq 1))");
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$expand=Products($filter=(Price gt 100) and (CategoryID eq 1))");
    }
    
    SECTION("Expand with function calls") {
        helper.ConsumeExpand("Products($filter=startswith(Name,'A') eq true)");
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$expand=Products($filter=startswith(Name,'A') eq true)");
    }
    
    SECTION("Expand with empty parentheses") {
        helper.ConsumeExpand("Products()");
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$expand=Products()");
    }
}

TEST_CASE("OData Predicate Pushdown Helper - Expand Real-world Examples") {
    std::vector<std::string> column_names = {"CustomerID", "CompanyName", "ContactName"};
    ODataPredicatePushdownHelper helper(column_names);
    
    SECTION("SAP Datasphere example") {
        helper.ConsumeExpand("DefaultSystem($expand=Services())");
        
        HttpUrl base_url("http://localhost:50000/sap/opu/odata4/iwfnd/config/default/iwfnd/catalog/0002/ServiceGroups");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://localhost:50000/sap/opu/odata4/iwfnd/config/default/iwfnd/catalog/0002/ServiceGroups?$expand=DefaultSystem($expand=Services())");
    }
    
    SECTION("Northwind example") {
        helper.ConsumeExpand("Orders($filter=Freight gt 100;$select=OrderID,Freight)");
        
        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Customers?$expand=Orders($filter=Freight gt 100;$select=OrderID,Freight)");
    }
    
    SECTION("Complex business scenario") {
        helper.ConsumeExpand("Customer($select=CustomerID,CompanyName)/Orders($filter=OrderDate gt 2023-01-01;$top=10)/OrderDetails($select=ProductID,Quantity,UnitPrice)");
        
        HttpUrl base_url("http://host/service/Invoices");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Invoices?$expand=Customer($select=CustomerID,CompanyName)/Orders($filter=OrderDate gt 2023-01-01;$top=10)/OrderDetails($select=ProductID,Quantity,UnitPrice)");
    }
}

TEST_CASE("OData Predicate Pushdown Helper - Expand Performance and Robustness") {
    std::vector<std::string> column_names = {"ID", "Name"};
    ODataPredicatePushdownHelper helper(column_names);
    
    SECTION("Large expand clause") {
        std::string large_expand = "Path1,Path2,Path3,Path4,Path5,Path6,Path7,Path8,Path9,Path10";
        helper.ConsumeExpand(large_expand);
        
        HttpUrl base_url("http://host/service/Entity");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Entity?$expand=" + large_expand);
    }
    
    SECTION("Very long filter in expand") {
        std::string long_filter = std::string(1000, 'a') + " eq 'test'";
        std::string expand_with_long_filter = "Products($filter=" + long_filter + ")";
        helper.ConsumeExpand(expand_with_long_filter);
        
        HttpUrl base_url("http://host/service/Entity");
        auto result_url = helper.ApplyFiltersToUrl(base_url);
        
        REQUIRE(result_url.ToString() == "http://host/service/Entity?$expand=" + expand_with_long_filter);
    }
    
    SECTION("Multiple expand operations") {
        for (int i = 0; i < 100; ++i) {
            helper.ConsumeExpand("Path" + std::to_string(i));
        }
        
        // Last one should be the final result
        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause == "$expand=Path99");
    }
}
