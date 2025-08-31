#include "catch.hpp"
#include "erpl_odata_predicate_pushdown_helper.hpp"
#include "erpl_http_client.hpp"

using namespace erpl_web;

TEST_CASE("OData Predicate Pushdown Helper Expand Tests") {
    std::vector<std::string> column_names = {"ID", "Name", "CategoryID"};
    ODataPredicatePushdownHelper helper(column_names);
    
    SECTION("Consume expand clause") {
        helper.ConsumeExpand("Category,Orders");
        
        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause == "$expand=Category,Orders");
    }
    
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
    
    SECTION("Empty expand clause") {
        helper.ConsumeExpand("");
        
        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause.empty());
    }
    
    SECTION("Expand clause with complex syntax") {
        helper.ConsumeExpand("Products($filter=DiscontinuedDate eq null),Category($select=Name)");
        
        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause == "$expand=Products($filter=DiscontinuedDate eq null),Category($select=Name)");
    }
}
