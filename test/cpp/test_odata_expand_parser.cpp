#include "catch.hpp"
#include "erpl_odata_expand_parser.hpp"

using namespace erpl_web;

TEST_CASE("OData Expand Parser Tests") {
    SECTION("Simple expand") {
        auto paths = ODataExpandParser::ParseExpandClause("Category");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[0].sub_expands.empty());
        REQUIRE(paths[0].filter_clause.empty());
        REQUIRE(paths[0].select_clause.empty());
    }
    
    SECTION("Multiple simple expands") {
        auto paths = ODataExpandParser::ParseExpandClause("Category,Orders");
        REQUIRE(paths.size() == 2);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[1].navigation_property == "Orders");
    }
    
    SECTION("Nested expand") {
        auto paths = ODataExpandParser::ParseExpandClause("Category/Products");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[0].sub_expands.size() == 1);
        REQUIRE(paths[0].sub_expands[0] == "Products");
    }
    
    SECTION("Expand with filter") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($filter=DiscontinuedDate eq null)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].filter_clause == "DiscontinuedDate eq null");
    }
    
    SECTION("Expand with select") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($select=Name,Price)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].select_clause == "Name,Price");
    }
    
    SECTION("Expand with top") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($top=10)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].top_clause == "10");
    }
    
    SECTION("Expand with skip") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($skip=20)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].skip_clause == "20");
    }
    
    SECTION("Complex expand with multiple options") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($filter=Price gt 100;$select=Name,Price;$top=5)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].filter_clause == "Price gt 100");
        REQUIRE(paths[0].select_clause == "Name,Price");
        REQUIRE(paths[0].top_clause == "5");
    }
    
    SECTION("Nested expand with options") {
        auto paths = ODataExpandParser::ParseExpandClause("Category($select=Name)/Products($filter=DiscontinuedDate eq null)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[0].select_clause == "Name");
        REQUIRE(paths[0].sub_expands.size() == 1);
        REQUIRE(paths[0].sub_expands[0] == "Products");
        // Note: nested options are not parsed in this simple implementation
    }
    
    SECTION("Empty expand clause") {
        auto paths = ODataExpandParser::ParseExpandClause("");
        REQUIRE(paths.empty());
    }
    
    SECTION("Whitespace handling") {
        auto paths = ODataExpandParser::ParseExpandClause(" Category , Orders ");
        REQUIRE(paths.size() == 2);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[1].navigation_property == "Orders");
    }
    
    SECTION("Build expand clause from paths") {
        std::vector<ODataExpandParser::ExpandPath> paths;
        
        ODataExpandParser::ExpandPath path1;
        path1.navigation_property = "Category";
        paths.push_back(path1);
        
        ODataExpandParser::ExpandPath path2;
        path2.navigation_property = "Orders";
        path2.filter_clause = "Total gt 100";
        paths.push_back(path2);
        
        auto clause = ODataExpandParser::BuildExpandClause(paths);
        REQUIRE(clause == "Category,Orders($filter=Total gt 100)");
    }
}
