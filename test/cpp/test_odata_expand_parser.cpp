#include "catch.hpp"
#include "odata_expand_parser.hpp"

using namespace erpl_web;

TEST_CASE("OData Expand Parser - Basic Functionality") {
    SECTION("Empty expand clause") {
        auto paths = ODataExpandParser::ParseExpandClause("");
        REQUIRE(paths.empty());
    }
    
    SECTION("Simple expand") {
        auto paths = ODataExpandParser::ParseExpandClause("Category");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[0].sub_expands.empty());
        REQUIRE(paths[0].filter_clause.empty());
        REQUIRE(paths[0].select_clause.empty());
        REQUIRE(paths[0].top_clause.empty());
        REQUIRE(paths[0].skip_clause.empty());
    }
    
    SECTION("Multiple simple expands") {
        auto paths = ODataExpandParser::ParseExpandClause("Category,Orders");
        REQUIRE(paths.size() == 2);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[1].navigation_property == "Orders");
    }
    
    SECTION("Whitespace handling") {
        auto paths = ODataExpandParser::ParseExpandClause(" Category , Orders ");
        REQUIRE(paths.size() == 2);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[1].navigation_property == "Orders");
    }
}

TEST_CASE("OData Expand Parser - Nested Expands") {
    SECTION("Single level nested expand") {
        auto paths = ODataExpandParser::ParseExpandClause("Category/Products");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[0].sub_expands.size() == 1);
        REQUIRE(paths[0].sub_expands[0] == "Products");
    }
    
    SECTION("Multiple level nested expand") {
        auto paths = ODataExpandParser::ParseExpandClause("Category/Products/Supplier");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[0].sub_expands.size() == 2);
        REQUIRE(paths[0].sub_expands[0] == "Products");
        REQUIRE(paths[0].sub_expands[1] == "Supplier");
    }
    
    SECTION("Multiple nested paths") {
        auto paths = ODataExpandParser::ParseExpandClause("Category/Products,Orders/OrderDetails");
        REQUIRE(paths.size() == 2);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[0].sub_expands.size() == 1);
        REQUIRE(paths[0].sub_expands[0] == "Products");
        REQUIRE(paths[1].navigation_property == "Orders");
        REQUIRE(paths[1].sub_expands.size() == 1);
        REQUIRE(paths[1].sub_expands[0] == "OrderDetails");
    }
}

TEST_CASE("OData Expand Parser - OData V2 Query Options") {
    SECTION("Expand with filter") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($filter=DiscontinuedDate eq null)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].filter_clause == "$filter=DiscontinuedDate eq null");
        REQUIRE(paths[0].select_clause.empty());
        REQUIRE(paths[0].top_clause.empty());
        REQUIRE(paths[0].skip_clause.empty());
    }
    
    SECTION("Expand with select") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($select=Name,Price)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].select_clause == "$select=Name,Price");
        REQUIRE(paths[0].filter_clause.empty());
        REQUIRE(paths[0].top_clause.empty());
        REQUIRE(paths[0].skip_clause.empty());
    }
    
    SECTION("Expand with top") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($top=10)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].top_clause == "$top=10");
        REQUIRE(paths[0].filter_clause.empty());
        REQUIRE(paths[0].select_clause.empty());
        REQUIRE(paths[0].skip_clause.empty());
    }
    
    SECTION("Expand with skip") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($skip=20)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].skip_clause == "$skip=20");
        REQUIRE(paths[0].filter_clause.empty());
        REQUIRE(paths[0].select_clause.empty());
        REQUIRE(paths[0].top_clause.empty());
    }
}

TEST_CASE("OData Expand Parser - OData V4 Query Options") {
    SECTION("Expand with multiple options separated by semicolon") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($filter=Price gt 100;$select=Name,Price;$top=5)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].filter_clause == "$filter=Price gt 100");
        REQUIRE(paths[0].select_clause == "$select=Name,Price");
        REQUIRE(paths[0].top_clause == "$top=5");
        REQUIRE(paths[0].skip_clause.empty());
    }
    
    SECTION("Expand with complex filter expressions") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($filter=Price gt 100 and CategoryID eq 1)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].filter_clause == "$filter=Price gt 100 and CategoryID eq 1");
    }
    
    SECTION("Expand with function calls in filter") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($filter=startswith(Name,'A') eq true)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].filter_clause == "$filter=startswith(Name,'A') eq true");
    }
}

TEST_CASE("OData Expand Parser - Complex Scenarios") {
    SECTION("Nested expand with options") {
        auto paths = ODataExpandParser::ParseExpandClause("Category($select=Name)/Products($filter=DiscontinuedDate eq null)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[0].select_clause == "$select=Name");
        REQUIRE(paths[0].sub_expands.size() == 1);
        REQUIRE(paths[0].sub_expands[0] == "Products");
        // Note: nested options are not parsed in this simple implementation
    }
    
    SECTION("Multiple expands with mixed options") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($filter=Price gt 100;$select=Name,Price),Category($select=Name)");
        REQUIRE(paths.size() == 2);
        
        // First path: Products with filter and select
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].filter_clause == "$filter=Price gt 100");
        REQUIRE(paths[0].select_clause == "$select=Name,Price");
        
        // Second path: Category with select
        REQUIRE(paths[1].navigation_property == "Category");
        REQUIRE(paths[1].select_clause == "$select=Name");
    }
    
    SECTION("Expand with complex nested structure") {
        auto paths = ODataExpandParser::ParseExpandClause("Category($select=Name)/Products($filter=Price gt 100;$select=Name,Price;$top=10)/Supplier($select=CompanyName,Country)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Category");
        REQUIRE(paths[0].select_clause == "$select=Name");
        REQUIRE(paths[0].sub_expands.size() == 2);
        REQUIRE(paths[0].sub_expands[0] == "Products");
        REQUIRE(paths[0].sub_expands[1] == "Supplier");
    }
}

TEST_CASE("OData Expand Parser - Edge Cases") {
    SECTION("Expand with empty parentheses") {
        auto paths = ODataExpandParser::ParseExpandClause("Products()");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].filter_clause.empty());
        REQUIRE(paths[0].select_clause.empty());
    }
    
    SECTION("Expand with malformed parentheses") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($filter=Price gt 100");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].filter_clause.empty()); // Malformed, so no filter parsed
    }
    
    SECTION("Expand with nested parentheses in filter") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($filter=(Price gt 100) and (CategoryID eq 1))");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].filter_clause == "$filter=(Price gt 100) and (CategoryID eq 1)");
    }
    
    SECTION("Expand with semicolon in filter value") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($filter=Name eq 'Product;Name')");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].filter_clause == "$filter=Name eq 'Product;Name'");
    }
}

TEST_CASE("OData Expand Parser - Build Expand Clause") {
    SECTION("Build simple expand clause") {
        std::vector<ODataExpandParser::ExpandPath> paths;
        
        ODataExpandParser::ExpandPath path1;
        path1.navigation_property = "Category";
        paths.push_back(path1);
        
        auto clause = ODataExpandParser::BuildExpandClause(paths);
        REQUIRE(clause == "Category");
    }
    
    SECTION("Build expand clause with options") {
        std::vector<ODataExpandParser::ExpandPath> paths;
        
        ODataExpandParser::ExpandPath path1;
        path1.navigation_property = "Products";
        path1.filter_clause = "$filter=Price gt 100";
        path1.select_clause = "$select=Name,Price";
        paths.push_back(path1);
        
        auto clause = ODataExpandParser::BuildExpandClause(paths);
        REQUIRE(clause == "Products($filter=Price gt 100;$select=Name,Price)");
    }
    
    SECTION("Build expand clause with nested paths") {
        std::vector<ODataExpandParser::ExpandPath> paths;
        
        ODataExpandParser::ExpandPath path1;
        path1.navigation_property = "Category";
        path1.sub_expands = {"Products", "Supplier"};
        paths.push_back(path1);
        
        auto clause = ODataExpandParser::BuildExpandClause(paths);
        REQUIRE(clause == "Category/Products/Supplier");
    }
    
    SECTION("Build expand clause with multiple paths") {
        std::vector<ODataExpandParser::ExpandPath> paths;
        
        ODataExpandParser::ExpandPath path1;
        path1.navigation_property = "Category";
        path1.select_clause = "$select=Name";
        paths.push_back(path1);
        
        ODataExpandParser::ExpandPath path2;
        path2.navigation_property = "Orders";
        path2.filter_clause = "$filter=Total gt 1000";
        paths.push_back(path2);
        
        auto clause = ODataExpandParser::BuildExpandClause(paths);
        REQUIRE(clause == "Category($select=Name),Orders($filter=Total gt 1000)");
    }
}

TEST_CASE("OData Expand Parser - Real-world Examples") {
    SECTION("SAP Datasphere example") {
        auto paths = ODataExpandParser::ParseExpandClause("DefaultSystem($expand=Services())");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "DefaultSystem");
        REQUIRE(paths[0].sub_expands.size() == 1);
        REQUIRE(paths[0].sub_expands[0] == "Services");
    }
    
    SECTION("Northwind example") {
        auto paths = ODataExpandParser::ParseExpandClause("Orders($filter=Freight gt 100;$select=OrderID,Freight)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Orders");
        REQUIRE(paths[0].filter_clause == "$filter=Freight gt 100");
        REQUIRE(paths[0].select_clause == "$select=OrderID,Freight");
    }
    
    SECTION("Complex business scenario") {
        auto paths = ODataExpandParser::ParseExpandClause("Customer($select=CustomerID,CompanyName)/Orders($filter=OrderDate gt 2023-01-01;$top=10)/OrderDetails($select=ProductID,Quantity,UnitPrice)");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Customer");
        REQUIRE(paths[0].select_clause == "$select=CustomerID,CompanyName");
        REQUIRE(paths[0].sub_expands.size() == 2);
        REQUIRE(paths[0].sub_expands[0] == "Orders");
        REQUIRE(paths[0].sub_expands[1] == "OrderDetails");
    }
}

TEST_CASE("OData Expand Parser - Performance and Robustness") {
    SECTION("Large number of expand paths") {
        std::string large_expand = "Path1,Path2,Path3,Path4,Path5,Path6,Path7,Path8,Path9,Path10";
        auto paths = ODataExpandParser::ParseExpandClause(large_expand);
        REQUIRE(paths.size() == 10);
        for (size_t i = 0; i < 10; ++i) {
            REQUIRE(paths[i].navigation_property == "Path" + std::to_string(i + 1));
        }
    }
    
    SECTION("Very long filter expressions") {
        std::string long_filter = "Products($filter=" + std::string(1000, 'a') + " eq 'test')";
        auto paths = ODataExpandParser::ParseExpandClause(long_filter);
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].filter_clause.length() > 1000);
    }
    
    SECTION("Unicode and special characters") {
        auto paths = ODataExpandParser::ParseExpandClause("Products($filter=Name eq 'Product-Name_123')");
        REQUIRE(paths.size() == 1);
        REQUIRE(paths[0].navigation_property == "Products");
        REQUIRE(paths[0].filter_clause == "$filter=Name eq 'Product-Name_123'");
    }
}
