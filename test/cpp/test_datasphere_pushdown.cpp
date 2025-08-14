#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "erpl_datasphere_pushdown.hpp"
#include <iostream>

using namespace erpl_web;
using namespace std;

TEST_CASE("Test GROUP BY to $apply=groupby() Translation", "[datasphere][pushdown][groupby]")
{
    std::cout << std::endl;
    
    // Test basic GROUP BY translation
    std::vector<std::string> dimensions = {"Year", "Region"};
    std::map<std::string, std::string> aggregations = {{"Sales", "sum"}};
    
    std::string apply_clause = DatasphereAnalyticalPushdownHelper::BuildApplyClauseWithAggregation(
        dimensions, aggregations);
    
    REQUIRE(apply_clause.find("groupby") != std::string::npos);
    REQUIRE(apply_clause.find("aggregate") != std::string::npos);
    REQUIRE(apply_clause.find("Year,Region") != std::string::npos);
    REQUIRE(apply_clause.find("Sales with sum") != std::string::npos);
    
    // Test single dimension
    std::vector<std::string> single_dim = {"Year"};
    std::string single_apply = DatasphereAnalyticalPushdownHelper::BuildApplyClauseWithAggregation(
        single_dim, aggregations);
    
    REQUIRE(single_apply.find("Year") != std::string::npos);
    REQUIRE(single_apply.find("Year,Region") == std::string::npos); // Should not contain multiple dimensions
}

TEST_CASE("Test Aggregate Function Mapping", "[datasphere][pushdown][aggregate]")
{
    std::cout << std::endl;
    
    // Test all supported aggregate functions
    std::vector<std::string> dimensions = {"Year"};
    std::map<std::string, std::string> test_aggregations = {
        {"Sales", "sum"},
        {"Quantity", "average"},
        {"Orders", "count"},
        {"MinValue", "min"},
        {"MaxValue", "max"}
    };
    
    std::string apply_clause = DatasphereAnalyticalPushdownHelper::BuildApplyClauseWithAggregation(
        dimensions, test_aggregations);
    
    REQUIRE(apply_clause.find("Sales with sum") != std::string::npos);
    REQUIRE(apply_clause.find("Quantity with average") != std::string::npos);
    REQUIRE(apply_clause.find("Orders with count") != std::string::npos);
    REQUIRE(apply_clause.find("MinValue with min") != std::string::npos);
    REQUIRE(apply_clause.find("MaxValue with max") != std::string::npos);
    
    // Test aggregation function validation
    REQUIRE(DatasphereAnalyticalPushdownHelper::ValidateAggregationFunction("sum") == "sum");
    REQUIRE(DatasphereAnalyticalPushdownHelper::ValidateAggregationFunction("average") == "average");
    REQUIRE(DatasphereAnalyticalPushdownHelper::ValidateAggregationFunction("count") == "count");
    REQUIRE(DatasphereAnalyticalPushdownHelper::ValidateAggregationFunction("min") == "min");
    REQUIRE(DatasphereAnalyticalPushdownHelper::ValidateAggregationFunction("max") == "max");
    REQUIRE(DatasphereAnalyticalPushdownHelper::ValidateAggregationFunction("invalid") == "");
}

TEST_CASE("Test Complex Analytical Queries with Multiple Dimensions", "[datasphere][pushdown][complex]")
{
    std::cout << std::endl;
    
    // Test complex query with multiple dimensions and aggregations
    std::vector<std::string> dimensions = {"Year", "Region", "Product", "Customer"};
    std::map<std::string, std::string> aggregations = {
        {"Sales", "sum"},
        {"Quantity", "sum"},
        {"Orders", "count"},
        {"Revenue", "sum"}
    };
    
    std::string apply_clause = DatasphereAnalyticalPushdownHelper::BuildApplyClauseWithAggregation(
        dimensions, aggregations);
    
    REQUIRE(apply_clause.find("Year,Region,Product,Customer") != std::string::npos);
    REQUIRE(apply_clause.find("Sales with sum") != std::string::npos);
    REQUIRE(apply_clause.find("Quantity with sum") != std::string::npos);
    REQUIRE(apply_clause.find("Orders with count") != std::string::npos);
    REQUIRE(apply_clause.find("Revenue with sum") != std::string::npos);
    
    // Test query components structure
    AnalyticalQueryComponents components;
    components.dimensions = dimensions;
    components.aggregations = aggregations;
    components.filter_clause = "Year eq 2024";
    components.orderby_clause = "Sales desc";
    components.top_limit = 100;
    
    std::string full_apply = DatasphereAnalyticalPushdownHelper::BuildApplyClause(components);
    
    REQUIRE(full_apply.find("groupby") != std::string::npos);
    REQUIRE(full_apply.find("filter") != std::string::npos);
    REQUIRE(full_apply.find("orderby") != std::string::npos);
    REQUIRE(full_apply.find("top") != std::string::npos);
}

TEST_CASE("Test Hierarchy Navigation Support", "[datasphere][pushdown][hierarchy]")
{
    std::cout << std::endl;
    
    // Test basic hierarchy navigation
    HierarchyNavigation hierarchy("TimeHierarchy");
    hierarchy.levels = {"Year", "Quarter", "Month"};
    hierarchy.drill_path = "2024/Q1/January";
    
    std::vector<std::string> dimensions = {"Product"};
    std::map<std::string, std::string> aggregations = {{"Sales", "sum"}};
    
    std::string apply_clause = DatasphereAnalyticalPushdownHelper::BuildApplyClauseWithHierarchy(
        dimensions, hierarchy, aggregations);
    
    REQUIRE(apply_clause.find("TimeHierarchy") != std::string::npos);
    REQUIRE(apply_clause.find("Year,Quarter,Month") != std::string::npos);
    REQUIRE(apply_clause.find("2024/Q1/January") != std::string::npos);
    
    // Test hierarchy without drill path
    HierarchyNavigation simple_hierarchy("ProductHierarchy");
    simple_hierarchy.levels = {"Category", "Subcategory"};
    
    std::string simple_apply = DatasphereAnalyticalPushdownHelper::BuildApplyClauseWithHierarchy(
        dimensions, simple_hierarchy, aggregations);
    
    REQUIRE(simple_apply.find("ProductHierarchy") != std::string::npos);
    REQUIRE(simple_apply.find("Category,Subcategory") != std::string::npos);
}

TEST_CASE("Test Calculated Measures", "[datasphere][pushdown][calculated]")
{
    std::cout << std::endl;
    
    // Test calculated measures in analytical queries
    std::vector<std::string> dimensions = {"Year", "Region"};
    std::map<std::string, std::string> aggregations = {{"Sales", "sum"}};
    
    std::vector<CalculatedMeasure> calculated_measures = {
        {"ProfitMargin", "Sales/Revenue*100", "decimal"},
        {"AverageOrderValue", "Sales/Orders", "decimal"},
        {"GrowthRate", "(Sales - PreviousSales)/PreviousSales*100", "decimal"}
    };
    
    std::string apply_clause = DatasphereAnalyticalPushdownHelper::BuildApplyClauseWithCalculatedMeasures(
        dimensions, calculated_measures, aggregations);
    
    REQUIRE(apply_clause.find("ProfitMargin as Sales/Revenue*100") != std::string::npos);
    REQUIRE(apply_clause.find("AverageOrderValue as Sales/Orders") != std::string::npos);
    REQUIRE(apply_clause.find("GrowthRate as (Sales - PreviousSales)/PreviousSales*100") != std::string::npos);
    
    // Test with no calculated measures
    std::vector<CalculatedMeasure> empty_calculated;
    std::string no_calc_apply = DatasphereAnalyticalPushdownHelper::BuildApplyClauseWithCalculatedMeasures(
        dimensions, empty_calculated, aggregations);
    
    REQUIRE(no_calc_apply.find("ProfitMargin") == std::string::npos);
    REQUIRE(no_calc_apply.find("groupby") != std::string::npos);
}

TEST_CASE("Test Input Variable Substitution", "[datasphere][pushdown][variables]")
{
    std::cout << std::endl;
    
    // Test input parameter substitution
    std::string query = "SELECT * FROM analytical_model WHERE Year = {year} AND Region = '{region}'";
    std::map<std::string, std::string> parameters = {
        {"year", "2024"},
        {"region", "EU"}
    };
    
    std::string substituted = DatasphereAnalyticalPushdownHelper::SubstituteInputParameters(query, parameters);
    
    REQUIRE(substituted.find("2024") != std::string::npos);
    REQUIRE(substituted.find("EU") != std::string::npos);
    REQUIRE(substituted.find("{year}") == std::string::npos);
    REQUIRE(substituted.find("{region}") == std::string::npos);
    
    // Test with no parameters
    std::map<std::string, std::string> empty_params;
    std::string no_sub = DatasphereAnalyticalPushdownHelper::SubstituteInputParameters(query, empty_params);
    
    REQUIRE(no_sub == query); // Should remain unchanged
}

TEST_CASE("Test SQL to OData Translation", "[datasphere][pushdown][translation]")
{
    std::cout << std::endl;
    
    // Test SQL WHERE to OData $filter translation
    std::string sql_where = "Year = 2024 AND Region = 'EU' OR Country = 'Germany'";
    std::string filter_clause = DatasphereAnalyticalPushdownHelper::BuildFilterClause(sql_where);
    
    REQUIRE(filter_clause.find(" eq ") != std::string::npos);
    REQUIRE(filter_clause.find(" and ") != std::string::npos);
    REQUIRE(filter_clause.find(" or ") != std::string::npos);
    
    // Test SQL ORDER BY to OData $orderby translation
    std::string sql_orderby = "Sales DESC, Region ASC";
    std::string orderby_clause = DatasphereAnalyticalPushdownHelper::BuildOrderByClause(sql_orderby);
    
    REQUIRE(orderby_clause.find("Sales desc") != std::string::npos);
    REQUIRE(orderby_clause.find("Region asc") != std::string::npos);
    
    // Test SQL LIMIT/OFFSET to OData $top/$skip translation
    std::string top_skip = DatasphereAnalyticalPushdownHelper::BuildTopSkipClause(100, 50);
    
    REQUIRE(top_skip.find("skip(50)") != std::string::npos);
    REQUIRE(top_skip.find("top(100)") != std::string::npos);
}

TEST_CASE("Test Query Validation", "[datasphere][pushdown][validation]")
{
    std::cout << std::endl;
    
    // Test valid query components
    AnalyticalQueryComponents valid_components;
    valid_components.dimensions = {"Year", "Region"};
    valid_components.aggregations = {{"Sales", "sum"}};
    valid_components.top_limit = 100;
    valid_components.skip_offset = 0;
    
    REQUIRE(DatasphereAnalyticalPushdownHelper::ValidateAnalyticalQuery(valid_components) == true);
    
    // Test invalid query - no dimensions
    AnalyticalQueryComponents invalid_no_dim;
    invalid_no_dim.aggregations = {{"Sales", "sum"}};
    
    REQUIRE(DatasphereAnalyticalPushdownHelper::ValidateAnalyticalQuery(invalid_no_dim) == false);
    
    // Test invalid query - no aggregations
    AnalyticalQueryComponents invalid_no_agg;
    invalid_no_agg.dimensions = {"Year"};
    
    REQUIRE(DatasphereAnalyticalPushdownHelper::ValidateAnalyticalQuery(invalid_no_agg) == false);
    
    // Test invalid query - negative limits
    AnalyticalQueryComponents invalid_limits;
    invalid_limits.dimensions = {"Year"};
    invalid_limits.aggregations = {{"Sales", "sum"}};
    invalid_limits.top_limit = -1;
    
    REQUIRE(DatasphereAnalyticalPushdownHelper::ValidateAnalyticalQuery(invalid_limits) == false);
}

TEST_CASE("Test URL Parameter Generation", "[datasphere][pushdown][url]")
{
    std::cout << std::endl;
    
    // Test $apply URL parameter generation
    std::string apply_clause = "groupby((Year,Region),aggregate(Sales with sum))";
    std::string url_params = DatasphereAnalyticalPushdownHelper::GenerateApplyUrlParameters(apply_clause);
    
    REQUIRE(url_params == "$apply=" + apply_clause);
    
    // Test empty apply clause
    std::string empty_params = DatasphereAnalyticalPushdownHelper::GenerateApplyUrlParameters("");
    REQUIRE(empty_params.empty());
    
    // Test combining multiple operations
    std::vector<std::string> operations = {
        "groupby((Year),aggregate(Sales with sum))",
        "filter(Year eq 2024)",
        "orderby(Sales desc)"
    };
    
    std::string combined = DatasphereAnalyticalPushdownHelper::CombineApplyOperations(operations);
    
    REQUIRE(combined.find("groupby") != std::string::npos);
    REQUIRE(combined.find("filter") != std::string::npos);
    REQUIRE(combined.find("orderby") != std::string::npos);
    REQUIRE(combined.find("/") != std::string::npos); // Should separate operations
}

TEST_CASE("Test SQL Parsing Functions", "[datasphere][pushdown][parsing]")
{
    std::cout << std::endl;
    
    // Test GROUP BY clause parsing
    std::string sql_groupby = "Year, Region, Product";
    std::vector<std::string> parsed_dimensions = DatasphereAnalyticalPushdownHelper::ParseGroupByClause(sql_groupby);
    
    REQUIRE(parsed_dimensions.size() == 3);
    REQUIRE(parsed_dimensions[0] == "Year");
    REQUIRE(parsed_dimensions[1] == "Region");
    REQUIRE(parsed_dimensions[2] == "Product");
    
    // Test aggregate function parsing
    std::string sql_select = "SUM(Sales) as TotalSales, AVG(Quantity) as AvgQuantity, COUNT(Orders) as OrderCount";
    std::map<std::string, std::string> parsed_aggregations = DatasphereAnalyticalPushdownHelper::ParseAggregateFunctions(sql_select);
    
    REQUIRE(parsed_aggregations.size() == 3);
    REQUIRE(parsed_aggregations["Sales"] == "sum");
    REQUIRE(parsed_aggregations["Quantity"] == "average");
    REQUIRE(parsed_aggregations["Orders"] == "count");
    
    // Test empty input parsing
    std::vector<std::string> empty_dim = DatasphereAnalyticalPushdownHelper::ParseGroupByClause("");
    std::map<std::string, std::string> empty_agg = DatasphereAnalyticalPushdownHelper::ParseAggregateFunctions("");
    
    REQUIRE(empty_dim.empty());
    REQUIRE(empty_agg.empty());
}
