#pragma once
#include <string>
#include <vector>
#include <map>
#include <memory>

namespace erpl_web {

// Forward declarations
class DatasphereAnalyticalBindData;

// Structure for analytical query components
struct AnalyticalQueryComponents {
    std::vector<std::string> dimensions;
    std::vector<std::string> measures;
    std::map<std::string, std::string> aggregations; // measure -> aggregation function
    std::string filter_clause;
    std::string orderby_clause;
    int top_limit;
    int skip_offset;
    bool count_only;
    
    AnalyticalQueryComponents() : top_limit(-1), skip_offset(0), count_only(false) {}
};

// Structure for hierarchy navigation
struct HierarchyNavigation {
    std::string hierarchy_name;
    std::vector<std::string> levels;
    std::string drill_path;
    
    HierarchyNavigation() = default;
    HierarchyNavigation(const std::string& name) : hierarchy_name(name) {}
};

// Structure for calculated measures
struct CalculatedMeasure {
    std::string name;
    std::string expression;
    std::string data_type;
    
    CalculatedMeasure() = default;
    CalculatedMeasure(const std::string& n, const std::string& expr, const std::string& type = "") 
        : name(n), expression(expr), data_type(type) {}
};

// Main analytical pushdown helper class
class DatasphereAnalyticalPushdownHelper {
public:
    // Build $apply clause from SQL components
    static std::string BuildApplyClause(const AnalyticalQueryComponents& components);
    
    // Build $apply clause with aggregation
    static std::string BuildApplyClauseWithAggregation(
        const std::vector<std::string>& dimensions,
        const std::map<std::string, std::string>& aggregations);
    
    // Build $apply clause with hierarchy navigation
    static std::string BuildApplyClauseWithHierarchy(
        const std::vector<std::string>& dimensions,
        const HierarchyNavigation& hierarchy,
        const std::map<std::string, std::string>& aggregations);
    
    // Build $apply clause with calculated measures
    static std::string BuildApplyClauseWithCalculatedMeasures(
        const std::vector<std::string>& dimensions,
        const std::vector<CalculatedMeasure>& calculated_measures,
        const std::map<std::string, std::string>& aggregations);
    
    // Build $filter clause from SQL WHERE
    static std::string BuildFilterClause(const std::string& sql_where);
    
    // Build $orderby clause from SQL ORDER BY
    static std::string BuildOrderByClause(const std::string& sql_orderby);
    
    // Build $top and $skip from SQL LIMIT/OFFSET
    static std::string BuildTopSkipClause(int limit, int offset);
    
    // Build $count clause
    static std::string BuildCountClause(bool count_only);
    
    // Parse SQL GROUP BY into dimensions
    static std::vector<std::string> ParseGroupByClause(const std::string& sql_groupby);
    
    // Parse SQL aggregate functions into aggregations map
    static std::map<std::string, std::string> ParseAggregateFunctions(const std::string& sql_select);
    
    // Validate analytical query components
    static bool ValidateAnalyticalQuery(const AnalyticalQueryComponents& components);
    
    // Generate OData $apply URL parameters
    static std::string GenerateApplyUrlParameters(const std::string& apply_clause);
    
    // Combine multiple $apply operations
    static std::string CombineApplyOperations(const std::vector<std::string>& operations);
    
    // Handle input parameters in analytical queries
    static std::string SubstituteInputParameters(const std::string& query, 
                                               const std::map<std::string, std::string>& parameters);
    
    // Helper methods for internal processing (public for testing)
    static std::string EscapeODataValue(const std::string& value);
    static std::string BuildDimensionList(const std::vector<std::string>& dimensions);
    static std::string BuildAggregationList(const std::map<std::string, std::string>& aggregations);
    static std::string ValidateAggregationFunction(const std::string& function);
    static std::string BuildHierarchyPath(const HierarchyNavigation& hierarchy);

private:
};

} // namespace erpl_web
