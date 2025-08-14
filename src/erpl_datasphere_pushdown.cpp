#include "erpl_datasphere_pushdown.hpp"
#include <sstream>
#include <algorithm>
#include <regex>

namespace erpl_web {

// Build $apply clause from SQL components
std::string DatasphereAnalyticalPushdownHelper::BuildApplyClause(const AnalyticalQueryComponents& components) {
    std::vector<std::string> operations;
    
    // Add groupby and aggregate operation
    if (!components.dimensions.empty() && !components.aggregations.empty()) {
        std::string aggregation_op = BuildApplyClauseWithAggregation(components.dimensions, components.aggregations);
        operations.push_back(aggregation_op);
    }
    
    // Add filter operation if present
    if (!components.filter_clause.empty()) {
        operations.push_back("filter(" + components.filter_clause + ")");
    }
    
    // Add orderby operation if present
    if (!components.orderby_clause.empty()) {
        operations.push_back("orderby(" + components.orderby_clause + ")");
    }
    
    // Add top/skip operations if present
    if (components.top_limit > 0 || components.skip_offset > 0) {
        std::string top_skip = BuildTopSkipClause(components.top_limit, components.skip_offset);
        if (!top_skip.empty()) {
            operations.push_back(top_skip);
        }
    }
    
    // Combine all operations
    return CombineApplyOperations(operations);
}

// Build $apply clause with aggregation
std::string DatasphereAnalyticalPushdownHelper::BuildApplyClauseWithAggregation(
    const std::vector<std::string>& dimensions,
    const std::map<std::string, std::string>& aggregations) {
    
    std::stringstream ss;
    ss << "groupby((" << BuildDimensionList(dimensions) << "),";
    ss << "aggregate(" << BuildAggregationList(aggregations) << "))";
    
    return ss.str();
}

// Build $apply clause with hierarchy navigation
std::string DatasphereAnalyticalPushdownHelper::BuildApplyClauseWithHierarchy(
    const std::vector<std::string>& dimensions,
    const HierarchyNavigation& hierarchy,
    const std::map<std::string, std::string>& aggregations) {
    
    std::stringstream ss;
    ss << "groupby((" << BuildDimensionList(dimensions) << ",";
    ss << BuildHierarchyPath(hierarchy) << "),";
    ss << "aggregate(" << BuildAggregationList(aggregations) << "))";
    
    return ss.str();
}

// Build $apply clause with calculated measures
std::string DatasphereAnalyticalPushdownHelper::BuildApplyClauseWithCalculatedMeasures(
    const std::vector<std::string>& dimensions,
    const std::vector<CalculatedMeasure>& calculated_measures,
    const std::map<std::string, std::string>& aggregations) {
    
    std::stringstream ss;
    ss << "groupby((" << BuildDimensionList(dimensions) << "),";
    ss << "aggregate(" << BuildAggregationList(aggregations);
    
    // Add calculated measures
    if (!calculated_measures.empty()) {
        ss << ",";
        for (size_t i = 0; i < calculated_measures.size(); ++i) {
            if (i > 0) ss << ",";
            ss << calculated_measures[i].name << " as " << calculated_measures[i].expression;
        }
    }
    
    ss << "))";
    return ss.str();
}

// Build $filter clause from SQL WHERE
std::string DatasphereAnalyticalPushdownHelper::BuildFilterClause(const std::string& sql_where) {
    if (sql_where.empty()) return "";
    
    // Basic SQL to OData filter translation
    std::string filter = sql_where;
    
    // Replace SQL operators with OData operators
    std::map<std::string, std::string> operator_map = {
        {" AND ", " and "},
        {" OR ", " or "},
        {" NOT ", " not "},
        {"=", " eq "},
        {"!=", " ne "},
        {">", " gt "},
        {">=", " ge "},
        {"<", " lt "},
        {"<=", " le "},
        {"LIKE", "contains"},
        {"IN", "in"}
    };
    
    for (const auto& op : operator_map) {
        size_t pos = 0;
        while ((pos = filter.find(op.first, pos)) != std::string::npos) {
            filter.replace(pos, op.first.length(), op.second);
            pos += op.second.length();
        }
    }
    
    return filter;
}

// Build $orderby clause from SQL ORDER BY
std::string DatasphereAnalyticalPushdownHelper::BuildOrderByClause(const std::string& sql_orderby) {
    if (sql_orderby.empty()) return "";
    
    std::string orderby = sql_orderby;
    
    // Replace SQL DESC/ASC with OData format
    std::regex desc_regex(R"(\b(\w+)\s+DESC\b)", std::regex_constants::icase);
    std::regex asc_regex(R"(\b(\w+)\s+ASC\b)", std::regex_constants::icase);
    
    orderby = std::regex_replace(orderby, desc_regex, "$1 desc");
    orderby = std::regex_replace(orderby, asc_regex, "$1 asc");
    
    return orderby;
}

// Build $top and $skip from SQL LIMIT/OFFSET
std::string DatasphereAnalyticalPushdownHelper::BuildTopSkipClause(int limit, int offset) {
    std::stringstream ss;
    
    if (offset > 0) {
        ss << "skip(" << offset << ")";
    }
    
    if (limit > 0) {
        if (offset > 0) ss << ",";
        ss << "top(" << limit << ")";
    }
    
    return ss.str();
}

// Build $count clause
std::string DatasphereAnalyticalPushdownHelper::BuildCountClause(bool count_only) {
    return count_only ? "count" : "";
}

// Parse SQL GROUP BY into dimensions
std::vector<std::string> DatasphereAnalyticalPushdownHelper::ParseGroupByClause(const std::string& sql_groupby) {
    std::vector<std::string> dimensions;
    
    if (sql_groupby.empty()) return dimensions;
    
    std::stringstream ss(sql_groupby);
    std::string dimension;
    
    while (std::getline(ss, dimension, ',')) {
        // Trim whitespace
        dimension.erase(0, dimension.find_first_not_of(" \t"));
        dimension.erase(dimension.find_last_not_of(" \t") + 1);
        
        if (!dimension.empty()) {
            dimensions.push_back(dimension);
        }
    }
    
    return dimensions;
}

// Parse SQL aggregate functions into aggregations map
std::map<std::string, std::string> DatasphereAnalyticalPushdownHelper::ParseAggregateFunctions(const std::string& sql_select) {
    std::map<std::string, std::string> aggregations;
    
    if (sql_select.empty()) return aggregations;
    
    // Basic regex patterns for common aggregate functions
    std::vector<std::pair<std::string, std::string>> patterns = {
        {R"(SUM\s*\(\s*(\w+)\s*\)\s+as\s+(\w+))", "sum"},
        {R"(AVG\s*\(\s*(\w+)\s*\)\s+as\s+(\w+))", "average"},
        {R"(COUNT\s*\(\s*(\w+)\s*\)\s+as\s+(\w+))", "count"},
        {R"(MIN\s*\(\s*(\w+)\s*\)\s+as\s+(\w+))", "min"},
        {R"(MAX\s*\(\s*(\w+)\s*\)\s+as\s+(\w+))", "max"}
    };
    
    for (const auto& pattern : patterns) {
        std::regex regex(pattern.first, std::regex_constants::icase);
        std::smatch match;
        
        if (std::regex_search(sql_select, match, regex)) {
            std::string measure = match[1].str();
            std::string alias = match[2].str();
            aggregations[measure] = pattern.second;
        }
    }
    
    return aggregations;
}

// Validate analytical query components
bool DatasphereAnalyticalPushdownHelper::ValidateAnalyticalQuery(const AnalyticalQueryComponents& components) {
    // Must have at least one dimension for groupby
    if (components.dimensions.empty()) {
        return false;
    }
    
    // Must have at least one aggregation
    if (components.aggregations.empty()) {
        return false;
    }
    
    // Validate aggregation functions
    for (const auto& agg : components.aggregations) {
        if (ValidateAggregationFunction(agg.second).empty()) {
            return false;
        }
    }
    
    // Validate limits
    if (components.top_limit < 0 || components.skip_offset < 0) {
        return false;
    }
    
    return true;
}

// Generate OData $apply URL parameters
std::string DatasphereAnalyticalPushdownHelper::GenerateApplyUrlParameters(const std::string& apply_clause) {
    if (apply_clause.empty()) return "";
    return "$apply=" + apply_clause;
}

// Combine multiple $apply operations
std::string DatasphereAnalyticalPushdownHelper::CombineApplyOperations(const std::vector<std::string>& operations) {
    if (operations.empty()) return "";
    if (operations.size() == 1) return operations[0];
    
    std::stringstream ss;
    for (size_t i = 0; i < operations.size(); ++i) {
        if (i > 0) ss << "/";
        ss << operations[i];
    }
    
    return ss.str();
}

// Handle input parameters in analytical queries
std::string DatasphereAnalyticalPushdownHelper::SubstituteInputParameters(const std::string& query, 
                                                                       const std::map<std::string, std::string>& parameters) {
    std::string result = query;
    
    for (const auto& param : parameters) {
        std::string placeholder = "{" + param.first + "}";
        size_t pos = result.find(placeholder);
        if (pos != std::string::npos) {
            result.replace(pos, placeholder.length(), param.second);
        }
    }
    
    return result;
}

// Private helper methods

std::string DatasphereAnalyticalPushdownHelper::EscapeODataValue(const std::string& value) {
    // Basic OData value escaping
    std::string escaped = value;
    
    // Escape single quotes
    size_t pos = 0;
    while ((pos = escaped.find("'", pos)) != std::string::npos) {
        escaped.replace(pos, 1, "''");
        pos += 2;
    }
    
    return escaped;
}

std::string DatasphereAnalyticalPushdownHelper::BuildDimensionList(const std::vector<std::string>& dimensions) {
    std::stringstream ss;
    for (size_t i = 0; i < dimensions.size(); ++i) {
        if (i > 0) ss << ",";
        ss << dimensions[i];
    }
    return ss.str();
}

std::string DatasphereAnalyticalPushdownHelper::BuildAggregationList(const std::map<std::string, std::string>& aggregations) {
    std::stringstream ss;
    size_t i = 0;
    for (const auto& agg : aggregations) {
        if (i > 0) ss << ",";
        ss << agg.first << " with " << agg.second;
        ++i;
    }
    return ss.str();
}

std::string DatasphereAnalyticalPushdownHelper::ValidateAggregationFunction(const std::string& function) {
    std::vector<std::string> valid_functions = {"sum", "average", "count", "min", "max", "countdistinct"};
    
    std::string lower_function = function;
    std::transform(lower_function.begin(), lower_function.end(), lower_function.begin(), ::tolower);
    
    for (const auto& valid : valid_functions) {
        if (lower_function == valid) {
            return valid;
        }
    }
    
    return ""; // Invalid function
}

std::string DatasphereAnalyticalPushdownHelper::BuildHierarchyPath(const HierarchyNavigation& hierarchy) {
    std::stringstream ss;
    ss << hierarchy.hierarchy_name;
    
    if (!hierarchy.levels.empty()) {
        ss << "(" << BuildDimensionList(hierarchy.levels) << ")";
    }
    
    if (!hierarchy.drill_path.empty()) {
        ss << "/" << hierarchy.drill_path;
    }
    
    return ss.str();
}

} // namespace erpl_web
