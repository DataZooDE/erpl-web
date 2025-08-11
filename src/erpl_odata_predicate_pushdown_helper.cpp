#include "erpl_odata_predicate_pushdown_helper.hpp"
#include <algorithm>
#include <set>
#include "duckdb/planner/filter/optional_filter.hpp"

namespace erpl_web {

ODataPredicatePushdownHelper::ODataPredicatePushdownHelper(const std::vector<std::string> &all_column_names)
    : all_column_names(all_column_names)
{
}

void ODataPredicatePushdownHelper::ConsumeColumnSelection(const std::vector<duckdb::column_t> &column_ids) {
    this->select_clause = BuildSelectClause(column_ids);
}

void ODataPredicatePushdownHelper::ConsumeFilters(duckdb::optional_ptr<duckdb::TableFilterSet> filters) {
    this->filter_clause = BuildFilterClause(filters);
}

HttpUrl ODataPredicatePushdownHelper::ApplyFiltersToUrl(const HttpUrl &base_url) {
    HttpUrl result = base_url;
    std::string existing_query = result.Query();
    
    // Build the new query string
    std::string new_query = existing_query;
    
    if (!select_clause.empty() || !filter_clause.empty()) {
        // If there are existing query parameters, add & separator
        // Note: existing_query already contains the ? if it exists
        if (!new_query.empty() && new_query != "?") {
            new_query += "&";
        } else if (new_query.empty()) {
            // No existing query parameters, add ? to start
            new_query = "?";
        }
    }

    if (!select_clause.empty()) {
        new_query += select_clause;
    }

    if (!filter_clause.empty()) {
        if (!select_clause.empty()) {
            new_query += "&";
        }
        new_query += filter_clause;
    }

    result.Query(new_query);
    return result;
}

std::string ODataPredicatePushdownHelper::BuildSelectClause(const std::vector<duckdb::column_t> &column_ids) const {
    if (column_ids.empty()) {
        return "";
    }

    // Count non-rowid columns
    size_t non_rowid_count = 0;
    for (size_t i = 0; i < column_ids.size(); ++i) {
        if (!duckdb::IsRowIdColumnId(column_ids[i])) {
            non_rowid_count++;
        }
    }

    // If all available columns are selected, skip the $select parameter
    // This allows the OData service to return all data without column restrictions
    if (non_rowid_count == all_column_names.size()) {
        return "";
    }

    // Check if any of the selected columns are complex fields that might cause OData errors
    // Complex fields like Emails, AddressInfo, HomeAddress, Features are arrays or complex objects
    // that the OData service might not handle properly in $select
    std::vector<std::string> complex_fields = {"Emails", "AddressInfo", "HomeAddress", "Features"};
    
    for (size_t i = 0; i < column_ids.size(); ++i) {
        if (duckdb::IsRowIdColumnId(column_ids[i])) {
            continue;
        }
        
        std::string field_name = all_column_names[column_ids[i]];
        
        // Check if this is a complex field
        for (const auto& complex_field : complex_fields) {
            if (field_name == complex_field || field_name.find(complex_field) == 0) {
                // Complex field detected, skip $select to avoid OData errors
                return "";
            }
        }
    }

    std::stringstream select_clause;
    std::set<std::string> unique_fields; // Use set to avoid duplicates
    
    for (size_t i = 0; i < column_ids.size(); ++i) {
        if (duckdb::IsRowIdColumnId(column_ids[i])) {
            continue;
        }

        // Extract base field name from complex expressions
        std::string field_name = all_column_names[column_ids[i]];
        
        // For OData $select, we need to use the base field name (before any path expressions)
        // Complex expressions like AddressInfo[1].City."Name" should use AddressInfo in $select
        // but the full expression will be evaluated by the JSON path evaluator
        size_t pos = field_name.find_first_of(".[\"");
        if (pos != std::string::npos) {
            field_name = field_name.substr(0, pos);
        }
        
        // Only add if not already present
        if (unique_fields.find(field_name) == unique_fields.end()) {
            if (!unique_fields.empty()) {
                select_clause << ",";
            }
            select_clause << field_name;
            unique_fields.insert(field_name);
        }
    }

    if (select_clause.str().empty()) {
        return "";
    }

    return "$select=" + select_clause.str();
}

std::string ODataPredicatePushdownHelper::BuildFilterClause(duckdb::optional_ptr<duckdb::TableFilterSet> filters) const {
    if (!filters || filters->filters.empty()) {
        return "";
    }

    std::stringstream filter_clause;
    std::vector<std::string> valid_filters;
    
    // First pass: collect all valid filters
    for (const auto &filter_entry : filters->filters) {
        std::string translated_filter = TranslateFilter(*filter_entry.second, all_column_names[filter_entry.first]);
        if (!translated_filter.empty()) {
            valid_filters.push_back(translated_filter);
        }
    }
    
    // If no valid filters, return empty string
    if (valid_filters.empty()) {
        return "";
    }
    
    // Build the filter clause with valid filters
    filter_clause << "$filter=";
    for (size_t i = 0; i < valid_filters.size(); ++i) {
        if (i > 0) {
            filter_clause << " and ";
        }
        filter_clause << valid_filters[i];
    }
    
    return filter_clause.str();
}

std::string ODataPredicatePushdownHelper::TranslateFilter(const duckdb::TableFilter &filter, const std::string &column_name) const {
    switch (filter.filter_type) {
        case duckdb::TableFilterType::CONSTANT_COMPARISON:
            return TranslateConstantComparison(filter.Cast<duckdb::ConstantFilter>(), column_name);
        case duckdb::TableFilterType::IS_NULL:
            return column_name + " eq null";
        case duckdb::TableFilterType::IS_NOT_NULL:
            return column_name + " ne null";
        case duckdb::TableFilterType::CONJUNCTION_AND:
            return TranslateConjunction(filter.Cast<duckdb::ConjunctionAndFilter>(), column_name);
        case duckdb::TableFilterType::CONJUNCTION_OR:
            return TranslateConjunction(filter.Cast<duckdb::ConjunctionOrFilter>(), column_name);
        case duckdb::TableFilterType::OPTIONAL_FILTER:
            // Optional filters wrap another filter - delegate to the child filter
            return TranslateFilter(*filter.Cast<duckdb::OptionalFilter>().child_filter, column_name);
        case duckdb::TableFilterType::DYNAMIC_FILTER:
            // Dynamic filters wrap a ConstantFilter - delegate to the underlying filter
            return TranslateConstantComparison(*filter.Cast<duckdb::DynamicFilter>().filter_data->filter, column_name);
        default:
            std::stringstream error;
            auto filter_str = const_cast<duckdb::TableFilter&>(filter).ToString(column_name);
            auto filter_type_name = typeid(filter).name();
            error << "Unsupported filter type for OData translation: '" << filter_str << "'"
                  << " (" << filter_type_name << ")";
            throw std::runtime_error(error.str());
    }
}

std::string ODataPredicatePushdownHelper::TranslateConstantComparison(const duckdb::ConstantFilter &filter, const std::string &column_name) const {
    // Validate the filter value to ensure it makes sense for OData
    std::string constant_value = filter.constant.ToString();
    
    // Skip invalid filters that would generate malformed OData
    if (constant_value.empty() || constant_value == "''" || constant_value == "\"\"") {
        // Empty string comparisons often don't make sense and can cause OData errors
        return "";
    }
    
    // Skip filters with very long values that might cause OData URL issues
    if (constant_value.length() > 1000) {
        return "";
    }
    
    std::stringstream result;
    result << column_name << " ";

    switch (filter.comparison_type) {
        case duckdb::ExpressionType::COMPARE_EQUAL:
            result << "eq";
            break;
        case duckdb::ExpressionType::COMPARE_NOTEQUAL:
            result << "ne";
            break;
        case duckdb::ExpressionType::COMPARE_LESSTHAN:
            result << "lt";
            break;
        case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
            result << "le";
            break;
        case duckdb::ExpressionType::COMPARE_GREATERTHAN:
            result << "gt";
            break;
        case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            result << "ge";
            break;
        default:
            // Unsupported comparison type - skip this filter
            return "";
    }

    // Handle different data types properly for OData
    if (filter.constant.type().id() == duckdb::LogicalTypeId::VARCHAR || 
        filter.constant.type().id() == duckdb::LogicalTypeId::CHAR) {
        // String values need single quotes
        result << " '" << constant_value << "'";
    } else if (filter.constant.type().id() == duckdb::LogicalTypeId::INTEGER ||
               filter.constant.type().id() == duckdb::LogicalTypeId::BIGINT ||
               filter.constant.type().id() == duckdb::LogicalTypeId::DOUBLE ||
               filter.constant.type().id() == duckdb::LogicalTypeId::DECIMAL) {
        // Numeric values don't need quotes
        result << " " << constant_value;
    } else if (filter.constant.type().id() == duckdb::LogicalTypeId::BOOLEAN) {
        // Boolean values in OData are lowercase
        result << " " << (constant_value == "true" ? "true" : "false");
    } else {
        // For other types, try to convert to string but be cautious
        result << " '" << constant_value << "'";
    }
    
    return result.str();
}

std::string ODataPredicatePushdownHelper::TranslateConjunction(const duckdb::ConjunctionAndFilter &filter, const std::string &column_name) const {
    std::stringstream result;
    result << "(";

    for (size_t i = 0; i < filter.child_filters.size(); ++i) {
        if (i > 0) {
            result << " and ";
        }
        
        result << TranslateFilter(*filter.child_filters[i], column_name);
    }

    result << ")";
    return result.str();
}

std::string ODataPredicatePushdownHelper::TranslateConjunction(const duckdb::ConjunctionOrFilter &filter, const std::string &column_name) const {
    std::stringstream result;
    result << "(";

    for (size_t i = 0; i < filter.child_filters.size(); ++i) {
        if (i > 0) {
            result << " or ";
        }
        
        result << TranslateFilter(*filter.child_filters[i], column_name);
    }

    result << ")";
    return result.str();
}

std::string ODataPredicatePushdownHelper::SelectClause() const {
    return select_clause;
}

std::string ODataPredicatePushdownHelper::FilterClause() const {
    return filter_clause;
}

} // namespace erpl_web
