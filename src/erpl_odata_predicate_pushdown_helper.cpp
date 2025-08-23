#include "erpl_odata_predicate_pushdown_helper.hpp"
#include <algorithm>
#include <set>
#include "duckdb/planner/filter/optional_filter.hpp"
#include "erpl_tracing.hpp"

namespace erpl_web {

ODataPredicatePushdownHelper::ODataPredicatePushdownHelper(const std::vector<std::string> &all_column_names)
    : all_column_names(all_column_names)
{
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Created predicate pushdown helper with " + std::to_string(all_column_names.size()) + " columns");
    
    // Log all available column names for debugging
    for (size_t i = 0; i < all_column_names.size(); ++i) {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Column " + std::to_string(i) + ": " + all_column_names[i]);
    }
}

void ODataPredicatePushdownHelper::SetColumnNameResolver(ColumnNameResolver resolver)
{
    column_name_resolver = resolver;
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Column name resolver set");
}

void ODataPredicatePushdownHelper::ConsumeColumnSelection(const std::vector<duckdb::column_t> &column_ids) {
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Consuming column selection: " + std::to_string(column_ids.size()) + " columns");
    this->select_clause = BuildSelectClause(column_ids);
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Built select clause: " + this->select_clause);
}

void ODataPredicatePushdownHelper::ConsumeFilters(duckdb::optional_ptr<duckdb::TableFilterSet> filters) {
    if (filters && !filters->filters.empty()) {
        std::stringstream filters_str;
        /*
        for (auto &[projected_column_idx, filter] : filters->filters) 
        {
            filters_str << "Column " << projected_column_idx << ": " << filter->DebugToString() << std::endl;
        }
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Consuming " + std::to_string(filters->filters.size()) + " filters: " + filters_str.str());
        */
        this->filter_clause = BuildFilterClause(filters);
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Built filter clause: " + this->filter_clause);
    } else {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "No filters to consume");
        this->filter_clause = "";
    }
}

void ODataPredicatePushdownHelper::ConsumeLimit(duckdb::idx_t limit) {
    if (limit > 0) {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Consuming LIMIT: " + std::to_string(limit));
        this->top_clause = BuildTopClause(limit);
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Built top clause: " + this->top_clause);
    } else {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "No LIMIT to consume");
        this->top_clause = "";
    }
}

void ODataPredicatePushdownHelper::ConsumeOffset(duckdb::idx_t offset) {
    if (offset > 0) {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Consuming OFFSET: " + std::to_string(offset));
        this->skip_clause = BuildSkipClause(offset);
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Built skip clause: " + this->skip_clause);
    } else {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "No OFFSET to consume");
        this->skip_clause = "";
    }
}

void ODataPredicatePushdownHelper::ConsumeResultModifiers(const std::vector<duckdb::unique_ptr<duckdb::BoundResultModifier>> &modifiers)
{
    if (modifiers.empty()) {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "No result modifiers to consume");
        return;
    }
    
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Consuming " + std::to_string(modifiers.size()) + " result modifiers");
    
    for (const auto &modifier : modifiers) {
        ProcessResultModifier(*modifier);
    }
}



HttpUrl ODataPredicatePushdownHelper::ApplyFiltersToUrl(const HttpUrl &base_url) {
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Applying filters to URL: " + base_url.ToString());
    
    HttpUrl result = base_url;
    std::string existing_query = result.Query();
    
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Existing query: '" + existing_query + "'");
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Select clause: '" + select_clause + "'");
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Filter clause: '" + filter_clause + "'");
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Top clause: '" + top_clause + "'");
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Skip clause: '" + skip_clause + "'");
    
    // Parse existing query parameters to avoid duplicates
    std::map<std::string, std::string> existing_params;
    if (!existing_query.empty() && existing_query != "?") {
        std::string query_str = existing_query;
        if (query_str[0] == '?') {
            query_str = query_str.substr(1);
        }
        
        std::istringstream iss(query_str);
        std::string param;
        while (std::getline(iss, param, '&')) {
            size_t pos = param.find('=');
            if (pos != std::string::npos) {
                std::string key = param.substr(0, pos);
                std::string value = param.substr(pos + 1);
                existing_params[key] = value;
            }
        }
    }
    
    // Build the new query string
    std::string new_query = "";
    
    // Collect all query clauses
    std::vector<std::string> clauses;
    
    // Handle $select - only add if not already present in URL
    if (!select_clause.empty() && existing_params.find("$select") == existing_params.end()) {
        clauses.push_back(select_clause);
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Adding select clause: " + select_clause);
    }
    
    if (!filter_clause.empty()) {
        clauses.push_back(filter_clause);
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Adding filter clause: " + filter_clause);
    }
    
    // Add new OData clauses
    if (!top_clause.empty()) {
        clauses.push_back(top_clause);
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Adding top clause: " + top_clause);
    }
    
    if (!skip_clause.empty()) {
        clauses.push_back(skip_clause);
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Adding skip clause: " + skip_clause);
    }
    

    
    // Add OData v2-specific options
    std::string inline_count = InlineCountClause();
    if (!inline_count.empty()) {
        clauses.push_back(inline_count);
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Adding inline count clause: " + inline_count);
    }
    
    std::string skip_token = SkipTokenClause();
    if (!skip_token.empty()) {
        clauses.push_back(skip_token);
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Adding skip token clause: " + skip_token);
    }
    
    // Build the final query string
    if (!clauses.empty()) {
        // Start with existing query parameters
        if (!existing_query.empty() && existing_query != "?") {
            new_query = existing_query;
        } else {
            new_query = "?";
        }
        
        // Add new clauses
        for (size_t i = 0; i < clauses.size(); ++i) {
            if (new_query != "?" && !new_query.empty()) {
                new_query += "&";
            }
            new_query += clauses[i];
        }
    } else {
        // No new clauses, keep existing query
        new_query = existing_query;
    }

    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Final query: '" + new_query + "'");
    result.Query(new_query);
    
    ERPL_TRACE_INFO("PREDICATE_PUSHDOWN", "Updated URL: " + result.ToString());
    return result;
}

std::string ODataPredicatePushdownHelper::BuildSelectClause(const std::vector<duckdb::column_t> &column_ids) const {
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Building select clause for " + std::to_string(column_ids.size()) + " columns");
    
    if (column_ids.empty()) {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "No columns selected, returning empty select clause");
        return "";
    }

    // Count non-rowid columns
    size_t non_rowid_count = 0;
    for (size_t i = 0; i < column_ids.size(); ++i) {
        if (!duckdb::IsRowIdColumnId(column_ids[i])) {
            non_rowid_count++;
        }
    }

    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Non-rowid columns: " + std::to_string(non_rowid_count) + " out of " + std::to_string(all_column_names.size()));

    // If all available columns are selected, skip the $select parameter
    // This allows the OData service to return all data without column restrictions
    if (non_rowid_count == all_column_names.size()) {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "All columns selected, skipping $select parameter");
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
        
        // Use column name resolver if available, otherwise fall back to direct indexing
        std::string field_name;
        if (column_name_resolver) {
            field_name = column_name_resolver(column_ids[i]);
            if (field_name.empty()) {
                ERPL_TRACE_ERROR("PREDICATE_PUSHDOWN", "Column name resolver returned empty string for index " + std::to_string(column_ids[i]));
                continue;
            }
        } else {
            field_name = all_column_names[column_ids[i]];
        }
        
        // Check if this is a complex field
        for (const auto& complex_field : complex_fields) {
            if (field_name == complex_field || field_name.find(complex_field) == 0) {
                // Complex field detected, skip $select to avoid OData errors
                ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Complex field detected: " + field_name + ", skipping $select to avoid OData errors");
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
        std::string field_name;
        if (column_name_resolver) {
            field_name = column_name_resolver(column_ids[i]);
            if (field_name.empty()) {
                ERPL_TRACE_ERROR("PREDICATE_PUSHDOWN", "Column name resolver returned empty string for index " + std::to_string(column_ids[i]));
                continue;
            }
        } else {
            field_name = all_column_names[column_ids[i]];
        }
        
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
            ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Added field to select: " + field_name);
        }
    }

    if (select_clause.str().empty()) {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "No valid fields for select, returning empty clause");
        return "";
    }

    std::string result = "$select=" + select_clause.str();
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Built select clause: " + result);
    return result;
}

std::string ODataPredicatePushdownHelper::BuildFilterClause(duckdb::optional_ptr<duckdb::TableFilterSet> filters) const {
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Building filter clause");
    
    if (!filters || filters->filters.empty()) {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "No filters provided, returning empty filter clause");
        return "";
    }

    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Processing " + std::to_string(filters->filters.size()) + " filters");
    
    std::stringstream filter_clause;
    std::vector<std::string> valid_filters;
    
    // First pass: collect all valid filters
    for (const auto &filter_entry : filters->filters) {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", duckdb::StringUtil::Format("Processing filter for DuckDB column index: %d", filter_entry.first));
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", duckdb::StringUtil::Format("Total columns available: %d", all_column_names.size()));
        
        // Use column name resolver if available, otherwise fall back to direct indexing
        std::string column_name;
        if (column_name_resolver) {
            column_name = column_name_resolver(filter_entry.first);
            if (column_name.empty()) {
                ERPL_TRACE_ERROR("PREDICATE_PUSHDOWN", "Column name resolver returned empty string for index " + std::to_string(filter_entry.first));
                continue;
            }
            ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Column name resolver mapped index " + std::to_string(filter_entry.first) + " to: " + column_name);
        } else {
            // Fallback to direct indexing (for backward compatibility)
            duckdb::idx_t column_index = filter_entry.first;
            ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "No column name resolver, using direct index: " + std::to_string(column_index));
            
            if (column_index >= all_column_names.size()) {
                ERPL_TRACE_ERROR("PREDICATE_PUSHDOWN", "Column index " + std::to_string(column_index) + " is out of bounds for column names array");
                continue;
            }
            
            column_name = all_column_names[column_index];
            ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Direct mapping: column index " + std::to_string(column_index) + " maps to column name: " + column_name);
        }
        
        std::string translated_filter = TranslateFilter(*filter_entry.second, column_name);
        if (!translated_filter.empty()) {
            valid_filters.push_back(translated_filter);
            ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Valid filter: " + translated_filter);
        } else {
            ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Filter translation resulted in empty string for column: " + column_name);
        }
    }
    
    // If no valid filters, return empty string
    if (valid_filters.empty()) {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "No valid filters found, returning empty filter clause");
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
    
    std::string result = filter_clause.str();
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Built filter clause: " + result);
    return result;
}

std::string ODataPredicatePushdownHelper::BuildTopClause(duckdb::idx_t limit) const {
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Building top clause for limit: " + std::to_string(limit));
    
    if (limit <= 0) {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Invalid limit, returning empty top clause");
        return "";
    }
    
    std::string result = "$top=" + std::to_string(limit);
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Built top clause: " + result);
    return result;
}

std::string ODataPredicatePushdownHelper::BuildSkipClause(duckdb::idx_t offset) const {
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Building skip clause for offset: " + std::to_string(offset));
    
    if (offset <= 0) {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Invalid offset, returning empty skip clause");
        return "";
    }
    
    std::string result = "$skip=" + std::to_string(offset);
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Built skip clause: " + result);
    return result;
}

void ODataPredicatePushdownHelper::ProcessResultModifier(const duckdb::BoundResultModifier &modifier)
{
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Processing result modifier of type: " + std::to_string(static_cast<int>(modifier.type)));
    
    switch (modifier.type) {
        case duckdb::ResultModifierType::LIMIT_MODIFIER: {
            const auto &limit_modifier = modifier.Cast<duckdb::BoundLimitModifier>();
            ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Processing LIMIT modifier");
            
            // Handle LIMIT
            if (limit_modifier.limit_val.Type() == duckdb::LimitNodeType::CONSTANT_VALUE) {
                auto limit_value = limit_modifier.limit_val.GetConstantValue();
                ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "LIMIT constant value: " + std::to_string(limit_value));
                ConsumeLimit(limit_value);
            }
            
            // Handle OFFSET
            if (limit_modifier.offset_val.Type() == duckdb::LimitNodeType::CONSTANT_VALUE) {
                auto offset_value = limit_modifier.offset_val.GetConstantValue();
                ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "OFFSET constant value: " + std::to_string(offset_value));
                ConsumeOffset(offset_value);
            }
            break;
        }
        case duckdb::ResultModifierType::ORDER_MODIFIER: {
            ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "ORDER BY modifier not yet supported");
            break;
        }
        default:
            ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Unsupported result modifier type: " + std::to_string(static_cast<int>(modifier.type)));
            break;
    }
}



std::string ODataPredicatePushdownHelper::InlineCountClause() const {
    if (!inline_count_enabled) {
        return "";
    }
    
    if (odata_version == ODataVersion::V2) {
        // OData v2: $inlinecount=allpages
        return "$inlinecount=allpages";
    } else {
        // OData v4: $count=true
        return "$count=true";
    }
}

std::string ODataPredicatePushdownHelper::SkipTokenClause() const {
    if (!skip_token.has_value()) {
        return "";
    }
    
    if (odata_version == ODataVersion::V2) {
        // OData v2: $skiptoken=value
        return "$skiptoken=" + skip_token.value();
    } else {
        // OData v4: $skip=value (or use @odata.nextLink)
        return "$skip=" + skip_token.value();
    }
}

std::string ODataPredicatePushdownHelper::TranslateFilter(const duckdb::TableFilter &filter, const std::string &column_name) const {
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Translating filter for column '" + column_name + "' with filter type: " + std::to_string(static_cast<int>(filter.filter_type)));
    
    std::string result;
    switch (filter.filter_type) {
        case duckdb::TableFilterType::CONSTANT_COMPARISON:
            result = TranslateConstantComparison(filter.Cast<duckdb::ConstantFilter>(), column_name);
            break;
        case duckdb::TableFilterType::IS_NULL:
            result = column_name + " eq null";
            break;
        case duckdb::TableFilterType::IS_NOT_NULL:
            result = column_name + " ne null";
            break;
        case duckdb::TableFilterType::CONJUNCTION_AND:
            result = TranslateConjunction(filter.Cast<duckdb::ConjunctionAndFilter>(), column_name);
            break;
        case duckdb::TableFilterType::CONJUNCTION_OR:
            result = TranslateConjunction(filter.Cast<duckdb::ConjunctionOrFilter>(), column_name);
            break;
        case duckdb::TableFilterType::OPTIONAL_FILTER:
            // Optional filters wrap another filter - delegate to the child filter
            result = TranslateFilter(*filter.Cast<duckdb::OptionalFilter>().child_filter, column_name);
            break;
        case duckdb::TableFilterType::DYNAMIC_FILTER:
            // Dynamic filters wrap a ConstantFilter - delegate to the underlying filter
            result = TranslateConstantComparison(*filter.Cast<duckdb::DynamicFilter>().filter_data->filter, column_name);
            break;
        default:
            std::stringstream error;
            auto filter_str = const_cast<duckdb::TableFilter&>(filter).ToString(column_name);
            auto filter_type_name = typeid(filter).name();
            error << "Unsupported filter type for OData translation: '" << filter_str << "'"
                  << " (" << filter_type_name << ")";
            ERPL_TRACE_ERROR("PREDICATE_PUSHDOWN", "Unsupported filter type: " + std::string(filter_type_name));
            throw std::runtime_error(error.str());
    }
    
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Translated filter result: '" + result + "'");
    return result;
}

std::string ODataPredicatePushdownHelper::TranslateConstantComparison(const duckdb::ConstantFilter &filter, const std::string &column_name) const {
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Translating constant comparison for column '" + column_name + "'");
    
    // Validate the filter value to ensure it makes sense for OData
    std::string constant_value = filter.constant.ToString();
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Constant value: '" + constant_value + "' (type: " + filter.constant.type().ToString() + ")");
    
    // Skip invalid filters that would generate malformed OData
    if (constant_value.empty() || constant_value == "''" || constant_value == "\"\"") {
        // Empty string comparisons often don't make sense and can cause OData errors
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Skipping empty string comparison for column: " + column_name);
        return "";
    }
    
    // Skip filters with very long values that might cause OData URL issues
    if (constant_value.length() > 1000) {
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Skipping filter with very long value (length: " + std::to_string(constant_value.length()) + ") for column: " + column_name);
        return "";
    }
    
    std::stringstream result;
    result << column_name << " ";

    std::string comparison_operator;
    switch (filter.comparison_type) {
        case duckdb::ExpressionType::COMPARE_EQUAL:
            comparison_operator = "eq";
            break;
        case duckdb::ExpressionType::COMPARE_NOTEQUAL:
            comparison_operator = "ne";
            break;
        case duckdb::ExpressionType::COMPARE_LESSTHAN:
            comparison_operator = "lt";
            break;
        case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
            comparison_operator = "le";
            break;
        case duckdb::ExpressionType::COMPARE_GREATERTHAN:
            comparison_operator = "gt";
            break;
        case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            comparison_operator = "ge";
            break;
        default:
            // Unsupported comparison type - skip this filter
            ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Unsupported comparison type for column: " + column_name);
            return "";
    }
    
    result << comparison_operator;
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Using comparison operator: " + comparison_operator);

    // Handle different data types properly for OData
    if (filter.constant.type().id() == duckdb::LogicalTypeId::VARCHAR || 
        filter.constant.type().id() == duckdb::LogicalTypeId::CHAR) {
        // String values need single quotes
        result << " '" << constant_value << "'";
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Added string value with quotes: '" + constant_value + "'");
    } else if (filter.constant.type().id() == duckdb::LogicalTypeId::INTEGER ||
               filter.constant.type().id() == duckdb::LogicalTypeId::BIGINT ||
               filter.constant.type().id() == duckdb::LogicalTypeId::DOUBLE ||
               filter.constant.type().id() == duckdb::LogicalTypeId::DECIMAL) {
        // Numeric values don't need quotes
        result << " " << constant_value;
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Added numeric value without quotes: " + constant_value);
    } else if (filter.constant.type().id() == duckdb::LogicalTypeId::BOOLEAN) {
        // Boolean values in OData are lowercase
        std::string bool_value = (constant_value == "true" ? "true" : "false");
        result << " " << bool_value;
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Added boolean value: " + bool_value);
    } else {
        // For other types, try to convert to string but be cautious
        result << " '" << constant_value << "'";
        ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Added other type value with quotes: '" + constant_value + "'");
    }
    
    std::string final_result = result.str();
    ERPL_TRACE_DEBUG("PREDICATE_PUSHDOWN", "Final constant comparison: '" + final_result + "'");
    return final_result;
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

std::string ODataPredicatePushdownHelper::TopClause() const {
    return top_clause;
}

std::string ODataPredicatePushdownHelper::SkipClause() const {
    return skip_clause;
}



} // namespace erpl_web
