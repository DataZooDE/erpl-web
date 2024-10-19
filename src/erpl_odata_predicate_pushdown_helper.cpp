#include "erpl_odata_predicate_pushdown_helper.hpp"
#include <algorithm>

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
    std::string query = result.Query();

    if (!select_clause.empty() || !filter_clause.empty()) {
        if (query.empty()) {
            query += "?";
        } else {
            query += "&";
        }
    }

    if (!select_clause.empty()) {
        query += select_clause;
    }

    if (!filter_clause.empty()) {
        if (!select_clause.empty()) {
            query += "&";
        }
        query += filter_clause;
    }

    result.Query(query);
    return result;
}

std::string ODataPredicatePushdownHelper::BuildSelectClause(const std::vector<duckdb::column_t> &column_ids) const {
    if (column_ids.empty()) {
        return "";
    }

    std::stringstream select_clause;
    for (size_t i = 0; i < column_ids.size(); ++i) {
        if (duckdb::IsRowIdColumnId(column_ids[i])) {
            continue;
        }

        if (i > 0) {
            select_clause << ",";
        }
        select_clause << all_column_names[column_ids[i]];
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
    filter_clause << "$filter=";
    bool first = true;
    for (const auto &filter_entry : filters->filters) {
        if (!first) {
            filter_clause << " and ";
        }
        first = false;
        filter_clause << TranslateFilter(*filter_entry.second, all_column_names[filter_entry.first]);
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
        case duckdb::TableFilterType::CONJUNCTION_OR:
            return TranslateConjunction(filter.Cast<duckdb::ConjunctionAndFilter>(), column_name);
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
            throw std::runtime_error("Unsupported contant filter comparison type for OData translation");
            
    }

    result << " '" << filter.constant.ToString() << "'";
    return result.str();
}

std::string ODataPredicatePushdownHelper::TranslateConjunction(const duckdb::ConjunctionFilter &filter, const std::string &column_name) const {
    std::stringstream result;
    result << "(";

    for (size_t i = 0; i < filter.child_filters.size(); ++i) {
        if (i > 0) {
            result << (filter.filter_type == duckdb::TableFilterType::CONJUNCTION_AND ? " and " : " or ");
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
