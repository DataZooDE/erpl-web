#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "erpl_http_client.hpp"
#include <string>
#include <vector>
#include <optional>
#include <sstream>

namespace erpl_web {

class ODataPredicatePushdownHelper {
public:
    ODataPredicatePushdownHelper(const std::vector<std::string> &all_column_names);

    void ConsumeColumnSelection(const std::vector<duckdb::column_t> &column_ids);
    void ConsumeFilters(duckdb::optional_ptr<duckdb::TableFilterSet> filters);

    HttpUrl ApplyFiltersToUrl(const HttpUrl &base_url);

    std::string SelectClause() const;
    std::string FilterClause() const;
private:
    std::vector<std::string> all_column_names;
    std::vector<duckdb::column_t> column_selection;
    std::string select_clause;
    std::string filter_clause;

    std::string BuildSelectClause(const std::vector<duckdb::column_t> &column_ids) const;
    std::string BuildFilterClause(duckdb::optional_ptr<duckdb::TableFilterSet> filters) const;
    std::string TranslateFilter(const duckdb::TableFilter &filter, const std::string &column_name) const;
    std::string TranslateConstantComparison(const duckdb::ConstantFilter &filter, const std::string &column_name) const;
    std::string TranslateConjunction(const duckdb::ConjunctionFilter &filter, const std::string &column_name) const;
};

} // namespace erpl_web