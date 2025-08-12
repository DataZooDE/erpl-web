#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "erpl_http_client.hpp"
#include "erpl_odata_edm.hpp"
#include <string>
#include <vector>
#include <optional>
#include <sstream>
#include <map>

namespace erpl_web {

class ODataPredicatePushdownHelper {
public:
    ODataPredicatePushdownHelper(const std::vector<std::string> &all_column_names);

    void ConsumeColumnSelection(const std::vector<duckdb::column_t> &column_ids);
    void ConsumeFilters(duckdb::optional_ptr<duckdb::TableFilterSet> filters);

    HttpUrl ApplyFiltersToUrl(const HttpUrl &base_url);

    std::string SelectClause() const;
    std::string FilterClause() const;
    
    // OData v2-specific query options
    void SetInlineCount(bool enabled) { inline_count_enabled = enabled; }
    void SetSkipToken(const std::string& token) { skip_token = token; }
    void SetODataVersion(ODataVersion version) { odata_version = version; }
    
    std::string InlineCountClause() const;
    std::string SkipTokenClause() const;

private:
    std::vector<std::string> all_column_names;
    std::vector<duckdb::column_t> column_selection;
    std::string select_clause;
    std::string filter_clause;
    
    // OData v2-specific options
    bool inline_count_enabled = false;
    std::optional<std::string> skip_token;
    ODataVersion odata_version = ODataVersion::V4;

    std::string BuildSelectClause(const std::vector<duckdb::column_t> &column_ids) const;
    std::string BuildFilterClause(duckdb::optional_ptr<duckdb::TableFilterSet> filters) const;
    std::string TranslateFilter(const duckdb::TableFilter &filter, const std::string &column_name) const;
    std::string TranslateConstantComparison(const duckdb::ConstantFilter &filter, const std::string &column_name) const;
    std::string TranslateConjunction(const duckdb::ConjunctionAndFilter &filter, const std::string &column_name) const;
    std::string TranslateConjunction(const duckdb::ConjunctionOrFilter &filter, const std::string &column_name) const;
};

} // namespace erpl_web