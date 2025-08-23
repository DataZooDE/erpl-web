#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

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
    // Function type for resolving column names from activated indices
    using ColumnNameResolver = std::function<std::string(duckdb::column_t)>;
    
    ODataPredicatePushdownHelper(const std::vector<std::string> &all_column_names);
    
    // Set the column name resolver function
    void SetColumnNameResolver(ColumnNameResolver resolver);

    void ConsumeColumnSelection(const std::vector<duckdb::column_t> &column_ids);
    void ConsumeFilters(duckdb::optional_ptr<duckdb::TableFilterSet> filters);
    
    // New methods for LIMIT and OFFSET support
    void ConsumeLimit(duckdb::idx_t limit);
    void ConsumeOffset(duckdb::idx_t offset);
    
    // Method to consume result modifiers (for LIMIT/OFFSET)
    void ConsumeResultModifiers(const std::vector<duckdb::unique_ptr<duckdb::BoundResultModifier>> &modifiers);

    HttpUrl ApplyFiltersToUrl(const HttpUrl &base_url);

    std::string SelectClause() const;
    std::string FilterClause() const;
    std::string TopClause() const;
    std::string SkipClause() const;
    
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
    
    // New clauses for LIMIT and OFFSET
    std::string top_clause;
    std::string skip_clause;
    
    // OData v2-specific options
    bool inline_count_enabled = false;
    std::optional<std::string> skip_token;
    ODataVersion odata_version = ODataVersion::V4;
    
    // Column name resolver function
    ColumnNameResolver column_name_resolver;

    std::string BuildSelectClause(const std::vector<duckdb::column_t> &column_ids) const;
    std::string BuildFilterClause(duckdb::optional_ptr<duckdb::TableFilterSet> filters) const;
    std::string BuildTopClause(duckdb::idx_t limit) const;
    std::string BuildSkipClause(duckdb::idx_t offset) const;
    
    // Helper method to process result modifiers
    void ProcessResultModifier(const duckdb::BoundResultModifier &modifier);
    std::string TranslateFilter(const duckdb::TableFilter &filter, const std::string &column_name) const;
    std::string TranslateConstantComparison(const duckdb::ConstantFilter &filter, const std::string &column_name) const;
    std::string TranslateConjunction(const duckdb::ConjunctionAndFilter &filter, const std::string &column_name) const;
    std::string TranslateConjunction(const duckdb::ConjunctionOrFilter &filter, const std::string &column_name) const;
};

} // namespace erpl_web