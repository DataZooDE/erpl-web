#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

#include "http_client.hpp"
#include "odata_edm.hpp"
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
    
    explicit ODataPredicatePushdownHelper(const std::vector<std::string> &all_column_names);
    
    // Set OData version for proper syntax generation
    void SetODataVersion(ODataVersion version);
    ODataVersion GetODataVersion() const;
    
    // Column name resolution
    void SetColumnNameResolver(ColumnNameResolver resolver);
    
    // Consume DuckDB operations and convert to OData clauses
    void ConsumeColumnSelection(const std::vector<duckdb::column_t> &column_ids);
    void ConsumeFilters(duckdb::optional_ptr<duckdb::TableFilterSet> filters);
    void ConsumeLimit(duckdb::idx_t limit);
    void ConsumeOffset(duckdb::idx_t offset);
    void ConsumeExpand(const std::string& expand_clause);
    void ConsumeResultModifiers(const std::vector<duckdb::unique_ptr<duckdb::BoundResultModifier>> &modifiers);
    
    // Get generated OData clauses
    std::string SelectClause() const;
    std::string FilterClause() const;
    std::string TopClause() const;
    std::string SkipClause() const;
    std::string ExpandClause() const;
    
    // Apply all clauses to a URL
    HttpUrl ApplyFiltersToUrl(const HttpUrl &base_url);
    
    // Inline count and skip token support
    void EnableInlineCount(bool enable);
    void SetSkipToken(const std::string& token);
    std::string InlineCountClause() const;
    std::string SkipTokenClause() const;

private:
    // OData version for proper syntax generation
    ODataVersion odata_version = ODataVersion::V4; // Default to V4
    
    // Column information
    std::vector<std::string> all_column_names;
    ColumnNameResolver column_name_resolver;
    
    // Generated OData clauses
    std::string select_clause;
    std::string filter_clause;
    std::string top_clause;
    std::string skip_clause;
    std::string expand_clause;
    
    // Additional features
    bool inline_count_enabled = false;
    std::optional<std::string> skip_token;
    
    // Helper methods for building clauses
    std::string BuildSelectClause(const std::vector<duckdb::column_t> &column_ids) const;
    std::string BuildFilterClause(duckdb::optional_ptr<duckdb::TableFilterSet> filters) const;
    std::string BuildTopClause(duckdb::idx_t limit) const;
    std::string BuildSkipClause(duckdb::idx_t offset) const;
    
    // Filter translation methods
    std::string TranslateFilter(const duckdb::TableFilter &filter, const std::string &column_name) const;
    std::string TranslateConstantComparison(const duckdb::ConstantFilter &filter, const std::string &column_name) const;
    std::string TranslateConjunction(const duckdb::ConjunctionAndFilter &filter, const std::string &column_name) const;
    std::string TranslateConjunction(const duckdb::ConjunctionOrFilter &filter, const std::string &column_name) const;
    
    // Result modifier processing
    void ProcessResultModifier(const duckdb::BoundResultModifier &modifier);
};

} // namespace erpl_web