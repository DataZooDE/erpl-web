#pragma once

#include <string>
#include <vector>

namespace erpl_web {

class ODataExpandParser {
public:
    struct ExpandPath {
        std::string navigation_property;
        std::vector<std::string> sub_expands;
        std::string filter_clause;
        std::string select_clause;
        std::string top_clause;
        std::string skip_clause;
        
        // Enhanced fields for data extraction
        std::string full_expand_path;  // Full path including options
        std::string column_name;       // Clean column name for result set
        bool has_options;              // Whether this expand has query options
        
        ExpandPath() = default;
        ExpandPath(const std::string& prop) : navigation_property(prop) {}
        
        // Helper methods
        bool IsSimpleExpand() const { return !has_options && sub_expands.empty(); }
        std::string GetCleanColumnName() const { return column_name.empty() ? navigation_property : column_name; }
    };
    
    // Parse an OData expand clause into structured paths
    static std::vector<ExpandPath> ParseExpandClause(const std::string& expand_clause);
    
    // Build an expand clause from structured paths
    static std::string BuildExpandClause(const std::vector<ExpandPath>& paths);
    
private:
    // Helper methods for parsing
    static std::string TrimWhitespace(const std::string& str);
    static std::vector<std::string> SplitByComma(const std::string& str);
    static std::string ExtractNavigationProperty(const std::string& path);
    static std::vector<std::string> ExtractSubExpands(const std::string& path);
    static std::string ExtractFilterClause(const std::string& path);
    static std::string ExtractSelectClause(const std::string& path);
    static std::string ExtractTopClause(const std::string& path);
    static std::string ExtractSkipClause(const std::string& path);
};

} // namespace erpl_web
