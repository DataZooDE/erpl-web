#include "erpl_odata_expand_parser.hpp"
#include <sstream>
#include <algorithm>
#include <cctype>

namespace erpl_web {

std::vector<ODataExpandParser::ExpandPath> ODataExpandParser::ParseExpandClause(const std::string& expand_clause) {
    std::vector<ExpandPath> paths;
    if (expand_clause.empty()) { return paths; }
    
    auto path_strings = SplitByComma(expand_clause);
    for (const auto& path_str : path_strings) {
        ExpandPath path;
        path.full_expand_path = path_str;
        path.navigation_property = ExtractNavigationProperty(path_str);
        path.sub_expands = ExtractSubExpands(path_str);
        path.filter_clause = ExtractFilterClause(path_str);
        path.select_clause = ExtractSelectClause(path_str);
        path.top_clause = ExtractTopClause(path_str);
        path.skip_clause = ExtractSkipClause(path_str);
        
        // Determine if this expand has options
        path.has_options = !path.filter_clause.empty() || 
                          !path.select_clause.empty() || 
                          !path.top_clause.empty() || 
                          !path.skip_clause.empty() ||
                          !path.sub_expands.empty();
        
        // Set clean column name (navigation property without options)
        path.column_name = path.navigation_property;
        
        paths.push_back(path);
    }
    return paths;
}

std::string ODataExpandParser::BuildExpandClause(const std::vector<ExpandPath>& paths) {
    if (paths.empty()) {
        return "";
    }
    
    std::stringstream result;
    
    for (size_t i = 0; i < paths.size(); ++i) {
        if (i > 0) {
            result << ",";
        }
        
        const auto& path = paths[i];
        result << path.navigation_property;
        
        // Add sub-expands
        for (const auto& sub_expand : path.sub_expands) {
            result << "/" << sub_expand;
        }
        
        // Add query options if any exist
        std::vector<std::string> options;
        if (!path.filter_clause.empty()) options.push_back(path.filter_clause);
        if (!path.select_clause.empty()) options.push_back(path.select_clause);
        if (!path.top_clause.empty()) options.push_back(path.top_clause);
        if (!path.skip_clause.empty()) options.push_back(path.skip_clause);
        
        if (!options.empty()) {
            result << "(";
            for (size_t j = 0; j < options.size(); ++j) {
                if (j > 0) result << ";";
                result << options[j];
            }
            result << ")";
        }
    }
    
    return result.str();
}

std::string ODataExpandParser::TrimWhitespace(const std::string& str) {
    size_t start = str.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) {
        return "";
    }
    size_t end = str.find_last_not_of(" \t\r\n");
    return str.substr(start, end - start + 1);
}

std::vector<std::string> ODataExpandParser::SplitByComma(const std::string& str) {
    std::vector<std::string> result;
    std::stringstream ss(str);
    std::string item;
    
    while (std::getline(ss, item, ',')) {
        std::string trimmed = TrimWhitespace(item);
        if (!trimmed.empty()) {
            result.push_back(trimmed);
        }
    }
    
    return result;
}

std::string ODataExpandParser::ExtractNavigationProperty(const std::string& path) {
    // Find the first occurrence of '(' or '/'
    size_t paren_pos = path.find('(');
    size_t slash_pos = path.find('/');
    
    size_t end_pos = std::string::npos;
    if (paren_pos != std::string::npos && slash_pos != std::string::npos) {
        end_pos = std::min(paren_pos, slash_pos);
    } else if (paren_pos != std::string::npos) {
        end_pos = paren_pos;
    } else if (slash_pos != std::string::npos) {
        end_pos = slash_pos;
    }
    
    if (end_pos != std::string::npos) {
        return TrimWhitespace(path.substr(0, end_pos));
    }
    
    return TrimWhitespace(path);
}

std::vector<std::string> ODataExpandParser::ExtractSubExpands(const std::string& path) {
    std::vector<std::string> sub_expands;
    
    // Find the part between the first slash and the first parenthesis
    size_t slash_pos = path.find('/');
    if (slash_pos == std::string::npos) {
        return sub_expands;
    }
    
    size_t paren_pos = path.find('(');
    size_t end_pos = (paren_pos != std::string::npos) ? paren_pos : path.length();
    
    std::string sub_path = path.substr(slash_pos + 1, end_pos - slash_pos - 1);
    
    // Split by slashes to get multiple sub-expands
    std::stringstream ss(sub_path);
    std::string item;
    while (std::getline(ss, item, '/')) {
        std::string trimmed = TrimWhitespace(item);
        if (!trimmed.empty()) {
            sub_expands.push_back(trimmed);
        }
    }
    
    return sub_expands;
}

std::string ODataExpandParser::ExtractFilterClause(const std::string& path) {
    size_t paren_start = path.find('(');
    if (paren_start == std::string::npos) {
        return "";
    }
    
    size_t paren_end = path.rfind(')');
    if (paren_end == std::string::npos || paren_end <= paren_start) {
        return "";
    }
    
    std::string options = path.substr(paren_start + 1, paren_end - paren_start - 1);
    
    // Look for $filter option
    size_t filter_pos = options.find("$filter=");
    if (filter_pos == std::string::npos) {
        return "";
    }
    
    // Find the end of the filter value (next semicolon or end of string)
    size_t value_start = filter_pos + 8; // length of "$filter="
    size_t value_end = options.find(';', value_start);
    if (value_end == std::string::npos) {
        value_end = options.length();
    }
    
    return "$filter=" + options.substr(value_start, value_end - value_start);
}

std::string ODataExpandParser::ExtractSelectClause(const std::string& path) {
    size_t paren_start = path.find('(');
    if (paren_start == std::string::npos) {
        return "";
    }
    
    size_t paren_end = path.rfind(')');
    if (paren_end == std::string::npos || paren_end <= paren_start) {
        return "";
    }
    
    std::string options = path.substr(paren_start + 1, paren_end - paren_start - 1);
    
    // Look for $select option
    size_t select_pos = options.find("$select=");
    if (select_pos == std::string::npos) {
        return "";
    }
    
    // Find the end of the select value (next semicolon or end of string)
    size_t value_start = select_pos + 8; // length of "$select="
    size_t value_end = options.find(';', value_start);
    if (value_end == std::string::npos) {
        value_end = options.length();
    }
    
    return "$select=" + options.substr(value_start, value_end - value_start);
}

std::string ODataExpandParser::ExtractTopClause(const std::string& path) {
    size_t paren_start = path.find('(');
    if (paren_start == std::string::npos) {
        return "";
    }
    
    size_t paren_end = path.rfind(')');
    if (paren_end == std::string::npos || paren_end <= paren_start) {
        return "";
    }
    
    std::string options = path.substr(paren_start + 1, paren_end - paren_start - 1);
    
    // Look for $top option
    size_t top_pos = options.find("$top=");
    if (top_pos == std::string::npos) {
        return "";
    }
    
    // Find the end of the top value (next semicolon or end of string)
    size_t value_start = top_pos + 5; // length of "$top="
    size_t value_end = options.find(';', value_start);
    if (value_end == std::string::npos) {
        value_end = options.length();
    }
    
    return "$top=" + options.substr(value_start, value_end - value_start);
}

std::string ODataExpandParser::ExtractSkipClause(const std::string& path) {
    size_t paren_start = path.find('(');
    if (paren_start == std::string::npos) {
        return "";
    }
    
    size_t paren_end = path.rfind(')');
    if (paren_end == std::string::npos || paren_end <= paren_start) {
        return "";
    }
    
    std::string options = path.substr(paren_start + 1, paren_end - paren_start - 1);
    
    // Look for $skip option
    size_t skip_pos = options.find("$skip=");
    if (skip_pos == std::string::npos) {
        return "";
    }
    
    // Find the end of the skip value (next semicolon or end of string)
    size_t value_start = skip_pos + 6; // length of "$skip="
    size_t value_end = options.find(';', value_start);
    if (value_end == std::string::npos) {
        value_end = options.length();
    }
    
    return "$skip=" + options.substr(value_start, value_end - value_start);
}

} // namespace erpl_web
