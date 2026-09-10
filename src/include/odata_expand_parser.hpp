#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace erpl_web {

// How a nested expand was spelled in the source clause. OData v2 chains
// navigation properties with slashes ("Nav/Sub"), while OData v4 nests them in
// the option list ("Nav($expand=Sub)"). The parser records which form it saw so
// that BuildExpandClause can reproduce it instead of silently rewriting it.
enum class ExpandSyntax : uint8_t {
	Path,
	Nested
};

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
		bool has_options = false;      // Whether this expand has query options

		// Structured children of a v4 nested expand ("Nav($expand=Sub(...))").
		// Empty for slash-path syntax; sub_expands always carries the plain
		// child names for both forms.
		std::vector<ExpandPath> nested_expands;
		ExpandSyntax syntax = ExpandSyntax::Path;

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

	// Split on top-level commas: commas nested in parentheses or inside a
	// quoted string literal are not separators.
	static std::vector<std::string> SplitByComma(const std::string& str);

	static ExpandPath ParseSinglePath(const std::string& path_str);
	static void ApplyOptions(const std::string& options, ExpandPath& path);
	static std::string BuildSingleExpand(const ExpandPath& path);
};

} // namespace erpl_web
