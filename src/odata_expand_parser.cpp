#include "odata_expand_parser.hpp"

#include <cstddef>
#include <string>
#include <vector>

namespace erpl_web {

namespace {

constexpr char SINGLE_QUOTE = '\'';
constexpr char PAREN_OPEN = '(';
constexpr char PAREN_CLOSE = ')';
constexpr char SEGMENT_SEPARATOR = '/';
constexpr char OPTION_SEPARATOR = ';';
constexpr char PATH_SEPARATOR = ',';

// Return the index just past the closing quote of the literal that starts at
// open_quote. OData escapes a single quote by doubling it ('O''Brien'), so a
// doubled quote continues the literal instead of terminating it. An
// unterminated literal consumes the rest of the input.
std::size_t SkipQuotedLiteral(const std::string& text, std::size_t open_quote) {
	std::size_t i = open_quote + 1;
	while (i < text.size()) {
		if (text[i] != SINGLE_QUOTE) {
			i++;
			continue;
		}
		if (i + 1 < text.size() && text[i + 1] == SINGLE_QUOTE) {
			i += 2;
			continue;
		}
		return i + 1;
	}
	return i;
}

// Split text at every occurrence of delimiter that is neither nested inside
// parentheses nor inside a quoted string literal.
std::vector<std::string> SplitTopLevel(const std::string& text, char delimiter) {
	std::vector<std::string> parts;
	std::size_t depth = 0;
	std::size_t start = 0;
	std::size_t i = 0;
	while (i < text.size()) {
		const char c = text[i];
		if (c == SINGLE_QUOTE) {
			i = SkipQuotedLiteral(text, i);
			continue;
		}
		if (c == PAREN_OPEN) {
			depth++;
			i++;
			continue;
		}
		if (c == PAREN_CLOSE) {
			if (depth > 0) {
				depth--;
			}
			i++;
			continue;
		}
		if (depth == 0 && c == delimiter) {
			parts.push_back(text.substr(start, i - start));
			start = i + 1;
		}
		i++;
	}
	parts.push_back(text.substr(start));
	return parts;
}

// Locate the balanced option group of a segment. Returns the index of the
// opening parenthesis and writes the index of its match to close_index, or
// npos when there is no balanced group (e.g. a malformed "Nav($filter=x").
std::size_t FindOptionGroup(const std::string& segment, std::size_t& close_index) {
	std::size_t i = 0;
	while (i < segment.size()) {
		const char c = segment[i];
		if (c == SINGLE_QUOTE) {
			i = SkipQuotedLiteral(segment, i);
			continue;
		}
		if (c == PAREN_OPEN) {
			break;
		}
		i++;
	}
	if (i >= segment.size()) {
		return std::string::npos;
	}

	const std::size_t open_index = i;
	std::size_t depth = 0;
	while (i < segment.size()) {
		const char c = segment[i];
		if (c == SINGLE_QUOTE) {
			i = SkipQuotedLiteral(segment, i);
			continue;
		}
		if (c == PAREN_OPEN) {
			depth++;
			i++;
			continue;
		}
		if (c == PAREN_CLOSE) {
			depth--;
			if (depth == 0) {
				close_index = i;
				return open_index;
			}
			i++;
			continue;
		}
		i++;
	}
	return std::string::npos;
}

std::string Trim(const std::string& str) {
	const std::size_t start = str.find_first_not_of(" \t\r\n");
	if (start == std::string::npos) {
		return "";
	}
	const std::size_t end = str.find_last_not_of(" \t\r\n");
	return str.substr(start, end - start + 1);
}

// Split one path segment into its navigation property name and the raw content
// of its option group (without the surrounding parentheses).
void SplitSegment(const std::string& segment, std::string& name, std::string& options) {
	std::size_t close_index = std::string::npos;
	const std::size_t open_index = FindOptionGroup(segment, close_index);
	if (open_index != std::string::npos) {
		name = Trim(segment.substr(0, open_index));
		options = segment.substr(open_index + 1, close_index - open_index - 1);
		return;
	}

	// Unbalanced parentheses: keep the name, discard the malformed options.
	const std::size_t paren = segment.find(PAREN_OPEN);
	name = Trim(paren == std::string::npos ? segment : segment.substr(0, paren));
	options.clear();
}

} // namespace

std::vector<ODataExpandParser::ExpandPath> ODataExpandParser::ParseExpandClause(const std::string& expand_clause) {
	std::vector<ExpandPath> paths;
	if (expand_clause.empty()) {
		return paths;
	}

	for (const auto& path_str : SplitByComma(expand_clause)) {
		paths.push_back(ParseSinglePath(path_str));
	}
	return paths;
}

ODataExpandParser::ExpandPath ODataExpandParser::ParseSinglePath(const std::string& path_str) {
	ExpandPath path;
	path.full_expand_path = path_str;

	const std::vector<std::string> segments = SplitTopLevel(path_str, SEGMENT_SEPARATOR);

	std::string name;
	std::string options;
	SplitSegment(segments.front(), name, options);
	path.navigation_property = name;

	// Slash-chained children ("Nav/Sub/SubSub"); their own options are not part
	// of the flat child name.
	for (std::size_t i = 1; i < segments.size(); ++i) {
		std::string sub_name;
		std::string sub_options;
		SplitSegment(segments[i], sub_name, sub_options);
		if (!sub_name.empty()) {
			path.sub_expands.push_back(sub_name);
		}
	}

	ApplyOptions(options, path);

	path.has_options = !path.filter_clause.empty() ||
	                   !path.select_clause.empty() ||
	                   !path.top_clause.empty() ||
	                   !path.skip_clause.empty() ||
	                   !path.sub_expands.empty();

	path.column_name = path.navigation_property;
	return path;
}

void ODataExpandParser::ApplyOptions(const std::string& options, ExpandPath& path) {
	if (options.empty()) {
		return;
	}

	for (const auto& part : SplitTopLevel(options, OPTION_SEPARATOR)) {
		const std::string option = Trim(part);
		if (option.empty()) {
			continue;
		}
		const std::size_t equals = option.find('=');
		if (equals == std::string::npos) {
			continue;
		}
		const std::string key = Trim(option.substr(0, equals));
		const std::string value = Trim(option.substr(equals + 1));
		if (value.empty()) {
			continue;
		}

		if (key == "$filter") {
			path.filter_clause = "$filter=" + value;
		} else if (key == "$select") {
			path.select_clause = "$select=" + value;
		} else if (key == "$top") {
			path.top_clause = "$top=" + value;
		} else if (key == "$skip") {
			path.skip_clause = "$skip=" + value;
		} else if (key == "$expand") {
			path.nested_expands = ParseExpandClause(value);
			path.syntax = ExpandSyntax::Nested;
			for (const auto& nested : path.nested_expands) {
				if (!nested.navigation_property.empty()) {
					path.sub_expands.push_back(nested.navigation_property);
				}
			}
		}
	}
}

std::string ODataExpandParser::BuildExpandClause(const std::vector<ExpandPath>& paths) {
	std::string result;
	for (std::size_t i = 0; i < paths.size(); ++i) {
		if (i > 0) {
			result += PATH_SEPARATOR;
		}
		result += BuildSingleExpand(paths[i]);
	}
	return result;
}

std::string ODataExpandParser::BuildSingleExpand(const ExpandPath& path) {
	std::string result = path.navigation_property;

	std::vector<std::string> options;
	if (path.syntax == ExpandSyntax::Nested) {
		// OData v4 nesting: children live in the option list, not in the path.
		std::string nested;
		if (!path.nested_expands.empty()) {
			nested = BuildExpandClause(path.nested_expands);
		} else {
			for (std::size_t i = 0; i < path.sub_expands.size(); ++i) {
				if (i > 0) {
					nested += PATH_SEPARATOR;
				}
				nested += path.sub_expands[i];
			}
		}
		if (!nested.empty()) {
			options.push_back("$expand=" + nested);
		}
	} else {
		// OData v2 path syntax.
		for (const auto& sub_expand : path.sub_expands) {
			result += SEGMENT_SEPARATOR;
			result += sub_expand;
		}
	}

	if (!path.filter_clause.empty()) {
		options.push_back(path.filter_clause);
	}
	if (!path.select_clause.empty()) {
		options.push_back(path.select_clause);
	}
	if (!path.top_clause.empty()) {
		options.push_back(path.top_clause);
	}
	if (!path.skip_clause.empty()) {
		options.push_back(path.skip_clause);
	}

	if (options.empty()) {
		return result;
	}

	result += PAREN_OPEN;
	for (std::size_t i = 0; i < options.size(); ++i) {
		if (i > 0) {
			result += OPTION_SEPARATOR;
		}
		result += options[i];
	}
	result += PAREN_CLOSE;
	return result;
}

std::string ODataExpandParser::TrimWhitespace(const std::string& str) {
	return Trim(str);
}

std::vector<std::string> ODataExpandParser::SplitByComma(const std::string& str) {
	std::vector<std::string> result;
	for (const auto& part : SplitTopLevel(str, PATH_SEPARATOR)) {
		const std::string trimmed = Trim(part);
		if (!trimmed.empty()) {
			result.push_back(trimmed);
		}
	}
	return result;
}

} // namespace erpl_web
