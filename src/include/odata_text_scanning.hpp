#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace erpl_web {

//! Scanning helpers shared by the $expand parser and the URL sanitizer.
//!
//! Both need to walk OData query text without being fooled by structural characters that
//! appear inside string literals or nested option groups. They previously had separate
//! tokenizers: the parser's was depth- and quote-aware, the sanitizer's only depth-aware,
//! so a literal containing ';' or ',' desynchronised the sanitizer and produced a
//! half-encoded filter with an unescaped quote (GitHub #122). One implementation now
//! serves both.
namespace odata_text {

constexpr char SINGLE_QUOTE = '\'';
constexpr char PAREN_OPEN = '(';
constexpr char PAREN_CLOSE = ')';

//! Returns the index just past the string literal that opens at `open_quote`.
//!
//! OData escapes a single quote by DOUBLING it ('O''Brien'), so a naive toggle-on-quote
//! scanner desynchronises on the very first escaped quote and treats the rest of the
//! expression as being outside the literal. An unterminated literal yields the end of the
//! text rather than an error: callers are sanitizers, not validators, and the service is
//! the right place for that complaint.
inline std::size_t SkipQuotedLiteral(const std::string &text, std::size_t open_quote)
{
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

//! Returns the index just past the option group that opens at `open_paren`, ignoring
//! parentheses that occur inside string literals.
inline std::size_t SkipOptionGroup(const std::string &text, std::size_t open_paren)
{
    std::size_t depth = 0;
    std::size_t i = open_paren;
    while (i < text.size()) {
        const char c = text[i];
        if (c == SINGLE_QUOTE) {
            i = SkipQuotedLiteral(text, i);
            continue;
        }
        if (c == PAREN_OPEN) {
            depth++;
        } else if (c == PAREN_CLOSE) {
            depth--;
            if (depth == 0) {
                return i + 1;
            }
        }
        i++;
    }
    return i;
}

//! Splits at every occurrence of any character in `delimiters` that is neither nested
//! inside parentheses nor inside a string literal.
inline std::vector<std::string> SplitTopLevel(const std::string &text, const std::string &delimiters)
{
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
        } else if (c == PAREN_CLOSE) {
            if (depth > 0) {
                depth--;
            }
        } else if (depth == 0 && delimiters.find(c) != std::string::npos) {
            parts.emplace_back(text.substr(start, i - start));
            parts.emplace_back(std::string(1, c));
            start = i + 1;
        }
        i++;
    }
    parts.emplace_back(text.substr(start));
    return parts;
}

} // namespace odata_text
} // namespace erpl_web
