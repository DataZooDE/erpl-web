#pragma once

#include <string>
#include <codecvt>
#include <locale>
#include <optional>
#include <stdexcept>

#include "duckdb.hpp"

namespace erpl_web
{

enum class CharsetType {
    UTF8,
    ISO8859_1,
    ISO8859_15,
    WINDOWS_1252,
    BINARY,
    UNKNOWN
};

class CharsetConverter {
public:
    explicit CharsetConverter(const std::string& content_type);

    std::string convert(const std::string& input) const;

private:
    mutable std::optional<std::wstring_convert<std::codecvt_utf8<wchar_t>, wchar_t>> utf8Converter;
    CharsetType charsetType;
    std::string charset;

    std::wstring from_bytes(const std::string& input) const;
    std::wstring from_bytes_iso8859_1(const std::string& input) const;
    std::wstring from_bytes_iso8859_15(const std::string& input) const;
    std::wstring from_bytes_windows_1252(const std::string& input) const;
    
    CharsetType detectCharsetType(const std::string& content_type) const;
    bool isBinaryContentType(const std::string& content_type) const;
};

} // namespace erpl_web
