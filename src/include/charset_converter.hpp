#pragma once

#include <string>
#include <codecvt>
#include <locale>
#include <optional>
#include <stdexcept>

#include "duckdb.hpp"

namespace erpl_web
{

class CharsetConverter {
public:
    explicit CharsetConverter(const std::string& content_type);

    std::string convert(const std::string& input);

private:
    std::optional<std::wstring_convert<std::codecvt_utf8<wchar_t>, wchar_t>> utf8Converter;
    std::string charset;

    std::wstring from_bytes(const std::string& input);
    std::wstring from_bytes_iso8859_1(const std::string& input);
    std::wstring from_bytes_iso8859_15(const std::string& input);
    std::wstring from_bytes_windows_1252(const std::string& input);
};

} // namespace erpl_web
