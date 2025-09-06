#include "charset_converter.hpp"
#include "tracing.hpp"

namespace erpl_web
{

// Constants for charset detection
namespace {
    constexpr std::string_view CHARSET_ISO8859_1 = "charset=ISO-8859-1";
    constexpr std::string_view CHARSET_ISO8859_15 = "charset=ISO-8859-15";
    constexpr std::string_view CHARSET_WINDOWS_1252 = "charset=windows-1252";
    constexpr std::string_view CHARSET_UTF8 = "charset=utf-8";
    
    // Binary content type patterns - use string_view for better performance
    constexpr std::array<std::string_view, 6> BINARY_PATTERNS = {{
        "application/octet-stream",
        "application/pdf",
        "image/",
        "video/",
        "audio/",
        "font/"
    }};
    
    // ISO-8859-15 special character mappings
    constexpr unsigned char EURO_SIGN = 0xA4;
    constexpr unsigned char BROKEN_BAR = 0xA6;
    constexpr unsigned char DIAERESIS = 0xA8;
    constexpr unsigned char ACUTE_ACCENT = 0xB4;
    constexpr unsigned char CEDILLA = 0xB8;
    constexpr unsigned char OE_LIGATURE_1 = 0xBC;
    constexpr unsigned char OE_LIGATURE_2 = 0xBD;
    constexpr unsigned char Y_WITH_DIAERESIS = 0xBE;
    
    // Windows-1252 special character table (0x80-0x9F)
    constexpr std::array<wchar_t, 32> WINDOWS_1252_TABLE = {{
        0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
        0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0x008D, 0x017D, 0x008F,
        0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
        0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178
    }};
}

CharsetConverter::CharsetConverter(const std::string& content_type) 
    : charsetType(detectCharsetType(content_type))
{
    ERPL_TRACE_DEBUG("CHARSET_CONVERTER", duckdb::StringUtil::Format("Creating charset converter for content type: %s", content_type));
    
    // Set charset string for backward compatibility
    switch (charsetType) {
        case CharsetType::ISO8859_1:
            charset = "ISO-8859-1";
            break;
        case CharsetType::ISO8859_15:
            charset = "ISO-8859-15";
            break;
        case CharsetType::WINDOWS_1252:
            charset = "windows-1252";
            break;
        case CharsetType::UTF8:
            charset = "utf-8";
            utf8Converter.emplace();
            break;
        case CharsetType::BINARY:
            charset = "binary";
            break;
        default:
            charset = "utf-8";
            utf8Converter.emplace();
            break;
    }
    
    ERPL_TRACE_DEBUG("CHARSET_CONVERTER", duckdb::StringUtil::Format("Charset converter initialized with charset: %s", charset));
}

CharsetType CharsetConverter::detectCharsetType(const std::string& content_type) const {
    // Check for binary content types first
    if (isBinaryContentType(content_type)) {
        ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Detected binary content type, setting charset to binary");
        return CharsetType::BINARY;
    }
    
    // Use string_view for more efficient string operations
    const std::string_view content_view(content_type);
    
    if (content_view.find(CHARSET_ISO8859_1) != std::string_view::npos) {
        ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Detected ISO-8859-1 charset");
        return CharsetType::ISO8859_1;
    } else if (content_view.find(CHARSET_ISO8859_15) != std::string_view::npos) {
        ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Detected ISO-8859-15 charset");
        return CharsetType::ISO8859_15;
    } else if (content_view.find(CHARSET_WINDOWS_1252) != std::string_view::npos) {
        ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Detected windows-1252 charset");
        return CharsetType::WINDOWS_1252;
    } else if (content_view.find(CHARSET_UTF8) != std::string_view::npos) {
        ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Detected UTF-8 charset, using UTF-8 converter");
        return CharsetType::UTF8;
    } else {
        ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "No charset specified, defaulting to UTF-8 converter");
        return CharsetType::UTF8;
    }
}

bool CharsetConverter::isBinaryContentType(const std::string& content_type) const {
    const std::string_view content_view(content_type);
    return std::any_of(BINARY_PATTERNS.begin(), BINARY_PATTERNS.end(),
        [&content_view](const std::string_view& pattern) {
            return content_view.find(pattern) != std::string_view::npos;
        });
}

std::string CharsetConverter::convert(const std::string& input) const {
    ERPL_TRACE_DEBUG("CHARSET_CONVERTER", duckdb::StringUtil::Format("Converting %d bytes using charset: %s", input.length(), charset));
    
    if (input.empty()) {
        ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Input is empty, returning empty string");
        return {};
    }
    
    // Check if this is a binary content type that shouldn't be converted
    if (charsetType == CharsetType::BINARY) {
        ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Binary content type detected, returning input as-is");
        return input;
    }
    
    try {
        std::wstring wide_string = from_bytes(input);

        if (utf8Converter) {
            ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Using UTF-8 converter for output");
            return utf8Converter->to_bytes(wide_string);
        } else {
            ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Using default UTF-8 converter for output");
            return std::wstring_convert<std::codecvt_utf8<wchar_t>>().to_bytes(wide_string);
        }
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("CHARSET_CONVERTER", "Conversion failed: " + std::string(e.what()));
        // For binary or problematic content, return the original input
        ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Falling back to returning original input");
        return input;
    }
}

std::wstring CharsetConverter::from_bytes(const std::string& input) const {
    ERPL_TRACE_DEBUG("CHARSET_CONVERTER", duckdb::StringUtil::Format("Converting from bytes using charset: %s", charset));
    
    switch (charsetType) {
        case CharsetType::ISO8859_1:
            ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Using ISO-8859-1 conversion");
            return from_bytes_iso8859_1(input);
        case CharsetType::ISO8859_15:
            ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Using ISO-8859-15 conversion");
            return from_bytes_iso8859_15(input);
        case CharsetType::WINDOWS_1252:
            ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Using windows-1252 conversion");
            return from_bytes_windows_1252(input);
        default:
            ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Using default UTF-8 conversion");
            return std::wstring_convert<std::codecvt_utf8<wchar_t>>().from_bytes(input);
    }
}

std::wstring CharsetConverter::from_bytes_iso8859_1(const std::string& input) const {
    ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Converting ISO-8859-1 bytes to wide string");
    
    std::wstring wide_string;
    wide_string.reserve(input.length()); // Pre-allocate for efficiency
    
    // Use range-based for loop for better readability
    for (const unsigned char c : input) {
        wide_string += static_cast<wchar_t>(c);
    }
    
    ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "ISO-8859-1 conversion completed: " + std::to_string(input.length()) + " -> " + std::to_string(wide_string.length()));
    return wide_string;
}

std::wstring CharsetConverter::from_bytes_iso8859_15(const std::string& input) const {
    ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Converting ISO-8859-15 bytes to wide string");
    
    std::wstring wide_string;
    wide_string.reserve(input.length()); // Pre-allocate for efficiency
    
    for (const unsigned char c : input) {
        switch (c) {
            case EURO_SIGN: // euro sign
                wide_string += 0x20AC;
                break;
            case BROKEN_BAR: // broken bar
                wide_string += 0x0160;
                break;
            case DIAERESIS: // diaeresis
                wide_string += 0x0161;
                break;
            case ACUTE_ACCENT: // acute accent
                wide_string += 0x017D;
                break;
            case CEDILLA: // cedilla
                wide_string += 0x017E;
                break;
            case OE_LIGATURE_1: // oe ligature
                wide_string += 0x0152;
                break;
            case OE_LIGATURE_2: // oe ligature
                wide_string += 0x0153;
                break;
            case Y_WITH_DIAERESIS: // y with diaeresis
                wide_string += 0x0178;
                break;
            default:
                wide_string += static_cast<wchar_t>(c);
                break;
        }
    }
    
    ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "ISO-8859-15 conversion completed: " + std::to_string(input.length()) + " -> " + std::to_string(wide_string.length()));
    return wide_string;
}

std::wstring CharsetConverter::from_bytes_windows_1252(const std::string& input) const {
    ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Converting windows-1252 bytes to wide string");
    
    std::wstring wide_string;
    wide_string.reserve(input.length()); // Pre-allocate for efficiency
    
    for (const unsigned char c : input) {
        if (c >= 0x80 && c <= 0x9F) {
            wide_string += WINDOWS_1252_TABLE[c - 0x80];
        } else {
            wide_string += static_cast<wchar_t>(c);
        }
    }
    
    ERPL_TRACE_DEBUG("CHARSET_CONVERTER", "Windows-1252 conversion completed: " + std::to_string(input.length()) + " -> " + std::to_string(wide_string.length()));
    return wide_string;
}

} // namespace erpl_web
