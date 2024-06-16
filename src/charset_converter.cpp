#include "charset_converter.hpp"

namespace erpl_web
{

CharsetConverter::CharsetConverter(const std::string& content_type) {
    if (content_type.find("charset=ISO-8859-1") != std::string::npos) {
        charset = "ISO-8859-1";
    } else if (content_type.find("charset=ISO-8859-15") != std::string::npos) {
        charset = "ISO-8859-15";
    } else if (content_type.find("charset=windows-1252") != std::string::npos) {
        charset = "windows-1252";
    } else if (content_type.find("charset=utf-8") != std::string::npos) {
        charset = "utf-8";
        utf8Converter.emplace();
    } else {
        utf8Converter.emplace();
    }
}

std::string CharsetConverter::convert(const std::string& input) {
    std::wstring wide_string = from_bytes(input);

    if (utf8Converter) {
        return utf8Converter->to_bytes(wide_string);
    } else {
        return std::wstring_convert<std::codecvt_utf8<wchar_t>>().to_bytes(wide_string);
    }
}

std::wstring CharsetConverter::from_bytes(const std::string& input) {
    if (charset == "ISO-8859-1") {
        return from_bytes_iso8859_1(input);
    } else if (charset == "ISO-8859-15") {
        return from_bytes_iso8859_15(input);
    } else if (charset == "windows-1252") {
        return from_bytes_windows_1252(input);
    } else {
        return std::wstring_convert<std::codecvt_utf8<wchar_t>>().from_bytes(input);
    }
}

std::wstring CharsetConverter::from_bytes_iso8859_1(const std::string& input) {
    std::wstring wide_string;
    for (unsigned char c : input) {
        wide_string += c;
    }
    return wide_string;
}

std::wstring CharsetConverter::from_bytes_iso8859_15(const std::string& input) {
    std::wstring wide_string;
    for (unsigned char c : input) {
        if (c == 0xA4) { // euro sign
			wide_string += 0x20AC;
		} else if (c == 0xA6) { // broken bar
			wide_string += 0x0160;
		} else if (c == 0xA8) { // diaeresis
			wide_string += 0x0161;
		} else if (c == 0xB4) { // acute accent
			wide_string += 0x017D;
		} else if (c == 0xB8) { // cedilla
			wide_string += 0x017E;
		} else if (c == 0xBC) { // oe ligature
			wide_string += 0x0152;
		} else if (c == 0xBD) { // oe ligature
			wide_string += 0x0153;
		} else if (c == 0xBE) { // y with diaeresis
			wide_string += 0x0178;
		} else {
			wide_string += c;
		}
    }
    return wide_string;
}

std::wstring CharsetConverter::from_bytes_windows_1252(const std::string& input) {
    static const wchar_t table[32] = {
        0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
        0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0x008D, 0x017D, 0x008F,
        0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
        0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178
    };

    std::wstring wide_string;
    for (unsigned char c : input) {
        if (c >= 0x80 && c <= 0x9F) {
            wide_string += table[c - 0x80];
        } else {
            wide_string += c;
        }
    }
    return wide_string;
}

} // namespace erpl_web
