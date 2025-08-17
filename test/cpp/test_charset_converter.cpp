#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"

#include "charset_converter.hpp"

using namespace erpl_web;

TEST_CASE("CharsetConverter Tests", "[charset_converter]")
{
    SECTION("Convert UTF-8 to UTF-8") {
        CharsetConverter converter("text/html; charset=utf-8");
        std::string input = u8"Hello, 世界!";
        std::string expected = u8"Hello, 世界!";
        std::string result = converter.convert(input);
        REQUIRE(result == expected);
    }

    SECTION("Convert UTF-8 to ISO-8859-1") {
        CharsetConverter converter("text/html; charset=ISO-8859-1");
        std::string input = u8"Hello, 世界!";
        std::string expected = "Hello, ?????!";
        std::string result = converter.convert(input);
        //REQUIRE(result == expected);
    }

    SECTION("Convert UTF-8 to ISO-8859-15") {
        CharsetConverter converter("text/html; charset=ISO-8859-15");
        std::string input = u8"Héllø, wørld!";
        std::string expected = "Héllø, wørld!";
        std::string result = converter.convert(input);
        //REQUIRE(result == expected);
    }

    SECTION("Convert UTF-8 to Windows-1252") {
        CharsetConverter converter("text/html; charset=windows-1252");
        std::string input = u8"Héllø, wørld!";
        std::string expected = "Héllø, wørld!";
        std::string result = converter.convert(input);
        //REQUIRE(result == expected);
    }

    SECTION("Binary content types should not be converted") {
        CharsetConverter converter("application/pdf");
        std::string input = "binary data \x00\x01\x02";
        std::string result = converter.convert(input);
        REQUIRE(result == input);
    }

    SECTION("Image content types should not be converted") {
        CharsetConverter converter("image/png");
        std::string input = "image data \x89PNG";
        std::string result = converter.convert(input);
        REQUIRE(result == input);
    }

    SECTION("Video content types should not be converted") {
        CharsetConverter converter("video/mp4");
        std::string input = "video data \x00\x00\x00";
        std::string result = converter.convert(input);
        REQUIRE(result == input);
    }

    SECTION("Audio content types should not be converted") {
        CharsetConverter converter("audio/mpeg");
        std::string input = "audio data \xFF\xFB";
        std::string result = converter.convert(input);
        REQUIRE(result == input);
    }

    SECTION("Font content types should not be converted") {
        CharsetConverter converter("font/woff2");
        std::string input = "font data \x77\x4F\x46\x32";
        std::string result = converter.convert(input);
        REQUIRE(result == input);
    }

    SECTION("Empty input should return empty string") {
        CharsetConverter converter("text/html; charset=utf-8");
        std::string input = "";
        std::string result = converter.convert(input);
        REQUIRE(result.empty());
    }

    SECTION("No charset specified should default to UTF-8") {
        CharsetConverter converter("text/html");
        std::string input = u8"Hello, 世界!";
        std::string expected = u8"Hello, 世界!";
        std::string result = converter.convert(input);
        REQUIRE(result == expected);
    }

    SECTION("ISO-8859-15 special characters") {
        CharsetConverter converter("text/html; charset=ISO-8859-15");
        // Test euro sign (0xA4)
        std::string input = "\xA4"; // Euro sign in ISO-8859-15
        std::string result = converter.convert(input);
        // Should convert to UTF-8 euro sign (should be different from input)
        REQUIRE(result != input);
        REQUIRE(result.length() > 0);
    }

    SECTION("Windows-1252 special characters") {
        CharsetConverter converter("text/html; charset=windows-1252");
        // Test euro sign (0x80 in Windows-1252)
        std::string input = "\x80"; // Euro sign in Windows-1252
        std::string result = converter.convert(input);
        // Should convert to UTF-8 euro sign (should be different from input)
        REQUIRE(result != input);
        REQUIRE(result.length() > 0);
    }

    SECTION("Const correctness") {
        const CharsetConverter converter("text/html; charset=utf-8");
        std::string input = "Hello, World!";
        std::string result = converter.convert(input);
        REQUIRE(result == input);
    }
}
