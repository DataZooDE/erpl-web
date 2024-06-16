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
}
