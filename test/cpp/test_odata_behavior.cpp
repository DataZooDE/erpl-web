#include "catch.hpp"
#include "odata_content.hpp"
#include "odata_behavior.hpp"

using namespace erpl_web;

TEST_CASE("ODataVersionDetector detects v4 via value and @odata.context", "[odata_behavior]") {
    std::string v4_json = R"({
        "@odata.context": "https://service/$metadata#Entities",
        "value": []
    })";
    ODataVersionDetector d;
    REQUIRE(d.detectFromJson(v4_json) == ODataVersion::V4);
}

TEST_CASE("ODataVersionDetector detects v2 via d.results", "[odata_behavior]") {
    std::string v2_json = R"({
        "d": { "results": [] }
    })";
    ODataVersionDetector d;
    REQUIRE(d.detectFromJson(v2_json) == ODataVersion::V2);
}


