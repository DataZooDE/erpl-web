#include "catch.hpp"
#include "odp_request_orchestrator.hpp"
#include "http_client.hpp"
#include <regex>

using namespace erpl_web;

TEST_CASE("OdpRequestOrchestrator - Construction and Configuration", "[odp_orchestrator]") {
    SECTION("Basic construction") {
        OdpRequestOrchestrator orchestrator;
        
        REQUIRE(orchestrator.GetDefaultPageSize() == 15000); // Theobald default
    }
    
    SECTION("Construction with custom page size") {
        uint32_t custom_page_size = 5000;
        OdpRequestOrchestrator orchestrator(nullptr, custom_page_size);
        
        REQUIRE(orchestrator.GetDefaultPageSize() == custom_page_size);
    }
    
    SECTION("Page size updates") {
        OdpRequestOrchestrator orchestrator;
        
        uint32_t new_page_size = 10000;
        orchestrator.SetDefaultPageSize(new_page_size);
        
        REQUIRE(orchestrator.GetDefaultPageSize() == new_page_size);
    }
}

TEST_CASE("OdpRequestOrchestrator - URL Manipulation", "[odp_url_manipulation]") {
    SECTION("Build delta URL") {
        std::string base_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest";
        std::string delta_token = "abc123def456";
        
        std::string delta_url = OdpRequestOrchestrator::BuildDeltaUrl(base_url, delta_token);
        
        // Should contain the delta token in OData v2 format
        REQUIRE(delta_url.find("!deltatoken=" + delta_token) != std::string::npos);
        
        // Should contain $format=json
        REQUIRE(delta_url.find("$format=json") != std::string::npos);
        
        // Should not contain original query parameters
        REQUIRE(delta_url.find("?") != std::string::npos); // Should have query params
    }
    
    SECTION("Build delta URL with existing query parameters") {
        std::string base_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest?$top=100&$skip=50";
        std::string delta_token = "xyz789";
        
        std::string delta_url = OdpRequestOrchestrator::BuildDeltaUrl(base_url, delta_token);
        
        // Should remove existing query parameters and add delta token
        REQUIRE(delta_url.find("!deltatoken=" + delta_token) != std::string::npos);
        REQUIRE(delta_url.find("$format=json") != std::string::npos);
        
        // Should keep original query parameters
        REQUIRE(delta_url.find("$top=100") != std::string::npos);
        REQUIRE(delta_url.find("$skip=50") != std::string::npos);
    }
}

TEST_CASE("OdpRequestOrchestrator - Delta Token Extraction", "[odp_delta_extraction]") {
    SECTION("Extract token from OData v2 delta URL") {
        std::string delta_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest!deltatoken=abc123def456&$format=json";
        
        std::string extracted_token = OdpRequestOrchestrator::ExtractTokenFromDeltaUrl(delta_url);
        
        REQUIRE(extracted_token == "abc123def456");
    }
    
    SECTION("Extract token from OData v4 delta URL") {
        std::string delta_url = "https://test.com/api/v4/EntitySet?$deltatoken=xyz789ghi012&$format=json";
        
        std::string extracted_token = OdpRequestOrchestrator::ExtractTokenFromDeltaUrl(delta_url);
        
        REQUIRE(extracted_token == "xyz789ghi012");
    }
    
    SECTION("Extract token from URL with multiple parameters") {
        std::string delta_url = "https://test.com/EntitySet?$top=100&!deltatoken=token123&$format=json";
        
        std::string extracted_token = OdpRequestOrchestrator::ExtractTokenFromDeltaUrl(delta_url);
        
        REQUIRE(extracted_token == "token123");
    }
    
    SECTION("No token in URL") {
        std::string regular_url = "https://test.com/EntitySet?$format=json&$top=100";
        
        std::string extracted_token = OdpRequestOrchestrator::ExtractTokenFromDeltaUrl(regular_url);
        
        REQUIRE(extracted_token.empty());
    }
}

TEST_CASE("OdpRequestOrchestrator - Delta Token Extraction from JSON", "[odp_json_extraction]") {
    SECTION("Extract from OData v2 JSON response") {
        std::string v2_response = R"({
            "d": {
                "results": [
                    {"ID": 1, "Name": "Test1"},
                    {"ID": 2, "Name": "Test2"}
                ],
                "__delta": "https://test.com/EntitySet!deltatoken=v2token123&$format=json"
            }
        })";
        
        std::string extracted_token = OdpRequestOrchestrator::ExtractDeltaTokenFromV2Response(v2_response);
        
        REQUIRE(extracted_token == "v2token123");
    }
    
    SECTION("Extract from OData v4 JSON response") {
        std::string v4_response = R"({
            "value": [
                {"ID": 1, "Name": "Test1"},
                {"ID": 2, "Name": "Test2"}
            ],
            "@odata.deltaLink": "https://test.com/EntitySet?$deltatoken=v4token456&$format=json"
        })";
        
        std::string extracted_token = OdpRequestOrchestrator::ExtractDeltaTokenFromV4Response(v4_response);
        
        REQUIRE(extracted_token == "v4token456");
    }
    
    SECTION("No delta token in v2 response") {
        std::string v2_response = R"({
            "d": {
                "results": [
                    {"ID": 1, "Name": "Test1"}
                ]
            }
        })";
        
        std::string extracted_token = OdpRequestOrchestrator::ExtractDeltaTokenFromV2Response(v2_response);
        
        REQUIRE(extracted_token.empty());
    }
    
    SECTION("No delta token in v4 response") {
        std::string v4_response = R"({
            "value": [
                {"ID": 1, "Name": "Test1"}
            ]
        })";
        
        std::string extracted_token = OdpRequestOrchestrator::ExtractDeltaTokenFromV4Response(v4_response);
        
        REQUIRE(extracted_token.empty());
    }
    
    SECTION("Invalid JSON response") {
        std::string invalid_json = "{ invalid json content";
        
        std::string v2_token = OdpRequestOrchestrator::ExtractDeltaTokenFromV2Response(invalid_json);
        std::string v4_token = OdpRequestOrchestrator::ExtractDeltaTokenFromV4Response(invalid_json);
        
        REQUIRE(v2_token.empty());
        REQUIRE(v4_token.empty());
    }
}

TEST_CASE("OdpRequestOrchestrator - HTTP Response Validation", "[odp_validation]") {
    SECTION("Validate preference applied - success") {
        // Create a mock HTTP response with preference-applied header
        HttpResponse response(HttpMethod::GET, HttpUrl("https://test.com"), 200, "application/json", "{}");
        response.headers["preference-applied"] = "odata.track-changes";
        
        bool is_valid = OdpRequestOrchestrator::ValidatePreferenceApplied(response);
        
        REQUIRE(is_valid);
    }
    
    SECTION("Validate preference applied - with multiple preferences") {
        HttpResponse response(HttpMethod::GET, HttpUrl("https://test.com"), 200, "application/json", "{}");
        response.headers["preference-applied"] = "odata.maxpagesize=1000, odata.track-changes";
        
        bool is_valid = OdpRequestOrchestrator::ValidatePreferenceApplied(response);
        
        REQUIRE(is_valid);
    }
    
    SECTION("Validate preference applied - missing header") {
        HttpResponse response(HttpMethod::GET, HttpUrl("https://test.com"), 200, "application/json", "{}");
        // No preference-applied header
        
        bool is_valid = OdpRequestOrchestrator::ValidatePreferenceApplied(response);
        
        REQUIRE(!is_valid);
    }
    
    SECTION("Validate preference applied - wrong preference") {
        HttpResponse response(HttpMethod::GET, HttpUrl("https://test.com"), 200, "application/json", "{}");
        response.headers["preference-applied"] = "odata.maxpagesize=1000";
        
        bool is_valid = OdpRequestOrchestrator::ValidatePreferenceApplied(response);
        
        REQUIRE(!is_valid);
    }
    
    SECTION("Validate preference applied - case sensitivity") {
        HttpResponse response(HttpMethod::GET, HttpUrl("https://test.com"), 200, "application/json", "{}");
        response.headers["Preference-Applied"] = "odata.track-changes"; // Different case header name
        
        // This should still work due to case-insensitive header lookup
        bool is_valid = OdpRequestOrchestrator::ValidatePreferenceApplied(response);
        
        REQUIRE(is_valid);
    }
}

TEST_CASE("OdpRequestOrchestrator - Request Result Structure", "[odp_result_structure]") {
    SECTION("Default request result") {
        OdpRequestOrchestrator::OdpRequestResult result;

        REQUIRE(!result.response);
        REQUIRE(result.extracted_delta_token.empty());
        REQUIRE(!result.preference_applied);
        REQUIRE(!result.has_more_pages);
        REQUIRE(result.http_status_code == 0);
        REQUIRE(result.response_size_bytes == 0);
        REQUIRE(result.accumulated_raw_content.empty());
    }
    
    SECTION("Request result with data") {
        OdpRequestOrchestrator::OdpRequestResult result;
        result.extracted_delta_token = "test_token";
        result.preference_applied = true;
        result.has_more_pages = true;
        result.http_status_code = 200;
        result.response_size_bytes = 1024;
        
        REQUIRE(result.extracted_delta_token == "test_token");
        REQUIRE(result.preference_applied);
        REQUIRE(result.has_more_pages);
        REQUIRE(result.http_status_code == 200);
        REQUIRE(result.response_size_bytes == 1024);
    }
}

TEST_CASE("OdpRequestOrchestrator - MergeV2Pages", "[odp_merge_pages]") {
    SECTION("Single page returns unchanged") {
        std::string page = R"({"d":{"results":[{"ID":"1","Name":"Foo"}],"__delta":"https://example.com/EntitySet?!deltatoken=tok1"}})";
        std::string merged = OdpRequestOrchestrator::MergeV2Pages({page});
        REQUIRE(merged == page);
    }

    SECTION("Empty page list returns empty results") {
        std::string merged = OdpRequestOrchestrator::MergeV2Pages({});
        REQUIRE(merged.find("\"results\"") != std::string::npos);
    }

    SECTION("Two pages — records from both pages are present in merged output") {
        std::string page1 = R"({"d":{"results":[{"ID":"REC001","Name":"A"},{"ID":"REC002","Name":"B"},{"ID":"REC003","Name":"C"}],"__next":"https://example.com/EntitySet?$skiptoken=MOCK_3"}})";
        std::string page2 = R"({"d":{"results":[{"ID":"REC004","Name":"D"},{"ID":"REC005","Name":"E"},{"ID":"REC006","Name":"F"}],"__delta":"https://example.com/EntitySet?!deltatoken=final-token"}})";

        std::string merged = OdpRequestOrchestrator::MergeV2Pages({page1, page2});

        // All 6 records must be present
        REQUIRE(merged.find("REC001") != std::string::npos);
        REQUIRE(merged.find("REC002") != std::string::npos);
        REQUIRE(merged.find("REC003") != std::string::npos);
        REQUIRE(merged.find("REC004") != std::string::npos);
        REQUIRE(merged.find("REC005") != std::string::npos);
        REQUIRE(merged.find("REC006") != std::string::npos);

        // Delta link from last page must be preserved
        REQUIRE(merged.find("final-token") != std::string::npos);
        REQUIRE(merged.find("__delta") != std::string::npos);

        // Intermediate __next must not bleed through
        REQUIRE(merged.find("__next") == std::string::npos);
    }

    SECTION("Three pages — all records accumulated, delta from last page") {
        std::string page1 = R"({"d":{"results":[{"ID":"REC001"},{"ID":"REC002"},{"ID":"REC003"}],"__next":"https://example.com/EntitySet?$skiptoken=MOCK_3"}})";
        std::string page2 = R"({"d":{"results":[{"ID":"REC004"},{"ID":"REC005"},{"ID":"REC006"}],"__next":"https://example.com/EntitySet?$skiptoken=MOCK_6"}})";
        std::string page3 = R"({"d":{"results":[{"ID":"REC007"},{"ID":"REC008"},{"ID":"REC009"}],"__delta":"https://example.com/EntitySet?!deltatoken=abc-123"}})";

        std::string merged = OdpRequestOrchestrator::MergeV2Pages({page1, page2, page3});

        for (int i = 1; i <= 9; ++i) {
            std::string id = "REC00" + std::to_string(i);
            REQUIRE(merged.find(id) != std::string::npos);
        }
        REQUIRE(merged.find("abc-123") != std::string::npos);
        REQUIRE(merged.find("__delta") != std::string::npos);
        REQUIRE(merged.find("__next") == std::string::npos);
    }

    SECTION("Last page without delta link — no __delta in merged output") {
        std::string page1 = R"({"d":{"results":[{"ID":"REC001"}],"__next":"https://example.com/EntitySet?$skiptoken=MOCK_1"}})";
        std::string page2 = R"({"d":{"results":[{"ID":"REC002"}]}})";

        std::string merged = OdpRequestOrchestrator::MergeV2Pages({page1, page2});

        REQUIRE(merged.find("REC001") != std::string::npos);
        REQUIRE(merged.find("REC002") != std::string::npos);
        REQUIRE(merged.find("__delta") == std::string::npos);
    }

    SECTION("__count from last page is preserved in merged output") {
        // OData v2 $inlinecount=allpages repeats __count on every page with the same
        // total value. The merged output must carry it through from the last page.
        std::string page1 = R"({"d":{"results":[{"ID":"REC001"},{"ID":"REC002"}],"__count":"4","__next":"https://example.com/EntitySet?$skiptoken=MOCK_2"}})";
        std::string page2 = R"({"d":{"results":[{"ID":"REC003"},{"ID":"REC004"}],"__count":"4","__delta":"https://example.com/EntitySet?!deltatoken=tok-final"}})";

        std::string merged = OdpRequestOrchestrator::MergeV2Pages({page1, page2});

        // All records present
        REQUIRE(merged.find("REC001") != std::string::npos);
        REQUIRE(merged.find("REC004") != std::string::npos);

        // __count must be preserved
        REQUIRE(merged.find("__count") != std::string::npos);
        REQUIRE(merged.find("\"4\"") != std::string::npos);

        // __next must not bleed through
        REQUIRE(merged.find("__next") == std::string::npos);
    }
}

TEST_CASE("OdpRequestOrchestrator - URL Format Utilities", "[odp_url_format]") {
    SECTION("Ensure JSON format - URL without format") {
        std::string url = "https://test.com/EntitySet";
        std::string formatted_url = OdpRequestOrchestrator::EnsureJsonFormat(url);
        
        REQUIRE(formatted_url.find("$format=json") != std::string::npos);
        REQUIRE(formatted_url.find("?$format=json") != std::string::npos);
    }
    
    SECTION("Ensure JSON format - URL with existing query params") {
        std::string url = "https://test.com/EntitySet?$top=100";
        std::string formatted_url = OdpRequestOrchestrator::EnsureJsonFormat(url);
        
        REQUIRE(formatted_url.find("$format=json") != std::string::npos);
        REQUIRE(formatted_url.find("&$format=json") != std::string::npos);
    }
    
    SECTION("Ensure JSON format - URL already has format") {
        std::string url = "https://test.com/EntitySet?$format=json";
        std::string formatted_url = OdpRequestOrchestrator::EnsureJsonFormat(url);
        
        REQUIRE(formatted_url == url); // Should be unchanged
    }
    
    SECTION("Has JSON format detection") {
        REQUIRE(OdpRequestOrchestrator::HasJsonFormat("https://test.com/EntitySet?$format=json"));
        REQUIRE(OdpRequestOrchestrator::HasJsonFormat("https://test.com/EntitySet?$top=100&$format=json"));
        REQUIRE(!OdpRequestOrchestrator::HasJsonFormat("https://test.com/EntitySet"));
        REQUIRE(!OdpRequestOrchestrator::HasJsonFormat("https://test.com/EntitySet?$format=xml"));
    }
}
