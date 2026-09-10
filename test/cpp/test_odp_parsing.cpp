#include "catch.hpp"
#include "odata_url_helpers.hpp"
#include "odata_odp_functions.hpp"
#include "datazoo/oauth2/http_client.hpp"
#include "tracing.hpp"
#include "yyjson.hpp"
#include <memory>

using namespace erpl_web;

TEST_CASE("ODP Entity Set Parsing", "[odp]") {
    // Test JSON parsing for ODP entity sets
    std::string mock_json = R"({
        "d": {
            "results": [
                {
                    "ID": "ZODP_SRV",
                    "Description": "ODP Service",
                    "ServiceUrl": "/sap/opu/odata/sap/ZODP_SRV/",
                    "EntitySets": {
                        "results": [
                            {
                                "ID": "EntityOfSEPM_ISO"
                            },
                            {
                                "ID": "FactsOf0D_NW_C01"
                            },
                            {
                                "ID": "RegularEntity"
                            }
                        ]
                    }
                },
                {
                    "ID": "REGULAR_SRV",
                    "Description": "Regular Service",
                    "ServiceUrl": "/sap/opu/odata/sap/REGULAR_SRV/",
                    "EntitySets": {
                        "results": [
                            {
                                "ID": "RegularEntity"
                            }
                        ]
                    }
                }
            ]
        }
    })";

    // Parse JSON
    auto doc = duckdb_yyjson::yyjson_read(mock_json.c_str(), mock_json.size(), 0);
    REQUIRE(doc != nullptr);
    
    auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
    REQUIRE(root != nullptr);
    
    auto data_obj = duckdb_yyjson::yyjson_obj_get(root, "d");
    REQUIRE(data_obj != nullptr);
    
    auto results_arr = duckdb_yyjson::yyjson_obj_get(data_obj, "results");
    REQUIRE(results_arr != nullptr);
    
    // Test first service (should have ODP entity sets)
    duckdb_yyjson::yyjson_arr_iter arr_it;
    duckdb_yyjson::yyjson_arr_iter_init(results_arr, &arr_it);
    
    auto service_entry = duckdb_yyjson::yyjson_arr_iter_next(&arr_it);
    REQUIRE(service_entry != nullptr);
    
    std::string entity_sets = ExtractOdpEntitySetsFromJson(service_entry);
    REQUIRE(!entity_sets.empty());
    REQUIRE(entity_sets.find("EntityOfSEPM_ISO") != std::string::npos);
    REQUIRE(entity_sets.find("FactsOf0D_NW_C01") != std::string::npos);
    REQUIRE(entity_sets.find("RegularEntity") == std::string::npos); // Should not include non-ODP entities
    
    // Test second service (should not have ODP entity sets)
    service_entry = duckdb_yyjson::yyjson_arr_iter_next(&arr_it);
    REQUIRE(service_entry != nullptr);
    
    entity_sets = ExtractOdpEntitySetsFromJson(service_entry);
    REQUIRE(entity_sets.empty()); // Should be empty for regular service
    
    duckdb_yyjson::yyjson_doc_free(doc);
}

TEST_CASE("ODP Entity Set Pattern Matching", "[odp]") {
    // Test various entity set name patterns
    std::vector<std::string> test_cases = {
        "EntityOfSEPM_ISO",      // Should match
        "FactsOf0D_NW_C01",      // Should match
        "EntityOf",              // Should match (starts with EntityOf)
        "FactsOf",               // Should match (starts with FactsOf)
        "RegularEntity",         // Should not match
        "EntityOfSomething",     // Should match
        "FactsOfSomething",      // Should match
        "entityof",              // Should match (case insensitive)
        "factsOf",               // Should match (case insensitive)
        "OtherEntityOf",         // Should not match (doesn't start with pattern)
        "OtherFactsOf"           // Should not match (doesn't start with pattern)
    };
    
    for (const auto& entity_name : test_cases) {
        // Convert to uppercase for case-insensitive matching (matches implementation)
        std::string entity_name_upper = entity_name;
        std::transform(entity_name_upper.begin(), entity_name_upper.end(), entity_name_upper.begin(), ::toupper);
        bool should_match = (entity_name_upper.find("ENTITYOF") == 0 || entity_name_upper.find("FACTSOF") == 0);
        
        // Create a mock JSON structure
        std::string mock_json = R"({
            "EntitySets": {
                "results": [
                    {
                        "ID": ")" + entity_name + R"("
                    }
                ]
            }
        })";
        
        auto doc = duckdb_yyjson::yyjson_read(mock_json.c_str(), mock_json.size(), 0);
        REQUIRE(doc != nullptr);
        
        auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
        std::string entity_sets = ExtractOdpEntitySetsFromJson(root);
        
        if (should_match) {
            REQUIRE(!entity_sets.empty());
            REQUIRE(entity_sets == entity_name);
        } else {
            REQUIRE(entity_sets.empty());
        }
        
        duckdb_yyjson::yyjson_doc_free(doc);
    }
}

// ---------------------------------------------------------------------------
// GitHub #102 - the delta token arrives on __next, not on __delta
// ---------------------------------------------------------------------------

TEST_CASE("ODP recognises a delta token delivered on __next", "[odp_parsing]") {
    // This is the shape SAP ODP actually returns to end a change-tracked extraction:
    // the token rides on "__next" with a "!deltatoken=" sigil. The orchestrator used to
    // read only "__delta", so extraction returned "", preference_applied went false, the
    // subscription stayed in initial-load mode, and the NEXT read re-extracted the whole
    // dataset instead of fetching deltas.
    const std::string terminal_page = R"({"d":{"results":[],)"
        R"("__next":"https://sap.example.com/sap/opu/odata/sap/X_SRV/EntityOfX?$format=json&!deltatoken=D20260910"}})";

    const auto link = ODataDeltaLink::ExtractDeltaLink(terminal_page);
    REQUIRE_FALSE(link.empty());
    REQUIRE(ODataDeltaLink::ExtractToken(link) == "D20260910");
}

TEST_CASE("ODP does not mistake ordinary server-driven paging for a delta link", "[odp_parsing]") {
    // The counterpart guard: a plain "__next" is a $skiptoken page and must keep paging.
    // Treating it as terminal would truncate the extraction.
    const std::string paging_page = R"({"d":{"results":[{"a":1}],)"
        R"("__next":"https://sap.example.com/sap/opu/odata/sap/X_SRV/EntityOfX?$skiptoken=100"}})";

    REQUIRE(ODataDeltaLink::ExtractDeltaLink(paging_page).empty());
}

TEST_CASE("A v2 __delta link still works", "[odp_parsing]") {
    const std::string delta_page = R"({"d":{"results":[],)"
        R"("__delta":"https://sap.example.com/sap/opu/odata/sap/X_SRV/EntityOfX?!deltatoken=D1"}})";
    REQUIRE(ODataDeltaLink::ExtractToken(ODataDeltaLink::ExtractDeltaLink(delta_page)) == "D1");
}
