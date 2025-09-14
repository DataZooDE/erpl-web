#include "catch.hpp"
#include "odata_odp_functions.hpp"
#include "http_client.hpp"
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
                                "Name": "EntityOfSEPM_ISO"
                            },
                            {
                                "Name": "FactsOf0D_NW_C01"
                            },
                            {
                                "Name": "RegularEntity"
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
                                "Name": "RegularEntity"
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
        "entityof",              // Should not match (case sensitive)
        "factsOf",               // Should not match (case sensitive)
        "OtherEntityOf",         // Should not match (doesn't start with pattern)
        "OtherFactsOf"           // Should not match (doesn't start with pattern)
    };
    
    for (const auto& entity_name : test_cases) {
        bool should_match = (entity_name.find("EntityOf") == 0 || entity_name.find("FactsOf") == 0);
        
        // Create a mock JSON structure
        std::string mock_json = R"({
            "EntitySets": {
                "results": [
                    {
                        "Name": ")" + entity_name + R"("
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
