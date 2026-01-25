find_package(OpenSSL REQUIRED)
find_package(tinyxml2 CONFIG REQUIRED)

include_directories(./include
                    ../../src/include
                    ../../duckdb/third_party/catch/
                    ../../duckdb/third_party/httplib/
                    ../../duckdb/test/include
                    ../../duckdb/extension/json/include
                    ../../duckdb/extension/json/yyjson/include
                    ${OPENSSL_INCLUDE_DIR})

set(TEST_SOURCES
    test_charset_converter.cpp
    test_http_client.cpp
    test_odata_edm.cpp
    test_odata_edm_builder.cpp
    test_odata_client.cpp
    test_odata_content.cpp
    test_odata_url_helpers.cpp
    test_odp_parsing.cpp
    test_odp_http_request_factory.cpp
    test_odp_subscription_repository.cpp
    test_odp_subscription_state_manager.cpp
    test_odp_request_orchestrator.cpp
    test_datasphere_integration.cpp
    test_datasphere_oauth2_consolidated.cpp
    test_datasphere_discovery.cpp
    test_datasphere_asset_consumption.cpp
    test_microsoft_entra_auth.cpp
    test_business_central.cpp
    test_dataverse.cpp
    test_graph_excel.cpp
    test_graph_sharepoint.cpp
    test_graph_planner.cpp
    test_graph_outlook.cpp

    test_main.cpp
)

add_executable(erpl_web_tests ${TEST_SOURCES})
# Avoid ODR violation by not linking both static and shared DuckDB simultaneously
# Link only test_helpers (which already links against DuckDB)
target_link_libraries(erpl_web_tests 
                      ${EXTENSION_NAME} 
                      test_helpers
                      OpenSSL::SSL OpenSSL::Crypto 
                      tinyxml2::tinyxml2)

# Register with CTest and inject sanitizer env overrides
include(CTest)
if (BUILD_TESTING)
    add_test(NAME erpl_web_tests COMMAND erpl_web_tests)
    set_property(TEST erpl_web_tests PROPERTY ENVIRONMENT 
        "ASAN_OPTIONS=detect_odr_violation=0:detect_leaks=0:halt_on_error=0:abort_on_error=0;UBSAN_OPTIONS=halt_on_error=0:print_stacktrace=0;ERPL_DISABLE_TELEMETRY=1")
endif()

                    
# Enable C++17
target_compile_features(${EXTENSION_NAME} PRIVATE cxx_std_17)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

if (WIN32)
    target_link_libraries(erpl_web_tests iphlpapi)
endif()

if(MINGW)
    find_package(ZLIB)
    target_link_libraries(erpl_web_tests ZLIB::ZLIB -lcrypt32)
endif()