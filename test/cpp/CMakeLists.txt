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
    test_odata_client.cpp
    test_odata_content.cpp
    test_datasphere_integration.cpp
    test_datasphere_oauth2_consolidated.cpp
    test_datasphere_discovery.cpp
    test_datasphere_asset_consumption.cpp

    test_main.cpp
)

add_executable(erpl_web_tests ${TEST_SOURCES})
target_link_libraries(erpl_web_tests 
                      ${EXTENSION_NAME} 
                      duckdb 
                      test_helpers
                      OpenSSL::SSL OpenSSL::Crypto 
                      tinyxml2::tinyxml2)

                    
# Enable C++17
target_compile_features(${EXTENSION_NAME} PRIVATE cxx_std_17)
target_compile_features(${LOADABLE_EXTENSION_NAME} PRIVATE cxx_std_17)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

if (WIN32)
    target_link_libraries(erpl_web_tests iphlpapi)
endif()

if(MINGW)
    find_package(ZLIB)
    target_link_libraries(erpl_web_tests ZLIB::ZLIB -lcrypt32)
endif()