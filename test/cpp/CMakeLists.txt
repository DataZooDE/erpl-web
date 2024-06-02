find_package(OpenSSL REQUIRED)
find_package(TinyXML2 REQUIRED)

set(CMAKE_CXX_STANDARD 17 CACHE STRING "v")
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_EXTENSIONS OFF)

include_directories(./include
                    ../../src/include
                    ../../duckdb/third_party/catch/
                    ../../duckdb/third_party/httplib/
                    ../../duckdb/test/include
                    ../../duckdb/extension/json/include
                    ../../duckdb/extension/json/yyjson/include
                    ${OPENSSL_INCLUDE_DIR})

set(TEST_SOURCES
    test_http_client.cpp
    test_odata_edm.cpp
    test_main.cpp
)

add_executable(erpl_web_tests ${TEST_SOURCES})
target_link_libraries(erpl_web_tests 
                      ${EXTENSION_NAME} 
                      duckdb_static 
                      test_helpers 
                      OpenSSL::SSL OpenSSL::Crypto 
                      tinyxml2::tinyxml2)