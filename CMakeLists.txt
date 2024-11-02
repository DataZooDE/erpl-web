cmake_minimum_required(VERSION 3.5...3.29)

option(BUILD_UNITTESTS "Build ERPL C++ Unit Tests." OFF)

# Set extension name here
set(TARGET_NAME erpl_web)

find_package(OpenSSL REQUIRED)
find_package(tinyxml2 CONFIG REQUIRED)
find_package(cpptrace CONFIG REQUIRED)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON) #Optional
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
set(CMAKE_TOOLCHAIN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake" CACHE STRING "Vcpkg toolchain file")

if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    add_compile_options(-Wno-deprecated-declarations)
endif()

if(MSVC)
    add_compile_options(/wd4996)
endif()

project(${TARGET_NAME})
include_directories(
    src/include
    ./duckdb/third_party/
    ./duckdb/third_party/httplib
)

set(EXTENSION_SOURCES
    src/duckdb_argument_helper.cpp
    src/charset_converter.cpp
    src/erpl_http_client.cpp
    src/erpl_odata_attach_functions.cpp
    src/erpl_odata_catalog.cpp
    src/erpl_odata_client.cpp
    src/erpl_odata_content.cpp
    src/erpl_odata_edm.cpp
    src/erpl_odata_predicate_pushdown_helper.cpp
    src/erpl_odata_read_functions.cpp
    src/erpl_odata_storage.cpp
    src/erpl_odata_transaction_manager.cpp
    src/erpl_secret_functions.cpp
    src/erpl_web_functions.cpp
    src/erpl_web_extension.cpp
    src/telemetry.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto tinyxml2::tinyxml2 cpptrace::cpptrace)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto tinyxml2::tinyxml2 cpptrace::cpptrace)

# Enable C++17
target_compile_features(${EXTENSION_NAME} PRIVATE cxx_std_17)
target_compile_features(${LOADABLE_EXTENSION_NAME} PRIVATE cxx_std_17)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

if (WIN32)
      target_link_libraries(${EXTENSION_NAME} iphlpapi)
      target_link_libraries(${LOADABLE_EXTENSION_NAME} iphlpapi)
endif()

if(MINGW)
   find_package(ZLIB)
   target_link_libraries(${EXTENSION_NAME} ZLIB::ZLIB -lcrypt32)
   target_link_libraries(${LOADABLE_EXTENSION_NAME} ZLIB::ZLIB -lcrypt32)
endif()

if(${BUILD_UNITTESTS})
      add_subdirectory(test)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
