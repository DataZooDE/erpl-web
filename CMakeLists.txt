cmake_minimum_required(VERSION 3.5...3.29)

option(BUILD_UNITTESTS "Build ERPL C++ Unit Tests." ON)

# Set extension name here
set(TARGET_NAME erpl_web)

find_package(OpenSSL REQUIRED)
find_package(tinyxml2 CONFIG REQUIRED)
find_package(cpptrace CONFIG REQUIRED)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON) #Optional
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
#set(CMAKE_TOOLCHAIN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake" CACHE STRING "Vcpkg toolchain file")

if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    add_compile_options(-Wno-deprecated-declarations)
endif()

if(MSVC)
    add_compile_options(/wd4996)
    # Disable min/max macros to prevent conflicts with std::min/std::max
    add_compile_definitions(NOMINMAX)
endif()

# Try to find mold binary
include(CheckCXXCompilerFlag)
find_program(MOLD_LINKER mold)

if(MOLD_LINKER)
    message(STATUS "Found mold: ${MOLD_LINKER}")
    
    # Check if compiler supports -fuse-ld=mold
    check_cxx_compiler_flag("-fuse-ld=mold" HAS_FUSE_LD_MOLD)
    if(HAS_FUSE_LD_MOLD)
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=mold")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fuse-ld=mold")
        set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -fuse-ld=mold")
        message(STATUS "Using mold as linker with -fuse-ld=mold")
    else()
        message(WARNING "Compiler does not support -fuse-ld=mold")
    endif()
else()
    message(STATUS "mold linker not found, using default linker")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(${TARGET_NAME})
include_directories(
    src/include
    ./duckdb/src/include
    ./duckdb/third_party/
    ./duckdb/third_party/httplib
)

set(EXTENSION_SOURCES
    src/tracing.cpp
    src/duckdb_argument_helper.cpp
    src/charset_converter.cpp
    src/http_client.cpp
    src/odata_attach_functions.cpp
    src/odata_catalog.cpp
    src/odata_client.cpp
    src/odata_content.cpp
    src/odata_edm.cpp
    src/odata_predicate_pushdown_helper.cpp
    src/odata_expand_parser.cpp
    src/odata_read_functions.cpp
    src/odata_storage.cpp
    src/odata_catalog.cpp
    src/odata_transaction_manager.cpp
    src/secret_functions.cpp
    src/web_functions.cpp
    src/erpl_web_extension.cpp
    src/telemetry.cpp
    src/datasphere_client.cpp
    src/datasphere_catalog.cpp
    src/datasphere_read.cpp
    src/datasphere_types.cpp
    src/oauth2_types.cpp
    src/oauth2_browser.cpp
    src/datasphere_local_server.cpp
    src/odata_odp_functions.cpp
    src/datasphere_secret.cpp
    src/oauth2_callback_handler.cpp
    src/oauth2_flow_v2.cpp
    src/oauth2_server.cpp
    src/timeout_http_client.cpp
    src/odata_url_helpers.cpp
    src/odp_http_request_factory.cpp
    src/odp_client_integration.cpp
    src/odp_subscription_repository.cpp
    src/odp_subscription_state_manager.cpp
    src/odp_request_orchestrator.cpp
    src/odp_odata_read_bind_data.cpp
    src/odp_odata_read_functions.cpp
    src/odp_pragma_functions.cpp
    src/sac_url_builder.cpp
    src/sac_client.cpp
    src/sac_catalog.cpp
    src/sac_attach_functions.cpp
    src/sac_read_functions.cpp
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

if(APPLE)
   find_library(COREFOUNDATION_FRAMEWORK CoreFoundation)
   find_library(APPLICATIONSERVICES_FRAMEWORK ApplicationServices)
   target_link_libraries(${EXTENSION_NAME} ${COREFOUNDATION_FRAMEWORK} ${APPLICATIONSERVICES_FRAMEWORK})
   target_link_libraries(${LOADABLE_EXTENSION_NAME} ${COREFOUNDATION_FRAMEWORK} ${APPLICATIONSERVICES_FRAMEWORK})
endif()

if(${BUILD_UNITTESTS})
    add_subdirectory(test)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")