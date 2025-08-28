cmake_minimum_required(VERSION 3.5...3.29)

# Prevent Windows min/max macro conflicts for all Windows builds
if(WIN32)
    add_compile_options(-DNOMINMAX)
endif()

if(MSVC)
    add_compile_options(/wd4996)
    # Prevent Windows min/max macro conflicts
    add_compile_options(/DNOMINMAX)
endif()

option(BUILD_UNITTESTS "Build ERPL C++ Unit Tests." ON)

# Set extension name here
set(TARGET_NAME erpl_web)

find_package(OpenSSL REQUIRED)
find_package(tinyxml2 CONFIG REQUIRED)
find_package(cpptrace CONFIG REQUIRED)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON) #Optional
set(EXTENSION_NAME ${TARGET_NAME}_extension)
#set(CMAKE_TOOLCHAIN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake" CACHE STRING "Vcpkg toolchain file")

if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    add_compile_options(-Wno-deprecated-declarations)
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
    src/erpl_tracing.cpp
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
    src/erpl_datasphere_client.cpp
    src/erpl_datasphere_catalog.cpp
    src/erpl_datasphere_read.cpp
    src/erpl_datasphere_types.cpp
    src/erpl_oauth2_types.cpp
    src/erpl_datasphere_browser.cpp
    src/erpl_datasphere_local_server.cpp

    src/erpl_datasphere_secret.cpp
    src/erpl_oauth2_callback_handler.cpp
    src/erpl_oauth2_flow_v2.cpp
    src/erpl_oauth2_server.cpp
    src/erpl_timeout_http_client.cpp
)

# Build only the static extension to avoid linking conflicts
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
# build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto tinyxml2::tinyxml2 cpptrace::cpptrace)

# Enable C++17
target_compile_features(${EXTENSION_NAME} PRIVATE cxx_std_17)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

if (WIN32)
      target_link_libraries(${EXTENSION_NAME} iphlpapi)
endif()

if(MINGW)
   find_package(ZLIB)
   target_link_libraries(${EXTENSION_NAME} ZLIB::ZLIB -lcrypt32)
endif()

if(APPLE)
   find_library(COREFOUNDATION_FRAMEWORK CoreFoundation)
   find_library(APPLICATIONSERVICES_FRAMEWORK ApplicationServices)
   target_link_libraries(${EXTENSION_NAME} ${COREFOUNDATION_FRAMEWORK} ${APPLICATIONSERVICES_FRAMEWORK})
endif()

if(${BUILD_UNITTESTS})
    add_subdirectory(test)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
