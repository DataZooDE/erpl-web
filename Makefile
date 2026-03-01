PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=erpl_web
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

VCPKG_TOOLCHAIN_PATH?=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

ifeq ($(OS),Windows_NT)
    SKIP_TESTS=1
endif

release_win: ${EXTENSION_CONFIG_STEP}
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Release -S $(DUCKDB_SRCDIR) -B build/release
	cmake --build build/release --config Release --parallel

test_debug_sap: ${EXTENSION_CONFIG_STEP}
	ERPL_SAP_BASE_URL='http://localhost:50000' ERPL_SAP_PASSWORD='ABAPtr2023#00' ./build/debug/test/unittest "[sap]"

# Run C++ unit tests. ASAN_OPTIONS=detect_odr_violation=0 suppresses a false-positive ODR
# warning that arises from DuckDB being compiled into both the static test binary and
# libduckdb.so. The duplicate symbol (LOOKUP_TABLE in nested_to_varchar_cast.cpp) is
# benign—both copies are identical—but ASAN fires anyway due to debug linking strategy.
test_cpp: ${EXTENSION_CONFIG_STEP}
	ASAN_OPTIONS=detect_odr_violation=0 ./build/debug/extension/erpl_web/test/cpp/erpl_web_tests

