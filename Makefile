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

# Fast incremental build for local development.
# Skips cmake reconfiguration (and the vcpkg compiler-hash check that comes with it)
# when the build directory is already configured. Ninja detects CMakeLists.txt changes
# on its own and re-runs cmake automatically when needed.
# Use 'make debug' to force a full reconfigure (e.g. after adding a new dependency).
.PHONY: dev
dev:
	@if [ ! -f build/debug/build.ninja ]; then \
		echo "Build directory not configured yet — running full cmake configure..."; \
		mkdir -p build/debug && \
		cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_DEBUG_FLAGS) $(VCPKG_MANIFEST_FLAGS) \
			-DCMAKE_BUILD_TYPE=Debug -S $(DUCKDB_SRCDIR) -B build/debug; \
	fi
	cmake --build build/debug --config Debug

release_win: ${EXTENSION_CONFIG_STEP}
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Release -S $(DUCKDB_SRCDIR) -B build/release
	cmake --build build/release --config Release --parallel

test_debug_sap: ${EXTENSION_CONFIG_STEP}
	ERPL_SAP_BASE_URL='http://localhost:50000' ERPL_SAP_PASSWORD='ABAPtr2023#00' ./build/debug/test/unittest "[sap]"

# Run Microsoft 365 / Graph API integration tests against a real tenant.
# Requires environment variables sourced from an Azure App Registration with
# application permissions granted and admin-consented. See .env.microsoft.
#   ERPL_MS_TENANT_ID           - Directory (tenant) ID
#   ERPL_MS_CLIENT_ID           - Application (client) ID
#   ERPL_MS_CLIENT_SECRET       - Client secret value (not the secret ID)
#   ERPL_MS_SHAREPOINT_SITE_ID  - SharePoint site composite ID for SP tests
#   ERPL_MS_SHAREPOINT_LIST_ID  - SharePoint list ID within that site for SP tests
#   ERPL_MS_PLANNER_GROUP_ID    - Microsoft 365 group ID for Planner tests
#   ERPL_MS_PLANNER_PLAN_ID     - Planner plan ID within that group
#   ERPL_MS_EXCEL_SITE_ID       - SharePoint site ID containing the Excel test file
#   ERPL_MS_EXCEL_DRIVE_ID      - Drive ID within that site (from graph_show_drives)
#   ERPL_MS_EXCEL_FILE_PATH     - Relative path to .xlsx file within the drive
# Usage:
#   source .env.microsoft && make test_debug_ms
test_debug_ms: ${EXTENSION_CONFIG_STEP}
	./build/debug/test/unittest "test/sql/graph_entra_integration.test"
	./build/debug/test/unittest "test/sql/graph_planner_integration.test"
	./build/debug/test/unittest "test/sql/graph_sharepoint_integration.test"
	./build/debug/test/unittest "test/sql/graph_excel_integration.test"
	./build/debug/test/unittest "test/sql/graph_outlook_integration.test"
	./build/debug/test/unittest "test/sql/graph_teams_integration.test"

# Run Business Central integration tests against a live BC environment.
# Requires environment variables sourced from an Azure App Registration registered in BC.
# See .env.business_central for the required variables:
#   ERPL_BC_TENANT_ID    - Azure Directory (tenant) ID
#   ERPL_BC_CLIENT_ID    - Application (client) ID
#   ERPL_BC_CLIENT_SECRET - Client secret value
#   ERPL_BC_ENVIRONMENT  - BC environment name (e.g. 'Production')
#   ERPL_BC_COMPANY_ID   - Company GUID to run entity-read tests against
# Usage:
#   source .env.business_central && make test_debug_bc
test_debug_bc: ${EXTENSION_CONFIG_STEP}
	./build/debug/test/unittest "test/sql/business_central_integration.test"

# Run C++ unit tests. ASAN_OPTIONS=detect_odr_violation=0 suppresses a false-positive ODR
# warning that arises from DuckDB being compiled into both the static test binary and
# libduckdb.so. The duplicate symbol (LOOKUP_TABLE in nested_to_varchar_cast.cpp) is
# benign—both copies are identical—but ASAN fires anyway due to debug linking strategy.
test_cpp: ${EXTENSION_CONFIG_STEP}
	ASAN_OPTIONS=detect_odr_violation=0 ./build/debug/extension/erpl_web/test/cpp/erpl_web_tests

