#include "catch.hpp"
#include "duckdb.hpp"
#include "graph_excel_client.hpp"

using namespace erpl_web;

// =============================================================================
// GraphExcelUrlBuilder Tests
// =============================================================================

TEST_CASE("GraphExcelUrlBuilder - GetBaseUrl", "[graph_excel][url_builder]") {
    REQUIRE(GraphExcelUrlBuilder::GetBaseUrl() == "https://graph.microsoft.com/v1.0");
}

TEST_CASE("GraphExcelUrlBuilder - BuildDriveItemUrl", "[graph_excel][url_builder]") {
    auto url = GraphExcelUrlBuilder::BuildDriveItemUrl("abc123");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/me/drive/items/abc123");
}

TEST_CASE("GraphExcelUrlBuilder - BuildDriveItemByPathUrl", "[graph_excel][url_builder]") {
    // Without leading slash
    auto url1 = GraphExcelUrlBuilder::BuildDriveItemByPathUrl("Documents/test.xlsx");
    REQUIRE(url1 == "https://graph.microsoft.com/v1.0/me/drive/root:/Documents/test.xlsx:");

    // With leading slash (should be removed)
    auto url2 = GraphExcelUrlBuilder::BuildDriveItemByPathUrl("/Documents/test.xlsx");
    REQUIRE(url2 == "https://graph.microsoft.com/v1.0/me/drive/root:/Documents/test.xlsx:");
}

TEST_CASE("GraphExcelUrlBuilder - BuildSiteDriveItemUrl", "[graph_excel][url_builder]") {
    auto url = GraphExcelUrlBuilder::BuildSiteDriveItemUrl("site-id-123", "item-id-456");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/sites/site-id-123/drive/items/item-id-456");
}

TEST_CASE("GraphExcelUrlBuilder - BuildWorkbookUrl", "[graph_excel][url_builder]") {
    std::string item_url = "https://graph.microsoft.com/v1.0/me/drive/items/abc123";
    auto url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    REQUIRE(url == "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook");
}

TEST_CASE("GraphExcelUrlBuilder - BuildTablesUrl", "[graph_excel][url_builder]") {
    std::string workbook_url = "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook";
    auto url = GraphExcelUrlBuilder::BuildTablesUrl(workbook_url);
    REQUIRE(url == "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook/tables");
}

TEST_CASE("GraphExcelUrlBuilder - BuildTableUrl", "[graph_excel][url_builder]") {
    std::string workbook_url = "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook";
    auto url = GraphExcelUrlBuilder::BuildTableUrl(workbook_url, "MyTable");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook/tables/MyTable");
}

TEST_CASE("GraphExcelUrlBuilder - BuildTableRowsUrl", "[graph_excel][url_builder]") {
    std::string workbook_url = "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook";
    auto url = GraphExcelUrlBuilder::BuildTableRowsUrl(workbook_url, "MyTable");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook/tables/MyTable/rows");
}

TEST_CASE("GraphExcelUrlBuilder - BuildWorksheetsUrl", "[graph_excel][url_builder]") {
    std::string workbook_url = "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook";
    auto url = GraphExcelUrlBuilder::BuildWorksheetsUrl(workbook_url);
    REQUIRE(url == "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook/worksheets");
}

TEST_CASE("GraphExcelUrlBuilder - BuildWorksheetUrl", "[graph_excel][url_builder]") {
    std::string workbook_url = "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook";
    auto url = GraphExcelUrlBuilder::BuildWorksheetUrl(workbook_url, "Sheet1");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook/worksheets/Sheet1");
}

TEST_CASE("GraphExcelUrlBuilder - BuildUsedRangeUrl", "[graph_excel][url_builder]") {
    std::string workbook_url = "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook";
    auto url = GraphExcelUrlBuilder::BuildUsedRangeUrl(workbook_url, "Sheet1");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook/worksheets/Sheet1/usedRange");
}

TEST_CASE("GraphExcelUrlBuilder - BuildRangeUrl", "[graph_excel][url_builder]") {
    std::string workbook_url = "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook";
    auto url = GraphExcelUrlBuilder::BuildRangeUrl(workbook_url, "Sheet1", "A1:D10");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/me/drive/items/abc123/workbook/worksheets/Sheet1/range(address='A1:D10')");
}

TEST_CASE("GraphExcelUrlBuilder - BuildDriveRootChildrenUrl", "[graph_excel][url_builder]") {
    auto url = GraphExcelUrlBuilder::BuildDriveRootChildrenUrl();
    REQUIRE(url == "https://graph.microsoft.com/v1.0/me/drive/root/children");
}

TEST_CASE("GraphExcelUrlBuilder - BuildDriveFolderChildrenUrl", "[graph_excel][url_builder]") {
    auto url = GraphExcelUrlBuilder::BuildDriveFolderChildrenUrl("folder-id-123");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/me/drive/items/folder-id-123/children");
}

TEST_CASE("GraphExcelUrlBuilder - BuildSiteDriveRootChildrenUrl", "[graph_excel][url_builder]") {
    auto url = GraphExcelUrlBuilder::BuildSiteDriveRootChildrenUrl("site-id-123");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/sites/site-id-123/drive/root/children");
}

// =============================================================================
// DuckDB Integration Tests (Secret Creation)
// =============================================================================

TEST_CASE("Microsoft Graph Secret Creation via DuckDB", "[graph_excel][secret]") {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    // Load the extension
    auto result = con.Query("LOAD erpl_web");
    REQUIRE_FALSE(result->HasError());

    SECTION("Create secret with client_credentials provider") {
        result = con.Query(R"(
            CREATE SECRET test_graph_secret (
                TYPE microsoft_graph,
                tenant_id 'test-tenant-12345',
                client_id 'test-client-67890',
                client_secret 'test-secret-abcde'
            )
        )");
        REQUIRE_FALSE(result->HasError());

        // Verify secret was created
        result = con.Query("SELECT count(*) FROM duckdb_secrets() WHERE name = 'test_graph_secret'");
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);

        // Cleanup
        con.Query("DROP SECRET test_graph_secret");
    }

    SECTION("Create secret with config provider") {
        result = con.Query(R"(
            CREATE SECRET test_graph_config_secret (
                TYPE microsoft_graph,
                PROVIDER config,
                tenant_id 'test-tenant-12345',
                access_token 'pre-acquired-token-xyz'
            )
        )");
        REQUIRE_FALSE(result->HasError());

        // Verify secret was created
        result = con.Query("SELECT count(*) FROM duckdb_secrets() WHERE name = 'test_graph_config_secret'");
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);

        // Cleanup
        con.Query("DROP SECRET test_graph_config_secret");
    }

    SECTION("Validation - missing tenant_id") {
        result = con.Query(R"(
            CREATE SECRET test_graph_invalid (
                TYPE microsoft_graph,
                client_id 'test-client',
                client_secret 'test-secret'
            )
        )");
        REQUIRE(result->HasError());
        REQUIRE(result->GetError().find("tenant_id") != std::string::npos);
    }

    SECTION("Validation - missing client_id") {
        result = con.Query(R"(
            CREATE SECRET test_graph_invalid (
                TYPE microsoft_graph,
                tenant_id 'test-tenant',
                client_secret 'test-secret'
            )
        )");
        REQUIRE(result->HasError());
        REQUIRE(result->GetError().find("client_id") != std::string::npos);
    }

    SECTION("Validation - missing client_secret") {
        result = con.Query(R"(
            CREATE SECRET test_graph_invalid (
                TYPE microsoft_graph,
                tenant_id 'test-tenant',
                client_id 'test-client'
            )
        )");
        REQUIRE(result->HasError());
        REQUIRE(result->GetError().find("client_secret") != std::string::npos);
    }
}

TEST_CASE("Microsoft Graph Excel Functions Exist", "[graph_excel][functions]") {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    // Load the extension
    auto result = con.Query("LOAD erpl_web");
    REQUIRE_FALSE(result->HasError());

    SECTION("graph_list_files function exists") {
        result = con.Query("SELECT count(*) FROM duckdb_functions() WHERE function_name = 'graph_list_files'");
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
    }

    SECTION("graph_excel_tables function exists") {
        result = con.Query("SELECT count(*) FROM duckdb_functions() WHERE function_name = 'graph_excel_tables'");
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
    }

    SECTION("graph_excel_worksheets function exists") {
        result = con.Query("SELECT count(*) FROM duckdb_functions() WHERE function_name = 'graph_excel_worksheets'");
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
    }

    SECTION("graph_excel_range function exists") {
        result = con.Query("SELECT count(*) FROM duckdb_functions() WHERE function_name = 'graph_excel_range'");
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
    }

    SECTION("graph_excel_table_data function exists") {
        result = con.Query("SELECT count(*) FROM duckdb_functions() WHERE function_name = 'graph_excel_table_data'");
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
    }
}
