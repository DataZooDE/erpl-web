#include "catch.hpp"
#include "duckdb.hpp"
#include "graph_sharepoint_client.hpp"

using namespace erpl_web;

// =============================================================================
// GraphSharePointUrlBuilder Tests
// =============================================================================

TEST_CASE("GraphSharePointUrlBuilder - GetBaseUrl", "[graph_sharepoint][url_builder]") {
    REQUIRE(GraphSharePointUrlBuilder::GetBaseUrl() == "https://graph.microsoft.com/v1.0");
}

TEST_CASE("GraphSharePointUrlBuilder - BuildSitesSearchUrl", "[graph_sharepoint][url_builder]") {
    // Empty search returns wildcard
    auto url1 = GraphSharePointUrlBuilder::BuildSitesSearchUrl();
    REQUIRE(url1 == "https://graph.microsoft.com/v1.0/sites?search=*");

    // With search query
    auto url2 = GraphSharePointUrlBuilder::BuildSitesSearchUrl("contoso");
    REQUIRE(url2 == "https://graph.microsoft.com/v1.0/sites?search=contoso");
}

TEST_CASE("GraphSharePointUrlBuilder - BuildSiteUrl", "[graph_sharepoint][url_builder]") {
    auto url = GraphSharePointUrlBuilder::BuildSiteUrl("site-id-123");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/sites/site-id-123");
}

TEST_CASE("GraphSharePointUrlBuilder - BuildSiteListsUrl", "[graph_sharepoint][url_builder]") {
    auto url = GraphSharePointUrlBuilder::BuildSiteListsUrl("site-id-123");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/sites/site-id-123/lists");
}

TEST_CASE("GraphSharePointUrlBuilder - BuildListUrl", "[graph_sharepoint][url_builder]") {
    auto url = GraphSharePointUrlBuilder::BuildListUrl("site-id-123", "list-id-456");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/sites/site-id-123/lists/list-id-456");
}

TEST_CASE("GraphSharePointUrlBuilder - BuildListColumnsUrl", "[graph_sharepoint][url_builder]") {
    auto url = GraphSharePointUrlBuilder::BuildListColumnsUrl("site-id-123", "list-id-456");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/sites/site-id-123/lists/list-id-456/columns");
}

TEST_CASE("GraphSharePointUrlBuilder - BuildListItemsUrl", "[graph_sharepoint][url_builder]") {
    auto url = GraphSharePointUrlBuilder::BuildListItemsUrl("site-id-123", "list-id-456");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/sites/site-id-123/lists/list-id-456/items");
}

TEST_CASE("GraphSharePointUrlBuilder - BuildListItemsWithFieldsUrl", "[graph_sharepoint][url_builder]") {
    auto url = GraphSharePointUrlBuilder::BuildListItemsWithFieldsUrl("site-id-123", "list-id-456");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/sites/site-id-123/lists/list-id-456/items?expand=fields");
}

TEST_CASE("GraphSharePointUrlBuilder - BuildListItemsWithSelectUrl", "[graph_sharepoint][url_builder]") {
    // With select and top
    auto url1 = GraphSharePointUrlBuilder::BuildListItemsWithSelectUrl("site-id-123", "list-id-456", "Title,Created", 100);
    REQUIRE(url1 == "https://graph.microsoft.com/v1.0/sites/site-id-123/lists/list-id-456/items?expand=fields&$select=Title,Created&$top=100");

    // Without select, with top
    auto url2 = GraphSharePointUrlBuilder::BuildListItemsWithSelectUrl("site-id-123", "list-id-456", "", 50);
    REQUIRE(url2 == "https://graph.microsoft.com/v1.0/sites/site-id-123/lists/list-id-456/items?expand=fields&$top=50");

    // Without select, without top
    auto url3 = GraphSharePointUrlBuilder::BuildListItemsWithSelectUrl("site-id-123", "list-id-456");
    REQUIRE(url3 == "https://graph.microsoft.com/v1.0/sites/site-id-123/lists/list-id-456/items?expand=fields");
}

TEST_CASE("GraphSharePointUrlBuilder - BuildItemUrl", "[graph_sharepoint][url_builder]") {
    auto url = GraphSharePointUrlBuilder::BuildItemUrl("site-id-123", "list-id-456", "item-id-789");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/sites/site-id-123/lists/list-id-456/items/item-id-789");
}

TEST_CASE("GraphSharePointUrlBuilder - BuildFollowedSitesUrl", "[graph_sharepoint][url_builder]") {
    auto url = GraphSharePointUrlBuilder::BuildFollowedSitesUrl();
    REQUIRE(url == "https://graph.microsoft.com/v1.0/me/followedSites");
}

TEST_CASE("GraphSharePointUrlBuilder - BuildSiteByPathUrl", "[graph_sharepoint][url_builder]") {
    // Without site path (root site)
    auto url1 = GraphSharePointUrlBuilder::BuildSiteByPathUrl("contoso.sharepoint.com", "");
    REQUIRE(url1 == "https://graph.microsoft.com/v1.0/sites/contoso.sharepoint.com");

    // With site path
    auto url2 = GraphSharePointUrlBuilder::BuildSiteByPathUrl("contoso.sharepoint.com", "sites/marketing");
    REQUIRE(url2 == "https://graph.microsoft.com/v1.0/sites/contoso.sharepoint.com:/sites/marketing:");

    // With leading slash (should be removed)
    auto url3 = GraphSharePointUrlBuilder::BuildSiteByPathUrl("contoso.sharepoint.com", "/sites/hr");
    REQUIRE(url3 == "https://graph.microsoft.com/v1.0/sites/contoso.sharepoint.com:/sites/hr:");
}

// =============================================================================
// DuckDB Integration Tests (Function Existence)
// =============================================================================

TEST_CASE("Microsoft Graph SharePoint Functions Exist", "[graph_sharepoint][functions]") {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    // Load the extension
    auto result = con.Query("LOAD erpl_web");
    REQUIRE_FALSE(result->HasError());

    SECTION("graph_show_sites function exists") {
        result = con.Query("SELECT count(*) FROM duckdb_functions() WHERE function_name = 'graph_show_sites'");
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
    }

    SECTION("graph_show_lists function exists") {
        result = con.Query("SELECT count(*) FROM duckdb_functions() WHERE function_name = 'graph_show_lists'");
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
    }

    SECTION("graph_describe_list function exists") {
        result = con.Query("SELECT count(*) FROM duckdb_functions() WHERE function_name = 'graph_describe_list'");
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
    }

    SECTION("graph_list_items function exists") {
        result = con.Query("SELECT count(*) FROM duckdb_functions() WHERE function_name = 'graph_list_items'");
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
    }
}

TEST_CASE("Microsoft Graph SharePoint uses Graph Secret Type", "[graph_sharepoint][secret]") {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    // Load the extension
    auto result = con.Query("LOAD erpl_web");
    REQUIRE_FALSE(result->HasError());

    SECTION("Graph secret type can be used for SharePoint") {
        result = con.Query(R"(
            CREATE SECRET test_sp_secret (
                TYPE microsoft_graph,
                tenant_id 'test-tenant-12345',
                client_id 'test-client-67890',
                client_secret 'test-secret-abcde'
            )
        )");
        REQUIRE_FALSE(result->HasError());

        // Verify secret was created
        result = con.Query("SELECT count(*) FROM duckdb_secrets() WHERE name = 'test_sp_secret'");
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);

        // Cleanup
        con.Query("DROP SECRET test_sp_secret");
    }
}
