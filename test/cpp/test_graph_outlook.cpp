#include "catch.hpp"
#include "graph_outlook_client.hpp"
#include "duckdb.hpp"

using namespace erpl_web;
using namespace duckdb;

// =============================================================================
// URL Builder Tests
// =============================================================================

TEST_CASE("GraphOutlookUrlBuilder builds correct base URL", "[graph_outlook][url]") {
    REQUIRE(GraphOutlookUrlBuilder::GetBaseUrl() == "https://graph.microsoft.com/v1.0");
}

TEST_CASE("GraphOutlookUrlBuilder builds calendar URLs", "[graph_outlook][url]") {
    SECTION("My events URL") {
        auto url = GraphOutlookUrlBuilder::BuildMyEventsUrl();
        REQUIRE(url == "https://graph.microsoft.com/v1.0/me/events");
    }

    SECTION("My calendars URL") {
        auto url = GraphOutlookUrlBuilder::BuildMyCalendarsUrl();
        REQUIRE(url == "https://graph.microsoft.com/v1.0/me/calendars");
    }

    SECTION("Calendar events URL") {
        auto url = GraphOutlookUrlBuilder::BuildCalendarEventsUrl("cal-123");
        REQUIRE(url == "https://graph.microsoft.com/v1.0/me/calendars/cal-123/events");
    }
}

TEST_CASE("GraphOutlookUrlBuilder builds contact URLs", "[graph_outlook][url]") {
    SECTION("My contacts URL") {
        auto url = GraphOutlookUrlBuilder::BuildMyContactsUrl();
        REQUIRE(url == "https://graph.microsoft.com/v1.0/me/contacts");
    }

    SECTION("Contact folders URL") {
        auto url = GraphOutlookUrlBuilder::BuildContactFoldersUrl();
        REQUIRE(url == "https://graph.microsoft.com/v1.0/me/contactFolders");
    }

    SECTION("Folder contacts URL") {
        auto url = GraphOutlookUrlBuilder::BuildFolderContactsUrl("folder-abc");
        REQUIRE(url == "https://graph.microsoft.com/v1.0/me/contactFolders/folder-abc/contacts");
    }
}

TEST_CASE("GraphOutlookUrlBuilder builds mail URLs", "[graph_outlook][url]") {
    SECTION("My messages URL") {
        auto url = GraphOutlookUrlBuilder::BuildMyMessagesUrl();
        REQUIRE(url == "https://graph.microsoft.com/v1.0/me/messages");
    }

    SECTION("Mail folders URL") {
        auto url = GraphOutlookUrlBuilder::BuildMailFoldersUrl();
        REQUIRE(url == "https://graph.microsoft.com/v1.0/me/mailFolders");
    }

    SECTION("Folder messages URL") {
        auto url = GraphOutlookUrlBuilder::BuildFolderMessagesUrl("inbox-xyz");
        REQUIRE(url == "https://graph.microsoft.com/v1.0/me/mailFolders/inbox-xyz/messages");
    }
}

// =============================================================================
// DuckDB Integration Tests
// =============================================================================

TEST_CASE("Graph Outlook functions are registered in DuckDB", "[graph_outlook][duckdb]") {
    DuckDB db(nullptr);
    Connection con(db);

    // Load extension
    con.Query("LOAD erpl_web");

    SECTION("graph_calendar_events function exists") {
        auto result = con.Query("SELECT function_name FROM duckdb_functions() WHERE function_name = 'graph_calendar_events'");
        REQUIRE(result->RowCount() == 1);
    }

    SECTION("graph_contacts function exists") {
        auto result = con.Query("SELECT function_name FROM duckdb_functions() WHERE function_name = 'graph_contacts'");
        REQUIRE(result->RowCount() == 1);
    }

    SECTION("graph_messages function exists") {
        auto result = con.Query("SELECT function_name FROM duckdb_functions() WHERE function_name = 'graph_messages'");
        REQUIRE(result->RowCount() == 1);
    }
}

TEST_CASE("Graph Outlook functions require secret parameter", "[graph_outlook][duckdb]") {
    DuckDB db(nullptr);
    Connection con(db);

    con.Query("LOAD erpl_web");

    SECTION("graph_calendar_events requires secret") {
        auto result = con.Query("SELECT * FROM graph_calendar_events()");
        REQUIRE(result->HasError());
    }

    SECTION("graph_contacts requires secret") {
        auto result = con.Query("SELECT * FROM graph_contacts()");
        REQUIRE(result->HasError());
    }

    SECTION("graph_messages requires secret") {
        auto result = con.Query("SELECT * FROM graph_messages()");
        REQUIRE(result->HasError());
    }
}
