// Per-execution scan state for the Microsoft Graph SharePoint and Excel readers, and for
// the ATTACHed SharePoint list scan - the #75 class, reported as GitHub #202.
//
// Every one of these functions kept its scan cursor - the parsed yyjson document, the
// array iterator over it, and a `done` flag - on the BIND DATA, which DuckDB reuses
// across executions of a bound plan. Worse, the parse step cleared the buffered response
// as it consumed it, so a second execution had nothing left to re-parse either. The
// second EXECUTE returned zero rows, with no error.
//
// The cursor now lives on GraphJsonArrayScanState (and ExcelRangeScanState for
// graph_excel_range), both GlobalTableFunctionState, created fresh per execution.
//
// WHAT THIS FILE DOES AND DOES NOT COVER
//
// GraphClient::BaseUrl() is a hardcoded https://graph.microsoft.com/v1.0 with no
// override, so - unlike Business Central, whose `environment` may already be a URL -
// these functions cannot be driven against the loopback ODataTestServer without adding a
// new base-URL escape hatch. Such a hatch decides where an OAuth bearer token is sent and
// is a security change well beyond the scope of a state-lifetime fix, so it is not made
// here.
//
// So this file asserts the structural property the fix consists of: every Graph scan
// function declares an init_global, which is the ONLY way a DuckDB scan can hold
// per-execution state. A function whose cursor is back on the bind data has no
// init_global to declare. This is not a substitute for an end-to-end test, but it is
// falsifiable and it is driven by a name table, so a new Graph reader must be added to it
// - an agent-crew review found excel_table_scan missed by the first cut of this fix
// precisely because nothing asserted the wiring. It is paired with direct coverage of the
// state objects themselves below. The full end-to-end re-execution behaviour is covered
// against a real server for the readers that can reach one
// (test_odata_scan_reexecution, test_microsoft_scan_reexecution,
// test_delta_share_scan_reexecution).
//
// Note on the include set: Catalog::GetEntry<TableFunctionCatalogEntry> odr-uses that
// entry type's static `Name`, which the linker then reports as duplicated against
// libduckdb_static.a. The untemplated GetEntry plus Cast<> avoids that and is why the
// lookup below is written the long way.

#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"

#include "graph_json_scan.hpp"

#include <string>
#include <vector>

namespace {

// See CLAUDE.md: a bare DuckDB(nullptr) crashes in DEBUG builds shortly after the first
// query unless jemalloc's background threads own arena management.
class TestDatabase {
public:
    TestDatabase()
    {
        config.SetOption("allocator_background_threads", duckdb::Value::BOOLEAN(true));
        database = duckdb::make_uniq<duckdb::DuckDB>(nullptr, &config);
        connection = duckdb::make_uniq<duckdb::Connection>(*database);
    }

    duckdb::Connection &Con() const { return *connection; }

private:
    duckdb::DBConfig config;
    duckdb::unique_ptr<duckdb::DuckDB> database;
    duckdb::unique_ptr<duckdb::Connection> connection;
};

}  // namespace

// Every table function whose scan walks a JSON array, follows @odata.nextLink, or steps
// through row vectors materialised at bind. The single-row write functions
// (graph_excel_add_rows, graph_sharepoint_create_item, ...) are deliberately absent: they
// are one-shot by design, tracked separately.
TEST_CASE("every Graph scan function declares per-execution state", "[graph][reexec]") {
    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    const std::vector<std::string> scanning_functions = {
        // SharePoint
        "graph_show_sites", "graph_show_drives", "graph_show_lists", "graph_describe_list",
        "graph_sharepoint_list_read",
        // Excel
        "graph_show_files", "graph_excel_tables", "graph_excel_worksheets", "graph_excel_range",
        "graph_excel_read",
        // Entra
        "graph_users", "graph_groups", "graph_devices", "graph_signin_logs",
        // Teams
        "graph_my_teams", "graph_teams_channels", "graph_teams_members", "graph_channel_messages",
        // Outlook
        "graph_calendars", "graph_calendar_events", "graph_contacts",
        "graph_outlook_mail_folders", "graph_outlook_emails",
        // Planner
        "graph_planner_plans", "graph_planner_buckets", "graph_planner_tasks",
        // Delta Sharing
        "delta_share_show_shares", "delta_share_show_schemas", "delta_share_show_tables",
    };

    // Not covered here: the two ATTACHed scans, sharepoint_list_scan and excel_table_scan.
    // They are never registered in the catalog - each is built on demand by its table
    // entry's GetScanFunction - so a name lookup cannot reach them. Both were converted in
    // the same change; excel_table_scan is the one this test would not have caught.
    auto &context = *con.context;
    context.transaction.BeginTransaction();
    for (const auto &name : scanning_functions) {
        INFO("table function " << name);
        auto &entry = duckdb::Catalog::GetEntry(context, duckdb::CatalogType::TABLE_FUNCTION_ENTRY,
                                                INVALID_CATALOG, DEFAULT_SCHEMA, name)
                          .Cast<duckdb::TableFunctionCatalogEntry>();
        REQUIRE_FALSE(entry.functions.functions.empty());
        for (const auto &overload : entry.functions.functions) {
            REQUIRE(overload.init_global != nullptr);
        }
    }
    context.transaction.Commit();
}

TEST_CASE("a fresh GraphJsonArrayScanState re-reads the whole payload", "[graph][reexec]") {
    const std::string payload = R"({"value":[{"id":"1"},{"id":"2"},{"id":"3"}]})";

    // Two states over the same response - which is what two executions of one bound plan
    // now get - each walk all three items. Before the fix there was one iterator, shared,
    // and the payload was destroyed by the first parse.
    for (int execution = 1; execution <= 2; execution++) {
        INFO("execution " << execution);
        erpl_web::GraphJsonArrayScanState state;
        REQUIRE(state.NeedsFetch());
        state.SeedFrom(payload);
        REQUIRE_FALSE(state.NeedsFetch());
        REQUIRE(state.InitIterator());

        int seen = 0;
        while (duckdb_yyjson::yyjson_arr_iter_next(&state.item_iter) != nullptr) {
            seen++;
        }
        REQUIRE(seen == 3);
    }
}

TEST_CASE("GraphJsonArrayScanState reports a payload it cannot use", "[graph][reexec]") {
    SECTION("unparseable JSON") {
        erpl_web::GraphJsonArrayScanState state;
        state.SeedFrom("{not json");
        REQUIRE_FALSE(state.InitIterator());
    }

    SECTION("the array key is absent") {
        erpl_web::GraphJsonArrayScanState state;
        state.SeedFrom(R"({"error":{"code":"itemNotFound"}})");
        REQUIRE_FALSE(state.InitIterator());
    }

    SECTION("the array key holds something other than an array") {
        erpl_web::GraphJsonArrayScanState state;
        state.SeedFrom(R"({"value":"not-an-array"})");
        REQUIRE_FALSE(state.InitIterator());
    }
}
