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
// So this file covers the state object the fix is built on - that a fresh instance
// re-reads the whole payload, which is what the second execution of a bound plan now
// gets, and that it reports an unusable payload rather than iterating garbage. The wiring
// of the ten Graph table functions to that state is NOT covered by a test; it was made by
// giving each an init_global and is visible only by reading the registration. The full
// end-to-end re-execution behaviour is covered against a real server for the readers that
// can reach one (test_odata_scan_reexecution, test_microsoft_scan_reexecution,
// test_delta_share_scan_reexecution).

#include "catch.hpp"

#include "graph_json_scan.hpp"

#include <string>

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
