#include "catch.hpp"
#include "odata_read_functions.hpp"

using namespace erpl_web;

TEST_CASE("ODataRowBuffer - basic AddRows and GetNextRow", "[odata_row_buffer]") {
    ODataRowBuffer buf;
    REQUIRE(buf.Size() == 0);
    REQUIRE(!buf.HasMoreRows());

    std::vector<std::vector<duckdb::Value>> rows = {
        {duckdb::Value("row0col0"), duckdb::Value("row0col1")},
        {duckdb::Value("row1col0"), duckdb::Value("row1col1")},
    };

    buf.AddRows(rows);

    REQUIRE(buf.Size() == 2);
    REQUIRE(buf.HasMoreRows());

    auto r0 = buf.GetNextRow();
    REQUIRE(r0.size() == 2);
    REQUIRE(r0[0].ToString() == "row0col0");
    REQUIRE(r0[1].ToString() == "row0col1");
    REQUIRE(buf.Size() == 1);

    auto r1 = buf.GetNextRow();
    REQUIRE(r1[0].ToString() == "row1col0");
    REQUIRE(!buf.HasMoreRows());

    // Empty deque returns empty row
    auto empty = buf.GetNextRow();
    REQUIRE(empty.empty());
}

TEST_CASE("ODataRowBuffer - AddRows with move (no copy)", "[odata_row_buffer]") {
    ODataRowBuffer buf;

    std::vector<std::vector<duckdb::Value>> rows = {
        {duckdb::Value("a"), duckdb::Value(42)},
        {duckdb::Value("b"), duckdb::Value(99)},
    };

    buf.AddRows(std::move(rows));

    // After move, rows is in a valid but unspecified state (usually empty)
    REQUIRE(buf.Size() == 2);

    auto r0 = buf.GetNextRow();
    REQUIRE(r0[0].ToString() == "a");
    REQUIRE(r0[1].GetValue<int32_t>() == 42);
}

TEST_CASE("ODataRowBuffer - page management", "[odata_row_buffer]") {
    ODataRowBuffer buf;
    REQUIRE(!buf.HasNextPage());

    buf.SetHasNextPage(true);
    REQUIRE(buf.HasNextPage());

    buf.SetHasNextPage(false);
    REQUIRE(!buf.HasNextPage());
}

TEST_CASE("ODataRowBuffer - Clear removes all rows", "[odata_row_buffer]") {
    ODataRowBuffer buf;

    std::vector<std::vector<duckdb::Value>> rows = {
        {duckdb::Value("x")}, {duckdb::Value("y")},
    };
    buf.AddRows(std::move(rows));
    REQUIRE(buf.Size() == 2);

    buf.Clear();
    REQUIRE(buf.Size() == 0);
    REQUIRE(!buf.HasMoreRows());
}
