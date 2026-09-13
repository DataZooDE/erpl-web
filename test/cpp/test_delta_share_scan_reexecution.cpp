// Per-execution scan state for the Delta Sharing catalog functions - the #75 class,
// reported as GitHub #202.
//
// delta_share_show_shares / _show_schemas / _show_tables each fetched their rows during
// bind and then walked them with a `current_index` / `finished` pair kept on the BIND
// DATA. DuckDB reuses bind data across executions of a bound plan and nothing reset that
// pair, so the second EXECUTE of a prepared statement resumed from a drained cursor and
// returned zero rows - with no error, which is what makes this class of defect dangerous.
//
// The cursor now lives on DeltaShareRowCursorState, a GlobalTableFunctionState, so every
// execution starts at index 0 over the same immutable payload.
//
// Delta Sharing needs no test-only escape hatch to be driven against a loopback server:
// the endpoint is a field of the profile file the caller passes in, so the fixture is
// just a profile pointing at ODataTestServer.

#include "catch.hpp"
#include "duckdb.hpp"

#include "odata_test_server.hpp"

#include <cstdio>
#include <fstream>
#include <string>

using erpl_web::test_support::CannedResponse;
using erpl_web::test_support::ODataTestServer;

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

// Writes a Delta Sharing profile next to the test binary and removes it again, so the
// fixture leaves nothing behind whichever way the test exits.
class ProfileFile {
public:
    explicit ProfileFile(const std::string &endpoint)
        : path("erpl_delta_share_reexec_profile.json")
    {
        std::ofstream out(path);
        out << R"({"shareCredentialsVersion":1,"endpoint":")" << endpoint
            << R"(","bearerToken":"test-token"})";
    }

    ~ProfileFile() { std::remove(path.c_str()); }

    const std::string &Path() const { return path; }

private:
    std::string path;
};

// Three shares, two schemas and two tables: more than one row, so a cursor that is
// merely off-by-one is also caught, and a count that survives three executions cannot
// be explained by an empty payload.
void ServeCatalog(ODataTestServer &server)
{
    server.OnPath("/shares",
                  CannedResponse::Json(
                      R"({"items":[],"shares":[{"name":"alpha","id":"1"},)"
                      R"({"name":"beta","id":"2"},{"name":"gamma","id":"3"}]})"));
    server.OnPath("/shares/alpha/schemas",
                  CannedResponse::Json(R"({"schemas":[{"name":"sales"},{"name":"hr"}]})"));
    server.OnPath("/shares/alpha/schemas/sales/tables",
                  CannedResponse::Json(R"({"tables":[{"name":"orders"},{"name":"returns"}]})"));
}

int64_t ScalarOf(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result)
{
    return result->GetValue(0, 0).GetValue<int64_t>();
}

// Prepares `sql`, executes it three times and requires `expected_rows` every time. One
// execution proves nothing here: the defect only shows from the second onwards.
void RequireStableAcrossExecutions(duckdb::Connection &con, const std::string &sql,
                                   int64_t expected_rows)
{
    auto prep = con.Query("PREPARE p AS " + sql);
    INFO("PREPARE: " << (prep->HasError() ? prep->GetError() : std::string("ok")));
    REQUIRE_FALSE(prep->HasError());

    for (int execution = 1; execution <= 3; execution++) {
        auto result = con.Query("EXECUTE p");
        INFO("execution " << execution);
        INFO((result->HasError() ? result->GetError() : std::string()));
        REQUIRE_FALSE(result->HasError());
        REQUIRE(ScalarOf(result) == expected_rows);
    }
    REQUIRE_FALSE(con.Query("DEALLOCATE p")->HasError());
}

}  // namespace

TEST_CASE("bound delta_share catalog plans return every row on each execution",
          "[delta_share][reexec]") {
    ODataTestServer server;
    ServeCatalog(server);
    ProfileFile profile(server.BaseUrl());

    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    SECTION("delta_share_show_shares") {
        RequireStableAcrossExecutions(
            con, "SELECT COUNT(*) FROM delta_share_show_shares('" + profile.Path() + "')", 3);
    }

    SECTION("delta_share_show_schemas") {
        RequireStableAcrossExecutions(
            con, "SELECT COUNT(*) FROM delta_share_show_schemas('" + profile.Path() + "', 'alpha')",
            2);
    }

    SECTION("delta_share_show_tables") {
        RequireStableAcrossExecutions(
            con,
            "SELECT COUNT(*) FROM delta_share_show_tables('" + profile.Path() +
                "', 'alpha', 'sales')",
            2);
    }
}

// Two scans of the same function inside one query share neither cursor. Before the fix
// this shape could not even be reached (the first EXECUTE already drained the cursor for
// the second scan), and it is the shape a join or a UNION produces in real use.
TEST_CASE("two delta_share_show_shares scans in one query each see every row",
          "[delta_share][reexec]") {
    ODataTestServer server;
    ServeCatalog(server);
    ProfileFile profile(server.BaseUrl());

    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    const std::string scan = "delta_share_show_shares('" + profile.Path() + "')";
    auto result = con.Query("SELECT (SELECT COUNT(*) FROM " + scan + ") + "
                            "(SELECT COUNT(*) FROM " + scan + ")");
    INFO((result->HasError() ? result->GetError() : std::string()));
    REQUIRE_FALSE(result->HasError());
    REQUIRE(ScalarOf(result) == 6);
}
