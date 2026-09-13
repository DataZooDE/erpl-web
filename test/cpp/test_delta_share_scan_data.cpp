// Data-path tests for delta_share_scan - GitHub #207.
//
// Four defects lived in this scan, all pre-existing and all surviving six review rounds
// for the same reason: every real `SELECT ... FROM delta_share_scan(...)` in test/sql was
// commented out, so nothing executed the scan body at all.
//
//   1. The share-server-supplied file URL was concatenated into SQL text and run through
//      Connection::Query, which accepts multiple statements - remote SQL injection.
//   2. Only the first Fetch() chunk of each file was emitted before the file index
//      advanced, so at most 2048 rows per file were ever returned.
//   3. The emitted chunk referenced a MaterializedQueryResult destroyed at the end of the
//      enclosing try - a use-after-free.
//   4. A file that failed to read was swallowed into an empty chunk, which ends the scan,
//      so one bad file truncated the table and reported success.
//
// The fixture needs no httpfs: a Delta Sharing file reference is just a URL, and a local
// path is one parquet_scan accepts. So the share server is ODataTestServer serving the
// Delta Sharing NDJSON, and the "presigned URLs" are local parquet files this test writes.

#include "catch.hpp"
#include "duckdb.hpp"

#include "odata_test_server.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

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

std::string UniqueSuffix()
{
    static std::atomic<unsigned> counter{0};
    const auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(static_cast<long long>(ticks)) + "_" + std::to_string(counter.fetch_add(1));
}

// A temporary file removed however the test exits.
class ScratchFile {
public:
    explicit ScratchFile(const std::string &suffix) : path("erpl_ds_" + UniqueSuffix() + suffix) {}
    ~ScratchFile() { std::remove(path.c_str()); }

    const std::string &Path() const { return path; }

private:
    std::string path;
};

// Writes `rows` rows of (id INTEGER, name VARCHAR) to a parquet file and returns its path.
void WriteParquet(duckdb::Connection &con, const std::string &path, int first_id, int rows)
{
    auto result = con.Query("COPY (SELECT i AS id, 'row-' || i AS name FROM range(" +
                            std::to_string(first_id) + ", " + std::to_string(first_id + rows) +
                            ") t(i)) TO '" + path + "' (FORMAT PARQUET)");
    INFO((result->HasError() ? result->GetError() : std::string()));
    REQUIRE_FALSE(result->HasError());
}

const char *const SCHEMA_LINE =
    R"({"metaData":{"schemaString":"{\"type\":\"struct\",\"fields\":[)"
    R"({\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},)"
    R"({\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"}})";

// Serves the Delta Sharing endpoints for one table whose data lives in `file_urls`.
void ServeShare(ODataTestServer &server, const std::vector<std::string> &file_urls)
{
    const std::string table_path = "/shares/s/schemas/sc/tables/t";

    server.OnPath(table_path + "/metadata",
                  CannedResponse::Json(std::string(R"({"protocol":{"minReaderVersion":1}})") + "\n" +
                                       SCHEMA_LINE));

    std::string query_body = R"({"protocol":{"minReaderVersion":1}})";
    query_body += "\n";
    query_body += SCHEMA_LINE;
    for (const auto &url : file_urls) {
        query_body += "\n";
        query_body += R"({"file":{"id":"f)" + std::to_string(&url - file_urls.data()) +
                      R"(","size":1,"url":")" + url + R"("}})";
    }
    server.OnPath(table_path + "/query", CannedResponse::Json(query_body));
}

// The Delta Sharing profile the scan is pointed at.
class ProfileFile {
public:
    explicit ProfileFile(const std::string &endpoint) : path("erpl_ds_profile_" + UniqueSuffix() + ".json")
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

int64_t ScalarOf(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result)
{
    return result->GetValue(0, 0).GetValue<int64_t>();
}

std::string ScanSql(const ProfileFile &profile)
{
    return "delta_share_scan('" + profile.Path() + "', 's', 'sc', 't')";
}

}  // namespace

// Defect 2 and 3: one file larger than a single output chunk. Before the fix this returned
// 2048 of 5000 rows, out of freed memory.
TEST_CASE("delta_share_scan returns every row of a multi-chunk file", "[delta_share][scan]") {
    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    ScratchFile parquet(".parquet");
    WriteParquet(con, parquet.Path(), 0, 5000);  // > STANDARD_VECTOR_SIZE

    ODataTestServer server;
    ServeShare(server, {parquet.Path()});
    ProfileFile profile(server.BaseUrl());

    auto result = con.Query("SELECT COUNT(*), SUM(id) FROM " + ScanSql(profile));
    INFO((result->HasError() ? result->GetError() : std::string()));
    REQUIRE_FALSE(result->HasError());
    REQUIRE(ScalarOf(result) == 5000);
    // Sum guards against a cursor that returns the right COUNT from the wrong rows, and
    // reads every value rather than only the chunk headers.
    REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == (4999LL * 5000LL) / 2);
}

TEST_CASE("delta_share_scan spans several files", "[delta_share][scan]") {
    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    ScratchFile first(".parquet");
    ScratchFile second(".parquet");
    WriteParquet(con, first.Path(), 0, 3000);
    WriteParquet(con, second.Path(), 3000, 2500);

    ODataTestServer server;
    ServeShare(server, {first.Path(), second.Path()});
    ProfileFile profile(server.BaseUrl());

    auto result = con.Query("SELECT COUNT(*), COUNT(DISTINCT id) FROM " + ScanSql(profile));
    INFO((result->HasError() ? result->GetError() : std::string()));
    REQUIRE_FALSE(result->HasError());
    REQUIRE(ScalarOf(result) == 5500);
    REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == 5500);
}

// Defect 1: the URL is chosen by the share server. It must be data, never SQL.
TEST_CASE("delta_share_scan does not execute SQL smuggled in a file URL",
          "[delta_share][scan][security]") {
    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    // The payload's FIRST statement has to succeed, or the injected one is never reached
    // and the test proves nothing: it reads a real file, then creates a table, then leaves
    // a well-formed third statement so the concatenated SQL parses.
    ScratchFile good(".parquet");
    WriteParquet(con, good.Path(), 0, 3);

    const std::string malicious = good.Path() + "'); CREATE TABLE pwned AS SELECT 1; " +
                                  "SELECT * FROM parquet_scan('" + good.Path();

    ODataTestServer server;
    ServeShare(server, {malicious});
    ProfileFile profile(server.BaseUrl());

    auto result = con.Query("SELECT COUNT(*) FROM " + ScanSql(profile));

    // The security assertion comes FIRST. REQUIRE aborts the test case, so checking the
    // read error before this would hide the very thing under test: pre-fix the injected
    // statements ran and the query then SUCCEEDED, so an error-first check aborted here
    // and never looked for the table.
    auto pwned = con.Query("SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'pwned'");
    REQUIRE_FALSE(pwned->HasError());
    INFO("a table named 'pwned' exists => the share-supplied URL was executed as SQL");
    REQUIRE(ScalarOf(pwned) == 0);

    // And the read itself fails, because there is no such file.
    INFO((result->HasError() ? result->GetError() : std::string("query unexpectedly succeeded")));
    REQUIRE(result->HasError());
}

// Defect 4: a file that cannot be read must fail the query, not silently end the scan.
TEST_CASE("delta_share_scan fails loudly on an unreadable file", "[delta_share][scan]") {
    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    ScratchFile good(".parquet");
    WriteParquet(con, good.Path(), 0, 10);

    ODataTestServer server;
    // The unreadable file is claimed FIRST, so a swallowed error would end the scan before
    // the good file was ever read - returning 0 rows and reporting success.
    ServeShare(server, {"no_such_file_" + UniqueSuffix() + ".parquet", good.Path()});
    ProfileFile profile(server.BaseUrl());

    auto result = con.Query("SELECT COUNT(*) FROM " + ScanSql(profile));
    INFO("a successful query here means the failure was swallowed");
    REQUIRE(result->HasError());
}
