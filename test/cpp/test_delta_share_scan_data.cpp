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
// WHAT THESE TESTS COVER, AND WHAT THEY CANNOT
//
// The scan now refuses any file URL that is not https (or http on loopback), because the
// URL is chosen by the share server and parquet_scan resolves whatever it is handed -
// file://, a bare path, a glob - which would let a hostile share read local files and
// return them through the query. Real Delta Sharing issues pre-signed https URLs, so the
// policy costs nothing in production.
//
// It does cost test coverage. Reading parquet over http needs httpfs, which is not built
// in this configuration, and a local path is now rejected - so the data-path cases that
// originally proved the row-truncation and use-after-free fixes cannot run here at all.
// They were written, run red against the pre-fix code (a 5000-row file returned 2048 rows;
// an unreadable file reported success with zero rows) and then removed with the policy
// decision, deliberately, rather than kept alive by relaxing the policy for tests.
//
// What remains is everything reachable without reading a remote parquet file: the URL
// policy itself, which is what now stops the injection, and the schema handling in bind.
// Restoring the data-path tests needs httpfs in the test build - see the follow-up issue.

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

// The URL is chosen by the share server. Before the policy it was concatenated into SQL;
// now it is both a bound value and required to be an https URL, so a payload like this is
// refused before anything is opened.
TEST_CASE("delta_share_scan refuses a file URL that is not https", "[delta_share][scan][security]") {
    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    const std::string malicious =
        "/tmp/x.parquet'); CREATE TABLE pwned AS SELECT 1; SELECT * FROM parquet_scan('/tmp/x.parquet";

    ODataTestServer server;
    ServeShare(server, {malicious});
    ProfileFile profile(server.BaseUrl());

    auto result = con.Query("SELECT COUNT(*) FROM " + ScanSql(profile));

    // The security assertion comes FIRST: REQUIRE aborts the case, and checking the error
    // before this would hide the very thing under test.
    auto pwned = con.Query("SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'pwned'");
    REQUIRE_FALSE(pwned->HasError());
    INFO("a table named 'pwned' exists => the share-supplied URL was executed as SQL");
    REQUIRE(ScalarOf(pwned) == 0);

    INFO((result->HasError() ? result->GetError() : std::string("query unexpectedly succeeded")));
    REQUIRE(result->HasError());
}

TEST_CASE("delta_share_scan refuses file:// and bare paths from the share server",
          "[delta_share][scan][security]") {
    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    // Each would otherwise be resolved by parquet_scan against the local filesystem.
    const std::vector<std::string> rejected = {
        "file:///etc/passwd",
        "/etc/passwd",
        "../../etc/passwd",
        "*.parquet",
        "s3://bucket/key.parquet",
        "http://example.com/data.parquet",  // plain http off loopback
    };

    for (const auto &url : rejected) {
        INFO("share-supplied URL: " << url);
        ODataTestServer server;
        ServeShare(server, {url});
        ProfileFile profile(server.BaseUrl());

        auto result = con.Query("SELECT COUNT(*) FROM " + ScanSql(profile));
        REQUIRE(result->HasError());

        // Assert WHY it failed. An earlier version of this test checked only that the
        // query errored, which it did for the local paths - but because parquet_scan could
        // not read them, not because the guard refused them. That passed while
        // "/etc/passwd" and "*.parquet" were still reaching the filesystem, and would have
        // gone on passing if the share had named a real parquet file on disk.
        const auto error = result->GetError();
        INFO("error was: " << error);
        REQUIRE(error.find("Delta Sharing data file URL") != std::string::npos);
    }
}

// An https URL passes the policy; the read then fails for want of httpfs in this build,
// which is what distinguishes "refused by policy" from "accepted and attempted".
TEST_CASE("delta_share_scan accepts an https file URL", "[delta_share][scan]") {
    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    ODataTestServer server;
    ServeShare(server, {"https://example.invalid/data.parquet"});
    ProfileFile profile(server.BaseUrl());

    auto result = con.Query("SELECT COUNT(*) FROM " + ScanSql(profile));
    REQUIRE(result->HasError());
    const auto error = result->GetError();
    INFO("error was: " << error);
    // Rejected by the URL policy would name the URL requirement; this must get past it.
    REQUIRE(error.find("must be") == std::string::npos);
}

// A column type the reader does not map must fail in bind, not be cast to VARCHAR behind
// the caller's back once the file is aligned to the bound schema.
TEST_CASE("delta_share_scan refuses a column type it cannot map", "[delta_share][scan]") {
    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    ODataTestServer server;
    const std::string table_path = "/shares/s/schemas/sc/tables/t";
    const char *const decimal_schema =
        R"({"metaData":{"schemaString":"{\"type\":\"struct\",\"fields\":[)"
        R"({\"name\":\"amount\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}}]}"}})";
    server.OnPath(table_path + "/metadata",
                  CannedResponse::Json(std::string(R"({"protocol":{"minReaderVersion":1}})") + "\n" +
                                       decimal_schema));
    server.OnPath(table_path + "/query",
                  CannedResponse::Json(std::string(R"({"protocol":{"minReaderVersion":1}})")));

    ProfileFile profile(server.BaseUrl());
    auto result = con.Query("SELECT COUNT(*) FROM " + ScanSql(profile));
    REQUIRE(result->HasError());
    INFO(result->GetError());
    REQUIRE(result->GetError().find("decimal(10,2)") != std::string::npos);
}

// The share server's response is the first untrusted thing this code touches.
// yyjson_get_str returns NULL for any value that is not a string, and assigning NULL to a
// std::string is undefined behaviour - a crash inside strlen - so a server answering with
// {"url":null} took the extension down before any URL policing could run.
TEST_CASE("a file entry with a non-string url is refused, not dereferenced",
          "[delta_share][scan][security]") {
    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    const std::vector<std::string> malformed = {
        R"({"file":{"id":"f0","size":1,"url":null}})",
        R"({"file":{"id":"f0","size":1,"url":123}})",
        R"({"file":{"id":"f0","size":1,"url":{"nested":"object"}}})",
        R"({"file":{"id":"f0","size":1}})",  // absent entirely
    };

    for (const auto &entry : malformed) {
        INFO("file entry: " << entry);
        ODataTestServer server;
        const std::string table_path = "/shares/s/schemas/sc/tables/t";
        server.OnPath(table_path + "/metadata",
                      CannedResponse::Json(std::string(R"({"protocol":{"minReaderVersion":1}})") +
                                           "\n" + SCHEMA_LINE));
        server.OnPath(table_path + "/query",
                      CannedResponse::Json(std::string(R"({"protocol":{"minReaderVersion":1}})") +
                                           "\n" + SCHEMA_LINE + "\n" + entry));
        ProfileFile profile(server.BaseUrl());

        // The point is that this returns at all rather than crashing the process.
        auto result = con.Query("SELECT COUNT(*) FROM " + ScanSql(profile));
        REQUIRE(result->HasError());
        INFO("error was: " << result->GetError());
        REQUIRE(result->GetError().find("'url'") != std::string::npos);
    }
}

// The bearer token is attached to every request built from the profile endpoint, and the
// profile is not necessarily user-authored - it may be loaded from a remote path.
TEST_CASE("a Delta Sharing profile endpoint must not be plain http off loopback",
          "[delta_share][scan][security]") {
    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    ProfileFile insecure("http://share.example.com/delta-sharing");
    auto result = con.Query("SELECT COUNT(*) FROM delta_share_show_shares('" + insecure.Path() + "')");
    REQUIRE(result->HasError());
    INFO("error was: " << result->GetError());
    REQUIRE(result->GetError().find("endpoint") != std::string::npos);
}

// https is fine, and so is loopback http - which is what every other test here relies on.
TEST_CASE("a Delta Sharing profile endpoint may be https or loopback http",
          "[delta_share][scan][security]") {
    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    ODataTestServer server;  // binds 127.0.0.1
    server.OnPath("/shares", CannedResponse::Json(R"({"shares":[{"name":"alpha","id":"1"}]})"));
    ProfileFile loopback(server.BaseUrl());

    auto result = con.Query("SELECT COUNT(*) FROM delta_share_show_shares('" + loopback.Path() + "')");
    INFO((result->HasError() ? result->GetError() : std::string()));
    REQUIRE_FALSE(result->HasError());
    REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
}
