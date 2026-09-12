// GitHub #75, still open on the Datasphere side.
//
// All mutable scan state - the row buffer, the activated projection, and the OData
// client's pagination cursor - used to live in the bind data. odata_read(), the ATTACHed
// odata_table_scan and the SAC readers were moved onto ODataReadGlobalState, so each
// execution of a bound plan gets a private clone. The two Datasphere readers reuse
// ODataReadScan but keep their own init-global functions, and those still return a bare
// GlobalTableFunctionState - which makes ResolveScanState fall back to scanning straight
// out of the bind data.
//
// The consequence is the original #75 symptom: the second EXECUTE of a prepared statement
// finds the buffer already drained and returns nothing, and a self-join of one call has
// both scans sharing a single cursor. Silently, in both cases - there is no error.
//
// These drive the real table functions. Passing a full URL as space_id bypasses the
// tenant/data-centre URL builder (see BuildDataUrl), which is what lets them run against
// the local server instead of a Datasphere tenant.

#include "catch.hpp"
#include "duckdb.hpp"

#include "odata_test_server.hpp"

#include <string>

using erpl_web::test_support::CannedResponse;
using erpl_web::test_support::MakeV4Page;
using erpl_web::test_support::ODataTestServer;
using erpl_web::test_support::RecordedRequest;

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

const char *const AIRLINE_AA = R"({"AirlineCode":"AA","Name":"American Airlines"})";
const char *const AIRLINE_FM = R"({"AirlineCode":"FM","Name":"Shanghai Airline"})";
const char *const AIRLINE_MU = R"({"AirlineCode":"MU","Name":"China Eastern Airlines"})";
const char *const AIRLINE_AF = R"({"AirlineCode":"AF","Name":"Air France"})";

// Two pages of four airlines under `prefix`, addressed as space_id=<base>, asset_id=Airlines.
void ServeTwoPageAirlines(ODataTestServer &server, const std::string &prefix)
{
    const std::string entity_url = server.Url(prefix + "/Airlines");
    const std::string context = server.Url(prefix + "/$metadata") + "#Airlines";

    server.ServeMetadataFixture(prefix + "/$metadata", "edm_trippin.xml");
    server.OnMatch(
        [prefix](const RecordedRequest &request) {
            return request.path == prefix + "/Airlines" && request.QueryParam("$skiptoken") == "2";
        },
        CannedResponse::Json(MakeV4Page(context, {AIRLINE_MU, AIRLINE_AF})));
    server.OnPath(prefix + "/Airlines",
                  CannedResponse::Json(MakeV4Page(context, {AIRLINE_AA, AIRLINE_FM},
                                                  entity_url + "?$format=json&$skiptoken=2")));

    // Without a secret the readers start an interactive authorization_code flow and the
    // test hangs until "Timeout waiting for OAuth2 callback". A client_credentials secret
    // pointed at this endpoint keeps the whole exchange on loopback.
    server.OnPath("/oauth/token",
                  CannedResponse::Json(
                      R"({"access_token":"test-token","token_type":"Bearer","expires_in":3600})"));
}

// Registers a Datasphere secret whose token endpoint is the test server.
void CreateLoopbackSecret(duckdb::Connection &con, const std::string &name,
                          const ODataTestServer &server)
{
    auto created = con.Query(
        "CREATE SECRET " + name + " (TYPE datasphere, PROVIDER oauth2, "
        "TENANT_NAME 'loopback', DATA_CENTER 'eu10', "
        "CLIENT_ID 'test-client', CLIENT_SECRET 'test-secret', "
        "GRANT_TYPE 'client_credentials', SCOPE 'default', "
        "TOKEN_URL '" + server.Url("/oauth/token") + "')");
    INFO((created->HasError() ? created->GetError() : std::string()));
    REQUIRE_FALSE(created->HasError());
}

int64_t ScalarOf(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result)
{
    return result->GetValue(0, 0).GetValue<int64_t>();
}

}  // namespace

TEST_CASE("a bound datasphere_read_relational plan returns every row on each execution",
          "[datasphere_reexec]") {
    ODataTestServer server;
    ServeTwoPageAirlines(server, "/dsrel");
    const std::string read =
        "datasphere_read_relational('" + server.Url("/dsrel") + "', 'Airlines', secret => 'ds')";

    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());
    CreateLoopbackSecret(con, "ds", server);

    REQUIRE_FALSE(con.Query("PREPARE p AS SELECT * FROM " + read)->HasError());

    for (int execution = 1; execution <= 3; execution++) {
        auto result = con.Query("EXECUTE p");
        INFO("execution " << execution);
        INFO((result->HasError() ? result->GetError() : std::string()));
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->RowCount() == 4);
    }
}

TEST_CASE("two datasphere_read_relational scans of one call each see every row",
          "[datasphere_reexec]") {
    ODataTestServer server;
    ServeTwoPageAirlines(server, "/dsreljoin");
    const std::string read =
        "datasphere_read_relational('" + server.Url("/dsreljoin") + "', 'Airlines', secret => 'ds')";

    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());
    CreateLoopbackSecret(con, "ds", server);

    auto result = con.Query("SELECT COUNT(*) FROM " + read + " a, " + read + " b");
    INFO((result->HasError() ? result->GetError() : std::string()));
    REQUIRE_FALSE(result->HasError());
    REQUIRE(ScalarOf(result) == 16);
}

TEST_CASE("a bound datasphere_read_analytical plan returns every row on each execution",
          "[datasphere_reexec]") {
    ODataTestServer server;
    ServeTwoPageAirlines(server, "/dsana");
    const std::string read =
        "datasphere_read_analytical('" + server.Url("/dsana") + "', 'Airlines', secret => 'ds')";

    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());
    CreateLoopbackSecret(con, "ds", server);

    REQUIRE_FALSE(con.Query("PREPARE q AS SELECT * FROM " + read)->HasError());

    for (int execution = 1; execution <= 3; execution++) {
        auto result = con.Query("EXECUTE q");
        INFO("execution " << execution);
        INFO((result->HasError() ? result->GetError() : std::string()));
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->RowCount() == 4);
    }
}

TEST_CASE("two datasphere_read_analytical scans of one call each see every row",
          "[datasphere_reexec]") {
    ODataTestServer server;
    ServeTwoPageAirlines(server, "/dsanajoin");
    const std::string read =
        "datasphere_read_analytical('" + server.Url("/dsanajoin") + "', 'Airlines', secret => 'ds')";

    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());
    CreateLoopbackSecret(con, "ds", server);

    auto result = con.Query("SELECT COUNT(*) FROM " + read + " a, " + read + " b");
    INFO((result->HasError() ? result->GetError() : std::string()));
    REQUIRE_FALSE(result->HasError());
    REQUIRE(ScalarOf(result) == 16);
}
