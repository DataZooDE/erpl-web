// GitHub #182: the Business Central and Dataverse readers have no per-execution scan
// state - the #75 class, in the last table functions still carrying it.
//
// Each keeps a `finished` flag on its BIND DATA, sets it when the scan drains, and never
// resets it; the init-global functions mutate the bind data and return nullptr, so there
// is no per-execution state at all. The second EXECUTE of a bound plan therefore returns
// zero rows, silently, and a self-join has the second scan return nothing.
//
// Dataverse is drivable against a local server because DataverseUrlBuilder::BuildApiUrl
// takes the secret's environment_url verbatim. Business Central hardcoded its host until
// this change; BuildApiUrl now uses `environment` as the API base when it already looks
// like a URL, the same escape hatch DatasphereReadRelational has had for space_id. That
// is what makes the reader testable at all.

#include "catch.hpp"
#include "duckdb.hpp"

#include "odata_test_server.hpp"
#include "business_central_client.hpp"

#include <ctime>
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

// Two pages of four rows under `prefix`, served as the entity set `Airlines`, plus a
// token endpoint so the client_credentials exchange stays on loopback.
void ServeTwoPages(ODataTestServer &server, const std::string &prefix)
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
    server.OnPath("/token",
                  CannedResponse::Json(
                      R"({"access_token":"test-token","token_type":"Bearer","expires_in":3600})"));
}

// MicrosoftEntraTokenManager::HasValidCachedToken requires BOTH access_token AND a
// non-expired expires_at. Without the expiry the supplied token is ignored entirely and
// the manager calls the real login.microsoftonline.com endpoint.
std::string FarFutureEpoch()
{
    return std::to_string(static_cast<long long>(std::time(nullptr)) + 86400);
}

int64_t ScalarOf(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result)
{
    return result->GetValue(0, 0).GetValue<int64_t>();
}

}  // namespace

TEST_CASE("a bound crm_read plan returns every row on each execution", "[ms_reexec][crm]") {
    ODataTestServer server;
    // DataverseUrlBuilder::BuildApiUrl appends "/api/data/v9.2" to environment_url.
    ServeTwoPages(server, "/api/data/v9.2");

    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    auto secret = con.Query(
        "CREATE SECRET crm (TYPE dataverse, PROVIDER config, "
        "TENANT_ID 'loopback', CLIENT_ID 'id', CLIENT_SECRET 'sec', "
        "ENVIRONMENT_URL '" + server.BaseUrl() + "', ACCESS_TOKEN 'test-token', "
        "EXPIRES_AT '" + FarFutureEpoch() + "')");
    INFO((secret->HasError() ? secret->GetError() : std::string()));
    REQUIRE_FALSE(secret->HasError());

    auto prep = con.Query("PREPARE p AS SELECT COUNT(*) FROM crm_read('Airlines', secret => 'crm')");
    INFO("crm PREPARE: " << (prep->HasError() ? prep->GetError() : std::string("ok")));
    REQUIRE_FALSE(prep->HasError());

    for (int execution = 1; execution <= 3; execution++) {
        auto result = con.Query("EXECUTE p");
        INFO("execution " << execution);
        INFO((result->HasError() ? result->GetError() : std::string()));
        REQUIRE_FALSE(result->HasError());
        REQUIRE(ScalarOf(result) == 4);
    }
}

TEST_CASE("a bound bc_read plan returns every row on each execution", "[ms_reexec][bc]") {
    // A GUID short-circuits BusinessCentralClientFactory::ResolveCompanyId, which would
    // otherwise GET <base>/companies to map a name to an id.
    const std::string COMPANY = "11111111-2222-3333-4444-555555555555";

    ODataTestServer server;
    const std::string entity_path = "/companies(" + COMPANY + ")/Airlines";
    const std::string entity_url = server.Url(entity_path);
    // BC serves $metadata at the API base, not under companies({id}) - see the comment in
    // BcReadBind.
    const std::string context = server.Url("/$metadata") + "#Airlines";

    server.ServeMetadataFixture("/$metadata", "edm_trippin.xml");
    server.OnMatch(
        [entity_path](const RecordedRequest &request) {
            return request.path == entity_path && request.QueryParam("$skiptoken") == "2";
        },
        CannedResponse::Json(MakeV4Page(context, {AIRLINE_MU, AIRLINE_AF})));
    server.OnPath(entity_path,
                  CannedResponse::Json(MakeV4Page(context, {AIRLINE_AA, AIRLINE_FM},
                                                  entity_url + "?$format=json&$skiptoken=2")));

    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    auto secret = con.Query(
        "CREATE SECRET bc (TYPE business_central, PROVIDER config, "
        "TENANT_ID 'loopback', CLIENT_ID 'id', CLIENT_SECRET 'sec', "
        "ENVIRONMENT '" + server.BaseUrl() + "', ACCESS_TOKEN 'test-token', "
        "EXPIRES_AT '" + FarFutureEpoch() + "')");
    INFO((secret->HasError() ? secret->GetError() : std::string()));
    REQUIRE_FALSE(secret->HasError());

    auto prep = con.Query("PREPARE q AS SELECT COUNT(*) FROM bc_read('Airlines', secret => 'bc', "
                          "company => '" + COMPANY + "')");
    INFO("bc PREPARE: " << (prep->HasError() ? prep->GetError() : std::string("ok")));
    REQUIRE_FALSE(prep->HasError());

    for (int execution = 1; execution <= 3; execution++) {
        auto result = con.Query("EXECUTE q");
        INFO("execution " << execution);
        INFO((result->HasError() ? result->GetError() : std::string()));
        REQUIRE_FALSE(result->HasError());
        REQUIRE(ScalarOf(result) == 4);
    }
}

// The verbatim-URL escape hatch added above is production code shared by bc_read,
// bc_describe and the BC catalog - not test-only. Before it existed the scheme was
// hardcoded https, so accepting plain http generally would be a security downgrade
// introduced by a testability change: the OAuth bearer token would go on the wire in
// cleartext, and the same-origin guard would not object because the configured URL IS the
// origin. Raised by an agent-crew review (F6).
TEST_CASE("the Business Central URL hatch refuses plain http off loopback",
          "[ms_reexec][bc][security]") {
    SECTION("loopback forms are accepted") {
        REQUIRE(erpl_web::BusinessCentralUrlBuilder::BuildApiUrl("t", "http://127.0.0.1:8080") ==
                "http://127.0.0.1:8080");
        REQUIRE(erpl_web::BusinessCentralUrlBuilder::BuildApiUrl("t", "http://localhost:8080/") ==
                "http://localhost:8080");
        REQUIRE(erpl_web::BusinessCentralUrlBuilder::BuildApiUrl("t", "http://[::1]:8080") ==
                "http://[::1]:8080");
    }

    SECTION("https is accepted anywhere") {
        REQUIRE(erpl_web::BusinessCentralUrlBuilder::BuildApiUrl("t", "https://api.example.com/v2") ==
                "https://api.example.com/v2");
    }

    SECTION("plain http to any other host is refused") {
        REQUIRE_THROWS_AS(erpl_web::BusinessCentralUrlBuilder::BuildApiUrl("t", "http://evil.example"),
                          duckdb::InvalidInputException);
        // A host that merely starts with a loopback-looking label must not slip through.
        REQUIRE_THROWS_AS(
            erpl_web::BusinessCentralUrlBuilder::BuildApiUrl("t", "http://localhost.evil.example"),
            duckdb::InvalidInputException);
    }

    SECTION("a non-URL environment still builds the real BC endpoint") {
        REQUIRE(erpl_web::BusinessCentralUrlBuilder::BuildApiUrl("tenant", "production") ==
                "https://api.businesscentral.dynamics.com/v2.0/tenant/production/api/v2.0");
    }
}
