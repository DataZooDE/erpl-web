// GitHub #183: a cross-origin @odata.nextLink receives the caller's Authorization header.
//
// ODataEntitySetClient::Get() assigns the server-supplied next link to the member `url`
// before calling DoHttpGet(request_url). DoHttpGet's guard reads
//
//     if (modified_url.IsSameOrigin(url)) { http_request.AuthHeadersFromParams(...); }
//
// where `url` is DoHttpGet's own PARAMETER, not the service the client was opened against.
// modified_url is that same parameter plus input parameters, which are query string only,
// so the comparison is a URL against itself and always passes. The guard cannot fire on
// the path it was written for.
//
// Server-driven paging follows whatever URL the service puts in @odata.nextLink / __next,
// so a hostile or compromised service returns an absolute link to a host it controls and
// harvests the caller's bearer token.

#include "catch.hpp"
#include "duckdb.hpp"

#include "odata_test_server.hpp"

#include <string>

using erpl_web::test_support::CannedResponse;
using erpl_web::test_support::MakeV4Page;
using erpl_web::test_support::ODataTestServer;

namespace {

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

}  // namespace

TEST_CASE("a cross-origin next link never receives the caller's credentials",
          "[odata_origin][security]") {
    // Declared before the database so both outlive every connection that talks to them.
    ODataTestServer trusted;
    ODataTestServer foreign;

    const std::string context = trusted.Url("/svc/$metadata") + "#Airlines";
    trusted.ServeMetadataFixture("/svc/$metadata", "edm_trippin.xml");

    // Page one is served by the service the user named, and points at a DIFFERENT host.
    trusted.OnPath("/svc/Airlines",
                   CannedResponse::Json(MakeV4Page(context, {AIRLINE_AA},
                                                   foreign.Url("/steal/Airlines"))));
    foreign.OnPath("/steal/Airlines", CannedResponse::Json(MakeV4Page(context, {AIRLINE_FM})));

    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    // A secret scoped to the trusted service, so the reader has a credential to leak.
    auto secret = con.Query("CREATE SECRET leaky (TYPE http_basic, USERNAME 'victim', "
                            "PASSWORD 'hunter2', SCOPE '" + trusted.BaseUrl() + "')");
    INFO((secret->HasError() ? secret->GetError() : std::string()));
    REQUIRE_FALSE(secret->HasError());

    auto result = con.Query("SELECT COUNT(*) FROM odata_read('" + trusted.Url("/svc/Airlines") +
                            "')");
    INFO((result->HasError() ? result->GetError() : std::string()));
    // Assert the read actually succeeded and spanned both pages. Without this the whole
    // test can pass on a build where binding or page-one parsing broke: the next link is
    // never followed, the foreign-host loop below iterates zero times, and the #183 guard
    // goes untested while the test stays green.
    REQUIRE_FALSE(result->HasError());
    REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 2);

    // The trusted host must have been given the credential - otherwise this test would
    // pass for the wrong reason, having never authenticated at all.
    bool trusted_was_authenticated = false;
    for (const auto &request : trusted.RequestsFor("/svc/Airlines")) {
        if (!request.Header("Authorization").empty()) {
            trusted_was_authenticated = true;
        }
    }
    INFO("the trusted service was never sent credentials, so this proves nothing");
    REQUIRE(trusted_was_authenticated);

    // The foreign host MUST have been contacted - following a next link is the documented
    // behaviour, and an empty request set would make the loop below assert nothing.
    INFO("the cross-origin next link was never followed, so the guard is untested");
    REQUIRE_FALSE(foreign.RequestsFor("/steal/Airlines").empty());

    // ...but it must never see the caller's credentials.
    for (const auto &request : foreign.Requests()) {
        INFO("foreign host " << request.method << " " << request.target
                             << " Authorization=[" << request.Header("Authorization") << "]");
        REQUIRE(request.Header("Authorization").empty());
    }
}

// GitHub #189. The credential guard compares against service_origin_url, which is const and
// set at construction. But a client is RE-MINTED mid-query at two sites - CloneForScan and
// the predicate-pushdown rebuild - and both used to construct the new client from
// odata_client->Url(). That member is the pagination cursor: once a next link has been
// followed it holds whatever the last response named. So a re-mint after paging would have
// adopted the foreign host as the trusted origin, restoring #183 with no visible edit to
// the guard.
//
// This drives a PROJECTING query, which is what forces the pushdown rebuild - SELECT *
// takes the early return and never re-mints. Raised by an agent-crew review.
TEST_CASE("the trusted origin survives a client rebuilt after paging",
          "[odata_origin][security]") {
    ODataTestServer trusted;
    ODataTestServer foreign;

    const std::string context = trusted.Url("/svc2/$metadata") + "#Airlines";
    trusted.ServeMetadataFixture("/svc2/$metadata", "edm_trippin.xml");
    trusted.OnPath("/svc2/Airlines",
                   CannedResponse::Json(MakeV4Page(context, {AIRLINE_AA},
                                                   foreign.Url("/steal2/Airlines"))));
    foreign.OnPath("/steal2/Airlines", CannedResponse::Json(MakeV4Page(context, {AIRLINE_FM})));

    TestDatabase database;
    duckdb::Connection &con = database.Con();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    auto secret = con.Query("CREATE SECRET leaky2 (TYPE http_basic, USERNAME 'victim', "
                            "PASSWORD 'hunter2', SCOPE '" + trusted.BaseUrl() + "')");
    REQUIRE_FALSE(secret->HasError());

    // Projecting: emits $select, changes the URL, and so rebuilds the client.
    auto result = con.Query("SELECT AirlineCode FROM odata_read('" +
                            trusted.Url("/svc2/Airlines") + "') ORDER BY AirlineCode");
    INFO((result->HasError() ? result->GetError() : std::string()));
    REQUIRE_FALSE(result->HasError());
    REQUIRE(result->RowCount() == 2);

    bool trusted_was_authenticated = false;
    for (const auto &request : trusted.RequestsFor("/svc2/Airlines")) {
        if (!request.Header("Authorization").empty()) {
            trusted_was_authenticated = true;
        }
    }
    INFO("the trusted service was never sent credentials, so this proves nothing");
    REQUIRE(trusted_was_authenticated);

    INFO("the cross-origin next link was never followed, so the guard is untested");
    REQUIRE_FALSE(foreign.RequestsFor("/steal2/Airlines").empty());

    for (const auto &request : foreign.Requests()) {
        INFO("foreign host " << request.method << " " << request.target
                             << " Authorization=[" << request.Header("Authorization") << "]");
        REQUIRE(request.Header("Authorization").empty());
    }
}
