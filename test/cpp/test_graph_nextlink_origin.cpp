// GitHub #205 / #202 fourth review, F1+F3: a Microsoft Graph @odata.nextLink arrives in a
// response body, so it is attacker-controlled the moment the service is impersonated or
// proxied. The bearer token may only follow it to the origin the scan was opened against.
//
// This is the same guarantee test_odata_nextlink_origin.cpp makes for the OData reader
// (#183) and OdpRequestOrchestrator makes for ODP (#101, #187). It was missing for Graph:
// a first cut of the fix gated only the two lazy-paging readers, leaving GetAllPagesMerged
// - the eager path behind most Graph readers - following next links through the unguarded
// Get(). Nothing failed, because nothing asserted the credential decision. This file does.
//
// GraphClient is exercised directly rather than through SQL: GetAllPagesMerged takes a
// full URL, so it can be pointed at a loopback server without a base-URL escape hatch.

#include "catch.hpp"
#include "duckdb.hpp"

#include "datasphere_client.hpp"
#include "graph_client.hpp"
#include "odata_test_server.hpp"

#include <memory>
#include <string>

using erpl_web::test_support::CannedResponse;
using erpl_web::test_support::ODataTestServer;

namespace {

std::shared_ptr<erpl_web::HttpAuthParams> BearerToken(const std::string &token)
{
    auto params = std::make_shared<erpl_web::HttpAuthParams>();
    params->bearer_token = token;
    return params;
}

// The value a page serves when it wants the reader to follow it to `next`.
std::string PageWithNext(const std::string &id, const std::string &next)
{
    std::string page = R"({"value":[{"id":")" + id + R"("}])";
    if (!next.empty()) {
        page += R"(,"@odata.nextLink":")" + next + R"(")";
    }
    return page + "}";
}

}  // namespace

TEST_CASE("a cross-origin Graph next link never receives the bearer token",
          "[graph_origin][security]") {
    ODataTestServer trusted;
    ODataTestServer foreign;

    // Page one comes from the host the reader was pointed at and advertises a next page on
    // a DIFFERENT host - the shape a compromised or impersonating service produces.
    trusted.OnPath("/v1.0/users",
                   CannedResponse::Json(PageWithNext("1", foreign.Url("/steal/users"))));
    foreign.OnPath("/steal/users", CannedResponse::Json(PageWithNext("2", "")));

    erpl_web::GraphClient client(BearerToken("tenant-access-token"), "GRAPH_TEST");
    const auto merged = client.GetAllPagesMerged(trusted.Url("/v1.0/users"));

    // The read must actually have spanned both pages. Without this the assertions below
    // would also pass if paging had simply stopped at page one.
    REQUIRE(merged.find("\"1\"") != std::string::npos);
    REQUIRE(merged.find("\"2\"") != std::string::npos);

    const auto trusted_requests = trusted.RequestsFor("/v1.0/users");
    REQUIRE(trusted_requests.size() == 1);
    REQUIRE(trusted_requests[0].Header("authorization") == "Bearer tenant-access-token");

    const auto foreign_requests = foreign.RequestsFor("/steal/users");
    REQUIRE(foreign_requests.size() == 1);
    INFO("foreign host received: " << foreign_requests[0].Header("authorization"));
    REQUIRE_FALSE(foreign_requests[0].HasHeader("authorization"));
}

TEST_CASE("a same-origin Graph next link still receives the bearer token",
          "[graph_origin][security]") {
    // The negative case alone would pass if the gate simply stripped credentials from every
    // next link, which would break every real multi-page read.
    ODataTestServer trusted;

    trusted.OnPath("/v1.0/users", CannedResponse::Json(PageWithNext("1", trusted.Url("/v1.0/users2"))));
    trusted.OnPath("/v1.0/users2", CannedResponse::Json(PageWithNext("2", "")));

    erpl_web::GraphClient client(BearerToken("tenant-access-token"), "GRAPH_TEST");
    const auto merged = client.GetAllPagesMerged(trusted.Url("/v1.0/users"));
    REQUIRE(merged.find("\"2\"") != std::string::npos);

    const auto second_page = trusted.RequestsFor("/v1.0/users2");
    REQUIRE(second_page.size() == 1);
    REQUIRE(second_page[0].Header("authorization") == "Bearer tenant-access-token");
}

// A next link that names the trusted origin with a different PORT is a different origin:
// IsSameOrigin compares scheme, host and port.
TEST_CASE("a Graph next link on another port is treated as cross-origin",
          "[graph_origin][security]") {
    ODataTestServer trusted;
    ODataTestServer other_port;

    trusted.OnPath("/v1.0/users",
                   CannedResponse::Json(PageWithNext("1", other_port.Url("/v1.0/users2"))));
    other_port.OnPath("/v1.0/users2", CannedResponse::Json(PageWithNext("2", "")));

    erpl_web::GraphClient client(BearerToken("tenant-access-token"), "GRAPH_TEST");
    (void)client.GetAllPagesMerged(trusted.Url("/v1.0/users"));

    const auto requests = other_port.RequestsFor("/v1.0/users2");
    REQUIRE(requests.size() == 1);
    REQUIRE_FALSE(requests[0].HasHeader("authorization"));
}

// GitHub #205: the Datasphere describe path takes relational_metadata_url and
// analytical_metadata_url out of the CATALOG RESPONSE BODY and sent the Datasphere OAuth
// bearer to them with no origin check - the same class as the Graph next link above.
//
// The gate is exercised through the predicate it is built on, because reaching
// FetchAssetExtendedMetadata needs a live Datasphere tenant. What the fix turns on is that
// the origin comes from the CONFIGURED tenant rather than from the returned URL: the
// tenant and data centre are also parsed out of those same URLs further down, so an origin
// derived from the URL under test would make the comparison vacuous.
TEST_CASE("a Datasphere metadata URL is compared against the configured tenant origin",
          "[graph_origin][security][datasphere]") {
    const std::string configured =
        erpl_web::DatasphereUrlBuilder::BuildCatalogUrl("acme", "eu10");

    // Same tenant: credentials may follow.
    REQUIRE(erpl_web::HttpUrl("https://acme.eu10.hcs.cloud.sap/api/v1/dwc/consumption/relational/SPACE/ASSET")
                .IsSameOrigin(erpl_web::HttpUrl(configured)));

    // A different tenant, a different data centre, a plain-http downgrade and an unrelated
    // host are all different origins, so none of them receives the bearer token.
    REQUIRE_FALSE(erpl_web::HttpUrl("https://evil.eu10.hcs.cloud.sap/api/v1/dwc/catalog")
                      .IsSameOrigin(erpl_web::HttpUrl(configured)));
    REQUIRE_FALSE(erpl_web::HttpUrl("https://acme.us10.hcs.cloud.sap/api/v1/dwc/catalog")
                      .IsSameOrigin(erpl_web::HttpUrl(configured)));
    REQUIRE_FALSE(erpl_web::HttpUrl("http://acme.eu10.hcs.cloud.sap/api/v1/dwc/catalog")
                      .IsSameOrigin(erpl_web::HttpUrl(configured)));
    REQUIRE_FALSE(erpl_web::HttpUrl("https://attacker.example/collect")
                      .IsSameOrigin(erpl_web::HttpUrl(configured)));
}
