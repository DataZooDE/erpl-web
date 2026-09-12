// Three places decide what counts as a credential header: the trace redactor, the response
// cache key, and the cross-origin redirect strip. They had drifted apart - the redirect
// strip listed five names where the redactor listed seven, so X-Access-Token survived a
// redirect to another origin and was sent to whatever host the Location named.

#include "catch.hpp"
#include "duckdb.hpp"

#include "datazoo/oauth2/http_client.hpp"
#include "odata_test_server.hpp"

#include <string>

using erpl_web::HttpClient;
using erpl_web::HttpMethod;
using erpl_web::HttpParams;
using erpl_web::HttpRequest;
using erpl_web::HttpUrl;
using erpl_web::test_support::CannedResponse;
using erpl_web::test_support::ODataTestServer;

TEST_CASE("a cross-origin redirect strips every credential header", "[http_redirect][security]") {
    ODataTestServer origin;       // where the caller sent the request
    ODataTestServer destination;  // where the response redirects it

    destination.OnPath("/landed", CannedResponse::Json(R"({"value":[]})"));
    origin.OnPath("/start", CannedResponse::Error(302).WithHeader("Location", destination.Url("/landed")));

    HttpParams params;
    HttpClient client(params);

    HttpRequest request(HttpMethod::GET, HttpUrl(origin.Url("/start")));
    request.headers["Authorization"] = "Bearer token-value";
    request.headers["Cookie"] = "session=abc";
    request.headers["X-API-Key"] = "api-key-value";
    request.headers["X-Auth-Token"] = "auth-token-value";
    request.headers["X-Access-Token"] = "access-token-value";
    request.headers["X-CSRF-Token"] = "csrf-token-value";

    auto response = client.SendRequest(request);
    REQUIRE(response != nullptr);

    auto landed = destination.RequestsFor("/landed");
    REQUIRE(landed.size() == 1);

    for (const char *header : {"Authorization", "Cookie", "X-API-Key", "X-Auth-Token",
                              "X-Access-Token", "X-CSRF-Token"}) {
        INFO("header that reached the redirect destination: " << header);
        REQUIRE_FALSE(landed[0].HasHeader(header));
    }
}
