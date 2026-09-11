// The process-wide HTTP response cache keys on method + URL + body hash. Credentials are
// not part of that key, so two callers holding different secrets for the same URL collided
// inside one process and the second was served the first one's rows. The OData readers no
// longer route through this cache, but every other caller still does.

#include "catch.hpp"
#include "duckdb.hpp"

#include "datazoo/oauth2/http_client.hpp"
#include "odata_test_server.hpp"

#include <string>

using erpl_web::CachingHttpClient;
using erpl_web::HttpClient;
using erpl_web::HttpMethod;
using erpl_web::HttpParams;
using erpl_web::HttpRequest;
using erpl_web::HttpUrl;
using erpl_web::test_support::CannedResponse;
using erpl_web::test_support::ODataTestServer;

namespace {

HttpRequest MakeGet(const std::string &url, const std::string &authorization)
{
    HttpRequest request(HttpMethod::GET, HttpUrl(url));
    if (!authorization.empty()) {
        request.headers["Authorization"] = authorization;
    }
    return request;
}

}  // namespace

TEST_CASE("the cache key distinguishes requests made under different credentials",
          "[http_cache][security]") {
    const std::string url = "https://example.invalid/service/Entity";

    auto alice = MakeGet(url, "Basic YWxpY2U6cHc=");
    auto bob = MakeGet(url, "Basic Ym9iOnB3");

    REQUIRE(alice.ToCacheKey() != bob.ToCacheKey());
}

TEST_CASE("two callers with different credentials each reach the server",
          "[http_cache][security]") {
    ODataTestServer server;
    server.OnPath("/svc/Entity", CannedResponse::Json(R"({"value":[]})"));

    HttpParams params;
    auto caching_client = std::make_shared<CachingHttpClient>(std::make_shared<HttpClient>(params));

    auto alice = MakeGet(server.Url("/svc/Entity"), "Basic YWxpY2U6cHc=");
    auto bob = MakeGet(server.Url("/svc/Entity"), "Basic Ym9iOnB3");

    auto first = caching_client->SendRequest(alice);
    REQUIRE(first != nullptr);
    auto second = caching_client->SendRequest(bob);
    REQUIRE(second != nullptr);

    // Before the fix the second call was answered out of the cache and never left the
    // process, handing Bob whatever Alice's credentials had fetched.
    REQUIRE(server.RequestsFor("/svc/Entity").size() == 2);
}

TEST_CASE("a credentialed response is not retained in the cache at all",
          "[http_cache][security]") {
    ODataTestServer server;
    server.OnPath("/svc/Secret", CannedResponse::Json(R"({"value":[1]})"));

    HttpParams params;
    auto caching_client = std::make_shared<CachingHttpClient>(std::make_shared<HttpClient>(params));

    auto request = MakeGet(server.Url("/svc/Secret"), "Bearer sometoken");
    auto response = caching_client->SendRequest(request);
    REQUIRE(response != nullptr);

    // Not merely keyed apart - a response fetched with credentials is never stored, so it
    // cannot be served to anyone, whatever their key happens to hash to.
    REQUIRE_FALSE(caching_client->IsInCache(request));

    auto repeated = caching_client->SendRequest(request);
    REQUIRE(repeated != nullptr);
    REQUIRE(server.RequestsFor("/svc/Secret").size() == 2);
}

TEST_CASE("an uncredentialed response is still cached", "[http_cache]") {
    // The fix must not quietly turn the cache off for everyone.
    ODataTestServer server;
    server.OnPath("/svc/Public", CannedResponse::Json(R"({"value":[2]})"));

    HttpParams params;
    auto caching_client = std::make_shared<CachingHttpClient>(std::make_shared<HttpClient>(params));

    auto request = MakeGet(server.Url("/svc/Public"), "");
    REQUIRE(caching_client->SendRequest(request) != nullptr);
    REQUIRE(caching_client->SendRequest(request) != nullptr);

    REQUIRE(server.RequestsFor("/svc/Public").size() == 1);
}
