#include <thread>
#include <chrono>

#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"

#include "charset_converter.hpp"
#include "erpl_http_client.hpp"
#include "duckdb_argument_helper.hpp"

using namespace erpl_web;
using namespace std;

TEST_CASE("HttpUrl Parsing and Serialization", "[http_url]") {
    SECTION("Parsing valid URLs") {
        HttpUrl url("https://www.example.com:8080/path?query=value#fragment");
        REQUIRE(url.Scheme() == "https");
        REQUIRE(url.Host() == "www.example.com");
        REQUIRE(url.Port() == "8080");
        REQUIRE(url.Path() == "/path");
        REQUIRE(url.Query() == "?query=value");
        REQUIRE(url.Fragment() == "#fragment");
    }

    SECTION("Serializing URL components back to string") {
        HttpUrl url("http://www.example.com/path");
        REQUIRE(url.ToSchemeHostAndPort() == "http://www.example.com");
        REQUIRE(url.ToPathQueryFragment() == "/path");
        REQUIRE(url.ToString() == "http://www.example.com/path");
    }

    SECTION("Comparison of URLs") {
        HttpUrl url1("http://example.com");
        HttpUrl url2("http://example.com/");
        REQUIRE_FALSE(url1.Equals(url2));
    }

    SECTION("Setting and getting URL components") {
        HttpUrl url("http://example.com");
        url.Scheme("https");
        url.Host("www.example.com");
        url.Port("443");
        url.Path("/newpath");
        url.Query("?newquery");
        url.Fragment("#newfragment");
        REQUIRE(url.Scheme() == "https");
        REQUIRE(url.Host() == "www.example.com");
        REQUIRE(url.Port() == "443");
        REQUIRE(url.Path() == "/newpath");
        REQUIRE(url.Query() == "?newquery");
        REQUIRE(url.Fragment() == "#newfragment");
        REQUIRE(url.ToSchemeHostAndPort() == "https://www.example.com:443");
        REQUIRE(url.ToPathQueryFragment() == "/newpath?newquery#newfragment");
        REQUIRE(url.ToString() == "https://www.example.com:443/newpath?newquery#newfragment");
    }

    SECTION("Merging paths") {
        std::filesystem::path base_path("/v4/northwind/Customers");
        std::filesystem::path rel_path_1("Customers");
        std::filesystem::path rel_path_2("northwind/Customers");
        std::filesystem::path rel_path_3("Products");
        std::filesystem::path rel_path_4("../Products");
        std::filesystem::path rel_path_5("../../../Products");
        std::filesystem::path rel_path_6("/Products");

        REQUIRE(HttpUrl::MergePaths(base_path, rel_path_1) == "/v4/northwind/Customers");
        REQUIRE(HttpUrl::MergePaths(base_path, rel_path_2) == "/v4/northwind/Customers");
        REQUIRE(HttpUrl::MergePaths(base_path, rel_path_3) == "/v4/northwind/Customers/Products");
        REQUIRE(HttpUrl::MergePaths(base_path, rel_path_4) == "/v4/northwind/Products");
        REQUIRE(HttpUrl::MergePaths(base_path, rel_path_5) == "/Products");
        REQUIRE(HttpUrl::MergePaths(base_path, rel_path_6) == "/Products");
    }

    SECTION("Merging relative URLs with base URLs") {
        HttpUrl base_url("https://services.odata.org/v4/northwind/northwind.svc/");
        std::string relative_url = "Customers?$skiptoken='ERNSH'";
        HttpUrl merged_url = HttpUrl::MergeWithBaseUrlIfRelative(base_url, relative_url);
        REQUIRE(merged_url.ToString() == "https://services.odata.org/v4/northwind/northwind.svc/Customers?$skiptoken='ERNSH'");

        base_url = HttpUrl("https://services.odata.org/v4/northwind/northwind.svc/Customers");
        relative_url = "Customers?$skiptoken='ERNSH'";
        merged_url = HttpUrl::MergeWithBaseUrlIfRelative(base_url, relative_url);
        REQUIRE(merged_url.ToString() == "https://services.odata.org/v4/northwind/northwind.svc/Customers?$skiptoken='ERNSH'");

        base_url = HttpUrl("https://services.odata.org/v4/northwind/northwind.svc/Customers");
        relative_url = "../../../foo/northwind/northwind.svc/Customers?$skiptoken='ERNSH'";
        merged_url = HttpUrl::MergeWithBaseUrlIfRelative(base_url, relative_url);
        REQUIRE(merged_url.ToString() == "https://services.odata.org/v4/foo/northwind/northwind.svc/Customers?$skiptoken='ERNSH'");

        base_url = HttpUrl("https://services.odata.org/v4/northwind/northwind.svc/Customers");
        relative_url = "/Customers?$skiptoken='ERNSH'";
        merged_url = HttpUrl::MergeWithBaseUrlIfRelative(base_url, relative_url);
        REQUIRE(merged_url.ToString() == "https://services.odata.org/Customers?$skiptoken='ERNSH'");

        base_url = HttpUrl("https://services.odata.org/v4/northwind/northwind.svc/Customers");
        relative_url = "https://services.odata.org/v4/northwind/northwind.svc/Products";
        merged_url = HttpUrl::MergeWithBaseUrlIfRelative(base_url, relative_url);
        REQUIRE(merged_url.ToString() == "https://services.odata.org/v4/northwind/northwind.svc/Products");
    }
}

TEST_CASE("HttpMethod Tests", "[http_method]") {
    SECTION("Convert string to HttpMethod") {
        REQUIRE(HttpMethod::FromString("GET") == HttpMethod::GET);
        REQUIRE(HttpMethod::FromString("POST") == HttpMethod::POST);
        REQUIRE(HttpMethod::FromString("PUT") == HttpMethod::PUT);
        REQUIRE(HttpMethod::FromString("DELETE") == HttpMethod::_DELETE);
        REQUIRE(HttpMethod::FromString("PATCH") == HttpMethod::PATCH);
        REQUIRE(HttpMethod::FromString("HEAD") == HttpMethod::HEAD);
        REQUIRE(HttpMethod::FromString("OPTIONS") == HttpMethod::OPTIONS);
        REQUIRE(HttpMethod::FromString("TRACE") == HttpMethod::TRACE);
        REQUIRE(HttpMethod::FromString("CONNECT") == HttpMethod::CONNECT);
    }

    SECTION("Convert HttpMethod to string") {
        REQUIRE(HttpMethod(HttpMethod::GET).ToString() == "GET");
        REQUIRE(HttpMethod(HttpMethod::POST).ToString() == "POST");
        REQUIRE(HttpMethod(HttpMethod::PUT).ToString() == "PUT");
        REQUIRE(HttpMethod(HttpMethod::_DELETE).ToString() == "DELETE");
        REQUIRE(HttpMethod(HttpMethod::PATCH).ToString() == "PATCH");
        REQUIRE(HttpMethod(HttpMethod::HEAD).ToString() == "HEAD");
        REQUIRE(HttpMethod(HttpMethod::OPTIONS).ToString() == "OPTIONS");
        REQUIRE(HttpMethod(HttpMethod::TRACE).ToString() == "TRACE");
        REQUIRE(HttpMethod(HttpMethod::CONNECT).ToString() == "CONNECT");
    }

    SECTION("Convert invalid string to HttpMethod") {
        REQUIRE_THROWS_AS(HttpMethod::FromString("INVALID"), std::runtime_error);
    }
}

TEST_CASE("Test http HEAD", "[http_client]")
{
    HttpClient client;

    auto response = client.Head("https://google.com");
    REQUIRE(response->code == 200);
}

TEST_CASE("Test http GET", "[http_client]")
{
    HttpClient client;

    auto response = client.Get("http://httpbun.com/get");
    REQUIRE(response->code == 200);
    REQUIRE(response->content_type == "application/json");

    response = client.Get("https://httpbun.com/get");
    REQUIRE(response->code == 200);
    REQUIRE(response->content_type == "application/json");

    auto val = response->ToValue();
    auto content = ValueHelper(val)["content"];
    auto content_json = content.DefaultCastAs(duckdb::LogicalType::JSON());
}

TEST_CASE("Test http GET on google.com", "[http_client]")
{
	HttpClient client;

	auto response = client.Get("https://google.com");
	REQUIRE(response->code == 200);
	REQUIRE(response->content_type == "text/html; charset=ISO-8859-1");

	auto conv = CharsetConverter(response->content_type);
	auto val = conv.convert(response->content);

	// Check if val starts with <!doctype html>
	REQUIRE(val.find("<!doctype html>") == 0);
}

TEST_CASE("Test http GET on erpl.io", "[http_client]")
{
	HttpClient client;

	auto response = client.Get("https://erpl.io");
	REQUIRE(response->code == 200);
	REQUIRE(response->content_type == "text/html; charset=utf-8");
}

TEST_CASE("Test CachingHttpClient", "[http_client]") {
    auto http_client = std::make_shared<HttpClient>();
    CachingHttpClient caching_client(http_client, std::chrono::seconds(2));

    SECTION("Test caching behavior") {
        // First request should hit the network
        auto response1 = caching_client.Get("https://httpbun.com/get");
        REQUIRE(response1->code == 200);

        // Second request should come from cache
        auto response2 = caching_client.Get("https://httpbun.com/get");
        REQUIRE(response2->code == 200);
        REQUIRE(response2->content == response1->content);

        // Wait for cache to expire
        std::this_thread::sleep_for(std::chrono::seconds(3));

        // Third request should hit the network again
        auto response3 = caching_client.Get("https://httpbun.com/get");
        REQUIRE(response3->code == 200);
        // Content might be different since it's a new request
    }

    SECTION("Test different URLs are cached separately") {
        auto response1 = caching_client.Get("https://httpbun.com/get");
        auto response2 = caching_client.Get("https://httpbun.com/get?param=1");
        
        REQUIRE(response1->content != response2->content);
    }

    SECTION("Test IsInCache") {
        auto request1 = HttpRequest(HttpMethod::GET, "https://httpbun.com/get");
        auto request2 = HttpRequest(HttpMethod::GET, "https://httpbun.com/get?param=1");
        
        // Initially nothing should be in cache
        REQUIRE_FALSE(caching_client.IsInCache(request1));
        REQUIRE_FALSE(caching_client.IsInCache(request2));
        
        // Make a request - should add to cache
        auto response1 = caching_client.SendRequest(request1);
        REQUIRE(response1->code == 200);
        REQUIRE(caching_client.IsInCache(request1));
        REQUIRE_FALSE(caching_client.IsInCache(request2));
        
        // Wait for cache to expire
        std::this_thread::sleep_for(std::chrono::seconds(3));
        REQUIRE_FALSE(caching_client.IsInCache(request1));
    }
}
