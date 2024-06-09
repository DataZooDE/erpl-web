#include <thread>
#include <chrono>

#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"

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