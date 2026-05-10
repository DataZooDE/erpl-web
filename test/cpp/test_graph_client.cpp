#include "catch.hpp"
#include "duckdb/common/exception.hpp"
#include "graph_client.hpp"
#include "yyjson.hpp"

using namespace erpl_web;

TEST_CASE("GraphClient URL helpers encode path segments", "[graph_client][url]") {
    REQUIRE(GraphClient::UrlEncode("jr@data-zoo.de") == "jr%40data-zoo.de");
    REQUIRE(GraphClient::UrlEncode("19:abc@thread.tacv2") == "19%3Aabc%40thread.tacv2");
    REQUIRE(GraphClient::UrlEncode("sites/hr", true) == "sites/hr");
    REQUIRE(GraphClient::UrlEncode("sites/hr") == "sites%2Fhr");
}

TEST_CASE("GraphClient user segment resolves me, GUIDs, and UPNs", "[graph_client][url]") {
    REQUIRE(GraphClient::ResolveUserSegment("") == "me");
    REQUIRE(GraphClient::ResolveUserSegment("ae6f7cf1-52a8-4e8b-9864-136bf1353604") ==
            "users/ae6f7cf1-52a8-4e8b-9864-136bf1353604");
    REQUIRE(GraphClient::ResolveUserSegment("jr@data-zoo.de") == "users/jr%40data-zoo.de");
}

TEST_CASE("GraphClient GUID detection checks canonical separator positions", "[graph_client][url]") {
    REQUIRE(GraphClient::LooksLikeGuid("ae6f7cf1-52a8-4e8b-9864-136bf1353604"));
    REQUIRE_FALSE(GraphClient::LooksLikeGuid("ae6f7cf152a84e8b9864136bf1353604"));
    REQUIRE_FALSE(GraphClient::LooksLikeGuid("ae6f7cf1_52a8-4e8b-9864-136bf1353604"));
}

TEST_CASE("GraphClient extracts nextLink from Graph JSON", "[graph_client][json]") {
    const std::string body = R"({"value":[],"@odata.nextLink":"https://graph.microsoft.com/v1.0/users?$skiptoken=abc"})";
    REQUIRE(GraphClient::ExtractNextLink(body).value() ==
            "https://graph.microsoft.com/v1.0/users?$skiptoken=abc");
    REQUIRE_FALSE(GraphClient::ExtractNextLink(R"({"value":[]})").has_value());
    REQUIRE_THROWS_AS(GraphClient::ExtractNextLink("{not-json"), duckdb::IOException);
}

TEST_CASE("Graph JSON helpers find root and array string values", "[graph_client][json]") {
    const std::string body = R"({
        "id": "root-id",
        "value": [
            {"id": "id-1", "displayName": "One"},
            {"id": "id-2", "displayName": "Two"}
        ]
    })";

    REQUIRE(GraphJsonGetRootString(body, "id", "test").value() == "root-id");
    REQUIRE_FALSE(GraphJsonGetRootString(body, "missing", "test").has_value());

    REQUIRE(GraphJsonFirstStringInArray(body, "value", "id", "test").value() == "id-1");
    REQUIRE(GraphJsonFindStringInArray(body, "value", "displayName", "Two", "id", "test").value() == "id-2");
    REQUIRE_FALSE(GraphJsonFindStringInArray(body, "value", "displayName", "Three", "id", "test").has_value());
    REQUIRE_THROWS_AS(GraphJsonFindStringInArray("{not-json", "value", "displayName", "Two", "id", "test"),
                      duckdb::IOException);
}

TEST_CASE("GraphJsonStringArray normalizes primitive arrays", "[graph_client][json]") {
    const std::string body = R"(["alpha", null, true, false])";
    auto doc = duckdb_yyjson::yyjson_read(body.c_str(), body.size(), 0);
    REQUIRE(doc != nullptr);

    auto values = GraphJsonStringArray(duckdb_yyjson::yyjson_doc_get_root(doc));
    REQUIRE(values.size() == 4);
    REQUIRE(values[0] == "alpha");
    REQUIRE(values[1] == "");
    REQUIRE(values[2] == "true");
    REQUIRE(values[3] == "false");

    duckdb_yyjson::yyjson_doc_free(doc);
}
