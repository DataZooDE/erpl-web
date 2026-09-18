#include "catch.hpp"

#include "datasphere_catalog.hpp"

#include <memory>
#include <string>

using namespace erpl_web;

// A failed page must not look like the last page.
//
// The Datasphere asset listings paginate until a short page arrives. Every failure check
// along the way - no response, non-200, unparseable JSON, a root that is not an array -
// used to `break`, which is the SAME exit the last page takes. So an HTTP 500 on page two
// of a catalog returned page one and reported success, and nothing distinguished a
// truncated catalog from a short one.
//
// Tested through this seam rather than end to end because the catalog URLs are built as
// https://<tenant>.<data_center>.hcs.cloud.sap/... with no loopback hatch, so the
// paginating callers cannot be pointed at a local server. See GitHub #245.

namespace {

std::unique_ptr<HttpResponse> MakeResponse(int status, const std::string &body) {
    HttpMethod method = HttpMethod::GET;
    HttpUrl url("https://tenant.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/SALES/tables");
    // Content() is a by-value getter; the five-argument constructor is what actually sets
    // the body. Assigning to Content() compiled and did nothing, so every "valid JSON" case
    // arrived here as an empty body and was rejected as unparseable.
    return std::make_unique<HttpResponse>(method, url, status, "application/json", body);
}

const char *const WHAT = "'https://tenant.eu10.hcs.cloud.sap/.../tables'";

}  // namespace

TEST_CASE("a valid listing page is accepted and its root returned", "[datasphere][listing_page]") {
    std::shared_ptr<duckdb_yyjson::yyjson_doc> doc;
    auto response = MakeResponse(200, R"(["CUSTOMER","ORDERS"])");

    auto *root = RequireValidListingPage(response.get(), WHAT, 0, doc);

    REQUIRE(root != nullptr);
    REQUIRE(duckdb_yyjson::yyjson_is_arr(root));
    REQUIRE(duckdb_yyjson::yyjson_arr_size(root) == 2);
}

TEST_CASE("an empty page is valid - it is how pagination legitimately ends",
          "[datasphere][listing_page]") {
    // The one case that must NOT throw. If it did, every complete listing would fail on its
    // final page.
    std::shared_ptr<duckdb_yyjson::yyjson_doc> doc;
    auto response = MakeResponse(200, "[]");

    auto *root = RequireValidListingPage(response.get(), WHAT, 100, doc);

    REQUIRE(root != nullptr);
    REQUIRE(duckdb_yyjson::yyjson_arr_size(root) == 0);
}

TEST_CASE("a failed page is refused rather than treated as the end", "[datasphere][listing_page]") {
    std::shared_ptr<duckdb_yyjson::yyjson_doc> doc;

    SECTION("no response at all") {
        REQUIRE_THROWS_AS(RequireValidListingPage(nullptr, WHAT, 100, doc), duckdb::IOException);
    }

    SECTION("HTTP 500 on a later page - the reported reproduction") {
        auto response = MakeResponse(500, "upstream exploded");
        REQUIRE_THROWS_AS(RequireValidListingPage(response.get(), WHAT, 100, doc),
                          duckdb::IOException);
    }

    SECTION("HTTP 401, which a truncating break would have hidden as success") {
        auto response = MakeResponse(401, "");
        REQUIRE_THROWS_AS(RequireValidListingPage(response.get(), WHAT, 0, doc),
                          duckdb::IOException);
    }

    SECTION("a body that is not JSON") {
        auto response = MakeResponse(200, "<html>proxy error</html>");
        REQUIRE_THROWS_AS(RequireValidListingPage(response.get(), WHAT, 0, doc),
                          duckdb::IOException);
    }

    SECTION("valid JSON whose root is not an array") {
        auto response = MakeResponse(200, R"({"error":"nope"})");
        REQUIRE_THROWS_AS(RequireValidListingPage(response.get(), WHAT, 0, doc),
                          duckdb::IOException);
    }
}

TEST_CASE("the failure names the page, so a truncated listing is diagnosable",
          "[datasphere][listing_page]") {
    std::shared_ptr<duckdb_yyjson::yyjson_doc> doc;
    auto response = MakeResponse(500, "");

    try {
        RequireValidListingPage(response.get(), WHAT, 300, doc);
        FAIL("expected a throw");
    } catch (const duckdb::IOException &e) {
        const std::string message = e.what();
        INFO(message);
        REQUIRE(message.find("500") != std::string::npos);
        REQUIRE(message.find("300") != std::string::npos);      // which page was lost
        REQUIRE(message.find("incomplete") != std::string::npos);
    }
}
