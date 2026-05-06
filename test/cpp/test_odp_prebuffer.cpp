#include "catch.hpp"
#include "odata_read_functions.hpp"
#include "odata_client.hpp"
#include "http_client.hpp"

using namespace erpl_web;

// Minimal OData v2 ODP tracking-response with 2 records (no __next, no __delta).
static const std::string kOdpPage = R"({
  "d": {
    "results": [
      {"ID": "REC001", "COL_A": "alpha", "COL_B": "1"},
      {"ID": "REC002", "COL_A": "beta",  "COL_B": "2"}
    ]
  }
})";

// Helper: create an ODataReadBindData whose HTTP client points at an
// unreachable address. PrefetchFirstPage() on this bind data fires a real
// GET that immediately fails — so any test that allows PrefetchFirstPage() to
// run will throw, and any test that suppresses it will not.
static duckdb::unique_ptr<ODataReadBindData> make_bind_data() {
    HttpParams params;
    params.url_encode = false;
    auto http  = std::make_shared<HttpClient>(params);
    auto url   = HttpUrl("http://127.0.0.1:0/sap/opu/odata/NS/SRV/FactsOfI_ENTITY");
    auto client = std::make_shared<ODataEntitySetClient>(http, url);
    client->SetODataVersionDirectly(ODataVersion::V2);
    return ODataReadBindData::FromEntitySetClient(client, kOdpPage);
}

// ---------------------------------------------------------------------------
// Red test: demonstrates the problem
// ---------------------------------------------------------------------------
TEST_CASE("ODP bare-GET duplication: FromEntitySetClient leaves first page uncached",
          "[odp_prebuffer]") {

    auto bind_data = make_bind_data();

    // Without PreBufferFirstPage the flag must be false — meaning the next
    // FetchNextResult() call will trigger PrefetchFirstPage() → bare HTTP GET.
    REQUIRE(!bind_data->IsFirstPageCached());
}

// ---------------------------------------------------------------------------
// Green test: describes the desired fix behaviour
// ---------------------------------------------------------------------------
TEST_CASE("ODP bare-GET duplication: PreBufferFirstPage suppresses bare GET",
          "[odp_prebuffer]") {

    SECTION("PreBufferFirstPage marks first page as cached") {
        auto bind_data = make_bind_data();
        bind_data->PreBufferFirstPage(kOdpPage, ODataVersion::V2);
        REQUIRE(bind_data->IsFirstPageCached());
    }

    SECTION("PrefetchFirstPage is a no-op after PreBufferFirstPage (no HTTP call)") {
        auto bind_data = make_bind_data();
        bind_data->PreBufferFirstPage(kOdpPage, ODataVersion::V2);
        // If first_page_cached_ is true, PrefetchFirstPage returns immediately
        // without touching the HTTP client — even though the URL is unreachable.
        REQUIRE_NOTHROW(bind_data->PrefetchFirstPage());
    }

    SECTION("column names are extracted from the pre-buffered content") {
        auto bind_data = make_bind_data();
        bind_data->PreBufferFirstPage(kOdpPage, ODataVersion::V2);
        auto names = bind_data->GetResultNames(true);
        REQUIRE(names.size() >= 3);
        REQUIRE(std::find(names.begin(), names.end(), "ID")    != names.end());
        REQUIRE(std::find(names.begin(), names.end(), "COL_A") != names.end());
        REQUIRE(std::find(names.begin(), names.end(), "COL_B") != names.end());
    }
}
