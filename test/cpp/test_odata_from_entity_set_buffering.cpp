#include "catch.hpp"
#include "odata_read_functions.hpp"
#include "odata_client.hpp"
#include "odata_edm.hpp"
#include "http_client.hpp"
#include <algorithm>

using namespace erpl_web;

// Minimal OData V4 EDM for a Customers entity set with id (String) and name (String).
// Pre-populated in EdmCache so GetResultTypes() never needs a real metadata HTTP request.
static const std::string MINIMAL_EDM_XML = R"(<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Test" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Customer">
        <Key><PropertyRef Name="id"/></Key>
        <Property Name="id" Type="Edm.String" Nullable="false"/>
        <Property Name="name" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="Customers" EntityType="Test.Customer"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>)";

// Use loopback on a port that is almost certainly closed. Any HTTP attempt would
// get ECONNREFUSED immediately, so a test that mistakenly triggers a network
// call fails fast rather than hanging.
static const std::string TEST_URL = "http://127.0.0.1:65534/TestService/Customers";
static const std::string METADATA_URL = "http://127.0.0.1:65534/TestService/$metadata";

// Two-row OData V4 payload.
static const std::string TWO_ROW_V4_JSON =
    R"({"value":[{"id":"1","name":"Alice"},{"id":"2","name":"Bob"}]})";

// Two-row OData V2 payload (the "d.results" envelope format used by SAP systems).
static const std::string TWO_ROW_V2_JSON =
    R"({"d":{"results":[{"id":"1","name":"Alice"},{"id":"2","name":"Bob"}]}})";

// Populate the global EdmCache singleton for the test metadata URL.
static void SeedEdmCache() {
    auto edmx = Edmx::FromXml(MINIMAL_EDM_XML);
    EdmCache::GetInstance().Set(METADATA_URL, edmx);
}

static std::shared_ptr<ODataEntitySetClient> MakeClientForUrl(ODataVersion version, const std::string &url_string) {
    auto http_client = std::make_shared<HttpClient>();
    HttpUrl url(url_string);
    auto auth_params = std::make_shared<HttpAuthParams>();
    auto client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
    client->SetODataVersionDirectly(version);
    return client;
}

static std::shared_ptr<ODataEntitySetClient> MakeClient(ODataVersion version) {
    return MakeClientForUrl(version, TEST_URL);
}

// ============================================================================
// Column-name extraction from initial_content
// ============================================================================

TEST_CASE("FromEntitySetClient - extracts column names from OData V4 initial_content") {
    SeedEdmCache();
    auto bind_data = ODataReadBindData::FromEntitySetClient(MakeClient(ODataVersion::V4), TWO_ROW_V4_JSON);

    auto names = bind_data->GetResultNames();
    REQUIRE_FALSE(names.empty());
    bool has_id   = std::find(names.begin(), names.end(), "id")   != names.end();
    bool has_name = std::find(names.begin(), names.end(), "name") != names.end();
    REQUIRE(has_id);
    REQUIRE(has_name);
}

TEST_CASE("FromEntitySetClient - extracts column names from OData V2 initial_content") {
    SeedEdmCache();
    auto bind_data = ODataReadBindData::FromEntitySetClient(MakeClient(ODataVersion::V2), TWO_ROW_V2_JSON);

    auto names = bind_data->GetResultNames();
    REQUIRE_FALSE(names.empty());
    bool has_id   = std::find(names.begin(), names.end(), "id")   != names.end();
    bool has_name = std::find(names.begin(), names.end(), "name") != names.end();
    REQUIRE(has_id);
    REQUIRE(has_name);
}

TEST_CASE("FromEntitySetClient - extracts initial_content columns without metadata") {
    auto client = MakeClientForUrl(
        ODataVersion::V4,
        "http://127.0.0.1:65534/NoMetadataService/Customers");
    auto bind_data = ODataReadBindData::FromEntitySetClient(client, TWO_ROW_V4_JSON);

    auto names = bind_data->GetResultNames();
    REQUIRE(names == std::vector<std::string>({"id", "name"}));
}

// ============================================================================
// Rows are pre-buffered (HasMoreResults is true immediately, no HTTP needed)
// ============================================================================

TEST_CASE("FromEntitySetClient - HasMoreResults is true after initial_content buffered") {
    SeedEdmCache();
    auto bind_data = ODataReadBindData::FromEntitySetClient(MakeClient(ODataVersion::V4), TWO_ROW_V4_JSON);
    REQUIRE(bind_data->HasMoreResults());
}

// ============================================================================
// Regression test (Issue #37): first_page_cached_ must be true so that the
// next EnsureInitialized() call skips PrefetchFirstPage() and does not issue
// a redundant GET. Against SAP ODP that GET returns the full dataset, causing
// N rows to become 2N (or more) in the query result.
//
// Observable proof: calling PrefetchFirstPage() on a bind_data whose
// first_page_cached_ is already true is a documented no-op. If first_page_cached_
// were false (the pre-fix behaviour), PrefetchFirstPage() would call
// odata_client->Get() which connects to 127.0.0.1:65534. That port is closed
// → ECONNREFUSED → std::runtime_error. REQUIRE_NOTHROW catches the regression.
// ============================================================================

TEST_CASE("FromEntitySetClient - regression: first_page_cached_ set after initial_content") {
    SeedEdmCache();
    auto bind_data = ODataReadBindData::FromEntitySetClient(MakeClient(ODataVersion::V4), TWO_ROW_V4_JSON);

    // Must not throw: first_page_cached_ is true, so PrefetchFirstPage returns immediately.
    REQUIRE_NOTHROW(bind_data->PrefetchFirstPage());

    // Result names must still be correct after the redundant call.
    auto names = bind_data->GetResultNames();
    bool has_id   = std::find(names.begin(), names.end(), "id")   != names.end();
    bool has_name = std::find(names.begin(), names.end(), "name") != names.end();
    REQUIRE(has_id);
    REQUIRE(has_name);
}

TEST_CASE("FromEntitySetClient - regression: OData V2 path also sets first_page_cached_") {
    SeedEdmCache();
    auto bind_data = ODataReadBindData::FromEntitySetClient(MakeClient(ODataVersion::V2), TWO_ROW_V2_JSON);

    REQUIRE_NOTHROW(bind_data->PrefetchFirstPage());
}

// ============================================================================
// Empty initial_content: no pre-buffering, but HasMoreResults is still true
// because first_page_cached_ is false (data will be fetched on demand).
// ============================================================================

TEST_CASE("FromEntitySetClient - empty initial_content leaves data to be fetched") {
    // No EdmCache seeding needed here: with empty initial_content the JSON
    // parsing branch is skipped entirely and HasMoreResults() returns true
    // via the !first_page_cached_ check, without any metadata fetch.
    auto bind_data = ODataReadBindData::FromEntitySetClient(MakeClient(ODataVersion::V4), "");
    REQUIRE(bind_data->HasMoreResults());
}
