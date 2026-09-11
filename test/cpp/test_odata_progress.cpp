// GitHub #160: rows were counted twice - once when a page was buffered
// (ProcessPageResponse) and again when the rows were emitted
// (UpdateProgressTracking). Progress therefore over-reported by up to 2x and
// saturated early. GetProgressFraction clamps to 100, so the only way to observe
// it is to sample progress mid-scan, which needs a service large enough to emit
// more than one vector.

#include "catch.hpp"
#include "duckdb.hpp"

#include "odata_client.hpp"
#include "odata_read_functions.hpp"
#include "odata_test_server.hpp"

#include <memory>
#include <sstream>
#include <string>

using erpl_web::HttpClient;
using erpl_web::HttpParams;
using erpl_web::HttpUrl;
using erpl_web::ODataEntitySetClient;
using erpl_web::ODataReadBindData;
using erpl_web::ODataVersion;
using erpl_web::test_support::CannedResponse;
using erpl_web::test_support::ODataTestServer;

namespace {

const char *const PROGRESS_EDMX = R"(<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Test" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Row">
        <Key><PropertyRef Name="id"/></Key>
        <Property Name="id" Type="Edm.String" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="Rows" EntityType="Test.Row"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>)";

// A page carrying @odata.count, which is what gives the tracker a denominator.
std::string MakeCountedPage(const std::string &context, idx_t first_id, idx_t row_count,
                            idx_t total_count, const std::string &next_link)
{
    std::ostringstream out;
    out << R"({"@odata.context":")" << context << R"(","@odata.count":)" << total_count
        << R"(,"value":[)";
    for (idx_t i = 0; i < row_count; i++) {
        if (i > 0) {
            out << ",";
        }
        out << R"({"id":"row-)" << (first_id + i) << R"("})";
    }
    out << "]";
    if (!next_link.empty()) {
        out << R"(,"@odata.nextLink":")" << next_link << R"(")";
    }
    out << "}";
    return out.str();
}

}  // namespace

TEST_CASE("progress reflects rows emitted, not rows counted twice", "[odata_progress]") {
    // The first page must be SMALLER than a vector, so the scan pulls page two before it
    // emits anything. That second page is the one that goes through ProcessPageResponse,
    // which is where the duplicate increment lived; a first page big enough to fill a
    // vector on its own never exercises it.
    constexpr idx_t FIRST_PAGE_ROWS = 1500;
    constexpr idx_t SECOND_PAGE_ROWS = 1600;
    constexpr idx_t TOTAL_ROWS = FIRST_PAGE_ROWS + SECOND_PAGE_ROWS;

    ODataTestServer server;
    const std::string entity_url = server.Url("/prog/Rows");
    const std::string context = server.Url("/prog/$metadata") + "#Rows";

    server.ServeMetadata("/prog/$metadata", PROGRESS_EDMX);
    server.OnMatch(
        [](const erpl_web::test_support::RecordedRequest &request) {
            return request.path == "/prog/Rows" && request.QueryParam("$skiptoken") == "2";
        },
        CannedResponse::Json(MakeCountedPage(context, FIRST_PAGE_ROWS, SECOND_PAGE_ROWS,
                                             TOTAL_ROWS, "")));
    server.OnPath("/prog/Rows",
                  CannedResponse::Json(MakeCountedPage(context, 0, FIRST_PAGE_ROWS, TOTAL_ROWS,
                                                       entity_url + "?$format=json&$skiptoken=2")));

    HttpParams params;
    auto http_client = std::make_shared<HttpClient>(params);
    auto client = std::make_shared<ODataEntitySetClient>(http_client, HttpUrl(entity_url), nullptr);
    client->SetODataVersionDirectly(ODataVersion::V4);

    auto bind_data = ODataReadBindData::FromEntitySetClient(client, "");
    auto types = bind_data->GetResultTypes();
    REQUIRE_FALSE(types.empty());

    duckdb::vector<duckdb::LogicalType> chunk_types(types.begin(), types.end());
    duckdb::DataChunk chunk;
    chunk.Initialize(duckdb::Allocator::DefaultAllocator(), chunk_types);

    chunk.Reset();
    const auto emitted = bind_data->FetchNextResult(chunk);
    REQUIRE(emitted > 0);
    REQUIRE(emitted <= STANDARD_VECTOR_SIZE);

    // Whatever share of the rows has actually been emitted, progress must be about that
    // share. Counting each row twice drove this past 100 (clamped) while barely two thirds
    // of the rows had been handed over.
    const double expected = 100.0 * static_cast<double>(emitted) / static_cast<double>(TOTAL_ROWS);
    const double reported = bind_data->GetProgressFraction();
    INFO("emitted=" << emitted << " reported=" << reported << " expected=~" << expected);
    REQUIRE(reported == Approx(expected).margin(1.0));
}
