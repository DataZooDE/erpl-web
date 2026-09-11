// GitHub #158: the ODP audit row is written once per emitted chunk, on a freshly
// constructed duckdb::Connection each time, and the UPDATE assigns rather than
// accumulates. A completed extraction therefore records the LAST chunk's row count
// (<= 2048) instead of the total, so the audit table cannot be used to verify that a
// delta package was fully delivered - which is the only thing it is for.

#include "catch.hpp"
#include "duckdb.hpp"

#include "odata_test_server.hpp"
#include "odp_test_db.hpp"

#include <sstream>
#include <string>

using erpl_web::test_support::CannedResponse;
using erpl_web::test_support::ODataTestServer;

namespace {

// An OData v2 ODP page: the shape the ODP reader parses.
std::string MakeOdpPage(std::size_t row_count)
{
    std::ostringstream out;
    out << R"({"d":{"results":[)";
    for (std::size_t i = 0; i < row_count; i++) {
        if (i > 0) {
            out << ",";
        }
        out << R"({"ID":")" << i << R"(","ODQ_CHANGEMODE":"C"})";
    }
    out << "]}}";
    return out.str();
}

}  // namespace

TEST_CASE("the ODP audit row records the whole extraction, not the last chunk",
          "[odp_audit]") {
    // More than one vector, so the scan emits several chunks. With a single chunk the
    // defect is invisible: the last chunk IS the total.
    constexpr std::size_t ROW_COUNT = 3000;

    ODataTestServer server;
    // Served at both the service root and the entity-relative path, so the test does not
    // depend on which of the two the metadata resolver computes for a v2 payload that
    // carries no @odata.context.
    server.ServeMetadataFixture("/sap/opu/odata/sap/Z_TEST_SRV/$metadata",
                                "edm_sap_odp_bw_fact.xml");
    server.ServeMetadataFixture("/sap/opu/odata/sap/Z_TEST_SRV/FactsOf0D_NW_C01/$metadata",
                                "edm_sap_odp_bw_fact.xml");
    server.OnPath("/sap/opu/odata/sap/Z_TEST_SRV/FactsOf0D_NW_C01",
                  CannedResponse::Json(MakeOdpPage(ROW_COUNT)));

    odp_test::TempDatabase db("odp_audit_totals");
    auto &con = db.Conn();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    const auto url = server.Url("/sap/opu/odata/sap/Z_TEST_SRV/FactsOf0D_NW_C01");
    auto read = con.Query("SELECT COUNT(*) FROM odp_odata_read('" + url + "')");
    INFO((read->HasError() ? read->GetError() : std::string()));
    REQUIRE_FALSE(read->HasError());
    REQUIRE(read->GetValue(0, 0).GetValue<int64_t>() == static_cast<int64_t>(ROW_COUNT));

    auto audit = con.Query(
        "SELECT max(rows_fetched) FROM erpl_web.odp_subscription_audit");
    INFO((audit->HasError() ? audit->GetError() : std::string()));
    REQUIRE_FALSE(audit->HasError());
    REQUIRE(audit->RowCount() == 1);

    const auto recorded = audit->GetValue(0, 0);
    INFO("audit rows_fetched = " << recorded.ToString() << ", extraction delivered " << ROW_COUNT);
    REQUIRE_FALSE(recorded.IsNull());
    REQUIRE(recorded.GetValue<int64_t>() == static_cast<int64_t>(ROW_COUNT));
}
