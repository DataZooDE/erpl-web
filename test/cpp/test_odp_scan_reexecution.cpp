// GitHub #146: odp_odata_read has no per-execution scan state - the #75 class, still open
// on the ODP side.
//
// odata_read, the ATTACHed odata_table_scan, the SAC readers and (as of #75's Datasphere
// follow-up) both Datasphere readers hand each execution a private clone owned by a real
// GlobalTableFunctionState. OdpODataReadInitGlobalState still returned a bare
// GlobalTableFunctionState and OdpODataReadScan cast data.bind_data directly, so every
// execution of a bound plan scanned the same object: the second EXECUTE found the row
// buffer already drained and returned nothing, silently.
//
// A note on what is NOT tested here. The issue also asks for a self-join of one
// odp_odata_read call. That shape cannot work against a real service and should not be
// made to: an ODQ subscription admits one open extraction at a time, and SAP refuses the
// second with "Could not open data access via extraction API RODPS_REPL_ODP_OPEN"
// (RSODP_ODATA/013). That was established in #173, where the same shape in
// odp_odata_read.test had to be rewritten as two sequential reads. Asserting that a
// self-join succeeds against a canned server would assert something untrue of every real
// service, so re-execution - which is sequential, and which real services do support - is
// what is covered.

#include "catch.hpp"
#include "duckdb.hpp"

#include "odata_test_server.hpp"
#include "odp_test_db.hpp"

#include <sstream>
#include <string>

using erpl_web::test_support::CannedResponse;
using erpl_web::test_support::ODataTestServer;

namespace {

// An OData v2 ODP page, the shape the ODP reader parses.
std::string MakeOdpPage(std::size_t row_count)
{
    std::ostringstream out;
    out << R"({"d":{"results":[)";
    for (std::size_t i = 0; i < row_count; i++) {
        if (i > 0) {
            out << ",";
        }
        // D_NW_DIV is a real column of edm_sap_odp_bw_fact.xml; the schema comes from
        // the EDM, so a made-up name binds to nothing.
        out << R"({"D_NW_DIV":")" << i << R"(","ODQ_CHANGEMODE":"C"})";
    }
    out << "]}}";
    return out.str();
}

void ServeOdpEntitySet(ODataTestServer &server, const std::string &path, std::size_t rows)
{
    server.ServeMetadataFixture("/sap/opu/odata/sap/Z_TEST_SRV/$metadata",
                                "edm_sap_odp_bw_fact.xml");
    server.ServeMetadataFixture(path + "/$metadata", "edm_sap_odp_bw_fact.xml");
    server.OnPath(path, CannedResponse::Json(MakeOdpPage(rows)));
}

}  // namespace

TEST_CASE("a bound odp_odata_read plan returns every row on each execution",
          "[odp_reexec]") {
    constexpr std::size_t ROW_COUNT = 120;
    const std::string path = "/sap/opu/odata/sap/Z_TEST_SRV/FactsOf0D_NW_C01";

    ODataTestServer server;
    ServeOdpEntitySet(server, path, ROW_COUNT);

    // ODP delta state is refused an in-memory catalog on purpose (GitHub #92), so the
    // state has to live in a real database file.
    odp_test::TempDatabase db("odp_reexec");
    auto &con = db.Conn();
    REQUIRE_FALSE(con.Query("LOAD erpl_web")->HasError());

    const auto url = server.Url(path);
    REQUIRE_FALSE(
        con.Query("PREPARE p AS SELECT COUNT(*) FROM odp_odata_read('" + url + "')")
            ->HasError());

    for (int execution = 1; execution <= 3; execution++) {
        auto result = con.Query("EXECUTE p");
        INFO("execution " << execution);
        INFO((result->HasError() ? result->GetError() : std::string()));
        REQUIRE_FALSE(result->HasError());
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() ==
                static_cast<int64_t>(ROW_COUNT));
    }
}

// The projecting re-execution case is deliberately absent. odp_odata_read returns NULL for
// every explicitly projected column while SELECT * returns the values - a pre-existing
// defect filed as GitHub #179, confirmed by reverting every ODP source change here and
// re-running. A projecting case cannot pass while that stands, and folding the two
// together would hide one behind the other. It belongs with the fix for #179.
