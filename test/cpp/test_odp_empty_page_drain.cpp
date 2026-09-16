// The ODP empty-page drain, and specifically its FAILURE path.
//
// A page that yields zero rows while still carrying a next link is a real ODP shape - delta
// packages and skip-token pages both produce it - so the reader must keep draining. But the
// run has to be bounded, and what it does at the bound matters enormously: zero rows is how
// a DuckDB table function says end-of-scan, so yielding there makes OdpODataReadScan call
// FinalizeScan(), which COMMITS THE DELTA TOKEN over rows that were never delivered. That
// is silent data loss in a delta extraction (GitHub #232).
//
// Reaching this through the reader needs a live ODP service with an open delta queue, so
// the branch had no test at all. The loop is a seam now, driven here with lambdas.

#include "catch.hpp"
#include "duckdb.hpp"

#include "odp_odata_read_bind_data.hpp"

#include <string>

namespace {

constexpr unsigned int BUDGET = 8;  // the shape matters, not the production number

}  // namespace

TEST_CASE("the drain returns rows that arrive after a run of empty pages", "[odp][drain]") {
    int fetches = 0;
    int pages_fetched = 0;

    // Empty, empty, then 5 rows - the legitimate delta-package shape.
    const auto rows = erpl_web::DrainEmptyOdpPages(
        [&] { return ++fetches < 3 ? 0u : 5u; },
        [&] { return true; },
        [&] { pages_fetched++; },
        BUDGET, "");

    REQUIRE(rows == 5);
    REQUIRE(pages_fetched == 2);
}

TEST_CASE("the drain stops as soon as no further page is advertised", "[odp][drain]") {
    int pages_fetched = 0;

    // Always empty, but the service says there is nothing more: that is a real end, and
    // returning 0 here is correct - the caller commits the token because everything WAS
    // delivered.
    const auto rows = erpl_web::DrainEmptyOdpPages(
        [] { return 0u; }, [] { return false; }, [&] { pages_fetched++; }, BUDGET, "");

    REQUIRE(rows == 0);
    REQUIRE(pages_fetched == 0);
}

// The case that matters: the budget runs out while the service is STILL advertising a page.
TEST_CASE("the drain throws rather than yielding a false end-of-scan", "[odp][drain]") {
    int pages_fetched = 0;

    REQUIRE_THROWS_AS(erpl_web::DrainEmptyOdpPages(
                          [] { return 0u; },      // never any rows
                          [] { return true; },    // always another page
                          [&] { pages_fetched++; }, BUDGET, ""),
                      duckdb::IOException);

    // It really did try, and it really did stop.
    REQUIRE(pages_fetched == BUDGET);
}

TEST_CASE("the drain's failure says the next run resumes from the last position", "[odp][drain]") {
    // The message is the only thing that tells a user what a retry costs, so it has to track
    // what reactivation actually does. It has now been wrong in both directions: it first
    // promised a resume that did not happen (the delta token was cleared on reactivation),
    // was corrected in #235 to promise a full re-extraction, and #236 then made the resume
    // real -- an 'error' subscription keeps its delta position, because a failure that would
    // invalidate the position never reaches that status.
    try {
        erpl_web::DrainEmptyOdpPages([] { return 0u; }, [] { return true; }, [] {}, BUDGET, "");
        FAIL("expected the drain to throw");
    } catch (const duckdb::IOException &e) {
        const std::string message = e.what();
        INFO(message);
        REQUIRE(message.find("resumes from there") != std::string::npos);
        REQUIRE(message.find("not advanced") != std::string::npos);
        // And it must not go back to promising the full re-extraction that no longer happens.
        REQUIRE(message.find("re-extracts in full") == std::string::npos);
    }
}
