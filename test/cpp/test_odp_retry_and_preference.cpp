// Coverage for GitHub #97: HTTP 202 Accepted / Retry-After handling in the ODP request layer,
// and validation (rather than inference) of `Preference-Applied: odata.track-changes`.
//
// Every test here drives the real OdpRequestOrchestrator against the local ODP-speaking test
// server; none of them sleep for a real Retry-After interval - the orchestrator's sleep function
// is replaced with a recorder so the retry arithmetic is asserted directly.

#include "catch.hpp"

#include "odata_test_server.hpp"
#include "odp_request_orchestrator.hpp"
#include "datazoo/oauth2/http_client.hpp"

#include <chrono>
#include <string>
#include <vector>

using erpl_web::OdpRequestOrchestrator;
using erpl_web::test_support::CannedResponse;
using erpl_web::test_support::ODataTestServer;

namespace {

/// An OData v2 ODP page carrying a delta link, in the shape ExtractDeltaTokenFromV2Response reads.
/// Built by hand rather than via MakeOdpDeltaPage so that a test can serve the identical BODY with
/// and without the `Preference-Applied` header - which is exactly the distinction #97 turns on.
std::string OdpDeltaBody(const std::string &delta_token)
{
    return std::string("{\"d\":{\"results\":[{\"ID\":\"1\",\"ODQ_CHANGEMODE\":\"C\"}],")
           + "\"__delta\":\"http://127.0.0.1/odp/Entity?$format=json&!deltatoken='" + delta_token + "'\"}}";
}

/// Install a sleep function that records the requested waits instead of performing them.
void RecordSleepsInto(OdpRequestOrchestrator &orchestrator, std::vector<std::chrono::milliseconds> &sink)
{
    orchestrator.SetSleepFunction(
        [&sink](std::chrono::milliseconds duration) { sink.push_back(duration); });
}

} // namespace

// ---------------------------------------------------------------------------
// Retry-After parsing
// ---------------------------------------------------------------------------

TEST_CASE("OdpRequestOrchestrator parses both Retry-After forms", "[odp_retry][odp_orchestrator]") {
    // Pre-fix failure: OdpRequestOrchestrator::ParseRetryAfter did not exist at all, so this
    // translation unit failed to compile ("no member named 'ParseRetryAfter'"). Nothing anywhere
    // in src/ read the Retry-After header.
    const auto now = std::chrono::system_clock::now();

    SECTION("delta-seconds") {
        auto parsed = OdpRequestOrchestrator::ParseRetryAfter("120", now);
        REQUIRE(parsed.has_value());
        REQUIRE(parsed->count() == 120000);
    }

    SECTION("delta-seconds with surrounding whitespace") {
        auto parsed = OdpRequestOrchestrator::ParseRetryAfter("  7 ", now);
        REQUIRE(parsed.has_value());
        REQUIRE(parsed->count() == 7000);
    }

    SECTION("zero delta-seconds means retry immediately") {
        auto parsed = OdpRequestOrchestrator::ParseRetryAfter("0", now);
        REQUIRE(parsed.has_value());
        REQUIRE(parsed->count() == 0);
    }

    SECTION("HTTP-date in the future") {
        // 1994-11-06T08:49:37Z is 784111777 in Unix seconds; measure from 60s earlier.
        const auto reference = std::chrono::system_clock::time_point(std::chrono::seconds(784111777 - 60));
        auto parsed = OdpRequestOrchestrator::ParseRetryAfter("Sun, 06 Nov 1994 08:49:37 GMT", reference);
        REQUIRE(parsed.has_value());
        REQUIRE(parsed->count() == 60000);
    }

    SECTION("HTTP-date already in the past yields no wait") {
        const auto reference = std::chrono::system_clock::time_point(std::chrono::seconds(784111777 + 500));
        auto parsed = OdpRequestOrchestrator::ParseRetryAfter("Sun, 06 Nov 1994 08:49:37 GMT", reference);
        REQUIRE(parsed.has_value());
        REQUIRE(parsed->count() == 0);
    }

    SECTION("asctime form is tolerated") {
        const auto reference = std::chrono::system_clock::time_point(std::chrono::seconds(784111777 - 10));
        auto parsed = OdpRequestOrchestrator::ParseRetryAfter("Sun Nov  6 08:49:37 1994", reference);
        REQUIRE(parsed.has_value());
        REQUIRE(parsed->count() == 10000);
    }

    SECTION("unparsable values are reported as such") {
        REQUIRE_FALSE(OdpRequestOrchestrator::ParseRetryAfter("later please", now).has_value());
        REQUIRE_FALSE(OdpRequestOrchestrator::ParseRetryAfter("", now).has_value());
    }
}

// ---------------------------------------------------------------------------
// 202 Accepted re-polling
// ---------------------------------------------------------------------------

TEST_CASE("ODP initial load re-polls a 202 Accepted and honours Retry-After",
          "[odp_retry][odp_e2e]") {
    // Pre-fix failure: ExecuteRequest sent exactly one request and, because 202 is below the 400
    // error threshold, handed the empty 202 body to the OData parser as if it were the result.
    // server.RequestsFor("/odp/Entity").size() was 1, not 2, and the delta token came back empty.
    ODataTestServer server;

    auto accepted = CannedResponse::Json("{}", 202);
    accepted.WithHeader("Retry-After", "3");

    auto ready = CannedResponse::Json(OdpDeltaBody("TOKEN_A"));
    ready.WithHeader("Preference-Applied", "odata.track-changes");

    server.OnPathSequence("/odp/Entity", {accepted, ready});

    OdpRequestOrchestrator orchestrator(nullptr, 100);
    std::vector<std::chrono::milliseconds> sleeps;
    RecordSleepsInto(orchestrator, sleeps);

    auto result = orchestrator.ExecuteInitialLoad(server.Url("/odp/Entity"));

    REQUIRE(result.http_status_code == 200);
    REQUIRE(result.extracted_delta_token == "TOKEN_A");

    // The 202 was re-polled exactly once, and the server's stated interval was the one waited.
    REQUIRE(server.RequestsFor("/odp/Entity").size() == 2);
    REQUIRE(sleeps.size() == 1);
    REQUIRE(sleeps.front().count() == 3000);
}

TEST_CASE("ODP re-polling falls back to the default delay when Retry-After is absent",
          "[odp_retry][odp_e2e]") {
    // Pre-fix failure: no re-poll happened at all, so there was no delay to fall back to and
    // RequestsFor(...).size() was 1.
    ODataTestServer server;

    auto accepted = CannedResponse::Json("{}", 202); // no Retry-After header
    auto ready = CannedResponse::Json(OdpDeltaBody("TOKEN_B"));
    ready.WithHeader("Preference-Applied", "odata.track-changes");

    server.OnPathSequence("/odp/Entity", {accepted, ready});

    OdpRequestOrchestrator orchestrator(nullptr, 100);

    OdpRequestOrchestrator::RetryPolicy policy;
    policy.default_delay = std::chrono::milliseconds(250);
    orchestrator.SetRetryPolicy(policy);

    std::vector<std::chrono::milliseconds> sleeps;
    RecordSleepsInto(orchestrator, sleeps);

    auto result = orchestrator.ExecuteInitialLoad(server.Url("/odp/Entity"));

    REQUIRE(result.http_status_code == 200);
    REQUIRE(sleeps.size() == 1);
    REQUIRE(sleeps.front().count() == 250);
}

TEST_CASE("ODP re-polling gives up with a clear error once the wait budget is exhausted",
          "[odp_retry][odp_e2e]") {
    // Pre-fix failure: a server that answered 202 forever was not retried and not reported - the
    // single 202 was accepted as a successful (empty) result, so no exception was thrown at all
    // and this REQUIRE_THROWS_AS failed.
    ODataTestServer server;

    auto accepted = CannedResponse::Json("{}", 202);
    accepted.WithHeader("Retry-After", "30");
    // A single-entry sequence repeats forever: the package never becomes ready.
    server.OnPathSequence("/odp/Entity", {accepted});

    OdpRequestOrchestrator orchestrator(nullptr, 100);

    OdpRequestOrchestrator::RetryPolicy policy;
    policy.max_attempts = 100;                                  // not the binding limit here
    policy.max_total_wait = std::chrono::milliseconds(65000);   // room for two 30s waits, not three
    orchestrator.SetRetryPolicy(policy);

    std::vector<std::chrono::milliseconds> sleeps;
    RecordSleepsInto(orchestrator, sleeps);

    REQUIRE_THROWS_AS(orchestrator.ExecuteInitialLoad(server.Url("/odp/Entity")), duckdb::IOException);

    // Two waits fit inside the budget; the third would exceed it and aborts instead.
    REQUIRE(sleeps.size() == 2);
    REQUIRE(server.RequestsFor("/odp/Entity").size() == 3);
}

TEST_CASE("ODP re-polling stops after the configured attempt limit", "[odp_retry][odp_e2e]") {
    // Pre-fix failure: no attempt counting existed; the call returned normally after one request.
    ODataTestServer server;

    auto accepted = CannedResponse::Json("{}", 202);
    accepted.WithHeader("Retry-After", "0"); // never blocks, so the attempt cap is what binds
    server.OnPathSequence("/odp/Entity", {accepted});

    OdpRequestOrchestrator orchestrator(nullptr, 100);

    OdpRequestOrchestrator::RetryPolicy policy;
    policy.max_attempts = 3;
    orchestrator.SetRetryPolicy(policy);

    REQUIRE_THROWS_AS(orchestrator.ExecuteInitialLoad(server.Url("/odp/Entity")), duckdb::IOException);

    // Initial request plus max_attempts re-polls.
    REQUIRE(server.RequestsFor("/odp/Entity").size() == 4);
}

// ---------------------------------------------------------------------------
// Preference-Applied validation - the data-loss guard
// ---------------------------------------------------------------------------

TEST_CASE("ODP initial load reports change tracking only when the server confirms it",
          "[odp_preference][odp_e2e]") {
    // This is the highest-value assertion in #97. Before the fix, ExecuteRequest set
    //     result.preference_applied = !result.extracted_delta_token.empty();
    // so ANY response carrying a delta token was treated as change-tracked. The "server omits
    // Preference-Applied" section below therefore observed preference_applied == true, the reader
    // transitioned to DELTA_FETCH over data that was never change-tracked, and every change made
    // between that load and the next read was silently lost.
    //
    // The `response_headers` assertions also could not compile before the fix: OdpRequestResult
    // had no such member, because the real headers were discarded when the HttpResponse was moved
    // into the OData wrapper.

    SECTION("server confirms odata.track-changes") {
        ODataTestServer server;
        auto ready = CannedResponse::Json(OdpDeltaBody("TOKEN_OK"));
        ready.WithHeader("Preference-Applied", "odata.track-changes");
        server.OnPath("/odp/Entity", ready);

        OdpRequestOrchestrator orchestrator(nullptr, 100);
        auto result = orchestrator.ExecuteInitialLoad(server.Url("/odp/Entity"));

        REQUIRE(result.extracted_delta_token == "TOKEN_OK");
        REQUIRE(result.preference_applied);
        REQUIRE(result.response_headers.find("preference-applied") != result.response_headers.end());
    }

    SECTION("server omits Preference-Applied although a delta token is present") {
        ODataTestServer server;
        // Identical body, no Preference-Applied header.
        server.OnPath("/odp/Entity", CannedResponse::Json(OdpDeltaBody("TOKEN_UNTRACKED")));

        OdpRequestOrchestrator orchestrator(nullptr, 100);
        auto result = orchestrator.ExecuteInitialLoad(server.Url("/odp/Entity"));

        // The token is still extracted...
        REQUIRE(result.extracted_delta_token == "TOKEN_UNTRACKED");
        // ...but change tracking was NOT established, so the subscription must not advance.
        REQUIRE_FALSE(result.preference_applied);
        REQUIRE(result.response_headers.find("preference-applied") == result.response_headers.end());
    }

    SECTION("server applies other preferences but not track-changes") {
        ODataTestServer server;
        auto ready = CannedResponse::Json(OdpDeltaBody("TOKEN_PAGESIZE_ONLY"));
        ready.WithHeader("Preference-Applied", "odata.maxpagesize=100");
        server.OnPath("/odp/Entity", ready);

        OdpRequestOrchestrator orchestrator(nullptr, 100);
        auto result = orchestrator.ExecuteInitialLoad(server.Url("/odp/Entity"));

        REQUIRE_FALSE(result.preference_applied);
    }

    SECTION("track-changes alongside other applied preferences is accepted") {
        ODataTestServer server;
        auto ready = CannedResponse::Json(OdpDeltaBody("TOKEN_BOTH"));
        ready.WithHeader("Preference-Applied", "odata.maxpagesize=100, odata.track-changes");
        server.OnPath("/odp/Entity", ready);

        OdpRequestOrchestrator orchestrator(nullptr, 100);
        auto result = orchestrator.ExecuteInitialLoad(server.Url("/odp/Entity"));

        REQUIRE(result.preference_applied);
    }
}

TEST_CASE("ODP preference validation survives a 202 re-poll", "[odp_preference][odp_retry][odp_e2e]") {
    // The headers that matter are those of the response that actually carries the data, not those
    // of the intermediate 202. Pre-fix failure: there was no re-poll, so the 202 itself was the
    // final response and preference_applied was inferred from its empty body as false.
    ODataTestServer server;

    auto accepted = CannedResponse::Json("{}", 202);
    accepted.WithHeader("Retry-After", "1");

    auto ready = CannedResponse::Json(OdpDeltaBody("TOKEN_AFTER_WAIT"));
    ready.WithHeader("Preference-Applied", "odata.track-changes");

    server.OnPathSequence("/odp/Entity", {accepted, ready});

    OdpRequestOrchestrator orchestrator(nullptr, 100);
    std::vector<std::chrono::milliseconds> sleeps;
    RecordSleepsInto(orchestrator, sleeps);

    auto result = orchestrator.ExecuteInitialLoad(server.Url("/odp/Entity"));

    REQUIRE(result.preference_applied);
    REQUIRE(result.extracted_delta_token == "TOKEN_AFTER_WAIT");
    REQUIRE(sleeps.size() == 1);
}

TEST_CASE("ODP delta fetch does not claim change tracking", "[odp_preference][odp_e2e]") {
    // A delta fetch does not send `Prefer: odata.track-changes` - tracking is already established -
    // so preference_applied stays false and is not consulted for that path.
    ODataTestServer server;
    server.OnPath("/odp/Entity", CannedResponse::Json(OdpDeltaBody("TOKEN_NEXT")));

    OdpRequestOrchestrator orchestrator(nullptr, 100);
    auto result = orchestrator.ExecuteDeltaFetch(server.Url("/odp/Entity"), "TOKEN_PREV");

    REQUIRE(result.extracted_delta_token == "TOKEN_NEXT");
    REQUIRE_FALSE(result.preference_applied);

    // The delta token really was put on the wire.
    const auto requests = server.RequestsFor("/odp/Entity");
    REQUIRE(requests.size() == 1);
    REQUIRE(requests.front().target.find("TOKEN_PREV") != std::string::npos);
}
