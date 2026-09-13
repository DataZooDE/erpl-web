// GitHub #208: the async workbook-session path polled its status monitor with no delay,
// no time budget and no test.
//
// The loops said "Poll up to 30 times with 1-second intervals" and then slept for none of
// them, so 30 requests went out in a few milliseconds and the loop gave up long before a
// long-running session could become ready - the async branch could not succeed in practice.
// Being untested is also how the credential leak on this path (fixed in #205) survived
// three review rounds.
//
// PollForSessionId now bounds the wait by TIME with an injectable clock and sleeper, so
// these tests drive delayed readiness and budget expiry without any wall-clock waiting.

#include "catch.hpp"
#include "duckdb.hpp"

#include "graph_client.hpp"
#include "graph_excel_client.hpp"

#include <chrono>
#include <string>
#include <vector>

using namespace std::chrono_literals;

namespace {

// A clock the test advances by hand. sleep_for moves it; nothing ever really sleeps.
struct FakeClock {
    std::chrono::steady_clock::time_point current = std::chrono::steady_clock::time_point{};
    std::vector<std::chrono::milliseconds> slept;

    erpl_web::SessionPollPolicy Policy(std::chrono::milliseconds interval,
                                       std::chrono::milliseconds budget)
    {
        erpl_web::SessionPollPolicy policy;
        policy.interval = interval;
        policy.budget = budget;
        policy.now = [this] { return current; };
        policy.sleep_for = [this](std::chrono::milliseconds duration) {
            slept.push_back(duration);
            current += duration;
        };
        return policy;
    }
};

std::string IdFromBody(const std::string &body) { return body; }

}  // namespace

TEST_CASE("PollForSessionId returns as soon as the session is ready", "[graph_excel][poll]") {
    FakeClock clock;
    int fetches = 0;

    const auto id = erpl_web::PollForSessionId(
        [&] { fetches++; return std::string("session-1"); }, IdFromBody,
        clock.Policy(1s, 30s));

    REQUIRE(id == "session-1");
    REQUIRE(fetches == 1);
    // Ready on the first look means no waiting at all.
    REQUIRE(clock.slept.empty());
}

TEST_CASE("PollForSessionId keeps waiting while the session is not ready yet",
          "[graph_excel][poll]") {
    FakeClock clock;
    int fetches = 0;

    // Ready on the fourth look - the shape the old loop could never reach, because it
    // never waited between looks.
    const auto id = erpl_web::PollForSessionId(
        [&] {
            fetches++;
            return fetches < 4 ? std::string() : std::string("session-2");
        },
        IdFromBody, clock.Policy(1s, 30s));

    REQUIRE(id == "session-2");
    REQUIRE(fetches == 4);
    // Three waits, one between each pair of attempts, each the configured interval.
    REQUIRE(clock.slept.size() == 3);
    for (const auto &waited : clock.slept) {
        REQUIRE(waited == 1s);
    }
}

TEST_CASE("PollForSessionId gives up when the time budget is spent", "[graph_excel][poll]") {
    FakeClock clock;
    int fetches = 0;

    const auto id = erpl_web::PollForSessionId(
        [&] { fetches++; return std::string(); }, IdFromBody, clock.Policy(1s, 5s));

    REQUIRE(id.empty());
    // The budget bounds the WAITING, not the attempt count: five one-second waits, and one
    // attempt after each, plus the immediate first.
    REQUIRE(fetches == 6);
    REQUIRE(clock.slept.size() == 5);
}

TEST_CASE("PollForSessionId honours a shorter interval", "[graph_excel][poll]") {
    FakeClock clock;
    int fetches = 0;

    const auto id = erpl_web::PollForSessionId(
        [&] { fetches++; return std::string(); }, IdFromBody, clock.Policy(250ms, 1s));

    REQUIRE(id.empty());
    REQUIRE(clock.slept.size() == 4);
    REQUIRE(fetches == 5);
}

// The status monitor URL arrives in the createSession response body. A foreign one can
// never return our session id, so the decision is made once rather than per request.
TEST_CASE("a status monitor on a foreign origin is not trusted", "[graph_excel][poll][security]") {
    const std::string session_url =
        "https://graph.microsoft.com/v1.0/me/drive/root:/book.xlsx:/workbook/createSession";

    REQUIRE(erpl_web::GraphClient::IsServerSuppliedUrlTrusted(
        "https://graph.microsoft.com/v1.0/operations/abc", session_url));
    // Relative links resolve against the session's own origin.
    REQUIRE(erpl_web::GraphClient::IsServerSuppliedUrlTrusted("/v1.0/operations/abc", session_url));

    REQUIRE_FALSE(erpl_web::GraphClient::IsServerSuppliedUrlTrusted(
        "https://attacker.example/collect", session_url));
    REQUIRE_FALSE(erpl_web::GraphClient::IsServerSuppliedUrlTrusted(
        "http://graph.microsoft.com/v1.0/operations/abc", session_url));  // scheme downgrade
    // Fails closed: no origin, or a link that cannot be resolved.
    REQUIRE_FALSE(erpl_web::GraphClient::IsServerSuppliedUrlTrusted(
        "https://graph.microsoft.com/v1.0/operations/abc", ""));
}

// GitHub #208 follow-up: the poll's readiness predicate, driven by realistic Graph bodies
// rather than the identity function above - which by construction cannot catch a body
// whose "id" belongs to the operation instead of the session.
//
// Graph answers a status-monitor poll with an OPERATION resource. Reading "id"
// unconditionally accepted the very first poll, so the time budget never engaged and an
// operation id was then sent as workbook-session-id on every write.
TEST_CASE("the readiness predicate waits while the operation is still running",
          "[graph_excel][poll]") {
    // These carry an "id" - the operation's - and must NOT be taken as a session.
    REQUIRE(erpl_web::ExtractWorkbookSessionId(
                R"({"id":"op-123","status":"running","resourceLocation":"/x"})")
                .empty());
    REQUIRE(erpl_web::ExtractWorkbookSessionId(R"({"id":"op-123","status":"notStarted"})").empty());
    REQUIRE(erpl_web::ExtractWorkbookSessionId(R"({"id":"op-123","status":"inProgress"})").empty());
}

TEST_CASE("the readiness predicate accepts a session body", "[graph_excel][poll]") {
    // The immediate (201) shape: no status, the body IS the session.
    REQUIRE(erpl_web::ExtractWorkbookSessionId(R"({"id":"session-abc","persistChanges":true})") ==
            "session-abc");
}

// A body carrying a STATUS never yields a session id - not even on success. Microsoft's
// documented succeeded body is
//   {"id": <operationId>, "status": "succeeded", "resourceLocation": ".../sessionInfoResource(...)"}
// so its root "id" belongs to the OPERATION on success exactly as it does while running.
// An earlier version of this file asserted the opposite, which encoded the bug as the
// contract: it returned that operation id as the workbook-session-id.
TEST_CASE("a succeeded operation body yields no session id of its own",
          "[graph_excel][poll]") {
    const std::string succeeded_body =
        std::string(R"({"id":"op-1","status":"succeeded","resourceLocation":)") +
        R"("https://graph.microsoft.com/v1.0/sessionInfoResource"})";
    REQUIRE(erpl_web::ExtractWorkbookSessionId(succeeded_body).empty());
    REQUIRE(erpl_web::ExtractWorkbookSessionId(R"({"id":"op-1","status":"completed"})").empty());
}

TEST_CASE("the readiness predicate fails fast when the operation failed",
          "[graph_excel][poll]") {
    REQUIRE_THROWS_AS(erpl_web::ExtractWorkbookSessionId(R"({"id":"op-1","status":"failed"})"),
                      duckdb::IOException);
    REQUIRE_THROWS_AS(erpl_web::ExtractWorkbookSessionId(R"({"status":"cancelled"})"),
                      duckdb::IOException);
}

// An ACCEPT list, not a reject list. Listing the not-ready statuses and returning the id
// for everything else meant any status Graph adds later - or any spelling this code has not
// seen - would read as "ready" and hand back an operation id as a session id. Unknown must
// mean keep waiting.
TEST_CASE("the readiness predicate waits on a status it does not recognise",
          "[graph_excel][poll]") {
    REQUIRE(erpl_web::ExtractWorkbookSessionId(R"({"id":"op-1","status":"pending"})").empty());
    REQUIRE(erpl_web::ExtractWorkbookSessionId(R"({"id":"op-1","status":"queued"})").empty());
    REQUIRE(erpl_web::ExtractWorkbookSessionId(R"({"id":"op-1","status":"whatever-comes-next"})")
                .empty());
}

// The session id is sent in a request header. What matters is that it cannot terminate or
// split that header, so control characters are refused - and nothing else is. A first
// version of this checked the RFC 7230 *token* charset, which rejected the ';' that every
// real Graph session id contains: the test caught it, but only because a realistic id was
// among the cases. Contract tests need a real-shaped positive case, not just the negatives.
TEST_CASE("the readiness predicate refuses a session id that cannot go in a header",
          "[graph_excel][poll][security]") {
    REQUIRE_THROWS_AS(
        erpl_web::ExtractWorkbookSessionId("{\"id\":\"abc\\r\\nX-Injected: 1\"}"),
        duckdb::IOException);
    REQUIRE_THROWS_AS(erpl_web::ExtractWorkbookSessionId("{\"id\":\"abc\\tdef\"}"),
                      duckdb::IOException);

    // A realistic Graph session id survives, spaces and all.
    REQUIRE(erpl_web::ExtractWorkbookSessionId(
                R"({"id":"cluster=WEU;session=1a2b3c4d-5e6f.7g8h_9i0j"})") ==
            "cluster=WEU;session=1a2b3c4d-5e6f.7g8h_9i0j");
    REQUIRE(erpl_web::ExtractWorkbookSessionId(R"({"id":"has space"})") == "has space");
}

TEST_CASE("the readiness predicate tolerates a body it cannot use", "[graph_excel][poll]") {
    REQUIRE(erpl_web::ExtractWorkbookSessionId("not json").empty());
    REQUIRE(erpl_web::ExtractWorkbookSessionId("{}").empty());
    REQUIRE(erpl_web::ExtractWorkbookSessionId(R"({"id":123})").empty());
}

// The whole point of the predicate plus the budget, together: a session that only becomes
// ready after several polls is reached, rather than an operation id being taken on the
// first one.
TEST_CASE("a session that becomes ready after several polls is returned",
          "[graph_excel][poll]") {
    FakeClock clock;
    int polls = 0;

    const auto id = erpl_web::PollForSessionId(
        [&] {
            polls++;
            // The monitor reports the operation while it runs, then hands back a session
            // body of its own (no status). A "succeeded" body would carry the OPERATION id
            // and a resourceLocation instead - that path is resolved by
            // CreateWorkbookSession, not by this predicate.
            return polls < 3 ? std::string(R"({"id":"op-1","status":"running"})")
                             : std::string(R"({"id":"session-xyz"})");
        },
        [](const std::string &body) { return erpl_web::ExtractWorkbookSessionId(body); },
        clock.Policy(1s, 30s));

    REQUIRE(id == "session-xyz");
    REQUIRE(polls == 3);
    REQUIRE(clock.slept.size() == 2);
}
