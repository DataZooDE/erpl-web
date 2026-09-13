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
