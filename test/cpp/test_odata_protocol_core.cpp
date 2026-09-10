// Tests for the shared OData protocol core: delta-link extraction, key-predicate
// formatting and the EdmCache. See GitHub #102, #104 and #106.

#include "catch.hpp"
#include "duckdb.hpp"

#include "odata_client.hpp"
#include "odata_edm.hpp"
#include "odata_test_server.hpp"
#include "odata_url_helpers.hpp"

#include <atomic>
#include <chrono>
#include <map>
#include <string>
#include <thread>
#include <vector>

using erpl_web::test_support::CannedResponse;
using erpl_web::test_support::ODataTestServer;

namespace {

// The EdmCache is a process-global singleton, so every test that touches it has to
// hand it back exactly as it found it or it will poison its neighbours.
class EdmCacheGuard {
public:
    EdmCacheGuard() : saved_lifetime(erpl_web::EdmCache::GetInstance().GetEntryLifetime())
    {
        erpl_web::EdmCache::GetInstance().Clear();
    }

    ~EdmCacheGuard()
    {
        erpl_web::EdmCache::GetInstance().SetEntryLifetime(saved_lifetime);
        erpl_web::EdmCache::GetInstance().Clear();
    }

    EdmCacheGuard(const EdmCacheGuard &) = delete;
    EdmCacheGuard &operator=(const EdmCacheGuard &) = delete;

private:
    std::chrono::seconds saved_lifetime;
};

// A minimal but complete v4 EDMX: enough for FindEntitySets()/FindType() and small
// enough that the tests below stay readable.
std::string MinimalEdmx(const char *odata_version_attribute, const char *entity_set_name)
{
    return std::string(R"(<?xml version="1.0" encoding="utf-8"?>)") +
           R"(<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version=")" +
           odata_version_attribute + R"(">)" +
           R"(<edmx:DataServices>)" +
           R"(<Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="Test">)" +
           R"(<EntityType Name="Thing"><Key><PropertyRef Name="Id"/></Key>)" +
           R"(<Property Name="Id" Type="Edm.String"/></EntityType>)" +
           R"(<EntityContainer Name="Container"><EntitySet Name=")" + entity_set_name +
           R"(" EntityType="Test.Thing"/></EntityContainer>)" +
           R"(</Schema></edmx:DataServices></edmx:Edmx>)";
}

}  // namespace

// ============================================================================
// GitHub #102 -- one delta-link parser, and it understands the __next form
// ============================================================================

TEST_CASE("Delta link is read from every form a service uses", "[odata_delta_link]")
{
    using erpl_web::ODataDeltaLink;

    SECTION("OData v4 @odata.deltaLink")
    {
        const std::string body =
            R"({"@odata.context":"http://svc/$metadata#Things","value":[],)"
            R"("@odata.deltaLink":"http://svc/Things?$deltatoken=v4token"})";

        REQUIRE(ODataDeltaLink::ExtractDeltaLink(body) == "http://svc/Things?$deltatoken=v4token");
        REQUIRE(ODataDeltaLink::ExtractDeltaToken(body) == "v4token");
    }

    SECTION("OData v2 __delta inside the d wrapper")
    {
        const std::string body =
            R"({"d":{"results":[],"__delta":"http://svc/Things?$format=json&!deltatoken=D2024"}})";

        REQUIRE(ODataDeltaLink::ExtractDeltaToken(body) == "D2024");
    }

    SECTION("SAP Gateway quotes the token")
    {
        const std::string body =
            R"({"d":{"results":[],"__delta":"http://svc/Things?!deltatoken='D2024'"}})";

        REQUIRE(ODataDeltaLink::ExtractDeltaToken(body) == "D2024");
    }

    // The regression this whole exercise is about. SAP ODP delivers the change-tracking
    // token on the terminal page as a paging link carrying the "!deltatoken=" sigil, not
    // as a "__delta" property. Both parsers that existed before #102 looked only at
    // "__delta", returned no token, and so left the ODP scan in initial-load mode -- the
    // next read then re-extracted the whole entity set instead of fetching deltas.
    SECTION("__next carrying !deltatoken is a delta link")
    {
        const std::string body =
            R"({"d":{"results":[{"Id":"1"}],)"
            R"("__next":"http://svc/Things?$format=json&!deltatoken=D20240101120000"}})";

        REQUIRE(ODataDeltaLink::ExtractDeltaLink(body) ==
                "http://svc/Things?$format=json&!deltatoken=D20240101120000");
        REQUIRE(ODataDeltaLink::ExtractDeltaToken(body) == "D20240101120000");
    }

    SECTION("the $deltatoken sigil identifies a link just as well")
    {
        REQUIRE(ODataDeltaLink::IsDeltaLink("http://svc/Things?$deltatoken=v4next"));
        REQUIRE(ODataDeltaLink::IsDeltaLink("http://svc/Things?!deltatoken=D1"));
        REQUIRE_FALSE(ODataDeltaLink::IsDeltaLink("http://svc/Things?$skiptoken=100"));
    }

    // ... but an ordinary paging link must NOT be mistaken for one, or every
    // intermediate page would look like the end of a change-tracked extraction.
    SECTION("an ordinary __next is not a delta link")
    {
        const std::string body =
            R"({"d":{"results":[{"Id":"1"}],)"
            R"("__next":"http://svc/Things?$format=json&$skiptoken=100"}})";

        REQUIRE(ODataDeltaLink::ExtractDeltaLink(body).empty());
        REQUIRE(ODataDeltaLink::ExtractDeltaToken(body).empty());
    }

    SECTION("no delta information at all")
    {
        REQUIRE(ODataDeltaLink::ExtractDeltaLink(R"({"d":{"results":[]}})").empty());
        REQUIRE(ODataDeltaLink::ExtractDeltaLink("not json").empty());
        REQUIRE(ODataDeltaLink::ExtractDeltaLink("").empty());
    }

    SECTION("token ends at the next query separator, not at the end of the URL")
    {
        REQUIRE(ODataDeltaLink::ExtractToken("http://svc/Set?!deltatoken=D1&$format=json") == "D1");
        REQUIRE(ODataDeltaLink::ExtractToken("http://svc/Set?$deltatoken=D2#frag") == "D2");
        REQUIRE(ODataDeltaLink::ExtractToken("http://svc/Set?$skiptoken=100").empty());
    }
}

// ============================================================================
// GitHub #104 -- key predicate formatting
// ============================================================================

TEST_CASE("Input parameters are escaped as OData string literals", "[odata_url_helpers]")
{
    erpl_web::InputParametersFormatter formatter;
    const erpl_web::HttpUrl base("http://svc/Service/Report");

    SECTION("a single quote in a value is doubled, not emitted raw")
    {
        // OData escapes ' inside a literal by doubling it. Emitting the raw quote closed
        // the literal early and let the value inject further OData syntax into the URL.
        const std::map<std::string, std::string> params{{"Customer", "O'Brien"}};
        const auto result = formatter.addParams(base, params).ToString();

        REQUIRE(result.find("(Customer='O''Brien')") != std::string::npos);
        REQUIRE(result.find("(Customer='O'Brien')") == std::string::npos);
    }

    SECTION("an injected clause stays inside the literal")
    {
        const std::map<std::string, std::string> params{{"Customer", "x')/Other('y"}};
        const auto result = formatter.addParams(base, params).ToString();

        REQUIRE(result.find("(Customer='x'')/Other(''y')") != std::string::npos);
    }

    SECTION("an empty value is an empty string literal, not a bare key")
    {
        const std::map<std::string, std::string> params{{"Customer", ""}};
        const auto result = formatter.addParams(base, params).ToString();

        REQUIRE(result.find("(Customer='')") != std::string::npos);
    }

    SECTION("a lone sign is a string, not a number")
    {
        const std::map<std::string, std::string> params{{"Customer", "-"}};
        const auto result = formatter.addParams(base, params).ToString();

        REQUIRE(result.find("(Customer='-')") != std::string::npos);
    }

    SECTION("numbers and ISO dates stay unquoted")
    {
        REQUIRE(formatter.addParams(base, {{"Year", "2024"}}).ToString().find("(Year=2024)") !=
                std::string::npos);
        REQUIRE(formatter.addParams(base, {{"Rate", "1.25"}}).ToString().find("(Rate=1.25)") !=
                std::string::npos);
        REQUIRE(formatter.addParams(base, {{"Day", "2024-01-31"}}).ToString().find("(Day=2024-01-31)") !=
                std::string::npos);
    }
}

// ============================================================================
// GitHub #104 -- EdmCache hands out snapshots, not pointers into its own map
// ============================================================================

TEST_CASE("EdmCache entries survive a concurrent overwrite", "[edm_cache]")
{
    EdmCacheGuard guard;
    auto &cache = erpl_web::EdmCache::GetInstance();

    const std::string url = "http://svc/Service/$metadata";
    cache.Set(url, erpl_web::Edmx::FromXmlV2(MinimalEdmx("1.0", "Things")));

    auto snapshot = cache.Get(url);
    REQUIRE(snapshot != nullptr);
    REQUIRE(snapshot->GetVersion() == erpl_web::ODataVersion::V2);

    // Another connection re-reads $metadata for the same service and stores the result.
    // The previous API handed out a pointer into the map slot this assignment overwrites,
    // so the reader above silently started seeing the new document -- or, once the entry
    // was erased rather than assigned, a destroyed one.
    cache.Set(url, erpl_web::Edmx::FromXmlV4(MinimalEdmx("4.0", "Things")));

    REQUIRE(snapshot->GetVersion() == erpl_web::ODataVersion::V2);
    REQUIRE(snapshot->FindEntitySets().size() == 1);

    auto fresh = cache.Get(url);
    REQUIRE(fresh != nullptr);
    REQUIRE(fresh->GetVersion() == erpl_web::ODataVersion::V4);
}

TEST_CASE("EdmCache entries survive invalidation while in use", "[edm_cache]")
{
    EdmCacheGuard guard;
    auto &cache = erpl_web::EdmCache::GetInstance();

    const std::string url = "http://svc/Service/$metadata";
    cache.Set(url, erpl_web::Edmx::FromXmlV2(MinimalEdmx("1.0", "Things")));

    auto snapshot = cache.Get(url);
    REQUIRE(snapshot != nullptr);

    cache.Invalidate(url);
    REQUIRE(cache.Get(url) == nullptr);

    // Erasing the map entry used to destroy the Edmx a scan was still reading.
    REQUIRE(snapshot->FindEntitySets().size() == 1);
    REQUIRE(snapshot->FindEntitySets()[0].name == "Things");
}

TEST_CASE("EdmCache expires entries", "[edm_cache]")
{
    EdmCacheGuard guard;
    auto &cache = erpl_web::EdmCache::GetInstance();

    const std::string url = "http://svc/Service/$metadata";

    // A zero lifetime expires on the next lookup, which makes the sweep testable without
    // sleeping. This is also what keeps the process-global map from growing without bound.
    cache.SetEntryLifetime(std::chrono::seconds(0));
    cache.Set(url, erpl_web::Edmx::FromXmlV4(MinimalEdmx("4.0", "Things")));
    REQUIRE(cache.Get(url) == nullptr);
    REQUIRE(cache.Size() == 0);

    // A negative lifetime disables expiry entirely.
    cache.SetEntryLifetime(std::chrono::seconds(-1));
    cache.Set(url, erpl_web::Edmx::FromXmlV4(MinimalEdmx("4.0", "Things")));
    REQUIRE(cache.Get(url) != nullptr);

    // The shipped default is finite, so a redeployed service is eventually noticed.
    REQUIRE(erpl_web::EdmCache::DEFAULT_ENTRY_LIFETIME_SECONDS > 0);
}

// Not a deterministic reproduction of the race -- it cannot be made one without a hook
// inside the cache -- but under AddressSanitizer a reader holding a borrowed pointer
// across a concurrent overwrite reports a use-after-free here with high probability,
// and the test can never fail once entries are handed out as shared snapshots.
TEST_CASE("EdmCache tolerates concurrent readers and writers", "[edm_cache]")
{
    EdmCacheGuard guard;
    auto &cache = erpl_web::EdmCache::GetInstance();

    const std::string url = "http://svc/Service/$metadata";
    cache.Set(url, erpl_web::Edmx::FromXmlV2(MinimalEdmx("1.0", "Things")));

    constexpr int ITERATIONS = 200;
    std::atomic<bool> reader_saw_a_valid_document{true};
    std::atomic<int> ready{0};

    std::thread writer([&]() {
        ready++;
        while (ready.load() < 2) {
        }
        for (int i = 0; i < ITERATIONS; i++) {
            cache.Set(url, erpl_web::Edmx::FromXmlV4(MinimalEdmx("4.0", "Things")));
        }
    });

    std::thread reader([&]() {
        ready++;
        while (ready.load() < 2) {
        }
        for (int i = 0; i < ITERATIONS; i++) {
            auto snapshot = cache.Get(url);
            if (!snapshot) {
                continue;
            }
            // Walking the document while the writer replaces the entry is the whole point.
            if (snapshot->FindEntitySets().size() != 1) {
                reader_saw_a_valid_document = false;
            }
        }
    });

    writer.join();
    reader.join();

    REQUIRE(reader_saw_a_valid_document.load());
}

// ============================================================================
// GitHub #106 -- $metadata is fetched once per service, not once per consumer
// ============================================================================

TEST_CASE("Metadata is fetched once and shared across clients", "[edm_cache][odata_e2e]")
{
    EdmCacheGuard guard;

    ODataTestServer server;
    server.ServeMetadata("/svc/$metadata", MinimalEdmx("4.0", "Things"));
    server.OnPath("/svc/", CannedResponse::Json(
                               std::string(R"({"@odata.context":")") + server.Url("/svc/$metadata") +
                               R"(","value":[{"name":"Things","kind":"EntitySet","url":"Things"}]})"));

    const erpl_web::HttpUrl service_url(server.Url("/svc/"));

    auto first = erpl_web::ODataServiceClient(erpl_web::CreateODataHttpClient(), service_url);
    auto first_metadata = first.GetMetadata();
    REQUIRE(first_metadata.FindEntitySets().size() == 1);

    const auto after_first = server.RequestsFor("/svc/$metadata").size();
    REQUIRE(after_first >= 1);

    // A second consumer of the same service must reuse the cached document. Each attached
    // catalog used to hold its own full copy and, worse, each client re-fetched it whenever
    // the cache had been reset.
    auto second = erpl_web::ODataServiceClient(erpl_web::CreateODataHttpClient(), service_url);
    auto second_metadata = second.GetMetadata();
    REQUIRE(second_metadata.FindEntitySets().size() == 1);

    REQUIRE(server.RequestsFor("/svc/$metadata").size() == after_first);

    // Exactly one document is held for the service, not one per consumer.
    REQUIRE(erpl_web::EdmCache::GetInstance().Size() == 1);
}
