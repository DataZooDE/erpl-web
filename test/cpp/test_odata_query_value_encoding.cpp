#include "catch.hpp"

#include "odata_url_helpers.hpp"

#include <string>

using erpl_web::ODataUrlCodec;

// One encoder, applied once, on the way into the request line.
//
// Both places that rebuild an OData query -- the predicate pushdown and
// ODataClientFactory::ProbeUrl -- percent-DECODE values when they parse them. They used to
// re-emit those values raw, so a caller's "%20" came back out as a literal space. That
// matters more on OData paths than elsewhere: every OData request sets url_encode = false,
// which installs the verbatim target writer, so httplib never re-encodes it and the space
// ends the request target. See GitHub #227.
//
// The rules are per option because OData puts structure inside some values. These cases pin
// which characters are structure and which are data.

TEST_CASE("an ordinary query value is encoded where it must be, and left alone where it may be",
          "[odata_query_encoding]") {
    SECTION("a space becomes %20 - this is the whole bug") {
        REQUIRE(ODataUrlCodec::encodeQueryValueForOption("$orderby", "Name desc") == "Name%20desc");
    }

    SECTION("commas and colons survive, because $select and $orderby are built from them") {
        REQUIRE(ODataUrlCodec::encodeQueryValueForOption("$select", "Name,Price,Category") ==
                "Name,Price,Category");
    }

    SECTION("characters that would end the value are escaped") {
        // '&' would start the next parameter, '#' would start a fragment.
        REQUIRE(ODataUrlCodec::encodeQueryValueForOption("$select", "A&B") == "A%26B");
        REQUIRE(ODataUrlCodec::encodeQueryValueForOption("$select", "A#B") == "A%23B");
    }

    SECTION("a literal percent is escaped, because the value is plaintext by then") {
        // The value has already been decoded, so a '%' here is data. Emitting it raw would
        // make the service read it as the start of an escape that is not there.
        REQUIRE(ODataUrlCodec::encodeQueryValueForOption("$select", "100%") == "100%25");
    }

    SECTION("a plain value is untouched") {
        REQUIRE(ODataUrlCodec::encodeQueryValueForOption("$top", "10") == "10");
        REQUIRE(ODataUrlCodec::encodeQueryValueForOption("$format", "json") == "json");
    }

    SECTION("an empty value stays empty rather than becoming anything") {
        REQUIRE(ODataUrlCodec::encodeQueryValueForOption("$select", "").empty());
    }
}

TEST_CASE("$filter is encoded as an expression", "[odata_query_encoding]") {
    // Strict services want the spaces and quotes of a filter expression escaped.
    const auto encoded = ODataUrlCodec::encodeQueryValueForOption("$filter", "Name eq 'Chai'");
    REQUIRE(encoded.find(' ') == std::string::npos);
    REQUIRE(encoded.find('\'') == std::string::npos);
    REQUIRE(ODataUrlCodec::decodeQueryValue(encoded) == "Name eq 'Chai'");
}

TEST_CASE("$expand keeps its structure and encodes only its nested values",
          "[odata_query_encoding]") {
    // This is why the encoder is per-option rather than one blanket call. An $expand value
    // carries nested option sections, and their parentheses, commas, semicolons and '$' are
    // structure. Encoding those mangles the clause -- which is exactly what happened when a
    // blanket encodeFilterExpression was tried, and why GitHub #227 said the fix had to be
    // per option.
    const std::string clause = "Products($filter=DiscontinuedDate eq null),Category($select=Name)";
    const auto encoded = ODataUrlCodec::encodeQueryValueForOption("$expand", clause);

    INFO("encoded: " << encoded);

    // Structure survives as itself.
    REQUIRE(encoded.find("Products(") != std::string::npos);
    REQUIRE(encoded.find("),Category(") != std::string::npos);
    REQUIRE(encoded.find("$select=Name") != std::string::npos);

    // The nested filter's spaces do not.
    REQUIRE(encoded.find("DiscontinuedDate%20eq%20null") != std::string::npos);
    REQUIRE(encoded.find(' ') == std::string::npos);
}

TEST_CASE("encoding a value twice is not the same as encoding it once", "[odata_query_encoding]") {
    // The reason every value in the parameter map is held as decoded plaintext. When a
    // value was encoded by whoever produced it AND again on the way out, a space became
    // %2520 rather than %20 -- and the workarounds for that ("keep the existing $filter")
    // then dropped pushed-down filters instead of fixing the cause.
    const std::string once = ODataUrlCodec::encodeQueryValueForOption("$orderby", "Name desc");
    const std::string twice = ODataUrlCodec::encodeQueryValueForOption("$orderby", once);

    REQUIRE(once == "Name%20desc");
    REQUIRE(twice == "Name%2520desc");
    REQUIRE(once != twice);

    // Only the single encoding round-trips back to what the caller wrote.
    REQUIRE(ODataUrlCodec::decodeQueryValue(once) == "Name desc");
    REQUIRE(ODataUrlCodec::decodeQueryValue(twice) != "Name desc");
}

// ---------------------------------------------------------------------------
// The second site, driven for real.
//
// ODataClientFactory::ProbeUrl rebuilds the query itself and never goes through the
// predicate pushdown, so it needs the same encoder. Emitting the original bytes preserved
// an ALREADY-correct %20 by accident, which is why the first version of this test passed
// against both the fixed and the unfixed code and proved nothing. What actually differs is
// an UNENCODED space in the caller's URL: emitted raw it ends the request target and the
// service answers 400; encoded on the way out it arrives intact.

#include "odata_client.hpp"
#include "odata_test_server.hpp"

using namespace erpl_web::test_support;

TEST_CASE("ProbeUrl encodes a caller's query value on the way out", "[odata_query_encoding][probe]") {
    ODataTestServer server;

    const std::string context = server.Url("/svc/$metadata") + "#Airlines";
    server.ServeMetadataFixture("/svc/$metadata", "edm_trippin.xml");
    const std::string airline = R"({"AirlineCode":"AA","Name":"American Airlines"})";
    server.OnPath("/svc/Airlines",
                  CannedResponse::Json(MakeV4Page(context, std::vector<std::string>{airline})));

    auto auth_params = std::make_shared<erpl_web::HttpAuthParams>();
    // A caller's correctly-encoded space. ProbeUrl decodes it while inspecting the query,
    // and must put it back encoded rather than emitting the literal space.
    // A RAW space, not an encoded one. An already-encoded value survives either way -
    // ProbeUrl used to emit the original bytes, which preserved a correct %20 by accident -
    // so only an unencoded value distinguishes emitting raw from encoding on the way out.
    (void)erpl_web::ODataClientFactory::ProbeUrl(server.Url("/svc/Airlines") + "?$orderby=Name desc",
                                                 auth_params, std::nullopt);

    const auto requests = server.RequestsFor("/svc/Airlines");
    REQUIRE_FALSE(requests.empty());  // otherwise the assertions below prove nothing

    for (const auto &request : requests) {
        INFO("target: " << request.target);
        REQUIRE(request.QueryParam("$orderby") == "Name%20desc");
        REQUIRE(request.DecodedQueryParam("$orderby") == "Name desc");
        REQUIRE(request.target.find("Name desc") == std::string::npos);
    }
}
