#include "catch.hpp"

#include "datasphere_client.hpp"
#include "odata_url_helpers.hpp"
#include "sac_url_builder.hpp"

#include <string>

using namespace erpl_web;

// A quote in a caller-supplied id must stay DATA, not become syntax.
//
// OData escapes a single quote inside a string literal by doubling it (ABNF:
// SQUOTE-in-string = SQUOTE SQUOTE). Without that, a value containing a quote does not
// fail - it ends the literal early, and everything after it is parsed as filter or path
// syntax. So this:
//
//     datasphere_describe_asset('SP', "x' or name ne 'zz")
//
// produced  $filter=name eq 'x' or name ne 'zz' and spaceName eq 'SP'  - a filter the
// caller never wrote, returning a different asset. A wrong answer, not an error, which is
// the worst shape a defect can take.
//
// The rule existed twice already (a GraphClient static and a file-local copy in the
// predicate pushdown) and was missing at the Datasphere and SAC URL builders. There is one
// definition now. See GitHub #243.

TEST_CASE("the escaper doubles quotes and leaves everything else alone",
          "[odata][literal_escaping]") {
    REQUIRE(EscapeODataStringLiteral("plain") == "plain");
    REQUIRE(EscapeODataStringLiteral("O'Brien") == "O''Brien");
    REQUIRE(EscapeODataStringLiteral("''") == "''''");
    REQUIRE(EscapeODataStringLiteral("") == "");

    // Not this function's job: spaces, ampersands and percent signs are handled by the
    // query-value encoder. Doubling quotes must not disturb them.
    REQUIRE(EscapeODataStringLiteral("a b&c%20d") == "a b&c%20d");
}

TEST_CASE("an injected quote cannot escape a Datasphere $filter literal",
          "[odata][literal_escaping][security]") {
    // The payload from the report.
    const std::string hostile = "x' or name ne 'zz";

    SECTION("the asset filter") {
        const auto url = DatasphereUrlBuilder::BuildCatalogAssetFilteredUrl("tenant", "eu10", "SP",
                                                                           hostile);
        INFO(url);
        // The payload survives as one literal, with its quote doubled.
        REQUIRE(url.find("name eq 'x'' or name ne ''zz'") != std::string::npos);
        // And the operator it was trying to smuggle in never appears as syntax: after
        // escaping there is exactly one ` or ` inside the literal, never a bare one joining
        // two complete comparisons.
        REQUIRE(url.find("eq 'x' or name ne 'zz'") == std::string::npos);
    }

    SECTION("the space filter, where the space id is the operand") {
        const auto url = DatasphereUrlBuilder::BuildSpaceFilteredUrl("tenant", "eu10", hostile);
        INFO(url);
        REQUIRE(url.find("name eq 'x'' or name ne ''zz'") != std::string::npos);
    }

    SECTION("the assets-in-space filter") {
        const auto url = DatasphereUrlBuilder::BuildCatalogAssetsFilteredUrl("tenant", "eu10",
                                                                            hostile);
        INFO(url);
        REQUIRE(url.find("spaceName eq 'x'' or name ne ''zz'") != std::string::npos);
    }

    SECTION("both operands are escaped, not just the first") {
        // Escaping one and forgetting the other would leave the other as the way in - which
        // is the shape this codebase has repeatedly shipped.
        const auto url = DatasphereUrlBuilder::BuildCatalogAssetFilteredUrl("tenant", "eu10",
                                                                            hostile, "ASSET");
        INFO(url);
        REQUIRE(url.find("spaceName eq 'x'' or name ne ''zz'") != std::string::npos);
    }
}

TEST_CASE("an injected quote cannot escape a SAC key predicate",
          "[odata][literal_escaping][security]") {
    // A key predicate is a string literal too, so it has the same hole. A story genuinely
    // named O'Brien must be addressable, and an id crafted to break out must not be.
    const auto url = SacUrlBuilder::BuildModelServiceUrl("acme", "eu10", "M1') or true or ('");
    INFO(url);

    REQUIRE(url.find("''") != std::string::npos);
    // The closing sequence of the ORIGINAL predicate must not appear unescaped mid-value.
    REQUIRE(url.find("M1') or true or ('")  == std::string::npos);
}
