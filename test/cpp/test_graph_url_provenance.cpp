#include "catch.hpp"

#include "graph_client.hpp"

#include <string>
#include <type_traits>

using namespace erpl_web;

// The credential-attaching entry points must not accept a bare std::string.
//
// Every URL that reaches GraphClient::Get is sent with the caller's bearer token attached, so
// the one thing that must never happen is a URL out of a response body arriving there. That was
// previously a matter of remembering, and it was forgotten eight times: @odata.nextLink,
// statusMonitorResource, resourceLocation, @odata.context, the ODP __next and its 202 Location,
// the ODP __delta link, and the redirect follower. A response-derived URL is always a
// std::string, so refusing that conversion is what turns the convention into a barrier.
//
// These are compile-time assertions on purpose: the failure they guard against is code that
// compiles, not code that misbehaves at runtime. If someone drops `explicit` from
// ExtensionBuiltUrl, or adds a std::string overload of Get, this file stops building.

static_assert(!std::is_convertible<std::string, ExtensionBuiltUrl>::value,
              "ExtensionBuiltUrl must not be implicitly constructible from std::string - the "
              "explicit constructor is what forces a call site to state where its URL came from");

static_assert(!std::is_convertible<const char *, ExtensionBuiltUrl>::value,
              "a string literal must not convert implicitly either");

static_assert(!std::is_invocable<decltype(&GraphClient::Get), GraphClient &, const std::string &>::value,
              "GraphClient::Get must not be callable with a std::string - that is the seam every "
              "service-supplied URL would otherwise slip through");

static_assert(!std::is_invocable<decltype(&GraphClient::GetAllPagesMerged), GraphClient &, const std::string &>::value,
              "GraphClient::GetAllPagesMerged seeds its own trusted origin from its argument, so it "
              "must likewise refuse a URL it cannot vouch for");

static_assert(std::is_invocable<decltype(&GraphClient::Get), GraphClient &, const ExtensionBuiltUrl &>::value,
              "the guarded path must still be usable");

// The escape hatch stays available and stays explicit about needing an origin to check against.
static_assert(std::is_invocable<decltype(&GraphClient::GetServerSuppliedUrl), GraphClient &,
                                const std::string &, const std::string &>::value,
              "a service-supplied URL must still have a way through - origin-checked, by name");

TEST_CASE("ExtensionBuiltUrl carries its value unchanged", "[graph][url_provenance]") {
    const std::string url = "https://graph.microsoft.com/v1.0/users?$top=5";
    const ExtensionBuiltUrl built{url};
    REQUIRE(built.Value() == url);
}
