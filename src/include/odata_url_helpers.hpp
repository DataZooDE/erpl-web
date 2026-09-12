#pragma once

#include "datazoo/oauth2/http_client.hpp"

#include <memory>

namespace erpl_web {

// Guard for the "an already-absolute URL is used verbatim" hatches that several readers
// expose so they can be pointed at a local server (Business Central's `environment`,
// Datasphere's `space_id`, Dataverse's `environment_url`).
//
// All three carry an OAuth bearer token, and all three hardcoded https before the hatch
// existed. Accepting plain http anywhere would make a testability affordance into a
// credential-in-cleartext path, and the same-origin guard would not object because the
// configured URL IS the origin. https is allowed anywhere; plain http only for loopback.
//
// `what` names the setting in the error, so the caller is told which of theirs is wrong.
// Whether a reader may point at a non-loopback https host it was not built for.
//
// This gates Business Central and Datasphere, and deliberately NOT Dataverse. The
// distinction is where the OAuth token audience comes from:
//
//   Dataverse   scope = environment_url + "/.default"  -> minted FOR the configured host,
//                                                          so a custom host is the normal
//                                                          product, not an exposure
//   Business    GetResourceUrl() hardcodes
//   Central     https://api.businesscentral.dynamics.com -> a token minted for one host
//                                                          would be sent to another
//   Datasphere  scope is a fixed 'default'/'apiaccess'  -> same concern, gated to match
//
// Gating Dataverse would make every real deployment set an unsafe flag for ordinary use,
// which devalues the flag (GitHub #199).
class ServiceUrlPolicy {
public:
    static void SetCustomServiceUrlsAllowed(bool allowed);
    static bool CustomServiceUrlsAllowed();
};

// Loopback http is always permitted; https to a non-loopback host requires the opt-in
// above. Use for readers whose token audience is fixed.
void RequireGatedServiceUrl(const std::string &url, const std::string &what);

void RequireSecureOrLoopbackUrl(const std::string &url, const std::string &what);

// Shared trigger for those same hatches: does this value already look like an absolute
// URL? Datasphere tested `find("http") == 0`, which is true of any string starting with
// those four letters ("http_archive"), so the three hatches disagreed on what they were
// even guarding while a comment claimed they shared one rule (GitHub #193).
bool LooksLikeAbsoluteHttpUrl(const std::string &value);



// The HTTP client every OData consumer needs.
//
// url_encode is off because the OData layer has already encoded what needs encoding:
// $filter expressions, $expand option groups and key predicates such as
// Customers('ALFKI') carry percent-escapes and reserved characters on purpose, and a
// second encoding pass in the transport corrupts them. This used to be spelled out
// three times, in the Business Central, Dataverse and Graph clients, plus a fourth
// spelling in datasphere_catalog.cpp that forgot the flag entirely (GitHub #102).
std::shared_ptr<HttpClient> CreateODataHttpClient();

// One place that knows how a service hands back a change-tracking (delta) link, and how
// to read the token out of it.
//
// This logic existed twice before GitHub #102, and the second copy looked only at the
// v2 "__delta" property. SAP ODP routinely delivers the token on the terminal page as a
// "__next" link carrying the "!deltatoken=" sigil instead, and that copy silently returned
// no token - which leaves the ODP scan in initial-load mode, so the next read re-extracts
// the whole entity set rather than fetching deltas.
//
// This belongs on ODataEntitySetResponse, next to NextUrl(); it lives here only because
// odata_client.* was being edited elsewhere when #102 was fixed.
class ODataDeltaLink {
public:
    // The delta link carried by an OData JSON payload, or "" when the page has none.
    // Recognised, in this order:
    //   v4  {"@odata.deltaLink": "<url>"}
    //   v2  {"d": {"__delta": "<url>"}}   (also honoured at the document root)
    //   v2  {"d": {"__next":  "<url with !deltatoken= or $deltatoken=>"}}
    // A plain "__next" without a delta sigil is ordinary server-driven paging and is not
    // a delta link.
    static std::string ExtractDeltaLink(const std::string &json_body);

    // The token inside a delta link: "!deltatoken=" (v2) or "$deltatoken=" (v4).
    // Surrounding single or double quotes, which SAP Gateway sometimes emits, are stripped.
    static std::string ExtractToken(const std::string &delta_link);

    // True when the URL carries a delta token in either dialect.
    static bool IsDeltaLink(const std::string &url);

    // Convenience: the token carried by a payload, "" when the payload has no delta link.
    static std::string ExtractDeltaToken(const std::string &json_body);

    // Read a token out of a DeltaLinksOf<EntitySet> collection response. The rows carry a
    // DeltaToken property directly rather than a link with a "!deltatoken" sigil, and an
    // initial-load row is preferred when several are present. See GitHub #169.
    static std::string ExtractTokenFromDeltaLinksPayload(const std::string &json_body);
};

class ODataUrlResolver {
public:
    std::string resolveMetadataUrl(const HttpUrl &request_url,
                                   const std::string &odata_context_if_any) const;
};

class InputParametersFormatter {
public:
    HttpUrl addParams(const HttpUrl &url,
                      const std::map<std::string, std::string> &params) const;
};

// URL encoding/decoding helpers for OData query construction.
// Always use httplib encode/decode to guarantee consistency with the HTTP client.
class ODataUrlCodec {
public:
    // Percent-encode a complete query value (e.g., filter expression) per RFC3986
    static std::string encodeQueryValue(const std::string &value);

    // Decode a query value; convert_plus_to_space=false to keep '+' literal
    static std::string decodeQueryValue(const std::string &value);

    // Ensure $format=json is present in the `HttpUrl` query
    static void ensureJsonFormat(HttpUrl &url);

    // Smart encoding for OData $filter expressions - only encodes literal values, not operators or spaces
    static std::string encodeFilterExpression(const std::string &filter_expr);

    // Normalize $expand value: ensure nested option keys have a leading '$' and retain structure
    static std::string normalizeExpand(const std::string &expand_value);

    // Normalize expand and percent-encode ONLY nested $filter values inside option sections
    static std::string normalizeAndSanitizeExpand(const std::string &expand_value);
};

} // namespace erpl_web


