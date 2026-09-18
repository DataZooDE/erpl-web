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

// Returns `value` with its leading http:// or https:// removed, so the caller can take the
// hostname off the front. Throws when `value` is not an absolute http(s) URL: callers used
// to assume "not https, therefore http" and chop seven characters off whatever they were
// given, which silently mangles an uppercase scheme or a bare name into a wrong host.
// Shares its scheme test with LooksLikeAbsoluteHttpUrl so predicate and strip cannot drift.
// True when `url` can be put on the wire as-is.
//
// A URL goes into the REQUEST LINE, which httplib writes verbatim: `s += path` then
// `" HTTP/1.1\r\n"`, with no validation - unlike header values, which it does validate.
// HttpUrl's parser captures path and query as [^?#]* and \?[^#]*, both of which match CR
// and LF, so a service-supplied link like ".../x\r\nX-Foo: bar" parses cleanly, keeps its
// host (so any same-origin check passes), and then injects a header. Space is rejected for
// the same reason: it terminates the path in the request line.
//
// This is deliberately enforced INSIDE the functions that follow service-supplied links,
// not at their call sites. The call-site version of this check was added to one reader and
// missed every sibling - the fourth time in this codebase that a guard applied per-call-site
// was forgotten somewhere.
bool IsWireSafeUrl(const std::string &url);

// Control characters only - no space check.
//
// For a URL the CALLER supplied. A raw space there is our own doing: the predicate
// pushdown decodes query values on parse and re-emits them unencoded, so a user's
// "%20" arrives here as a literal space. Refusing it turned a long-standing mangling
// into a hard failure on URLs that had always worked. Control characters are a
// different matter - they are an injection vector, never a legitimate value - so
// those are still refused.
bool HasNoControlCharacters(const std::string &url);

// A URL rendered safe to put in an exception message, a trace or a terminal: control
// characters replaced, length capped. Service-supplied URLs reach messages and logs, and
// echoing their raw bytes hands control characters to whatever reads them.
std::string SummariseUrlForMessage(const std::string &url);

// The bytes a request line carries for `url`: scheme, host, port, path and query - the
// fragment excluded, because it is never sent. Guards check THIS rather than ToString(),
// and having one definition of it stops a guard being narrowed to the path alone, which
// happened twice and left the host unchecked.
std::string WireTargetOf(const HttpUrl &url);

std::string StripHttpScheme(const std::string &value);

// The host of an absolute http(s) URL, without scheme, port-path or trailing path. Throws
// on input that is not an absolute http(s) URL.
std::string HostOfAbsoluteHttpUrl(const std::string &value);



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

    // Percent-encode one query option's value on its way into a request line, per option.
    //
    // Both places that rebuild an OData query -- the predicate pushdown and
    // ODataClientFactory::ProbeUrl -- decode values when they parse them, so both must
    // encode on the way out. They used to re-emit raw, which turned a caller's %20 into a
    // literal space; a space ends the request target, and OData paths set url_encode =
    // false so httplib never re-encodes it. Sharing one function is what keeps the two
    // sites from drifting, which is how one of them stayed broken. See GitHub #227.
    static std::string encodeQueryValueForOption(const std::string &key, const std::string &value);
};


// Escapes a value for use inside an OData string literal.
//
// OData escapes a single quote by doubling it, in both V2 and V4
// (ABNF: SQUOTE-in-string = SQUOTE SQUOTE). Without it, a value containing a quote does not
// fail - it ENDS the literal early and the rest is parsed as filter syntax, so
// `name eq 'x'' or name ne ''zz'` returns a different asset than the one asked for. A wrong
// answer, not an error.
//
// One definition, because there were three: GraphClient had a static, the predicate
// pushdown had a file-local copy, and the Datasphere URL builders had none at all - which
// is where GitHub #246 was found.
std::string EscapeODataStringLiteral(const std::string &value);

} // namespace erpl_web


