#pragma once

// A test-only, in-process HTTP server that stands in for a real OData / SAP ODP
// service. It is deliberately NOT a mock of our own code: the extension talks to
// it over a real TCP socket with the real HTTP client, so the real request
// building, parsing and pagination code paths are exercised end to end.
//
// The two things it gives a test are:
//   1. canned responses, registered per path (or per arbitrary predicate), and
//   2. a full recording of every request the extension actually made, so a test
//      can assert on the generated $filter / $select / $top and on headers such
//      as Prefer.
//
// The header intentionally does not pull in httplib or duckdb.hpp; the server is
// hidden behind a pimpl so test translation units stay cheap and free of the
// macro pollution those headers bring.

#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace erpl_web {
namespace test_support {

// One request as the server saw it on the wire.
struct RecordedRequest {
    std::string method;
    std::string path;    // decoded path, no query
    std::string query;   // raw query string, without the leading '?'
    std::string target;  // path + optional "?" + query, exactly as received
    std::string body;
    std::map<std::string, std::string> headers;  // keys lower-cased

    // Query helpers. `name` may be given with or without the OData '$' prefix
    // sigil; both the literal and the percent-encoded ("%24...") spellings of a
    // '$'-prefixed name are recognised, because different code paths encode the
    // sigil differently.
    //
    // QueryParam() returns the bytes exactly as they arrived. DecodedQueryParam()
    // undoes percent escapes and also reads '+' as a space, so it recovers the
    // value the extension asked for whether the request went out verbatim
    // (url_encode = false) or through httplib's form-urlencoded round trip
    // (url_encode = true). Prefer the decoded form for assertions about a clause;
    // use the raw form only when the encoding itself is what is under test.
    bool HasQueryParam(const std::string &name) const;
    std::string QueryParam(const std::string &name) const;         // raw wire bytes
    std::string DecodedQueryParam(const std::string &name) const;  // form-decoded

    // Case-insensitive header lookup; returns an empty string when absent.
    std::string Header(const std::string &name) const;
    bool HasHeader(const std::string &name) const;
};

// One canned reply.
struct CannedResponse {
    int status = 200;
    std::string content_type = "application/json";
    std::string body;
    std::map<std::string, std::string> headers;  // extra response headers

    // When set, the server announces the full body length and then writes only this
    // many bytes before dropping the connection. That is what a real service looks like
    // when it dies mid-response, and it is the shape GitHub #166 suspects behind a
    // segfault that appeared once against the public TripPin service and never again:
    // a short read reaching a parser that assumed a complete document. 0 means "send the
    // body whole", which is every other response.
    std::size_t truncate_after_bytes = 0;

    static CannedResponse Json(std::string body, int status = 200);
    static CannedResponse Xml(std::string body, int status = 200);
    static CannedResponse Error(int status, std::string body = std::string());

    CannedResponse &WithHeader(const std::string &name, const std::string &value);

    // Cut the body short after `bytes`, announcing the untruncated length.
    CannedResponse &TruncatedAfter(std::size_t bytes);
};

using RequestMatcher = std::function<bool(const RecordedRequest &)>;

// Binds 127.0.0.1 on an ephemeral port, serves on a background thread, and shuts
// down in the destructor. Construction does not return until the server is ready
// to accept connections.
//
// Declare the server BEFORE any DuckDB instance in a test so that it outlives the
// connections that talk to it.
class ODataTestServer {
public:
    ODataTestServer();
    ~ODataTestServer();

    ODataTestServer(const ODataTestServer &) = delete;
    ODataTestServer &operator=(const ODataTestServer &) = delete;

    int Port() const;
    std::string BaseUrl() const;                     // "http://127.0.0.1:<port>"
    std::string Url(const std::string &path) const;  // BaseUrl() + path

    // Registration. Routes are evaluated in registration order and the first
    // match wins. Anything unmatched gets a 404 with a JSON error body, which
    // makes a wrong URL fail loudly instead of silently returning nothing.
    void OnPath(const std::string &path, CannedResponse response);

    // Serve `responses` in order for successive requests to `path`; the last
    // entry is repeated once the sequence is exhausted. This is how multi-page
    // reads are set up when every page is fetched from the same path.
    void OnPathSequence(const std::string &path, std::vector<CannedResponse> responses);

    void OnMatch(RequestMatcher matcher, CannedResponse response);
    void OnMatchSequence(RequestMatcher matcher, std::vector<CannedResponse> responses);

    void ServeMetadata(const std::string &path, const std::string &edmx_xml);
    void ServeMetadataFixture(const std::string &path, const std::string &fixture_file_name);

    // Request recording -- the whole point of the harness.
    std::vector<RecordedRequest> Requests() const;
    std::vector<RecordedRequest> RequestsFor(const std::string &path) const;
    std::size_t RequestCount() const;
    void ClearRequests();

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

// ----------------------------------------------------------------------
// Fixtures

// Directory holding the committed edm_*.xml fixtures (test/cpp).
std::string FixtureDirectory();
std::string ReadFixture(const std::string &file_name);

// ----------------------------------------------------------------------
// Payload builders. `json_objects` are complete JSON object literals, e.g.
// R"({"AirlineCode":"AA","Name":"American Airlines"})".

std::string MakeV4Page(const std::string &context_url,
                       const std::vector<std::string> &json_objects,
                       const std::string &next_link = std::string());

std::string MakeV2Page(const std::vector<std::string> &json_objects,
                       const std::string &next_link = std::string());

// SAP ODP delta payload: an OData v2 page whose "d" wrapper carries a __delta
// link with the "!deltatoken=" query sigil, plus the
// "Preference-Applied: odata.track-changes" response header a real ODP service
// sends back when change tracking was requested.
CannedResponse MakeOdpDeltaPage(const std::vector<std::string> &json_objects,
                                const std::string &delta_link);

}  // namespace test_support
}  // namespace erpl_web
