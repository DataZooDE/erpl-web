#include "catch.hpp"
#include "datazoo/oauth2/http_client.hpp"
#include "odata_url_helpers.hpp"

using namespace erpl_web;

TEST_CASE("ODataUrlResolver uses @odata.context when present", "[odata_url]") {
    ODataUrlResolver r;
    HttpUrl base("https://example.com/api/v1/dwc/consumption/relational/tenant/asset/Entity");
    auto meta = r.resolveMetadataUrl(base, "./$metadata#Entity/Set");
    REQUIRE(meta.find("$metadata") != std::string::npos);
}

TEST_CASE("ODataUrlResolver falls back for Datasphere without context", "[odata_url]") {
    ODataUrlResolver r;
    HttpUrl base("https://host/api/v1/dwc/consumption/relational/ten/ass/Entity");
    auto meta = r.resolveMetadataUrl(base, "");
    REQUIRE(meta.find("/api/v1/dwc/consumption/relational/ten/ass/$metadata") != std::string::npos);
}

TEST_CASE("InputParametersFormatter formats params and inserts before /Set", "[odata_url]") {
    InputParametersFormatter f;
    HttpUrl base("https://example.com/svc/Entity/Set");
    std::map<std::string,std::string> params{{"CARRIER","AA"},{"YEAR","2024"}};
    auto with = f.addParams(base, params);
    auto p = with.Path();
    REQUIRE(p.find("(CARRIER='AA',YEAR=2024)/Set") != std::string::npos);
}

TEST_CASE("ODataUrlCodec encodes and decodes using httplib", "[odata_url]") {
    // Spaces, quotes, semicolon, unicode
    std::string raw = "Country eq 'Ger many';v=2 ";
    auto enc = ODataUrlCodec::encodeFilterExpression(raw);
    // Expect %20, %27, %3B etc.
    REQUIRE(enc.find("%20") != std::string::npos);
    REQUIRE(enc.find("%27") != std::string::npos);
    REQUIRE(enc.find("%3B") != std::string::npos);
    auto dec = ODataUrlCodec::decodeQueryValue(enc);
    REQUIRE(dec == raw);
}

TEST_CASE("ODataUrlCodec ensureJsonFormat appends $format=json", "[odata_url]") {
    HttpUrl url1("https://h/svc/Entity");
    ODataUrlCodec::ensureJsonFormat(url1);
    REQUIRE(url1.Query().find("$format=json") != std::string::npos);

    HttpUrl url2("https://h/svc/Entity?$top=3");
    ODataUrlCodec::ensureJsonFormat(url2);
    REQUIRE(url2.Query().find("$format=json") != std::string::npos);

    HttpUrl url3("https://h/svc/Entity?$format=json");
    ODataUrlCodec::ensureJsonFormat(url3);
    // no duplicate
    std::string q3 = url3.Query();
    REQUIRE(q3.find("$format=json") == q3.rfind("$format=json"));
}



TEST_CASE("ODataUrlResolver keeps a .svc service root for OData V4", "[odata_url]") {
    // GitHub #60: the V2 branch honoured a .svc service root while the V4 branch
    // took the first segment after /V4/, truncating "Northwind.svc" away and
    // making every WCF-convention V4 service unreachable.
    ODataUrlResolver r;
    HttpUrl base("https://services.odata.org/V4/Northwind/Northwind.svc/Orders");
    auto meta = r.resolveMetadataUrl(base, "");
    REQUIRE(meta == "https://services.odata.org/V4/Northwind/Northwind.svc/$metadata");
}

TEST_CASE("ODataUrlResolver keeps a .svc service root for OData V2", "[odata_url]") {
    ODataUrlResolver r;
    HttpUrl base("https://services.odata.org/V2/Northwind/Northwind.svc/Orders");
    auto meta = r.resolveMetadataUrl(base, "");
    REQUIRE(meta == "https://services.odata.org/V2/Northwind/Northwind.svc/$metadata");
}

TEST_CASE("ODataUrlResolver honours a .svc service root with no version segment", "[odata_url]") {
    ODataUrlResolver r;
    HttpUrl base("https://erp.example.com/odata/Sales.svc/Customers");
    auto meta = r.resolveMetadataUrl(base, "");
    REQUIRE(meta == "https://erp.example.com/odata/Sales.svc/$metadata");
}

TEST_CASE("ODataUrlResolver drops the query string when deriving $metadata", "[odata_url]") {
    ODataUrlResolver r;
    HttpUrl base("https://services.odata.org/V4/Northwind/Northwind.svc/Orders?$top=5");
    auto meta = r.resolveMetadataUrl(base, "");
    REQUIRE(meta.find('?') == std::string::npos);
}

TEST_CASE("ODataUrlResolver only treats .svc as a service root at a segment boundary", "[odata_url]") {
    // Matching ".svc" as a bare substring would truncate these at the wrong place.
    ODataUrlResolver r;

    HttpUrl not_a_service_root("https://host/catalog.svcdata/Orders");
    REQUIRE(r.resolveMetadataUrl(not_a_service_root, "") == "https://host/catalog.svcdata/$metadata");

    HttpUrl trailing("https://host/odata/Sales.svc");
    REQUIRE(r.resolveMetadataUrl(trailing, "") == "https://host/odata/Sales.svc/$metadata");

    // A later, genuine .svc segment must still be found when an earlier one is a false match.
    HttpUrl later_segment("https://host/a.svcx/Sales.svc/Orders");
    REQUIRE(r.resolveMetadataUrl(later_segment, "") == "https://host/a.svcx/Sales.svc/$metadata");
}

TEST_CASE("normalizeAndSanitizeExpand does not split options inside a string literal", "[odata_url]") {
    // GitHub #122: the option splitter treated ';' as a separator even inside a quoted
    // literal, so "$filter=Name eq 'Product;Name'" was cut in half - the first part was
    // encoded as a filter value and the trailing "Name'" was re-emitted as a separate
    // valueless option key, with its apostrophe left raw. A round-trip decode still
    // matched the input, which is why it looked benign, but what reached the wire was a
    // half-encoded expression containing an unescaped quote.
    auto sanitized = ODataUrlCodec::normalizeAndSanitizeExpand("Products($filter=Name eq 'Product;Name')");

    // The whole literal must be inside one encoded $filter value: the semicolon belongs
    // to the string, so it must be percent-encoded rather than left as a separator.
    REQUIRE(sanitized.find("%3B") != std::string::npos);
    REQUIRE(sanitized.find(";Name'") == std::string::npos);
}

TEST_CASE("normalizeAndSanitizeExpand handles a comma inside a string literal", "[odata_url]") {
    auto sanitized = ODataUrlCodec::normalizeAndSanitizeExpand("Products($filter=Name eq 'A,B')");
    REQUIRE(sanitized.find("%2C") != std::string::npos);
}

TEST_CASE("normalizeAndSanitizeExpand survives OData's doubled-quote escape", "[odata_url]") {
    // A naive toggle-on-quote scanner desynchronises on the escaped quote and treats the
    // rest of the expression as being outside the literal.
    auto sanitized = ODataUrlCodec::normalizeAndSanitizeExpand("Products($filter=Name eq 'O''Brien;X')");
    REQUIRE(sanitized.find("%3B") != std::string::npos);
    REQUIRE(sanitized.find(";X'") == std::string::npos);
}

TEST_CASE("normalizeAndSanitizeExpand keeps a parenthesis inside a literal from closing the group",
          "[odata_url]") {
    auto sanitized = ODataUrlCodec::normalizeAndSanitizeExpand("Products($filter=Name eq 'A)B';$select=Name)");
    // $select is part of the same option group, so it must still be $-prefixed and present.
    REQUIRE(sanitized.find("$select=Name") != std::string::npos);
}
