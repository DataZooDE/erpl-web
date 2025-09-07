#include "catch.hpp"
#include "http_client.hpp"
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
    auto enc = ODataUrlCodec::encodeQueryValue(raw);
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


