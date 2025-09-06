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


