#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"

#include "odp_http_request_factory.hpp"
#include "http_client.hpp"

using namespace erpl_web;
using namespace std;

TEST_CASE("OdpHttpRequestFactory Basic Construction", "[odp_http_factory]") {
    SECTION("Create factory without auth params") {
        OdpHttpRequestFactory factory;
        REQUIRE(factory.GetDefaultPageSize() == 15000); // Theobald default
    }

    SECTION("Create factory with auth params") {
        auto auth_params = std::make_shared<HttpAuthParams>();
        auth_params->basic_credentials = std::make_tuple("testuser", "testpass");
        
        OdpHttpRequestFactory factory(auth_params);
        REQUIRE(factory.GetDefaultPageSize() == 15000);
    }

    SECTION("Set and get default page size") {
        OdpHttpRequestFactory factory;
        factory.SetDefaultPageSize(5000);
        REQUIRE(factory.GetDefaultPageSize() == 5000);
    }
}

TEST_CASE("OdpHttpRequestFactory Initial Load Request", "[odp_http_factory]") {
    OdpHttpRequestFactory factory;
    std::string test_url = "https://example.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest";

    SECTION("Create initial load request with default page size") {
        auto request = factory.CreateInitialLoadRequest(test_url);
        
        // Verify method and URL
        REQUIRE(request.method == HttpMethod::GET);
        REQUIRE(request.url.ToString().find("$format=json") != std::string::npos);
        
        // Verify OData v2 headers
        REQUIRE(request.headers.find("DataServiceVersion") != request.headers.end());
        REQUIRE(request.headers.at("DataServiceVersion") == "2.0");
        REQUIRE(request.headers.find("MaxDataServiceVersion") != request.headers.end());
        REQUIRE(request.headers.at("MaxDataServiceVersion") == "2.0");
        
        // Verify Accept header for JSON
        REQUIRE(request.headers.find("Accept") != request.headers.end());
        REQUIRE(request.headers.at("Accept") == "application/json;odata=verbose");
        
        // Verify Prefer header with change tracking and page size
        REQUIRE(request.headers.find("Prefer") != request.headers.end());
        std::string prefer_header = request.headers.at("Prefer");
        REQUIRE(prefer_header.find("odata.track-changes") != std::string::npos);
        REQUIRE(prefer_header.find("odata.maxpagesize=15000") != std::string::npos);
    }

    SECTION("Create initial load request with custom page size") {
        auto request = factory.CreateInitialLoadRequest(test_url, 5000);
        
        REQUIRE(request.headers.find("Prefer") != request.headers.end());
        std::string prefer_header = request.headers.at("Prefer");
        REQUIRE(prefer_header.find("odata.maxpagesize=5000") != std::string::npos);
    }
}

TEST_CASE("OdpHttpRequestFactory Delta Fetch Request", "[odp_http_factory]") {
    OdpHttpRequestFactory factory;
    std::string delta_url = "https://example.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest!deltatoken=abc123";

    SECTION("Create delta fetch request") {
        auto request = factory.CreateDeltaFetchRequest(delta_url);
        
        // Verify method and URL
        REQUIRE(request.method == HttpMethod::GET);
        REQUIRE(request.url.ToString().find("$format=json") != std::string::npos);
        
        // Verify OData v2 headers
        REQUIRE(request.headers.at("DataServiceVersion") == "2.0");
        REQUIRE(request.headers.at("MaxDataServiceVersion") == "2.0");
        REQUIRE(request.headers.at("Accept") == "application/json;odata=verbose");
        
        // Verify Prefer header does NOT contain change tracking (only for initial load)
        REQUIRE(request.headers.find("Prefer") != request.headers.end());
        std::string prefer_header = request.headers.at("Prefer");
        REQUIRE(prefer_header.find("odata.track-changes") == std::string::npos);
        REQUIRE(prefer_header.find("odata.maxpagesize=15000") != std::string::npos);
    }
}

TEST_CASE("OdpHttpRequestFactory Metadata Request", "[odp_http_factory]") {
    OdpHttpRequestFactory factory;
    std::string metadata_url = "https://example.com/sap/opu/odata/sap/TEST_SRV/$metadata";

    SECTION("Create metadata request") {
        auto request = factory.CreateMetadataRequest(metadata_url);
        
        // Verify method and URL (should NOT have $format=json for metadata)
        REQUIRE(request.method == HttpMethod::GET);
        REQUIRE(request.url.ToString().find("$format=json") == std::string::npos);
        
        // Verify OData v2 headers
        REQUIRE(request.headers.at("DataServiceVersion") == "2.0");
        REQUIRE(request.headers.at("MaxDataServiceVersion") == "2.0");
        
        // Verify Accept header for XML (metadata is XML)
        REQUIRE(request.headers.at("Accept") == "application/xml");
        
        // Verify no Prefer header for metadata requests
        REQUIRE(request.headers.find("Prefer") == request.headers.end());
    }
}

TEST_CASE("OdpHttpRequestFactory Termination Request", "[odp_http_factory]") {
    OdpHttpRequestFactory factory;
    std::string termination_url = "https://example.com/sap/opu/odata/sap/TEST_SRV/TerminateDeltasForEntityOfTest";

    SECTION("Create termination request") {
        auto request = factory.CreateTerminationRequest(termination_url);
        
        // Verify method and URL
        REQUIRE(request.method == HttpMethod::GET);
        REQUIRE(request.url.ToString().find("$format=json") != std::string::npos);
        
        // Verify OData v2 headers
        REQUIRE(request.headers.at("DataServiceVersion") == "2.0");
        REQUIRE(request.headers.at("MaxDataServiceVersion") == "2.0");
        REQUIRE(request.headers.at("Accept") == "application/json;odata=verbose");
        
        // Verify no Prefer header for termination requests
        REQUIRE(request.headers.find("Prefer") == request.headers.end());
    }
}

TEST_CASE("OdpHttpRequestFactory Delta Token Discovery Request", "[odp_http_factory]") {
    OdpHttpRequestFactory factory;
    std::string delta_links_url = "https://example.com/sap/opu/odata/sap/TEST_SRV/DeltaLinksOfEntityOfTest";

    SECTION("Create delta token discovery request") {
        auto request = factory.CreateDeltaTokenDiscoveryRequest(delta_links_url);
        
        // Verify method and URL
        REQUIRE(request.method == HttpMethod::GET);
        REQUIRE(request.url.ToString().find("$format=json") != std::string::npos);
        
        // Verify OData v2 headers
        REQUIRE(request.headers.at("DataServiceVersion") == "2.0");
        REQUIRE(request.headers.at("MaxDataServiceVersion") == "2.0");
        REQUIRE(request.headers.at("Accept") == "application/json;odata=verbose");
        
        // Verify no Prefer header for discovery requests
        REQUIRE(request.headers.find("Prefer") == request.headers.end());
    }
}

TEST_CASE("OdpHttpRequestFactory Custom Request Configuration", "[odp_http_factory]") {
    OdpHttpRequestFactory factory;
    std::string test_url = "https://example.com/test";

    SECTION("Create request with custom configuration") {
        OdpHttpRequestFactory::OdpRequestConfig config;
        config.enable_change_tracking = true;
        config.max_page_size = 1000;
        config.request_json = true;
        config.odata_version = ODataVersion::V2;
        
        auto request = factory.CreateRequest(HttpMethod::POST, test_url, config);
        
        // Verify method
        REQUIRE(request.method == HttpMethod::POST);
        
        // Verify custom page size
        std::string prefer_header = request.headers.at("Prefer");
        REQUIRE(prefer_header.find("odata.maxpagesize=1000") != std::string::npos);
        REQUIRE(prefer_header.find("odata.track-changes") != std::string::npos);
    }

    SECTION("Create request without change tracking") {
        OdpHttpRequestFactory::OdpRequestConfig config;
        config.enable_change_tracking = false;
        config.max_page_size = 2000;
        config.request_json = true;
        config.odata_version = ODataVersion::V2;
        
        auto request = factory.CreateRequest(HttpMethod::GET, test_url, config);
        
        // Verify Prefer header does not contain change tracking
        std::string prefer_header = request.headers.at("Prefer");
        REQUIRE(prefer_header.find("odata.track-changes") == std::string::npos);
        REQUIRE(prefer_header.find("odata.maxpagesize=2000") != std::string::npos);
    }
}

TEST_CASE("OdpHttpRequestFactory Authentication Integration", "[odp_http_factory]") {
    SECTION("Basic authentication") {
        auto auth_params = std::make_shared<HttpAuthParams>();
        auth_params->basic_credentials = std::make_tuple("testuser", "testpass");
        
        OdpHttpRequestFactory factory(auth_params);
        auto request = factory.CreateInitialLoadRequest("https://example.com/test");
        
        // Verify Authorization header is present
        REQUIRE(request.headers.find("Authorization") != request.headers.end());
        std::string auth_header = request.headers.at("Authorization");
        REQUIRE(auth_header.find("Basic ") == 0);
    }

    SECTION("Bearer token authentication") {
        auto auth_params = std::make_shared<HttpAuthParams>();
        auth_params->bearer_token = "test_token_123";
        
        OdpHttpRequestFactory factory(auth_params);
        auto request = factory.CreateDeltaFetchRequest("https://example.com/test");
        
        // Verify Authorization header is present
        REQUIRE(request.headers.find("Authorization") != request.headers.end());
        std::string auth_header = request.headers.at("Authorization");
        REQUIRE(auth_header == "Bearer test_token_123");
    }
}

TEST_CASE("OdpHttpRequestFactory URL Format Parameter Handling", "[odp_http_factory]") {
    OdpHttpRequestFactory factory;

    SECTION("Add $format=json to URL without existing query parameters") {
        std::string url = "https://example.com/test";
        auto request = factory.CreateInitialLoadRequest(url);
        REQUIRE(request.url.ToString() == "https://example.com/test?$format=json");
    }

    SECTION("Add $format=json to URL with existing query parameters") {
        std::string url = "https://example.com/test?param=value";
        auto request = factory.CreateInitialLoadRequest(url);
        REQUIRE(request.url.ToString() == "https://example.com/test?param=value&$format=json");
    }

    SECTION("Do not add $format=json if already present") {
        std::string url = "https://example.com/test?$format=json";
        auto request = factory.CreateInitialLoadRequest(url);
        REQUIRE(request.url.ToString() == "https://example.com/test?$format=json");
    }

    SECTION("Do not add $format=json for metadata requests") {
        std::string url = "https://example.com/test/$metadata";
        auto request = factory.CreateMetadataRequest(url);
        REQUIRE(request.url.ToString() == "https://example.com/test/$metadata");
    }
}
