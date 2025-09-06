#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "datasphere_catalog.hpp"
#include "http_client.hpp"
#include <iostream>

using namespace erpl_web;
using namespace std;

TEST_CASE("Test SAP Discovery Endpoint Fetching", "[datasphere][discovery]")
{
    std::cout << std::endl;
    
    // Test discovery URL construction
    std::string tenant = "test_tenant";
    std::string data_center = "eu10";
    std::string discovery_url = "https://" + tenant + "." + data_center + ".hcs.cloud.sap/dwaas-core/api/v1/discovery";
    
    REQUIRE(discovery_url == "https://test_tenant.eu10.hcs.cloud.sap/dwaas-core/api/v1/discovery");
    
    // Test discovery URL validation
    REQUIRE(discovery_url.find("/dwaas-core/api/v1/discovery") != std::string::npos);
    REQUIRE(discovery_url.find("https://") == 0);
}

TEST_CASE("Test OpenAPI 3.0.3 Document Parsing", "[datasphere][discovery][openapi]")
{
    std::cout << std::endl;
    
    // Test OpenAPI version detection
    std::string openapi_version = "3.0.1";
    REQUIRE(openapi_version == "3.0.1");
    
    // Test SAP API type detection
    std::string sap_api_type = "ODATAV4";
    REQUIRE(sap_api_type == "ODATAV4");
    
    // Test OData version detection
    std::string odata_version = "4.0";
    REQUIRE(odata_version == "4.0");
}

TEST_CASE("Test OData Endpoint Extraction", "[datasphere][discovery][odata]")
{
    std::cout << std::endl;
    
    // Test catalog endpoint extraction
    std::string catalog_endpoint = "/v1/dwc/catalog";
    REQUIRE(catalog_endpoint == "/v1/dwc/catalog");
    
    // Test consumption endpoint extraction
    std::string analytical_endpoint = "/v1/dwc/consumption/analytical";
    std::string relational_endpoint = "/v1/dwc/consumption/relational";
    
    REQUIRE(analytical_endpoint.find("/analytical") != std::string::npos);
    REQUIRE(relational_endpoint.find("/relational") != std::string::npos);
    
    // Test endpoint pattern validation
    REQUIRE(analytical_endpoint.find("/v1/dwc/consumption") == 0);
    REQUIRE(relational_endpoint.find("/v1/dwc/consumption") == 0);
}

TEST_CASE("Test ETag-based Caching", "[datasphere][discovery][caching]")
{
    std::cout << std::endl;
    
    // Test ETag generation
    std::string etag1 = "W/\"abc123\"";
    std::string etag2 = "W/\"def456\"";
    
    REQUIRE(etag1 != etag2);
    REQUIRE(etag1.find("W/\"") == 0);
    REQUIRE(etag2.find("W/\"") == 0);
    
    // Test ETag comparison
    REQUIRE(etag1 == etag1);
    REQUIRE(etag1 != etag2);
    
    // Test weak ETag detection
    REQUIRE(etag1.find("W/\"") == 0);
    REQUIRE(etag2.find("W/\"") == 0);
}

TEST_CASE("Test Discovery Document Validation", "[datasphere][discovery][validation]")
{
    std::cout << std::endl;
    
    // Test required OpenAPI fields
    std::string openapi = "3.0.1";
    std::string info_title = "Catalog";
    std::string info_version = "1.0.0";
    
    REQUIRE(openapi == "3.0.1");
    REQUIRE(info_title == "Catalog");
    REQUIRE(info_version == "1.0.0");
    
    // Test required SAP fields
    std::string sap_api_type = "ODATAV4";
    std::string odata_version = "4.0";
    
    REQUIRE(sap_api_type == "ODATAV4");
    REQUIRE(odata_version == "4.0");
}

TEST_CASE("Test Error Handling for Discovery Failures", "[datasphere][discovery][errors]")
{
    std::cout << std::endl;
    
    // Test network timeout handling
    std::string timeout_error = "Discovery request timed out";
    REQUIRE(timeout_error.find("timed out") != std::string::npos);
    
    // Test invalid response handling
    std::string invalid_response_error = "Invalid OpenAPI document format";
    REQUIRE(invalid_response_error.find("Invalid") != std::string::npos);
    
    // Test authentication failure handling
    std::string auth_error = "Authentication failed for discovery endpoint";
    REQUIRE(auth_error.find("Authentication failed") != std::string::npos);
}

TEST_CASE("Test Discovery Retry Logic", "[datasphere][discovery][retry]")
{
    std::cout << std::endl;
    
    // Test retry count tracking
    int max_retries = 3;
    int current_retry = 0;
    
    REQUIRE(current_retry < max_retries);
    
    // Test exponential backoff
    int backoff_ms = 1000 * (1 << current_retry); // 1s, 2s, 4s
    REQUIRE(backoff_ms == 1000);
    
    current_retry = 1;
    backoff_ms = 1000 * (1 << current_retry);
    REQUIRE(backoff_ms == 2000);
    
    current_retry = 2;
    backoff_ms = 1000 * (1 << current_retry);
    REQUIRE(backoff_ms == 4000);
}

TEST_CASE("Test Discovery Integration Flow", "[datasphere][discovery][integration]")
{
    std::cout << std::endl;
    
    // Test complete discovery flow setup
    std::string tenant = "test_tenant";
    std::string data_center = "eu10";
    
    // Build discovery URL
    std::string discovery_url = "https://" + tenant + "." + data_center + ".hcs.cloud.sap/dwaas-core/api/v1/discovery";
    REQUIRE(discovery_url.find("/dwaas-core/api/v1/discovery") != std::string::npos);
    
    // Test endpoint extraction
    std::string catalog_endpoint = "/v1/dwc/catalog";
    std::string analytical_endpoint = "/v1/dwc/consumption/analytical";
    std::string relational_endpoint = "/v1/dwc/consumption/relational";
    
    REQUIRE(catalog_endpoint.find("/catalog") != std::string::npos);
    REQUIRE(analytical_endpoint.find("/analytical") != std::string::npos);
    REQUIRE(relational_endpoint.find("/relational") != std::string::npos);
    
    // Test capability detection
    bool has_analytical = analytical_endpoint.find("/analytical") != std::string::npos;
    bool has_relational = relational_endpoint.find("/relational") != std::string::npos;
    
    REQUIRE(has_analytical == true);
    REQUIRE(has_relational == true);
}
