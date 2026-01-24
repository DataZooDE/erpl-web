#include "catch.hpp"
#include "business_central_client.hpp"

using namespace erpl_web;

// =============================================================================
// URL Builder Tests
// =============================================================================

TEST_CASE("Business Central URL Builder - API URL generation", "[business_central][url]") {
    SECTION("Standard API URL generation") {
        auto url = BusinessCentralUrlBuilder::BuildApiUrl("test-tenant-123", "sandbox");
        REQUIRE(url == "https://api.businesscentral.dynamics.com/v2.0/test-tenant-123/sandbox/api/v2.0");
    }

    SECTION("Production environment") {
        auto url = BusinessCentralUrlBuilder::BuildApiUrl("prod-tenant-456", "production");
        REQUIRE(url == "https://api.businesscentral.dynamics.com/v2.0/prod-tenant-456/production/api/v2.0");
    }

    SECTION("Empty tenant ID") {
        auto url = BusinessCentralUrlBuilder::BuildApiUrl("", "sandbox");
        REQUIRE(url == "https://api.businesscentral.dynamics.com/v2.0//sandbox/api/v2.0");
    }
}

TEST_CASE("Business Central URL Builder - Company URL generation", "[business_central][url]") {
    std::string base_url = "https://api.businesscentral.dynamics.com/v2.0/tenant/sandbox/api/v2.0";

    SECTION("Standard company URL with GUID") {
        auto url = BusinessCentralUrlBuilder::BuildCompanyUrl(base_url, "12345678-abcd-1234-abcd-123456789012");
        REQUIRE(url == "https://api.businesscentral.dynamics.com/v2.0/tenant/sandbox/api/v2.0/companies(12345678-abcd-1234-abcd-123456789012)");
    }

    SECTION("Company URL with simple ID") {
        auto url = BusinessCentralUrlBuilder::BuildCompanyUrl(base_url, "my-company");
        REQUIRE(url == "https://api.businesscentral.dynamics.com/v2.0/tenant/sandbox/api/v2.0/companies(my-company)");
    }
}

TEST_CASE("Business Central URL Builder - Entity Set URL generation", "[business_central][url]") {
    std::string company_url = "https://api.businesscentral.dynamics.com/v2.0/tenant/sandbox/api/v2.0/companies(company-id)";

    SECTION("Standard entity set URL") {
        auto url = BusinessCentralUrlBuilder::BuildEntitySetUrl(company_url, "customers");
        REQUIRE(url == "https://api.businesscentral.dynamics.com/v2.0/tenant/sandbox/api/v2.0/companies(company-id)/customers");
    }

    SECTION("Different entity sets") {
        REQUIRE(BusinessCentralUrlBuilder::BuildEntitySetUrl(company_url, "vendors") ==
                "https://api.businesscentral.dynamics.com/v2.0/tenant/sandbox/api/v2.0/companies(company-id)/vendors");

        REQUIRE(BusinessCentralUrlBuilder::BuildEntitySetUrl(company_url, "items") ==
                "https://api.businesscentral.dynamics.com/v2.0/tenant/sandbox/api/v2.0/companies(company-id)/items");

        REQUIRE(BusinessCentralUrlBuilder::BuildEntitySetUrl(company_url, "salesOrders") ==
                "https://api.businesscentral.dynamics.com/v2.0/tenant/sandbox/api/v2.0/companies(company-id)/salesOrders");
    }
}

TEST_CASE("Business Central URL Builder - Metadata URL generation", "[business_central][url]") {
    std::string base_url = "https://api.businesscentral.dynamics.com/v2.0/tenant/sandbox/api/v2.0";

    SECTION("Standard metadata URL") {
        auto url = BusinessCentralUrlBuilder::BuildMetadataUrl(base_url);
        REQUIRE(url == "https://api.businesscentral.dynamics.com/v2.0/tenant/sandbox/api/v2.0/$metadata");
    }
}

TEST_CASE("Business Central URL Builder - Companies URL generation", "[business_central][url]") {
    std::string base_url = "https://api.businesscentral.dynamics.com/v2.0/tenant/sandbox/api/v2.0";

    SECTION("Standard companies URL") {
        auto url = BusinessCentralUrlBuilder::BuildCompaniesUrl(base_url);
        REQUIRE(url == "https://api.businesscentral.dynamics.com/v2.0/tenant/sandbox/api/v2.0/companies");
    }
}

TEST_CASE("Business Central URL Builder - Resource URL", "[business_central][url]") {
    SECTION("Resource URL for OAuth2 scope") {
        auto url = BusinessCentralUrlBuilder::GetResourceUrl();
        REQUIRE(url == "https://api.businesscentral.dynamics.com");
    }
}

// =============================================================================
// Integration URL Flow Tests
// =============================================================================

TEST_CASE("Business Central URL Builder - Full URL chain", "[business_central][url][integration]") {
    std::string tenant_id = "contoso-tenant";
    std::string environment = "production";
    std::string company_id = "company-guid-12345";
    std::string entity_set = "customers";

    SECTION("Build complete URL chain") {
        auto base_url = BusinessCentralUrlBuilder::BuildApiUrl(tenant_id, environment);
        REQUIRE(base_url == "https://api.businesscentral.dynamics.com/v2.0/contoso-tenant/production/api/v2.0");

        auto companies_url = BusinessCentralUrlBuilder::BuildCompaniesUrl(base_url);
        REQUIRE(companies_url == "https://api.businesscentral.dynamics.com/v2.0/contoso-tenant/production/api/v2.0/companies");

        auto company_url = BusinessCentralUrlBuilder::BuildCompanyUrl(base_url, company_id);
        REQUIRE(company_url == "https://api.businesscentral.dynamics.com/v2.0/contoso-tenant/production/api/v2.0/companies(company-guid-12345)");

        auto entity_url = BusinessCentralUrlBuilder::BuildEntitySetUrl(company_url, entity_set);
        REQUIRE(entity_url == "https://api.businesscentral.dynamics.com/v2.0/contoso-tenant/production/api/v2.0/companies(company-guid-12345)/customers");

        auto metadata_url = BusinessCentralUrlBuilder::BuildMetadataUrl(base_url);
        REQUIRE(metadata_url == "https://api.businesscentral.dynamics.com/v2.0/contoso-tenant/production/api/v2.0/$metadata");
    }
}
