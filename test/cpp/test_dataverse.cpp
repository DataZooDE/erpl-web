#include "catch.hpp"
#include "dataverse_client.hpp"

using namespace erpl_web;

// =============================================================================
// URL Builder Tests
// =============================================================================

TEST_CASE("Dataverse URL Builder - API URL generation", "[dataverse][url]") {
    SECTION("Standard API URL with default version") {
        auto url = DataverseUrlBuilder::BuildApiUrl("https://myorg.crm.dynamics.com");
        REQUIRE(url == "https://myorg.crm.dynamics.com/api/data/v9.2");
    }

    SECTION("API URL with custom version") {
        auto url = DataverseUrlBuilder::BuildApiUrl("https://myorg.crm.dynamics.com", "v9.1");
        REQUIRE(url == "https://myorg.crm.dynamics.com/api/data/v9.1");
    }

    SECTION("API URL with trailing slash") {
        auto url = DataverseUrlBuilder::BuildApiUrl("https://myorg.crm.dynamics.com/");
        REQUIRE(url == "https://myorg.crm.dynamics.com/api/data/v9.2");
    }

    SECTION("Different regional endpoints") {
        // US (default)
        REQUIRE(DataverseUrlBuilder::BuildApiUrl("https://contoso.crm.dynamics.com") ==
                "https://contoso.crm.dynamics.com/api/data/v9.2");

        // Europe
        REQUIRE(DataverseUrlBuilder::BuildApiUrl("https://contoso.crm4.dynamics.com") ==
                "https://contoso.crm4.dynamics.com/api/data/v9.2");

        // UK
        REQUIRE(DataverseUrlBuilder::BuildApiUrl("https://contoso.crm11.dynamics.com") ==
                "https://contoso.crm11.dynamics.com/api/data/v9.2");
    }
}

TEST_CASE("Dataverse URL Builder - Entity Set URL generation", "[dataverse][url]") {
    std::string base_url = "https://myorg.crm.dynamics.com/api/data/v9.2";

    SECTION("Standard entity sets") {
        REQUIRE(DataverseUrlBuilder::BuildEntitySetUrl(base_url, "accounts") ==
                "https://myorg.crm.dynamics.com/api/data/v9.2/accounts");

        REQUIRE(DataverseUrlBuilder::BuildEntitySetUrl(base_url, "contacts") ==
                "https://myorg.crm.dynamics.com/api/data/v9.2/contacts");

        REQUIRE(DataverseUrlBuilder::BuildEntitySetUrl(base_url, "leads") ==
                "https://myorg.crm.dynamics.com/api/data/v9.2/leads");

        REQUIRE(DataverseUrlBuilder::BuildEntitySetUrl(base_url, "opportunities") ==
                "https://myorg.crm.dynamics.com/api/data/v9.2/opportunities");
    }
}

TEST_CASE("Dataverse URL Builder - Metadata URL generation", "[dataverse][url]") {
    std::string base_url = "https://myorg.crm.dynamics.com/api/data/v9.2";

    SECTION("Standard metadata URL") {
        auto url = DataverseUrlBuilder::BuildMetadataUrl(base_url);
        REQUIRE(url == "https://myorg.crm.dynamics.com/api/data/v9.2/$metadata");
    }
}

TEST_CASE("Dataverse URL Builder - Entity Definitions URL generation", "[dataverse][url]") {
    std::string base_url = "https://myorg.crm.dynamics.com/api/data/v9.2";

    SECTION("Entity definitions list URL") {
        auto url = DataverseUrlBuilder::BuildEntityDefinitionsUrl(base_url);
        REQUIRE(url == "https://myorg.crm.dynamics.com/api/data/v9.2/EntityDefinitions");
    }

    SECTION("Single entity definition URL") {
        auto url = DataverseUrlBuilder::BuildEntityDefinitionUrl(base_url, "account");
        REQUIRE(url == "https://myorg.crm.dynamics.com/api/data/v9.2/EntityDefinitions(LogicalName='account')");
    }

    SECTION("Entity attributes URL") {
        auto url = DataverseUrlBuilder::BuildEntityAttributesUrl(base_url, "account");
        REQUIRE(url == "https://myorg.crm.dynamics.com/api/data/v9.2/EntityDefinitions(LogicalName='account')/Attributes");
    }
}

// =============================================================================
// Integration URL Flow Tests
// =============================================================================

TEST_CASE("Dataverse URL Builder - Full URL chain", "[dataverse][url][integration]") {
    std::string environment_url = "https://contoso.crm.dynamics.com";
    std::string api_version = "v9.2";

    SECTION("Build complete URL chain for entity data access") {
        auto base_url = DataverseUrlBuilder::BuildApiUrl(environment_url, api_version);
        REQUIRE(base_url == "https://contoso.crm.dynamics.com/api/data/v9.2");

        auto entity_defs_url = DataverseUrlBuilder::BuildEntityDefinitionsUrl(base_url);
        REQUIRE(entity_defs_url == "https://contoso.crm.dynamics.com/api/data/v9.2/EntityDefinitions");

        auto account_def_url = DataverseUrlBuilder::BuildEntityDefinitionUrl(base_url, "account");
        REQUIRE(account_def_url == "https://contoso.crm.dynamics.com/api/data/v9.2/EntityDefinitions(LogicalName='account')");

        auto account_attrs_url = DataverseUrlBuilder::BuildEntityAttributesUrl(base_url, "account");
        REQUIRE(account_attrs_url == "https://contoso.crm.dynamics.com/api/data/v9.2/EntityDefinitions(LogicalName='account')/Attributes");

        auto accounts_url = DataverseUrlBuilder::BuildEntitySetUrl(base_url, "accounts");
        REQUIRE(accounts_url == "https://contoso.crm.dynamics.com/api/data/v9.2/accounts");

        auto metadata_url = DataverseUrlBuilder::BuildMetadataUrl(base_url);
        REQUIRE(metadata_url == "https://contoso.crm.dynamics.com/api/data/v9.2/$metadata");
    }
}

TEST_CASE("Dataverse URL Builder - Common Dataverse entities", "[dataverse][url]") {
    std::string base_url = "https://org.crm.dynamics.com/api/data/v9.2";

    SECTION("Core CRM entities") {
        // Accounts
        REQUIRE(DataverseUrlBuilder::BuildEntitySetUrl(base_url, "accounts") ==
                "https://org.crm.dynamics.com/api/data/v9.2/accounts");

        // Contacts
        REQUIRE(DataverseUrlBuilder::BuildEntitySetUrl(base_url, "contacts") ==
                "https://org.crm.dynamics.com/api/data/v9.2/contacts");

        // Leads
        REQUIRE(DataverseUrlBuilder::BuildEntitySetUrl(base_url, "leads") ==
                "https://org.crm.dynamics.com/api/data/v9.2/leads");

        // Opportunities
        REQUIRE(DataverseUrlBuilder::BuildEntitySetUrl(base_url, "opportunities") ==
                "https://org.crm.dynamics.com/api/data/v9.2/opportunities");

        // Cases
        REQUIRE(DataverseUrlBuilder::BuildEntitySetUrl(base_url, "incidents") ==
                "https://org.crm.dynamics.com/api/data/v9.2/incidents");

        // Activities
        REQUIRE(DataverseUrlBuilder::BuildEntitySetUrl(base_url, "activitypointers") ==
                "https://org.crm.dynamics.com/api/data/v9.2/activitypointers");
    }
}
