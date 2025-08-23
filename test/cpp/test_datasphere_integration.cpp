#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "erpl_datasphere_catalog.hpp"
#include "erpl_datasphere_read.hpp"

using namespace erpl_web;
using namespace std;

TEST_CASE("Test Datasphere Function Registration", "[datasphere]")
{
    std::cout << std::endl;
    
    // Test that we can create the function sets
    auto show_spaces_func = CreateDatasphereShowSpacesFunction();
    auto show_assets_func = CreateDatasphereShowAssetsFunction();
    auto describe_space_func = CreateDatasphereDescribeSpaceFunction();
    auto describe_asset_func = CreateDatasphereDescribeAssetFunction();
    
    auto relational_func = CreateDatasphereReadRelationalFunction();
    
    REQUIRE(show_spaces_func.name == "datasphere_show_spaces");
    REQUIRE(show_assets_func.name == "datasphere_show_assets");
    REQUIRE(describe_space_func.name == "datasphere_describe_space");
    REQUIRE(describe_asset_func.name == "datasphere_describe_asset");
    
    REQUIRE(relational_func.name == "datasphere_read_relational");
}

TEST_CASE("Test Datasphere URL Building", "[datasphere]")
{
    std::cout << std::endl;
    
    // Test URL builder functionality
    auto catalog_url = DatasphereUrlBuilder::BuildCatalogUrl("test_tenant", "eu10");
    REQUIRE(catalog_url == "https://test_tenant.eu10.hcs.cloud.sap/api/v1/dwc/catalog");
    
    auto relational_url = DatasphereUrlBuilder::BuildRelationalUrl("test_tenant", "eu10", "test_space", "test_asset");
    REQUIRE(relational_url == "https://test_tenant.eu10.hcs.cloud.sap/api/v1/dwc/consumption/relational/test_space/test_asset");
    
    auto analytical_url = DatasphereUrlBuilder::BuildAnalyticalUrl("test_tenant", "eu10", "test_space", "test_asset");
    REQUIRE(analytical_url == "https://test_tenant.eu10.hcs.cloud.sap/api/v1/dwc/consumption/analytical/test_space/test_asset");
}

TEST_CASE("Test Datasphere Client Factory", "[datasphere]")
{
    std::cout << std::endl;
    
    // Test client factory creation
    auto auth_params = std::make_shared<HttpAuthParams>();
    
    auto catalog_client = DatasphereClientFactory::CreateCatalogClient("test_tenant", "eu10", auth_params);
    REQUIRE(catalog_client != nullptr);
    REQUIRE(catalog_client->GetODataVersion() == ODataVersion::V4);
    
    auto relational_client = DatasphereClientFactory::CreateRelationalClient("test_tenant", "eu10", "test_space", "test_asset", auth_params);
    REQUIRE(relational_client != nullptr);
    REQUIRE(relational_client->GetODataVersion() == ODataVersion::V4);
    
    auto analytical_client = DatasphereClientFactory::CreateAnalyticalClient("test_tenant", "eu10", "test_space", "test_asset", auth_params);
    REQUIRE(analytical_client != nullptr);
    REQUIRE(analytical_client->GetODataVersion() == ODataVersion::V4);
}

TEST_CASE("Test Datasphere Auth Params", "[datasphere]")
{
    std::cout << std::endl;
    
    DatasphereAuthParams auth_params;
    auth_params.tenant_name = "test_tenant";
    auth_params.data_center = "eu10";
    auth_params.client_id = "test_client";
    auth_params.client_secret = "test_secret";
    auth_params.scope = "default";
    
    REQUIRE(auth_params.tenant_name == "test_tenant");
    REQUIRE(auth_params.data_center == "eu10");
    REQUIRE(auth_params.client_id == "test_client");
    REQUIRE(auth_params.client_secret == "test_secret");
    REQUIRE(auth_params.scope == "default");
    
    // Test OAuth2 endpoints
    auto auth_url = auth_params.GetAuthorizationUrl();
    REQUIRE(auth_url == "https://test_tenant.eu10.hcs.cloud.sap/oauth/authorize");
    
    auto token_url = auth_params.GetTokenUrl();
    REQUIRE(token_url == "https://test_tenant.eu10.hcs.cloud.sap/oauth/token");
    
    // Test token expiry
    REQUIRE(auth_params.IsTokenExpired() == true);
    REQUIRE(auth_params.NeedsRefresh() == true);
}

