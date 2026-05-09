#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "datasphere_catalog.hpp"
#include "datasphere_read.hpp"
#include <stdexcept>

using namespace erpl_web;
using namespace std;

namespace {

const duckdb::Value &StructField(const duckdb::Value &value, const std::string &field_name) {
    const auto &children = duckdb::StructValue::GetChildren(value);
    const auto &type = value.type();
    for (duckdb::idx_t index = 0; index < children.size(); index++) {
        if (duckdb::StructType::GetChildName(type, index) == field_name) {
            return children[index];
        }
    }
    throw std::runtime_error("Missing struct field: " + field_name);
}

const duckdb::Value &ListItem(const duckdb::Value &value, duckdb::idx_t index) {
    const auto &children = duckdb::ListValue::GetChildren(value);
    if (index >= children.size()) {
        throw std::runtime_error("List index out of bounds");
    }
    return children[index];
}

} // namespace

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

TEST_CASE("Datasphere DWAAS analytical schema parser builds typed schema values", "[datasphere][parser]")
{
    DatasphereDescribeBindData bind_data(nullptr, "asset", "SALES_MODEL");
    const std::string json = R"json(
        {
            "definitions": {
                "SalesModel": {
                    "elements": {
                        "net_revenue": {
                            "@EndUserText.label": "Net Revenue",
                            "type": "cds.Decimal"
                        },
                        "ProductID": {
                            "@EndUserText.label": "Product"
                        },
                        "row_count": {}
                    }
                }
            }
        }
    )json";

    auto schema = bind_data.ParseDwaasAnalyticalSchema(json);
    REQUIRE(schema.has_value());

    const auto &measures = duckdb::ListValue::GetChildren(StructField(*schema, "measures"));
    const auto &dimensions = duckdb::ListValue::GetChildren(StructField(*schema, "dimensions"));
    const auto &variables = duckdb::ListValue::GetChildren(StructField(*schema, "variables"));

    REQUIRE(measures.size() == 2);
    REQUIRE(dimensions.size() == 1);
    REQUIRE(variables.empty());

    REQUIRE(StructField(ListItem(StructField(*schema, "measures"), 0), "name").ToString() == "Net Revenue");
    REQUIRE(StructField(ListItem(StructField(*schema, "measures"), 0), "type").ToString() == "FactSourceMeasure");
    REQUIRE(StructField(ListItem(StructField(*schema, "measures"), 0), "edm_type").ToString() == "Edm.String");
    REQUIRE(StructField(ListItem(StructField(*schema, "dimensions"), 0), "name").ToString() == "Product");
    REQUIRE(StructField(ListItem(StructField(*schema, "dimensions"), 0), "type").ToString() == "FactSourceAttribute");
}

TEST_CASE("Datasphere DWAAS relational schema parser builds typed column values", "[datasphere][parser]")
{
    DatasphereDescribeBindData bind_data(nullptr, "asset", "SALES_TABLE");
    const std::string json = R"json(
        {
            "definitions": {
                "SalesTable": {
                    "elements": {
                        "CustomerID": {
                            "@EndUserText.label": "Customer",
                            "type": "cds.String",
                            "length": 12
                        },
                        "Amount": {
                            "type": "cds.Decimal",
                            "length": 15
                        },
                        "ChangedAt": {}
                    }
                }
            }
        }
    )json";

    auto schema = bind_data.ParseDwaasRelationalSchema(json);
    REQUIRE(schema.has_value());

    const auto &columns = duckdb::ListValue::GetChildren(StructField(*schema, "columns"));
    REQUIRE(columns.size() == 3);

    REQUIRE(StructField(columns[0], "name").ToString() == "Customer");
    REQUIRE(StructField(columns[0], "technical_name").ToString() == "CustomerID");
    REQUIRE(StructField(columns[0], "type").ToString() == "cds.String");
    REQUIRE(StructField(columns[0], "length").ToString() == "12");

    REQUIRE(StructField(columns[1], "name").ToString() == "Amount");
    REQUIRE(StructField(columns[1], "technical_name").ToString() == "Amount");
    REQUIRE(StructField(columns[1], "type").ToString() == "cds.Decimal");
    REQUIRE(StructField(columns[1], "length").ToString() == "15");

    REQUIRE(StructField(columns[2], "name").ToString() == "ChangedAt");
    REQUIRE(StructField(columns[2], "type").ToString() == "Unknown");
    REQUIRE(StructField(columns[2], "length").ToString().empty());
}

TEST_CASE("Datasphere DWAAS schema parsers reject invalid JSON", "[datasphere][parser]")
{
    DatasphereDescribeBindData bind_data(nullptr, "asset", "BROKEN");

    REQUIRE_FALSE(bind_data.ParseDwaasAnalyticalSchema("{").has_value());
    REQUIRE_FALSE(bind_data.ParseDwaasRelationalSchema("{").has_value());
}
