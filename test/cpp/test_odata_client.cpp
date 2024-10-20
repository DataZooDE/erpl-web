#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"

#include "erpl_odata_client.hpp"

using namespace erpl_web;
using namespace std;

TEST_CASE("Test ODataEntitySetClient Metadata initalization", "[odata_client]")
{
    std::cout << std::endl;

    auto http_client = std::make_shared<HttpClient>();
    auto url = HttpUrl("https://services.odata.org/V4/Northwind/Northwind.svc/Customers");
    
    ODataEntitySetClient client(http_client, url);
    
    auto edmx = client.GetMetadata();
    auto entity_set = edmx.FindEntitySet("Customers");
    
    REQUIRE(entity_set.name == "Customers");
    REQUIRE(entity_set.entity_type_name == "NorthwindModel.Customer");

    auto entity_type = std::get<EntityType>(edmx.FindType(entity_set.entity_type_name));
    REQUIRE(entity_type.name == "Customer");
}

TEST_CASE("Test ODataEntitySetClient GetResultNames & GetResultTypes", "[odata_client]")
{
    std::cout << std::endl;

    auto http_client = std::make_shared<HttpClient>();
    auto url = HttpUrl("https://services.odata.org/V4/Northwind/Northwind.svc/Customers");
    
    ODataEntitySetClient client(http_client, url);
    
    /*
    {
      "CustomerID": "ALFKI",
      "CompanyName": "Alfreds Futterkiste",
      "ContactName": "Maria Anders",
      "ContactTitle": "Sales Representative",
      "Address": "Obere Str. 57",
      "City": "Berlin",
      "Region": null,
      "PostalCode": "12209",
      "Country": "Germany",
      "Phone": "030-0074321",
      "Fax": "030-0076545"
    },
    */

    auto result_names = client.GetResultNames();
    auto result_types = client.GetResultTypes();

    REQUIRE(result_names.size() == 11);
    REQUIRE(result_types.size() == 11);

    REQUIRE(result_names[0] == "CustomerID");
    REQUIRE(result_types[0] == duckdb::LogicalTypeId::VARCHAR);

    REQUIRE(result_names[1] == "CompanyName");
    REQUIRE(result_types[1] == duckdb::LogicalTypeId::VARCHAR);

    REQUIRE(result_names[2] == "ContactName");
    REQUIRE(result_types[2] == duckdb::LogicalTypeId::VARCHAR);

    REQUIRE(result_names[3] == "ContactTitle");
    REQUIRE(result_types[3] == duckdb::LogicalTypeId::VARCHAR);

    REQUIRE(result_names[4] == "Address");
    REQUIRE(result_types[4] == duckdb::LogicalTypeId::VARCHAR);

    REQUIRE(result_names[5] == "City");
    REQUIRE(result_types[5] == duckdb::LogicalTypeId::VARCHAR);

    REQUIRE(result_names[6] == "Region");
    REQUIRE(result_types[6] == duckdb::LogicalTypeId::VARCHAR);

    REQUIRE(result_names[7] == "PostalCode");
    REQUIRE(result_types[7] == duckdb::LogicalTypeId::VARCHAR);

    REQUIRE(result_names[8] == "Country");
    REQUIRE(result_types[8] == duckdb::LogicalTypeId::VARCHAR);

    REQUIRE(result_names[9] == "Phone");
    REQUIRE(result_types[9] == duckdb::LogicalTypeId::VARCHAR);

    REQUIRE(result_names[10] == "Fax");
    REQUIRE(result_types[10] == duckdb::LogicalTypeId::VARCHAR);
}

TEST_CASE("Test ODataClient Get with get_next", "[odata_client]")
{
    std::cout << std::endl;
    auto http_client = std::make_shared<HttpClient>();
    auto url = HttpUrl("https://services.odata.org/V4/Northwind/Northwind.svc/Customers");
    
    ODataEntitySetClient client(http_client, url);
    
    idx_t i = 0;
    for (auto response = client.Get(); response != nullptr; response = client.Get(true)) {
        i++;
    }

    REQUIRE(i == 5);
}

TEST_CASE("Test ODataEntitySetClient ToRows", "[odata_client]")
{
    std::cout << std::endl;

    auto http_client = std::make_shared<HttpClient>();
    auto url = HttpUrl("https://services.odata.org/V4/Northwind/Northwind.svc/Customers");
    
    ODataEntitySetClient client(http_client, url);

    auto response = client.Get();
    auto result_names = client.GetResultNames();
    auto result_types = client.GetResultTypes();
    auto rows = response->ToRows(result_names, result_types);

    REQUIRE(rows.size() == 20);
    REQUIRE(rows[0].size() == 11);
    REQUIRE(rows[19].size() == 11);

    REQUIRE(rows[0][0].ToString() == "ALFKI");
    REQUIRE(rows[19][0].ToString() == "ERNSH");
}


TEST_CASE("Test ODataServiceClient Get", "[odata_client]")
{
    std::cout << std::endl;

    auto http_client = std::make_shared<HttpClient>();
    auto url = HttpUrl("https://services.odata.org/V4/Northwind/Northwind.svc");
    
    ODataServiceClient client(http_client, url);
    
    auto metadata_context_url = client.GetMetadataContextUrl();
    REQUIRE(metadata_context_url == "https://services.odata.org/V4/Northwind/Northwind.svc/$metadata");

    auto response = client.Get();
    REQUIRE(response != nullptr);

    auto entity_sets = response->EntitySets();
    REQUIRE(entity_sets.size() == 26);
    REQUIRE(entity_sets[0].name == "Categories");
    REQUIRE(entity_sets[0].url == "Categories");
}
