#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "erpl_odata_content.hpp"

using namespace erpl_web;
using namespace std;

TEST_CASE("Test ODataEntitySetJsonContent ToRows", "[odata_content]")
{
    std::cout << std::endl;

    // Sample JSON content representing an OData response
    std::string json_content = R"({
        "@odata.context": "https://services.odata.org/V4/Northwind/Northwind.svc/$metadata#Customers",
        "value": [
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
            {
                "CustomerID": "ANATR",
                "CompanyName": "Ana Trujillo Emparedados y helados",
                "ContactName": "Ana Trujillo",
                "ContactTitle": "Owner",
                "Address": "Av. de la Constitución 2222",
                "City": "México D.F.",
                "Region": null,
                "PostalCode": "05021",
                "Country": "Mexico",
                "Phone": "(5) 555-4729",
                "Fax": "(5) 555-3745"
            }
        ]
    })";

    // Create an instance of ODataEntitySetJsonContent
    ODataEntitySetJsonContent json_content_instance(json_content);

    // Define expected column names and types
    std::vector<std::string> expected_column_names = {
        "CustomerID", "CompanyName", "ContactName", "ContactTitle", 
        "Address", "City", "Region", "PostalCode", "Country", 
        "Phone", "Fax"
    };

    std::vector<duckdb::LogicalType> expected_column_types = {
        duckdb::LogicalTypeId::VARCHAR, duckdb::LogicalTypeId::VARCHAR,
        duckdb::LogicalTypeId::VARCHAR, duckdb::LogicalTypeId::VARCHAR,
        duckdb::LogicalTypeId::VARCHAR, duckdb::LogicalTypeId::VARCHAR,
        duckdb::LogicalTypeId::VARCHAR, duckdb::LogicalTypeId::VARCHAR,
        duckdb::LogicalTypeId::VARCHAR, duckdb::LogicalTypeId::VARCHAR,
        duckdb::LogicalTypeId::VARCHAR
    };

    // Convert JSON content to DuckDB rows
    auto rows = json_content_instance.ToRows(expected_column_names, expected_column_types);

    // Validate the number of rows
    REQUIRE(rows.size() == 2); // Expecting 2 entries in the sample JSON

    // Validate the first row
    REQUIRE(rows[0][0].ToString() == "ALFKI");
    REQUIRE(rows[0][1].ToString() == "Alfreds Futterkiste");
    REQUIRE(rows[0][2].ToString() == "Maria Anders");
    REQUIRE(rows[0][3].ToString() == "Sales Representative");
    REQUIRE(rows[0][4].ToString() == "Obere Str. 57");
    REQUIRE(rows[0][5].ToString() == "Berlin");
    REQUIRE(rows[0][6].IsNull()); // Region is null
    REQUIRE(rows[0][7].ToString() == "12209");
    REQUIRE(rows[0][8].ToString() == "Germany");
    REQUIRE(rows[0][9].ToString() == "030-0074321");
    REQUIRE(rows[0][10].ToString() == "030-0076545");

    // Validate the second row
    REQUIRE(rows[1][0].ToString() == "ANATR");
    REQUIRE(rows[1][1].ToString() == "Ana Trujillo Emparedados y helados");
    REQUIRE(rows[1][2].ToString() == "Ana Trujillo");
    REQUIRE(rows[1][3].ToString() == "Owner");
    REQUIRE(rows[1][4].ToString() == "Av. de la Constitución 2222");
    REQUIRE(rows[1][5].ToString() == "México D.F.");
    REQUIRE(rows[1][6].IsNull()); // Region is null
    REQUIRE(rows[1][7].ToString() == "05021");
    REQUIRE(rows[1][8].ToString() == "Mexico");
    REQUIRE(rows[1][9].ToString() == "(5) 555-4729");
    REQUIRE(rows[1][10].ToString() == "(5) 555-3745");
}

TEST_CASE("Test ODataServiceJsonContent get entity sets", "[odata_content]")
{
    std::cout << std::endl;

    std::string json_content = R"({
        "@odata.context": "https://services.odata.org/TripPinRESTierService/(S(jj44j3jieutp01qdhh0ep20b))/$metadata",
        "value": [
            {
                "kind": "EntitySet",
                "name": "People",
                "url": "People"
            },
            {
                "kind": "EntitySet",
                "name": "Airlines",
                "url": "Airlines"
            },
            {
                "kind": "EntitySet",
                "name": "Airports",
                "url": "Airports"
            },
            {
                "kind": "Singleton",
                "name": "Me",
                "url": "Me"
            }
        ]
    })";

    ODataServiceJsonContent json_content_instance(json_content);

    auto entity_sets = json_content_instance.EntitySets();
    REQUIRE(entity_sets.size() == 3);

    REQUIRE(entity_sets[0].name == "People");
    REQUIRE(entity_sets[0].url == "People");

    REQUIRE(entity_sets[1].name == "Airlines");
    REQUIRE(entity_sets[1].url == "Airlines");

    REQUIRE(entity_sets[2].name == "Airports");
    REQUIRE(entity_sets[2].url == "Airports");
}

TEST_CASE("Test ODataServiceReference MergeWithBaseUrlIfRelative", "[odata_content]")
{
    std::cout << std::endl;

    HttpUrl base_url("https://services.odata.org/TripPinRESTierService/");

    auto svc_ref = ODataEntitySetReference{ "People", "People" };
    svc_ref.MergeWithBaseUrlIfRelative(base_url);

    REQUIRE(svc_ref.url == "https://services.odata.org/TripPinRESTierService/People");

    svc_ref = ODataEntitySetReference{ "Airlines", "https://services.odata.org/MyOtherService/Airlines" };
    svc_ref.MergeWithBaseUrlIfRelative(base_url);

    REQUIRE(svc_ref.url == "https://services.odata.org/MyOtherService/Airlines");
}
