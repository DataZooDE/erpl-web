#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "odata_content.hpp"

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

// ============================================================================
// OData v2 Support Tests
// ============================================================================

TEST_CASE("Test OData v2 EntitySet JSON Content", "[odata_content_v2]")
{
    std::cout << std::endl;

    // Sample OData v2 JSON content with "d" wrapper and "results" array
    std::string json_content_v2 = "{\n"
        "    \"d\": {\n"
        "        \"results\": [\n"
        "            {\n"
        "                \"__metadata\": {\n"
        "                    \"uri\": \"https://services.odata.org/V2/Northwind/Northwind.svc/Customers('ALFKI')\",\n"
        "                    \"type\": \"NorthwindModel.Customer\"\n"
        "                },\n"
        "                \"CustomerID\": \"ALFKI\",\n"
        "                \"CompanyName\": \"Alfreds Futterkiste\",\n"
        "                \"ContactName\": \"Maria Anders\",\n"
        "                \"ContactTitle\": \"Sales Representative\",\n"
        "                \"Address\": \"Obere Str. 57\",\n"
        "                \"City\": \"Berlin\",\n"
        "                \"Region\": null,\n"
        "                \"PostalCode\": \"12209\",\n"
        "                \"Country\": \"Germany\",\n"
        "                \"Phone\": \"030-0074321\",\n"
        "                \"Fax\": \"030-0076545\"\n"
        "            },\n"
        "            {\n"
        "                \"__metadata\": {\n"
        "                    \"uri\": \"https://services.odata.org/V2/Northwind/Northwind.svc/Customers('ANATR')\",\n"
        "                    \"type\": \"NorthwindModel.Customer\"\n"
        "                },\n"
        "                \"CustomerID\": \"ANATR\",\n"
        "                \"CompanyName\": \"Ana Trujillo Emparedados y helados\",\n"
        "                \"ContactName\": \"Ana Trujillo\",\n"
        "                \"ContactTitle\": \"Owner\",\n"
        "                \"Address\": \"Av. de la Constitución 2222\",\n"
        "                \"City\": \"México D.F.\",\n"
        "                \"Region\": null,\n"
        "                \"PostalCode\": \"05021\",\n"
        "                \"Country\": \"Mexico\",\n"
        "                \"Phone\": \"(5) 555-4729\",\n"
        "                \"Fax\": \"(5) 555-3745\"\n"
        "            }\n"
        "        ]\n"
        "    }\n"
        "}";

    // Create an instance and set OData version to v2
    ODataEntitySetJsonContent json_content_instance(json_content_v2);
    json_content_instance.SetODataVersion(ODataVersion::V2);

    // Verify version is set correctly
    REQUIRE(json_content_instance.GetODataVersion() == ODataVersion::V2);

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
    REQUIRE(rows.size() == 2);

    // Validate the first row (should ignore __metadata)
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

TEST_CASE("Test OData v2 Service JSON Content", "[odata_content_v2]")
{
    std::cout << std::endl;

    // Sample OData v2 service document JSON
    std::string json_content_v2 = "{\n"
        "    \"d\": {\n"
        "        \"EntitySets\": [\n"
        "            \"Customers\",\n"
        "            \"Orders\",\n"
        "            \"Products\"\n"
        "        ]\n"
        "    }\n"
        "}";

    // Create an instance and set OData version to v2
    ODataServiceJsonContent json_content_instance(json_content_v2);
    json_content_instance.SetODataVersion(ODataVersion::V2);

    // Verify version is set correctly
    REQUIRE(json_content_instance.GetODataVersion() == ODataVersion::V2);

    auto entity_sets = json_content_instance.EntitySets();
    REQUIRE(entity_sets.size() == 3);

    REQUIRE(entity_sets[0].name == "Customers");
    REQUIRE(entity_sets[0].url == "Customers");

    REQUIRE(entity_sets[1].name == "Orders");
    REQUIRE(entity_sets[1].url == "Orders");

    REQUIRE(entity_sets[2].name == "Products");
    REQUIRE(entity_sets[2].url == "Products");
}

TEST_CASE("Test OData v2 Context URL extraction", "[odata_content_v2]")
{
    std::cout << std::endl;

    // Sample OData v2 JSON with context URL
    std::string json_content_v2 = "{\n"
        "    \"@odata.context\": \"https://services.odata.org/V2/Northwind/Northwind.svc/$metadata#Customers\",\n"
        "    \"d\": {\n"
        "        \"results\": [\n"
        "            {\n"
        "                \"CustomerID\": \"ALFKI\",\n"
        "                \"CompanyName\": \"Alfreds Futterkiste\"\n"
        "            }\n"
        "        ]\n"
        "    }\n"
        "}";

    // Create an instance and set OData version to v2
    ODataEntitySetJsonContent json_content_instance(json_content_v2);
    json_content_instance.SetODataVersion(ODataVersion::V2);

    // Test context URL extraction
    auto context_url = json_content_instance.MetadataContextUrl();
    REQUIRE(context_url == "https://services.odata.org/V2/Northwind/Northwind.svc/$metadata#Customers");
}

TEST_CASE("Test OData v2 Context URL in d wrapper", "[odata_content_v2]")
{
    std::cout << std::endl;

    // Sample OData v2 JSON with context URL inside d wrapper
    std::string json_content_v2 = "{\n"
        "    \"d\": {\n"
        "        \"results\": [\n"
        "            {\n"
        "                \"CustomerID\": \"ALFKI\",\n"
        "                \"CompanyName\": \"Alfreds Futterkiste\"\n"
        "            }\n"
        "        ]\n"
        "    }\n"
        "}";

    // Create an instance and set OData version to v2
    ODataEntitySetJsonContent json_content_instance(json_content_v2);
    json_content_instance.SetODataVersion(ODataVersion::V2);

}

TEST_CASE("Test OData v2 Error handling - missing d wrapper", "[odata_content_v2]")
{
    std::cout << std::endl;

    // Sample JSON missing the "d" wrapper (invalid v2 format)
    std::string invalid_json_v2 = "{\n"
        "    \"value\": [\n"
        "        {\n"
        "            \"CustomerID\": \"ALFKI\",\n"
        "            \"CompanyName\": \"Alfreds Futterkiste\"\n"
        "        }\n"
        "    ]\n"
        "}";

    // Create an instance and set OData version to v2
    ODataEntitySetJsonContent json_content_instance(invalid_json_v2);
    json_content_instance.SetODataVersion(ODataVersion::V2);

    // This should throw an error when trying to parse
    std::vector<std::string> column_names = {"CustomerID", "CompanyName"};
    std::vector<duckdb::LogicalType> column_types = {duckdb::LogicalTypeId::VARCHAR, duckdb::LogicalTypeId::VARCHAR};

    REQUIRE_THROWS_AS(json_content_instance.ToRows(column_names, column_types), std::runtime_error);
}

TEST_CASE("Test OData v2 Error handling - missing results array", "[odata_content_v2]")
{
    std::cout << std::endl;

    // Sample JSON with d wrapper but missing results array
    std::string invalid_json_v2 = "{\n"
        "    \"d\": {\n"
        "        \"value\": [\n"
        "            {\n"
        "                \"CustomerID\": \"ALFKI\",\n"
        "                \"CompanyName\": \"Alfreds Futterkiste\"\n"
        "            }\n"
        "        ]\n"
        "    }\n"
        "}";

    // Create an instance and set OData version to v2
    ODataEntitySetJsonContent json_content_instance(invalid_json_v2);
    json_content_instance.SetODataVersion(ODataVersion::V2);

    // This should throw an error when trying to parse
    std::vector<std::string> column_names = {"CustomerID", "CompanyName"};
    std::vector<duckdb::LogicalType> column_types = {duckdb::LogicalTypeId::VARCHAR, duckdb::LogicalTypeId::VARCHAR};

    REQUIRE_THROWS_AS(json_content_instance.ToRows(column_names, column_types), std::runtime_error);
}
