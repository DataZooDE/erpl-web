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

TEST_CASE("Test OData v2 NextUrl - no next link returns nullopt", "[odata_content_v2]")
{
    std::cout << std::endl;

    // Response without any pagination link
    std::string json_content = R"({
        "d": {
            "results": [
                {"ID": "1", "Name": "Foo"}
            ],
            "__delta": "https://example.com/sap/opu/odata/TYED/MySvc/MySet?!deltatoken='abc'"
        }
    })";

    ODataEntitySetJsonContent instance(json_content);
    REQUIRE_FALSE(instance.NextUrl().has_value());
}

TEST_CASE("Test OData v2 NextUrl - __next inside d wrapper (SAP ODP standard)", "[odata_content_v2]")
{
    std::cout << std::endl;

    // Real SAP ODP OData v2 pagination response: __next lives inside d, not at root.
    // Confirmed with production SAP PP_ADOPS_PRODUCTIONORDER_SR responses.
    std::string json_content = R"({
        "d": {
            "results": [
                {"ID": "1", "Name": "Foo"},
                {"ID": "2", "Name": "Bar"}
            ],
            "__next": "https://example.com/sap/opu/odata/TYED/MySvc/MySet?$format=json&$skiptoken=D20260415084410_000038000_0000000001_0000000010_0000000001"
        }
    })";

    ODataEntitySetJsonContent instance(json_content);
    auto next_url = instance.NextUrl();
    REQUIRE(next_url.has_value());
    REQUIRE(next_url.value() == "https://example.com/sap/opu/odata/TYED/MySvc/MySet?$format=json&$skiptoken=D20260415084410_000038000_0000000001_0000000010_0000000001");
}

TEST_CASE("Test OData v2 NextUrl - __next at root level (backward compat)", "[odata_content_v2]")
{
    std::cout << std::endl;

    // Non-standard placement: __next at root. Retained for backward compatibility.
    std::string json_content = R"({
        "d": {
            "results": [
                {"ID": "1", "Name": "Foo"}
            ]
        },
        "__next": "https://example.com/sap/opu/odata/TYED/MySvc/MySet?$skiptoken=page2"
    })";

    ODataEntitySetJsonContent instance(json_content);
    auto next_url = instance.NextUrl();
    REQUIRE(next_url.has_value());
    REQUIRE(next_url.value() == "https://example.com/sap/opu/odata/TYED/MySvc/MySet?$skiptoken=page2");
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

// ---------------------------------------------------------------------------------------------
// Regression coverage for the typed-column deserialization defects (#69, #70, #72, #76).
// The pre-existing coverage above is all-VARCHAR, which is exactly why these survived.
// ---------------------------------------------------------------------------------------------

TEST_CASE("Test Edm.Binary is decoded into a BLOB", "[odata_content]")
{
    std::cout << std::endl;

    // Northwind's Categories.Picture is an Edm.Binary, which the EDM mapper turns into BLOB.
    // Before the fix DeserializeJsonValue had no BLOB case, threw "Unsupported DuckDB type"
    // and ToRows swallowed it into a silent NULL - every Edm.Binary column read as NULL.
    std::string json_content = R"({
        "@odata.context": "https://services.odata.org/V4/Northwind/Northwind.svc/$metadata#Categories",
        "value": [
            { "CategoryName": "Beverages",   "Picture": "SGVsbG8=" },
            { "CategoryName": "Condiments",  "Picture": "SGVsbG8" },
            { "CategoryName": "Confections", "Picture": "-_8=" },
            { "CategoryName": "Dairy",       "Picture": null }
        ]
    })";

    ODataEntitySetJsonContent json_content_instance(json_content);

    std::vector<std::string> column_names = {"CategoryName", "Picture"};
    std::vector<duckdb::LogicalType> column_types = {duckdb::LogicalTypeId::VARCHAR, duckdb::LogicalTypeId::BLOB};

    auto rows = json_content_instance.ToRows(column_names, column_types);
    REQUIRE(rows.size() == 4);

    // Padded standard base64.
    REQUIRE_FALSE(rows[0][1].IsNull());
    REQUIRE(rows[0][1].type().id() == duckdb::LogicalTypeId::BLOB);
    REQUIRE(duckdb::StringValue::Get(rows[0][1]) == std::string("Hello"));

    // The same payload with the padding stripped.
    REQUIRE_FALSE(rows[1][1].IsNull());
    REQUIRE(duckdb::StringValue::Get(rows[1][1]) == std::string("Hello"));

    // base64url alphabet (RFC 4648 section 5), which the OData JSON format mandates.
    REQUIRE_FALSE(rows[2][1].IsNull());
    const auto &url_alphabet_bytes = duckdb::StringValue::Get(rows[2][1]);
    REQUIRE(url_alphabet_bytes.size() == 2);
    REQUIRE(static_cast<unsigned char>(url_alphabet_bytes[0]) == 0xFB);
    REQUIRE(static_cast<unsigned char>(url_alphabet_bytes[1]) == 0xFF);

    // A JSON null still maps to SQL NULL.
    REQUIRE(rows[3][1].IsNull());
}

TEST_CASE("Test Edm.Duration is decoded into an INTERVAL", "[odata_content]")
{
    std::cout << std::endl;

    // TripPin's Trips.Duration is an Edm.Duration, which the EDM mapper turns into INTERVAL.
    // Before the fix DeserializeJsonValue had no INTERVAL case, so every Edm.Duration read NULL.
    std::string json_content = R"({
        "@odata.context": "https://services.odata.org/V4/TripPinService/$metadata#Trips",
        "value": [
            { "TripId": "1", "Duration": "PT12H30M" },
            { "TripId": "2", "Duration": "P3DT4H" },
            { "TripId": "3", "Duration": "-PT1H" },
            { "TripId": "4", "Duration": "PT0.5S" },
            { "TripId": "5", "Duration": null }
        ]
    })";

    ODataEntitySetJsonContent json_content_instance(json_content);

    std::vector<std::string> column_names = {"TripId", "Duration"};
    std::vector<duckdb::LogicalType> column_types = {duckdb::LogicalTypeId::VARCHAR, duckdb::LogicalTypeId::INTERVAL};

    auto rows = json_content_instance.ToRows(column_names, column_types);
    REQUIRE(rows.size() == 5);

    constexpr int64_t MICROS_PER_HOUR = 3600000000LL;

    REQUIRE_FALSE(rows[0][1].IsNull());
    REQUIRE(rows[0][1].type().id() == duckdb::LogicalTypeId::INTERVAL);
    auto twelve_thirty = duckdb::IntervalValue::Get(rows[0][1]);
    REQUIRE(twelve_thirty.months == 0);
    REQUIRE(twelve_thirty.days == 0);
    REQUIRE(twelve_thirty.micros == 45000000000LL); // 12h30m

    REQUIRE_FALSE(rows[1][1].IsNull());
    auto three_days_four_hours = duckdb::IntervalValue::Get(rows[1][1]);
    REQUIRE(three_days_four_hours.months == 0);
    REQUIRE(three_days_four_hours.days == 3);
    REQUIRE(three_days_four_hours.micros == 4 * MICROS_PER_HOUR);

    REQUIRE_FALSE(rows[2][1].IsNull());
    auto negative_hour = duckdb::IntervalValue::Get(rows[2][1]);
    REQUIRE(negative_hour.months == 0);
    REQUIRE(negative_hour.days == 0);
    REQUIRE(negative_hour.micros == -MICROS_PER_HOUR);

    REQUIRE_FALSE(rows[3][1].IsNull());
    auto half_second = duckdb::IntervalValue::Get(rows[3][1]);
    REQUIRE(half_second.micros == 500000LL);

    REQUIRE(rows[4][1].IsNull());
}

TEST_CASE("Test unknown enum member does not silently become the first member", "[odata_content]")
{
    std::cout << std::endl;

    // OData v4 services add enum members over time. Before the fix an unmatched name fell through
    // the linear search with enum_index still 0, so "Weekend" was silently reported as "Sun" -
    // data corruption that is indistinguishable from a genuine value.
    auto members = duckdb::Vector(duckdb::LogicalType::VARCHAR, 3);
    members.SetValue(0, duckdb::Value("Sun"));
    members.SetValue(1, duckdb::Value("Mon"));
    members.SetValue(2, duckdb::Value("Tue"));
    auto weekday_enum = duckdb::LogicalType::ENUM("Weekday", members, 3);

    std::string json_content = R"({
        "@odata.context": "https://example.com/svc/$metadata#Shifts",
        "value": [
            { "ShiftId": "1", "Day": "Mon" },
            { "ShiftId": "2", "Day": "Weekend" },
            { "ShiftId": "3", "Day": null }
        ]
    })";

    ODataEntitySetJsonContent json_content_instance(json_content);

    std::vector<std::string> column_names = {"ShiftId", "Day"};
    std::vector<duckdb::LogicalType> column_types = {duckdb::LogicalTypeId::VARCHAR, weekday_enum};

    auto rows = json_content_instance.ToRows(column_names, column_types);
    REQUIRE(rows.size() == 3);

    // A declared member still resolves correctly.
    REQUIRE_FALSE(rows[0][1].IsNull());
    REQUIRE(rows[0][1].ToString() == "Mon");

    // The undeclared member must NOT be reported as "Sun". DeserializeJsonEnum now throws and
    // ToRows turns that into an honest NULL.
    REQUIRE(rows[1][1].IsNull());

    REQUIRE(rows[2][1].IsNull());
}

TEST_CASE("Test service document with a non-string property does not crash", "[odata_content]")
{
    std::cout << std::endl;

    // GetStringProperty used to call std::string(yyjson_get_str(...)) after only checking for
    // presence. yyjson_get_str returns NULL for any non-string node and std::string(nullptr) is
    // undefined behaviour, so this service document crashed the process.
    std::string numeric_name = R"({"value":[{"kind":"EntitySet","name":123,"url":"x"}]})";
    ODataServiceJsonContent numeric_name_instance(numeric_name);
    REQUIRE_THROWS_AS(numeric_name_instance.EntitySets(), std::runtime_error);

    std::string numeric_kind = R"({"value":[{"kind":42,"name":"Products","url":"Products"}]})";
    ODataServiceJsonContent numeric_kind_instance(numeric_kind);
    REQUIRE_THROWS_AS(numeric_kind_instance.EntitySets(), std::runtime_error);

    // A well-formed service document is still parsed.
    std::string valid = R"({"value":[{"kind":"EntitySet","name":"Products","url":"Products"}]})";
    ODataServiceJsonContent valid_instance(valid);
    auto entity_sets = valid_instance.EntitySets();
    REQUIRE(entity_sets.size() == 1);
    REQUIRE(entity_sets[0].name == "Products");
}

TEST_CASE("Test OData v2 /Date(ms)/ keeps millisecond precision", "[odata_content_v2]")
{
    std::cout << std::endl;

    // DuckDB TIMESTAMP is microsecond-precise, but the parser used to do `ms / 1000` and
    // Timestamp::FromEpochSeconds, discarding the milliseconds outright. Pre-1970 values were
    // additionally truncated towards zero.
    std::string json_content = R"({
        "d": {
            "results": [
                { "ID": "1", "Created": "/Date(1451606400123)/" },
                { "ID": "2", "Created": "/Date(-1500)/" }
            ]
        }
    })";

    ODataEntitySetJsonContent json_content_instance(json_content);
    json_content_instance.SetODataVersion(ODataVersion::V2);

    std::vector<std::string> column_names = {"ID", "Created"};
    std::vector<duckdb::LogicalType> column_types = {duckdb::LogicalTypeId::VARCHAR,
                                                     duckdb::LogicalTypeId::TIMESTAMP};

    auto rows = json_content_instance.ToRows(column_names, column_types);
    REQUIRE(rows.size() == 2);

    // 1451606400123 ms == 2016-01-01 00:00:00.123 UTC
    REQUIRE_FALSE(rows[0][1].IsNull());
    REQUIRE(duckdb::TimestampValue::Get(rows[0][1]).value == 1451606400123000LL);

    // -1500 ms == 1969-12-31 23:59:58.5, not 23:59:59 (truncation towards zero).
    REQUIRE_FALSE(rows[1][1].IsNull());
    REQUIRE(duckdb::TimestampValue::Get(rows[1][1]).value == -1500000LL);
}

TEST_CASE("Test OData v2 /Date(ms+HHMM)/ ignores the SAP offset suffix", "[odata_content_v2]")
{
    std::cout << std::endl;

    // SAP appends a local-time offset such as "+0060" to the literal. The epoch value in front of
    // it is ALREADY UTC, so the offset must be ignored - applying it would shift the timestamp.
    // This test locks that behaviour in: the result must be exactly the same instant as the
    // literal without the suffix.
    std::string json_content = R"({
        "d": {
            "results": [
                { "ID": "1", "Created": "/Date(1451606400123+0060)/" },
                { "ID": "2", "Created": "/Date(1451606400123-0300)/" }
            ]
        }
    })";

    ODataEntitySetJsonContent json_content_instance(json_content);
    json_content_instance.SetODataVersion(ODataVersion::V2);

    std::vector<std::string> column_names = {"ID", "Created"};
    std::vector<duckdb::LogicalType> column_types = {duckdb::LogicalTypeId::VARCHAR,
                                                     duckdb::LogicalTypeId::TIMESTAMP};

    auto rows = json_content_instance.ToRows(column_names, column_types);
    REQUIRE(rows.size() == 2);

    REQUIRE_FALSE(rows[0][1].IsNull());
    REQUIRE(duckdb::TimestampValue::Get(rows[0][1]).value == 1451606400123000LL);

    REQUIRE_FALSE(rows[1][1].IsNull());
    REQUIRE(duckdb::TimestampValue::Get(rows[1][1]).value == 1451606400123000LL);
}

TEST_CASE("Test OData v2 /Date(ms)/ rendered as VARCHAR keeps milliseconds", "[odata_content_v2]")
{
    std::cout << std::endl;

    // The same truncation existed a second time in DeserializeJsonString, which renders the
    // legacy literal into an ISO string for VARCHAR columns.
    std::string json_content = R"({
        "d": {
            "results": [
                { "ID": "1", "Created": "/Date(1451606400123)/" }
            ]
        }
    })";

    ODataEntitySetJsonContent json_content_instance(json_content);
    json_content_instance.SetODataVersion(ODataVersion::V2);

    std::vector<std::string> column_names = {"ID", "Created"};
    std::vector<duckdb::LogicalType> column_types = {duckdb::LogicalTypeId::VARCHAR,
                                                     duckdb::LogicalTypeId::VARCHAR};

    auto rows = json_content_instance.ToRows(column_names, column_types);
    REQUIRE(rows.size() == 1);
    REQUIRE(rows[0][1].ToString() == "2016-01-01 00:00:00.123");
}


// ---------------------------------------------------------------------------------------------
// Regression coverage for GitHub #77 (version detection from response headers, and surfacing the
// error the service itself reported) and GitHub #79 (an empty page that carries a next link).
// ---------------------------------------------------------------------------------------------

TEST_CASE("Test OData version is taken from the response headers", "[odata_content][version]")
{
    // OData v4 declares OData-Version; SAP Gateway appends a parameter list to it.
    erpl_web::HeaderMap v4_headers;
    v4_headers["OData-Version"] = "4.0";
    REQUIRE(ODataJsonContentMixin::DetectODataVersionFromHeaders(v4_headers) == ODataVersion::V4);

    erpl_web::HeaderMap sap_headers;
    sap_headers["OData-Version"] = "4.0;NetFx";
    REQUIRE(ODataJsonContentMixin::DetectODataVersionFromHeaders(sap_headers) == ODataVersion::V4);

    // v2 (and v3) services declare DataServiceVersion instead.
    erpl_web::HeaderMap v2_headers;
    v2_headers["DataServiceVersion"] = "2.0";
    REQUIRE(ODataJsonContentMixin::DetectODataVersionFromHeaders(v2_headers) == ODataVersion::V2);

    // Header names are case-insensitive on the wire.
    erpl_web::HeaderMap lowercase_headers;
    lowercase_headers["dataserviceversion"] = " 2.0 ";
    REQUIRE(ODataJsonContentMixin::DetectODataVersionFromHeaders(lowercase_headers) == ODataVersion::V2);

    // Nothing to go on must be reported as such, not guessed.
    erpl_web::HeaderMap no_version_headers;
    no_version_headers["Content-Type"] = "application/json";
    REQUIRE(ODataJsonContentMixin::DetectODataVersionFromHeaders(no_version_headers) == ODataVersion::UNKNOWN);
}

TEST_CASE("Test headers win over payload sniffing for version detection", "[odata_content][version]")
{
    // A body with no discriminator at all: payload sniffing cannot decide, so the version the
    // service declared has to be used instead of falling through to V4.
    const std::string inconclusive_body = R"({"error":{"code":"SY/530","message":{"lang":"en","value":"boom"}}})";
    REQUIRE(ODataJsonContentMixin::DetectODataVersionFromPayload(inconclusive_body) == ODataVersion::UNKNOWN);

    erpl_web::HeaderMap v2_headers;
    v2_headers["DataServiceVersion"] = "2.0";
    REQUIRE(ODataJsonContentMixin::DetectODataVersion(inconclusive_body, v2_headers) == ODataVersion::V2);

    // An empty body with a v2 header is still v2.
    REQUIRE(ODataJsonContentMixin::DetectODataVersion("", v2_headers) == ODataVersion::V2);

    // Without any header, the payload still decides.
    erpl_web::HeaderMap no_headers;
    const std::string v2_body = R"({"d":{"results":[{"CustomerID":"ALFKI"}]}})";
    REQUIRE(ODataJsonContentMixin::DetectODataVersion(v2_body, no_headers) == ODataVersion::V2);
}

TEST_CASE("Test OData v4 error payload is surfaced with code and message", "[odata_content][error]")
{
    // v4 shape: {"error":{"code":"...","message":"..."}}
    const std::string error_body =
        R"({"error":{"code":"Request_ResourceNotFound","message":"Resource 'Foo' does not exist."}})";

    ODataEntitySetJsonContent content(error_body);
    content.SetODataVersion(ODataVersion::V4);

    std::vector<std::string> column_names = {"CustomerID"};
    std::vector<duckdb::LogicalType> column_types = {duckdb::LogicalTypeId::VARCHAR};

    try {
        content.ToRows(column_names, column_types);
        FAIL("expected the service error to be raised");
    } catch (const std::exception &e) {
        const std::string message = e.what();
        INFO(message);
        REQUIRE(message.find("Request_ResourceNotFound") != std::string::npos);
        REQUIRE(message.find("Resource 'Foo' does not exist.") != std::string::npos);
    }
}

TEST_CASE("Test OData v2 error payload is surfaced with code and message", "[odata_content_v2][error]")
{
    // v2 shape: the message is an object, {"lang":"en","value":"..."}
    const std::string error_body =
        R"({"error":{"code":"SY/530","message":{"lang":"en","value":"Invalid filter on property Foo"},)"
        R"("innererror":{"application":{"service_id":"ZSVC"}}}})";

    ODataEntitySetJsonContent content(error_body);
    content.SetODataVersion(ODataVersion::V2);

    std::vector<std::string> column_names = {"CustomerID"};
    std::vector<duckdb::LogicalType> column_types = {duckdb::LogicalTypeId::VARCHAR};

    try {
        content.ToRows(column_names, column_types);
        FAIL("expected the service error to be raised");
    } catch (const std::exception &e) {
        const std::string message = e.what();
        INFO(message);
        REQUIRE(message.find("SY/530") != std::string::npos);
        REQUIRE(message.find("Invalid filter on property Foo") != std::string::npos);
    }
}

TEST_CASE("Test a v4 page without a value array but with a next link yields zero rows",
          "[odata_content][paging]")
{
    // Graph delta / skip-token pages do this. It is an empty page, not a failure.
    const std::string page =
        R"({"@odata.context":"https://example.com/$metadata#Customers",)"
        R"("@odata.nextLink":"https://example.com/Customers?$skiptoken=abc"})";

    ODataEntitySetJsonContent content(page);
    content.SetODataVersion(ODataVersion::V4);

    std::vector<std::string> column_names = {"CustomerID"};
    std::vector<duckdb::LogicalType> column_types = {duckdb::LogicalTypeId::VARCHAR};

    REQUIRE(content.ToRows(column_names, column_types).empty());
    REQUIRE(content.NextUrl().has_value());
}

TEST_CASE("Test a v2 page without a results array but with a next link yields zero rows",
          "[odata_content_v2][paging]")
{
    const std::string page =
        R"({"d":{"__next":"https://example.com/MySet?$skiptoken=page2"}})";

    ODataEntitySetJsonContent content(page);
    content.SetODataVersion(ODataVersion::V2);

    std::vector<std::string> column_names = {"CustomerID"};
    std::vector<duckdb::LogicalType> column_types = {duckdb::LogicalTypeId::VARCHAR};

    REQUIRE(content.ToRows(column_names, column_types).empty());
    REQUIRE(content.NextUrl().has_value());
}
