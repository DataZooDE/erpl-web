#include <fstream>

#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"

#include "erpl_odata_edm.hpp"

using namespace erpl_web;
using namespace std;

static std::string LoadTestFile(const std::string& filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filePath);
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    file.close();

    return buffer.str();
}

TEST_CASE("Test Property class", "[odata_edm]")
{
    SECTION("Test FromXml method")
    {
        // Create a sample XML element for testing
        const char *xml = R"(
            <Property Name="CategoryName" Type="Edm.String" Nullable="false" MaxLength="15" />
        )";

        tinyxml2::XMLDocument doc;
        doc.Parse(xml);
        tinyxml2::XMLElement *element = doc.FirstChildElement("Property");

        // Call the FromXml method
        Property property = Property::FromXml(*element);

        // Check the parsed values
        REQUIRE(property.name == "CategoryName");
        REQUIRE(property.type_name == "Edm.String");
        REQUIRE(property.nullable == false);
        REQUIRE(property.max_length == 15);
    }
}

TEST_CASE("Test NavigationProperty class", "[odata_edm]")
{
    SECTION("Test FromXml method")
    {
        // Create a sample XML element for testing
        const char *xml = R"~(
            <NavigationProperty 
                Name="Orders" 
                Type="Collection(NorthwindModel.Order)" 
                Partner="Customer" />
        )~";

        tinyxml2::XMLDocument doc;
        doc.Parse(xml);
        tinyxml2::XMLElement *element = doc.FirstChildElement("NavigationProperty");

        // Call the FromXml method
        NavigationProperty navigation_property = NavigationProperty::FromXml(*element);

        // Check the parsed values
        REQUIRE(navigation_property.name == "Orders");
        REQUIRE(navigation_property.type == "Collection(NorthwindModel.Order)");
        REQUIRE(navigation_property.partner == "Customer");
    }

    SECTION("Test FromXml with ReferentialConstraint")
    {
        // Create a sample XML element for testing
        const char *xml = R"~(
            <NavigationProperty Name="Category" Type="NorthwindModel.Category" Partner="Products">
                <ReferentialConstraint Property="CategoryID" ReferencedProperty="CategoryID" />
            </NavigationProperty>
        )~";

        tinyxml2::XMLDocument doc;
        doc.Parse(xml);
        tinyxml2::XMLElement *element = doc.FirstChildElement("NavigationProperty");

        // Call the FromXml method
        NavigationProperty navigation_property = NavigationProperty::FromXml(*element);

        // Check the parsed values
        REQUIRE(navigation_property.name == "Category");
        REQUIRE(navigation_property.type == "NorthwindModel.Category");
        REQUIRE(navigation_property.partner == "Products");
        REQUIRE(navigation_property.referential_constraints.size() == 1);
        REQUIRE(navigation_property.referential_constraints[0].property == "CategoryID");
        REQUIRE(navigation_property.referential_constraints[0].referenced_property == "CategoryID");
    }
}

TEST_CASE("Test Key class", "[odata_edm]")
{
    SECTION("Test FromXml method")
    {
        // Create a sample XML element for testing
        const char *xml = R"(
            <Key>
                <PropertyRef Name="CategoryID" />
            </Key>
        )";

        tinyxml2::XMLDocument doc;
        doc.Parse(xml);
        tinyxml2::XMLElement *element = doc.FirstChildElement("Key");

        // Call the FromXml method
        Key key = Key::FromXml(*element);

        // Check the parsed values
        REQUIRE(key.property_refs.size() == 1);
        REQUIRE(key.property_refs[0].name == "CategoryID");
    }
}

TEST_CASE("Test ComplexType class", "[odata_edm]")
{
    SECTION("Test FromXml method")
    {
        // Create a sample XML element for testing
        const char *xml = R"(
            <ComplexType Name="Location">
                <Property Name="Address" Type="Edm.String" />
                <Property Name="City" Type="Trippin.City" />
            </ComplexType>
        )";

        tinyxml2::XMLDocument doc;
        doc.Parse(xml);
        tinyxml2::XMLElement *element = doc.FirstChildElement("ComplexType");

        // Call the FromXml method
        ComplexType complex_type = ComplexType::FromXml(*element);

        // Check the parsed values
        REQUIRE(complex_type.name == "Location");
        REQUIRE(complex_type.properties.size() == 2);
        REQUIRE(complex_type.properties[0].name == "Address");
        REQUIRE(complex_type.properties[0].type_name == "Edm.String");
        REQUIRE(complex_type.properties[1].name == "City");
        REQUIRE(complex_type.properties[1].type_name == "Trippin.City");
    }
}

TEST_CASE("Test EntitySet class", "[odata_edm]")
{
    SECTION("Test FromXml method")
    {
        // Create a sample XML element for testing
        const char *xml = R"(
            <EntitySet Name="Products" EntityType="NorthwindModel.Product" />
        )";

        tinyxml2::XMLDocument doc;
        doc.Parse(xml);
        tinyxml2::XMLElement *element = doc.FirstChildElement("EntitySet");

        // Call the FromXml method
        EntitySet entity_set = EntitySet::FromXml(*element);

        // Check the parsed values
        REQUIRE(entity_set.name == "Products");
        REQUIRE(entity_set.entity_type_name == "NorthwindModel.Product");
    }
}

TEST_CASE("Test EntityType class", "[odata_edm]")
{
    SECTION("Test FromXml method 1")
    {
        // Create a sample XML element for testing
        const char *xml = R"~(
            <EntityType Name="Trip">
                <Key>
                    <PropertyRef Name="TripId" />
                </Key>
                <Property Name="TripId" Type="Edm.Int32" Nullable="false" />
                <Property Name="ShareId" Type="Edm.Guid" Nullable="false" />
                <Property Name="Name" Type="Edm.String" />
                <Property Name="Budget" Type="Edm.Single" Nullable="false" />
                <Property Name="Description" Type="Edm.String" />
                <Property Name="Tags" Type="Collection(Edm.String)" />
                <Property Name="StartsAt" Type="Edm.DateTimeOffset" Nullable="false" />
                <Property Name="EndsAt" Type="Edm.DateTimeOffset" Nullable="false" />
                <NavigationProperty Name="PlanItems" Type="Collection(Trippin.PlanItem)" />
            </EntityType>
        )~";

        tinyxml2::XMLDocument doc;
        doc.Parse(xml);
        tinyxml2::XMLElement *element = doc.FirstChildElement("EntityType");

        // Call the FromXml method
        EntityType entity_type = EntityType::FromXml(*element);

        // Check the parsed values
        REQUIRE(entity_type.name == "Trip");
        REQUIRE(entity_type.key.property_refs.size() == 1);
        REQUIRE(entity_type.key.property_refs[0].name == "TripId");
        REQUIRE(entity_type.properties.size() == 8);
        REQUIRE(entity_type.properties[0].name == "TripId");
        REQUIRE(entity_type.properties[0].type_name == "Edm.Int32");
        REQUIRE(entity_type.properties[0].nullable == false);
        REQUIRE(entity_type.navigation_properties.size() == 1);
        REQUIRE(entity_type.navigation_properties[0].name == "PlanItems");
        REQUIRE(entity_type.navigation_properties[0].type == "Collection(Trippin.PlanItem)");
    }

    SECTION("Test FromXml method 2")
    {
        // Create a sample XML element for testing
        const char *xml = R"~(
            <EntityType Name="Alphabetical_list_of_product">
                <Key>
                    <PropertyRef Name="CategoryName" />
                    <PropertyRef Name="Discontinued" />
                    <PropertyRef Name="ProductID" />
                    <PropertyRef Name="ProductName" />
                </Key>
                <Property Name="ProductID" Type="Edm.Int32" Nullable="false" />
                <Property Name="ProductName" Type="Edm.String" Nullable="false" MaxLength="40" />
                <Property Name="SupplierID" Type="Edm.Int32" />
                <Property Name="CategoryID" Type="Edm.Int32" />
                <Property Name="QuantityPerUnit" Type="Edm.String" MaxLength="20" />
                <Property Name="UnitPrice" Type="Edm.Decimal" Precision="19" Scale="4" />
                <Property Name="UnitsInStock" Type="Edm.Int16" />
                <Property Name="UnitsOnOrder" Type="Edm.Int16" />
                <Property Name="ReorderLevel" Type="Edm.Int16" />
                <Property Name="Discontinued" Type="Edm.Boolean" Nullable="false" />
                <Property Name="CategoryName" Type="Edm.String" Nullable="false" MaxLength="15" />
            </EntityType>
        )~";

        tinyxml2::XMLDocument doc;
        doc.Parse(xml);
        tinyxml2::XMLElement *element = doc.FirstChildElement("EntityType");

        // Call the FromXml method
        EntityType entity_type = EntityType::FromXml(*element);

        // Check the parsed values
        REQUIRE(entity_type.name == "Alphabetical_list_of_product");
        REQUIRE(entity_type.key.property_refs.size() == 4);
        REQUIRE(entity_type.key.property_refs[0].name == "CategoryName");
        REQUIRE(entity_type.properties.size() == 11);
        REQUIRE(entity_type.properties[0].name == "ProductID");
        REQUIRE(entity_type.properties[0].type_name == "Edm.Int32");
        REQUIRE(entity_type.properties[0].nullable == false);
    }
}

TEST_CASE("Test Edmx class", "[odata_edm]")
{
    SECTION("Test FromXml with Northwind")
    {
        // Create a sample XML element for testing
        auto xml = LoadTestFile("./test/cpp/edm_northwind.xml");
        auto edmx = Edmx::FromXml(xml);
        auto data_svc = edmx.data_services;

        // Check the parsed values
        REQUIRE(edmx.references.size() == 0);
        REQUIRE(data_svc.schemas.size() == 2);
        REQUIRE(data_svc.schemas[0].ns == "NorthwindModel");
        REQUIRE(data_svc.schemas[0].entity_types.size() == 26);
    }

    SECTION("Test FromXml with Trippin")
    {
        // Create a sample XML element for testing
        auto xml = LoadTestFile("./test/cpp/edm_trippin.xml");
        auto edmx = Edmx::FromXml(xml);
        auto data_svc = edmx.data_services;

        // Check the parsed values
        REQUIRE(edmx.references.size() == 0);
        REQUIRE(data_svc.schemas.size() == 1);
        REQUIRE(data_svc.schemas[0].ns == "Trippin");
        REQUIRE(data_svc.schemas[0].entity_types.size() == 10);
        REQUIRE(data_svc.schemas[0].complex_types.size() == 4);
        REQUIRE(data_svc.schemas[0].entity_containers.size() == 1);
        REQUIRE(data_svc.schemas[0].enum_types.size() == 2);
        REQUIRE(data_svc.schemas[0].functions.size() == 5);
    }

    SECTION("Test FromXml with SAP GWDEMO")
    {
        // Create a sample XML element for testing
        auto xml = LoadTestFile("./test/cpp/edm_sap_gsample_basic.xml");
        auto edmx = Edmx::FromXml(xml);
        auto data_svc = edmx.data_services;
    }

    SECTION("Test FromXml with Trippin RESTIER")
    {
        // Create a sample XML element for testing
        auto xml = LoadTestFile("./test/cpp/edm_trippin_restier.xml");
        auto edmx = Edmx::FromXml(xml);
        auto data_svc = edmx.data_services;
    }

    /*
    SECTION("Test FromXml with MSGraph")
    {
        // Create a sample XML element for testing
        auto xml = LoadTestFile("./test/cpp/edm_msgraph.xml");
        auto edmx = Edmx::FromXml(xml);
        auto data_svc = edmx.data_services;

        // Check the parsed values
    }
    */

    SECTION("Test EntitySet type resolution")
    {
        // Create a sample XML element for testing
        auto xml = LoadTestFile("./test/cpp/edm_trippin.xml");
        auto edmx = Edmx::FromXml(xml);
        auto type_conv = DuckTypeConverter(edmx);

        auto type = edmx.FindType("Edm.String");
        REQUIRE(std::get<PrimitiveType>(type).name == "Edm.String");
        REQUIRE(std::visit(type_conv, type) == duckdb::LogicalTypeId::VARCHAR);

        type = edmx.FindType("Trippin.PersonGender");
        REQUIRE(std::get<EnumType>(type).name == "PersonGender");
        auto duck_type = std::visit(type_conv, type);
        REQUIRE(duck_type.id() == duckdb::LogicalTypeId::ENUM);

        type = edmx.FindType("Trippin.Employee");
        REQUIRE(std::get<EntityType>(type).name == "Employee");
        duck_type = std::visit(type_conv, type);
        REQUIRE(duck_type.id() == duckdb::LogicalTypeId::STRUCT);
        REQUIRE(duck_type.ToString() == "STRUCT(UserName VARCHAR, FirstName VARCHAR, LastName VARCHAR, MiddleName VARCHAR, Gender ENUM('Male', 'Female', 'Unknown'), Age BIGINT, Emails VARCHAR[], AddressInfo STRUCT(Address VARCHAR, City STRUCT(\"Name\" VARCHAR, CountryRegion VARCHAR, Region VARCHAR))[], HomeAddress STRUCT(Address VARCHAR, City STRUCT(\"Name\" VARCHAR, CountryRegion VARCHAR, Region VARCHAR)), FavoriteFeature ENUM('Feature1', 'Feature2', 'Feature3', 'Feature4'), Features ENUM('Feature1', 'Feature2', 'Feature3', 'Feature4')[], \"Cost\" BIGINT)");

        type = edmx.FindType("Trippin.AirportLocation");
        REQUIRE(std::get<ComplexType>(type).name == "AirportLocation");
        duck_type = std::visit(type_conv, type);
        REQUIRE(duck_type.id() == duckdb::LogicalTypeId::STRUCT);
        REQUIRE(duck_type.ToString() == "STRUCT(Address VARCHAR, City STRUCT(\"Name\" VARCHAR, CountryRegion VARCHAR, Region VARCHAR), Loc VARCHAR)");
    }
}