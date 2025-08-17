#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "erpl_odata_edm.hpp"
#include "erpl_odata_content.hpp"

using namespace erpl_web;
using namespace std;

TEST_CASE("Test OData Version Detection", "[odata_version_detection]")
{
    std::cout << std::endl;

    SECTION("Test v2 metadata parsing and version detection")
    {
        // Test OData v2 metadata parsing
        std::string v2_metadata = R"(
            <?xml version="1.0" encoding="utf-8"?>
            <edmx:Edmx Version="1.0" xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx">
                <edmx:DataServices xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" m:DataServiceVersion="2.0">
                    <Schema Namespace="NorthwindModel" xmlns="http://schemas.microsoft.com/ado/2008/09/edm">
                        <EntityType Name="Customer">
                            <Key>
                                <PropertyRef Name="CustomerID" />
                            </Key>
                            <Property Name="CustomerID" Type="Edm.String" Nullable="false" MaxLength="5" />
                            <Property Name="CompanyName" Type="Edm.String" Nullable="false" MaxLength="40" />
                            <Property Name="ContactName" Type="Edm.String" MaxLength="30" />
                            <Property Name="ContactTitle" Type="Edm.String" MaxLength="30" />
                            <Property Name="Address" Type="Edm.String" MaxLength="60" />
                            <Property Name="City" Type="Edm.String" MaxLength="15" />
                            <Property Name="Region" Type="Edm.String" MaxLength="15" />
                            <Property Name="PostalCode" Type="Edm.String" MaxLength="10" />
                            <Property Name="Country" Type="Edm.String" MaxLength="15" />
                            <Property Name="Phone" Type="Edm.String" MaxLength="24" />
                            <Property Name="Fax" Type="Edm.String" MaxLength="24" />
                            <NavigationProperty Name="Orders" Relationship="NorthwindModel.FK_Orders_Customers" FromRole="Customers" ToRole="Orders" />
                        </EntityType>
                        <Association Name="FK_Orders_Customers">
                            <End Type="NorthwindModel.Customer" Role="Customers" Multiplicity="1" />
                            <End Type="NorthwindModel.Order" Role="Orders" Multiplicity="*" />
                        </Association>
                    </Schema>
                    <EntityContainer Name="NorthwindEntities" m:IsDefaultEntityContainer="true">
                        <EntitySet Name="Customers" EntityType="NorthwindModel.Customer" />
                        <AssociationSet Name="FK_Orders_Customers" Association="NorthwindModel.FK_Orders_Customers">
                            <End Role="Customers" EntitySet="Customers" />
                            <End Role="Orders" EntitySet="Orders" />
                        </AssociationSet>
                    </EntityContainer>
                </edmx:DataServices>
            </edmx:Edmx>
        )";

        auto edmx = Edmx::FromXml(v2_metadata);
        
        // Verify version detection
        REQUIRE(edmx.GetVersion() == ODataVersion::V2);
        
        // Verify schema parsing
        REQUIRE(edmx.data_services.schemas.size() == 1);
        auto& schema = edmx.data_services.schemas[0];
        REQUIRE(schema.ns == "NorthwindModel");
        
        // Verify entity types
        REQUIRE(schema.entity_types.size() == 1);
        auto& customer = schema.entity_types[0];
        REQUIRE(customer.name == "Customer");
        REQUIRE(customer.properties.size() == 11);
        
        // Verify associations (v2 specific)
        REQUIRE(schema.associations.size() == 1);
        auto& association = schema.associations[0];
        REQUIRE(association.name == "FK_Orders_Customers");
        REQUIRE(association.ends.size() == 2);
        
        // Verify association sets
        REQUIRE(schema.entity_containers.size() == 1);
        auto& container = schema.entity_containers[0];
        REQUIRE(container.association_sets.size() == 1);
        auto& association_set = container.association_sets[0];
        REQUIRE(association_set.name == "FK_Orders_Customers");
        REQUIRE(association_set.ends.size() == 2);
    }

    SECTION("Test v4 metadata parsing and version detection")
    {
        // Test OData v4 metadata parsing
        std::string v4_metadata = R"(
            <?xml version="1.0" encoding="utf-8"?>
            <edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
                <edmx:DataServices>
                    <Schema Namespace="Microsoft.OData.SampleService.Models.TripPin" xmlns="http://docs.oasis-open.org/odata/ns/edm">
                        <EntityType Name="Person">
                            <Key>
                                <PropertyRef Name="UserName" />
                            </Key>
                            <Property Name="UserName" Type="Edm.String" Nullable="false" />
                            <Property Name="FirstName" Type="Edm.String" Nullable="false" />
                            <Property Name="LastName" Type="Edm.String" Nullable="false" />
                            <Property Name="Emails" Type="Collection(Edm.String)" />
                            <Property Name="AddressInfo" Type="Collection(Microsoft.OData.SampleService.Models.TripPin.Location)" />
                            <NavigationProperty Name="Friends" Type="Collection(Microsoft.OData.SampleService.Models.TripPin.Person)" />
                            <NavigationProperty Name="Trips" Type="Collection(Microsoft.OData.SampleService.Models.TripPin.Trip)" />
                        </EntityType>
                    </Schema>
                    <EntityContainer Name="DefaultContainer">
                        <EntitySet Name="People" EntityType="Microsoft.OData.SampleService.Models.TripPin.Person" />
                    </EntityContainer>
                </edmx:DataServices>
            </edmx:Edmx>
        )";

        auto edmx = Edmx::FromXml(v4_metadata);
        
        // Verify version detection
        REQUIRE(edmx.GetVersion() == ODataVersion::V4);
        
        // Verify schema parsing
        REQUIRE(edmx.data_services.schemas.size() == 1);
        auto& schema = edmx.data_services.schemas[0];
        REQUIRE(schema.ns == "Microsoft.OData.SampleService.Models.TripPin");
        
        // Verify entity types
        REQUIRE(schema.entity_types.size() == 1);
        auto& person = schema.entity_types[0];
        REQUIRE(person.name == "Person");
        REQUIRE(person.properties.size() == 5);
        
        // Verify navigation properties (v4 style)
        REQUIRE(person.navigation_properties.size() == 2);
        auto& friends_nav = person.navigation_properties[0];
        REQUIRE(friends_nav.name == "Friends");
        REQUIRE(friends_nav.type == "Collection(Microsoft.OData.SampleService.Models.TripPin.Person)");
        
        // Verify no associations (v4 doesn't use them)
        REQUIRE(schema.associations.empty());
        REQUIRE(schema.entity_containers[0].association_sets.empty());
    }

    SECTION("Test JSON content version detection")
    {
        // Test OData v2 JSON content
        std::string v2_json = R"({
            "d": {
                "results": [
                    {
                        "__metadata": {
                            "uri": "https://services.odata.org/V2/Northwind/Northwind.svc/Customers('ALFKI')",
                            "type": "NorthwindModel.Customer"
                        },
                        "CustomerID": "ALFKI",
                        "CompanyName": "Alfreds Futterkiste",
                        "ContactName": "Maria Anders"
                    }
                ]
            }
        })";

        auto v2_version = ODataJsonContentMixin::DetectODataVersion(v2_json);
        REQUIRE(v2_version == ODataVersion::V2);

        // Test OData v4 JSON content
        std::string v4_json = R"({
            "@odata.context": "https://services.odata.org/TripPinRESTierService/$metadata#People",
            "value": [
                {
                    "UserName": "russellwhyte",
                    "FirstName": "Russell",
                    "LastName": "Whyte",
                    "Emails": ["Russell@example.com", "Russell@contoso.com"]
                }
            ]
        })";

        auto v4_version = ODataJsonContentMixin::DetectODataVersion(v4_json);
        REQUIRE(v4_version == ODataVersion::V4);

        // Test fallback to v4 for unknown format
        std::string unknown_json = R"({
            "data": [
                {"id": 1, "name": "test"}
            ]
        })";

        auto unknown_version = ODataJsonContentMixin::DetectODataVersion(unknown_json);
        REQUIRE(unknown_version == ODataVersion::V4); // Default fallback
    }

    SECTION("Test mixed version scenarios")
    {
        // Test that v2 content can be parsed with v4 detection
        std::string v2_content = R"({
            "d": {
                "results": [
                    {"id": 1, "name": "test"}
                ]
            }
        })";

        auto content = std::make_shared<ODataEntitySetJsonContent>(v2_content);
        REQUIRE(content->GetODataVersion() == ODataVersion::V2);

        // Test that v4 content can be parsed with v2 detection
        std::string v4_content = R"({
            "@odata.context": "https://example.com/$metadata#EntitySet",
            "value": [
                {"id": 1, "name": "test"}
            ]
        })";

        auto content_v4 = std::make_shared<ODataEntitySetJsonContent>(v4_content);
        REQUIRE(content_v4->GetODataVersion() == ODataVersion::V4);
    }
}
