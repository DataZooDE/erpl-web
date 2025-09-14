#include "catch.hpp"

#include "odata_edm.hpp"
#include "duckdb.hpp"

using namespace erpl_web;

TEST_CASE("ODataEdmTypeBuilder - BuildExpandedColumnType for DefaultSystem with Services", "[odata][edm]") {
    // Create a minimal EDMX with DefaultSystem entity type and Services navigation property
    Edmx edmx;
    edmx.version = "4.0";
    
    Schema schema;
    schema.ns = "SAP.IWND";
    
    // Create DefaultSystem entity type
    EntityType default_system;
    default_system.name = "DefaultSystem";
    default_system.base_type = "";
    
    // Add some properties to DefaultSystem
    Property system_alias;
    system_alias.name = "SystemAlias";
    system_alias.type_name = "Edm.String";
    default_system.properties.push_back(system_alias);
    
    Property description;
    description.name = "Description";
    description.type_name = "Edm.String";
    default_system.properties.push_back(description);
    
    // Add Services navigation property (collection)
    NavigationProperty services_nav;
    services_nav.name = "Services";
    services_nav.type = "Collection(SAP.IWND.Service)";
    default_system.navigation_properties.push_back(services_nav);
    
    // Add a single navigation property for testing
    NavigationProperty single_nav;
    single_nav.name = "SingleService";
    single_nav.type = "SAP.IWND.Service";
    default_system.navigation_properties.push_back(single_nav);
    
    schema.entity_types.push_back(default_system);
    
    // Create Service entity type
    EntityType service;
    service.name = "Service";
    service.base_type = "";
    
    Property service_name;
    service_name.name = "Name";
    service_name.type_name = "Edm.String";
    service.properties.push_back(service_name);
    
    Property service_url;
    service_url.name = "Url";
    service_url.type_name = "Edm.String";
    service.properties.push_back(service_url);
    
    schema.entity_types.push_back(service);
    
    edmx.data_services.schemas.push_back(schema);
    
    // Test the builder
    ODataEdmTypeBuilder builder(edmx);
    
    SECTION("Build expanded column type for DefaultSystem with SingleService") {
        std::vector<std::string> nested_children = {};
        auto result_type = builder.BuildExpandedColumnType("DefaultSystem", "SingleService", nested_children);
        
        // Should be a STRUCT with Name and Url fields from Service entity
        REQUIRE(result_type.id() == duckdb::LogicalTypeId::STRUCT);
        
        auto child_types = duckdb::StructType::GetChildTypes(result_type);
        REQUIRE(child_types.size() == 2);
        
        // Check Name field
        auto name_it = std::find_if(child_types.begin(), child_types.end(),
            [](const auto& pair) { return pair.first == "Name"; });
        REQUIRE(name_it != child_types.end());
        REQUIRE(name_it->second.id() == duckdb::LogicalTypeId::VARCHAR);
        
        // Check Url field
        auto url_it = std::find_if(child_types.begin(), child_types.end(),
            [](const auto& pair) { return pair.first == "Url"; });
        REQUIRE(url_it != child_types.end());
        REQUIRE(url_it->second.id() == duckdb::LogicalTypeId::VARCHAR);
    }
    
    SECTION("Build expanded column type for collection navigation") {
        // Test when the navigation property itself is a collection
        std::vector<std::string> nested_children = {};
        auto result_type = builder.BuildExpandedColumnType("DefaultSystem", "Services", nested_children);
        
        // Should be LIST of STRUCT (Service entity)
        REQUIRE(result_type.id() == duckdb::LogicalTypeId::LIST);
        
        auto element_type = duckdb::ListType::GetChildType(result_type);
        REQUIRE(element_type.id() == duckdb::LogicalTypeId::STRUCT);
        
        auto child_types = duckdb::StructType::GetChildTypes(element_type);
        REQUIRE(child_types.size() == 2); // Name and Url
        
        auto name_it = std::find_if(child_types.begin(), child_types.end(),
            [](const auto& pair) { return pair.first == "Name"; });
        REQUIRE(name_it != child_types.end());
        
        auto url_it = std::find_if(child_types.begin(), child_types.end(),
            [](const auto& pair) { return pair.first == "Url"; });
        REQUIRE(url_it != child_types.end());
    }
    
    SECTION("Handle missing entity type gracefully") {
        std::vector<std::string> nested_children = {};
        auto result_type = builder.BuildExpandedColumnType("NonExistentEntity", "SomeNav", nested_children);
        
        // Should fall back to VARCHAR
        REQUIRE(result_type.id() == duckdb::LogicalTypeId::VARCHAR);
    }
    
    SECTION("Handle missing navigation property gracefully") {
        std::vector<std::string> nested_children = {};
        auto result_type = builder.BuildExpandedColumnType("DefaultSystem", "NonExistentNav", nested_children);
        
        // Should fall back to VARCHAR
        REQUIRE(result_type.id() == duckdb::LogicalTypeId::VARCHAR);
    }
}

TEST_CASE("ODataEdmTypeBuilder - ResolveNavTargetOnEntity", "[odata][edm]") {
    Edmx edmx;
    edmx.version = "4.0";
    
    Schema schema;
    schema.ns = "Test";
    
    EntityType entity;
    entity.name = "TestEntity";
    
    NavigationProperty single_nav;
    single_nav.name = "SingleNav";
    single_nav.type = "Test.SingleTarget";
    entity.navigation_properties.push_back(single_nav);
    
    NavigationProperty collection_nav;
    collection_nav.name = "CollectionNav";
    collection_nav.type = "Collection(Test.CollectionTarget)";
    entity.navigation_properties.push_back(collection_nav);
    
    schema.entity_types.push_back(entity);
    edmx.data_services.schemas.push_back(schema);
    
    ODataEdmTypeBuilder builder(edmx);
    
    SECTION("Resolve single navigation property") {
        auto [is_collection, type_name] = builder.ResolveNavTargetOnEntity("TestEntity", "SingleNav");
        REQUIRE_FALSE(is_collection);
        REQUIRE(type_name == "Test.SingleTarget");
    }
    
    SECTION("Resolve collection navigation property") {
        auto [is_collection, type_name] = builder.ResolveNavTargetOnEntity("TestEntity", "CollectionNav");
        REQUIRE(is_collection);
        REQUIRE(type_name == "Test.CollectionTarget");
    }
    
    SECTION("Handle missing navigation property") {
        auto [is_collection, type_name] = builder.ResolveNavTargetOnEntity("TestEntity", "MissingNav");
        REQUIRE_FALSE(is_collection);
        REQUIRE(type_name.empty());
    }
    
    SECTION("Handle missing entity type") {
        auto [is_collection, type_name] = builder.ResolveNavTargetOnEntity("MissingEntity", "SomeNav");
        REQUIRE_FALSE(is_collection);
        REQUIRE(type_name.empty());
    }
}

TEST_CASE("ODataEdmTypeBuilder - BuildEntityStruct", "[odata][edm]") {
    Edmx edmx;
    edmx.version = "4.0";
    
    Schema schema;
    schema.ns = "Test";
    
    EntityType entity;
    entity.name = "TestEntity";
    
    Property string_prop;
    string_prop.name = "StringProp";
    string_prop.type_name = "Edm.String";
    entity.properties.push_back(string_prop);
    
    Property int_prop;
    int_prop.name = "IntProp";
    int_prop.type_name = "Edm.Int32";
    entity.properties.push_back(int_prop);
    
    schema.entity_types.push_back(entity);
    edmx.data_services.schemas.push_back(schema);
    
    ODataEdmTypeBuilder builder(edmx);
    
    SECTION("Build entity struct with properties") {
        auto result_type = builder.BuildEntityStruct("TestEntity");
        
        REQUIRE(result_type.id() == duckdb::LogicalTypeId::STRUCT);
        
        auto child_types = duckdb::StructType::GetChildTypes(result_type);
        REQUIRE(child_types.size() == 2);
        
        auto string_it = std::find_if(child_types.begin(), child_types.end(),
            [](const auto& pair) { return pair.first == "StringProp"; });
        REQUIRE(string_it != child_types.end());
        REQUIRE(string_it->second.id() == duckdb::LogicalTypeId::VARCHAR);
        
        auto int_it = std::find_if(child_types.begin(), child_types.end(),
            [](const auto& pair) { return pair.first == "IntProp"; });
        REQUIRE(int_it != child_types.end());
        REQUIRE(int_it->second.id() == duckdb::LogicalTypeId::INTEGER);
    }
    
    SECTION("Handle missing entity type") {
        auto result_type = builder.BuildEntityStruct("MissingEntity");
        REQUIRE(result_type.id() == duckdb::LogicalTypeId::VARCHAR);
    }
}
