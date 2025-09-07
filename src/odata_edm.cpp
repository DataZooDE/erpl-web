#include "odata_edm.hpp"
#include "http_client.hpp"

namespace erpl_web {

EdmCache& EdmCache::GetInstance() {
    static EdmCache instance;
    return instance;
}

duckdb::optional_ptr<Edmx> EdmCache::Get(const std::string& metadata_url) {
    std::lock_guard<std::mutex> lock(cache_lock);
    auto url_without_fragment = UrlWithoutFragment(metadata_url);
    auto it = cache.find(url_without_fragment);
    if (it != cache.end()) {
        return duckdb::optional_ptr<Edmx>(&(it->second));
    }
    return duckdb::optional_ptr<Edmx>();
}

void EdmCache::Set(const std::string& metadata_url, Edmx edmx) {
    std::lock_guard<std::mutex> lock(cache_lock);
    auto url_without_fragment = UrlWithoutFragment(metadata_url);
    cache[url_without_fragment] = std::move(edmx);
}

std::string EdmCache::UrlWithoutFragment(const std::string& url_str) const {
    std::stringstream ss;
    auto url = HttpUrl(url_str);
    ss << url.ToSchemeHostAndPort() << url.ToPathQuery();
    return ss.str();
}

// Version-specific parsing methods
Edmx Edmx::FromXmlV2(const std::string& xml) {
    tinyxml2::XMLDocument doc;
    tinyxml2::XMLError result = doc.Parse(xml.c_str());
    if (result != tinyxml2::XML_SUCCESS) {
        std::stringstream ss;
        ss << "Failed to parse XML [" << tinyxml2::XMLDocument::ErrorIDToName(result) << "]" << std::endl;   
        ss << "Description: " << doc.ErrorStr() << std::endl;
        ss << "Content: " << std::endl << xml << std::endl;
        throw std::runtime_error(ss.str());
    }
    return FromXmlV2(doc);
}

Edmx Edmx::FromXmlV4(const std::string& xml) {
    tinyxml2::XMLDocument doc;
    tinyxml2::XMLError result = doc.Parse(xml.c_str());
    if (result != tinyxml2::XML_SUCCESS) {
        std::stringstream ss;
        ss << "Failed to parse XML [" << tinyxml2::XMLDocument::ErrorIDToName(result) << "]" << std::endl;   
        ss << "Description: " << doc.ErrorStr() << std::endl;
        ss << "Content: " << std::endl << xml << std::endl;
        throw std::runtime_error(ss.str());
    }
    return FromXmlV4(doc);
}

Edmx Edmx::FromXmlV2(const tinyxml2::XMLDocument& doc) {
    Edmx edmx;
    edmx.SetVersion(ODataVersion::V2);
    
    const tinyxml2::XMLElement* edmx_el = doc.RootElement();
    if (edmx_el == nullptr) {
        throw std::runtime_error("Missing Edmx root element");
    }

    // Parse Edmx version
    const char* version_attr = edmx_el->Attribute("Version");
    if (version_attr) {
        edmx.version = version_attr;
    }

    // Parse DataServices element (v2 namespace)
    const tinyxml2::XMLElement* data_svc_el = edmx_el->FirstChildElement("edmx:DataServices");
    if (data_svc_el) {
        edmx.data_services = DataServices::FromXml(*data_svc_el);
    } else {
        // Try without namespace prefix (some v2 services don't use it)
        data_svc_el = edmx_el->FirstChildElement("DataServices");
        if (data_svc_el) {
            edmx.data_services = DataServices::FromXml(*data_svc_el);
        }
    }

    // Parse Reference elements
    for (const tinyxml2::XMLElement* ref_el = edmx_el->FirstChildElement("Reference");
        ref_el != nullptr;
        ref_el = ref_el->NextSiblingElement("Reference")) 
    {
        edmx.references.push_back(Reference::FromXml(*ref_el));
    }

    return edmx;
}

Edmx Edmx::FromXmlV4(const tinyxml2::XMLDocument& doc) {
    Edmx edmx;
    edmx.SetVersion(ODataVersion::V4);
    
    const tinyxml2::XMLElement* edmx_el = doc.RootElement();
    if (edmx_el == nullptr) {
        throw std::runtime_error("Missing Edmx root element");
    }

    // Parse Edmx version
    const char* version_attr = edmx_el->Attribute("Version");
    if (version_attr) {
        edmx.version = version_attr;
    }

    // Parse DataServices element (v4 namespace)
    const tinyxml2::XMLElement* data_svc_el = edmx_el->FirstChildElement("edmx:DataServices");
    if (data_svc_el) {
        edmx.data_services = DataServices::FromXml(*data_svc_el);
    }

    // Parse Reference elements
    for (const tinyxml2::XMLElement* ref_el = edmx_el->FirstChildElement("Reference");
        ref_el != nullptr;
        ref_el = ref_el->NextSiblingElement("Reference")) 
    {
        edmx.references.push_back(Reference::FromXml(*ref_el));
    }

    return edmx;
}

// Helper methods for v2 parsing
void Edmx::ParseV2Associations(const tinyxml2::XMLElement& element, Schema& schema) {
    // Parse Association elements and convert them to v4-style navigation properties
    for (const tinyxml2::XMLElement* association_el = element.FirstChildElement("Association");
        association_el != nullptr;
        association_el = association_el->NextSiblingElement("Association")) 
    {
        Association association = Association::FromXml(*association_el);
        schema.associations.push_back(association);
        
        // Convert v2 association to v4-style navigation properties
        if (association.ends.size() == 2 && !association.referential_constraints.empty()) {
            // Find the entity types involved in this association
            EntityType* entity_type1 = nullptr;
            EntityType* entity_type2 = nullptr;
            
            for (auto& et : schema.entity_types) {
                if (et.name == association.ends[0].type.substr(association.ends[0].type.find_last_of('.') + 1)) {
                    entity_type1 = &et;
                }
                if (et.name == association.ends[1].type.substr(association.ends[1].type.find_last_of('.') + 1)) {
                    entity_type2 = &et;
                }
            }
            
            if (entity_type1 && entity_type2) {
                // Create navigation properties for both entity types
                NavigationProperty nav_prop1;
                nav_prop1.name = "To" + entity_type2->name; // Simple naming convention
                nav_prop1.type = "Collection(" + entity_type2->name + ")";
                nav_prop1.nullable = true;
                
                NavigationProperty nav_prop2;
                nav_prop2.name = "To" + entity_type1->name;
                nav_prop2.type = entity_type1->name;
                nav_prop2.nullable = true;
                
                // Set partners
                nav_prop1.partner = nav_prop2.name;
                nav_prop2.partner = nav_prop1.name;
                
                // Add referential constraints
                for (const auto& constraint : association.referential_constraints) {
                    ReferentialConstraint v4_constraint;
                    v4_constraint.property = constraint.property;
                    v4_constraint.referenced_property = constraint.referenced_property;
                    nav_prop1.referential_constraints.push_back(v4_constraint);
                }
                
                entity_type1->navigation_properties.push_back(nav_prop1);
                entity_type2->navigation_properties.push_back(nav_prop2);
            }
        }
    }
}

void Edmx::ParseV2NavigationProperties(const tinyxml2::XMLElement& element, EntityType& entity_type) {
    // Parse v2-style navigation properties and convert them to v4 style
    for (const tinyxml2::XMLElement* nav_prop_el = element.FirstChildElement("NavigationProperty");
        nav_prop_el != nullptr;
        nav_prop_el = nav_prop_el->NextSiblingElement("NavigationProperty")) 
    {
        NavigationProperty nav_prop;
        
        // Parse Name attribute
        const char* name_attr = nav_prop_el->Attribute("Name");
        if (name_attr) {
            nav_prop.name = name_attr;
        }
        
        // Parse Relationship attribute (v2 specific)
        const char* relationship_attr = nav_prop_el->Attribute("Relationship");
        if (relationship_attr) {
            // Store relationship info for later processing
            // This will be used to link with Association definitions
        }
    
        nav_prop_el->Attribute("FromRole");
        nav_prop_el->Attribute("ToRole");
        entity_type.navigation_properties.push_back(nav_prop);
    }
}

} // namespace erpl_web
// -----------------------------------------------------------------------------
// ODataEdmTypeBuilder implementation
// -----------------------------------------------------------------------------
namespace erpl_web {

std::pair<bool, std::string> ODataEdmTypeBuilder::ResolveNavTargetOnEntity(const std::string &entity_type_name, const std::string &nav_prop) const {
    try {
        auto tv = edmx.FindType(entity_type_name);
        if (!std::holds_alternative<EntityType>(tv)) return {false, std::string()};
        const auto &entity = std::get<EntityType>(tv);
        for (const auto &np : entity.navigation_properties) {
            if (np.name == nav_prop) {
                auto [is_collection, type_name] = converter.ExtractCollectionType(np.type);
                return {is_collection, type_name};
            }
        }
    } catch (...) {}
    return {false, std::string()};
}

duckdb::LogicalType ODataEdmTypeBuilder::BuildEntityStruct(const std::string &entity_type_name) const {
    try {
        auto tv = edmx.FindType(entity_type_name);
        if (!std::holds_alternative<EntityType>(tv)) return duckdb::LogicalType::VARCHAR;
        const auto &entity = std::get<EntityType>(tv);
        return converter(entity);
    } catch (...) {
        return duckdb::LogicalType::VARCHAR;
    }
}

duckdb::LogicalType ODataEdmTypeBuilder::BuildExpandedColumnType(const std::string &root_entity_type_name,
                                                                 const std::string &top_nav_prop,
                                                                 const std::vector<std::string> &nested_children) const {
    // Resolve target of the top navigation property
    auto [is_collection, target_type_name] = ResolveNavTargetOnEntity(root_entity_type_name, top_nav_prop);
    if (target_type_name.empty()) {
        // Fallback aligns with previous behavior
        return is_collection ? duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR) : duckdb::LogicalType::VARCHAR;
    }

    // Base STRUCT for the target entity (properties only)
    auto base_struct = BuildEntityStruct(target_type_name);
    if (base_struct.id() != duckdb::LogicalTypeId::STRUCT) {
        base_struct = duckdb::LogicalType::VARCHAR;
    }

    // Augment with nested child navigation properties
    if (!nested_children.empty() && base_struct.id() == duckdb::LogicalTypeId::STRUCT) {
        auto children = duckdb::StructType::GetChildTypes(base_struct);
        std::set<std::string> seen;
        for (const auto &p : children) seen.insert(p.first);
        for (const auto &child_nav : nested_children) {
            if (seen.count(child_nav)) continue;
            auto [child_is_coll, child_target] = ResolveNavTargetOnEntity(target_type_name, child_nav);
            duckdb::LogicalType child_type = duckdb::LogicalType::VARCHAR;
            if (!child_target.empty()) {
                auto child_struct = BuildEntityStruct(child_target);
                child_type = child_is_coll ? duckdb::LogicalType::LIST(child_struct) : child_struct;
            } else {
                child_type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
            }
            children.emplace_back(child_nav, child_type);
            seen.insert(child_nav);
        }
        base_struct = duckdb::LogicalType::STRUCT(children);
    }

    // If the top nav itself is a collection, wrap result in LIST
    if (is_collection) {
        return duckdb::LogicalType::LIST(base_struct);
    }
    return base_struct;
}

} // namespace erpl_web