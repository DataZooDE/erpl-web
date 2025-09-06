#include "datasphere_types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/value.hpp"
#include <sstream>

namespace erpl_web {
namespace DatasphereTypes {

// Create DuckDB struct types for each component
duckdb::LogicalType CreateAssetBasicInfoType() {
    duckdb::child_list_t<duckdb::LogicalType> children;
    children.emplace_back("name", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("spaceName", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("label", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("assetRelationalMetadataUrl", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("assetRelationalDataUrl", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("assetAnalyticalMetadataUrl", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("assetAnalyticalDataUrl", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("supportsAnalyticalQueries", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    
    return duckdb::LogicalType::STRUCT(std::move(children));
}

duckdb::LogicalType CreateODataContextType() {
    duckdb::child_list_t<duckdb::LogicalType> children;
    children.emplace_back("odataContext", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("odataMetadataEtag", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("odataNextLink", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("odataCount", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    
    return duckdb::LogicalType::STRUCT(std::move(children));
}

duckdb::LogicalType CreateRelationalMetadataType() {
    duckdb::child_list_t<duckdb::LogicalType> children;
    children.emplace_back("entitySetName", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("entityTypeName", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("propertyNames", duckdb::LogicalType::LIST(duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)));
    children.emplace_back("propertyTypes", duckdb::LogicalType::LIST(duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)));
    children.emplace_back("metadataSummary", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("isAvailable", duckdb::LogicalType(duckdb::LogicalTypeId::BOOLEAN));
    
    return duckdb::LogicalType::STRUCT(std::move(children));
}

duckdb::LogicalType CreateAnalyticalMetadataType() {
    duckdb::child_list_t<duckdb::LogicalType> children;
    children.emplace_back("cubeName", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("dimensionNames", duckdb::LogicalType::LIST(duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)));
    children.emplace_back("measureNames", duckdb::LogicalType::LIST(duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)));
    children.emplace_back("hierarchyNames", duckdb::LogicalType::LIST(duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)));
    children.emplace_back("metadataSummary", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("isAvailable", duckdb::LogicalType(duckdb::LogicalTypeId::BOOLEAN));
    
    return duckdb::LogicalType::STRUCT(std::move(children));
}

duckdb::LogicalType CreateAssetCompleteType() {
    duckdb::child_list_t<duckdb::LogicalType> children;
    children.emplace_back("basicInfo", CreateAssetBasicInfoType());
    children.emplace_back("odataContext", CreateODataContextType());
    children.emplace_back("relationalMetadata", CreateRelationalMetadataType());
    children.emplace_back("analyticalMetadata", CreateAnalyticalMetadataType());
    children.emplace_back("assetType", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("hasRelationalAccess", duckdb::LogicalType(duckdb::LogicalTypeId::BOOLEAN));
    children.emplace_back("hasAnalyticalAccess", duckdb::LogicalType(duckdb::LogicalTypeId::BOOLEAN));
    
    return duckdb::LogicalType::STRUCT(std::move(children));
}

// Convert C++ structs to DuckDB values
duckdb::Value AssetBasicInfoToValue(const AssetBasicInfo& info) {
    duckdb::child_list_t<duckdb::Value> children;
    children.emplace_back("name", duckdb::Value(info.name));
    children.emplace_back("spaceName", duckdb::Value(info.spaceName));
    children.emplace_back("label", duckdb::Value(info.label));
    children.emplace_back("assetRelationalMetadataUrl", duckdb::Value(info.assetRelationalMetadataUrl));
    children.emplace_back("assetRelationalDataUrl", duckdb::Value(info.assetRelationalDataUrl));
    children.emplace_back("assetAnalyticalMetadataUrl", duckdb::Value(info.assetAnalyticalMetadataUrl));
    children.emplace_back("assetAnalyticalDataUrl", duckdb::Value(info.assetAnalyticalDataUrl));
    children.emplace_back("supportsAnalyticalQueries", duckdb::Value(info.supportsAnalyticalQueries));
    
    return duckdb::Value::STRUCT(std::move(children));
}

duckdb::Value ODataContextToValue(const ODataContext& context) {
    duckdb::child_list_t<duckdb::Value> children;
    children.emplace_back("odataContext", duckdb::Value(context.odataContext));
    children.emplace_back("odataMetadataEtag", duckdb::Value(context.odataMetadataEtag));
    children.emplace_back("odataNextLink", duckdb::Value(context.odataNextLink));
    children.emplace_back("odataCount", duckdb::Value(context.odataCount));
    
    return duckdb::Value::STRUCT(std::move(children));
}

duckdb::Value RelationalMetadataToValue(const RelationalMetadata& metadata) {
    // Convert string vectors to DuckDB list values
    duckdb::vector<duckdb::Value> property_names_values;
    for (const auto& name : metadata.propertyNames) {
        property_names_values.push_back(duckdb::Value(name));
    }
    
    duckdb::vector<duckdb::Value> property_types_values;
    for (const auto& type : metadata.propertyTypes) {
        property_types_values.push_back(duckdb::Value(type));
    }
    
    duckdb::child_list_t<duckdb::Value> children;
    children.emplace_back("entitySetName", duckdb::Value(metadata.entitySetName));
    children.emplace_back("entityTypeName", duckdb::Value(metadata.entityTypeName));
    children.emplace_back("propertyNames", duckdb::Value::LIST(duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR), std::move(property_names_values)));
    children.emplace_back("propertyTypes", duckdb::Value::LIST(duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR), std::move(property_types_values)));
    children.emplace_back("metadataSummary", duckdb::Value(metadata.metadataSummary));
    children.emplace_back("isAvailable", duckdb::Value::BOOLEAN(metadata.isAvailable));
    
    return duckdb::Value::STRUCT(std::move(children));
}

duckdb::Value AnalyticalMetadataToValue(const AnalyticalMetadata& metadata) {
    // Convert string vectors to DuckDB list values
    duckdb::vector<duckdb::Value> dimension_names_values;
    for (const auto& name : metadata.dimensionNames) {
        dimension_names_values.push_back(duckdb::Value(name));
    }
    
    duckdb::vector<duckdb::Value> measure_names_values;
    for (const auto& name : metadata.measureNames) {
        measure_names_values.push_back(duckdb::Value(name));
    }
    
    duckdb::vector<duckdb::Value> hierarchy_names_values;
    for (const auto& name : metadata.hierarchyNames) {
        hierarchy_names_values.push_back(duckdb::Value(name));
    }
    
    duckdb::child_list_t<duckdb::Value> children;
    children.emplace_back("cubeName", duckdb::Value(metadata.cubeName));
    children.emplace_back("dimensionNames", duckdb::Value::LIST(duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR), std::move(dimension_names_values)));
    children.emplace_back("measureNames", duckdb::Value::LIST(duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR), std::move(measure_names_values)));
    children.emplace_back("hierarchyNames", duckdb::Value::LIST(duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR), std::move(hierarchy_names_values)));
    children.emplace_back("metadataSummary", duckdb::Value(metadata.metadataSummary));
    children.emplace_back("isAvailable", duckdb::Value::BOOLEAN(metadata.isAvailable));
    
    return duckdb::Value::STRUCT(std::move(children));
}

duckdb::Value AssetCompleteToValue(const AssetComplete& asset) {
    duckdb::child_list_t<duckdb::Value> children;
    children.emplace_back("basicInfo", AssetBasicInfoToValue(asset.basicInfo));
    children.emplace_back("odataContext", ODataContextToValue(asset.odataContext));
    children.emplace_back("relationalMetadata", RelationalMetadataToValue(asset.relationalMetadata));
    children.emplace_back("analyticalMetadata", AnalyticalMetadataToValue(asset.analyticalMetadata));
    children.emplace_back("assetType", duckdb::Value(asset.assetType));
    children.emplace_back("hasRelationalAccess", duckdb::Value::BOOLEAN(asset.hasRelationalAccess));
    children.emplace_back("hasAnalyticalAccess", duckdb::Value::BOOLEAN(asset.hasAnalyticalAccess));
    
    return duckdb::Value::STRUCT(std::move(children));
}

// Helper function to create a simple flat schema for backward compatibility
duckdb::LogicalType CreateFlatAssetSchemaType() {
    duckdb::child_list_t<duckdb::LogicalType> children;
    // Basic fields (8)
    children.emplace_back("name", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("spaceName", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("label", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("assetRelationalMetadataUrl", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("assetRelationalDataUrl", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("assetAnalyticalMetadataUrl", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("assetAnalyticalDataUrl", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("supportsAnalyticalQueries", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    
    // OData context fields (4)
    children.emplace_back("odataContext", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("odataMetadataEtag", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("odataNextLink", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("odataCount", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    
    // Relational metadata fields (6)
    children.emplace_back("relationalEntitySetName", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("relationalEntityTypeName", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("relationalPropertyNames", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)); // JSON string for now
    children.emplace_back("relationalPropertyTypes", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)); // JSON string for now
    children.emplace_back("relationalMetadataSummary", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("relationalIsAvailable", duckdb::LogicalType(duckdb::LogicalTypeId::BOOLEAN));
    
    // Analytical metadata fields (6)
    children.emplace_back("analyticalCubeName", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("analyticalDimensionNames", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)); // JSON string for now
    children.emplace_back("analyticalMeasureNames", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));   // JSON string for now
    children.emplace_back("analyticalHierarchyNames", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)); // JSON string for now
    children.emplace_back("analyticalMetadataSummary", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("analyticalIsAvailable", duckdb::LogicalType(duckdb::LogicalTypeId::BOOLEAN));
    
    // Derived fields (3)
    children.emplace_back("assetType", duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR));
    children.emplace_back("hasRelationalAccess", duckdb::LogicalType(duckdb::LogicalTypeId::BOOLEAN));
    children.emplace_back("hasAnalyticalAccess", duckdb::LogicalType(duckdb::LogicalTypeId::BOOLEAN));
    
    return duckdb::LogicalType::STRUCT(std::move(children));
}

} // namespace DatasphereTypes
} // namespace erpl_web
