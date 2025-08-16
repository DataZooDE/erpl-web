#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>

namespace erpl_web {

// Datasphere Asset Types based on SAP's EDMX and OpenAPI schemas
namespace DatasphereTypes {

// Basic asset properties from AssetEntityV1 schema
struct AssetBasicInfo {
    std::string name;                           // Technical name of the asset
    std::string spaceName;                      // Technical name of the space
    std::string label;                          // User-friendly label
    std::string assetRelationalMetadataUrl;     // URL to relational metadata
    std::string assetRelationalDataUrl;         // URL to relational data service
    std::string assetAnalyticalMetadataUrl;     // URL to analytical metadata
    std::string assetAnalyticalDataUrl;         // URL to analytical data service
    std::string supportsAnalyticalQueries;      // Boolean as string
};

// OData context and metadata annotations
struct ODataContext {
    std::string odataContext;                   // The @odata.context value
    std::string odataMetadataEtag;              // The @odata.metadataEtag value
    std::string odataNextLink;                  // The @odata.nextLink for pagination
    std::string odataCount;                     // The @odata.count value
};

// Relational metadata structure (simplified for now)
struct RelationalMetadata {
    std::string entitySetName;                  // Name of the OData entity set
    std::string entityTypeName;                 // Name of the entity type
    std::vector<std::string> propertyNames;     // List of property names
    std::vector<std::string> propertyTypes;     // List of property types
    std::string metadataSummary;                // Human-readable summary
    bool isAvailable;                           // Whether relational access is available
};

// Analytical metadata structure (simplified for now)
struct AnalyticalMetadata {
    std::string cubeName;                       // Name of the analytical cube
    std::vector<std::string> dimensionNames;    // List of dimension names
    std::vector<std::string> measureNames;      // List of measure names
    std::vector<std::string> hierarchyNames;    // List of hierarchy names
    std::string metadataSummary;                // Human-readable summary
    bool isAvailable;                           // Whether analytical access is available
};

// Complete asset structure combining all components
struct AssetComplete {
    AssetBasicInfo basicInfo;
    ODataContext odataContext;
    RelationalMetadata relationalMetadata;
    AnalyticalMetadata analyticalMetadata;
    
    // Derived properties
    std::string assetType;                      // "Relational", "Analytical", "Multi-Modal"
    bool hasRelationalAccess;                   // Derived from relational metadata availability
    bool hasAnalyticalAccess;                   // Derived from analytical metadata availability
};

// Helper functions to create DuckDB struct types
duckdb::LogicalType CreateAssetBasicInfoType();
duckdb::LogicalType CreateODataContextType();
duckdb::LogicalType CreateRelationalMetadataType();
duckdb::LogicalType CreateAnalyticalMetadataType();
duckdb::LogicalType CreateAssetCompleteType();

// Helper functions to convert between C++ structs and DuckDB values
duckdb::Value AssetBasicInfoToValue(const AssetBasicInfo& info);
duckdb::Value ODataContextToValue(const ODataContext& context);
duckdb::Value RelationalMetadataToValue(const RelationalMetadata& metadata);
duckdb::Value AnalyticalMetadataToValue(const AnalyticalMetadata& metadata);
duckdb::Value AssetCompleteToValue(const AssetComplete& asset);

// Helper functions to extract values from DuckDB values
AssetBasicInfo ValueToAssetBasicInfo(const duckdb::Value& value);
ODataContext ValueToODataContext(const duckdb::Value& value);
RelationalMetadata ValueToRelationalMetadata(const duckdb::Value& value);
AnalyticalMetadata ValueToAnalyticalMetadata(const duckdb::Value& value);
AssetComplete ValueToAssetComplete(const duckdb::Value& value);

} // namespace DatasphereTypes

} // namespace erpl_web
