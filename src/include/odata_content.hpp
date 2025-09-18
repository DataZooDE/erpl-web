#pragma once

#include "odata_edm.hpp"
#include "http_client.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace erpl_web {

// -------------------------------------------------------------------------------------------------

class ODataContent {
public:
    virtual ~ODataContent() = default;
    virtual std::string MetadataContextUrl() = 0;

    virtual void PrettyPrint() = 0;
};

class ODataEntitySetContent : public ODataContent {
public:
    virtual ~ODataEntitySetContent() = default;
    virtual std::optional<std::string> NextUrl() = 0;
    virtual std::vector<std::vector<duckdb::Value>> ToRows(std::vector<std::string> &column_names, 
                                                           std::vector<duckdb::LogicalType> &column_types) = 0;
    // Optional total row count for OData v4 when $count=true is used
    virtual std::optional<uint64_t> TotalCount() { return std::nullopt; }
};

struct ODataEntitySetReference {
    std::string name;
    std::string url;

    void MergeWithBaseUrlIfRelative(const HttpUrl &base_url) {
        url = HttpUrl::MergeWithBaseUrlIfRelative(base_url, url);
    }
};

class ODataServiceContent : public ODataContent {
public:
    virtual std::vector<ODataEntitySetReference> EntitySets() = 0;
};

// -------------------------------------------------------------------------------------------------
class ODataJsonContentMixin {
public:
    static bool IsJsonContentType(const std::string& content_type);

    ODataJsonContentMixin(const std::string& content);

    // OData version support
    void SetODataVersion(ODataVersion version) { odata_version = version; }
    ODataVersion GetODataVersion() const { return odata_version; }
    
    // Auto-detect OData version from JSON content
    static ODataVersion DetectODataVersion(const std::string& content);

protected:
    std::shared_ptr<yyjson_doc> doc;
    ODataVersion odata_version = ODataVersion::V4; // Default to v4 for backward compatibility

    void ThrowTypeError(yyjson_val *json_value, const std::string &expected);
    void PrettyPrint();
    std::string MetadataContextUrl();
    std::optional<std::string> NextUrl();
    
    // Version-aware JSON parsing methods
    yyjson_val* GetValueArray(yyjson_val* root);
    std::string GetMetadataContextUrl(yyjson_val* root);
    std::optional<std::string> GetNextUrl(yyjson_val* root);
    
    duckdb::Value DeserializeJsonValue(yyjson_val *json_value, const duckdb::LogicalType &duck_type);
    duckdb::Value DeserializeJsonBool(yyjson_val *json_value);
    duckdb::Value DeserializeJsonSignedInt8(yyjson_val *json_value);
    duckdb::Value DeserializeJsonUnsignedInt8(yyjson_val *json_value);
    duckdb::Value DeserializeJsonSignedInt16(yyjson_val *json_value);
    duckdb::Value DeserializeJsonUnsignedInt16(yyjson_val *json_value);
    duckdb::Value DeserializeJsonSignedInt32(yyjson_val *json_value);
    duckdb::Value DeserializeJsonUnsignedInt32(yyjson_val *json_value);
    duckdb::Value DeserializeJsonSignedInt64(yyjson_val *json_value);
    duckdb::Value DeserializeJsonUnsignedInt64(yyjson_val *json_value);
    duckdb::Value DeserializeJsonFloat(yyjson_val *json_value);
    duckdb::Value DeserializeJsonDouble(yyjson_val *json_value);
    duckdb::Value DeserializeJsonString(yyjson_val *json_value);
    duckdb::Value DeserializeJsonDecimal(yyjson_val *json_value, const duckdb::LogicalType &duck_type);
    duckdb::Value DeserializeJsonDate(yyjson_val *json_value);
    duckdb::Value DeserializeJsonTime(yyjson_val *json_value);
    duckdb::Value DeserializeJsonTimestamp(yyjson_val *json_value);
    duckdb::Value DeserializeJsonEnum(yyjson_val *json_value, const duckdb::LogicalType &duck_type);
    duckdb::Value DeserializeJsonArray(yyjson_val *json_value, const duckdb::LogicalType &duck_type);
    duckdb::Value DeserializeJsonObject(yyjson_val *json_value, const duckdb::LogicalType &duck_type);

    std::string GetStringProperty(yyjson_val *json_value, const std::string &property_name) const;

    // JSON path evaluation for complex expressions like AddressInfo[1].City."Name"
    yyjson_val* EvaluateJsonPath(yyjson_val* root, const std::string& path);
    std::vector<std::string> ParseJsonPath(const std::string& path);
};

// -------------------------------------------------------------------------------------------------

class ODataEntitySetJsonContent : public ODataEntitySetContent, public ODataJsonContentMixin {
public:
    ODataEntitySetJsonContent(const std::string& content);
    virtual ~ODataEntitySetJsonContent() = default;

    std::string MetadataContextUrl() override;
    std::optional<std::string> NextUrl() override;
    void PrettyPrint() override;

    std::vector<std::vector<duckdb::Value>> ToRows(std::vector<std::string> &column_names, 
                                                   std::vector<duckdb::LogicalType> &column_types) override;

    std::optional<uint64_t> TotalCount() override;
};

class ODataServiceJsonContent : public ODataServiceContent, public ODataJsonContentMixin {
public:
    ODataServiceJsonContent(const std::string& content);
    virtual ~ODataServiceJsonContent() = default;

    std::string MetadataContextUrl() override;
    std::vector<ODataEntitySetReference> EntitySets() override;
    void PrettyPrint() override;
};

} // namespace erpl_web
