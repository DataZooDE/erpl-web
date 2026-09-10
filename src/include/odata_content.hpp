#pragma once

#include "odata_edm.hpp"
#include "conversion_failure_log.hpp"
#include "datazoo/oauth2/http_client.hpp"
#include "yyjson.hpp"

#include <memory>
#include <optional>
#include <string>

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

    // Attach the scan-wide log that collects per-cell conversion failures.
    // Content objects are replaced on every page, the log is not - it is owned
    // by the bind data so the summary spans the whole scan.
    virtual void SetConversionFailureLog(std::shared_ptr<ConversionFailureLog> log) { (void)log; }
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

//! The error a service reported in its own response body. Both OData v2 and v4 wrap it in a
//! top-level "error" object; only the shape of "message" differs (a string in v4, a
//! {"lang","value"} object in v2), which is why both are decoded into the same struct.
struct ODataErrorInfo {
    std::string code;
    std::string message;

    //! Renders the error the way it is surfaced to the user, e.g.
    //! "OData service reported an error (code 'SY/530'): Property 'Foo' does not exist".
    std::string ToString() const;
};

// -------------------------------------------------------------------------------------------------
class ODataJsonContentMixin {
public:
    static bool IsJsonContentType(const std::string& content_type);

    ODataJsonContentMixin(const std::string& content);

    // OData version support
    void SetODataVersion(ODataVersion version) { odata_version = version; }
    ODataVersion GetODataVersion() const { return odata_version; }
    
    //! Detects the version from the payload alone, falling back to V4 when nothing is conclusive.
    //! Kept for callers that have no access to the response headers.
    static ODataVersion DetectODataVersion(const std::string& content);

    //! Header-based detection: the service tells us the version in "OData-Version" (v4) or
    //! "DataServiceVersion" (v2/v3). Version suffixes such as SAP's "4.0;NetFx" are tolerated.
    //! Returns UNKNOWN when no such header is present or the value is not understood.
    static ODataVersion DetectODataVersionFromHeaders(const HeaderMap& headers);

    //! Payload sniffing, honest about failure: returns UNKNOWN when the body carries no
    //! discriminator (empty body, non-JSON body, or an error payload), instead of guessing V4.
    static ODataVersion DetectODataVersionFromPayload(const std::string& content);

    //! The detection order the readers should use: what the service declared in its headers
    //! first, payload sniffing second, V4 as the last resort.
    static ODataVersion DetectODataVersion(const std::string& content, const HeaderMap& headers);

    //! Decodes a top-level OData error payload (v2 or v4 shape). Returns nullopt when the body
    //! is not an error document.
    static std::optional<ODataErrorInfo> TryGetODataError(const std::string& content);

protected:
    std::shared_ptr<yyjson_doc> doc;

    //! Same as the public overload, on an already-parsed document root.
    static std::optional<ODataErrorInfo> TryGetODataError(yyjson_val *root);

    //! Throws with the service's own code and message when `root` is an error document; a no-op
    //! otherwise. Used wherever we would otherwise report a generic parse failure and discard
    //! what the server actually said.
    static void ThrowIfODataError(yyjson_val *root);

    // Scan-wide failure log (may be null when the content is used standalone)
    // and the column currently being deserialized, used to attribute failures.
    std::shared_ptr<ConversionFailureLog> conversion_failure_log;
    std::string current_column_context;

    // Record one failed value conversion against the current column. Throws
    // StrictTypingViolation when strict typing is enabled.
    void RecordConversionFailure(const std::string &offending_value, const std::string &error_message);
    // Compact JSON rendering of a value, for inclusion in the failure report.
    std::string JsonValueToDisplayString(yyjson_val *json_value) const;
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
    duckdb::Value DeserializeJsonBlob(yyjson_val *json_value);
    duckdb::Value DeserializeJsonInterval(yyjson_val *json_value);

    //! Parses the OData V2 legacy date literal "/Date(<epoch-millis>[+/-HHMM])/" with millisecond
    //! precision. The trailing offset is deliberately ignored because the epoch value is already
    //! expressed in UTC; applying the offset on top of it would shift the timestamp incorrectly.
    static bool TryParseODataV2DateLiteral(const std::string &literal, duckdb::timestamp_t &result);

    //! Parses an ISO-8601 duration as produced for Edm.Duration, e.g. "PT12H30M", "P3DT4H", "-PT1H".
    static bool TryParseIso8601Duration(const std::string &literal, duckdb::interval_t &result);

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

    void SetConversionFailureLog(std::shared_ptr<ConversionFailureLog> log) override {
        conversion_failure_log = std::move(log);
    }
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
