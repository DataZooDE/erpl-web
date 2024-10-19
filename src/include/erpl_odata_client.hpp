#pragma once

#include "erpl_http_client.hpp"
#include "erpl_odata_edm.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace erpl_web {

class ODataContent {
public:
    virtual std::string MetadataContextUrl() = 0;
    virtual std::optional<std::string> NextUrl() = 0;

    virtual void PrettyPrint() = 0;

    virtual std::vector<std::vector<duckdb::Value>> ToRows(std::vector<std::string> &column_names, 
                                                           std::vector<duckdb::LogicalType> &column_types) = 0;

protected:
    void ThrowTypeError(yyjson_val *json_value, const std::string &expected);
};

class ODataJsonContent : public ODataContent {
public:
    virtual ~ODataJsonContent() = default;

    static bool IsJsonContentType(const std::string& content_type);

    ODataJsonContent(const std::string& content);

    std::string MetadataContextUrl() override;
    std::optional<std::string> NextUrl() override;

    void PrettyPrint() override;

    std::vector<std::vector<duckdb::Value>> ToRows(std::vector<std::string> &column_names, 
                                                   std::vector<duckdb::LogicalType> &column_types) override;
private:
    std::shared_ptr<yyjson_doc> doc;

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
    duckdb::Value DeserializeJsonEnum(yyjson_val *json_value, const duckdb::LogicalType &duck_type);
    duckdb::Value DeserializeJsonArray(yyjson_val *json_value, const duckdb::LogicalType &duck_type);
    duckdb::Value DeserializeJsonObject(yyjson_val *json_value, const duckdb::LogicalType &duck_type);
};

class ODataResponse {
public:
    ODataResponse(std::unique_ptr<HttpResponse> http_response);

    std::string ContentType();
    std::shared_ptr<ODataContent> Content();

    std::string MetadataContextUrl();
    std::optional<std::string> NextUrl();

    std::vector<std::vector<duckdb::Value>> ToRows(std::vector<std::string> &column_names, 
                                                   std::vector<duckdb::LogicalType> &column_types);
private:
    std::unique_ptr<HttpResponse> http_response;
    std::shared_ptr<ODataContent> parsed_content;
};

// ----------------------------------------------------------------------

class ODataClient {
public:
    ODataClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url, const Edmx& edmx);
    ODataClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url);

    Edmx GetMetadata();
    std::shared_ptr<ODataResponse> Get(bool get_next = false);

    std::string Url();
    std::vector<std::string> GetResultNames();
    std::vector<duckdb::LogicalType> GetResultTypes();

    std::shared_ptr<HttpClient> GetHttpClient();

private:
    std::shared_ptr<HttpClient> http_client;
    HttpUrl url;
    std::optional<Edmx> cached_edmx;
    std::shared_ptr<ODataResponse> current_response;

    EntitySet GetCurrentEntitySetType();
    EntityType GetCurrentEntityType();
};

} // namespace erpl_web