#pragma once

#include "delta_share_types.hpp"
#include "http_client.hpp"
#include "timeout_http_client.hpp"
#include <memory>
#include <vector>

using namespace duckdb;

namespace erpl_web {

// Delta Sharing REST API client
class DeltaShareClient {
public:
    // Constructor
    explicit DeltaShareClient(ClientContext& context, const DeltaShareProfile& profile);

    // Destructor
    ~DeltaShareClient() = default;

    // Discovery APIs
    vector<DeltaShareInfo> ListShares();
    vector<DeltaSchemaInfo> ListSchemas(const string& share);
    vector<DeltaTableInfo> ListTables(const string& share, const string& schema);

    // Table metadata and data access
    DeltaTableMetadata GetTableMetadata(const string& share, const string& schema, const string& table);
    std::vector<DeltaFileReference> QueryTable(const std::string& share, const std::string& schema, const std::string& table,
                                          const std::optional<DeltaShareQueryRequest>& query_request = std::nullopt);

    // Get table version
    int64_t GetTableVersion(const string& share, const string& schema, const string& table);

    // Change Data Feed (for future implementation)
    std::vector<DeltaFileReference> GetTableChanges(const std::string& share, const std::string& schema, const std::string& table,
                                               int64_t starting_version, std::optional<int64_t> ending_version = std::nullopt);

private:
    DeltaShareProfile profile_;
    shared_ptr<TimeoutHttpClient> http_client_;

    // Helper methods for URL building and request execution
    string BuildUrl(const string& path) const;
    HeaderMap BuildHeaders() const;
    string BuildAuthorizationHeader() const;

    // HTTP request execution
    DeltaShareResponse ExecuteGet(const string& endpoint, const HeaderMap& headers = {});
    DeltaShareResponse ExecutePost(const string& endpoint, const string& body = "", const HeaderMap& headers = {});

    // Response parsing helpers
    DeltaTableMetadata ParseMetadataResponse(const string& ndjson_content);
    vector<DeltaFileReference> ParseQueryResponse(const string& ndjson_content);
    vector<DeltaShareInfo> ParseSharesResponse(const string& json_content);
    vector<DeltaSchemaInfo> ParseSchemasResponse(const string& json_content, const string& share_name);
    vector<DeltaTableInfo> ParseTablesResponse(const string& json_content, const string& share_name, const string& schema_name);

    // Schema conversion helper
    vector<LogicalType> ConvertDeltaSchema(const string& schema_json);

    // Validation and error handling
    void ValidateProfile() const;
    void HandleApiError(int32_t status_code, const string& error_body);
};

} // namespace erpl_web
