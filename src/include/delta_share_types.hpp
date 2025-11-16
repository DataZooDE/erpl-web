#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>
#include <map>
#include <optional>
#include <chrono>
#include <memory>

using namespace duckdb;
using std::string;

namespace erpl_web {

// Forward declarations
class DeltaShareClient;

// Delta Sharing Profile - loaded from profile.json
struct DeltaShareProfile {
    int32_t share_credentials_version;
    string endpoint;
    string bearer_token;
    std::optional<std::string> expiration_time;

    // Parse profile from file path (supports local, HTTP, S3 URIs)
    // Uses DuckDB's FileSystem API for transparent remote file access
    static DeltaShareProfile FromFile(ClientContext& context, const string& profile_path);

    // Parse profile from local file path only (backward compatibility)
    static DeltaShareProfile FromLocalFile(const string& profile_path);

    // Parse profile from JSON string
    static DeltaShareProfile FromJson(const string& json_content);

    // Check if bearer token is expired
    bool IsExpired() const;

    // Get debug string representation
    string ToDebugString() const;
};

// Delta Sharing discovery types
struct DeltaShareInfo {
    string name;
    string id;
};

struct DeltaSchemaInfo {
    string name;
    string share;
};

struct DeltaTableInfo {
    string name;
    string schema;
    string share;
    string id;
    std::optional<std::string> description;
};

// File reference from Delta Sharing server (pre-signed URL)
struct DeltaFileReference {
    string url;                                // Pre-signed Parquet URL with authentication
    uint64_t size;                             // File size in bytes
    string id;                                 // File ID
    map<string, string> partition_values;      // Partition values if table is partitioned
    std::optional<std::string> stats;               // JSON statistics (minValues, maxValues, etc.)
};

// Table metadata from Delta Sharing server
struct DeltaTableMetadata {
    string schema_json;                        // Delta Lake schema as JSON string
    vector<string> partition_columns;          // List of partition column names
    vector<LogicalType> duckdb_types;         // Converted DuckDB types
    vector<string> column_names;               // Column names in order
};

// Query request for Delta Sharing /query endpoint
struct DeltaShareQueryRequest {
    vector<string> predicate_hints;            // SQL predicates for filtering (e.g., "col1 > 100")
    map<string, string> json_predicate_hints;  // JSON predicate format for complex filters
    std::optional<int64_t> limit_hint;         // Row limit hint
    std::optional<int64_t> version;            // Specific table version to query

    string ToJson() const;
};

// Response from Delta Sharing API endpoints
struct DeltaShareResponse {
    int32_t http_status;
    string content;
    map<string, string> headers;
};

// Utility function to convert Delta types to DuckDB types
// Maps Delta Lake EDM types (string, integer, long, etc.) to DuckDB LogicalTypes
LogicalType ConvertDeltaTypeToLogicalType(const string& delta_type);

} // namespace erpl_web
