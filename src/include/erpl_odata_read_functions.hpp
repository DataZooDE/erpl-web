#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include <map>
#include <memory>
#include <deque>
#include <mutex>
#include <algorithm>

#include "erpl_odata_client.hpp"
#include "erpl_odata_edm.hpp"
#include "erpl_odata_predicate_pushdown_helper.hpp"

using namespace duckdb;

namespace erpl_web {

// Forward declarations
class ODataDataExtractor;
class ODataTypeResolver;
class ODataProgressTracker;
class ODataRowBuffer;

// ============================================================================
// Core Data Binding Class - Focused on DuckDB integration
// ============================================================================
class ODataReadBindData : public TableFunctionData
{
public: 
    static duckdb::unique_ptr<ODataReadBindData> FromEntitySetRoot(
        const std::string& entity_set_url, 
        std::shared_ptr<HttpAuthParams> auth_params);

public:
    ODataReadBindData(std::shared_ptr<ODataEntitySetClient> odata_client);
    ~ODataReadBindData() = default;

    // Core DuckDB interface methods
    std::vector<std::string> GetResultNames(bool all_columns = false);
    std::vector<duckdb::LogicalType> GetResultTypes(bool all_columns = false);
    bool HasMoreResults();
    unsigned int FetchNextResult(DataChunk &output);

    // DuckDB lifecycle methods
    void ActivateColumns(const std::vector<duckdb::column_t> &column_ids);
    void AddFilters(const duckdb::optional_ptr<duckdb::TableFilterSet> &filters);
    void AddResultModifiers(const std::vector<duckdb::unique_ptr<duckdb::BoundResultModifier>> &modifiers);
    void UpdateUrlFromPredicatePushdown();
    void PrefetchFirstPage();

    // Progress reporting
    double GetProgressFraction() const;

    // Column name resolution
    std::string GetOriginalColumnName(duckdb::column_t activated_column_index) const;

    // Input parameters
    void SetInputParameters(const std::map<std::string, std::string>& input_params);
    const std::map<std::string, std::string>& GetInputParameters() const;

    // OData client access
    std::shared_ptr<ODataEntitySetClient> GetODataClient() const;

    // Expand functionality
    void SetExpandClause(const std::string& expand_clause);
    std::string GetExpandClause() const;
    void SetExpandedDataSchema(const std::vector<std::string>& expand_paths);
    bool HasExpandedData() const;
    void UpdateExpandedColumnType(const std::string& expand_path, const duckdb::LogicalType& new_type);

    // Extracted column names (for Datasphere compatibility)
    void SetExtractedColumnNames(const std::vector<std::string>& column_names);

    // Predicate pushdown helper access (made public for ODataReadBind)
    std::shared_ptr<ODataPredicatePushdownHelper> PredicatePushdownHelper();

private:
    // Core components
    std::shared_ptr<ODataEntitySetClient> odata_client;
    std::shared_ptr<ODataPredicatePushdownHelper> predicate_pushdown_helper;
    std::shared_ptr<ODataDataExtractor> data_extractor;
    std::shared_ptr<ODataTypeResolver> type_resolver;
    std::shared_ptr<ODataProgressTracker> progress_tracker;
    std::shared_ptr<ODataRowBuffer> row_buffer;

    // State management
    std::vector<std::string> all_result_names;
    std::vector<duckdb::column_t> active_column_ids;
    std::vector<duckdb::LogicalType> all_result_types;
    std::vector<std::string> extracted_column_names;
    std::vector<duckdb::column_t> activated_to_original_mapping;
    
    // Configuration
    std::map<std::string, std::string> input_parameters;
    std::string expand_clause;
    bool has_expanded_data = false;
    
    // State tracking
    bool first_page_cached_ = false;
    // Tracks how many rows have been emitted so far to align expanded cache row-wise
    size_t emitted_row_index_ = 0;

    // Helper methods
    void InitializeComponents();

private:
    // FromEntitySetRoot refactoring helper methods
    static bool IsDatasphereUrl(const std::string& entity_set_url);
    static bool IsODataV2Url(const std::string& entity_set_url);
    static bool ShouldUseDirectHttp(const std::string& entity_set_url);
    static void ParseODataV4Response(duckdb_yyjson::yyjson_val* root, 
                                   std::shared_ptr<ODataEntitySetClient> odata_client,
                                   std::vector<std::string>& extracted_column_names);
    static void ParseODataV2Response(duckdb_yyjson::yyjson_val* root,
                                   std::shared_ptr<ODataEntitySetClient> odata_client,
                                   std::vector<std::string>& extracted_column_names);
    static void HandleServiceDocument(duckdb_yyjson::yyjson_val* value_arr,
                                    const std::string& entity_set_url,
                                    std::shared_ptr<ODataEntitySetClient> odata_client);
    static std::vector<std::string> GetNavigationPropertyNames(std::shared_ptr<ODataEntitySetClient> odata_client);
    static std::string ExtractEntitySetNameFromUrl(const std::string& entity_set_url);
    static bool IsServiceDocumentResponse(const std::vector<std::string>& column_names);
};

// ============================================================================
// Data Extraction - Handles OData response parsing and expand functionality
// ============================================================================
class ODataDataExtractor
{
public:
    explicit ODataDataExtractor(std::shared_ptr<ODataEntitySetClient> odata_client);
    
    // Core extraction methods
    void ExtractExpandedDataFromResponse(const std::string& response_content);
    duckdb::Value ExtractExpandedDataForRow(const std::string& row_id, const std::string& expand_path);
    
    // Schema management
    void SetExpandedDataSchema(const std::vector<std::string>& expand_paths);
    std::vector<std::string> GetExpandedDataSchema() const;
    std::vector<duckdb::LogicalType> GetExpandedDataTypes() const;
    bool HasExpandedData() const;
    void UpdateExpandedColumnType(size_t index, const duckdb::LogicalType& new_type);
    
    // Performance and memory management
    void SetBatchSize(size_t batch_size);
    void EnableCompression(bool enable);
    void ClearCache();
    size_t GetCacheSize() const;
    
    // Error handling and validation
    bool ValidateExpandedData(const std::string& expand_path) const;
    std::string GetLastError() const;
    void ResetErrorState();

private:
    std::shared_ptr<ODataEntitySetClient> odata_client;
    std::shared_ptr<ODataTypeResolver> type_resolver;
    
    std::vector<std::string> expanded_data_schema;
    std::vector<duckdb::LogicalType> expanded_data_types;
    std::map<std::string, std::vector<duckdb::Value>> expanded_data_cache;
    std::vector<std::string> expand_paths;
    
    // Performance configuration
    size_t batch_size_ = 1000;
    bool compression_enabled_ = false;
    
    // Error handling
    mutable std::string last_error_;
    mutable std::map<std::string, size_t> error_counts_;
    mutable std::mutex error_mutex_;
    
    // Processing methods
    void ProcessODataV4ExpandedData(duckdb_yyjson::yyjson_val* value_arr);
    void ProcessODataV2ExpandedData(duckdb_yyjson::yyjson_val* results_arr);
    duckdb::Value ParseJsonToDuckDBValue(const std::string& json_str, const duckdb::LogicalType& target_type);
    duckdb::Value ParseJsonValueToDuckDBValue(duckdb_yyjson::yyjson_val* value, const duckdb::LogicalType& target_type);
    
    // Enhanced parsing methods
    duckdb::Value ParseJsonArray(duckdb_yyjson::yyjson_val* array_val, const duckdb::LogicalType& target_type);
    duckdb::Value ParseJsonObject(duckdb_yyjson::yyjson_val* obj_val, const duckdb::LogicalType& target_type);
    
    // Recursive expand handling methods
    duckdb::Value ParseExpandedDataRecursively(duckdb_yyjson::yyjson_val* expand_data, 
                                              const std::string& expand_path, 
                                              const duckdb::LogicalType& target_type);
    duckdb::LogicalType InferStructTypeFromJsonObjectWithNestedExpands(duckdb_yyjson::yyjson_val* obj_val, 
                                                                      const std::string& expand_path);
    duckdb::LogicalType InferStructTypeFromJsonObject(duckdb_yyjson::yyjson_val* obj_val);
    duckdb::LogicalType InferTypeFromJsonValue(duckdb_yyjson::yyjson_val* value);

    
    // Error handling helpers
    void LogError(const std::string& context, const std::string& error_msg) const;
    bool ShouldRetryAfterError(const std::string& context) const;
    duckdb::Value CreateFallbackValue(const duckdb::LogicalType& target_type) const;
    
    // Memory optimization
    void OptimizeCacheMemory();
    void CompressCacheData();
};

// ============================================================================
// Type Resolution - Handles EDM to DuckDB type mapping
// ============================================================================
class ODataTypeResolver
{
public:
    explicit ODataTypeResolver(std::shared_ptr<ODataEntitySetClient> odata_client);
    
    // Type conversion methods
    duckdb::LogicalType ResolveNavigationPropertyType(const std::string& property_name) const;
    duckdb::LogicalType ConvertPrimitiveTypeString(const std::string& type_name) const;
    
    // Collection type handling
    std::tuple<bool, std::string> ExtractCollectionType(const std::string& type_name) const;

private:
    std::shared_ptr<ODataEntitySetClient> odata_client;
    
    // Type caching for performance
    mutable std::map<std::string, duckdb::LogicalType> type_cache_;
    
    // Helper methods
    duckdb::LogicalType ResolveEntityType(const std::string& type_name) const;
    duckdb::LogicalType ResolveComplexType(const std::string& type_name) const;
    
    // Error handling
    duckdb::LogicalType HandleTypeResolutionError(const std::string& type_name, const std::string& error_msg) const;
};

// ============================================================================
// Progress Tracking - Handles progress reporting and row counting
// ============================================================================
class ODataProgressTracker
{
public:
    ODataProgressTracker() = default;
    
    // Progress management
    void SetTotalCount(uint64_t total);
    void IncrementRowsFetched(uint64_t count);
    double GetProgressFraction() const;
    bool HasTotalCount() const;
    uint64_t GetTotalCount() const;
    uint64_t GetRowsFetched() const;
    
    // Reset functionality
    void Reset();

private:
    uint64_t rows_fetched_ = 0;
    uint64_t total_count_ = 0;
    bool has_total_ = false;
};

// ============================================================================
// Row Buffer - Handles row caching and vector management
// ============================================================================
class ODataRowBuffer
{
public:
    ODataRowBuffer() = default;
    
    // Buffer management
    void AddRows(const std::vector<std::vector<duckdb::Value>>& rows);
    std::vector<duckdb::Value> GetNextRow();
    bool HasMoreRows() const;
    size_t Size() const;
    void Clear();
    
    // Page management
    void SetHasNextPage(bool has_next);
    bool HasNextPage() const;

private:
    std::deque<std::vector<duckdb::Value>> row_buffer_;
    bool has_next_page_ = false;
};

// ============================================================================
// Function Declarations
// ============================================================================
void ODataReadScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);
unique_ptr<GlobalTableFunctionState> ODataReadTableInitGlobalState(ClientContext &context, TableFunctionInitInput &input);
unique_ptr<FunctionData> ODataReadBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names);
double ODataReadTableProgress(ClientContext &, const FunctionData *func_data, const GlobalTableFunctionState *);
TableFunctionSet CreateODataReadFunction();

} // namespace erpl_web