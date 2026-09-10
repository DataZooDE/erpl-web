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
#include <unordered_map>

#include "odata_client.hpp"
#include "odata_edm.hpp"
#include "odata_predicate_pushdown_helper.hpp"
#include "conversion_failure_log.hpp"

using namespace duckdb;

namespace erpl_web {

// Forward declarations
class ODataDataExtractor;
class ODataTypeResolver;
class ODataProgressTracker;
class ODataRowBuffer;

// Helper structs
struct SchemaInfo {
    std::vector<std::string> all_result_names;
    std::vector<duckdb::LogicalType> all_result_types;
    std::unordered_map<std::string, duckdb::idx_t> name_to_index;
    bool has_expand;
};

// ============================================================================
// Core Data Binding Class - Focused on DuckDB integration
// ============================================================================
class ODataReadBindData : public TableFunctionData
{
public: 
    static duckdb::unique_ptr<ODataReadBindData> FromEntitySetRoot(
        const std::string& entity_set_url, 
        std::shared_ptr<HttpAuthParams> auth_params);
    static duckdb::unique_ptr<ODataReadBindData> FromServiceRoot(
        const std::string& service_root_url,
        std::shared_ptr<HttpAuthParams> auth_params);
    
    // Factory pattern methods
    static duckdb::unique_ptr<ODataReadBindData> FromProbeResult(
        const ODataClientFactory::ProbeResult& result);
    static duckdb::unique_ptr<ODataReadBindData> FromEntitySetClient(
        std::shared_ptr<ODataEntitySetClient> client,
        const std::string& initial_content = "");
    static duckdb::unique_ptr<ODataReadBindData> FromServiceClient(
        std::shared_ptr<ODataServiceClient> client,
        const std::string& initial_content = "");

public:
    ODataReadBindData(std::shared_ptr<ODataEntitySetClient> odata_client);
    ODataReadBindData(std::shared_ptr<ODataEntitySetClient> odata_client, bool defer_initialization);
    ~ODataReadBindData();

    // Service-root mode (list resources from service document)
    void EnableServiceRootMode() { service_root_mode_ = true; }

    // Core DuckDB interface methods
    std::vector<std::string> GetResultNames(bool all_columns = false);
    std::vector<duckdb::LogicalType> GetResultTypes(bool all_columns = false);
    bool HasMoreResults();
    unsigned int FetchNextResult(DataChunk &output);

    // DuckDB lifecycle methods
    void ActivateColumns(const std::vector<duckdb::column_t> &column_ids);
    void AddFilters(const duckdb::optional_ptr<duckdb::TableFilterSet> &filters);
    // NOTE (GitHub #90): SQL `LIMIT`/`OFFSET` are NOT translated into `$top`/`$skip`.
    // DuckDB never hands a table function its bound result modifiers, so this
    // entry point has no caller in a real plan; the only remaining caller is
    // OdpODataReadBindData::AddResultModifiers, which is itself uncalled. The
    // machinery that used to walk the modifiers and synthesise $top/$skip has
    // been deleted because it made the code read as though LIMIT were pushed
    // down when it never was. `$top`/`$skip` come exclusively from the explicit
    // `top=`/`skip=` named parameters. Kept only so the ODP compilation unit
    // still links; delete together with the two ODP stubs.
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
    // Forward nested expand paths into the extractor for recursive processing
    void SetNestedExpandPaths(const std::vector<std::string>& nested_paths);
    bool HasExpandedData() const;
    void UpdateExpandedColumnType(const std::string& expand_path, const duckdb::LogicalType& new_type);

    // Extracted column names (for Datasphere compatibility)
    void SetExtractedColumnNames(const std::vector<std::string>& column_names);

    // Predicate pushdown helper access (made public for ODataReadBind)
    std::shared_ptr<ODataPredicatePushdownHelper> PredicatePushdownHelper();

    // Conversion failure reporting
    void SetStrictTyping(bool strict);
    std::shared_ptr<ConversionFailureLog> GetConversionFailureLog() const;
    // Emit the accumulated per-column failure summary once, at end of scan.
    void ReportConversionFailures();
    // Produce an independent scan instance for one execution of a bound plan.
    // See ODataReadGlobalState below and GitHub #75: all mutable scan state
    // (row buffer, paging cursor, progress, emitted-row counter, request
    // client) lives on the returned copy, so the bind data itself stays
    // immutable after bind and a plan can be executed more than once.
    duckdb::unique_ptr<ODataReadBindData> CloneForScan() const;

    // Reconcile the schema column order. EDMX/metadata order is authoritative
    // because that is the order the DuckDB catalog (ODataTableEntry) declares
    // its columns in; JSON key order may only contribute names metadata does
    // not know about, which are appended at the end. See GitHub #88.
    static std::vector<std::string> ReconcileSchemaOrder(
        const std::vector<std::string> &metadata_names,
        const std::vector<std::string> &json_names);

private:
    // Core components
    std::shared_ptr<ODataEntitySetClient> odata_client;
    std::shared_ptr<ODataPredicatePushdownHelper> predicate_pushdown_helper;
    std::shared_ptr<ODataDataExtractor> data_extractor;
    std::shared_ptr<ODataTypeResolver> type_resolver;
    std::shared_ptr<ODataProgressTracker> progress_tracker;
    std::shared_ptr<ODataRowBuffer> row_buffer;
    // Owned by the bind data so it survives the page responses and the row
    // buffer, both of which are replaced during pagination.
    std::shared_ptr<ConversionFailureLog> conversion_failure_log;

    // Schema, settled during bind and read-only afterwards.
    // all_result_names / all_result_types are the EDMX (metadata) schema, in
    // metadata order. extracted_column_names holds the JSON key order observed
    // in the first data page (V2 / Datasphere only); it is used for type
    // inference and as a fallback when metadata is unavailable, never to
    // define column order (GitHub #88).
    std::vector<std::string> all_result_names;
    std::vector<duckdb::LogicalType> all_result_types;
    std::vector<std::string> extracted_column_names;
    std::vector<std::string> base_result_names;
    std::vector<duckdb::LogicalType> base_result_types;
    bool base_schema_resolved_ = false;

    // Projection, settled in ActivateColumns during global-state init.
    std::vector<duckdb::column_t> active_column_ids;
    std::vector<duckdb::column_t> activated_to_original_mapping;

    // One entry per OUTPUT column, in output order, marking the slots DuckDB asked to be
    // filled with row ids rather than data. Row ids are dropped from the data projection
    // but they still occupy a position in the chunk, so the positional correspondence has
    // to be recorded or every later slot reads the wrong column. See GitHub #132.
    std::vector<bool> output_column_is_row_id;
    
    // Configuration
    std::map<std::string, std::string> input_parameters;
    std::string expand_clause;
    bool has_expanded_data = false;
    
    // Mutable scan state. On the instance held as bind data these only ever
    // carry what bind itself produced (a pre-fetched first page, if any); the
    // scan mutates the per-execution clone returned by CloneForScan, never the
    // bind data (GitHub #75). Together with row_buffer, progress_tracker,
    // data_extractor's row cache and odata_client's paging cursor, this is the
    // complete set of fields the clone must own.
    bool first_page_cached_ = false;
    // Tracks how many rows have been emitted so far to align expanded cache row-wise
    size_t emitted_row_index_ = 0;
    bool service_root_mode_ = false;
    bool conversion_failures_reported_ = false;

    // Helper methods
    void InitializeComponents(bool service_root_mode = false);

    // (Re-)install the column-name resolver on the predicate pushdown helper,
    // capturing this instance. Must be called on every object that owns a
    // helper, including scan clones.
    void BindPredicateColumnResolver();

    // Metadata (EDMX) schema, fetched once and cached.
    const std::vector<std::string> &MetadataColumnNames();
    const std::vector<duckdb::LogicalType> &MetadataColumnTypes();

    // Base (non-expanded) schema in authoritative metadata order, with the
    // per-name types resolved from metadata. Computed once, then cached.
    void EnsureBaseSchemaResolved();

    // Buffer the rows of an already-fetched first page into row_buffer and
    // mark first_page_cached_. Shared by PrefetchFirstPage (response from
    // odata_client->Get()) and FromEntitySetClient (response synthesised from
    // pre-fetched initial_content). Without this, callers that hand a page's
    // content into FromEntitySetClient would later trigger a redundant bare
    // GET inside PrefetchFirstPage — which against SAP ODP returns the entire
    // dataset and inflates the result by N×.
    void BufferFirstPageFromResponse(std::shared_ptr<ODataEntitySetResponse> response);

    // FetchNextResult helper methods
    void EnsureInitialized();
    SchemaInfo PrepareSchemaInfo();
    void FetchAdditionalPagesIfNeeded(const SchemaInfo& schema_info);
    void ProcessPageResponse(std::shared_ptr<ODataEntitySetResponse> response, const SchemaInfo& schema_info);
    idx_t EmitRowsToOutput(duckdb::DataChunk &output, const SchemaInfo& schema_info);
    void EmitSingleRowToOutput(duckdb::DataChunk &output, const std::vector<duckdb::Value> &row, idx_t row_index, const SchemaInfo& schema_info);
    duckdb::idx_t GetOriginalColumnIndex(idx_t activated_column_index) const;
    duckdb::Value GetColumnValue(duckdb::idx_t original_column_index, const std::vector<duckdb::Value> &row, const SchemaInfo& schema_info);
    bool IsExpandedColumn(duckdb::idx_t original_column_index, const SchemaInfo& schema_info) const;
    duckdb::Value GetExpandedColumnValue(duckdb::idx_t original_column_index, const SchemaInfo& schema_info);
    duckdb::Value GetRegularColumnValue(duckdb::idx_t original_column_index, const std::vector<duckdb::Value> &row, const SchemaInfo& schema_info);
    void UpdateProgressTracking(idx_t rows_emitted);

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
    static std::vector<std::string> GetNavigationPropertyNames(std::shared_ptr<ODataEntitySetClient> odata_client);
    static std::string ExtractEntitySetNameFromUrl(const std::string& entity_set_url);
public:
    static bool LooksLikeServiceRootUrl(const std::string &url);
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
    // Provide full nested expand paths for type inference (e.g., "DefaultSystem/Services")
    void SetNestedExpandPaths(const std::vector<std::string>& nested_paths);
    std::vector<std::string> GetExpandedDataSchema() const;
    std::vector<duckdb::LogicalType> GetExpandedDataTypes() const;
    std::vector<std::string> GetNestedExpandPaths() const;
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

    // Scan-wide conversion failure reporting (shared with the bind data)
    void SetConversionFailureLog(std::shared_ptr<ConversionFailureLog> log);

private:
    // Modular helpers for JSON -> DuckDB conversions
    duckdb::Value ConvertList(duckdb_yyjson::yyjson_val* value, const duckdb::LogicalType& target_type);
    duckdb::Value ConvertStruct(duckdb_yyjson::yyjson_val* value, const duckdb::LogicalType& target_type);
    duckdb::Value ConvertVarchar(duckdb_yyjson::yyjson_val* value);
    duckdb::Value ConvertInteger(duckdb_yyjson::yyjson_val* value, const duckdb::LogicalType& target_type);
    duckdb::Value ConvertBigint(duckdb_yyjson::yyjson_val* value);
    duckdb::Value ConvertFloatLike(duckdb_yyjson::yyjson_val* value);
    duckdb::Value ConvertDecimal(duckdb_yyjson::yyjson_val* value, const duckdb::LogicalType& target_type);
    duckdb::Value ConvertBoolean(duckdb_yyjson::yyjson_val* value);
    duckdb::Value ConvertTimestamp(duckdb_yyjson::yyjson_val* value);
    duckdb::Value ConvertFallbackAsString(duckdb_yyjson::yyjson_val* value, const duckdb::LogicalType& target_type);

    std::shared_ptr<ODataEntitySetClient> odata_client;
    std::shared_ptr<ODataTypeResolver> type_resolver;
    
    std::vector<std::string> expanded_data_schema;
    std::vector<duckdb::LogicalType> expanded_data_types;
    std::map<std::string, std::vector<duckdb::Value>> expanded_data_cache;
    // Top-level expanded column names (iterate columns)
    std::vector<std::string> expand_paths;
    // Full nested expand paths for recursive inference
    std::vector<std::string> nested_expand_paths;
    
    // Performance configuration
    size_t batch_size_ = 1000;
    bool compression_enabled_ = false;
    
    // Error handling
    mutable std::string last_error_;
    mutable std::map<std::string, size_t> error_counts_;
    mutable std::mutex error_mutex_;

    // Scan-wide failure log (may be null) and the expand path currently being
    // parsed, used to attribute a failure to a column.
    std::shared_ptr<ConversionFailureLog> conversion_failure_log_;
    std::string current_column_context_;
    
    // Processing methods
    void ProcessODataV4ExpandedData(duckdb_yyjson::yyjson_val* value_arr);
    void ProcessODataV2ExpandedData(duckdb_yyjson::yyjson_val* results_arr);
    void ProcessExpandedDataRows(duckdb_yyjson::yyjson_val* rows_arr, const std::string& response_format);
    duckdb::LogicalType GetExpandedTargetType(const std::string& expand_path) const;
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
    // Returns a typed NULL - never a fabricated 0/""/false - and records the
    // failure against the current column so it is reported at end of scan.
    duckdb::Value CreateFallbackValue(const duckdb::LogicalType& target_type,
                                      const std::string& reason = "",
                                      const std::string& offending_value = "") const;
    void RecordConversionFailure(const std::string& column_name,
                                 const std::string& offending_value,
                                 const std::string& error_message) const;
    
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
    // Resolve navigation target entity name from the current entity
    std::pair<bool, std::string> GetNavTargetFromCurrentEntity(const std::string &nav_prop) const;
    // Resolve navigation target entity name from a given entity type name
    std::pair<bool, std::string> GetNavTargetOnEntity(const std::string &entity_type_name, const std::string &nav_prop) const;
    // Resolve navigation property type on a given entity type (returns LIST(child) when collection)
    duckdb::LogicalType ResolveNavigationOnEntity(const std::string &entity_type_name, const std::string &nav_prop) const;
    
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
    void AddRows(std::vector<std::vector<duckdb::Value>> rows);
    std::vector<duckdb::Value> GetNextRow();
    bool HasMoreRows() const;
    size_t Size() const;
    // Snapshot of the still-buffered rows, used to seed a per-scan clone with
    // rows that were buffered during bind (see ODataReadBindData::CloneForScan).
    std::vector<std::vector<duckdb::Value>> CopyRows() const;
    void Clear();
    
    // Page management
    void SetHasNextPage(bool has_next);
    bool HasNextPage() const;

private:
    std::deque<std::vector<duckdb::Value>> row_buffer_;
    bool has_next_page_ = false;
};

// ============================================================================
// Scan State - one instance per execution of a bound plan
// ============================================================================

/**
 * @brief Owns all mutable scan state for a single execution of an OData scan.
 *
 * Historically the row buffer, paging cursor, progress tracker and
 * emitted-row counter lived directly on ODataReadBindData behind a bare
 * GlobalTableFunctionState. Bind data outlives a single execution, so a
 * re-executed bound plan (PREPARE then EXECUTE twice) found the buffer already
 * drained and returned zero rows, and a self-join of one odata_read() call had
 * two scans sharing one cursor (GitHub #75).
 *
 * ODataReadTableInitGlobalState now clones the bind data into this object and
 * runs projection/filter pushdown and the first-page prefetch against the
 * clone, leaving the bind data untouched by the scan.
 */
class ODataReadGlobalState : public duckdb::GlobalTableFunctionState {
public:
    explicit ODataReadGlobalState(duckdb::unique_ptr<ODataReadBindData> scan_state);

    // The OData scan is inherently sequential: pagination is server-driven via
    // @odata.nextLink / __next, so page N+1 is unknowable until page N has been
    // read. There is no partitioning scheme to hand to a second thread.
    duckdb::idx_t MaxThreads() const override { return 1; }

    ODataReadBindData &Scan() { return *scan_state; }
    const ODataReadBindData &Scan() const { return *scan_state; }

private:
    duckdb::unique_ptr<ODataReadBindData> scan_state;
};

// ============================================================================
// Helper Functions for ODataReadBind Modularization
// ============================================================================
namespace ODataReadBindHelpers {
    void ProcessNamedParameters(ODataReadBindData* bind_data, const TableFunctionBindInput& input);
    void ProcessExpandClause(ODataReadBindData* bind_data, const std::string& expand_clause);
    std::string ExtractExpandClauseFromUrl(const std::string& url);
    void SetupSchemaFromProbeResult(const ODataClientFactory::ProbeResult& probe_result, 
                                   ODataReadBindData* bind_data,
                                   vector<LogicalType>& return_types, 
                                   vector<string>& names);
}

// ============================================================================
// Function Declarations
// ============================================================================
void ODataReadScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);
unique_ptr<GlobalTableFunctionState> ODataReadTableInitGlobalState(ClientContext &context, TableFunctionInitInput &input);
unique_ptr<FunctionData> ODataReadBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names);
double ODataReadTableProgress(ClientContext &, const FunctionData *func_data, const GlobalTableFunctionState *);
TableFunctionSet CreateODataReadFunction();

// OData Describe function
TableFunctionSet CreateODataDescribeFunction();

// ============================================================================
// Shared Error Handling Utilities
// ============================================================================

namespace ODataErrorHandling {

/**
 * @brief Convert runtime errors (especially HTTP errors) to user-friendly InvalidInputException
 * 
 * @param e The runtime error to convert
 * @param url The URL that caused the error
 * @param service_type Type of service ("OData" or "ODP OData") for error messages
 * @param discovery_function Name of discovery function to suggest (e.g., "sap_odata_show()" or "sap_odp_odata_show()")
 * @return InvalidInputException with user-friendly error message
 */
duckdb::InvalidInputException ConvertHttpErrorToUserFriendly(const std::runtime_error& e, 
                                                           const std::string& url,
                                                           const std::string& service_type,
                                                           const std::string& discovery_function);

} // namespace ODataErrorHandling

} // namespace erpl_web
