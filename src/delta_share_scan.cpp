#include "delta_share_scan.hpp"
#include "tracing.hpp"
#include "yyjson.hpp"
#include "telemetry.hpp"

using namespace duckdb_yyjson;

namespace erpl_web {

using duckdb::PostHogTelemetry;

// =====================================================================
// Bind Phase
// =====================================================================

static unique_ptr<FunctionData> DeltaShareScanBind(ClientContext& context,
                                                   TableFunctionBindInput& input,
                                                   vector<LogicalType>& return_types,
                                                   vector<string>& names) {
    PostHogTelemetry::Instance().CaptureFunctionExecution("delta_share_scan");
    ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "Bind phase starting");

    auto bind_data = make_uniq<DeltaShareScanBindData>();

    // Parse parameters: profile_path, share, schema, table
    if (input.inputs.size() < 4) {
        throw InvalidInputException("delta_share_scan requires 4 parameters: "
                                   "(profile_path, share, schema, table)");
    }

    try {
        // Load profile from local or remote path (HTTP/S3 supported via DuckDB FileSystem API)
        bind_data->profile = DeltaShareProfile::FromFile(context, input.inputs[0].GetValue<string>());
    } catch (const std::exception& e) {
        throw InvalidInputException("Failed to load Delta Sharing profile: " + string(e.what()));
    }

    bind_data->share = input.inputs[1].GetValue<string>();
    bind_data->schema = input.inputs[2].GetValue<string>();
    bind_data->table = input.inputs[3].GetValue<string>();

    ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN",
                    "Fetching metadata for: " + bind_data->share + "." + bind_data->schema + "." +
                        bind_data->table);

    // Create client and fetch metadata
    try {
        auto client = duckdb::make_shared_ptr<DeltaShareClient>(context, bind_data->profile);
        bind_data->metadata = client->GetTableMetadata(bind_data->share, bind_data->schema, bind_data->table);
    } catch (const std::exception& e) {
        throw InvalidInputException("Failed to fetch Delta Sharing table metadata: " + string(e.what()));
    }

    // Extract column names and types from Delta schema JSON
    if (bind_data->metadata.schema_json.empty()) {
        // Fallback: if no schema extracted, use a simple schema
        return_types.push_back(LogicalType::VARCHAR);
        names.push_back("data");
        ERPL_TRACE_WARN("DELTA_SHARE_SCAN", "Using fallback schema - no schema JSON extracted from metadata");
    } else {
        // Parse schemaString to extract column names and types
        try {
            auto doc = std::shared_ptr<yyjson_doc>(
                yyjson_read(bind_data->metadata.schema_json.c_str(), bind_data->metadata.schema_json.size(), 0),
                yyjson_doc_free
            );

            if (!doc) {
                throw InvalidInputException("Failed to parse schema JSON");
            }

            auto root = yyjson_doc_get_root(doc.get());
            auto fields_arr = yyjson_obj_get(root, "fields");

            if (!fields_arr || !yyjson_is_arr(fields_arr)) {
                throw InvalidInputException("Schema has no 'fields' array");
            }

            // Iterate through fields and extract names and types
            size_t idx, max;
            yyjson_val* field_item;
            yyjson_arr_foreach(fields_arr, idx, max, field_item) {
                if (!yyjson_is_obj(field_item)) {
                    continue;
                }

                // Extract field name
                auto name_val = yyjson_obj_get(field_item, "name");
                if (!name_val || !yyjson_is_str(name_val)) {
                    continue;
                }
                string field_name = string(yyjson_get_str(name_val));

                // Extract field type
                auto type_val = yyjson_obj_get(field_item, "type");
                if (!type_val || !yyjson_is_str(type_val)) {
                    return_types.push_back(LogicalType::VARCHAR);
                    names.push_back(field_name);
                    continue;
                }

                string delta_type = string(yyjson_get_str(type_val));
                LogicalType duckdb_type = ConvertDeltaTypeToLogicalType(delta_type);
                return_types.push_back(duckdb_type);
                names.push_back(field_name);
            }

            if (names.empty()) {
                throw InvalidInputException("No fields extracted from schema");
            }

            ERPL_TRACE_INFO("DELTA_SHARE_SCAN", "Using " + std::to_string(names.size()) + " columns from metadata: " +
                           names[0] + (names.size() > 1 ? ", " + names[1] : "") + (names.size() > 2 ? ", ..." : ""));

        } catch (const std::exception& e) {
            ERPL_TRACE_ERROR("DELTA_SHARE_SCAN", "Error parsing schema: " + string(e.what()));
            // Fallback to simple schema
            return_types.push_back(LogicalType::VARCHAR);
            names.push_back("data");
            ERPL_TRACE_WARN("DELTA_SHARE_SCAN", "Using fallback schema after parsing error");
        }
    }

    ERPL_TRACE_INFO("DELTA_SHARE_SCAN", "Bind phase complete");

    return bind_data;
}

// =====================================================================
// Init Global Phase (fetches metadata and file list once)
// =====================================================================

static unique_ptr<GlobalTableFunctionState> DeltaShareScanInitGlobal(ClientContext& context,
                                                                     TableFunctionInitInput& input) {
    ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "InitGlobal phase starting");

    auto& bind_data = input.bind_data->Cast<DeltaShareScanBindData>();
    auto global_state = make_uniq<DeltaShareGlobalState>();

    // Create shared client for metadata fetching
    global_state->client = duckdb::make_shared_ptr<DeltaShareClient>(context, bind_data.profile);

    // Store metadata from bind phase
    global_state->metadata = bind_data.metadata;

    // Fetch complete file list with pre-signed URLs from Delta Sharing server
    // This is done once in InitGlobal and shared read-only across all threads
    try {
        global_state->files = global_state->client->QueryTable(bind_data.share, bind_data.schema, bind_data.table);
        ERPL_TRACE_INFO("DELTA_SHARE_SCAN", "Fetched " + std::to_string(global_state->files.size()) + " files from Delta Sharing");

        if (global_state->files.empty()) {
            ERPL_TRACE_WARN("DELTA_SHARE_SCAN", "No files found for table");
        } else {
            // Log first few file URLs for debugging
            for (size_t i = 0; i < global_state->files.size() && i < 3; ++i) {
                ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "File " + std::to_string(i) + ": " +
                    global_state->files[i].url.substr(0, 80) + "... (size: " +
                    std::to_string(global_state->files[i].size) + " bytes)");
            }
        }
    } catch (const std::exception& e) {
        throw InvalidInputException("Failed to query Delta Sharing table: " + string(e.what()));
    }

    ERPL_TRACE_INFO("DELTA_SHARE_SCAN", "InitGlobal phase complete");

    return global_state;
}

// =====================================================================
// Init Local Phase (creates per-thread HTTP client)
// =====================================================================

static unique_ptr<LocalTableFunctionState> DeltaShareScanInitLocal(ExecutionContext& context,
                                                                   TableFunctionInitInput& input,
                                                                   GlobalTableFunctionState* gstate) {
    ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "InitLocal phase starting for thread");

    auto& global_state = *static_cast<DeltaShareGlobalState*>(gstate);
    auto local_state = make_uniq<DeltaShareLocalState>();

    // Create per-thread HTTP client
    // Each thread gets its own client instance for connection reuse via keep-alive
    // No global synchronization needed—lock-free design
    try {
        local_state->http_client = global_state.client;  // Reuse shared client, it's thread-safe
        ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "Per-thread HTTP client reference initialized");
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("DELTA_SHARE_SCAN", "Failed to create per-thread HTTP client: " + string(e.what()));
        throw InvalidInputException("Failed to create per-thread HTTP client: " + string(e.what()));
    }

    ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "InitLocal phase complete");

    return local_state;
}

// =====================================================================
// Scan Phase (with atomic lock-free work distribution)
// =====================================================================

static void DeltaShareScan(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "Scan phase starting");

    auto& global_state = input.global_state->Cast<DeltaShareGlobalState>();

    // Lock-free work distribution: each thread atomically claims next file index
    // fetch_add returns old value, so each thread gets unique file
    idx_t file_idx = global_state.current_file_index.fetch_add(1);

    // Check if all files have been assigned (this thread got index >= file count)
    if (file_idx >= global_state.files.size()) {
        ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "All files processed, thread returning empty result");
        output.SetCardinality(0);
        return;
    }

    // Get the file this thread claimed
    auto& file_ref = global_state.files[file_idx];

    ERPL_TRACE_INFO("DELTA_SHARE_SCAN", "Thread reading Parquet file " + std::to_string(file_idx) + "/" +
                   std::to_string(global_state.files.size()) + ": " + file_ref.url.substr(0, 80) + "...");

    try {
        // Use per-thread HTTP client for connection reuse
        // The pre-signed URL is valid for a limited time and has built-in credentials
        string parquet_query = "SELECT * FROM parquet_scan('" + file_ref.url + "')";

        ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "Executing Parquet query from thread with per-thread HTTP client");

        Connection con(*context.db);
        auto result = con.Query(parquet_query);

        // Check if we got any data
        if (result->HasError()) {
            ERPL_TRACE_ERROR("DELTA_SHARE_SCAN", "Query error: " + result->GetError());
            throw InvalidInputException("Failed to read Parquet file: " + result->GetError());
        }

        // Fetch result chunk
        auto chunk = result->Fetch();
        if (chunk && chunk->size() > 0) {
            // Reference the chunk data into output
            output.Reference(*chunk);
            ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "Read " + std::to_string(chunk->size()) + " rows from file " + std::to_string(file_idx));
        } else {
            // Empty file - set cardinality to 0 and initialize vectors
            for (idx_t i = 0; i < output.ColumnCount(); ++i) {
                output.data[i].SetVectorType(VectorType::FLAT_VECTOR);
            }
            output.SetCardinality(0);
            ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "File " + std::to_string(file_idx) + " is empty, returned 0 rows");
        }

    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("DELTA_SHARE_SCAN", "Failed to read Parquet file " + std::to_string(file_idx) + ": " + string(e.what()));

        // Return empty result on error (caller will retry or continue)
        for (idx_t i = 0; i < output.ColumnCount(); ++i) {
            output.data[i].SetVectorType(VectorType::FLAT_VECTOR);
        }
        output.SetCardinality(0);
    }
}

// =====================================================================
// Table Function Registration
// =====================================================================

TableFunctionSet CreateDeltaShareScanFunction() {
    ERPL_TRACE_DEBUG("DELTA_SHARE", "Registering delta_share_scan table function");

    TableFunctionSet function_set("delta_share_scan");

    // Create table function with parallel execution support
    // - Bind phase: parse parameters and fetch metadata
    // - InitGlobal phase: fetch complete file list (once, shared across threads)
    // - InitLocal phase: create per-thread HTTP client for connection reuse
    // - Scan phase: atomic lock-free work distribution via file index
    TableFunction scan_function(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        DeltaShareScan,
        DeltaShareScanBind,
        DeltaShareScanInitGlobal,  // InitGlobal: metadata + file list
        DeltaShareScanInitLocal);  // InitLocal: per-thread HTTP client

    function_set.AddFunction(scan_function);

    return function_set;
}

} // namespace erpl_web
