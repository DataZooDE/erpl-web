#include "delta_share_scan.hpp"
#include "tracing.hpp"

namespace erpl_web {

// =====================================================================
// Bind Phase
// =====================================================================

static unique_ptr<FunctionData> DeltaShareScanBind(ClientContext& context,
                                                   TableFunctionBindInput& input,
                                                   vector<LogicalType>& return_types,
                                                   vector<string>& names) {
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
        auto client = shared_ptr<DeltaShareClient>(new DeltaShareClient(context, bind_data->profile));
        bind_data->metadata = client->GetTableMetadata(bind_data->share, bind_data->schema, bind_data->table);
    } catch (const std::exception& e) {
        throw InvalidInputException("Failed to fetch Delta Sharing table metadata: " + string(e.what()));
    }

    // Convert Delta Lake schema to DuckDB LogicalTypes
    // For now, use a simple placeholder schema
    // TODO: Parse Delta schema JSON to extract column types
    if (bind_data->metadata.column_names.empty()) {
        // Fallback: if no schema extracted, use a simple schema
        return_types.push_back(LogicalType::VARCHAR);
        names.push_back("data");
        ERPL_TRACE_WARN("DELTA_SHARE_SCAN", "Using fallback schema - no columns extracted from metadata");
    } else {
        // Use extracted column names from metadata
        for (const auto& col_name : bind_data->metadata.column_names) {
            return_types.push_back(LogicalType::VARCHAR);  // TODO: proper type mapping
            names.push_back(col_name);
        }
        ERPL_TRACE_INFO("DELTA_SHARE_SCAN", "Using " + std::to_string(names.size()) + " columns from metadata");
    }

    ERPL_TRACE_INFO("DELTA_SHARE_SCAN", "Bind phase complete");

    return bind_data;
}

// =====================================================================
// Init Phase
// =====================================================================

static unique_ptr<GlobalTableFunctionState> DeltaShareScanInit(ClientContext& context,
                                                               TableFunctionInitInput& input) {
    ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "Init phase starting");

    auto& bind_data = input.bind_data->Cast<DeltaShareScanBindData>();
    auto global_state = make_uniq<DeltaShareGlobalState>();

    // Create client - use duckdb's make_shared_ptr
    global_state->client = shared_ptr<DeltaShareClient>(new DeltaShareClient(context, bind_data.profile));

    // Fetch file list from Delta Sharing server
    try {
        global_state->files = global_state->client->QueryTable(bind_data.share, bind_data.schema, bind_data.table);
        ERPL_TRACE_INFO("DELTA_SHARE_SCAN", "Fetched " + std::to_string(global_state->files.size()) + " files from Delta Sharing");

        if (global_state->files.empty()) {
            ERPL_TRACE_WARN("DELTA_SHARE_SCAN", "No files found for table");
            global_state->finished = true;
        } else {
            // Log file URLs for debugging
            for (size_t i = 0; i < global_state->files.size() && i < 3; ++i) {
                ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "File " + std::to_string(i) + ": " +
                    global_state->files[i].url.substr(0, 80) + "... (size: " +
                    std::to_string(global_state->files[i].size) + " bytes)");
            }
        }
    } catch (const std::exception& e) {
        throw InvalidInputException("Failed to query Delta Sharing table: " + string(e.what()));
    }

    ERPL_TRACE_INFO("DELTA_SHARE_SCAN", "Init phase complete");

    return global_state;
}

// =====================================================================
// Scan Phase
// =====================================================================

static void DeltaShareScan(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "Scan phase - current_file: " + std::to_string(input.global_state->Cast<DeltaShareGlobalState>().current_file_index));

    auto& global_state = input.global_state->Cast<DeltaShareGlobalState>();

    // Check if we've finished processing all files
    if (global_state.finished) {
        output.SetCardinality(0);
        ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "Scan complete - all files processed");
        return;
    }

    // If no files, return empty
    if (global_state.files.empty()) {
        output.SetCardinality(0);
        global_state.finished = true;
        ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "No files to scan");
        return;
    }

    // For MVP: Return file information (URLs, sizes, IDs)
    // TODO: Integrate with parquet_scan to actually read Parquet data from pre-signed URLs

    // Create output with file metadata for now
    idx_t row_count = 0;

    while (global_state.current_file_index < global_state.files.size() && row_count < STANDARD_VECTOR_SIZE) {
        auto& file_ref = global_state.files[global_state.current_file_index];
        string url_value = file_ref.url;

        // Set first column to file URL
        if (output.ColumnCount() > 0) {
            output.SetValue(0, row_count, Value(url_value));
        }

        // If we have more columns, set them to file metadata
        if (output.ColumnCount() > 1) {
            string size_value = std::to_string(file_ref.size);
            output.SetValue(1, row_count, Value(size_value));
        }

        ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "Processing file " + std::to_string(global_state.current_file_index) +
            ": " + url_value.substr(0, 60) + "...");

        row_count++;
        global_state.current_file_index++;

        // For MVP, return one file per batch to show it's working
        break;
    }

    // If we've processed all files, mark as finished
    if (global_state.current_file_index >= global_state.files.size()) {
        global_state.finished = true;
        ERPL_TRACE_INFO("DELTA_SHARE_SCAN", "Finished scanning all " + std::to_string(global_state.files.size()) + " files");
    }

    output.SetCardinality(row_count);
    ERPL_TRACE_DEBUG("DELTA_SHARE_SCAN", "Returning " + std::to_string(row_count) + " rows");
}

// =====================================================================
// Table Function Registration
// =====================================================================

TableFunctionSet CreateDeltaShareScanFunction() {
    ERPL_TRACE_DEBUG("DELTA_SHARE", "Registering delta_share_scan table function");

    TableFunctionSet function_set("delta_share_scan");

    TableFunction scan_function(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        DeltaShareScan, DeltaShareScanBind, DeltaShareScanInit);

    function_set.AddFunction(scan_function);

    return function_set;
}

} // namespace erpl_web
