#pragma once

#include "delta_share_types.hpp"
#include "delta_share_client.hpp"
#include "duckdb/function/table_function.hpp"
#include <memory>

using namespace duckdb;

namespace erpl_web {

// Bind data for delta_share_scan table function
struct DeltaShareScanBindData : public TableFunctionData {
    DeltaShareProfile profile;
    string share;
    string schema;
    string table;
    DeltaTableMetadata metadata;
    // The schema the bind settled on. The scan needs it to align each file to the bound
    // types: the share server controls BOTH the declared Delta schema and the parquet
    // files, so the two can disagree, and referencing a mismatched vector straight into
    // the output raises ExceptionType::INTERNAL - which DuckDB treats as fatal to the
    // whole database instance (GitHub #207).
    vector<LogicalType> column_types;
    vector<string> column_names;
    // Note: files, current_file_index, and finished are stored in global state instead

    // Constructor and copy prevention
    DeltaShareScanBindData() = default;
    DeltaShareScanBindData(const DeltaShareScanBindData&) = delete;
    DeltaShareScanBindData& operator=(const DeltaShareScanBindData&) = delete;
};

// Global state for delta_share_scan (extends GlobalTableFunctionState)
// Follows DuckDB Parquet extension pattern with atomic work distribution
struct DeltaShareGlobalState : public GlobalTableFunctionState {
    // Shared metadata (read-only)
    shared_ptr<DeltaShareClient> client;
    DeltaTableMetadata metadata;
    vector<DeltaFileReference> files;

    // Lock-free work distribution (Parquet pattern)
    // Atomic index for thread-safe file claiming without locks
    atomic<idx_t> current_file_index = 0;

    // Explicitly single-threaded for now. The atomic claim above and the per-thread local
    // state make the design parallel-ready, but nothing tests it under concurrency, and
    // inheriting the base's 1 by accident is not the same as deciding it. Raising this is a
    // separate, testable change (GitHub #207).
    idx_t MaxThreads() const override {
        return 1;
    }
    // Note: current_batch unused (implicit in current_file_index)
    // Note: finished unused (implicit when current_file_index >= files.size())
};

// Local state for delta_share_scan (extends LocalTableFunctionState)
// Follows DuckDB Parquet extension pattern with per-thread resources
struct DeltaShareLocalState : public LocalTableFunctionState {
    // Per-thread HTTP client for connection reuse via keep-alive
    // Each thread gets its own client—no global synchronization needed
    shared_ptr<DeltaShareClient> http_client;

    // The file this thread is currently draining. The reader for one file spans MANY scan
    // calls - one per output chunk - so the connection and the result it owns have to live
    // here, not on the stack of a single call. Emitting a chunk that references a result
    // destroyed at the end of that call is a use-after-free, and advancing to the next
    // file after a single chunk drops every row past the first 2048 (GitHub #207).
    duckdb::unique_ptr<duckdb::Connection> connection;
    duckdb::unique_ptr<duckdb::QueryResult> result;
    duckdb::unique_ptr<duckdb::DataChunk> current_chunk;

    // Note: which file this is comes from the atomic claim in the global state.
};

// Table function set creation
TableFunctionSet CreateDeltaShareScanFunction();

} // namespace erpl_web
