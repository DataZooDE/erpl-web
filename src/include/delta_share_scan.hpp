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
    // Note: current_batch unused (implicit in current_file_index)
    // Note: finished unused (implicit when current_file_index >= files.size())
};

// Local state for delta_share_scan (extends LocalTableFunctionState)
// Follows DuckDB Parquet extension pattern with per-thread resources
struct DeltaShareLocalState : public LocalTableFunctionState {
    // Per-thread HTTP client for connection reuse via keep-alive
    // Each thread gets its own client—no global synchronization needed
    shared_ptr<DeltaShareClient> http_client;
    // Note: current_file_index is managed via atomic in global state
};

// Table function set creation
TableFunctionSet CreateDeltaShareScanFunction();

} // namespace erpl_web
