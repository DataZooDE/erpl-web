#pragma once

#include "duckdb/function/table_function.hpp"

namespace erpl_web {

// Per-execution cursor for a table function that walks rows already materialised on its
// bind data (a discovery or describe function that fetched everything during bind).
//
// The position must not live on the bind data. DuckDB reuses bind data across executions
// of a bound plan and resets nothing, so a cursor kept there is already spent on the
// second EXECUTE and the scan returns zero rows with no error - the defect class behind
// GitHub #75, #191 and #202. Only the position is per-execution; the rows stay shared,
// because there is nothing there to clone.
//
// Registering this as a function's init_global is what makes the plan re-executable:
//
//     fn.init_global = ScanRowCursorState::Init;
//
// A one-shot function that must NOT repeat its work per execution (a writer that performs
// its write in the scan) deliberately keeps its flag on the bind data and says so there.
struct ScanRowCursorState : public duckdb::GlobalTableFunctionState {
    duckdb::idx_t current_index = 0;
    bool finished = false;

    static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(duckdb::ClientContext &,
                                                                    duckdb::TableFunctionInitInput &) {
        return duckdb::make_uniq<ScanRowCursorState>();
    }
};

} // namespace erpl_web
