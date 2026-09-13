#pragma once

#include "duckdb/function/table_function.hpp"
#include "yyjson.hpp"
#include <string>

namespace erpl_web {

// Per-execution scan cursor: the fetched response, the parsed document and the
// array iterator over it. A fresh instance is created for every execution of
// the bound plan, which is what makes a bound plan re-executable (GitHub #75, #202).
//
// The iterator sits in GLOBAL rather than local state, which is only safe because none of
// these functions overrides MaxThreads(): the base GlobalTableFunctionState returns 1, so
// DuckDB runs one thread per scan and no two threads call yyjson_arr_iter_next on this
// iterator. Any function that later declares itself parallel must move the iterator into
// a LocalTableFunctionState and leave only the payload here.
struct GraphJsonArrayScanState : public duckdb::GlobalTableFunctionState {
    std::string json_response;
    duckdb_yyjson::yyjson_doc *parsed_doc = nullptr;
    duckdb_yyjson::yyjson_arr_iter item_iter = {};
    bool done = false;

    ~GraphJsonArrayScanState() override {
        ResetDoc();
    }

    static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(duckdb::ClientContext &,
                                                                    duckdb::TableFunctionInitInput &) {
        return duckdb::make_uniq<GraphJsonArrayScanState>();
    }

    // Seed the state from a response the bind phase already fetched (kept on the bind
    // data as an immutable payload, so every execution can re-seed from it).
    //
    // Re-execution contract. A reader that fetches in the SCAN re-authorizes and re-fetches
    // on every execution, so a repeated EXECUTE sees current data and a revoked credential
    // fails. A reader that fetches at BIND and re-seeds through this method replays that
    // bind-time snapshot instead: no second request, no second authorization. Use SeedFrom
    // only where bind must fetch anyway to infer the schema (graph_excel_read), and say so
    // in the function's description; everywhere else fetch in the scan.
    void SeedFrom(const std::string &payload) {
        json_response = payload;
    }

    bool NeedsFetch() const {
        return parsed_doc == nullptr && json_response.empty();
    }

    bool InitIterator(const char *array_key = "value") {
        ResetDoc();
        parsed_doc = duckdb_yyjson::yyjson_read(json_response.c_str(), json_response.length(), 0);
        json_response.clear();
        json_response.shrink_to_fit();
        if (!parsed_doc) {
            return false;
        }

        auto *root = duckdb_yyjson::yyjson_doc_get_root(parsed_doc);
        auto *arr = duckdb_yyjson::yyjson_obj_get(root, array_key);
        if (!arr || !duckdb_yyjson::yyjson_is_arr(arr)) {
            return false;
        }

        duckdb_yyjson::yyjson_arr_iter_init(arr, &item_iter);
        return true;
    }

private:
    void ResetDoc() {
        if (parsed_doc) {
            duckdb_yyjson::yyjson_doc_free(parsed_doc);
            parsed_doc = nullptr;
        }
    }
};

// Per-execution cursor for a scan that follows @odata.nextLink across pages, holding one
// page at a time (the Teams and Outlook readers). The paging position belongs here for
// the same reason the iterator does: a bound plan executed twice must start at page one.
struct GraphPagedScanState : public duckdb::GlobalTableFunctionState {
    duckdb_yyjson::yyjson_doc *current_doc = nullptr;
    duckdb_yyjson::yyjson_arr_iter item_iter = {};
    std::string next_url;
    bool initialized = false;
    bool done = false;

    ~GraphPagedScanState() override {
        FreeDoc();
    }

    static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(duckdb::ClientContext &,
                                                                    duckdb::TableFunctionInitInput &) {
        return duckdb::make_uniq<GraphPagedScanState>();
    }

    // Parses one page body, takes its @odata.nextLink as the next page to fetch, and
    // points the iterator at `array_key`.
    bool LoadPage(const std::string &body, const char *array_key = "value") {
        FreeDoc();
        current_doc = duckdb_yyjson::yyjson_read(body.c_str(), body.size(), 0);
        if (!current_doc) {
            return false;
        }
        auto *root = duckdb_yyjson::yyjson_doc_get_root(current_doc);
        auto *next = duckdb_yyjson::yyjson_obj_get(root, "@odata.nextLink");
        next_url = (next && duckdb_yyjson::yyjson_is_str(next)) ? duckdb_yyjson::yyjson_get_str(next) : "";
        auto *arr = duckdb_yyjson::yyjson_obj_get(root, array_key);
        if (!arr || !duckdb_yyjson::yyjson_is_arr(arr)) {
            return false;
        }
        duckdb_yyjson::yyjson_arr_iter_init(arr, &item_iter);
        return true;
    }

private:
    void FreeDoc() {
        if (current_doc) {
            duckdb_yyjson::yyjson_doc_free(current_doc);
            current_doc = nullptr;
        }
    }
};

// Per-execution cursor for a scan that walks IMMUTABLE row vectors already materialised
// on the bind data (the Entra readers). Only the position is per-execution; the payload
// is shared, because there is nothing there to clone.
struct GraphRowCursorState : public duckdb::GlobalTableFunctionState {
    duckdb::idx_t current_idx = 0;
    bool done = false;

    static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(duckdb::ClientContext &,
                                                                    duckdb::TableFunctionInitInput &) {
        return duckdb::make_uniq<GraphRowCursorState>();
    }
};

} // namespace erpl_web
