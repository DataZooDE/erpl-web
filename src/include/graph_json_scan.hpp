#pragma once

#include "duckdb/function/table_function.hpp"
#include "yyjson.hpp"
#include <string>

namespace erpl_web {

// Bind data for a Graph function that scans a JSON array out of a single
// response. It carries only the immutable request configuration: everything
// that advances while scanning lives in GraphJsonArrayScanState, so a bound
// plan that is EXECUTEd twice starts from the beginning both times (GitHub #75).
struct GraphJsonArrayScanBindData : public duckdb::TableFunctionData {
};

// Per-execution scan cursor: the fetched response, the parsed document and the
// array iterator over it. A fresh instance is created for every execution of
// the bound plan.
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

    // Seed the state from a response the bind phase already fetched (kept on the
    // bind data as an immutable payload, so every execution can re-seed from it).
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

} // namespace erpl_web
