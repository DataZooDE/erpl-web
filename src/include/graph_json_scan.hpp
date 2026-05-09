#pragma once

#include "duckdb/function/table_function.hpp"
#include "yyjson.hpp"
#include <string>

namespace erpl_web {

struct GraphJsonArrayScanBindData : public duckdb::TableFunctionData {
    std::string json_response;
    duckdb_yyjson::yyjson_doc *parsed_doc = nullptr;
    duckdb_yyjson::yyjson_arr_iter item_iter = {};
    bool done = false;

    ~GraphJsonArrayScanBindData() override {
        ResetDoc();
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
