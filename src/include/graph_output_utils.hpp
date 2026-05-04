#pragma once

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "yyjson.hpp"

// Typed FlatVector write helpers used by all graph_* scan functions.
// Replaces output.SetValue(col, row, Value(...)) — avoids generic Value boxing/unboxing.
// All helpers write to an already-flat Vector; validity defaults to all-valid so only
// nulls need explicit SetInvalid.

namespace erpl_web {

using namespace duckdb_yyjson;

inline void SetStrCell(duckdb::Vector &vec, duckdb::idx_t row, yyjson_val *val) {
    if (val && yyjson_is_str(val)) {
        duckdb::FlatVector::GetData<duckdb::string_t>(vec)[row] =
            duckdb::StringVector::AddString(vec, yyjson_get_str(val));
    } else {
        duckdb::FlatVector::Validity(vec).SetInvalid(row);
    }
}

inline void SetStrCellNN(duckdb::Vector &vec, duckdb::idx_t row, const char *str) {
    duckdb::FlatVector::GetData<duckdb::string_t>(vec)[row] =
        duckdb::StringVector::AddString(vec, str);
}

inline void SetBoolCell(duckdb::Vector &vec, duckdb::idx_t row, yyjson_val *val) {
    if (val && yyjson_is_bool(val)) {
        duckdb::FlatVector::GetData<bool>(vec)[row] = yyjson_get_bool(val);
    } else {
        duckdb::FlatVector::Validity(vec).SetInvalid(row);
    }
}

inline void SetBoolCellNN(duckdb::Vector &vec, duckdb::idx_t row, bool b) {
    duckdb::FlatVector::GetData<bool>(vec)[row] = b;
}

inline void SetInt64Cell(duckdb::Vector &vec, duckdb::idx_t row, yyjson_val *val) {
    if (val && yyjson_is_num(val)) {
        duckdb::FlatVector::GetData<int64_t>(vec)[row] = yyjson_get_sint(val);
    } else {
        duckdb::FlatVector::Validity(vec).SetInvalid(row);
    }
}

inline void SetInt32Cell(duckdb::Vector &vec, duckdb::idx_t row, yyjson_val *val) {
    if (val && yyjson_is_num(val)) {
        duckdb::FlatVector::GetData<int32_t>(vec)[row] = yyjson_get_int(val);
    } else {
        duckdb::FlatVector::Validity(vec).SetInvalid(row);
    }
}

} // namespace erpl_web
