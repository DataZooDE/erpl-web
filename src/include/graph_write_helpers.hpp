#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include <string>
#include <vector>

namespace erpl_web {

// Serialise a single DuckDB Value to a JSON-safe string fragment (no surrounding quotes for strings).
// Returns the JSON literal: "\"hello\"", "42", "3.14", "true", "null", etc.
inline std::string DuckDbValueToJsonLiteral(const duckdb::Value &v) {
    if (v.IsNull()) {
        return "null";
    }
    switch (v.type().id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
        return v.GetValue<bool>() ? "true" : "false";
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::HUGEINT:
    case duckdb::LogicalTypeId::UTINYINT:
    case duckdb::LogicalTypeId::USMALLINT:
    case duckdb::LogicalTypeId::UINTEGER:
    case duckdb::LogicalTypeId::UBIGINT:
        return v.ToString();
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE:
    case duckdb::LogicalTypeId::DECIMAL:
        return v.ToString();
    default: {
        // Strings, timestamps, dates, blobs etc — serialise as JSON strings with escaping
        std::string raw = v.ToString();
        std::string escaped;
        escaped.reserve(raw.size() + 2);
        escaped += '"';
        for (char c : raw) {
            if (c == '"') {
                escaped += "\\\"";
            } else if (c == '\\') {
                escaped += "\\\\";
            } else if (c == '\n') {
                escaped += "\\n";
            } else if (c == '\r') {
                escaped += "\\r";
            } else if (c == '\t') {
                escaped += "\\t";
            } else {
                escaped += c;
            }
        }
        escaped += '"';
        return escaped;
    }
    }
}

// Build a SharePoint fields JSON object from one row of a DataChunk.
// Skips columns whose name is "id" (read-only system field).
// Returns e.g. {"Title":"Foo","Amount":42}
inline std::string DataChunkRowToSharePointFieldsJson(const duckdb::DataChunk &chunk,
                                                       duckdb::idx_t row,
                                                       const std::vector<std::string> &column_names)
{
    std::string json = "{";
    bool first = true;
    for (duckdb::idx_t col = 0; col < column_names.size(); col++) {
        const std::string &name = column_names[col];
        if (name == "id") {
            continue;  // read-only system field, never written
        }
        const duckdb::Value v = chunk.GetValue(col, row);
        if (!first) {
            json += ',';
        }
        first = false;
        // Escape the field name
        json += '"';
        for (char c : name) {
            if (c == '"') { json += "\\\""; }
            else if (c == '\\') { json += "\\\\"; }
            else { json += c; }
        }
        json += "\":";
        json += DuckDbValueToJsonLiteral(v);
    }
    json += '}';
    return json;
}

// Find the column index of "id" in a column name list (-1 if not found).
inline duckdb::idx_t FindIdColumnIndex(const std::vector<std::string> &column_names) {
    for (duckdb::idx_t i = 0; i < static_cast<duckdb::idx_t>(column_names.size()); i++) {
        if (column_names[i] == "id") {
            return i;
        }
    }
    return static_cast<duckdb::idx_t>(-1);
}

// Build a 2-D array of row values for the Excel "rows/add" endpoint.
// Returns e.g. {"values":[[1,"Alice",true],[2,"Bob",false]]}
inline std::string DataChunkToExcelAddRowsBody(const duckdb::DataChunk &chunk,
                                                const std::vector<std::string> &column_names)
{
    std::string json = "{\"values\":[";
    const duckdb::idx_t row_count = chunk.size();
    const duckdb::idx_t col_count = static_cast<duckdb::idx_t>(column_names.size());

    for (duckdb::idx_t row = 0; row < row_count; row++) {
        if (row > 0) { json += ','; }
        json += '[';
        for (duckdb::idx_t col = 0; col < col_count; col++) {
            if (col > 0) { json += ','; }
            json += DuckDbValueToJsonLiteral(chunk.GetValue(col, row));
        }
        json += ']';
    }
    json += "]}";
    return json;
}

} // namespace erpl_web
