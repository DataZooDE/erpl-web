#include "graph_excel_functions.hpp"
#include "graph_client.hpp"
#include "graph_excel_client.hpp"
#include "graph_sharepoint_client.hpp"
#include "graph_excel_secret.hpp"
#include "tracing.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace erpl_web {

using namespace duckdb;

// ============================================================================
// Bind Data Structures
// ============================================================================

struct ListFilesBindData : public TableFunctionData {
    std::string secret_name;
    std::string folder_path;
    std::string drive_id;
    std::string json_response;
    idx_t current_idx = 0;
    bool done = false;
};

struct ExcelTablesBindData : public TableFunctionData {
    std::string secret_name;
    std::string file_path;
    std::string drive_id;
    std::string json_response;
    idx_t current_idx = 0;
    bool done = false;
};

struct ExcelWorksheetsBindData : public TableFunctionData {
    std::string secret_name;
    std::string file_path;
    std::string drive_id;
    std::string json_response;
    idx_t current_idx = 0;
    bool done = false;
};

struct ExcelRangeBindData : public TableFunctionData {
    std::string secret_name;
    std::string file_path;
    std::string sheet_name;
    std::string range;  // Optional, empty means usedRange
    std::string drive_id;
    std::string json_response;
    vector<LogicalType> column_types;  // per-column types inferred from valueTypes
    // JSON is parsed once on first scan call, then freed. cached_rows[row][col].
    std::vector<std::vector<duckdb::Value>> cached_rows;
    size_t current_row = 0;  // index into cached_rows
    bool rows_cached = false;
    bool done = false;
};

struct ExcelTableDataBindData : public TableFunctionData {
    std::string secret_name;
    std::string file_path;
    std::string table_name;
    std::string drive_id;
    std::string json_response;
    idx_t current_idx = 0;
    bool done = false;
};

// ============================================================================
// Helper Functions
// ============================================================================

// Year tokens (y/Y) only appear in date format codes, never in pure number/time formats.
// This is the reliable server-side signal that a cell column holds date values.
static bool IsDateFormatCode(const char *fmt) {
    if (!fmt) {
        return false;
    }
    bool in_literal = false;
    char literal_char = 0;
    for (const char *p = fmt; *p; ++p) {
        // Skip quoted literals: "..." or '...'
        if (*p == '"' || *p == '\'') {
            if (!in_literal) {
                in_literal = true;
                literal_char = *p;
            } else if (*p == literal_char) {
                in_literal = false;
            }
            continue;
        }
        if (in_literal) {
            continue;
        }
        if (*p == 'y' || *p == 'Y') {
            return true;
        }
    }
    return false;
}

// Convert a single yyjson cell value to a DuckDB Value of the given column type.
static duckdb::Value ExtractCellValue(yyjson_val *cell_val, yyjson_val *cell_type_val,
                                      const LogicalType &col_type) {
    const bool is_empty = !cell_val || yyjson_is_null(cell_val) ||
                          (cell_type_val && yyjson_is_str(cell_type_val) &&
                           std::string(yyjson_get_str(cell_type_val)) == "Empty");
    if (is_empty) {
        return Value();
    }

    if (col_type.id() == LogicalTypeId::DATE) {
        if (yyjson_is_num(cell_val)) {
            const int32_t epoch_days = static_cast<int32_t>(yyjson_get_num(cell_val)) - 25569;
            return Value::DATE(date_t(epoch_days));
        }
        if (yyjson_is_str(cell_val)) {
            const char *s = yyjson_get_str(cell_val);
            date_t parsed;
            idx_t pos = 0;
            bool special = false;
            if (Date::TryConvertDate(s, strlen(s), pos, parsed, special) == DateCastResult::SUCCESS) {
                return Value::DATE(parsed);
            }
        }
        return Value();
    }

    if (col_type.id() == LogicalTypeId::DOUBLE && yyjson_is_num(cell_val)) {
        return Value::DOUBLE(yyjson_get_num(cell_val));
    }
    if (col_type.id() == LogicalTypeId::BOOLEAN && yyjson_is_bool(cell_val)) {
        return Value::BOOLEAN(yyjson_get_bool(cell_val));
    }

    // Fallback: stringify
    if (yyjson_is_str(cell_val)) {
        return Value(std::string(yyjson_get_str(cell_val)));
    }
    if (yyjson_is_num(cell_val)) {
        return Value(std::to_string(yyjson_get_num(cell_val)));
    }
    if (yyjson_is_bool(cell_val)) {
        return Value(yyjson_get_bool(cell_val) ? "true" : "false");
    }
    return Value();
}

// Resolves a Graph drive ID from either an explicit drive_id named param or
// site + drive named params (name-based lookup). Returns empty string if none given.
static std::string ResolveGraphDriveId(ClientContext &context,
                                       const std::string &secret_name,
                                       const TableFunctionBindInput &input) {
    const bool has_drive = input.named_parameters.find("drive") != input.named_parameters.end();
    const bool has_site  = input.named_parameters.find("site")  != input.named_parameters.end();

    if (!has_drive && !has_site) {
        return "";
    }

    if (has_drive) {
        const std::string drive_param = input.named_parameters.at("drive").GetValue<std::string>();

        // Raw drive ID (b!...) — use directly, no API call needed
        if (GraphSharePointClient::LooksLikeDriveId(drive_param)) {
            return drive_param;
        }

        // Web URL — resolve by matching webUrl field across the site's drives
        if (drive_param.rfind("https://", 0) == 0 || drive_param.rfind("http://", 0) == 0) {
            auto auth_info = ResolveGraphAuth(context, secret_name);
            GraphSharePointClient sp_client(auth_info.auth_params);
            return sp_client.ResolveDriveIdFromUrl(drive_param);
        }

        // Display name — requires site for lookup
        if (!has_site) {
            throw BinderException(
                "The 'drive' parameter requires 'site' when specifying a drive by name. "
                "Pass a drive ID (b!...) or URL (https://...) to omit 'site'.");
        }
    }

    // Name-based or auto-select: resolve site first, then drive within it
    auto auth_info = ResolveGraphAuth(context, secret_name);
    GraphSharePointClient sp_client(auth_info.auth_params);
    const std::string site_param  = input.named_parameters.at("site").GetValue<std::string>();
    const std::string drive_param = has_drive
        ? input.named_parameters.at("drive").GetValue<std::string>()
        : "";
    const std::string site_id = sp_client.ResolveSiteId(site_param);
    return sp_client.ResolveDriveId(site_id, drive_param);
}

// ============================================================================
// graph_list_files - List files in OneDrive
// ============================================================================

unique_ptr<FunctionData> GraphExcelFunctions::ListFilesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<ListFilesBindData>();

    // Optional folder_path is first positional param
    if (!input.inputs.empty() && !input.inputs[0].IsNull()) {
        bind_data->folder_path = input.inputs[0].GetValue<std::string>();
    }

    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        bind_data->secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->drive_id = ResolveGraphDriveId(context, bind_data->secret_name, input);

    // Return schema: id, name, webUrl, size, createdDateTime, lastModifiedDateTime, mimeType, isFolder
    names = {"id", "name", "web_url", "size", "created_at", "modified_at", "mime_type", "is_folder"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // name
        LogicalType::VARCHAR,   // web_url
        LogicalType::BIGINT,    // size
        LogicalType::VARCHAR,   // created_at
        LogicalType::VARCHAR,   // modified_at
        LogicalType::VARCHAR,   // mime_type
        LogicalType::BOOLEAN    // is_folder
    };

    return std::move(bind_data);
}

void GraphExcelFunctions::ListFilesScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<ListFilesBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    // Fetch data if not already fetched
    if (bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphExcelClient client(auth_info.auth_params);
        bind_data.json_response = client.ListDriveFiles(bind_data.folder_path, bind_data.drive_id);
    }

    // Parse JSON response
    yyjson_doc *doc = yyjson_read(bind_data.json_response.c_str(), bind_data.json_response.length(), 0);
    if (!doc) {
        throw InvalidInputException("Failed to parse Graph API response");
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    if (!value_arr || !yyjson_is_arr(value_arr)) {
        yyjson_doc_free(doc);
        bind_data.done = true;
        output.SetCardinality(0);
        return;
    }

    const size_t total = yyjson_arr_size(value_arr);
    idx_t row = 0;

    while (bind_data.current_idx < total && row < STANDARD_VECTOR_SIZE) {
        yyjson_val *item = yyjson_arr_get(value_arr, bind_data.current_idx);

        // id
        yyjson_val *id_val = yyjson_obj_get(item, "id");
        output.SetValue(0, row, id_val && yyjson_is_str(id_val) ?
            Value(yyjson_get_str(id_val)) : Value());

        // name
        yyjson_val *name_val = yyjson_obj_get(item, "name");
        output.SetValue(1, row, name_val && yyjson_is_str(name_val) ?
            Value(yyjson_get_str(name_val)) : Value());

        // webUrl
        yyjson_val *url_val = yyjson_obj_get(item, "webUrl");
        output.SetValue(2, row, url_val && yyjson_is_str(url_val) ?
            Value(yyjson_get_str(url_val)) : Value());

        // size
        yyjson_val *size_val = yyjson_obj_get(item, "size");
        output.SetValue(3, row, size_val && yyjson_is_num(size_val) ?
            Value::BIGINT(yyjson_get_sint(size_val)) : Value());

        // createdDateTime
        yyjson_val *created_val = yyjson_obj_get(item, "createdDateTime");
        output.SetValue(4, row, created_val && yyjson_is_str(created_val) ?
            Value(yyjson_get_str(created_val)) : Value());

        // lastModifiedDateTime
        yyjson_val *modified_val = yyjson_obj_get(item, "lastModifiedDateTime");
        output.SetValue(5, row, modified_val && yyjson_is_str(modified_val) ?
            Value(yyjson_get_str(modified_val)) : Value());

        // mimeType (from file object)
        yyjson_val *file_obj = yyjson_obj_get(item, "file");
        yyjson_val *mime_val = file_obj ? yyjson_obj_get(file_obj, "mimeType") : nullptr;
        output.SetValue(6, row, mime_val && yyjson_is_str(mime_val) ?
            Value(yyjson_get_str(mime_val)) : Value());

        // isFolder (folder object exists)
        yyjson_val *folder_obj = yyjson_obj_get(item, "folder");
        output.SetValue(7, row, Value::BOOLEAN(folder_obj != nullptr));

        row++;
        bind_data.current_idx++;
    }

    yyjson_doc_free(doc);
    bind_data.done = bind_data.current_idx >= total;
    output.SetCardinality(row);
}

// ============================================================================
// graph_excel_tables - List tables in Excel workbook
// ============================================================================

unique_ptr<FunctionData> GraphExcelFunctions::ExcelTablesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<ExcelTablesBindData>();

    // file_path is required positional parameter
    if (input.inputs.empty()) {
        throw BinderException("graph_excel_tables requires a file_path parameter");
    }
    bind_data->file_path = input.inputs[0].GetValue<std::string>();

    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        bind_data->secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->drive_id = ResolveGraphDriveId(context, bind_data->secret_name, input);

    // Return schema: name, id, showHeaders, showTotals
    names = {"name", "id", "show_headers", "show_totals"};
    return_types = {
        LogicalType::VARCHAR,   // name
        LogicalType::VARCHAR,   // id
        LogicalType::BOOLEAN,   // show_headers
        LogicalType::BOOLEAN    // show_totals
    };

    return std::move(bind_data);
}

void GraphExcelFunctions::ExcelTablesScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<ExcelTablesBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    if (bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphExcelClient client(auth_info.auth_params);
        bind_data.json_response = client.ListTablesByPath(bind_data.file_path, bind_data.drive_id);
    }

    yyjson_doc *doc = yyjson_read(bind_data.json_response.c_str(), bind_data.json_response.length(), 0);
    if (!doc) {
        throw InvalidInputException("Failed to parse Graph API response");
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    if (!value_arr || !yyjson_is_arr(value_arr)) {
        yyjson_doc_free(doc);
        bind_data.done = true;
        output.SetCardinality(0);
        return;
    }

    const size_t total = yyjson_arr_size(value_arr);
    idx_t row = 0;

    while (bind_data.current_idx < total && row < STANDARD_VECTOR_SIZE) {
        yyjson_val *item = yyjson_arr_get(value_arr, bind_data.current_idx);
        // name
        yyjson_val *name_val = yyjson_obj_get(item, "name");
        output.SetValue(0, row, name_val && yyjson_is_str(name_val) ?
            Value(yyjson_get_str(name_val)) : Value());

        // id
        yyjson_val *id_val = yyjson_obj_get(item, "id");
        output.SetValue(1, row, id_val && yyjson_is_str(id_val) ?
            Value(yyjson_get_str(id_val)) : Value());

        // showHeaders
        yyjson_val *headers_val = yyjson_obj_get(item, "showHeaders");
        output.SetValue(2, row, headers_val && yyjson_is_bool(headers_val) ?
            Value::BOOLEAN(yyjson_get_bool(headers_val)) : Value());

        // showTotals
        yyjson_val *totals_val = yyjson_obj_get(item, "showTotals");
        output.SetValue(3, row, totals_val && yyjson_is_bool(totals_val) ?
            Value::BOOLEAN(yyjson_get_bool(totals_val)) : Value());

        row++;
        bind_data.current_idx++;
    }

    yyjson_doc_free(doc);
    bind_data.done = bind_data.current_idx >= total;
    output.SetCardinality(row);
}

// ============================================================================
// graph_excel_worksheets - List worksheets in Excel workbook
// ============================================================================

unique_ptr<FunctionData> GraphExcelFunctions::ExcelWorksheetsBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<ExcelWorksheetsBindData>();

    // file_path is required positional parameter
    if (input.inputs.empty()) {
        throw BinderException("graph_excel_worksheets requires a file_path parameter");
    }
    bind_data->file_path = input.inputs[0].GetValue<std::string>();

    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        bind_data->secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->drive_id = ResolveGraphDriveId(context, bind_data->secret_name, input);

    // Return schema: name, id, position, visibility
    names = {"name", "id", "position", "visibility"};
    return_types = {
        LogicalType::VARCHAR,   // name
        LogicalType::VARCHAR,   // id
        LogicalType::INTEGER,   // position
        LogicalType::VARCHAR    // visibility
    };

    return std::move(bind_data);
}

void GraphExcelFunctions::ExcelWorksheetsScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<ExcelWorksheetsBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    if (bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphExcelClient client(auth_info.auth_params);
        bind_data.json_response = client.ListWorksheetsByPath(bind_data.file_path, bind_data.drive_id);
    }

    yyjson_doc *doc = yyjson_read(bind_data.json_response.c_str(), bind_data.json_response.length(), 0);
    if (!doc) {
        throw InvalidInputException("Failed to parse Graph API response");
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    if (!value_arr || !yyjson_is_arr(value_arr)) {
        yyjson_doc_free(doc);
        bind_data.done = true;
        output.SetCardinality(0);
        return;
    }

    const size_t total = yyjson_arr_size(value_arr);
    idx_t row = 0;

    while (bind_data.current_idx < total && row < STANDARD_VECTOR_SIZE) {
        yyjson_val *item = yyjson_arr_get(value_arr, bind_data.current_idx);
        // name
        yyjson_val *name_val = yyjson_obj_get(item, "name");
        output.SetValue(0, row, name_val && yyjson_is_str(name_val) ?
            Value(yyjson_get_str(name_val)) : Value());

        // id
        yyjson_val *id_val = yyjson_obj_get(item, "id");
        output.SetValue(1, row, id_val && yyjson_is_str(id_val) ?
            Value(yyjson_get_str(id_val)) : Value());

        // position
        yyjson_val *pos_val = yyjson_obj_get(item, "position");
        output.SetValue(2, row, pos_val && yyjson_is_num(pos_val) ?
            Value::INTEGER(yyjson_get_int(pos_val)) : Value());

        // visibility
        yyjson_val *vis_val = yyjson_obj_get(item, "visibility");
        output.SetValue(3, row, vis_val && yyjson_is_str(vis_val) ?
            Value(yyjson_get_str(vis_val)) : Value());

        row++;
        bind_data.current_idx++;
    }

    yyjson_doc_free(doc);
    bind_data.done = bind_data.current_idx >= total;
    output.SetCardinality(row);
}

// ============================================================================
// graph_excel_range - Read Excel range data
// ============================================================================

unique_ptr<FunctionData> GraphExcelFunctions::ExcelRangeBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<ExcelRangeBindData>();

    // file_path and sheet_name are required positional parameters
    if (input.inputs.size() < 2) {
        throw BinderException("graph_excel_range requires file_path and sheet_name parameters");
    }
    bind_data->file_path = input.inputs[0].GetValue<std::string>();
    bind_data->sheet_name = input.inputs[1].GetValue<std::string>();

    // Optional range parameter
    if (input.inputs.size() > 2 && !input.inputs[2].IsNull()) {
        bind_data->range = input.inputs[2].GetValue<std::string>();
    }

    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        bind_data->secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->drive_id = ResolveGraphDriveId(context, bind_data->secret_name, input);

    // Fetch the range data to determine column count
    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    GraphExcelClient client(auth_info.auth_params);

    if (bind_data->range.empty()) {
        bind_data->json_response = client.GetUsedRangeByPath(bind_data->file_path, bind_data->sheet_name, bind_data->drive_id);
    } else {
        bind_data->json_response = client.GetRangeByPath(bind_data->file_path, bind_data->sheet_name, bind_data->range, bind_data->drive_id);
    }

    // Parse to determine schema
    yyjson_doc *doc = yyjson_read(bind_data->json_response.c_str(), bind_data->json_response.length(), 0);
    if (!doc) {
        throw InvalidInputException("Failed to parse Graph API response");
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *values_arr = yyjson_obj_get(root, "values");

    if (!values_arr || !yyjson_is_arr(values_arr)) {
        yyjson_doc_free(doc);
        // Return minimal schema if no data
        names = {"value"};
        return_types = {LogicalType::VARCHAR};
        bind_data->column_types = {LogicalType::VARCHAR};
        return std::move(bind_data);
    }

    // Get first row to determine column count
    yyjson_val *first_row = yyjson_arr_get_first(values_arr);
    size_t col_count = 0;

    if (first_row && yyjson_is_arr(first_row)) {
        col_count = yyjson_arr_size(first_row);
    }

    if (col_count == 0) {
        yyjson_doc_free(doc);
        names = {"value"};
        return_types = {LogicalType::VARCHAR};
        bind_data->column_types = {LogicalType::VARCHAR};
        return std::move(bind_data);
    }

    // Use first row as headers
    auto headers = GraphJsonStringArray(first_row);

    // Infer column types from valueTypes (parallel 2D array from Graph API)
    // Each cell is one of "String", "Double", "Boolean", "Empty"
    yyjson_val *types_arr = yyjson_obj_get(root, "valueTypes");
    const size_t total_rows = yyjson_arr_size(values_arr);

    // For each column: track whether we've seen only Double, only Boolean, or mixed/String
    enum class ColKind { Unknown, Double, Boolean, String };
    std::vector<ColKind> col_kinds(col_count, ColKind::Unknown);

    if (types_arr && yyjson_is_arr(types_arr)) {
        // Skip row 0 (header row), scan data rows
        for (size_t r = 1; r < total_rows; r++) {
            yyjson_val *type_row = yyjson_arr_get(types_arr, r);
            if (!type_row || !yyjson_is_arr(type_row)) {
                continue;
            }
            for (size_t c = 0; c < col_count; c++) {
                if (col_kinds[c] == ColKind::String) {
                    continue;  // Already degraded to VARCHAR — no point checking further
                }
                yyjson_val *cell_type = yyjson_arr_get(type_row, c);
                if (!cell_type || !yyjson_is_str(cell_type)) {
                    continue;
                }
                const std::string t = yyjson_get_str(cell_type);
                if (t == "Empty") {
                    // Nulls are compatible with any type — skip
                } else if (t == "Double") {
                    if (col_kinds[c] == ColKind::Unknown) {
                        col_kinds[c] = ColKind::Double;
                    } else if (col_kinds[c] != ColKind::Double) {
                        col_kinds[c] = ColKind::String;
                    }
                } else if (t == "Boolean") {
                    if (col_kinds[c] == ColKind::Unknown) {
                        col_kinds[c] = ColKind::Boolean;
                    } else if (col_kinds[c] != ColKind::Boolean) {
                        col_kinds[c] = ColKind::String;
                    }
                } else {
                    col_kinds[c] = ColKind::String;
                }
            }
        }
    }

    // Detect date columns via numberFormat: a column with a date format code (contains
    // year token 'y'/'Y') should be returned as DATE regardless of valueTypes kind.
    // This covers both: real Excel serial dates (valueTypes="Double") and ISO date strings
    // typed into date-formatted cells (valueTypes="String").
    yyjson_val *nfmt_arr = yyjson_obj_get(root, "numberFormat");
    std::vector<bool> col_is_date(col_count, false);

    if (nfmt_arr && yyjson_is_arr(nfmt_arr)) {
        for (size_t r = 1; r < total_rows; r++) {
            yyjson_val *fmt_row = yyjson_arr_get(nfmt_arr, r);
            if (!fmt_row || !yyjson_is_arr(fmt_row)) {
                continue;
            }
            for (size_t c = 0; c < col_count; c++) {
                if (col_is_date[c]) {
                    continue;
                }
                yyjson_val *fmt_val = yyjson_arr_get(fmt_row, c);
                if (!fmt_val || !yyjson_is_str(fmt_val)) {
                    continue;
                }
                if (IsDateFormatCode(yyjson_get_str(fmt_val))) {
                    col_is_date[c] = true;
                }
            }
        }
    }

    for (size_t i = 0; i < col_count; i++) {
        if (i < headers.size() && !headers[i].empty()) {
            names.push_back(headers[i]);
        } else {
            names.push_back("column_" + std::to_string(i));
        }

        LogicalType col_type = LogicalType::VARCHAR;
        if (col_is_date[i]) {
            col_type = LogicalType::DATE;
        } else if (col_kinds[i] == ColKind::Double) {
            col_type = LogicalType::DOUBLE;
        } else if (col_kinds[i] == ColKind::Boolean) {
            col_type = LogicalType::BOOLEAN;
        }
        return_types.push_back(col_type);
        bind_data->column_types.push_back(col_type);
    }

    yyjson_doc_free(doc);
    return std::move(bind_data);
}

void GraphExcelFunctions::ExcelRangeScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<ExcelRangeBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    // Parse the JSON response exactly once and cache all rows as DuckDB Values.
    // This avoids reparsing the same (potentially large) JSON string on every scan call.
    if (!bind_data.rows_cached) {
        yyjson_doc *doc = yyjson_read(bind_data.json_response.c_str(), bind_data.json_response.length(), 0);
        if (!doc) {
            throw InvalidInputException("Failed to parse Graph API response");
        }

        yyjson_val *root       = yyjson_doc_get_root(doc);
        yyjson_val *values_arr = yyjson_obj_get(root, "values");

        if (!values_arr || !yyjson_is_arr(values_arr)) {
            yyjson_doc_free(doc);
            bind_data.done = true;
            output.SetCardinality(0);
            return;
        }

        yyjson_val *types_arr  = yyjson_obj_get(root, "valueTypes");
        const size_t total_rows = yyjson_arr_size(values_arr);
        const size_t col_count  = bind_data.column_types.size();

        // Skip header row (index 0), extract data rows into cache
        bind_data.cached_rows.reserve(total_rows > 1 ? total_rows - 1 : 0);
        for (size_t r = 1; r < total_rows; r++) {
            yyjson_val *row_arr  = yyjson_arr_get(values_arr, r);
            yyjson_val *type_row = (types_arr && yyjson_is_arr(types_arr))
                ? yyjson_arr_get(types_arr, r)
                : nullptr;

            std::vector<duckdb::Value> row_vals;
            row_vals.reserve(col_count);
            for (size_t c = 0; c < col_count; c++) {
                yyjson_val *cell_val  = (row_arr  && yyjson_is_arr(row_arr))  ? yyjson_arr_get(row_arr,  c) : nullptr;
                yyjson_val *cell_type = (type_row && yyjson_is_arr(type_row)) ? yyjson_arr_get(type_row, c) : nullptr;
                row_vals.push_back(ExtractCellValue(cell_val, cell_type, bind_data.column_types[c]));
            }
            bind_data.cached_rows.push_back(std::move(row_vals));
        }

        yyjson_doc_free(doc);
        // Free the raw JSON — no longer needed
        bind_data.json_response.clear();
        bind_data.json_response.shrink_to_fit();
        bind_data.rows_cached = true;
    }

    // Serve rows from cache in STANDARD_VECTOR_SIZE chunks
    const size_t start     = bind_data.current_row;
    const size_t remaining = (start < bind_data.cached_rows.size())
        ? bind_data.cached_rows.size() - start : 0;
    const size_t data_rows = std::min(remaining, static_cast<size_t>(STANDARD_VECTOR_SIZE));
    const size_t col_count = output.ColumnCount();

    output.SetCardinality(data_rows);
    for (size_t row_idx = 0; row_idx < data_rows; row_idx++) {
        const auto &row = bind_data.cached_rows[start + row_idx];
        for (size_t col = 0; col < col_count; col++) {
            output.SetValue(col, row_idx, col < row.size() ? row[col] : Value());
        }
    }

    bind_data.current_row += data_rows;
    if (bind_data.current_row >= bind_data.cached_rows.size()) {
        bind_data.done = true;
    }
}

// ============================================================================
// graph_excel_table_data - Read Excel table data
// ============================================================================

unique_ptr<FunctionData> GraphExcelFunctions::ExcelTableDataBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<ExcelTableDataBindData>();

    // file_path and table_name are required positional parameters
    if (input.inputs.size() < 2) {
        throw BinderException("graph_excel_table_data requires file_path and table_name parameters");
    }
    bind_data->file_path = input.inputs[0].GetValue<std::string>();
    bind_data->table_name = input.inputs[1].GetValue<std::string>();

    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        bind_data->secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->drive_id = ResolveGraphDriveId(context, bind_data->secret_name, input);

    // Fetch the table data to determine schema
    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    GraphExcelClient client(auth_info.auth_params);
    bind_data->json_response = client.GetTableRowsByPath(bind_data->file_path, bind_data->table_name, bind_data->drive_id);

    // Parse to determine schema from first row
    yyjson_doc *doc = yyjson_read(bind_data->json_response.c_str(), bind_data->json_response.length(), 0);
    if (!doc) {
        throw InvalidInputException("Failed to parse Graph API response");
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    if (!value_arr || !yyjson_is_arr(value_arr) || yyjson_arr_size(value_arr) == 0) {
        yyjson_doc_free(doc);
        // Return minimal schema if no data
        names = {"value"};
        return_types = {LogicalType::VARCHAR};
        return std::move(bind_data);
    }

    // Get first row to determine column count
    yyjson_val *first_item = yyjson_arr_get_first(value_arr);
    yyjson_val *first_row_values = first_item ? yyjson_obj_get(first_item, "values") : nullptr;

    size_t col_count = 0;
    if (first_row_values && yyjson_is_arr(first_row_values)) {
        yyjson_val *inner_arr = yyjson_arr_get_first(first_row_values);
        if (inner_arr && yyjson_is_arr(inner_arr)) {
            col_count = yyjson_arr_size(inner_arr);
        }
    }

    if (col_count == 0) {
        yyjson_doc_free(doc);
        names = {"value"};
        return_types = {LogicalType::VARCHAR};
        return std::move(bind_data);
    }

    // Generate column names (table rows don't include headers, so use generic names)
    for (size_t i = 0; i < col_count; i++) {
        names.push_back("column_" + std::to_string(i));
        return_types.push_back(LogicalType::VARCHAR);
    }

    yyjson_doc_free(doc);
    return std::move(bind_data);
}

void GraphExcelFunctions::ExcelTableDataScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<ExcelTableDataBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    yyjson_doc *doc = yyjson_read(bind_data.json_response.c_str(), bind_data.json_response.length(), 0);
    if (!doc) {
        throw InvalidInputException("Failed to parse Graph API response");
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    if (!value_arr || !yyjson_is_arr(value_arr)) {
        yyjson_doc_free(doc);
        bind_data.done = true;
        output.SetCardinality(0);
        return;
    }

    const size_t total = yyjson_arr_size(value_arr);
    size_t col_count = output.ColumnCount();

    idx_t row_idx = 0;

    while (bind_data.current_idx < total && row_idx < STANDARD_VECTOR_SIZE) {
        yyjson_val *item = yyjson_arr_get(value_arr, bind_data.current_idx);
        bind_data.current_idx++;

        yyjson_val *values_arr = yyjson_obj_get(item, "values");
        if (!values_arr || !yyjson_is_arr(values_arr)) {
            for (size_t col = 0; col < col_count; col++) {
                output.SetValue(col, row_idx, Value());
            }
            row_idx++;
            continue;
        }

        // Table rows have nested array structure: values: [[cell1, cell2, ...]]
        yyjson_val *inner_arr = yyjson_arr_get_first(values_arr);
        if (!inner_arr || !yyjson_is_arr(inner_arr)) {
            for (size_t col = 0; col < col_count; col++) {
                output.SetValue(col, row_idx, Value());
            }
            row_idx++;
            continue;
        }

        auto row_values = GraphJsonStringArray(inner_arr);

        for (size_t col = 0; col < col_count; col++) {
            if (col < row_values.size()) {
                output.SetValue(col, row_idx, Value(row_values[col]));
            } else {
                output.SetValue(col, row_idx, Value());
            }
        }

        row_idx++;
    }

    yyjson_doc_free(doc);
    bind_data.done = bind_data.current_idx >= total;
    output.SetCardinality(row_idx);
}

// ============================================================================
// Registration
// ============================================================================

void GraphExcelFunctions::Register(ExtensionLoader &loader) {
    ERPL_TRACE_INFO("GRAPH_EXCEL", "Registering Microsoft Graph Excel functions");

    {
        TableFunction list_files("graph_list_files", {}, ListFilesScan, ListFilesBind);
        list_files.varargs = LogicalType::VARCHAR;
        list_files.named_parameters["secret"] = LogicalType::VARCHAR;
        list_files.named_parameters["drive"] = LogicalType::VARCHAR;
        list_files.named_parameters["site"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(list_files);
        FunctionDescription desc;
        desc.description = "List files in OneDrive/SharePoint. Pass a drive ID (b!...), web URL, or name + site.";
        desc.parameter_names = {};
        desc.parameter_types = {};
        desc.examples = {
            "SELECT * FROM graph_list_files(drive := 'b!abc...', secret := 'ms_graph')",
            "SELECT * FROM graph_list_files(drive := 'https://tenant.sharepoint.com/Shared%20Documents', secret := 'ms_graph')",
            "SELECT * FROM graph_list_files(site := 'Finance', drive := 'Documents', secret := 'ms_graph')"
        };
        desc.categories = {"microsoft", "graph", "excel"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction excel_tables("graph_excel_tables", {LogicalType::VARCHAR},
                                   ExcelTablesScan, ExcelTablesBind);
        excel_tables.named_parameters["secret"] = LogicalType::VARCHAR;
        excel_tables.named_parameters["drive"] = LogicalType::VARCHAR;
        excel_tables.named_parameters["site"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(excel_tables);
        FunctionDescription desc;
        desc.description = "List all named tables in a Microsoft Excel workbook.";
        desc.parameter_names = {"file_path", "secret", "drive", "site"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR,
                                 LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {
            "SELECT * FROM graph_excel_tables('test.xlsx', drive := 'b!abc...', secret := 'ms_graph')",
            "SELECT * FROM graph_excel_tables('test.xlsx', drive := 'https://tenant.sharepoint.com/Shared%20Documents', secret := 'ms_graph')",
            "SELECT * FROM graph_excel_tables('test.xlsx', site := 'Finance', drive := 'Documents', secret := 'ms_graph')"
        };
        desc.categories = {"microsoft", "graph", "excel"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction excel_worksheets("graph_excel_worksheets", {LogicalType::VARCHAR},
                                       ExcelWorksheetsScan, ExcelWorksheetsBind);
        excel_worksheets.named_parameters["secret"] = LogicalType::VARCHAR;
        excel_worksheets.named_parameters["drive"] = LogicalType::VARCHAR;
        excel_worksheets.named_parameters["site"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(excel_worksheets);
        FunctionDescription desc;
        desc.description = "List all worksheets in a Microsoft Excel workbook.";
        desc.parameter_names = {"file_path", "secret", "drive", "site"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR,
                                 LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {
            "SELECT * FROM graph_excel_worksheets('test.xlsx', drive := 'b!abc...', secret := 'ms_graph')",
            "SELECT * FROM graph_excel_worksheets('test.xlsx', drive := 'https://tenant.sharepoint.com/Shared%20Documents', secret := 'ms_graph')",
            "SELECT * FROM graph_excel_worksheets('test.xlsx', site := 'Finance', drive := 'Documents', secret := 'ms_graph')"
        };
        desc.categories = {"microsoft", "graph", "excel"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction excel_range("graph_excel_range",
                                  {LogicalType::VARCHAR, LogicalType::VARCHAR},
                                  ExcelRangeScan, ExcelRangeBind);
        excel_range.varargs = LogicalType::VARCHAR;
        excel_range.named_parameters["secret"] = LogicalType::VARCHAR;
        excel_range.named_parameters["drive"] = LogicalType::VARCHAR;
        excel_range.named_parameters["site"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(excel_range);
        FunctionDescription desc;
        desc.description = "Read a cell range from a worksheet in a Microsoft Excel workbook.";
        desc.parameter_names = {"file_path", "sheet_name", "secret", "drive", "site"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                 LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {
            "SELECT * FROM graph_excel_range('test.xlsx', 'Sheet1', drive := 'b!abc...', secret := 'ms_graph')",
            "SELECT * FROM graph_excel_range('test.xlsx', 'Sheet1', drive := 'https://tenant.sharepoint.com/Shared%20Documents', secret := 'ms_graph')",
            "SELECT * FROM graph_excel_range('test.xlsx', 'Sheet1', site := 'Finance', drive := 'Documents', secret := 'ms_graph')"
        };
        desc.categories = {"microsoft", "graph", "excel"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction excel_table_data("graph_excel_table_data",
                                       {LogicalType::VARCHAR, LogicalType::VARCHAR},
                                       ExcelTableDataScan, ExcelTableDataBind);
        excel_table_data.named_parameters["secret"] = LogicalType::VARCHAR;
        excel_table_data.named_parameters["drive"] = LogicalType::VARCHAR;
        excel_table_data.named_parameters["site"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(excel_table_data);
        FunctionDescription desc;
        desc.description = "Read all rows from a named table in a Microsoft Excel workbook.";
        desc.parameter_names = {"file_path", "table_name", "secret", "drive", "site"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                 LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {
            "SELECT * FROM graph_excel_table_data('test.xlsx', 'SalesTable', drive := 'b!abc...', secret := 'ms_graph')",
            "SELECT * FROM graph_excel_table_data('test.xlsx', 'SalesTable', drive := 'https://tenant.sharepoint.com/Shared%20Documents', secret := 'ms_graph')",
            "SELECT * FROM graph_excel_table_data('test.xlsx', 'SalesTable', site := 'Finance', drive := 'Documents', secret := 'ms_graph')"
        };
        desc.categories = {"microsoft", "graph", "excel"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }

    ERPL_TRACE_INFO("GRAPH_EXCEL", "Successfully registered Microsoft Graph Excel functions");
}

} // namespace erpl_web
