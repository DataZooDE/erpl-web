#include "graph_excel_functions.hpp"
#include "graph_excel_client.hpp"
#include "graph_excel_secret.hpp"
#include "tracing.hpp"
#include "duckdb/common/exception.hpp"
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
    std::string json_response;
    bool done = false;
};

struct ExcelTablesBindData : public TableFunctionData {
    std::string secret_name;
    std::string file_path;
    std::string json_response;
    bool done = false;
};

struct ExcelWorksheetsBindData : public TableFunctionData {
    std::string secret_name;
    std::string file_path;
    std::string json_response;
    bool done = false;
};

struct ExcelRangeBindData : public TableFunctionData {
    std::string secret_name;
    std::string file_path;
    std::string sheet_name;
    std::string range;  // Optional, empty means usedRange
    std::string json_response;
    bool done = false;
};

struct ExcelTableDataBindData : public TableFunctionData {
    std::string secret_name;
    std::string file_path;
    std::string table_name;
    std::string json_response;
    bool done = false;
};

// ============================================================================
// Helper Functions
// ============================================================================

static std::vector<std::string> ParseJsonStringArray(yyjson_val *arr) {
    std::vector<std::string> result;
    if (!arr || !yyjson_is_arr(arr)) {
        return result;
    }

    size_t idx, max;
    yyjson_val *val;
    yyjson_arr_foreach(arr, idx, max, val) {
        if (yyjson_is_str(val)) {
            result.push_back(yyjson_get_str(val));
        } else if (yyjson_is_null(val)) {
            result.push_back("");
        } else if (yyjson_is_num(val)) {
            result.push_back(std::to_string(yyjson_get_num(val)));
        } else if (yyjson_is_bool(val)) {
            result.push_back(yyjson_get_bool(val) ? "true" : "false");
        } else {
            result.push_back("");
        }
    }
    return result;
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

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

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
        bind_data.json_response = client.ListDriveFiles(bind_data.folder_path);
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

    size_t count = yyjson_arr_size(value_arr);
    if (count > STANDARD_VECTOR_SIZE) {
        count = STANDARD_VECTOR_SIZE;
    }

    output.SetCardinality(count);

    size_t idx, max;
    yyjson_val *item;
    idx_t row = 0;

    yyjson_arr_foreach(value_arr, idx, max, item) {
        if (row >= count) break;

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
    }

    yyjson_doc_free(doc);
    bind_data.done = true;
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

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

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
        bind_data.json_response = client.ListTablesByPath(bind_data.file_path);
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

    size_t count = yyjson_arr_size(value_arr);
    output.SetCardinality(count);

    size_t idx, max;
    yyjson_val *item;
    idx_t row = 0;

    yyjson_arr_foreach(value_arr, idx, max, item) {
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
    }

    yyjson_doc_free(doc);
    bind_data.done = true;
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

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

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
        bind_data.json_response = client.ListWorksheetsByPath(bind_data.file_path);
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

    size_t count = yyjson_arr_size(value_arr);
    output.SetCardinality(count);

    size_t idx, max;
    yyjson_val *item;
    idx_t row = 0;

    yyjson_arr_foreach(value_arr, idx, max, item) {
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
    }

    yyjson_doc_free(doc);
    bind_data.done = true;
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

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    // Fetch the range data to determine column count
    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    GraphExcelClient client(auth_info.auth_params);

    if (bind_data->range.empty()) {
        bind_data->json_response = client.GetUsedRangeByPath(bind_data->file_path, bind_data->sheet_name);
    } else {
        bind_data->json_response = client.GetRangeByPath(bind_data->file_path, bind_data->sheet_name, bind_data->range);
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
        return std::move(bind_data);
    }

    // Use first row as headers
    auto headers = ParseJsonStringArray(first_row);

    for (size_t i = 0; i < col_count; i++) {
        if (i < headers.size() && !headers[i].empty()) {
            names.push_back(headers[i]);
        } else {
            names.push_back("column_" + std::to_string(i));
        }
        return_types.push_back(LogicalType::VARCHAR);
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

    yyjson_doc *doc = yyjson_read(bind_data.json_response.c_str(), bind_data.json_response.length(), 0);
    if (!doc) {
        throw InvalidInputException("Failed to parse Graph API response");
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *values_arr = yyjson_obj_get(root, "values");

    if (!values_arr || !yyjson_is_arr(values_arr)) {
        yyjson_doc_free(doc);
        bind_data.done = true;
        output.SetCardinality(0);
        return;
    }

    size_t total_rows = yyjson_arr_size(values_arr);
    // Skip header row
    size_t data_rows = total_rows > 1 ? total_rows - 1 : 0;

    if (data_rows > STANDARD_VECTOR_SIZE) {
        data_rows = STANDARD_VECTOR_SIZE;
    }

    output.SetCardinality(data_rows);

    size_t col_count = output.ColumnCount();

    for (size_t row_idx = 0; row_idx < data_rows; row_idx++) {
        // Skip first row (headers), get row at row_idx + 1
        yyjson_val *row_arr = yyjson_arr_get(values_arr, row_idx + 1);

        if (!row_arr || !yyjson_is_arr(row_arr)) {
            for (size_t col = 0; col < col_count; col++) {
                output.SetValue(col, row_idx, Value());
            }
            continue;
        }

        auto row_values = ParseJsonStringArray(row_arr);

        for (size_t col = 0; col < col_count; col++) {
            if (col < row_values.size()) {
                output.SetValue(col, row_idx, Value(row_values[col]));
            } else {
                output.SetValue(col, row_idx, Value());
            }
        }
    }

    yyjson_doc_free(doc);
    bind_data.done = true;
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

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    // Fetch the table data to determine schema
    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    GraphExcelClient client(auth_info.auth_params);
    bind_data->json_response = client.GetTableRowsByPath(bind_data->file_path, bind_data->table_name);

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

    size_t row_count = yyjson_arr_size(value_arr);
    if (row_count > STANDARD_VECTOR_SIZE) {
        row_count = STANDARD_VECTOR_SIZE;
    }

    output.SetCardinality(row_count);
    size_t col_count = output.ColumnCount();

    size_t idx, max;
    yyjson_val *item;
    idx_t row_idx = 0;

    yyjson_arr_foreach(value_arr, idx, max, item) {
        if (row_idx >= row_count) break;

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

        auto row_values = ParseJsonStringArray(inner_arr);

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
    bind_data.done = true;
}

// ============================================================================
// Registration
// ============================================================================

void GraphExcelFunctions::Register(ExtensionLoader &loader) {
    ERPL_TRACE_INFO("GRAPH_EXCEL", "Registering Microsoft Graph Excel functions");

    // graph_list_files(folder_path?) - optional secret named param
    TableFunction list_files("graph_list_files", {}, ListFilesScan, ListFilesBind);
    list_files.varargs = LogicalType::VARCHAR;
    list_files.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(list_files);

    // graph_excel_tables(file_path) - optional secret named param
    TableFunction excel_tables("graph_excel_tables", {LogicalType::VARCHAR},
                               ExcelTablesScan, ExcelTablesBind);
    excel_tables.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(excel_tables);

    // graph_excel_worksheets(file_path) - optional secret named param
    TableFunction excel_worksheets("graph_excel_worksheets", {LogicalType::VARCHAR},
                                   ExcelWorksheetsScan, ExcelWorksheetsBind);
    excel_worksheets.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(excel_worksheets);

    // graph_excel_range(file_path, sheet_name, range?) - optional secret named param
    TableFunction excel_range("graph_excel_range",
                              {LogicalType::VARCHAR, LogicalType::VARCHAR},
                              ExcelRangeScan, ExcelRangeBind);
    excel_range.varargs = LogicalType::VARCHAR;
    excel_range.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(excel_range);

    // graph_excel_table_data(file_path, table_name) - optional secret named param
    TableFunction excel_table_data("graph_excel_table_data",
                                   {LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   ExcelTableDataScan, ExcelTableDataBind);
    excel_table_data.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(excel_table_data);

    ERPL_TRACE_INFO("GRAPH_EXCEL", "Successfully registered Microsoft Graph Excel functions");
}

} // namespace erpl_web
