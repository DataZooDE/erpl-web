#include "graph_sharepoint_functions.hpp"
#include "graph_client.hpp"
#include "graph_sharepoint_client.hpp"
#include "graph_sharepoint_type_mapper.hpp"
#include "graph_excel_secret.hpp"
#include "graph_output_utils.hpp"
#include "tracing.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "yyjson.hpp"
#include <algorithm>
#include <unordered_set>

using namespace duckdb_yyjson;

namespace erpl_web {

using namespace duckdb;

// ============================================================================
// Bind Data Structures
// ============================================================================

struct ShowSitesBindData : public TableFunctionData {
    std::string secret_name;
    std::string search_query;
    std::string json_response;
    yyjson_doc *parsed_doc = nullptr;
    yyjson_arr_iter item_iter = {};
    bool done = false;

    ~ShowSitesBindData() override {
        if (parsed_doc) { yyjson_doc_free(parsed_doc); }
    }

    bool InitIterator() {
        parsed_doc = yyjson_read(json_response.c_str(), json_response.length(), 0);
        json_response.clear();
        json_response.shrink_to_fit();
        if (!parsed_doc) { return false; }
        yyjson_val *root = yyjson_doc_get_root(parsed_doc);
        yyjson_val *arr = yyjson_obj_get(root, "value");
        if (!arr || !yyjson_is_arr(arr)) { return false; }
        yyjson_arr_iter_init(arr, &item_iter);
        return true;
    }
};

struct ShowListsBindData : public TableFunctionData {
    std::string secret_name;
    std::string site_id;
    std::string json_response;
    yyjson_doc *parsed_doc = nullptr;
    yyjson_arr_iter item_iter = {};
    bool done = false;

    ~ShowListsBindData() override {
        if (parsed_doc) { yyjson_doc_free(parsed_doc); }
    }

    bool InitIterator() {
        parsed_doc = yyjson_read(json_response.c_str(), json_response.length(), 0);
        json_response.clear();
        json_response.shrink_to_fit();
        if (!parsed_doc) { return false; }
        yyjson_val *root = yyjson_doc_get_root(parsed_doc);
        yyjson_val *arr = yyjson_obj_get(root, "value");
        if (!arr || !yyjson_is_arr(arr)) { return false; }
        yyjson_arr_iter_init(arr, &item_iter);
        return true;
    }
};

struct ShowDrivesBindData : public TableFunctionData {
    std::string secret_name;
    std::string site_id;
    std::string json_response;
    yyjson_doc *parsed_doc = nullptr;
    yyjson_arr_iter item_iter = {};
    bool done = false;

    ~ShowDrivesBindData() override {
        if (parsed_doc) { yyjson_doc_free(parsed_doc); }
    }

    bool InitIterator() {
        parsed_doc = yyjson_read(json_response.c_str(), json_response.length(), 0);
        json_response.clear();
        json_response.shrink_to_fit();
        if (!parsed_doc) { return false; }
        yyjson_val *root = yyjson_doc_get_root(parsed_doc);
        yyjson_val *arr = yyjson_obj_get(root, "value");
        if (!arr || !yyjson_is_arr(arr)) { return false; }
        yyjson_arr_iter_init(arr, &item_iter);
        return true;
    }
};

struct DescribeListBindData : public TableFunctionData {
    std::string secret_name;
    std::string site_id;
    std::string list_id;
    std::string json_response;
    yyjson_doc *parsed_doc = nullptr;
    yyjson_arr_iter item_iter = {};
    bool done = false;

    ~DescribeListBindData() override {
        if (parsed_doc) { yyjson_doc_free(parsed_doc); }
    }

    bool InitIterator() {
        parsed_doc = yyjson_read(json_response.c_str(), json_response.length(), 0);
        json_response.clear();
        json_response.shrink_to_fit();
        if (!parsed_doc) { return false; }
        yyjson_val *root = yyjson_doc_get_root(parsed_doc);
        yyjson_val *arr = yyjson_obj_get(root, "value");
        if (!arr || !yyjson_is_arr(arr)) { return false; }
        yyjson_arr_iter_init(arr, &item_iter);
        return true;
    }
};

struct ListItemsBindData : public TableFunctionData {
    std::string secret_name;
    std::string site_id;
    std::string list_id;
    std::string json_response;
    std::vector<std::string> column_names;
    std::vector<LogicalType> column_types;
    yyjson_doc *parsed_doc = nullptr;
    yyjson_arr_iter item_iter = {};
    bool done = false;

    ~ListItemsBindData() override {
        if (parsed_doc) { yyjson_doc_free(parsed_doc); }
    }

    bool InitIterator() {
        parsed_doc = yyjson_read(json_response.c_str(), json_response.length(), 0);
        json_response.clear();
        json_response.shrink_to_fit();
        if (!parsed_doc) { return false; }
        yyjson_val *root = yyjson_doc_get_root(parsed_doc);
        yyjson_val *arr = yyjson_obj_get(root, "value");
        if (!arr || !yyjson_is_arr(arr)) { return false; }
        yyjson_arr_iter_init(arr, &item_iter);
        return true;
    }
};

// ============================================================================
// graph_show_sites - Search/list SharePoint sites
// ============================================================================

unique_ptr<FunctionData> GraphSharePointFunctions::ShowSitesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<ShowSitesBindData>();

    // Optional search query is first positional param
    if (!input.inputs.empty() && !input.inputs[0].IsNull()) {
        bind_data->search_query = input.inputs[0].GetValue<std::string>();
    }

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    // Return schema: id, name, displayName, webUrl, createdDateTime
    names = {"id", "name", "display_name", "web_url", "created_at"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // name
        LogicalType::VARCHAR,   // display_name
        LogicalType::VARCHAR,   // web_url
        LogicalType::VARCHAR    // created_at
    };

    return std::move(bind_data);
}

void GraphSharePointFunctions::ShowSitesScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<ShowSitesBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    if (!bind_data.parsed_doc && bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphSharePointClient client(auth_info.auth_params);
        bind_data.json_response = client.SearchSites(bind_data.search_query);
    }

    if (!bind_data.parsed_doc) {
        if (!bind_data.InitIterator()) {
            bind_data.done = true;
            output.SetCardinality(0);
            return;
        }
    }

    idx_t row = 0;
    yyjson_val *item;
    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bind_data.item_iter))) {
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "id"));
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "name"));
        SetStrCell(output.data[2], row, yyjson_obj_get(item, "displayName"));
        SetStrCell(output.data[3], row, yyjson_obj_get(item, "webUrl"));
        SetStrCell(output.data[4], row, yyjson_obj_get(item, "createdDateTime"));
        row++;
    }

    if (row < STANDARD_VECTOR_SIZE) { bind_data.done = true; }
    output.SetCardinality(row);
}

// ============================================================================
// graph_show_lists - List SharePoint lists in a site
// ============================================================================

unique_ptr<FunctionData> GraphSharePointFunctions::ShowListsBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<ShowListsBindData>();

    // Accept either a positional site_id or named site := 'name'
    if (input.named_parameters.find("site") != input.named_parameters.end()) {
        bind_data->site_id = input.named_parameters.at("site").GetValue<std::string>();
    } else if (!input.inputs.empty() && !input.inputs[0].IsNull()) {
        bind_data->site_id = input.inputs[0].GetValue<std::string>();
    } else {
        throw BinderException("graph_show_lists requires either a positional site_id or site := 'name'");
    }

    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        bind_data->secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }

    // Return schema: id, name, displayName, webUrl, createdDateTime, lastModifiedDateTime
    names = {"id", "name", "display_name", "description", "web_url", "created_at", "modified_at"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // name
        LogicalType::VARCHAR,   // display_name
        LogicalType::VARCHAR,   // description
        LogicalType::VARCHAR,   // web_url
        LogicalType::VARCHAR,   // created_at
        LogicalType::VARCHAR    // modified_at
    };

    return std::move(bind_data);
}

void GraphSharePointFunctions::ShowListsScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<ShowListsBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    if (!bind_data.parsed_doc && bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphSharePointClient client(auth_info.auth_params);
        bind_data.json_response = client.ListLists(client.ResolveSiteId(bind_data.site_id));
    }

    if (!bind_data.parsed_doc) {
        if (!bind_data.InitIterator()) {
            bind_data.done = true;
            output.SetCardinality(0);
            return;
        }
    }

    idx_t row = 0;
    yyjson_val *item;
    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bind_data.item_iter))) {
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "id"));
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "name"));
        SetStrCell(output.data[2], row, yyjson_obj_get(item, "displayName"));
        SetStrCell(output.data[3], row, yyjson_obj_get(item, "description"));
        SetStrCell(output.data[4], row, yyjson_obj_get(item, "webUrl"));
        SetStrCell(output.data[5], row, yyjson_obj_get(item, "createdDateTime"));
        SetStrCell(output.data[6], row, yyjson_obj_get(item, "lastModifiedDateTime"));
        row++;
    }

    if (row < STANDARD_VECTOR_SIZE) { bind_data.done = true; }
    output.SetCardinality(row);
}

// ============================================================================
// graph_describe_list - Get list columns/schema
// ============================================================================

unique_ptr<FunctionData> GraphSharePointFunctions::DescribeListBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<DescribeListBindData>();

    // site_id and list_id are required positional parameters
    if (input.inputs.size() < 2) {
        throw BinderException("graph_describe_list requires site_id and list_id parameters");
    }
    bind_data->site_id = input.inputs[0].GetValue<std::string>();
    bind_data->list_id = input.inputs[1].GetValue<std::string>();

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    // Return schema: name, displayName, columnType, description, required
    names = {"name", "display_name", "column_type", "description", "required"};
    return_types = {
        LogicalType::VARCHAR,   // name
        LogicalType::VARCHAR,   // display_name
        LogicalType::VARCHAR,   // column_type
        LogicalType::VARCHAR,   // description
        LogicalType::BOOLEAN    // required
    };

    return std::move(bind_data);
}

void GraphSharePointFunctions::DescribeListScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<DescribeListBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    if (!bind_data.parsed_doc && bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphSharePointClient client(auth_info.auth_params);
        bind_data.json_response = client.GetListColumns(bind_data.site_id, bind_data.list_id);
    }

    if (!bind_data.parsed_doc) {
        if (!bind_data.InitIterator()) {
            bind_data.done = true;
            output.SetCardinality(0);
            return;
        }
    }

    idx_t row = 0;
    yyjson_val *item;
    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bind_data.item_iter))) {
        // name
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "name"));

        // displayName
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "displayName"));

        // Column type - check various type indicators
        const char *column_type = "unknown";
        if (yyjson_obj_get(item, "text")) {
            column_type = "text";
        } else if (yyjson_obj_get(item, "number")) {
            column_type = "number";
        } else if (yyjson_obj_get(item, "dateTime")) {
            column_type = "dateTime";
        } else if (yyjson_obj_get(item, "boolean")) {
            column_type = "boolean";
        } else if (yyjson_obj_get(item, "choice")) {
            column_type = "choice";
        } else if (yyjson_obj_get(item, "lookup")) {
            column_type = "lookup";
        } else if (yyjson_obj_get(item, "personOrGroup")) {
            column_type = "personOrGroup";
        } else if (yyjson_obj_get(item, "currency")) {
            column_type = "currency";
        } else if (yyjson_obj_get(item, "calculated")) {
            column_type = "calculated";
        }
        SetStrCellNN(output.data[2], row, column_type);

        // description
        SetStrCell(output.data[3], row, yyjson_obj_get(item, "description"));

        // required
        yyjson_val *req_val = yyjson_obj_get(item, "required");
        if (req_val && yyjson_is_bool(req_val)) {
            SetBoolCellNN(output.data[4], row, yyjson_get_bool(req_val));
        } else {
            SetBoolCellNN(output.data[4], row, false);
        }

        row++;
    }

    if (row < STANDARD_VECTOR_SIZE) { bind_data.done = true; }
    output.SetCardinality(row);
}

// ============================================================================
// graph_list_items - Read list items
// ============================================================================

unique_ptr<FunctionData> GraphSharePointFunctions::ListItemsBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<ListItemsBindData>();

    // site_id and list_id are required positional parameters
    if (input.inputs.size() < 2) {
        throw BinderException("graph_list_items requires site_id and list_id parameters");
    }
    bind_data->site_id = input.inputs[0].GetValue<std::string>();
    bind_data->list_id = input.inputs[1].GetValue<std::string>();

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    // Fetch list columns to determine schema
    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    GraphSharePointClient client(auth_info.auth_params);

    // Get columns
    auto columns_json = client.GetListColumns(bind_data->site_id, bind_data->list_id);

    yyjson_doc *col_doc = yyjson_read(columns_json.c_str(), columns_json.length(), 0);
    if (!col_doc) {
        throw InvalidInputException("Failed to parse list columns response");
    }

    yyjson_val *col_root = yyjson_doc_get_root(col_doc);
    yyjson_val *col_arr = yyjson_obj_get(col_root, "value");

    // Always include id as first column (always VARCHAR — it's the Graph item GUID)
    names.push_back("id");
    return_types.push_back(LogicalType::VARCHAR);
    bind_data->column_names.push_back("id");
    bind_data->column_types.push_back(LogicalType::VARCHAR);

    // Track seen names (lowercase) to prevent duplicate column errors
    std::unordered_set<std::string> seen_names = {"id"};

    if (col_arr && yyjson_is_arr(col_arr)) {
        size_t idx, max;
        yyjson_val *col;

        yyjson_arr_foreach(col_arr, idx, max, col) {
            yyjson_val *name_val = yyjson_obj_get(col, "name");
            if (name_val && yyjson_is_str(name_val)) {
                std::string col_name = yyjson_get_str(name_val);

                // Skip internal columns
                if (col_name.empty() || col_name[0] == '_') continue;
                if (col_name == "Edit" || col_name == "LinkTitle" ||
                    col_name == "LinkTitleNoMenu" || col_name == "DocIcon" ||
                    col_name == "ItemChildCount" || col_name == "FolderChildCount" ||
                    col_name == "AppAuthor" || col_name == "AppEditor") {
                    continue;
                }

                // Skip case-insensitive duplicates (e.g. "ID" collides with "id")
                std::string lower_name = col_name;
                std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
                if (!seen_names.insert(lower_name).second) continue;

                const LogicalType col_type = SharePointColumnToDuckDBType(col);
                names.push_back(col_name);
                return_types.push_back(col_type);
                bind_data->column_names.push_back(col_name);
                bind_data->column_types.push_back(col_type);
            }
        }
    }

    yyjson_doc_free(col_doc);

    // If we only have the id column, add a default "fields" column
    if (names.size() == 1) {
        names.push_back("fields");
        return_types.push_back(LogicalType::VARCHAR);
        bind_data->column_names.push_back("fields");
        bind_data->column_types.push_back(LogicalType::VARCHAR);
    }

    // Fetch items
    bind_data->json_response = client.GetListItems(bind_data->site_id, bind_data->list_id);

    return std::move(bind_data);
}

void GraphSharePointFunctions::ListItemsScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<ListItemsBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    if (!bind_data.parsed_doc) {
        if (!bind_data.InitIterator()) {
            bind_data.done = true;
            output.SetCardinality(0);
            return;
        }
    }

    const size_t col_count = bind_data.column_names.size();
    idx_t row = 0;
    yyjson_val *item;

    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bind_data.item_iter))) {
        yyjson_val *fields_obj = yyjson_obj_get(item, "fields");

        for (size_t col = 0; col < col_count; col++) {
            const std::string &col_name = bind_data.column_names[col];
            auto &vec = output.data[col];

            yyjson_val *field_val = (col_name == "id")
                ? yyjson_obj_get(item, "id")
                : (fields_obj ? yyjson_obj_get(fields_obj, col_name.c_str()) : nullptr);

            WriteSharePointField(vec, row, field_val, bind_data.column_types[col]);
        }
        row++;
    }

    if (row < STANDARD_VECTOR_SIZE) { bind_data.done = true; }
    output.SetCardinality(row);
}

// ============================================================================
// graph_show_drives - List document library drives in a SharePoint site
// ============================================================================

unique_ptr<FunctionData> GraphSharePointFunctions::ShowDrivesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<ShowDrivesBindData>();

    // Accept either a positional site_id or named site := 'name'
    if (input.named_parameters.find("site") != input.named_parameters.end()) {
        bind_data->site_id = input.named_parameters.at("site").GetValue<std::string>();
    } else if (!input.inputs.empty() && !input.inputs[0].IsNull()) {
        bind_data->site_id = input.inputs[0].GetValue<std::string>();
    } else {
        throw BinderException("graph_show_drives requires either a positional site_id or site := 'name'");
    }

    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        bind_data->secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }

    names = {"id", "name", "drive_type", "web_url", "created_at", "modified_at"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // name
        LogicalType::VARCHAR,   // drive_type
        LogicalType::VARCHAR,   // web_url
        LogicalType::VARCHAR,   // created_at
        LogicalType::VARCHAR    // modified_at
    };

    return std::move(bind_data);
}

void GraphSharePointFunctions::ShowDrivesScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<ShowDrivesBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    if (!bind_data.parsed_doc && bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphSharePointClient client(auth_info.auth_params);
        bind_data.json_response = client.ListDrives(client.ResolveSiteId(bind_data.site_id));
    }

    if (!bind_data.parsed_doc) {
        if (!bind_data.InitIterator()) {
            bind_data.done = true;
            output.SetCardinality(0);
            return;
        }
    }

    idx_t row = 0;
    yyjson_val *item;
    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bind_data.item_iter))) {
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "id"));
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "name"));
        SetStrCell(output.data[2], row, yyjson_obj_get(item, "driveType"));
        SetStrCell(output.data[3], row, yyjson_obj_get(item, "webUrl"));
        SetStrCell(output.data[4], row, yyjson_obj_get(item, "createdDateTime"));
        SetStrCell(output.data[5], row, yyjson_obj_get(item, "lastModifiedDateTime"));
        row++;
    }

    if (row < STANDARD_VECTOR_SIZE) { bind_data.done = true; }
    output.SetCardinality(row);
}

// ============================================================================
// Registration
// ============================================================================

void GraphSharePointFunctions::Register(ExtensionLoader &loader) {
    ERPL_TRACE_INFO("GRAPH_SHAREPOINT", "Registering Microsoft Graph SharePoint functions");

    {
        TableFunction show_sites("graph_show_sites", {}, ShowSitesScan, ShowSitesBind);
        show_sites.varargs = LogicalType::VARCHAR;
        show_sites.named_parameters["secret"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(show_sites);
        FunctionDescription desc;
        desc.description = "Search and list SharePoint sites accessible via Microsoft Graph.";
        desc.parameter_names = {};
        desc.parameter_types = {};
        desc.examples = {"SELECT * FROM graph_show_sites(secret := 'ms_graph')"};
        desc.categories = {"microsoft", "graph", "sharepoint"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction show_drives("graph_show_drives", {}, ShowDrivesScan, ShowDrivesBind);
        show_drives.varargs = LogicalType::VARCHAR;
        show_drives.named_parameters["secret"] = LogicalType::VARCHAR;
        show_drives.named_parameters["site"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(show_drives);
        FunctionDescription desc;
        desc.description = "List document library drives in a SharePoint site. Accepts a composite site_id or a friendly site name.";
        desc.parameter_names = {"site_id", "secret", "site"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {
            "SELECT * FROM graph_show_drives('site-id,guid,guid', secret := 'ms_graph')",
            "SELECT * FROM graph_show_drives(site := 'Finance', secret := 'ms_graph')"
        };
        desc.categories = {"microsoft", "graph", "sharepoint"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction show_lists("graph_show_lists", {}, ShowListsScan, ShowListsBind);
        show_lists.varargs = LogicalType::VARCHAR;
        show_lists.named_parameters["secret"] = LogicalType::VARCHAR;
        show_lists.named_parameters["site"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(show_lists);
        FunctionDescription desc;
        desc.description = "List all lists in a SharePoint site. Accepts a composite site_id or a friendly site name.";
        desc.parameter_names = {"site_id", "secret", "site"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {
            "SELECT * FROM graph_show_lists('site-id-here', secret := 'ms_graph')",
            "SELECT * FROM graph_show_lists(site := 'Finance', secret := 'ms_graph')"
        };
        desc.categories = {"microsoft", "graph", "sharepoint"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction describe_list("graph_describe_list",
                                    {LogicalType::VARCHAR, LogicalType::VARCHAR},
                                    DescribeListScan, DescribeListBind);
        describe_list.named_parameters["secret"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(describe_list);
        FunctionDescription desc;
        desc.description = "Describe the column schema of a SharePoint list.";
        desc.parameter_names = {"site_id", "list_id", "secret"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM graph_describe_list('site-id', 'list-id', secret := 'ms_graph')"};
        desc.categories = {"microsoft", "graph", "sharepoint"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction list_items("graph_list_items",
                                 {LogicalType::VARCHAR, LogicalType::VARCHAR},
                                 ListItemsScan, ListItemsBind);
        list_items.named_parameters["secret"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(list_items);
        FunctionDescription desc;
        desc.description = "Read all items from a SharePoint list.";
        desc.parameter_names = {"site_id", "list_id", "secret"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM graph_list_items('site-id', 'list-id', secret := 'ms_graph')"};
        desc.categories = {"microsoft", "graph", "sharepoint"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }

    ERPL_TRACE_INFO("GRAPH_SHAREPOINT", "Successfully registered Microsoft Graph SharePoint functions");
}

} // namespace erpl_web
