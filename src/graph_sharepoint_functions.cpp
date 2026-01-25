#include "graph_sharepoint_functions.hpp"
#include "graph_sharepoint_client.hpp"
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

struct ShowSitesBindData : public TableFunctionData {
    std::string secret_name;
    std::string search_query;
    std::string json_response;
    bool done = false;
};

struct ShowListsBindData : public TableFunctionData {
    std::string secret_name;
    std::string site_id;
    std::string json_response;
    bool done = false;
};

struct DescribeListBindData : public TableFunctionData {
    std::string secret_name;
    std::string site_id;
    std::string list_id;
    std::string json_response;
    bool done = false;
};

struct ListItemsBindData : public TableFunctionData {
    std::string secret_name;
    std::string site_id;
    std::string list_id;
    std::string json_response;
    std::vector<std::string> column_names;
    bool done = false;
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

    if (bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphSharePointClient client(auth_info.auth_params);
        bind_data.json_response = client.SearchSites(bind_data.search_query);
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

        // displayName
        yyjson_val *display_val = yyjson_obj_get(item, "displayName");
        output.SetValue(2, row, display_val && yyjson_is_str(display_val) ?
            Value(yyjson_get_str(display_val)) : Value());

        // webUrl
        yyjson_val *url_val = yyjson_obj_get(item, "webUrl");
        output.SetValue(3, row, url_val && yyjson_is_str(url_val) ?
            Value(yyjson_get_str(url_val)) : Value());

        // createdDateTime
        yyjson_val *created_val = yyjson_obj_get(item, "createdDateTime");
        output.SetValue(4, row, created_val && yyjson_is_str(created_val) ?
            Value(yyjson_get_str(created_val)) : Value());

        row++;
    }

    yyjson_doc_free(doc);
    bind_data.done = true;
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

    // site_id is required positional parameter
    if (input.inputs.empty()) {
        throw BinderException("graph_show_lists requires a site_id parameter");
    }
    bind_data->site_id = input.inputs[0].GetValue<std::string>();

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

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

    if (bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphSharePointClient client(auth_info.auth_params);
        bind_data.json_response = client.ListLists(bind_data.site_id);
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

        // displayName
        yyjson_val *display_val = yyjson_obj_get(item, "displayName");
        output.SetValue(2, row, display_val && yyjson_is_str(display_val) ?
            Value(yyjson_get_str(display_val)) : Value());

        // description
        yyjson_val *desc_val = yyjson_obj_get(item, "description");
        output.SetValue(3, row, desc_val && yyjson_is_str(desc_val) ?
            Value(yyjson_get_str(desc_val)) : Value());

        // webUrl
        yyjson_val *url_val = yyjson_obj_get(item, "webUrl");
        output.SetValue(4, row, url_val && yyjson_is_str(url_val) ?
            Value(yyjson_get_str(url_val)) : Value());

        // createdDateTime
        yyjson_val *created_val = yyjson_obj_get(item, "createdDateTime");
        output.SetValue(5, row, created_val && yyjson_is_str(created_val) ?
            Value(yyjson_get_str(created_val)) : Value());

        // lastModifiedDateTime
        yyjson_val *modified_val = yyjson_obj_get(item, "lastModifiedDateTime");
        output.SetValue(6, row, modified_val && yyjson_is_str(modified_val) ?
            Value(yyjson_get_str(modified_val)) : Value());

        row++;
    }

    yyjson_doc_free(doc);
    bind_data.done = true;
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

    if (bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphSharePointClient client(auth_info.auth_params);
        bind_data.json_response = client.GetListColumns(bind_data.site_id, bind_data.list_id);
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
    if (count > STANDARD_VECTOR_SIZE) {
        count = STANDARD_VECTOR_SIZE;
    }

    output.SetCardinality(count);

    size_t idx, max;
    yyjson_val *item;
    idx_t row = 0;

    yyjson_arr_foreach(value_arr, idx, max, item) {
        if (row >= count) break;

        // name
        yyjson_val *name_val = yyjson_obj_get(item, "name");
        output.SetValue(0, row, name_val && yyjson_is_str(name_val) ?
            Value(yyjson_get_str(name_val)) : Value());

        // displayName
        yyjson_val *display_val = yyjson_obj_get(item, "displayName");
        output.SetValue(1, row, display_val && yyjson_is_str(display_val) ?
            Value(yyjson_get_str(display_val)) : Value());

        // Column type - check various type indicators
        std::string column_type = "unknown";
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
        output.SetValue(2, row, Value(column_type));

        // description
        yyjson_val *desc_val = yyjson_obj_get(item, "description");
        output.SetValue(3, row, desc_val && yyjson_is_str(desc_val) ?
            Value(yyjson_get_str(desc_val)) : Value());

        // required
        yyjson_val *req_val = yyjson_obj_get(item, "required");
        output.SetValue(4, row, req_val && yyjson_is_bool(req_val) ?
            Value::BOOLEAN(yyjson_get_bool(req_val)) : Value::BOOLEAN(false));

        row++;
    }

    yyjson_doc_free(doc);
    bind_data.done = true;
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

    // Always include id as first column
    names.push_back("id");
    return_types.push_back(LogicalType::VARCHAR);
    bind_data->column_names.push_back("id");

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

                names.push_back(col_name);
                return_types.push_back(LogicalType::VARCHAR);
                bind_data->column_names.push_back(col_name);
            }
        }
    }

    yyjson_doc_free(col_doc);

    // If we only have the id column, add a default "fields" column
    if (names.size() == 1) {
        names.push_back("fields");
        return_types.push_back(LogicalType::VARCHAR);
        bind_data->column_names.push_back("fields");
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

        // Get the fields object
        yyjson_val *fields_obj = yyjson_obj_get(item, "fields");

        for (size_t col = 0; col < bind_data.column_names.size(); col++) {
            const std::string &col_name = bind_data.column_names[col];

            if (col_name == "id") {
                yyjson_val *id_val = yyjson_obj_get(item, "id");
                output.SetValue(col, row, id_val && yyjson_is_str(id_val) ?
                    Value(yyjson_get_str(id_val)) : Value());
            } else if (fields_obj) {
                yyjson_val *field_val = yyjson_obj_get(fields_obj, col_name.c_str());
                if (field_val) {
                    if (yyjson_is_str(field_val)) {
                        output.SetValue(col, row, Value(yyjson_get_str(field_val)));
                    } else if (yyjson_is_num(field_val)) {
                        output.SetValue(col, row, Value(std::to_string(yyjson_get_num(field_val))));
                    } else if (yyjson_is_bool(field_val)) {
                        output.SetValue(col, row, Value(yyjson_get_bool(field_val) ? "true" : "false"));
                    } else if (yyjson_is_null(field_val)) {
                        output.SetValue(col, row, Value());
                    } else {
                        // Complex object - serialize as JSON
                        size_t json_len;
                        char *json_str = yyjson_val_write(field_val, 0, &json_len);
                        if (json_str) {
                            output.SetValue(col, row, Value(std::string(json_str, json_len)));
                            free(json_str);
                        } else {
                            output.SetValue(col, row, Value());
                        }
                    }
                } else {
                    output.SetValue(col, row, Value());
                }
            } else {
                output.SetValue(col, row, Value());
            }
        }

        row++;
    }

    yyjson_doc_free(doc);
    bind_data.done = true;
}

// ============================================================================
// Registration
// ============================================================================

void GraphSharePointFunctions::Register(ExtensionLoader &loader) {
    ERPL_TRACE_INFO("GRAPH_SHAREPOINT", "Registering Microsoft Graph SharePoint functions");

    // graph_show_sites(search_query?) - optional secret named param
    TableFunction show_sites("graph_show_sites", {}, ShowSitesScan, ShowSitesBind);
    show_sites.varargs = LogicalType::VARCHAR;
    show_sites.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(show_sites);

    // graph_show_lists(site_id) - optional secret named param
    TableFunction show_lists("graph_show_lists", {LogicalType::VARCHAR},
                             ShowListsScan, ShowListsBind);
    show_lists.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(show_lists);

    // graph_describe_list(site_id, list_id) - optional secret named param
    TableFunction describe_list("graph_describe_list",
                                {LogicalType::VARCHAR, LogicalType::VARCHAR},
                                DescribeListScan, DescribeListBind);
    describe_list.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(describe_list);

    // graph_list_items(site_id, list_id) - optional secret named param
    TableFunction list_items("graph_list_items",
                             {LogicalType::VARCHAR, LogicalType::VARCHAR},
                             ListItemsScan, ListItemsBind);
    list_items.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(list_items);

    ERPL_TRACE_INFO("GRAPH_SHAREPOINT", "Successfully registered Microsoft Graph SharePoint functions");
}

} // namespace erpl_web
