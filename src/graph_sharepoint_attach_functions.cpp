#include "graph_sharepoint_attach_functions.hpp"
#include "graph_sharepoint_client.hpp"
#include "graph_excel_client.hpp"
#include "graph_excel_secret.hpp"
#include "tracing.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "yyjson.hpp"
#include <unordered_set>

using namespace duckdb_yyjson;

namespace erpl_web {

using namespace duckdb;

// ============================================================================
// Helpers
// ============================================================================

// Sanitise an arbitrary string to a valid lowercase SQL identifier.
// Non-alphanumeric runs become a single '_'; leading digits get 't_' prefix.
static std::string ToSqlIdentifier(const std::string &s) {
    std::string result;
    bool prev_under = false;

    for (unsigned char c : s) {
        if (std::isalnum(c)) {
            result += static_cast<char>(std::tolower(c));
            prev_under = false;
        } else if (!result.empty() && !prev_under) {
            result += '_';
            prev_under = true;
        }
    }

    while (!result.empty() && result.back() == '_') {
        result.pop_back();
    }

    if (!result.empty() && std::isdigit(static_cast<unsigned char>(result[0]))) {
        result = "t_" + result;
    }

    return result.empty() ? "unnamed" : result;
}

// Strip extension from filename: "Budget 2024.xlsx" → "Budget 2024"
static std::string StripExtension(const std::string &filename) {
    auto dot = filename.rfind('.');
    return dot == std::string::npos ? filename : filename.substr(0, dot);
}

// Single-quote escape for SQL string literals.
static std::string SqlEscape(const std::string &s) {
    return StringUtil::Replace(s, "'", "''");
}

// Deduplicate a view name by appending _2, _3, … if already in the set.
static std::string DeduplicateName(const std::string &base,
                                   std::unordered_set<std::string> &used) {
    std::string candidate = base;
    int suffix = 2;
    while (!used.insert(candidate).second) {
        candidate = base + "_" + std::to_string(suffix++);
    }
    return candidate;
}

// Helpful error message for WAC / Files.ReadWrite.All permission issues.
static void ThrowWacPermissionError(const std::string &detail) {
    throw InvalidInputException(
        "Excel API error: %s\n"
        "The graph_excel_* functions require the 'Files.ReadWrite.All' application permission.\n"
        "To fix:\n"
        "  1. Azure Portal → App Registrations → [your app] → API Permissions\n"
        "  2. Add Microsoft Graph → Application permissions → Files.ReadWrite.All\n"
        "  3. Click 'Grant admin consent for [your tenant]'",
        detail.c_str());
}

// ============================================================================
// Bind data
// ============================================================================

struct SharePointAttachBindData : public TableFunctionData {
    std::string secret_name;
    std::string site;       // name or composite ID
    std::string drive;      // name or ID (for Excel content)
    std::string content;    // "lists" | "excel" | "tables" | "both"
    bool overwrite = false;
    bool done = false;
};

// ============================================================================
// Bind
// ============================================================================

static unique_ptr<FunctionData> SharePointAttachBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<SharePointAttachBindData>();

    if (input.inputs.empty() || input.inputs[0].IsNull()) {
        throw BinderException("sharepoint_attach requires a site name or site ID");
    }
    bind_data->site = input.inputs[0].GetValue<std::string>();

    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        bind_data->secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    if (input.named_parameters.find("drive") != input.named_parameters.end()) {
        bind_data->drive = input.named_parameters.at("drive").GetValue<std::string>();
    }
    if (input.named_parameters.find("overwrite") != input.named_parameters.end()) {
        bind_data->overwrite = input.named_parameters.at("overwrite").GetValue<bool>();
    }

    // Auto-infer content: drive present → excel, else lists
    if (input.named_parameters.find("content") != input.named_parameters.end()) {
        bind_data->content = input.named_parameters.at("content").GetValue<std::string>();
    } else {
        bind_data->content = bind_data->drive.empty() ? "lists" : "excel";
    }

    // Validate content value
    const auto &c = bind_data->content;
    if (c != "lists" && c != "excel" && c != "tables" && c != "both") {
        throw BinderException(
            "sharepoint_attach: content must be one of 'lists', 'excel', 'tables', 'both' (got '%s')",
            c.c_str());
    }

    // Excel modes require a drive
    if ((c == "excel" || c == "tables" || c == "both") && bind_data->drive.empty()) {
        throw BinderException(
            "sharepoint_attach: content := '%s' requires the 'drive' parameter", c.c_str());
    }

    return_types.push_back(LogicalType::BOOLEAN);
    names.push_back("success");

    return std::move(bind_data);
}

// ============================================================================
// Scan helpers
// ============================================================================

static void CreateView(Connection &duck_conn, const std::string &view_name,
                       const std::string &select_sql, bool overwrite) {
    std::string ddl = overwrite
        ? "CREATE OR REPLACE VIEW " + view_name + " AS " + select_sql
        : "CREATE VIEW IF NOT EXISTS " + view_name + " AS " + select_sql;
    auto result = duck_conn.Query(ddl);
    if (result->HasError()) {
        ERPL_TRACE_WARN("GRAPH_ATTACH", "Failed to create view " + view_name + ": " + result->GetError());
    } else {
        ERPL_TRACE_DEBUG("GRAPH_ATTACH", "Created view: " + view_name);
    }
}

static void AttachLists(Connection &duck_conn,
                        GraphSharePointClient &sp_client,
                        const std::string &site_id,
                        const std::string &secret_name,
                        bool overwrite) {
    ERPL_TRACE_INFO("GRAPH_ATTACH", "Attaching SharePoint lists for site: " + site_id);

    auto json = sp_client.ListLists(site_id);
    yyjson_doc *doc = yyjson_read(json.c_str(), json.size(), 0);
    if (!doc) {
        throw InvalidInputException("Failed to parse lists response for site: %s", site_id.c_str());
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    if (!value_arr || !yyjson_is_arr(value_arr)) {
        yyjson_doc_free(doc);
        ERPL_TRACE_INFO("GRAPH_ATTACH", "No lists found for site: " + site_id);
        return;
    }

    std::unordered_set<std::string> used_names;
    size_t idx, max;
    yyjson_val *item;

    yyjson_arr_foreach(value_arr, idx, max, item) {
        yyjson_val *id_val = yyjson_obj_get(item, "id");
        yyjson_val *disp_val = yyjson_obj_get(item, "displayName");
        if (!id_val || !yyjson_is_str(id_val)) {
            continue;
        }

        const std::string list_id = yyjson_get_str(id_val);
        const std::string display_name = (disp_val && yyjson_is_str(disp_val))
            ? yyjson_get_str(disp_val) : list_id;

        const std::string view_name = DeduplicateName(ToSqlIdentifier(display_name), used_names);
        const std::string select_sql =
            "SELECT * FROM graph_list_items('" + SqlEscape(site_id) + "', '" +
            SqlEscape(list_id) + "', secret := '" + SqlEscape(secret_name) + "')";

        CreateView(duck_conn, view_name, select_sql, overwrite);
    }

    yyjson_doc_free(doc);
}

static void AttachExcelSheets(Connection &duck_conn,
                              GraphExcelClient &excel_client,
                              const std::string &site,
                              const std::string &drive,
                              const std::string &drive_id,
                              const std::string &secret_name,
                              bool overwrite) {
    ERPL_TRACE_INFO("GRAPH_ATTACH", "Attaching Excel sheets from drive: " + drive_id);

    auto files_json = excel_client.ListDriveFiles("", drive_id);
    yyjson_doc *doc = yyjson_read(files_json.c_str(), files_json.size(), 0);
    if (!doc) {
        throw InvalidInputException("Failed to parse drive file listing");
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    if (!value_arr || !yyjson_is_arr(value_arr)) {
        yyjson_doc_free(doc);
        return;
    }

    // Collect xlsx entries first so we can free the doc before inner API calls
    struct XlsxEntry { std::string name; std::string path; };
    std::vector<XlsxEntry> xlsx_files;

    size_t idx, max;
    yyjson_val *item;
    yyjson_arr_foreach(value_arr, idx, max, item) {
        yyjson_val *name_val = yyjson_obj_get(item, "name");
        if (!name_val || !yyjson_is_str(name_val)) {
            continue;
        }
        const std::string filename = yyjson_get_str(name_val);
        // Only process Excel workbooks
        if (filename.size() < 5 ||
            filename.substr(filename.size() - 5) != ".xlsx") {
            continue;
        }
        xlsx_files.push_back({filename, "/" + filename});
    }
    yyjson_doc_free(doc);

    std::unordered_set<std::string> used_names;
    for (const auto &xlsx : xlsx_files) {
        const std::string file_stem = StripExtension(xlsx.name);
        const std::string stem_id = ToSqlIdentifier(file_stem);

        std::string sheets_json;
        try {
            sheets_json = excel_client.ListWorksheetsByPath(xlsx.path, drive_id);
        } catch (const std::exception &e) {
            const std::string msg = e.what();
            if (msg.find("WAC") != std::string::npos ||
                msg.find("403") != std::string::npos ||
                msg.find("access token") != std::string::npos) {
                ThrowWacPermissionError(msg);
            }
            ERPL_TRACE_WARN("GRAPH_ATTACH",
                "Could not list worksheets for " + xlsx.name + ": " + msg);
            continue;
        }

        yyjson_doc *sheet_doc = yyjson_read(sheets_json.c_str(), sheets_json.size(), 0);
        if (!sheet_doc) {
            continue;
        }

        yyjson_val *sheet_root = yyjson_doc_get_root(sheet_doc);
        yyjson_val *sheet_arr = yyjson_obj_get(sheet_root, "value");

        if (!sheet_arr || !yyjson_is_arr(sheet_arr)) {
            yyjson_doc_free(sheet_doc);
            continue;
        }

        size_t sidx, smax;
        yyjson_val *sheet_item;
        yyjson_arr_foreach(sheet_arr, sidx, smax, sheet_item) {
            yyjson_val *sname_val = yyjson_obj_get(sheet_item, "name");
            if (!sname_val || !yyjson_is_str(sname_val)) {
                continue;
            }
            const std::string sheet_name = yyjson_get_str(sname_val);
            const std::string view_name = DeduplicateName(
                stem_id + "_" + ToSqlIdentifier(sheet_name), used_names);

            const std::string select_sql =
                "SELECT * FROM graph_excel_range('" + SqlEscape(xlsx.path) + "', '" +
                SqlEscape(sheet_name) + "', site := '" + SqlEscape(site) +
                "', drive := '" + SqlEscape(drive) +
                "', secret := '" + SqlEscape(secret_name) + "')";

            CreateView(duck_conn, view_name, select_sql, overwrite);
        }

        yyjson_doc_free(sheet_doc);
    }
}

static void AttachExcelTables(Connection &duck_conn,
                              GraphExcelClient &excel_client,
                              const std::string &site,
                              const std::string &drive,
                              const std::string &drive_id,
                              const std::string &secret_name,
                              bool overwrite) {
    ERPL_TRACE_INFO("GRAPH_ATTACH", "Attaching Excel named tables from drive: " + drive_id);

    auto files_json = excel_client.ListDriveFiles("", drive_id);
    yyjson_doc *doc = yyjson_read(files_json.c_str(), files_json.size(), 0);
    if (!doc) {
        throw InvalidInputException("Failed to parse drive file listing");
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    if (!value_arr || !yyjson_is_arr(value_arr)) {
        yyjson_doc_free(doc);
        return;
    }

    struct XlsxEntry { std::string name; std::string path; };
    std::vector<XlsxEntry> xlsx_files;

    size_t idx, max;
    yyjson_val *item;
    yyjson_arr_foreach(value_arr, idx, max, item) {
        yyjson_val *name_val = yyjson_obj_get(item, "name");
        if (!name_val || !yyjson_is_str(name_val)) {
            continue;
        }
        const std::string filename = yyjson_get_str(name_val);
        if (filename.size() < 5 ||
            filename.substr(filename.size() - 5) != ".xlsx") {
            continue;
        }
        xlsx_files.push_back({filename, "/" + filename});
    }
    yyjson_doc_free(doc);

    std::unordered_set<std::string> used_names;
    for (const auto &xlsx : xlsx_files) {
        const std::string file_stem = StripExtension(xlsx.name);
        const std::string stem_id = ToSqlIdentifier(file_stem);

        std::string tables_json;
        try {
            tables_json = excel_client.ListTablesByPath(xlsx.path, drive_id);
        } catch (const std::exception &e) {
            const std::string msg = e.what();
            if (msg.find("WAC") != std::string::npos ||
                msg.find("403") != std::string::npos ||
                msg.find("access token") != std::string::npos) {
                ThrowWacPermissionError(msg);
            }
            ERPL_TRACE_WARN("GRAPH_ATTACH",
                "Could not list tables for " + xlsx.name + ": " + msg);
            continue;
        }

        yyjson_doc *tbl_doc = yyjson_read(tables_json.c_str(), tables_json.size(), 0);
        if (!tbl_doc) {
            continue;
        }

        yyjson_val *tbl_root = yyjson_doc_get_root(tbl_doc);
        yyjson_val *tbl_arr = yyjson_obj_get(tbl_root, "value");

        if (!tbl_arr || !yyjson_is_arr(tbl_arr)) {
            yyjson_doc_free(tbl_doc);
            continue;
        }

        size_t tidx, tmax;
        yyjson_val *tbl_item;
        yyjson_arr_foreach(tbl_arr, tidx, tmax, tbl_item) {
            yyjson_val *tname_val = yyjson_obj_get(tbl_item, "name");
            if (!tname_val || !yyjson_is_str(tname_val)) {
                continue;
            }
            const std::string table_name = yyjson_get_str(tname_val);
            const std::string view_name = DeduplicateName(
                stem_id + "_" + ToSqlIdentifier(table_name), used_names);

            const std::string select_sql =
                "SELECT * FROM graph_excel_table_data('" + SqlEscape(xlsx.path) + "', '" +
                SqlEscape(table_name) + "', site := '" + SqlEscape(site) +
                "', drive := '" + SqlEscape(drive) +
                "', secret := '" + SqlEscape(secret_name) + "')";

            CreateView(duck_conn, view_name, select_sql, overwrite);
        }

        yyjson_doc_free(tbl_doc);
    }
}

// ============================================================================
// Scan
// ============================================================================

static void SharePointAttachScan(ClientContext &context,
                                 TableFunctionInput &data_p,
                                 DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<SharePointAttachBindData>();
    if (bind_data.done) {
        return;
    }
    bind_data.done = true;

    auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
    GraphSharePointClient sp_client(auth_info.auth_params);

    const std::string site_id = sp_client.ResolveSiteId(bind_data.site);

    auto duck_conn = Connection(context.db->GetDatabase(context));

    const bool do_lists  = (bind_data.content == "lists"  || bind_data.content == "both");
    const bool do_sheets = (bind_data.content == "excel"  || bind_data.content == "both");
    const bool do_tables = (bind_data.content == "tables");

    if (do_lists) {
        AttachLists(duck_conn, sp_client, site_id, bind_data.secret_name, bind_data.overwrite);
    }

    if (do_sheets || do_tables) {
        const std::string drive_id = sp_client.ResolveDriveId(site_id, bind_data.drive);
        GraphExcelClient excel_client(auth_info.auth_params);

        if (do_sheets) {
            AttachExcelSheets(duck_conn, excel_client,
                              bind_data.site, bind_data.drive, drive_id,
                              bind_data.secret_name, bind_data.overwrite);
        } else {
            AttachExcelTables(duck_conn, excel_client,
                              bind_data.site, bind_data.drive, drive_id,
                              bind_data.secret_name, bind_data.overwrite);
        }
    }

    output.SetCardinality(1);
    output.SetValue(0, 0, Value::BOOLEAN(true));
}

// ============================================================================
// Registration
// ============================================================================

void GraphSharePointAttachFunctions::Register(ExtensionLoader &loader) {
    ERPL_TRACE_INFO("GRAPH_ATTACH", "Registering sharepoint_attach function");

    TableFunction attach_fn("sharepoint_attach", {LogicalType::VARCHAR},
                            SharePointAttachScan, SharePointAttachBind);
    attach_fn.named_parameters["secret"]    = LogicalType::VARCHAR;
    attach_fn.named_parameters["drive"]     = LogicalType::VARCHAR;
    attach_fn.named_parameters["content"]   = LogicalType::VARCHAR;
    attach_fn.named_parameters["overwrite"] = LogicalType::BOOLEAN;

    CreateTableFunctionInfo info(attach_fn);
    FunctionDescription desc;
    desc.description =
        "Attach a SharePoint site as DuckDB views. By default exposes lists; "
        "pass drive := 'name' to expose Excel workbooks. "
        "Use content := 'lists'|'excel'|'tables'|'both' to control what is attached.";
    desc.parameter_names  = {"site", "secret", "drive", "content", "overwrite"};
    desc.parameter_types  = {LogicalType::VARCHAR, LogicalType::VARCHAR,
                              LogicalType::VARCHAR, LogicalType::VARCHAR,
                              LogicalType::BOOLEAN};
    desc.examples = {
        "SELECT * FROM sharepoint_attach('Finance', secret := 'ms_graph')",
        "SELECT * FROM sharepoint_attach('Finance', drive := 'Documents', secret := 'ms_graph')",
        "SELECT * FROM sharepoint_attach('Finance', drive := 'Documents', content := 'tables', secret := 'ms_graph')",
        "SELECT * FROM sharepoint_attach('Finance', drive := 'Documents', content := 'both', secret := 'ms_graph', overwrite := true)"
    };
    desc.categories = {"microsoft", "graph", "sharepoint", "excel"};
    info.descriptions.push_back(std::move(desc));

    loader.RegisterFunction(std::move(info));
    ERPL_TRACE_INFO("GRAPH_ATTACH", "Successfully registered sharepoint_attach");
}

} // namespace erpl_web
