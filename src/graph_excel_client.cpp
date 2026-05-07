#include "graph_excel_client.hpp"
#include "graph_client.hpp"
#include "tracing.hpp"
#include "yyjson.hpp"

#include <algorithm>
#include <map>
#include <stdexcept>

namespace erpl_web {

// URL Builder implementation
std::string GraphExcelUrlBuilder::GetBaseUrl() {
    return GraphClient::BaseUrl();
}

std::string GraphExcelUrlBuilder::BuildDriveItemUrl(const std::string &item_id) {
    return GetBaseUrl() + "/me/drive/items/" + item_id;
}

std::string GraphExcelUrlBuilder::BuildDriveItemByPathUrl(const std::string &path) {
    // Ensure path doesn't start with /
    std::string clean_path = GraphClient::StripLeadingSlash(path);
    return GetBaseUrl() + "/me/drive/root:/" + GraphClient::UrlEncode(clean_path, true) + ":";
}

std::string GraphExcelUrlBuilder::BuildSiteDriveItemUrl(const std::string &site_id, const std::string &item_id) {
    return GetBaseUrl() + "/sites/" + site_id + "/drive/items/" + item_id;
}

std::string GraphExcelUrlBuilder::BuildWorkbookUrl(const std::string &item_url) {
    return item_url + "/workbook";
}

std::string GraphExcelUrlBuilder::BuildTablesUrl(const std::string &workbook_url) {
    return workbook_url + "/tables";
}

std::string GraphExcelUrlBuilder::BuildTableUrl(const std::string &workbook_url, const std::string &table_name) {
    return workbook_url + "/tables/" + GraphClient::UrlEncode(table_name);
}

std::string GraphExcelUrlBuilder::BuildTableRowsUrl(const std::string &workbook_url, const std::string &table_name, int32_t top) {
    std::string url = workbook_url + "/tables/" + GraphClient::UrlEncode(table_name) + "/rows";
    if (top > 0) {
        url += "?$top=" + std::to_string(top);
    }
    return url;
}

std::string GraphExcelUrlBuilder::BuildWorksheetsUrl(const std::string &workbook_url) {
    return workbook_url + "/worksheets";
}

std::string GraphExcelUrlBuilder::BuildWorksheetUrl(const std::string &workbook_url, const std::string &sheet_name) {
    return workbook_url + "/worksheets/" + GraphClient::UrlEncode(sheet_name);
}

std::string GraphExcelUrlBuilder::BuildUsedRangeUrl(const std::string &workbook_url, const std::string &sheet_name) {
    // $select eliminates unused 2D arrays (text, formulas, formulasLocal, formulasR1C1, …)
    // reducing the JSON payload to ~43% of the full response for large sheets.
    return workbook_url + "/worksheets/" + GraphClient::UrlEncode(sheet_name) +
           "/usedRange?$select=values,valueTypes,numberFormat";
}

std::string GraphExcelUrlBuilder::BuildRangeUrl(const std::string &workbook_url, const std::string &sheet_name, const std::string &range) {
    return workbook_url + "/worksheets/" + GraphClient::UrlEncode(sheet_name) +
           "/range(address='" + GraphClient::EscapeODataStringLiteral(range) +
           "')?$select=values,valueTypes,numberFormat";
}

std::string GraphExcelUrlBuilder::BuildDriveRootChildrenUrl() {
    return GetBaseUrl() + "/me/drive/root/children";
}

std::string GraphExcelUrlBuilder::BuildDriveFolderChildrenUrl(const std::string &folder_id) {
    return GetBaseUrl() + "/me/drive/items/" + folder_id + "/children";
}

std::string GraphExcelUrlBuilder::BuildSiteDriveRootChildrenUrl(const std::string &site_id) {
    return GetBaseUrl() + "/sites/" + site_id + "/drive/root/children";
}

std::string GraphExcelUrlBuilder::BuildDriveItemByPathWithDriveUrl(const std::string &drive_id, const std::string &path) {
    std::string clean_path = GraphClient::StripLeadingSlash(path);
    return GetBaseUrl() + "/drives/" + drive_id + "/root:/" + GraphClient::UrlEncode(clean_path, true) + ":";
}

std::string GraphExcelUrlBuilder::BuildDriveRootChildrenWithDriveUrl(const std::string &drive_id) {
    return GetBaseUrl() + "/drives/" + drive_id + "/root/children";
}

std::string GraphExcelUrlBuilder::BuildDriveFolderChildrenWithDriveUrl(const std::string &drive_id, const std::string &folder_path) {
    std::string clean_path = GraphClient::StripLeadingSlash(folder_path);
    return GetBaseUrl() + "/drives/" + drive_id + "/root:/" + GraphClient::UrlEncode(clean_path, true) + ":/children";
}

std::string GraphExcelUrlBuilder::BuildSiteDefaultDriveItemByPathUrl(const std::string &site_id, const std::string &path) {
    std::string clean_path = GraphClient::StripLeadingSlash(path);
    return GetBaseUrl() + "/sites/" + site_id + "/drive/root:/" + GraphClient::UrlEncode(clean_path, true) + ":";
}

std::string GraphExcelUrlBuilder::BuildCreateSessionUrl(const std::string &workbook_url) {
    return workbook_url + "/createSession";
}

std::string GraphExcelUrlBuilder::BuildCloseSessionUrl(const std::string &workbook_url) {
    return workbook_url + "/closeSession";
}

std::string GraphExcelUrlBuilder::BuildTableRowsAddUrl(const std::string &workbook_url, const std::string &table_name) {
    return workbook_url + "/tables/" + GraphClient::UrlEncode(table_name) + "/rows/add";
}

std::string GraphExcelUrlBuilder::BuildTableRowDeleteUrl(const std::string &workbook_url,
                                                          const std::string &table_name,
                                                          idx_t row_index)
{
    return workbook_url + "/tables/" + GraphClient::UrlEncode(table_name) +
           "/rows/itemAt(index=" + std::to_string(row_index) + ")";
}

std::string GraphExcelUrlBuilder::BuildTableColumnsUrl(const std::string &workbook_url, const std::string &table_name) {
    return workbook_url + "/tables/" + GraphClient::UrlEncode(table_name) + "/columns?$select=id,index,name";
}

// GraphExcelClient implementation
GraphExcelClient::GraphExcelClient(std::shared_ptr<HttpAuthParams> auth_params)
    : auth_params(auth_params) {
}

std::string GraphExcelClient::DoGraphGet(const std::string &url) {
    return GraphClient(auth_params, "GRAPH_EXCEL").Get(url);
}

std::string GraphExcelClient::ResolveWorkbookItemUrl(const std::string &file_path, const std::string &drive_id) {
    if (drive_id.empty()) {
        // Delegated auth: /me/drive path is WAC-compatible
        return GraphExcelUrlBuilder::BuildDriveItemByPathUrl(file_path);
    }

    // App auth: /drives/{id} triggers WAC failure; resolve the site coordinates
    // so we can use /sites/{site-id}/drive/root:/{path}: instead.
    ERPL_TRACE_DEBUG("GRAPH_EXCEL", "Resolving SharePoint site coordinates for drive: " + drive_id);
    const std::string metadata_url = GraphExcelUrlBuilder::GetBaseUrl()
        + "/drives/" + drive_id + "?$select=id,sharePointIds";

    const std::string json_body = DoGraphGet(metadata_url);

    duckdb_yyjson::yyjson_doc *doc = duckdb_yyjson::yyjson_read(json_body.c_str(), json_body.size(), 0);
    if (!doc) {
        throw std::runtime_error("Failed to parse sharePointIds response for drive: " + drive_id);
    }

    duckdb_yyjson::yyjson_val *root = duckdb_yyjson::yyjson_doc_get_root(doc);
    duckdb_yyjson::yyjson_val *sp_ids = duckdb_yyjson::yyjson_obj_get(root, "sharePointIds");

    std::string site_id;
    if (sp_ids) {
        duckdb_yyjson::yyjson_val *site_url_val = duckdb_yyjson::yyjson_obj_get(sp_ids, "siteUrl");
        duckdb_yyjson::yyjson_val *site_guid_val = duckdb_yyjson::yyjson_obj_get(sp_ids, "siteId");
        duckdb_yyjson::yyjson_val *web_guid_val  = duckdb_yyjson::yyjson_obj_get(sp_ids, "webId");

        if (site_url_val && site_guid_val && web_guid_val) {
            std::string site_url  = duckdb_yyjson::yyjson_get_str(site_url_val);
            std::string site_guid = duckdb_yyjson::yyjson_get_str(site_guid_val);
            std::string web_guid  = duckdb_yyjson::yyjson_get_str(web_guid_val);

            // Extract hostname from siteUrl (e.g. "https://tenant.sharepoint.com/sites/team" → "tenant.sharepoint.com")
            std::string hostname = site_url;
            const std::string https_prefix = "https://";
            const std::string http_prefix  = "http://";
            if (hostname.rfind(https_prefix, 0) == 0) {
                hostname = hostname.substr(https_prefix.size());
            } else if (hostname.rfind(http_prefix, 0) == 0) {
                hostname = hostname.substr(http_prefix.size());
            }
            // Trim any path component — site_id uses only the hostname
            const auto slash_pos = hostname.find('/');
            if (slash_pos != std::string::npos) {
                hostname = hostname.substr(0, slash_pos);
            }

            site_id = hostname + "," + site_guid + "," + web_guid;
        }
    }

    duckdb_yyjson::yyjson_doc_free(doc);

    if (site_id.empty()) {
        throw std::runtime_error(
            "Could not resolve SharePoint site coordinates for drive '" + drive_id +
            "'. The drive may not be a SharePoint drive, or the token lacks Sites.Read.All.");
    }

    ERPL_TRACE_DEBUG("GRAPH_EXCEL", "Resolved site_id: " + site_id);
    return GraphExcelUrlBuilder::BuildSiteDefaultDriveItemByPathUrl(site_id, file_path);
}

std::string GraphExcelClient::ListDriveFiles(const std::string &folder_path, const std::string &drive_id) {
    std::string url;
    if (!drive_id.empty()) {
        if (folder_path.empty()) {
            url = GraphExcelUrlBuilder::BuildDriveRootChildrenWithDriveUrl(drive_id);
        } else {
            url = GraphExcelUrlBuilder::BuildDriveFolderChildrenWithDriveUrl(drive_id, folder_path);
        }
    } else if (folder_path.empty()) {
        url = GraphExcelUrlBuilder::BuildDriveRootChildrenUrl();
    } else {
        url = GraphExcelUrlBuilder::BuildDriveFolderChildrenUrl(folder_path);
    }
    return GraphClient(auth_params, "GRAPH_EXCEL").GetAllPagesMerged(url);
}

std::string GraphExcelClient::ListSiteFiles(const std::string &site_id, const std::string &folder_path) {
    std::string url = GraphExcelUrlBuilder::BuildSiteDriveRootChildrenUrl(site_id);
    return GraphClient(auth_params, "GRAPH_EXCEL").GetAllPagesMerged(url);
}

std::string GraphExcelClient::GetTableRows(const std::string &item_id, const std::string &table_name) {
    auto item_url = GraphExcelUrlBuilder::BuildDriveItemUrl(item_id);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto rows_url = GraphExcelUrlBuilder::BuildTableRowsUrl(workbook_url, table_name);
    return GraphClient(auth_params, "GRAPH_EXCEL").GetAllPagesMerged(rows_url);
}

std::string GraphExcelClient::GetTableRowsByPath(const std::string &file_path, const std::string &table_name, const std::string &drive_id) {
    auto item_url = ResolveWorkbookItemUrl(file_path, drive_id);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto rows_url = GraphExcelUrlBuilder::BuildTableRowsUrl(workbook_url, table_name);
    return GraphClient(auth_params, "GRAPH_EXCEL").GetAllPagesMerged(rows_url);
}

std::string GraphExcelClient::GetUsedRange(const std::string &item_id, const std::string &sheet_name) {
    auto item_url = GraphExcelUrlBuilder::BuildDriveItemUrl(item_id);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto range_url = GraphExcelUrlBuilder::BuildUsedRangeUrl(workbook_url, sheet_name);
    return DoGraphGet(range_url);
}

std::string GraphExcelClient::GetUsedRangeByPath(const std::string &file_path, const std::string &sheet_name, const std::string &drive_id) {
    auto item_url = ResolveWorkbookItemUrl(file_path, drive_id);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto range_url = GraphExcelUrlBuilder::BuildUsedRangeUrl(workbook_url, sheet_name);
    return DoGraphGet(range_url);
}

std::string GraphExcelClient::GetRange(const std::string &item_id, const std::string &sheet_name, const std::string &range) {
    auto item_url = GraphExcelUrlBuilder::BuildDriveItemUrl(item_id);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto range_url = GraphExcelUrlBuilder::BuildRangeUrl(workbook_url, sheet_name, range);
    return DoGraphGet(range_url);
}

std::string GraphExcelClient::GetRangeByPath(const std::string &file_path, const std::string &sheet_name, const std::string &range, const std::string &drive_id) {
    auto item_url = ResolveWorkbookItemUrl(file_path, drive_id);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto range_url = GraphExcelUrlBuilder::BuildRangeUrl(workbook_url, sheet_name, range);
    return DoGraphGet(range_url);
}

std::string GraphExcelClient::ListTables(const std::string &item_id) {
    auto item_url = GraphExcelUrlBuilder::BuildDriveItemUrl(item_id);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto tables_url = GraphExcelUrlBuilder::BuildTablesUrl(workbook_url);
    return GraphClient(auth_params, "GRAPH_EXCEL").GetAllPagesMerged(tables_url);
}

std::string GraphExcelClient::ListTablesByPath(const std::string &file_path, const std::string &drive_id) {
    auto item_url = ResolveWorkbookItemUrl(file_path, drive_id);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto tables_url = GraphExcelUrlBuilder::BuildTablesUrl(workbook_url);
    return GraphClient(auth_params, "GRAPH_EXCEL").GetAllPagesMerged(tables_url);
}

std::string GraphExcelClient::ListWorksheets(const std::string &item_id) {
    auto item_url = GraphExcelUrlBuilder::BuildDriveItemUrl(item_id);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto worksheets_url = GraphExcelUrlBuilder::BuildWorksheetsUrl(workbook_url);
    return GraphClient(auth_params, "GRAPH_EXCEL").GetAllPagesMerged(worksheets_url);
}

std::string GraphExcelClient::ListWorksheetsByPath(const std::string &file_path, const std::string &drive_id) {
    auto item_url = ResolveWorkbookItemUrl(file_path, drive_id);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto worksheets_url = GraphExcelUrlBuilder::BuildWorksheetsUrl(workbook_url);
    return GraphClient(auth_params, "GRAPH_EXCEL").GetAllPagesMerged(worksheets_url);
}

std::vector<std::string> GraphExcelClient::GetTableColumnsByPath(const std::string &file_path,
                                                                   const std::string &table_name,
                                                                   const std::string &drive_id)
{
    const auto item_url    = ResolveWorkbookItemUrl(file_path, drive_id);
    const auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    const auto cols_url    = GraphExcelUrlBuilder::BuildTableColumnsUrl(workbook_url, table_name);
    const std::string json = GraphClient(auth_params, "GRAPH_EXCEL").GetAllPagesMerged(cols_url);

    std::vector<std::string> names;
    duckdb_yyjson::yyjson_doc *doc = duckdb_yyjson::yyjson_read(json.c_str(), json.size(), 0);
    if (!doc) {
        return names;
    }

    duckdb_yyjson::yyjson_val *root = duckdb_yyjson::yyjson_doc_get_root(doc);
    duckdb_yyjson::yyjson_val *arr  = duckdb_yyjson::yyjson_obj_get(root, "value");
    if (arr && duckdb_yyjson::yyjson_is_arr(arr)) {
        // Build index → name map first (API order may not be index order)
        std::map<int, std::string> index_to_name;
        size_t idx, max;
        duckdb_yyjson::yyjson_val *col;
        yyjson_arr_foreach(arr, idx, max, col) {
            duckdb_yyjson::yyjson_val *name_val  = duckdb_yyjson::yyjson_obj_get(col, "name");
            duckdb_yyjson::yyjson_val *index_val = duckdb_yyjson::yyjson_obj_get(col, "index");
            if (name_val && duckdb_yyjson::yyjson_is_str(name_val) &&
                index_val && duckdb_yyjson::yyjson_is_int(index_val)) {
                index_to_name[static_cast<int>(duckdb_yyjson::yyjson_get_int(index_val))] =
                    duckdb_yyjson::yyjson_get_str(name_val);
            }
        }
        names.reserve(index_to_name.size());
        for (const auto &kv : index_to_name) {
            names.push_back(kv.second);
        }
    }
    duckdb_yyjson::yyjson_doc_free(doc);
    return names;
}

// Extract the workbook-session-id from a createSession response JSON
static std::string ExtractSessionId(const std::string &json_body) {
    duckdb_yyjson::yyjson_doc *doc = duckdb_yyjson::yyjson_read(json_body.c_str(), json_body.size(), 0);
    if (!doc) { return ""; }
    duckdb_yyjson::yyjson_val *root = duckdb_yyjson::yyjson_doc_get_root(doc);
    duckdb_yyjson::yyjson_val *id_val = duckdb_yyjson::yyjson_obj_get(root, "id");
    std::string session_id;
    if (id_val && duckdb_yyjson::yyjson_is_str(id_val)) {
        session_id = duckdb_yyjson::yyjson_get_str(id_val);
    }
    duckdb_yyjson::yyjson_doc_free(doc);
    return session_id;
}

idx_t GraphExcelClient::AddTableRows(const std::string &file_path, const std::string &table_name,
                                      const std::string &rows_json, const std::string &drive_id)
{
    GraphClient graph_client(auth_params, "GRAPH_EXCEL");

    const std::string item_url  = ResolveWorkbookItemUrl(file_path, drive_id);
    const std::string wb_url    = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);

    // Create a persistent workbook session (required for write operations per Graph best practices)
    const std::string create_session_url = GraphExcelUrlBuilder::BuildCreateSessionUrl(wb_url);
    const std::string session_body = "{\"persistChanges\":true}";

    // Prefer long-running session creation to avoid 504 timeouts on large workbooks
    std::map<std::string, std::string> prefer_header = {{"Prefer", "respond-async"}};
    std::string session_response = graph_client.PostWithHeaders(create_session_url, session_body, prefer_header);

    // Check if the server returned a status monitor URL (async 202) by inspecting for "statusMonitorResource"
    std::string session_id;
    {
        duckdb_yyjson::yyjson_doc *doc = duckdb_yyjson::yyjson_read(session_response.c_str(), session_response.size(), 0);
        if (doc) {
            duckdb_yyjson::yyjson_val *root = duckdb_yyjson::yyjson_doc_get_root(doc);
            duckdb_yyjson::yyjson_val *monitor_val = duckdb_yyjson::yyjson_obj_get(root, "statusMonitorResource");
            duckdb_yyjson::yyjson_val *id_val = duckdb_yyjson::yyjson_obj_get(root, "id");
            if (monitor_val && duckdb_yyjson::yyjson_is_str(monitor_val)) {
                // Long-running: poll the status monitor until session is ready
                const std::string monitor_url = duckdb_yyjson::yyjson_get_str(monitor_val);
                duckdb_yyjson::yyjson_doc_free(doc);
                // Poll up to 30 times with 1-second intervals
                for (int poll = 0; poll < 30 && session_id.empty(); poll++) {
                    const std::string poll_response = graph_client.Get(monitor_url);
                    session_id = ExtractSessionId(poll_response);
                }
            } else if (id_val && duckdb_yyjson::yyjson_is_str(id_val)) {
                // Immediate (201): session id is in the response
                session_id = duckdb_yyjson::yyjson_get_str(id_val);
                duckdb_yyjson::yyjson_doc_free(doc);
            } else {
                duckdb_yyjson::yyjson_doc_free(doc);
            }
        }
    }

    if (session_id.empty()) {
        throw duckdb::IOException("Failed to create Excel workbook session for file: " + file_path);
    }

    ERPL_TRACE_DEBUG("GRAPH_EXCEL", "Created workbook session: " + session_id);

    // Session header — required for all write operations per Graph best practices
    std::map<std::string, std::string> session_headers = {{"workbook-session-id", session_id}};

    // RAII: close session on scope exit (even if rows/add fails)
    const std::string close_session_url = GraphExcelUrlBuilder::BuildCloseSessionUrl(wb_url);
    struct SessionGuard {
        GraphClient &client;
        const std::string &close_url;
        const std::map<std::string, std::string> &headers;
        ~SessionGuard() noexcept {
            try {
                client.PostWithHeaders(close_url, "{}", headers);
            } catch (...) {
                // Best-effort close; swallow errors during teardown
            }
        }
    } guard{graph_client, close_session_url, session_headers};

    // POST rows to the table
    const std::string rows_add_url = GraphExcelUrlBuilder::BuildTableRowsAddUrl(wb_url, table_name);
    graph_client.PostWithHeaders(rows_add_url, rows_json, session_headers);

    // Count the rows added from the rows_json (count top-level array elements)
    idx_t row_count = 0;
    {
        duckdb_yyjson::yyjson_doc *doc = duckdb_yyjson::yyjson_read(rows_json.c_str(), rows_json.size(), 0);
        if (doc) {
            duckdb_yyjson::yyjson_val *root = duckdb_yyjson::yyjson_doc_get_root(doc);
            duckdb_yyjson::yyjson_val *values_arr = duckdb_yyjson::yyjson_obj_get(root, "values");
            if (values_arr && duckdb_yyjson::yyjson_is_arr(values_arr)) {
                row_count = duckdb_yyjson::yyjson_arr_size(values_arr);
            }
            duckdb_yyjson::yyjson_doc_free(doc);
        }
    }
    return row_count;
}

idx_t GraphExcelClient::DeleteTableRowsMatchingColumn(const std::string &file_path,
                                                       const std::string &table_name,
                                                       idx_t col_index,
                                                       const std::string &col_value,
                                                       const std::string &drive_id)
{
    GraphClient graph_client(auth_params, "GRAPH_EXCEL");

    const std::string item_url = ResolveWorkbookItemUrl(file_path, drive_id);
    const std::string wb_url   = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);

    // Create a workbook session (required for write operations)
    const std::string create_session_url = GraphExcelUrlBuilder::BuildCreateSessionUrl(wb_url);
    const std::string session_body = "{\"persistChanges\":true}";
    std::map<std::string, std::string> prefer_header = {{"Prefer", "respond-async"}};
    const std::string session_response = graph_client.PostWithHeaders(create_session_url, session_body, prefer_header);

    std::string session_id;
    {
        duckdb_yyjson::yyjson_doc *doc = duckdb_yyjson::yyjson_read(session_response.c_str(), session_response.size(), 0);
        if (doc) {
            duckdb_yyjson::yyjson_val *root = duckdb_yyjson::yyjson_doc_get_root(doc);
            duckdb_yyjson::yyjson_val *monitor_val = duckdb_yyjson::yyjson_obj_get(root, "statusMonitorResource");
            duckdb_yyjson::yyjson_val *id_val = duckdb_yyjson::yyjson_obj_get(root, "id");
            if (monitor_val && duckdb_yyjson::yyjson_is_str(monitor_val)) {
                const std::string monitor_url = duckdb_yyjson::yyjson_get_str(monitor_val);
                duckdb_yyjson::yyjson_doc_free(doc);
                for (int poll = 0; poll < 30 && session_id.empty(); poll++) {
                    session_id = ExtractSessionId(graph_client.Get(monitor_url));
                }
            } else if (id_val && duckdb_yyjson::yyjson_is_str(id_val)) {
                session_id = duckdb_yyjson::yyjson_get_str(id_val);
                duckdb_yyjson::yyjson_doc_free(doc);
            } else {
                duckdb_yyjson::yyjson_doc_free(doc);
            }
        }
    }

    if (session_id.empty()) {
        throw duckdb::IOException("Failed to create Excel workbook session for file: " + file_path);
    }

    const std::map<std::string, std::string> session_headers = {{"workbook-session-id", session_id}};
    const std::string close_session_url = GraphExcelUrlBuilder::BuildCloseSessionUrl(wb_url);
    struct SessionGuard {
        GraphClient &client;
        const std::string &close_url;
        const std::map<std::string, std::string> &headers;
        ~SessionGuard() noexcept {
            try { client.PostWithHeaders(close_url, "{}", headers); } catch (...) {}
        }
    } guard{graph_client, close_session_url, session_headers};

    // Fetch all rows to find matching indices (no $top → follow nextLink pages)
    const std::string rows_url = GraphExcelUrlBuilder::BuildTableRowsUrl(wb_url, table_name, 0);
    const std::string rows_json = graph_client.GetAllPagesMerged(rows_url);

    std::vector<idx_t> matching_indices;
    {
        duckdb_yyjson::yyjson_doc *doc = duckdb_yyjson::yyjson_read(rows_json.c_str(), rows_json.size(), 0);
        if (doc) {
            duckdb_yyjson::yyjson_val *root = duckdb_yyjson::yyjson_doc_get_root(doc);
            duckdb_yyjson::yyjson_val *value_arr = duckdb_yyjson::yyjson_obj_get(root, "value");
            if (value_arr && duckdb_yyjson::yyjson_is_arr(value_arr)) {
                duckdb_yyjson::yyjson_arr_iter row_iter;
                duckdb_yyjson::yyjson_arr_iter_init(value_arr, &row_iter);
                duckdb_yyjson::yyjson_val *row_obj = nullptr;
                while ((row_obj = duckdb_yyjson::yyjson_arr_iter_next(&row_iter)) != nullptr) {
                    duckdb_yyjson::yyjson_val *idx_val = duckdb_yyjson::yyjson_obj_get(row_obj, "index");
                    duckdb_yyjson::yyjson_val *vals_arr = duckdb_yyjson::yyjson_obj_get(row_obj, "values");
                    if (!idx_val || !vals_arr) { continue; }
                    // values is [[v0, v1, ...]] — the outer array has one element (the row)
                    duckdb_yyjson::yyjson_val *inner_row = duckdb_yyjson::yyjson_arr_get_first(vals_arr);
                    if (!inner_row || !duckdb_yyjson::yyjson_is_arr(inner_row)) { continue; }
                    duckdb_yyjson::yyjson_val *cell = duckdb_yyjson::yyjson_arr_get(inner_row, col_index);
                    if (!cell) { continue; }
                    // Cells may be string or number; compare as string
                    std::string cell_str;
                    if (duckdb_yyjson::yyjson_is_str(cell)) {
                        cell_str = duckdb_yyjson::yyjson_get_str(cell);
                    } else if (duckdb_yyjson::yyjson_is_num(cell)) {
                        cell_str = std::to_string(duckdb_yyjson::yyjson_get_real(cell));
                    }
                    if (cell_str == col_value) {
                        matching_indices.push_back(static_cast<idx_t>(duckdb_yyjson::yyjson_get_sint(idx_val)));
                    }
                }
            }
            duckdb_yyjson::yyjson_doc_free(doc);
        }
    }

    // Delete from highest index down to avoid shifting
    std::sort(matching_indices.rbegin(), matching_indices.rend());
    for (idx_t row_idx : matching_indices) {
        const std::string del_url = GraphExcelUrlBuilder::BuildTableRowDeleteUrl(wb_url, table_name, row_idx);
        graph_client.DeleteWithHeaders(del_url, session_headers);
        ERPL_TRACE_DEBUG("GRAPH_EXCEL", "Deleted row at index " + std::to_string(row_idx));
    }

    return matching_indices.size();
}

} // namespace erpl_web
