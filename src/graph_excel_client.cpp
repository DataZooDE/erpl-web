#include "graph_excel_client.hpp"
#include "graph_client.hpp"
#include "tracing.hpp"
#include "yyjson.hpp"

#include <algorithm>
#include <map>
#include <stdexcept>
#include <thread>
#include "odata_url_helpers.hpp"

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

        // All three come out of a Graph RESPONSE BODY. yyjson_get_str returns NULL for any
        // value that is not a string, and assigning NULL to a std::string is strlen(nullptr)
        // - the same shape guarded in the Delta Sharing parsers. A sharePointIds object
        // carrying a null or numeric member took the process down here.
        if (site_url_val && duckdb_yyjson::yyjson_is_str(site_url_val) &&
            site_guid_val && duckdb_yyjson::yyjson_is_str(site_guid_val) &&
            web_guid_val && duckdb_yyjson::yyjson_is_str(web_guid_val)) {
            std::string site_url  = duckdb_yyjson::yyjson_get_str(site_url_val);
            std::string site_guid = duckdb_yyjson::yyjson_get_str(site_guid_val);
            std::string web_guid  = duckdb_yyjson::yyjson_get_str(web_guid_val);

            // Extract hostname from siteUrl (e.g. "https://tenant.sharepoint.com/sites/team" → "tenant.sharepoint.com")
            // Shared predicate and strip, so an uppercase scheme is handled like any other.
            const std::string hostname =
                LooksLikeAbsoluteHttpUrl(site_url) ? HostOfAbsoluteHttpUrl(site_url) : site_url;

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

SessionPollPolicy::SessionPollPolicy()
    : sleep_for([](std::chrono::milliseconds duration) { std::this_thread::sleep_for(duration); }),
      now([] { return std::chrono::steady_clock::now(); }) {
}

std::string PollForSessionId(const std::function<std::string()> &fetch,
                             const std::function<std::string(const std::string &)> &extract,
                             const SessionPollPolicy &policy) {
    const auto deadline = policy.now() + policy.budget;

    while (true) {
        const auto id = extract(fetch());
        if (!id.empty()) {
            return id;
        }
        if (policy.now() >= deadline) {
            return std::string();
        }
        policy.sleep_for(policy.interval);
    }
}

// The session id from a body that IS a workbook session, or empty while a long-running
// operation is still working. Throws when the operation reports failure.
//
// The status matters. Graph answers a status-monitor poll with an OPERATION resource -
// {"id":"<operationId>","status":"running",...} - whose "id" belongs to the operation, not
// to the session. Reading "id" unconditionally therefore accepted the very first poll,
// never engaged the time budget, and then sent an operation id as workbook-session-id on
// every subsequent write, which Graph rejects. So "has an id" is not the readiness
// predicate; "is not still running" is.
std::string ExtractWorkbookSessionId(const std::string &json_body) {
    duckdb_yyjson::yyjson_doc *doc = duckdb_yyjson::yyjson_read(json_body.c_str(), json_body.size(), 0);
    if (!doc) { return ""; }
    duckdb_yyjson::yyjson_val *root = duckdb_yyjson::yyjson_doc_get_root(doc);

    std::string status;
    if (auto *status_val = duckdb_yyjson::yyjson_obj_get(root, "status")) {
        if (duckdb_yyjson::yyjson_is_str(status_val)) {
            status = duckdb_yyjson::yyjson_get_str(status_val);
        }
    }

    std::string session_id;
    if (auto *id_val = duckdb_yyjson::yyjson_obj_get(root, "id")) {
        if (duckdb_yyjson::yyjson_is_str(id_val)) {
            session_id = duckdb_yyjson::yyjson_get_str(id_val);
        }
    }
    duckdb_yyjson::yyjson_doc_free(doc);

    const auto lowered = duckdb::StringUtil::Lower(status);
    if (lowered == "failed" || lowered == "cancelled" || lowered == "canceled") {
        throw duckdb::IOException(
            "Microsoft Graph reported the workbook session operation as '" + status + "'.");
    }
    if (lowered == "notstarted" || lowered == "running" || lowered == "inprogress") {
        return "";  // keep waiting; the id here belongs to the operation
    }

    // No status at all is the immediate (201) case: the body is the session itself.
    return session_id;
}

// Opens a workbook session, waiting out the long-running (202) form.
//
// One definition for both write paths. It was a ~20-line verbatim duplicate, and the two
// copies had already started to diverge - every fix here had to be made twice, and the
// second copy was the one a review found still calling the unguarded Get().
static std::string CreateWorkbookSession(GraphClient &graph_client, const std::string &wb_url,
                                         const std::string &file_path,
                                         const SessionPollPolicy &policy) {
    const std::string session_url = GraphExcelUrlBuilder::BuildCreateSessionUrl(wb_url);
    const std::string session_body = "{\"persistChanges\":true}";

    // Prefer long-running session creation to avoid 504 timeouts on large workbooks
    std::map<std::string, std::string> prefer_header = {{"Prefer", "respond-async"}};
    const std::string session_response =
        graph_client.PostWithHeaders(session_url, session_body, prefer_header);

    std::string monitor_url;
    std::string immediate_id;
    if (auto *doc = duckdb_yyjson::yyjson_read(session_response.c_str(), session_response.size(), 0)) {
        auto *root = duckdb_yyjson::yyjson_doc_get_root(doc);
        if (auto *monitor_val = duckdb_yyjson::yyjson_obj_get(root, "statusMonitorResource")) {
            if (duckdb_yyjson::yyjson_is_str(monitor_val)) {
                monitor_url = duckdb_yyjson::yyjson_get_str(monitor_val);
            }
        }
        if (auto *id_val = duckdb_yyjson::yyjson_obj_get(root, "id")) {
            if (duckdb_yyjson::yyjson_is_str(id_val)) {
                immediate_id = duckdb_yyjson::yyjson_get_str(id_val);
            }
        }
        duckdb_yyjson::yyjson_doc_free(doc);
    }

    if (monitor_url.empty()) {
        // Immediate (201): the session id is in the response.
        if (immediate_id.empty()) {
            throw duckdb::IOException(
                "Microsoft Graph returned neither a session id nor a status monitor when opening "
                "a workbook session for: " + file_path);
        }
        return immediate_id;
    }

    // The monitor URL comes out of the createSession RESPONSE BODY, so it gets the same
    // treatment as an @odata.nextLink: the bearer token follows it only when it names the
    // origin we opened the session against (GitHub #205). That decision is made ONCE here
    // rather than per request - a foreign monitor can never hand back our session id, so
    // polling it is pointless, and doing so repeatedly just aims a burst of unauthenticated
    // requests at a host the service chose (GitHub #208).
    if (!GraphClient::IsServerSuppliedUrlTrusted(monitor_url, session_url)) {
        throw duckdb::IOException(
            "Microsoft Graph returned a workbook session status monitor on a different origin "
            "than the session itself (" + monitor_url + "); refusing to poll it.");
    }

    const auto session_id = PollForSessionId(
        [&] { return graph_client.GetServerSuppliedUrl(monitor_url, session_url); },
        [](const std::string &body) { return ExtractWorkbookSessionId(body); }, policy);

    if (session_id.empty()) {
        // Distinct from every other failure here: the operation was still running when we
        // stopped waiting. That is a timeout the caller may retry with a longer budget, not
        // a malformed response, a refused monitor, or a failed operation - all of which
        // used to surface as the same "Failed to create Excel workbook session" line.
        throw duckdb::IOException(
            "Timed out after " + std::to_string(policy.budget.count()) +
            "ms waiting for Microsoft Graph to open a workbook session for: " + file_path +
            ". The session was still being created; retry, or allow more time.");
    }
    return session_id;
}


idx_t GraphExcelClient::AddTableRows(const std::string &file_path, const std::string &table_name,
                                      const std::string &rows_json, const std::string &drive_id)
{
    GraphClient graph_client(auth_params, "GRAPH_EXCEL");

    const std::string item_url  = ResolveWorkbookItemUrl(file_path, drive_id);
    const std::string wb_url    = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);

    // Create a persistent workbook session (required for write operations per Graph best practices)
    const std::string session_id =
        CreateWorkbookSession(graph_client, wb_url, file_path, SessionPollPolicy{});

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
    const std::string session_id =
        CreateWorkbookSession(graph_client, wb_url, file_path, SessionPollPolicy{});

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
