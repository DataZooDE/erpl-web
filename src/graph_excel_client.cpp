#include "graph_excel_client.hpp"
#include "graph_client.hpp"
#include "tracing.hpp"
#include "yyjson.hpp"

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

} // namespace erpl_web
