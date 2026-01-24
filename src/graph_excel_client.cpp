#include "graph_excel_client.hpp"
#include "tracing.hpp"

namespace erpl_web {

// URL Builder implementation
std::string GraphExcelUrlBuilder::GetBaseUrl() {
    return "https://graph.microsoft.com/v1.0";
}

std::string GraphExcelUrlBuilder::BuildDriveItemUrl(const std::string &item_id) {
    return GetBaseUrl() + "/me/drive/items/" + item_id;
}

std::string GraphExcelUrlBuilder::BuildDriveItemByPathUrl(const std::string &path) {
    // Ensure path doesn't start with /
    std::string clean_path = path;
    if (!clean_path.empty() && clean_path[0] == '/') {
        clean_path = clean_path.substr(1);
    }
    return GetBaseUrl() + "/me/drive/root:/" + clean_path + ":";
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
    return workbook_url + "/tables/" + table_name;
}

std::string GraphExcelUrlBuilder::BuildTableRowsUrl(const std::string &workbook_url, const std::string &table_name) {
    return workbook_url + "/tables/" + table_name + "/rows";
}

std::string GraphExcelUrlBuilder::BuildWorksheetsUrl(const std::string &workbook_url) {
    return workbook_url + "/worksheets";
}

std::string GraphExcelUrlBuilder::BuildWorksheetUrl(const std::string &workbook_url, const std::string &sheet_name) {
    return workbook_url + "/worksheets/" + sheet_name;
}

std::string GraphExcelUrlBuilder::BuildUsedRangeUrl(const std::string &workbook_url, const std::string &sheet_name) {
    return workbook_url + "/worksheets/" + sheet_name + "/usedRange";
}

std::string GraphExcelUrlBuilder::BuildRangeUrl(const std::string &workbook_url, const std::string &sheet_name, const std::string &range) {
    return workbook_url + "/worksheets/" + sheet_name + "/range(address='" + range + "')";
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

// GraphExcelClient implementation
GraphExcelClient::GraphExcelClient(std::shared_ptr<HttpAuthParams> auth_params)
    : auth_params(auth_params) {
    HttpParams http_params;
    http_params.url_encode = false;
    http_client = std::make_shared<HttpClient>(http_params);
}

std::string GraphExcelClient::DoGraphGet(const std::string &url) {
    ERPL_TRACE_DEBUG("GRAPH_EXCEL", "GET request to: " + url);

    HttpUrl http_url(url);
    HttpRequest request(HttpMethod::GET, http_url);

    if (auth_params) {
        request.AuthHeadersFromParams(*auth_params);
    }

    request.headers["Accept"] = "application/json";

    auto response = http_client->SendRequest(request);

    if (!response || response->Code() != 200) {
        std::string error_msg = "Graph API request failed";
        if (response) {
            error_msg += " (HTTP " + std::to_string(response->Code()) + ")";
            if (!response->Content().empty()) {
                error_msg += ": " + response->Content().substr(0, 500);
            }
        }
        ERPL_TRACE_ERROR("GRAPH_EXCEL", error_msg);
        throw std::runtime_error(error_msg);
    }

    ERPL_TRACE_DEBUG("GRAPH_EXCEL", "Response received: " + std::to_string(response->Content().length()) + " bytes");
    return response->Content();
}

std::string GraphExcelClient::ListDriveFiles(const std::string &folder_path) {
    std::string url;
    if (folder_path.empty()) {
        url = GraphExcelUrlBuilder::BuildDriveRootChildrenUrl();
    } else {
        url = GraphExcelUrlBuilder::BuildDriveFolderChildrenUrl(folder_path);
    }
    return DoGraphGet(url);
}

std::string GraphExcelClient::ListSiteFiles(const std::string &site_id, const std::string &folder_path) {
    std::string url = GraphExcelUrlBuilder::BuildSiteDriveRootChildrenUrl(site_id);
    return DoGraphGet(url);
}

std::string GraphExcelClient::GetTableRows(const std::string &item_id, const std::string &table_name) {
    auto item_url = GraphExcelUrlBuilder::BuildDriveItemUrl(item_id);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto rows_url = GraphExcelUrlBuilder::BuildTableRowsUrl(workbook_url, table_name);
    return DoGraphGet(rows_url);
}

std::string GraphExcelClient::GetTableRowsByPath(const std::string &file_path, const std::string &table_name) {
    auto item_url = GraphExcelUrlBuilder::BuildDriveItemByPathUrl(file_path);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto rows_url = GraphExcelUrlBuilder::BuildTableRowsUrl(workbook_url, table_name);
    return DoGraphGet(rows_url);
}

std::string GraphExcelClient::GetUsedRange(const std::string &item_id, const std::string &sheet_name) {
    auto item_url = GraphExcelUrlBuilder::BuildDriveItemUrl(item_id);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto range_url = GraphExcelUrlBuilder::BuildUsedRangeUrl(workbook_url, sheet_name);
    return DoGraphGet(range_url);
}

std::string GraphExcelClient::GetUsedRangeByPath(const std::string &file_path, const std::string &sheet_name) {
    auto item_url = GraphExcelUrlBuilder::BuildDriveItemByPathUrl(file_path);
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

std::string GraphExcelClient::GetRangeByPath(const std::string &file_path, const std::string &sheet_name, const std::string &range) {
    auto item_url = GraphExcelUrlBuilder::BuildDriveItemByPathUrl(file_path);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto range_url = GraphExcelUrlBuilder::BuildRangeUrl(workbook_url, sheet_name, range);
    return DoGraphGet(range_url);
}

std::string GraphExcelClient::ListTables(const std::string &item_id) {
    auto item_url = GraphExcelUrlBuilder::BuildDriveItemUrl(item_id);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto tables_url = GraphExcelUrlBuilder::BuildTablesUrl(workbook_url);
    return DoGraphGet(tables_url);
}

std::string GraphExcelClient::ListTablesByPath(const std::string &file_path) {
    auto item_url = GraphExcelUrlBuilder::BuildDriveItemByPathUrl(file_path);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto tables_url = GraphExcelUrlBuilder::BuildTablesUrl(workbook_url);
    return DoGraphGet(tables_url);
}

std::string GraphExcelClient::ListWorksheets(const std::string &item_id) {
    auto item_url = GraphExcelUrlBuilder::BuildDriveItemUrl(item_id);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto worksheets_url = GraphExcelUrlBuilder::BuildWorksheetsUrl(workbook_url);
    return DoGraphGet(worksheets_url);
}

std::string GraphExcelClient::ListWorksheetsByPath(const std::string &file_path) {
    auto item_url = GraphExcelUrlBuilder::BuildDriveItemByPathUrl(file_path);
    auto workbook_url = GraphExcelUrlBuilder::BuildWorkbookUrl(item_url);
    auto worksheets_url = GraphExcelUrlBuilder::BuildWorksheetsUrl(workbook_url);
    return DoGraphGet(worksheets_url);
}

} // namespace erpl_web
