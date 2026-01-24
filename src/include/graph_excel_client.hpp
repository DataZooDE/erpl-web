#pragma once

#include "http_client.hpp"
#include <string>
#include <memory>
#include <vector>

namespace erpl_web {

// URL builder for Microsoft Graph Excel API endpoints
class GraphExcelUrlBuilder {
public:
    // Base Graph API URL
    static std::string GetBaseUrl();

    // Build URL for drive items in user's OneDrive
    // /me/drive/items/{item-id}
    static std::string BuildDriveItemUrl(const std::string &item_id);

    // Build URL for drive item by path in user's OneDrive
    // /me/drive/root:/{path}:
    static std::string BuildDriveItemByPathUrl(const std::string &path);

    // Build URL for drive items in a SharePoint site
    // /sites/{site-id}/drive/items/{item-id}
    static std::string BuildSiteDriveItemUrl(const std::string &site_id, const std::string &item_id);

    // Build URL for workbook (Excel file)
    // appends /workbook to item URL
    static std::string BuildWorkbookUrl(const std::string &item_url);

    // Build URL for Excel tables in a workbook
    // /workbook/tables
    static std::string BuildTablesUrl(const std::string &workbook_url);

    // Build URL for a specific table
    // /workbook/tables/{table-name}
    static std::string BuildTableUrl(const std::string &workbook_url, const std::string &table_name);

    // Build URL for table rows
    // /workbook/tables/{table-name}/rows
    static std::string BuildTableRowsUrl(const std::string &workbook_url, const std::string &table_name);

    // Build URL for worksheets
    // /workbook/worksheets
    static std::string BuildWorksheetsUrl(const std::string &workbook_url);

    // Build URL for a specific worksheet
    // /workbook/worksheets/{sheet-name}
    static std::string BuildWorksheetUrl(const std::string &workbook_url, const std::string &sheet_name);

    // Build URL for used range in a worksheet
    // /workbook/worksheets/{sheet-name}/usedRange
    static std::string BuildUsedRangeUrl(const std::string &workbook_url, const std::string &sheet_name);

    // Build URL for a specific range in a worksheet
    // /workbook/worksheets/{sheet-name}/range(address='{range}')
    static std::string BuildRangeUrl(const std::string &workbook_url, const std::string &sheet_name, const std::string &range);

    // Build URL for listing files in OneDrive root
    // /me/drive/root/children
    static std::string BuildDriveRootChildrenUrl();

    // Build URL for listing files in a folder
    // /me/drive/items/{folder-id}/children
    static std::string BuildDriveFolderChildrenUrl(const std::string &folder_id);

    // Build URL for listing files in a SharePoint site drive
    // /sites/{site-id}/drive/root/children
    static std::string BuildSiteDriveRootChildrenUrl(const std::string &site_id);
};

// Represents an Excel table's data
struct ExcelTableData {
    std::vector<std::string> column_names;
    std::vector<std::vector<std::string>> rows;
};

// Represents an Excel range's data
struct ExcelRangeData {
    std::string address;
    std::vector<std::vector<std::string>> values;
    int row_count;
    int column_count;
};

// Client for Microsoft Graph Excel API operations
class GraphExcelClient {
public:
    GraphExcelClient(std::shared_ptr<HttpAuthParams> auth_params);

    // List files in OneDrive or SharePoint
    std::string ListDriveFiles(const std::string &folder_path = "");
    std::string ListSiteFiles(const std::string &site_id, const std::string &folder_path = "");

    // Get table data from an Excel workbook
    std::string GetTableRows(const std::string &item_id, const std::string &table_name);
    std::string GetTableRowsByPath(const std::string &file_path, const std::string &table_name);

    // Get worksheet data
    std::string GetUsedRange(const std::string &item_id, const std::string &sheet_name);
    std::string GetUsedRangeByPath(const std::string &file_path, const std::string &sheet_name);

    // Get specific range data
    std::string GetRange(const std::string &item_id, const std::string &sheet_name, const std::string &range);
    std::string GetRangeByPath(const std::string &file_path, const std::string &sheet_name, const std::string &range);

    // List tables in a workbook
    std::string ListTables(const std::string &item_id);
    std::string ListTablesByPath(const std::string &file_path);

    // List worksheets in a workbook
    std::string ListWorksheets(const std::string &item_id);
    std::string ListWorksheetsByPath(const std::string &file_path);

private:
    std::shared_ptr<HttpAuthParams> auth_params;
    std::shared_ptr<HttpClient> http_client;

    std::string DoGraphGet(const std::string &url);
};

} // namespace erpl_web
