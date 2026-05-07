#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace erpl_web {

// Microsoft Graph SharePoint table functions
class GraphSharePointFunctions {
public:
    static void Register(duckdb::ExtensionLoader &loader);

private:
    // graph_show_sites(secret_name, search_query?) - Search/list SharePoint sites
    static duckdb::unique_ptr<duckdb::FunctionData> ShowSitesBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void ShowSitesScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_show_drives(site_id) - List drives (document libraries) in a SharePoint site
    static duckdb::unique_ptr<duckdb::FunctionData> ShowDrivesBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void ShowDrivesScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_show_lists(secret_name, site_id) - List SharePoint lists in a site
    static duckdb::unique_ptr<duckdb::FunctionData> ShowListsBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void ShowListsScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_describe_list(secret_name, site_id, list_id) - Get list schema/columns
    static duckdb::unique_ptr<duckdb::FunctionData> DescribeListBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void DescribeListScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_sharepoint_list_read(secret_name, site_id, list_id) - Read list items
    static duckdb::unique_ptr<duckdb::FunctionData> ListItemsBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void ListItemsScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_sharepoint_create_item(site, list, fields_json, secret:=...) - Create an item
    static duckdb::unique_ptr<duckdb::FunctionData> CreateItemBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void CreateItemScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_sharepoint_update_item(site, list, item_id, fields_json, secret:=...) - Update an item
    static duckdb::unique_ptr<duckdb::FunctionData> UpdateItemBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void UpdateItemScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_sharepoint_delete_item(site, list, item_id, secret:=...) - Delete an item
    static duckdb::unique_ptr<duckdb::FunctionData> DeleteItemBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void DeleteItemScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);
};

} // namespace erpl_web
