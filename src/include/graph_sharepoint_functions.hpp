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

    // graph_list_items(secret_name, site_id, list_id) - Read list items
    static duckdb::unique_ptr<duckdb::FunctionData> ListItemsBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void ListItemsScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);
};

} // namespace erpl_web
