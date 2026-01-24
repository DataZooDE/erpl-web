#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace erpl_web {

// Microsoft Graph Excel table functions
class GraphExcelFunctions {
public:
    static void Register(duckdb::ExtensionLoader &loader);

private:
    // graph_list_files(secret_name, folder_path) - List files in OneDrive
    static duckdb::unique_ptr<duckdb::FunctionData> ListFilesBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void ListFilesScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_excel_tables(secret_name, file_path) - List tables in Excel workbook
    static duckdb::unique_ptr<duckdb::FunctionData> ExcelTablesBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void ExcelTablesScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_excel_worksheets(secret_name, file_path) - List worksheets in Excel workbook
    static duckdb::unique_ptr<duckdb::FunctionData> ExcelWorksheetsBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void ExcelWorksheetsScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_excel_range(secret_name, file_path, sheet_name, range?) - Read Excel range
    static duckdb::unique_ptr<duckdb::FunctionData> ExcelRangeBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void ExcelRangeScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_excel_table_data(secret_name, file_path, table_name) - Read Excel table data
    static duckdb::unique_ptr<duckdb::FunctionData> ExcelTableDataBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void ExcelTableDataScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);
};

} // namespace erpl_web
