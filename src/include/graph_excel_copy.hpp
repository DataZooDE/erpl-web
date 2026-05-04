#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace erpl_web {

// Register COPY TO function for Excel tables.
// Usage: COPY (SELECT ...) TO 'path/to/file.xlsx'
//        (FORMAT ms_excel_table, table 'TableName', drive '...', secret '...')
void RegisterExcelTableCopyFunction(duckdb::ExtensionLoader &loader);

} // namespace erpl_web
