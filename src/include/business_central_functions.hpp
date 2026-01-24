#pragma once

#include "duckdb/function/table_function.hpp"

namespace erpl_web {

// Discovery functions
duckdb::TableFunctionSet CreateBcShowCompaniesFunction();
duckdb::TableFunctionSet CreateBcShowEntitiesFunction();
duckdb::TableFunctionSet CreateBcDescribeFunction();

// Data reading function
duckdb::TableFunctionSet CreateBcReadFunction();

} // namespace erpl_web
