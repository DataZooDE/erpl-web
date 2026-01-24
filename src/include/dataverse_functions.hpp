#pragma once

#include "duckdb/function/table_function.hpp"

namespace erpl_web {

// Discovery functions
duckdb::TableFunctionSet CreateCrmShowEntitiesFunction();
duckdb::TableFunctionSet CreateCrmDescribeFunction();

// Data reading function
duckdb::TableFunctionSet CreateCrmReadFunction();

} // namespace erpl_web
