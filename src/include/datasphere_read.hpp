#pragma once

#include "duckdb/function/function_set.hpp"

namespace erpl_web {

// Function declaration for datasphere read relational table function
duckdb::TableFunctionSet CreateDatasphereReadRelationalFunction();

// Function declaration for datasphere read analytical table function
duckdb::TableFunctionSet CreateDatasphereReadAnalyticalFunction();

} // namespace erpl_web
