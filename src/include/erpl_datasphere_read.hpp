#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "erpl_datasphere_client.hpp"
#include "erpl_odata_client.hpp"
#include <memory>
#include <string>
#include <map>
#include <vector>

namespace erpl_web {

// Function declaration for datasphere read relational table function
duckdb::TableFunctionSet CreateDatasphereReadRelationalFunction();

} // namespace erpl_web
