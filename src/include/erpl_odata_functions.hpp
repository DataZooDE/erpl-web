#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "erpl_odata_read_functions.hpp"

namespace erpl_web {

// This header provides access to OData functions and bind data classes
// It's a convenience header that includes the necessary OData functionality

using ODataReadBindData = erpl_web::ODataReadBindData;

} // namespace erpl_web
