#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/table_function.hpp"
#include <string>

namespace erpl_web {

class GraphSharePointAttachFunctions {
public:
    static void Register(duckdb::ExtensionLoader &loader);
};

} // namespace erpl_web
