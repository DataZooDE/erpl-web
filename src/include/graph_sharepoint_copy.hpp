#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace erpl_web {

// Register COPY TO function for SharePoint lists.
// Usage: COPY (SELECT ...) TO 'site_name_or_url'
//        (FORMAT graph_list_items, list 'ListName', secret '...')
void RegisterSharePointListCopyFunction(duckdb::ExtensionLoader &loader);

} // namespace erpl_web
