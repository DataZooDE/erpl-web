#pragma once

#include "duckdb/function/function_set.hpp"

namespace erpl_web {

// Function declaration for datasphere read relational table function
duckdb::TableFunctionSet CreateDatasphereReadRelationalFunction();

// Function declaration for datasphere read analytical table function
duckdb::TableFunctionSet CreateDatasphereReadAnalyticalFunction();


// Appends the Datasphere asset segments to a URL's PATH.
//
// Exported as a seam so the query-string case is testable: this appended to the end of the
// whole string, so "https://host/path?$top=5" became "https://host/path?$top=5/A/A", with
// the segments buried inside the query value. See GitHub #243.
void EnsureAssetSegmentPattern(std::string &url, const std::string &asset_id);

} // namespace erpl_web
