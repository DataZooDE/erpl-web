#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"

#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "erpl_web_extension.hpp"
#include "erpl_web_functions.hpp"

namespace duckdb {



static void LoadInternal(DatabaseInstance &instance) 
{
    ExtensionUtil::RegisterType(instance, "HTTP_HEADER", erpl_web::CreateHttpHeaderType());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateHttpGetFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateHttpPostFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateHttpPutFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateHttpPatchFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateHttpDeleteFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateHttpHeadFunction());
}

void ErplWebExtension::Load(DuckDB &db) 
{
	LoadInternal(*db.instance);
}
std::string ErplWebExtension::Name() 
{
	return "erpl_web";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void erpl_web_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::ErplWebExtension>();
}

DUCKDB_EXTENSION_API const char *erpl_web_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
