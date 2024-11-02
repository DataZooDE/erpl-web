#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"

#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "erpl_web_extension.hpp"
#include "erpl_web_functions.hpp"
#include "erpl_secret_functions.hpp"
#include "erpl_odata_attach_functions.hpp"
#include "erpl_odata_read_functions.hpp"
#include "erpl_odata_storage.hpp"

#include "telemetry.hpp"

namespace duckdb {

static void OnTelemetryEnabled(ClientContext &context, SetScope scope, Value &parameter)
{
    auto telemetry_enabled = parameter.GetValue<bool>();
    PostHogTelemetry::Instance().SetEnabled(telemetry_enabled);
}

static void OnAPIKey(ClientContext &context, SetScope scope, Value &parameter)
{
    auto api_key = parameter.GetValue<std::string>();
    PostHogTelemetry::Instance().SetAPIKey(api_key);
}

static void RegisterConfiguration(DatabaseInstance &instance)
{
    auto &config = DBConfig::GetConfig(instance);

    config.AddExtensionOption("erpl_telemetry_enabled", "Enable ERPL telemetry, see https://erpl.io/telemetry for details.", 
                                  LogicalTypeId::BOOLEAN, Value(true), OnTelemetryEnabled);
    config.AddExtensionOption("erpl_telemetry_key", "Telemetry key, see https://erpl.io/telemetry for details.", LogicalTypeId::VARCHAR, 
                                Value("phc_t3wwRLtpyEmLHYaZCSszG0MqVr74J6wnCrj9D41zk2t"), OnAPIKey);
}

static void RegisterWebFunctions(DatabaseInstance &instance)
{
    ExtensionUtil::RegisterType(instance, "HTTP_HEADER", erpl_web::CreateHttpHeaderType());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateHttpGetFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateHttpPostFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateHttpPutFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateHttpPatchFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateHttpDeleteFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateHttpHeadFunction());

    erpl::CreateBasicSecretFunctions::Register(instance);
    erpl::CreateBearerTokenSecretFunctions::Register(instance);
}

static void RegisterODataFunctions(DatabaseInstance &instance)
{
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateODataReadFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateODataAttachFunction());

    auto &config = DBConfig::GetConfig(instance);
    config.storage_extensions["odata"] = erpl_web::CreateODataStorageExtension();

    duckdb::ExtensionInstallInfo extension_install_info;
    instance.SetExtensionLoaded("odata", extension_install_info);
}

void ErplWebExtension::Load(DuckDB &db) 
{
    PostHogTelemetry::Instance().CaptureExtensionLoad("erpl_web");

	RegisterConfiguration(*db.instance);
    RegisterWebFunctions(*db.instance);
    RegisterODataFunctions(*db.instance);
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
