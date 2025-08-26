#define DUCKDB_EXTENSION_MAIN

// #include "duckdb.hpp"

#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/function/pragma_function.hpp"

#include "erpl_web_extension.hpp"
#include "erpl_web_functions.hpp"
#include "erpl_secret_functions.hpp"
#include "erpl_odata_attach_functions.hpp"
#include "erpl_odata_read_functions.hpp"
#include "erpl_odata_storage.hpp"
#include "erpl_datasphere_catalog.hpp"
#include "erpl_datasphere_read.hpp"
#include "erpl_datasphere_secret.hpp"

#include "telemetry.hpp"
#include "erpl_tracing.hpp"

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

// Tracing configuration callbacks
static void OnTraceEnabled(ClientContext &context, SetScope scope, Value &parameter)
{
    auto enabled = parameter.GetValue<bool>();
    erpl_web::ErplTracer::Instance().SetEnabled(enabled);
}

static void OnTraceLevel(ClientContext &context, SetScope scope, Value &parameter)
{
    auto level_str = parameter.GetValue<string>();
    auto level_str_upper = StringUtil::Upper(level_str);
    
    erpl_web::TraceLevel level = erpl_web::TraceLevel::INFO;
    
    if (level_str_upper == "NONE") level = erpl_web::TraceLevel::NONE;
    else if (level_str_upper == "ERROR") level = erpl_web::TraceLevel::ERROR;
    else if (level_str_upper == "WARN") level = erpl_web::TraceLevel::WARN;
    else if (level_str_upper == "INFO") level = erpl_web::TraceLevel::INFO;
    else if (level_str_upper == "DEBUG") level = erpl_web::TraceLevel::DEBUG_LEVEL;
    else if (level_str_upper == "TRACE") level = erpl_web::TraceLevel::TRACE;
    else {
        throw BinderException("Invalid trace level: " + level_str + ". Valid levels are: NONE, ERROR, WARN, INFO, DEBUG, TRACE");
    }
    
    erpl_web::ErplTracer::Instance().SetLevel(level);
}

static void OnTraceOutput(ClientContext &context, SetScope scope, Value &parameter)
{
    auto output = parameter.GetValue<string>();
    auto output_upper = StringUtil::Upper(output);
    
    if (output_upper != "CONSOLE" && output_upper != "FILE" && output_upper != "BOTH") {
        throw BinderException("Invalid trace output: " + output + ". Valid outputs are: console, file, both");
    }
    
    // Convert to lowercase for tracer internal use, but don't modify the parameter
    auto output_lower = StringUtil::Lower(output);
    erpl_web::ErplTracer::Instance().SetOutputMode(output_lower);
}

static void OnTraceFilePath(ClientContext &context, SetScope scope, Value &parameter)
{
    auto file_path = parameter.GetValue<string>();
    erpl_web::ErplTracer::Instance().SetTraceDirectory(file_path);
}

static void OnTraceMaxFileSize(ClientContext &context, SetScope scope, Value &parameter)
{
    auto max_size = parameter.GetValue<int64_t>();
    if (max_size < 0) {
        throw BinderException("Trace max file size must be non-negative");
    }
    erpl_web::ErplTracer::Instance().SetMaxFileSize(max_size);
}

static void OnTraceRotation(ClientContext &context, SetScope scope, Value &parameter)
{
    auto rotation = parameter.GetValue<bool>();
    erpl_web::ErplTracer::Instance().SetRotation(rotation);
}

// Pragma function to enable/disable tracing
static string EnableTracingPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
    if (parameters.values.empty()) {
        throw BinderException("erpl_trace_enable pragma requires a boolean parameter");
    }
    
    auto enabled = parameters.values[0].GetValue<bool>();
    erpl_web::ErplTracer::Instance().SetEnabled(enabled);
    
    stringstream result;
    result << "Tracing " << (enabled ? "enabled" : "disabled");
    return result.str();
}

// Pragma function to set trace level
static string SetTraceLevelPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
    if (parameters.values.empty()) {
        throw BinderException("erpl_trace_level pragma requires a string parameter");
    }
    
    auto level_str = parameters.values[0].GetValue<string>();
    StringUtil::Upper(level_str);
    
    erpl_web::TraceLevel level = erpl_web::TraceLevel::INFO;
    
    if (level_str == "NONE") level = erpl_web::TraceLevel::NONE;
    else if (level_str == "ERROR") level = erpl_web::TraceLevel::ERROR;
    else if (level_str == "WARN") level = erpl_web::TraceLevel::WARN;
    else if (level_str == "INFO") level = erpl_web::TraceLevel::INFO;
    else if (level_str == "DEBUG") level = erpl_web::TraceLevel::DEBUG_LEVEL;
    else if (level_str == "TRACE") level = erpl_web::TraceLevel::TRACE;
    else {
        throw BinderException("Invalid trace level: " + level_str + ". Valid levels are: NONE, ERROR, WARN, INFO, DEBUG, TRACE");
    }
    
    erpl_web::ErplTracer::Instance().SetLevel(level);
    
    stringstream result;
    result << "Trace level set to: " << level_str;
    return result.str();
}

// Pragma function to set trace directory
static string SetTraceDirectoryPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
    if (parameters.values.empty()) {
        throw BinderException("erpl_trace_directory pragma requires a string parameter");
    }
    
    auto directory = parameters.values[0].GetValue<string>();
    erpl_web::ErplTracer::Instance().SetTraceDirectory(directory);
    
    stringstream result;
    result << "Trace directory set to: " << directory;
    return result.str();
}

// Pragma function to get current tracing status
static string GetTracingStatusPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
    auto &tracer = erpl_web::ErplTracer::Instance();
    
    stringstream result;
    result << "Tracing Status:\n";
    result << "  Enabled: " << (tracer.IsEnabled() ? "true" : "false") << "\n";
    result << "  Level: " << static_cast<int>(tracer.GetLevel()) << "\n";
    result << "  Directory: " << "."; // Note: We don't have a getter for directory in the current interface
    
    return result.str();
}

static void RegisterConfiguration(DatabaseInstance &instance)
{
    auto &config = DBConfig::GetConfig(instance);

    config.AddExtensionOption("erpl_telemetry_enabled", "Enable ERPL telemetry, see https://erpl.io/telemetry for details.", 
                                  LogicalTypeId::BOOLEAN, Value(true), OnTelemetryEnabled);
    config.AddExtensionOption("erpl_telemetry_key", "Telemetry key, see https://erpl.io/telemetry for details.", LogicalTypeId::VARCHAR, 
                                Value("phc_t3wwRLtpyEmLHYaZCSszG0MqVr74J6wnCrj9D41zk2t"), OnAPIKey);
    
    // Tracing configuration options
    config.AddExtensionOption("erpl_trace_enabled", "Enable ERPL Web extension tracing functionality", 
                                  LogicalTypeId::BOOLEAN, Value(false), OnTraceEnabled);
    config.AddExtensionOption("erpl_trace_level", "Set ERPL Web extension trace level (TRACE, DEBUG, INFO, WARN, ERROR)", 
                                  LogicalTypeId::VARCHAR, Value("INFO"), OnTraceLevel);
    config.AddExtensionOption("erpl_trace_output", "Set ERPL Web extension trace output (console, file, both)", 
                                  LogicalTypeId::VARCHAR, Value("console"), OnTraceOutput);
    config.AddExtensionOption("erpl_trace_file_path", "Set ERPL Web extension trace file path", 
                                  LogicalTypeId::VARCHAR, Value(""), OnTraceFilePath);
    config.AddExtensionOption("erpl_trace_max_file_size", "Set ERPL Web extension trace file max size in bytes", 
                                  LogicalTypeId::BIGINT, Value(10485760), OnTraceMaxFileSize);
    config.AddExtensionOption("erpl_trace_rotation", "Enable ERPL Web extension trace file rotation", 
                                  LogicalTypeId::BOOLEAN, Value(true), OnTraceRotation);
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

static void RegisterDatasphereFunctions(DatabaseInstance &instance)
{
    // Register catalog discovery functions
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateDatasphereShowSpacesFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateDatasphereShowAssetsFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateDatasphereDescribeSpaceFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateDatasphereDescribeAssetFunction());
    
    // Register asset consumption functions
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateDatasphereReadRelationalFunction());
    ExtensionUtil::RegisterFunction(instance, erpl_web::CreateDatasphereReadAnalyticalFunction());
    
    // Register Datasphere secret management functions
    erpl_web::CreateDatasphereSecretFunctions::Register(instance);
}

static void RegisterTracingPragmas(DatabaseInstance &instance)
{
    // Register tracing pragma functions
    ExtensionUtil::RegisterFunction(instance, PragmaFunctionSet(PragmaFunction::PragmaCall(
        "erpl_trace_enable", EnableTracingPragmaFunction, {LogicalType::BOOLEAN})));
    
    ExtensionUtil::RegisterFunction(instance, PragmaFunctionSet(PragmaFunction::PragmaCall(
        "erpl_trace_level", SetTraceLevelPragmaFunction, {LogicalType::VARCHAR})));
    
    ExtensionUtil::RegisterFunction(instance, PragmaFunctionSet(PragmaFunction::PragmaCall(
        "erpl_trace_directory", SetTraceDirectoryPragmaFunction, {LogicalType::VARCHAR})));
    
    ExtensionUtil::RegisterFunction(instance, PragmaFunctionSet(PragmaFunction::PragmaCall(
        "erpl_trace_status", GetTracingStatusPragmaFunction, {})));
}

void ErplWebExtension::Load(DuckDB &db) 
{
    PostHogTelemetry::Instance().CaptureExtensionLoad("erpl_web");

	RegisterConfiguration(*db.instance);
    RegisterWebFunctions(*db.instance);
    RegisterODataFunctions(*db.instance);
    RegisterDatasphereFunctions(*db.instance);
    RegisterTracingPragmas(*db.instance);
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
