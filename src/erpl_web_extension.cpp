#include "duckdb.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "erpl_web_extension.hpp"
#include "web_functions.hpp"
#include "secret_functions.hpp"
#include "odata_attach_functions.hpp"
#include "odata_read_functions.hpp"
#include "odata_storage.hpp"
#include "datasphere_catalog.hpp"
#include "datasphere_read.hpp"
#include "datasphere_secret.hpp"
#include "odata_odp_functions.hpp"
#include "odp_odata_read_functions.hpp"
#include "odp_pragma_functions.hpp"
#include "sac_catalog.hpp"
#include "sac_attach_functions.hpp"
#include "sac_read_functions.hpp"
#include "delta_share_scan.hpp"
#include "delta_share_storage.hpp"
#include "delta_share_catalog.hpp"
#include "telemetry.hpp"
#include "tracing.hpp"

// Windows headers may redefine macros after our tracing.hpp include
#ifdef _WIN32
#ifdef DEBUG
    #undef DEBUG
#endif
#ifdef INFO
    #undef INFO
#endif
#ifdef WARN
    #undef WARN
#endif
#ifdef ERROR
    #undef ERROR
#endif
#ifdef TRACE
    #undef TRACE
#endif
#ifdef NONE
    #undef NONE
#endif
#ifdef CONSOLE
    #undef CONSOLE
#endif
#ifdef FILE
    #undef FILE
#endif
#ifdef BOTH
    #undef BOTH
#endif
#endif

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

static erpl_web::TraceLevel StringToTraceLevel(const string &level_str) {
    auto level_str_upper = StringUtil::Upper(level_str);
    
    erpl_web::TraceLevel level = erpl_web::TraceLevel::INFO;
    
    if (level_str_upper == "NONE") {
        level = erpl_web::TraceLevel::NONE;
    } else if (level_str_upper == "ERROR") {
        level = erpl_web::TraceLevel::ERROR;
    } else if (level_str_upper == "WARN") {
        level = erpl_web::TraceLevel::WARN;
    } else if (level_str_upper == "INFO") {
        level = erpl_web::TraceLevel::INFO;
    } else if (level_str_upper == "DEBUG") {
        level = erpl_web::TraceLevel::DEBUG_LEVEL;
    } else if (level_str_upper == "TRACE") {
        level = erpl_web::TraceLevel::TRACE;
    }
    else {
        throw BinderException("Invalid trace level: " + level_str + ". Valid levels are: NONE, ERROR, WARN, INFO, DEBUG, TRACE");
    }

    return level;
}

static void OnTraceLevel(ClientContext &context, SetScope scope, Value &parameter)
{
    auto level_str = parameter.GetValue<string>();
    erpl_web::TraceLevel level = StringToTraceLevel(level_str);
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
    erpl_web::TraceLevel level = StringToTraceLevel(level_str);    
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

static void RegisterWebFunctions(ExtensionLoader &loader)
{
    loader.RegisterType("HTTP_HEADER", erpl_web::CreateHttpHeaderType());
    loader.RegisterFunction(erpl_web::CreateHttpGetFunction());
    loader.RegisterFunction(erpl_web::CreateHttpPostFunction());
    loader.RegisterFunction(erpl_web::CreateHttpPutFunction());
    loader.RegisterFunction(erpl_web::CreateHttpPatchFunction());
    loader.RegisterFunction(erpl_web::CreateHttpDeleteFunction());
    loader.RegisterFunction(erpl_web::CreateHttpHeadFunction());

    erpl::CreateBasicSecretFunctions::Register(loader);
    erpl::CreateBearerTokenSecretFunctions::Register(loader);
}

static void RegisterODataFunctions(ExtensionLoader &loader)
{
    loader.RegisterFunction(erpl_web::CreateODataReadFunction());
    loader.RegisterFunction(erpl_web::CreateODataDescribeFunction());
    loader.RegisterFunction(erpl_web::CreateODataAttachFunction());
    loader.RegisterFunction(erpl_web::CreateODataSapShowFunction());
    

    auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
    config.storage_extensions["odata"] = erpl_web::CreateODataStorageExtension();
    config.storage_extensions["delta_share"] = erpl_web::CreateDeltaShareStorageExtension();
}

static void RegisterDatasphereFunctions(ExtensionLoader &loader)
{
    // Register catalog discovery functions
    loader.RegisterFunction(erpl_web::CreateDatasphereShowSpacesFunction());
    loader.RegisterFunction(erpl_web::CreateDatasphereShowAssetsFunction());
    loader.RegisterFunction(erpl_web::CreateDatasphereDescribeSpaceFunction());
    loader.RegisterFunction(erpl_web::CreateDatasphereDescribeAssetFunction());
    
    // Register asset consumption functions
    loader.RegisterFunction(erpl_web::CreateDatasphereReadRelationalFunction());
    loader.RegisterFunction(erpl_web::CreateDatasphereReadAnalyticalFunction());
    
    // Register Datasphere secret management functions
    erpl_web::CreateDatasphereSecretFunctions::Register(loader);
}

static void RegisterOdpFunctions(ExtensionLoader &loader)
{
    loader.RegisterFunction(erpl_web::CreateOdpODataShowFunction());
    loader.RegisterFunction(erpl_web::CreateOdpODataReadFunction());
    loader.RegisterFunction(erpl_web::CreateOdpListSubscriptionsFunction());
    loader.RegisterFunction(erpl_web::CreateOdpRemoveSubscriptionFunction());
}

static void RegisterSacFunctions(ExtensionLoader &loader)
{
    // Register catalog discovery functions
    loader.RegisterFunction(erpl_web::CreateSacShowModelsFunction());
    loader.RegisterFunction(erpl_web::CreateSacShowStoriesFunction());
    loader.RegisterFunction(erpl_web::CreateSacGetModelInfoFunction());
    loader.RegisterFunction(erpl_web::CreateSacGetStoryInfoFunction());

    // Register data consumption functions
    loader.RegisterFunction(erpl_web::CreateSacReadPlanningDataFunction());
    loader.RegisterFunction(erpl_web::CreateSacReadAnalyticalFunction());
    loader.RegisterFunction(erpl_web::CreateSacReadStoryDataFunction());

    // Register SAC storage extension (handles ATTACH support)
    auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
    config.storage_extensions["sac"] = erpl_web::CreateSacStorageExtension();
}

static void RegisterDeltaShareFunctions(ExtensionLoader &loader)
{
    // Register Delta Sharing table function
    loader.RegisterFunction(erpl_web::CreateDeltaShareScanFunction());

    // Register Delta Sharing discovery functions
    loader.RegisterFunction(erpl_web::CreateDeltaShareShowSharesFunction());
    loader.RegisterFunction(erpl_web::CreateDeltaShareShowSchemasFunction());
    loader.RegisterFunction(erpl_web::CreateDeltaShareShowTablesFunction());
}

static void RegisterTracingPragmas(ExtensionLoader &loader)
{
    // Register tracing pragma functions
    loader.RegisterFunction(PragmaFunctionSet(PragmaFunction::PragmaCall(
        "erpl_trace_enable", EnableTracingPragmaFunction, {LogicalType::BOOLEAN})));
    
    loader.RegisterFunction(PragmaFunctionSet(PragmaFunction::PragmaCall(
        "erpl_trace_level", SetTraceLevelPragmaFunction, {LogicalType::VARCHAR})));
    
    loader.RegisterFunction(PragmaFunctionSet(PragmaFunction::PragmaCall(
        "erpl_trace_directory", SetTraceDirectoryPragmaFunction, {LogicalType::VARCHAR})));
    
    loader.RegisterFunction(PragmaFunctionSet(PragmaFunction::PragmaCall(
        "erpl_trace_status", GetTracingStatusPragmaFunction, {})));
}

static void LoadInternal(ExtensionLoader &loader) {
    auto &instance = loader.GetDatabaseInstance();
    PostHogTelemetry::Instance().CaptureExtensionLoad("erpl_web");

    RegisterConfiguration(instance);
    RegisterWebFunctions(loader);
    RegisterODataFunctions(loader);
    RegisterDatasphereFunctions(loader);
    RegisterSacFunctions(loader);
    RegisterOdpFunctions(loader);
    RegisterDeltaShareFunctions(loader);
    RegisterTracingPragmas(loader);
}
    
void ErplWebExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string ErplWebExtension::Name() {
    return "erpl_web";
}

std::string ErplWebExtension::Version() {
    return "be623fc";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(erpl_web, loader) {
    duckdb::ErplWebExtension::Load(loader);
}

}

