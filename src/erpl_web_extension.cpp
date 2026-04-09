#include "duckdb.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#ifdef DUCKDB_HAS_EXTENSION_CALLBACK_MANAGER
#include "duckdb/main/extension_callback_manager.hpp"
#endif

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
#include "microsoft_entra_secret.hpp"
#include "business_central_secret.hpp"
#include "business_central_functions.hpp"
#include "dataverse_secret.hpp"
#include "dataverse_functions.hpp"
#include "graph_excel_secret.hpp"
#include "graph_excel_functions.hpp"
#include "graph_sharepoint_functions.hpp"
#include "graph_planner_functions.hpp"
#include "graph_outlook_functions.hpp"
#include "graph_entra_functions.hpp"
#include "graph_teams_functions.hpp"
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

    {
        CreateTableFunctionInfo info(erpl_web::CreateHttpGetFunction());
        FunctionDescription desc;
        desc.description = "Send an HTTP GET request and return the response as a table.";
        desc.parameter_names = {"url"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM http_get('https://httpbin.org/ip')"};
        desc.categories = {"http"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateHttpHeadFunction());
        FunctionDescription desc;
        desc.description = "Send an HTTP HEAD request and return the response headers as a table.";
        desc.parameter_names = {"url"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM http_head('https://httpbin.org/ip')"};
        desc.categories = {"http"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateHttpPostFunction());
        FunctionDescription desc1;
        desc1.description = "Send an HTTP POST request with a JSON body and return the response as a table.";
        desc1.parameter_names = {"url", "body"};
        desc1.parameter_types = {LogicalType::VARCHAR, LogicalType::JSON()};
        desc1.examples = {"SELECT * FROM http_post('https://httpbin.org/post', '{\"key\": \"value\"}'::JSON)"};
        desc1.categories = {"http"};
        info.descriptions.push_back(std::move(desc1));
        FunctionDescription desc2;
        desc2.description = "Send an HTTP POST request with raw content and return the response as a table.";
        desc2.parameter_names = {"url", "content", "content_type"};
        desc2.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc2.examples = {"SELECT * FROM http_post('https://httpbin.org/post', 'hello', 'text/plain')"};
        desc2.categories = {"http"};
        info.descriptions.push_back(std::move(desc2));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateHttpPutFunction());
        FunctionDescription desc1;
        desc1.description = "Send an HTTP PUT request with a JSON body and return the response as a table.";
        desc1.parameter_names = {"url", "body"};
        desc1.parameter_types = {LogicalType::VARCHAR, LogicalType::JSON()};
        desc1.examples = {"SELECT * FROM http_put('https://httpbin.org/put', '{\"key\": \"value\"}'::JSON)"};
        desc1.categories = {"http"};
        info.descriptions.push_back(std::move(desc1));
        FunctionDescription desc2;
        desc2.description = "Send an HTTP PUT request with raw content and return the response as a table.";
        desc2.parameter_names = {"url", "content", "content_type"};
        desc2.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc2.examples = {"SELECT * FROM http_put('https://example.com/resource', 'data', 'text/plain')"};
        desc2.categories = {"http"};
        info.descriptions.push_back(std::move(desc2));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateHttpPatchFunction());
        FunctionDescription desc1;
        desc1.description = "Send an HTTP PATCH request with a JSON body and return the response as a table.";
        desc1.parameter_names = {"url", "body"};
        desc1.parameter_types = {LogicalType::VARCHAR, LogicalType::JSON()};
        desc1.examples = {"SELECT * FROM http_patch('https://httpbin.org/patch', '{\"key\": \"value\"}'::JSON)"};
        desc1.categories = {"http"};
        info.descriptions.push_back(std::move(desc1));
        FunctionDescription desc2;
        desc2.description = "Send an HTTP PATCH request with raw content and return the response as a table.";
        desc2.parameter_names = {"url", "content", "content_type"};
        desc2.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc2.examples = {"SELECT * FROM http_patch('https://example.com/resource', 'patch', 'text/plain')"};
        desc2.categories = {"http"};
        info.descriptions.push_back(std::move(desc2));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateHttpDeleteFunction());
        FunctionDescription desc1;
        desc1.description = "Send an HTTP DELETE request with a JSON body and return the response as a table.";
        desc1.parameter_names = {"url", "body"};
        desc1.parameter_types = {LogicalType::VARCHAR, LogicalType::JSON()};
        desc1.examples = {"SELECT * FROM http_delete('https://httpbin.org/delete', '{}'::JSON)"};
        desc1.categories = {"http"};
        info.descriptions.push_back(std::move(desc1));
        FunctionDescription desc2;
        desc2.description = "Send an HTTP DELETE request with raw content and return the response as a table.";
        desc2.parameter_names = {"url", "content", "content_type"};
        desc2.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc2.examples = {"SELECT * FROM http_delete('https://example.com/resource', '', 'text/plain')"};
        desc2.categories = {"http"};
        info.descriptions.push_back(std::move(desc2));
        loader.RegisterFunction(std::move(info));
    }

    erpl::CreateBasicSecretFunctions::Register(loader);
    erpl::CreateBearerTokenSecretFunctions::Register(loader);
    erpl_web::CreateMicrosoftEntraSecretFunctions::Register(loader);
}

static void RegisterODataFunctions(ExtensionLoader &loader)
{
    {
        CreateTableFunctionInfo info(erpl_web::CreateODataReadFunction());
        FunctionDescription desc;
        desc.description = "Read data from an OData v2/v4 entity set with automatic version detection and predicate pushdown.";
        desc.parameter_names = {"url"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM odata_read('https://services.odata.org/V4/Northwind/Northwind.svc/Customers')"};
        desc.categories = {"odata"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateODataDescribeFunction());
        FunctionDescription desc;
        desc.description = "Describe the schema of an OData entity set by reading its metadata document.";
        desc.parameter_names = {"url"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM odata_describe('https://services.odata.org/V4/Northwind/Northwind.svc/Customers')"};
        desc.categories = {"odata"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateODataAttachFunction());
        FunctionDescription desc;
        desc.description = "Attach all entity sets of an OData service as views in the current DuckDB catalog.";
        desc.parameter_names = {"url"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM odata_attach('https://services.odata.org/V4/Northwind/Northwind.svc/')"};
        desc.categories = {"odata"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateODataSapShowFunction());
        FunctionDescription desc;
        desc.description = "List available SAP OData entity sets and services at a given service root URL.";
        desc.parameter_names = {"url"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM odata_sap_show('https://<host>/sap/opu/odata/sap/')"};
        desc.categories = {"odata", "sap"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    

#ifdef DUCKDB_HAS_EXTENSION_CALLBACK_MANAGER
    auto &callback_manager = ExtensionCallbackManager::Get(loader.GetDatabaseInstance());
    callback_manager.Register("odata", shared_ptr<StorageExtension>(erpl_web::CreateODataStorageExtension().release()));
    callback_manager.Register("delta_share", shared_ptr<StorageExtension>(erpl_web::CreateDeltaShareStorageExtension().release()));
#else
    auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
    config.storage_extensions["odata"] = erpl_web::CreateODataStorageExtension();
    config.storage_extensions["delta_share"] = erpl_web::CreateDeltaShareStorageExtension();
#endif
}

static void RegisterDatasphereFunctions(ExtensionLoader &loader)
{
    {
        CreateTableFunctionInfo info(erpl_web::CreateDatasphereShowSpacesFunction());
        FunctionDescription desc;
        desc.description = "List all SAP Datasphere spaces accessible with the configured credentials.";
        desc.parameter_names = {};
        desc.parameter_types = {};
        desc.examples = {"SELECT * FROM datasphere_show_spaces()"};
        desc.categories = {"sap", "datasphere"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateDatasphereShowAssetsFunction());
        FunctionDescription desc1;
        desc1.description = "List all assets in a specific SAP Datasphere space.";
        desc1.parameter_names = {"space_id"};
        desc1.parameter_types = {LogicalType::VARCHAR};
        desc1.examples = {"SELECT * FROM datasphere_show_assets('MY_SPACE')"};
        desc1.categories = {"sap", "datasphere"};
        info.descriptions.push_back(std::move(desc1));
        FunctionDescription desc2;
        desc2.description = "List all assets across all accessible SAP Datasphere spaces.";
        desc2.parameter_names = {};
        desc2.parameter_types = {};
        desc2.examples = {"SELECT * FROM datasphere_show_assets()"};
        desc2.categories = {"sap", "datasphere"};
        info.descriptions.push_back(std::move(desc2));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateDatasphereDescribeSpaceFunction());
        FunctionDescription desc;
        desc.description = "Describe the metadata and configuration of a SAP Datasphere space.";
        desc.parameter_names = {"space_id"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM datasphere_describe_space('MY_SPACE')"};
        desc.categories = {"sap", "datasphere"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateDatasphereDescribeAssetFunction());
        FunctionDescription desc;
        desc.description = "Describe the schema and metadata of a specific asset in a SAP Datasphere space.";
        desc.parameter_names = {"space_id", "asset_id"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM datasphere_describe_asset('MY_SPACE', 'MY_VIEW')"};
        desc.categories = {"sap", "datasphere"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateDatasphereReadRelationalFunction());
        FunctionDescription desc1;
        desc1.description = "Read data from a relational asset in SAP Datasphere using space and asset ID.";
        desc1.parameter_names = {"space_id", "asset_id"};
        desc1.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc1.examples = {"SELECT * FROM datasphere_read_relational('MY_SPACE', 'MY_VIEW')"};
        desc1.categories = {"sap", "datasphere"};
        info.descriptions.push_back(std::move(desc1));
        FunctionDescription desc2;
        desc2.description = "Read data from a relational asset in SAP Datasphere using space, asset ID, and explicit secret name.";
        desc2.parameter_names = {"space_id", "asset_id", "secret"};
        desc2.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc2.examples = {"SELECT * FROM datasphere_read_relational('MY_SPACE', 'MY_VIEW', 'my_secret')"};
        desc2.categories = {"sap", "datasphere"};
        info.descriptions.push_back(std::move(desc2));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateDatasphereReadAnalyticalFunction());
        FunctionDescription desc1;
        desc1.description = "Read data from an analytical (cube/view) asset in SAP Datasphere using space and asset ID.";
        desc1.parameter_names = {"space_id", "asset_id"};
        desc1.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc1.examples = {"SELECT * FROM datasphere_read_analytical('MY_SPACE', 'MY_CUBE')"};
        desc1.categories = {"sap", "datasphere"};
        info.descriptions.push_back(std::move(desc1));
        FunctionDescription desc2;
        desc2.description = "Read data from an analytical asset in SAP Datasphere using space, asset ID, and explicit secret name.";
        desc2.parameter_names = {"space_id", "asset_id", "secret"};
        desc2.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc2.examples = {"SELECT * FROM datasphere_read_analytical('MY_SPACE', 'MY_CUBE', 'my_secret')"};
        desc2.categories = {"sap", "datasphere"};
        info.descriptions.push_back(std::move(desc2));
        loader.RegisterFunction(std::move(info));
    }

    erpl_web::CreateDatasphereSecretFunctions::Register(loader);
}

static void RegisterOdpFunctions(ExtensionLoader &loader)
{
    {
        CreateTableFunctionInfo info(erpl_web::CreateOdpODataShowFunction());
        FunctionDescription desc;
        desc.description = "List available SAP ODP (Operational Data Provisioning) contexts and extractors at a given OData service root URL.";
        desc.parameter_names = {"url"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM odp_odata_show('https://<host>/sap/opu/odata/iwbep/GWSAMPLE_BASIC/')"};
        desc.categories = {"sap", "odp"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateOdpODataReadFunction());
        FunctionDescription desc;
        desc.description = "Read data from a SAP ODP extractor via OData with delta token support for incremental/CDC loads.";
        desc.parameter_names = {"url"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM odp_odata_read('https://<host>/sap/opu/odata/sap/ZEXTRACTOR_SRV/DataSet')"};
        desc.categories = {"sap", "odp"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateOdpListSubscriptionsFunction());
        FunctionDescription desc;
        desc.description = "List all active ODP subscriptions with their status and current delta tokens.";
        desc.parameter_names = {};
        desc.parameter_types = {};
        desc.examples = {"SELECT * FROM odp_odata_list_subscriptions()"};
        desc.categories = {"sap", "odp"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    loader.RegisterFunction(erpl_web::CreateOdpRemoveSubscriptionFunction());
}

static void RegisterSacFunctions(ExtensionLoader &loader)
{
    {
        CreateTableFunctionInfo info(erpl_web::CreateSacShowModelsFunction());
        FunctionDescription desc;
        desc.description = "List all planning and analytics models available in SAP Analytics Cloud.";
        desc.parameter_names = {};
        desc.parameter_types = {};
        desc.examples = {"SELECT * FROM sac_show_models()"};
        desc.categories = {"sap", "sac"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateSacShowStoriesFunction());
        FunctionDescription desc;
        desc.description = "List all stories (dashboards/reports) available in SAP Analytics Cloud.";
        desc.parameter_names = {};
        desc.parameter_types = {};
        desc.examples = {"SELECT * FROM sac_show_stories()"};
        desc.categories = {"sap", "sac"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateSacGetModelInfoFunction());
        FunctionDescription desc;
        desc.description = "Get detailed metadata and dimension/measure schema for a SAP Analytics Cloud model.";
        desc.parameter_names = {"model_id"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM sac_get_model_info('t.2:MY_MODEL')"};
        desc.categories = {"sap", "sac"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateSacGetStoryInfoFunction());
        FunctionDescription desc;
        desc.description = "Get detailed metadata for a SAP Analytics Cloud story.";
        desc.parameter_names = {"story_id"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM sac_get_story_info('ABC12345')"};
        desc.categories = {"sap", "sac"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateSacReadPlanningDataFunction());
        FunctionDescription desc;
        desc.description = "Read planning data from a SAP Analytics Cloud planning model.";
        desc.parameter_names = {"model_id"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM sac_read_planning_data('t.2:MY_PLANNING_MODEL')"};
        desc.categories = {"sap", "sac"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateSacReadAnalyticalFunction());
        FunctionDescription desc;
        desc.description = "Read analytical data from a SAP Analytics Cloud model with optional dimension and measure filtering.";
        desc.parameter_names = {"model_id"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM sac_read_analytical('t.2:MY_ANALYTICAL_MODEL')"};
        desc.categories = {"sap", "sac"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateSacReadStoryDataFunction());
        FunctionDescription desc;
        desc.description = "Extract underlying data from a SAP Analytics Cloud story's visualizations.";
        desc.parameter_names = {"story_id"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM sac_read_story_data('ABC12345')"};
        desc.categories = {"sap", "sac"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }

    // Register SAC storage extension (handles ATTACH support)
#ifdef DUCKDB_HAS_EXTENSION_CALLBACK_MANAGER
    auto &callback_manager = ExtensionCallbackManager::Get(loader.GetDatabaseInstance());
    callback_manager.Register("sac", shared_ptr<StorageExtension>(erpl_web::CreateSacStorageExtension().release()));
#else
    auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
    config.storage_extensions["sac"] = erpl_web::CreateSacStorageExtension();
#endif
}

static void RegisterDeltaShareFunctions(ExtensionLoader &loader)
{
    {
        CreateTableFunctionInfo info(erpl_web::CreateDeltaShareScanFunction());
        FunctionDescription desc;
        desc.description = "Read data from a Delta Sharing table using a profile file and share/schema/table path.";
        desc.parameter_names = {"profile_path", "share", "schema", "table"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM delta_share_scan('/path/to/profile.json', 'my_share', 'my_schema', 'my_table')"};
        desc.categories = {"delta", "sharing"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateDeltaShareShowSharesFunction());
        FunctionDescription desc;
        desc.description = "List all shares available in a Delta Sharing server using a profile file.";
        desc.parameter_names = {"profile_path"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM delta_share_show_shares('/path/to/profile.json')"};
        desc.categories = {"delta", "sharing"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateDeltaShareShowSchemasFunction());
        FunctionDescription desc;
        desc.description = "List all schemas within a Delta Sharing share.";
        desc.parameter_names = {"profile_path", "share"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM delta_share_show_schemas('/path/to/profile.json', 'my_share')"};
        desc.categories = {"delta", "sharing"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateDeltaShareShowTablesFunction());
        FunctionDescription desc;
        desc.description = "List all tables within a Delta Sharing schema.";
        desc.parameter_names = {"profile_path", "share", "schema"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM delta_share_show_tables('/path/to/profile.json', 'my_share', 'my_schema')"};
        desc.categories = {"delta", "sharing"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
}

static void RegisterBusinessCentralFunctions(ExtensionLoader &loader)
{
    erpl_web::CreateBusinessCentralSecretFunctions::Register(loader);

    {
        CreateTableFunctionInfo info(erpl_web::CreateBcShowCompaniesFunction());
        FunctionDescription desc;
        desc.description = "List all companies accessible in Microsoft Dynamics 365 Business Central.";
        desc.parameter_names = {};
        desc.parameter_types = {};
        desc.examples = {"SELECT * FROM bc_show_companies()"};
        desc.categories = {"microsoft", "business_central"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateBcShowEntitiesFunction());
        FunctionDescription desc;
        desc.description = "List all OData entity sets available in Microsoft Dynamics 365 Business Central.";
        desc.parameter_names = {};
        desc.parameter_types = {};
        desc.examples = {"SELECT * FROM bc_show_entities()"};
        desc.categories = {"microsoft", "business_central"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateBcDescribeFunction());
        FunctionDescription desc;
        desc.description = "Describe the schema (property names, types, keys) of a Business Central entity.";
        desc.parameter_names = {"entity"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM bc_describe('customers')"};
        desc.categories = {"microsoft", "business_central"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateBcReadFunction());
        FunctionDescription desc;
        desc.description = "Read data from a Business Central entity with filter and predicate pushdown support.";
        desc.parameter_names = {"entity"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM bc_read('customers')"};
        desc.categories = {"microsoft", "business_central"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
}

static void RegisterDataverseFunctions(ExtensionLoader &loader)
{
    erpl_web::CreateDataverseSecretFunctions::Register(loader);

    {
        CreateTableFunctionInfo info(erpl_web::CreateCrmShowEntitiesFunction());
        FunctionDescription desc;
        desc.description = "List all entity types available in Microsoft Dataverse (Dynamics 365 CRM).";
        desc.parameter_names = {};
        desc.parameter_types = {};
        desc.examples = {"SELECT * FROM crm_show_entities()"};
        desc.categories = {"microsoft", "dataverse"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateCrmDescribeFunction());
        FunctionDescription desc;
        desc.description = "Describe the attribute schema (names, types, keys) of a Microsoft Dataverse entity.";
        desc.parameter_names = {"entity"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM crm_describe('accounts')"};
        desc.categories = {"microsoft", "dataverse"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        CreateTableFunctionInfo info(erpl_web::CreateCrmReadFunction());
        FunctionDescription desc;
        desc.description = "Read data from a Microsoft Dataverse entity with filter and predicate pushdown support.";
        desc.parameter_names = {"entity"};
        desc.parameter_types = {LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM crm_read('accounts')"};
        desc.categories = {"microsoft", "dataverse"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
}

static void RegisterGraphExcelFunctions(ExtensionLoader &loader)
{
    // Register Microsoft Graph secret type
    erpl_web::CreateGraphSecretFunctions::Register(loader);

    // Register Microsoft Graph Excel table functions
    erpl_web::GraphExcelFunctions::Register(loader);
}

static void RegisterGraphSharePointFunctions(ExtensionLoader &loader)
{
    // Register Microsoft Graph SharePoint table functions
    erpl_web::GraphSharePointFunctions::Register(loader);
}

static void RegisterGraphPlannerFunctions(ExtensionLoader &loader)
{
    // Register Microsoft Graph Planner table functions
    erpl_web::GraphPlannerFunctions::Register(loader);
}

static void RegisterGraphOutlookFunctions(ExtensionLoader &loader)
{
    // Register Microsoft Graph Outlook table functions
    erpl_web::GraphOutlookFunctions::Register(loader);
}

static void RegisterGraphEntraFunctions(ExtensionLoader &loader)
{
    // Register Microsoft Graph Entra ID Directory table functions
    erpl_web::GraphEntraFunctions::Register(loader);
}

static void RegisterGraphTeamsFunctions(ExtensionLoader &loader)
{
    // Register Microsoft Graph Teams table functions
    erpl_web::GraphTeamsFunctions::Register(loader);
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

    loader.SetDescription(
        "HTTP/REST, OData v2/v4, SAP Datasphere, SAP Analytics Cloud, SAP ODP, "
        "Delta Sharing, Microsoft 365 (Graph), Business Central, and Dataverse "
        "data access functions for DuckDB."
    );

    // Initialize telemetry with API key before capturing extension load
    PostHogTelemetry::Instance().SetAPIKey("phc_t3wwRLtpyEmLHYaZCSszG0MqVr74J6wnCrj9D41zk2t");
    PostHogTelemetry::Instance().CaptureExtensionLoad("erpl_web", "0.1.0");

    RegisterConfiguration(instance);
    RegisterWebFunctions(loader);
    RegisterODataFunctions(loader);
    RegisterDatasphereFunctions(loader);
    RegisterSacFunctions(loader);
    RegisterOdpFunctions(loader);
    RegisterDeltaShareFunctions(loader);
    RegisterBusinessCentralFunctions(loader);
    RegisterDataverseFunctions(loader);
    RegisterGraphExcelFunctions(loader);
    RegisterGraphSharePointFunctions(loader);
    RegisterGraphPlannerFunctions(loader);
    RegisterGraphOutlookFunctions(loader);
    RegisterGraphEntraFunctions(loader);
    RegisterGraphTeamsFunctions(loader);
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

