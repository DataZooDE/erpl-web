#include "erpl_web_functions.hpp"
#include "duckdb_argument_helper.hpp"

#include "telemetry.hpp"
#include "erpl_tracing.hpp"

namespace erpl_web {


HttpBindData::HttpBindData(std::shared_ptr<HttpRequest> request, std::shared_ptr<HttpAuthParams> auth_params) 
    : TableFunctionData(), request(std::move(request)), auth_params(std::move(auth_params)), done(std::make_shared<bool>(false))
{ }

std::vector<std::string> HttpBindData::GetResultNames() 
{
    return HttpResponse::DuckDbResponseNames();
}

std::vector<LogicalType> HttpBindData::GetResultTypes() 
{
    std::vector<LogicalType> ret;
    
    auto child_types = StructType::GetChildTypes(HttpResponse::DuckDbResponseType());
    ret.reserve(child_types.size()); // Pre-allocate for efficiency
    for (const auto &type : child_types) {
        ret.push_back(type.second);
    }

    return ret;
}

bool HttpBindData::HasMoreResults() const
{
    return *done == false;
}

unsigned int HttpBindData::FetchNextResult(DataChunk &output) const
{
    HttpClient client;
    auto response = client.SendRequest(*request);
    auto row = response->ToRow();

    for (unsigned int i = 0; i < row.size(); i++) {
        output.SetValue(i, 0, row[i]);
    }

    *done = true;
    output.SetCardinality(1);
    return 1;
}

// ----------------------------------------------------------------------

static void PopulateHeadersFromHeadersParam(duckdb::named_parameter_map_t &named_params, 
                                            duckdb::vector<duckdb::Value> &header_keys, 
                                            duckdb::vector<duckdb::Value> &header_vals)
{
    if (! HasParam(named_params, "headers"))
    {
        ERPL_TRACE_DEBUG("HTTP_HEADERS", "No headers parameter provided");
        return;
    }

    auto headers_value = named_params["headers"];
    ERPL_TRACE_DEBUG("HTTP_HEADERS", "Processing headers parameter of type: " + headers_value.type().ToString());
    
    if (headers_value.type().id() == LogicalTypeId::MAP) {
        // Map case: MAP{'key': 'value', 'key2': 'value2'}
        auto map_entries = MapValue::GetChildren(headers_value);
        ERPL_TRACE_DEBUG("HTTP_HEADERS", "Found " + std::to_string(map_entries.size()) + " map entries");
        
        // Pre-allocate for efficiency
        header_keys.reserve(map_entries.size());
        header_vals.reserve(map_entries.size());
        
        // DuckDB MAPs are stored as a list of structs with 'key' and 'value' fields
        for (size_t i = 0; i < map_entries.size(); i++) {
            auto entry = map_entries[i];
            if (entry.type().id() == LogicalTypeId::STRUCT) {
                auto struct_entries = StructValue::GetChildren(entry);
                auto struct_types = StructType::GetChildTypes(entry.type());
                
                // Find the key and value fields
                std::string key, value;
                for (size_t j = 0; j < struct_types.size() && j < struct_entries.size(); j++) {
                    if (struct_types[j].first == "key") {
                        key = struct_entries[j].ToString();
                    } else if (struct_types[j].first == "value") {
                        value = struct_entries[j].ToString();
                    }
                }
                
                if (!key.empty() && !value.empty()) {
                    header_keys.emplace_back(Value(key));
                    header_vals.emplace_back(Value(value));
                    ERPL_TRACE_DEBUG("HTTP_HEADERS", "Extracted header: " + key + " = " + value);
                }
            }
        }
        ERPL_TRACE_DEBUG("HTTP_HEADERS", "Successfully processed " + std::to_string(header_keys.size()) + " headers from MAP");
    } else {
        ERPL_TRACE_ERROR("HTTP_HEADERS", "Unsupported headers type: " + headers_value.type().ToString());
        throw std::runtime_error("Headers parameter must be a MAP<VARCHAR, VARCHAR> type");
    }
}

static Value CreateHttpHeaderFromArgs(TableFunctionBindInput &input)
{
    auto args = input.inputs;
    auto named_params = input.named_parameters;

    vector<Value> header_keys;
    vector<Value> header_vals;

    PopulateHeadersFromHeadersParam(named_params, header_keys, header_vals);

    // Only add Content-Type header if there's actual content
    if (args.size() >= 2 && !args[1].IsNull() && args[1].ToString().length() > 0) {
        header_keys.emplace_back(Value("Content-Type")); 
        header_vals.emplace_back(args.size() < 3 ? Value("application/json") : args[2]);
    }

    // Only add Accept header if user didn't provide one in custom headers
    bool has_accept_header = false;
    for (size_t i = 0; i < header_keys.size(); i++) {
        if (header_keys[i].ToString() == "Accept") {
            has_accept_header = true;
            break;
        }
    }
    
    if (!has_accept_header) {
        if (HasParam(named_params, "accept")) {
            header_keys.emplace_back(Value("Accept"));
            header_vals.emplace_back(named_params["accept"]);
        } else {
            header_keys.emplace_back(Value("Accept"));
            header_vals.emplace_back(Value("application/json"));
        }
    }
    
    // Create a proper MAP using the correct signature
    return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, header_keys, header_vals);
}

static std::string CreateContentFromArgs(TableFunctionBindInput &input)
{
    auto args = input.inputs;
    auto named_params = input.named_parameters;

    if (args.size() == 3) {
        return args[1].ToString();
    }

    if (args.size() == 2) {
        return args[1].DefaultCastAs(LogicalType::VARCHAR).ToString();
    }

    return "";
}

static std::shared_ptr<HttpRequest> RequestFromInput(const HttpAuthParams &auth_params, TableFunctionBindInput &input, HttpMethod method) 
{
    ERPL_TRACE_DEBUG("HTTP_REQUEST", "Creating HTTP " + method.ToString() + " request");
    
    auto args = input.inputs;
    auto url = args[0].GetValue<std::string>();
    ERPL_TRACE_INFO("HTTP_REQUEST", "Request URL: " + url);
    
    auto headers = CreateHttpHeaderFromArgs(input);
    auto content = CreateContentFromArgs(input);
    
    ERPL_TRACE_DEBUG("HTTP_REQUEST", "Request created with " + std::to_string(content.length()) + " bytes of content");
    
    // Extract content type from headers
    std::string content_type = "application/json";
    if (args.size() >= 3) {
        content_type = args[2].GetValue<std::string>();
    }
    
    auto request = std::make_shared<HttpRequest>(method, url, content_type, content);
    request->HeadersFromMapArg(headers);
    request->AuthHeadersFromParams(auth_params);
    return request;
}

static std::shared_ptr<HttpRequest> MutatingRequestFromInput(const HttpAuthParams &auth_params, TableFunctionBindInput &input, HttpMethod method) 
{
    auto request = RequestFromInput(auth_params, input, method);
    request->content = CreateContentFromArgs(input);
    
    return request;
}

static std::shared_ptr<HttpAuthParams> AuthParamsFromInput(duckdb::ClientContext &context, TableFunctionBindInput &input)
{
    auto args = input.inputs;
    auto url = args[0].ToString();
    auto named_params = input.named_parameters;
    
    // Check if auth parameter is provided - this takes precedence over secrets
    if (HasParam(named_params, "auth")) {
        auto auth_value = named_params["auth"].GetValue<std::string>();
        ERPL_TRACE_DEBUG("HTTP_AUTH", "Using auth parameter: " + auth_value);
        
        auto auth_params = std::make_shared<HttpAuthParams>();
        
        // Check if auth_type is specified, default to BASIC if not provided
        std::string auth_type = "BASIC";
        if (HasParam(named_params, "auth_type")) {
            auth_type = named_params["auth_type"].GetValue<std::string>();
            ERPL_TRACE_DEBUG("HTTP_AUTH", "Using auth_type parameter: " + auth_type);
        } else {
            ERPL_TRACE_DEBUG("HTTP_AUTH", "No auth_type specified, defaulting to BASIC");
        }
        
        if (auth_type == "BASIC") {
            // Parse the auth string (assume format: username:password)
            size_t colon_pos = auth_value.find(':');
            if (colon_pos != std::string::npos) {
                std::string username = auth_value.substr(0, colon_pos);
                std::string password = auth_value.substr(colon_pos + 1);
                
                auth_params->basic_credentials = std::make_tuple(username, password);
                ERPL_TRACE_DEBUG("HTTP_AUTH", "Parsed basic auth from parameter - username: " + username + ", password: ***");
            } else {
                // If no colon found, treat the entire string as username with empty password
                auth_params->basic_credentials = std::make_tuple(auth_value, "");
                ERPL_TRACE_DEBUG("HTTP_AUTH", "Parsed basic auth from parameter - username: " + auth_value + ", password: (empty)");
            }
        } else if (auth_type == "BEARER") {
            // For bearer auth, the entire auth_value is the token
            auth_params->bearer_token = auth_value;
            ERPL_TRACE_DEBUG("HTTP_AUTH", "Parsed bearer auth from parameter - token: ***");
        } else {
            ERPL_TRACE_ERROR("HTTP_AUTH", "Unsupported auth_type: " + auth_type + ", falling back to registered secrets");
            return HttpAuthParams::FromDuckDbSecrets(context, url);
        }
        
        return auth_params;
    }
    
    // Fall back to registered secrets
    ERPL_TRACE_DEBUG("HTTP_AUTH", "No auth parameter provided, using registered secrets");
    return HttpAuthParams::FromDuckDbSecrets(context, url);
}


static unique_ptr<FunctionData> HttpGetBind(ClientContext &context, 
                                            TableFunctionBindInput &input, 
                                            vector<LogicalType> &return_types, 
                                            vector<string> &names) 
{
    PostHogTelemetry::Instance().CaptureFunctionExecution("http_get");
    auto auth_params = AuthParamsFromInput(context, input);
    auto bind_data = make_uniq<HttpBindData>(RequestFromInput(*auth_params, input, HttpMethod::GET), auth_params);
    
    names = bind_data->GetResultNames();
    return_types = bind_data->GetResultTypes();
    
    return std::move(bind_data);
}

static unique_ptr<FunctionData> HttpHeadBind(ClientContext &context, 
                                            TableFunctionBindInput &input, 
                                            vector<LogicalType> &return_types, 
                                            vector<string> &names) 
{
    PostHogTelemetry::Instance().CaptureFunctionExecution("http_head");
    auto auth_params = AuthParamsFromInput(context, input);
    auto bind_data = make_uniq<HttpBindData>(RequestFromInput(*auth_params, input, HttpMethod::HEAD), auth_params);
    
    names = bind_data->GetResultNames();
    return_types = bind_data->GetResultTypes();
    
    return std::move(bind_data);
}

static unique_ptr<FunctionData> HttpMutatingBind(ClientContext &context, 
                                                 TableFunctionBindInput &input, 
                                                 vector<LogicalType> &return_types, 
                                                 vector<string> &names,
                                                 HttpMethod method) 
{
    auto auth_params = AuthParamsFromInput(context, input);
    auto bind_data = make_uniq<HttpBindData>(MutatingRequestFromInput(*auth_params, input, method), auth_params); 

    names = bind_data->GetResultNames();
    return_types = bind_data->GetResultTypes();

    return std::move(bind_data);
}

static unique_ptr<FunctionData> HttpPostBind(ClientContext &context, 
                                             TableFunctionBindInput &input, 
                                             vector<LogicalType> &return_types, 
                                             vector<string> &names) 
{
    PostHogTelemetry::Instance().CaptureFunctionExecution("http_post");
    return HttpMutatingBind(context, input, return_types, names, HttpMethod::POST);
}

static unique_ptr<FunctionData> HttpPutBind(ClientContext &context, 
                                             TableFunctionBindInput &input, 
                                             vector<LogicalType> &return_types, 
                                             vector<string> &names) 
{
    PostHogTelemetry::Instance().CaptureFunctionExecution("http_put");
    return HttpMutatingBind(context, input, return_types, names, HttpMethod::PUT);
}

static unique_ptr<FunctionData> HttpPatchBind(ClientContext &context, 
                                             TableFunctionBindInput &input, 
                                             vector<LogicalType> &return_types, 
                                             vector<string> &names) 
{
    PostHogTelemetry::Instance().CaptureFunctionExecution("http_patch");
    return HttpMutatingBind(context, input, return_types, names, HttpMethod::PATCH);
}

static unique_ptr<FunctionData> HttpDeleteBind(ClientContext &context, 
                                              TableFunctionBindInput &input, 
                                              vector<LogicalType> &return_types, 
                                              vector<string> &names) 
{
    PostHogTelemetry::Instance().CaptureFunctionExecution("http_delete");
    return HttpMutatingBind(context, input, return_types, names, HttpMethod::_DELETE);
}

// ----------------------------------------------------------------------

static void HttpScan(ClientContext &context, 
                        TableFunctionInput &data, 
                        DataChunk &output) 
{
    ERPL_TRACE_DEBUG("HTTP_SCAN", "Starting HTTP scan operation");
    
    auto &bind_data = data.bind_data->Cast<HttpBindData>();
    
    if (!bind_data.HasMoreResults()) {
        ERPL_TRACE_DEBUG("HTTP_SCAN", "No more results available");
        output.SetCardinality(0);
        return;
    }
    
    try {
        auto rows_fetched = bind_data.FetchNextResult(output);
        ERPL_TRACE_INFO("HTTP_SCAN", "Successfully fetched " + std::to_string(rows_fetched) + " rows");
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("HTTP_SCAN", "Failed to fetch results: " + std::string(e.what()));
        throw;
    }
}

// ----------------------------------------------------------------------

LogicalType CreateHttpHeaderType() 
{
    // Use MAP type to accept key-value pairs like MAP{'key': 'value', 'key2': 'value2'}
    // This is the proper DuckDB MAP type for headers
    auto type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
    type.SetAlias("ERPL_HTTP_HEADER");
    return type;
}

LogicalType CreateHttpAuthTypeType() 
{
    auto auth_type_enum = Vector(LogicalType::VARCHAR, 3);
    auth_type_enum.SetValue(0, Value("BASIC"));
    auth_type_enum.SetValue(1, Value("DIGEST"));
    auth_type_enum.SetValue(2, Value("BEARER"));
    
    auto typ = LogicalType::ENUM("AUTH_TYPE", auth_type_enum, 3);
    typ.SetAlias("HTTP_AUTH_TYPE");

    return typ;
}

static void AddDefaultHttpNamedParams(TableFunction &func) 
{
    func.named_parameters["headers"] = CreateHttpHeaderType();
    func.named_parameters["content_type"] = LogicalType::VARCHAR;
    func.named_parameters["accept"] = LogicalType::VARCHAR;
    func.named_parameters["auth"] = LogicalType::VARCHAR;
    func.named_parameters["auth_type"] = CreateHttpAuthTypeType();
    func.named_parameters["timeout"] = LogicalType::INTEGER;
}

TableFunctionSet CreatHttpFunction(const std::string &http_verb, const duckdb::table_function_bind_t bind_func)
{
    TableFunctionSet function_set(StringUtil::Format("http_%s", http_verb));
	
    auto get_with_url = TableFunction({ LogicalType::VARCHAR}, HttpScan, bind_func);
    AddDefaultHttpNamedParams(get_with_url);

    function_set.AddFunction(get_with_url);

	return function_set;
}

TableFunctionSet CreateMutatingHttpFunction(const std::string &http_verb, const duckdb::table_function_bind_t bind_func)
{
    TableFunctionSet function_set(StringUtil::Format("http_%s", http_verb));

    auto with_json_data = TableFunction({ LogicalType::VARCHAR, LogicalType::JSON() }, HttpScan, bind_func);
    AddDefaultHttpNamedParams(with_json_data);
    with_json_data.named_parameters.erase("content_type"); // remove content_type, as for this overload JSON is fixed

    auto with_content_and_type = TableFunction({ LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR }, HttpScan, bind_func);
    AddDefaultHttpNamedParams(with_content_and_type);
    
    function_set.AddFunction(with_json_data);
    function_set.AddFunction(with_content_and_type);

    return function_set;
}

TableFunctionSet CreateHttpGetFunction() 
{
    return CreatHttpFunction("get", HttpGetBind);
}

TableFunctionSet CreateHttpHeadFunction() 
{
    return CreatHttpFunction("head", HttpHeadBind);
}

TableFunctionSet CreateHttpPostFunction() 
{
    return CreateMutatingHttpFunction("post", HttpPostBind);
}

TableFunctionSet CreateHttpPutFunction() 
{
    return CreateMutatingHttpFunction("put", HttpPutBind);
}

TableFunctionSet CreateHttpPatchFunction() 
{
    return CreateMutatingHttpFunction("patch", HttpPatchBind);
}

TableFunctionSet CreateHttpDeleteFunction() 
{
    return CreateMutatingHttpFunction("delete", HttpDeleteBind);
}

} // namespace erpl_web