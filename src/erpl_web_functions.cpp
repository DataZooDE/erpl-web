#include "erpl_web_functions.hpp"
#include "duckdb_argument_helper.hpp"

#include "telemetry.hpp"

namespace erpl_web {


HttpBindData::HttpBindData(std::shared_ptr<HttpRequest> request, std::shared_ptr<HttpAuthParams> auth_params) 
    : TableFunctionData(), request(request), auth_params(auth_params), done(false)
{ }

std::vector<std::string> HttpBindData::GetResultNames() 
{
    return HttpResponse::DuckDbResponseNames();
}

std::vector<LogicalType> HttpBindData::GetResultTypes() 
{
    std::vector<LogicalType> ret;
    
    auto child_types = StructType::GetChildTypes(HttpResponse::DuckDbResponseType());
    for (auto &type : child_types) {
        ret.push_back(type.second);
    }

    return ret;
}

bool HttpBindData::HasMoreResults() 
{
    return done == false;
}

unsigned int HttpBindData::FetchNextResult(DataChunk &output) 
{
    HttpClient client;
    auto response = client.SendRequest(*request);
    auto row = response->ToRow();

    for (unsigned int i = 0; i < row.size(); i++) {
        output.SetValue(i, 0, row[i]);
    }

    done = true;
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
        return;
    }

    auto map_entries = ListValue::GetChildren(named_params["headers"]);
    for (auto &el : map_entries)
    {
        auto struct_entries = StructValue::GetChildren(el);
        if (struct_entries.size() != 2)
        {
            throw std::runtime_error("Header map must contain key-value pairs");
        }

        header_keys.emplace_back(struct_entries[0]);
        header_vals.emplace_back(struct_entries[1]);
    }
}

static Value CreateHttpHeaderFromArgs(TableFunctionBindInput &input)
{
    auto args = input.inputs;
    auto named_params = input.named_parameters;

    vector<Value> header_keys;
    vector<Value> header_vals;

    PopulateHeadersFromHeadersParam(named_params, header_keys, header_vals);

    header_keys.emplace_back(Value("Content-Type")); 
    header_vals.emplace_back(args.size() < 3 ? Value("application/json") : args[2]);

    if (HasParam(named_params, "accept")) {
        header_keys.emplace_back(Value("Accept"));
        header_vals.emplace_back(named_params["accept"]);
    } else {
        header_keys.emplace_back(Value("Accept"));
        header_vals.emplace_back(Value("application/json"));
    }
    
    return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, 
                      header_keys, header_vals);
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
    auto args = input.inputs;
    auto url = args[0].ToString();
    auto request = std::make_shared<HttpRequest>(method, url);
    request->AuthHeadersFromParams(auth_params);
    request->HeadersFromMapArg(CreateHttpHeaderFromArgs(input));
    
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
    return std::make_shared<HttpAuthParams>(HttpAuthParams::FromDuckDbSecrets(context, url));
}


static unique_ptr<FunctionData> HttpBind(duckdb::ClientContext &context, 
                                         TableFunctionBindInput &input, 
                                         vector<LogicalType> &return_types, 
                                         vector<string> &names,
                                         HttpMethod method) 
{
    auto auth_params = AuthParamsFromInput(context, input);
    auto bind_data = make_uniq<HttpBindData>(RequestFromInput(*auth_params, input, method), auth_params); 

    names = bind_data->GetResultNames();
    return_types = bind_data->GetResultTypes();

    return std::move(bind_data);
}

static unique_ptr<FunctionData> HttpGetBind(ClientContext &context, 
                                            TableFunctionBindInput &input, 
                                            vector<LogicalType> &return_types, 
                                            vector<string> &names) 
{
    PostHogTelemetry::Instance().CaptureFunctionExecution("http_get");
    return HttpBind(context, input, return_types, names, HttpMethod::GET);
}

static unique_ptr<FunctionData> HttpHeadBind(ClientContext &context, 
                                            TableFunctionBindInput &input, 
                                            vector<LogicalType> &return_types, 
                                            vector<string> &names) 
{
    PostHogTelemetry::Instance().CaptureFunctionExecution("http_head");
    return HttpBind(context, input, return_types, names, HttpMethod::HEAD);
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
    auto &bind_data = data.bind_data->CastNoConst<HttpBindData>();
    
    if (! bind_data.HasMoreResults()) {
        return;
    }

    bind_data.FetchNextResult(output);
}

// ----------------------------------------------------------------------

LogicalType CreateHttpHeaderType() 
{
    auto type = HttpResponse::DuckDbHeaderType();
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