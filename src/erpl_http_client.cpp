#include <algorithm>
#include <regex>
#include <sstream>
#include <vector>
#include <numeric>
#include <cmath>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <condition_variable>

#include "duckdb.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "charset_converter.hpp"
#include "erpl_http_client.hpp"
#include "erpl_tracing.hpp"

using namespace duckdb;

namespace erpl_web
{

HttpUrl::HttpUrl(const std::string& url) {
    ParseUrl(url);
}

void HttpUrl::ParseUrl(const std::string& url) {
    const static std::regex re(R"(^(?:(https?):)?(?://(([^:/?#]*)(?::([^@/?#]*))?@)?([^:/?#]+)(?::(\d+))?)?([^?#]*)(\?[^#]*)?(#.*)?)");
    std::smatch m;
    if (!std::regex_match(url, m, re)) {
        throw std::runtime_error("Invalid URL, cannot be parsed");
    }

    scheme = m[1].str();
    username = m[3].str();
    password = m[4].str();
    host = m[5].str();
    port = m[6].str();
    path = m[7].str();
    query = m[8].str();
    fragment = m[9].str();
}

std::string HttpUrl::ToSchemeHostAndPort() const {
    std::ostringstream ss;
    ss << scheme << "://" << host;
    if (!port.empty()) {
        ss << ":" << port;
    }
    return ss.str();
}

std::string HttpUrl::ToPathQuery() const {
    std::ostringstream ss;
    ss << (path.empty() ? "/" : path) << query;
    return ss.str();
}

std::string HttpUrl::ToPathQueryFragment() const {
    std::ostringstream ss;
    ss << ToPathQuery() << fragment;
    return ss.str();
}

std::string HttpUrl::ToString() const {
    std::ostringstream ss;
    ss << ToSchemeHostAndPort() << ToPathQueryFragment();
    return ss.str();
}

HttpUrl::operator std::string() const {
    return ToString();
}

bool HttpUrl::Equals(const HttpUrl& other) const {
    return ToLower(scheme) == ToLower(other.scheme) &&
           ToLower(host) == ToLower(other.host) &&
           port == other.port &&
           path == other.path &&
           query == other.query &&
           fragment == other.fragment &&
           username == other.username &&
           password == other.password;
}

std::string HttpUrl::ToLower(const std::string& str) {
    std::string lowerStr;
    lowerStr.reserve(str.size()); // Pre-allocate for efficiency
    std::transform(str.begin(), str.end(), std::back_inserter(lowerStr),
                   [](unsigned char c){ return std::tolower(c); });
    return lowerStr;
}

HttpUrl HttpUrl::PopPath() 
{
    auto path_obj = std::filesystem::path(Path());
    auto new_path = path_obj.parent_path();
    auto new_url = *this;
    new_url.Path(new_path.string());
    return new_url;
}

std::filesystem::path HttpUrl::MergePaths(const std::filesystem::path& base_path, const std::filesystem::path& rel_path) 
{
    // Check if the relative path is actually an absolute path
    if (rel_path.is_absolute()) {
        return rel_path.lexically_normal();
    }

    // Decompose the paths into their respective components
    std::vector<std::string> base_parts;
    base_parts.reserve(std::distance(base_path.begin(), base_path.end()));
    for (const auto& part : base_path) {
        base_parts.emplace_back(part.string());
    }

    std::vector<std::string> rel_parts;
    rel_parts.reserve(std::distance(rel_path.begin(), rel_path.end()));
    for (const auto& part : rel_path) {
        rel_parts.emplace_back(part.string());
    }

    // Find the overlap between the end of base_parts and the beginning of rel_parts
    std::size_t overlap_start = 0;
    for (std::size_t i = 0; i < base_parts.size(); ++i) {
        std::size_t j = 0;
        while (i + j < base_parts.size() && j < rel_parts.size() && base_parts[i + j] == rel_parts[j]) {
            ++j;
        }
        if (i + j == base_parts.size()) {
            overlap_start = j; // Overlap found
            break;
        }
    }

    // Build the merged path from base_parts and the remaining rel_parts
    std::filesystem::path merged_path;
    for (const auto& part : base_parts) {
        merged_path /= part;
    }
    for (std::size_t i = overlap_start; i < rel_parts.size(); ++i) {
        merged_path /= rel_parts[i];
    }

    merged_path = merged_path.lexically_normal();
    return merged_path;
}
 

HttpUrl HttpUrl::MergeWithBaseUrlIfRelative(const HttpUrl& base_url, const std::string& relative_url_or_path)
{
    if (relative_url_or_path.empty()) {
        return base_url;
    }

    if (relative_url_or_path.find("://") != std::string::npos) {
        return HttpUrl(relative_url_or_path);
    }

    // Now parse the path, query, and fragment
    const std::regex path_query_fragment_regex(
        R"(([^?#]*)(?:\?([^#]*))?(?:#(.*))?)"
    );

    std::smatch match;
    if (!std::regex_match(relative_url_or_path, match, path_query_fragment_regex)) {
        throw std::runtime_error("Invalid path, query, or fragment in URL: " + relative_url_or_path);
    }

    std::string rel_path = match[1].str();
    std::string rel_query = match[2].str();
    std::string rel_fragment = match[3].str();

    // Merge the base URL with the relative URL
    HttpUrl merged_url = base_url;

    // Handle path
    if (!rel_path.empty()) {
        if (rel_path[0] == '/') {
            // Path starting with / should be treated as relative to base URL's root
            // Remove the leading / and merge with base path
            std::string rel_path_without_slash = rel_path.substr(1);
            auto merged_path = MergePaths(base_url.Path(), rel_path_without_slash);
            merged_url.Path(merged_path.string());
        } else {
            // Relative path - merge with base path
            auto merged_path = MergePaths(base_url.Path(), rel_path);
            merged_url.Path(merged_path.string());
        }
    }

    // Handle query and fragment
    if (!rel_query.empty()) {
        merged_url.Query(rel_query.empty() ? "" : "?" + rel_query);
    }
    if (!rel_fragment.empty()) {
        merged_url.Fragment(rel_fragment.empty() ? "" : "#" + rel_fragment);
    }

    return merged_url;
}

// Setters
void HttpUrl::Scheme(const std::string& value) { scheme = value; }
void HttpUrl::Host(const std::string& value) { host = value; }
void HttpUrl::Port(const std::string& value) { port = value; }
void HttpUrl::Path(const std::string& value) { path = value; }
void HttpUrl::Query(const std::string& value) { query = value; }
void HttpUrl::Fragment(const std::string& value) { fragment = value; }
void HttpUrl::Username(const std::string& value) { username = value; }
void HttpUrl::Password(const std::string& value) { password = value; }

// Getters
std::string HttpUrl::Scheme() const { return scheme; }
std::string HttpUrl::Host() const { return host; }
std::string HttpUrl::Port() const { return port; }
std::string HttpUrl::Path() const { return path; }
std::string HttpUrl::Query() const { return query; }
std::string HttpUrl::Fragment() const { return fragment; }
std::string HttpUrl::Username() const { return username; }
std::string HttpUrl::Password() const { return password; }

// ----------------------------------------------------------------------

HttpParams::HttpParams()
    : timeout(DEFAULT_TIMEOUT),
      retries(DEFAULT_RETRIES),
      retry_wait_ms(DEFAULT_RETRY_WAIT_MS),
      retry_backoff(DEFAULT_RETRY_BACKOFF),
      force_download(DEFAULT_FORCE_DOWNLOAD),
      keep_alive(DEFAULT_KEEP_ALIVE)
{
}

// ----------------------------------------------------------------------

std::shared_ptr<HttpAuthParams> HttpAuthParams::FromDuckDbSecrets(duckdb::ClientContext &context, const std::string &url)
{
    auto ret = std::make_shared<HttpAuthParams>();

    auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
    auto &secret_manager = duckdb::SecretManager::Get(context);

    auto basic_match = secret_manager.LookupSecret(transaction, url, "http_basic");
	if (basic_match.HasMatch()) {
        const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(basic_match.GetSecret());
		auto username = kv_secret.TryGetValue("username", true); // error_on_missing = true
		auto password = kv_secret.TryGetValue("password", true); // error_on_missing = true

        ret->basic_credentials = std::make_tuple(username.ToString(), password.ToString());
        return ret;
	}

    auto bearer_match = secret_manager.LookupSecret(transaction, url, "http_bearer");
	if (bearer_match.HasMatch()) {
		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(bearer_match.GetSecret());
		auto token = kv_secret.TryGetValue("token", true); // error_on_missing = true
		ret->bearer_token = token.ToString();
	}

	return ret;
}

std::shared_ptr<HttpAuthParams> HttpAuthParams::FromDuckDbSecrets(duckdb::ClientContext &context, const HttpUrl &url)
{
    return FromDuckDbSecrets(context, url.ToString());
}

HttpAuthType HttpAuthParams::AuthType() const
{
    if (basic_credentials.has_value()) {
        return HttpAuthType::BASIC;
    }
    else if (bearer_token.has_value()) {
        return HttpAuthType::BEARER;
    }
    return HttpAuthType::NONE;
}

std::optional<std::string> HttpAuthParams::BasicCredentialsBase64() const
{
    if (!basic_credentials.has_value()) {
        return std::nullopt;
    }

    auto [username, password] = basic_credentials.value();
    return Base64Encode(username + ":" + password);
}

std::string HttpAuthParams::Base64Encode(const std::string &input)
{
    auto result_str = std::string();
    result_str.resize(duckdb::Blob::ToBase64Size(input));

    duckdb::Blob::ToBase64(input, &result_str.front());

    return result_str;
}

std::string HttpAuthParams::CredsToStars(const std::string &creds) const
{
    return std::string(creds.size(), '*');
}

std::string HttpAuthParams::ToString() const
{
    if (basic_credentials.has_value()) {
        return "Basic:" + CredsToStars(std::get<0>(basic_credentials.value()) + ":" + std::get<1>(basic_credentials.value()));
    }
    else if (bearer_token.has_value()) {
        return "Bearer:" + CredsToStars(bearer_token.value());
    }
    return "None";
}

// ----------------------------------------------------------------------

HttpMethod HttpMethod::FromString(const std::string &method)
{
    std::string upperMethod = method;
    std::transform(upperMethod.begin(), upperMethod.end(), upperMethod.begin(), ::toupper);

    if (upperMethod == "GET")
    {
        return HttpMethod(GET);
    }
    else if (upperMethod == "POST")
    {
        return HttpMethod(POST);
    }
    else if (upperMethod == "PUT")
    {
        return HttpMethod(PUT);
    }
    else if (upperMethod == "DELETE")
    {
        return HttpMethod(_DELETE);
    }
    else if (upperMethod == "PATCH")
    {
        return HttpMethod(PATCH);
    }
    else if (upperMethod == "HEAD")
    {
        return HttpMethod(HEAD);
    }
    else if (upperMethod == "OPTIONS")
    {
        return HttpMethod(OPTIONS);
    }
    else if (upperMethod == "TRACE")
    {
        return HttpMethod(TRACE);
    }
    else if (upperMethod == "CONNECT")
    {
        return HttpMethod(CONNECT);
    }
    else
    {
        throw std::runtime_error(StringUtil::Format("Invalid HTTP method: '%s'", method));
    }
}

std::string HttpMethod::ToString() const
{
    switch (variant)
    {
    case GET:
        return "GET";
    case POST:
        return "POST";
    case PUT:
        return "PUT";
    case _DELETE:
        return "DELETE";
    case PATCH:
        return "PATCH";
    case HEAD:
        return "HEAD";
    case OPTIONS:
        return "OPTIONS";
    case TRACE:
        return "TRACE";
    case CONNECT:
        return "CONNECT";
    default:
        return "UNDEFINED";
    }
}

// ----------------------------------------------------------------------

HttpRequest::HttpRequest(HttpMethod method, const std::string &url, std::string content_type, std::string content)
    : method(method), url(HttpUrl(url)), content_type(std::move(content_type)), content(std::move(content))
{ }

HttpRequest::HttpRequest(HttpMethod method, const std::string &url)
    : HttpRequest(method, url, std::string("application/json"), std::string())
{ }

void HttpRequest::HeadersFromMapArg(duckdb::Value &header_map)
{
    if (header_map.IsNull())
    {
        return;
    }

    ERPL_TRACE_DEBUG("HTTP_HEADERS", "Processing headers from type: " + header_map.type().ToString());

    if (header_map.type().id() == LogicalTypeId::MAP) {
        // Map case: MAP{'key': 'value', 'key2': 'value2'}
        auto map_entries = MapValue::GetChildren(header_map);
        ERPL_TRACE_DEBUG("HTTP_HEADERS", "Processing " + std::to_string(map_entries.size()) + " map entries");
        
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
                    headers.emplace(key, value);
                    ERPL_TRACE_DEBUG("HTTP_HEADERS", "Added header: " + key + " = " + value);

                    if (ToLower(key) == "content-type") {
                        content_type = value;
                        ERPL_TRACE_DEBUG("HTTP_HEADERS", "Updated content_type to: " + value);
                    }
                }
            }
        }
    } else {
        throw std::runtime_error("Header map must be a MAP<VARCHAR, VARCHAR> type");
    }

    ERPL_TRACE_INFO("HTTP_HEADERS", "Final headers count: " + std::to_string(headers.size()));
}

void HttpRequest::HeadersFromMapArg(const duckdb::Value &header_map)
{
    HeadersFromMapArg(const_cast<duckdb::Value &>(header_map));
}

void HttpRequest::AuthHeadersFromParams(const HttpAuthParams &auth_params)
{
    if (auth_params.AuthType() == HttpAuthType::BASIC) {
        headers.emplace("Authorization", "Basic " + auth_params.BasicCredentialsBase64().value());
    }
    else if (auth_params.AuthType() == HttpAuthType::BEARER) {
        headers.emplace("Authorization", "Bearer " + auth_params.bearer_token.value());
    }
}

void HttpRequest::SetODataVersion(ODataVersion version)
{
    odata_version = version;
}

ODataVersion HttpRequest::GetODataVersion() const
{
    return odata_version;
}

void HttpRequest::AddODataVersionHeaders()
{
    if (odata_version == ODataVersion::V2) {
        // OData v2 headers
        headers.emplace("DataServiceVersion", "2.0");
        headers.emplace("MaxDataServiceVersion", "2.0");
        headers.emplace("Accept", "application/json;odata=verbose");
    } else {
        // OData v4 headers
        headers.emplace("OData-Version", "4.0");
        headers.emplace("OData-MaxVersion", "4.0");
        headers.emplace("Accept", "application/json;odata.metadata=minimal");
    }
}

std::string HttpRequest::ToCacheKey() const
{
    auto content_hasher = std::hash<std::string>();

    auto strstream = std::stringstream();
    strstream << method.ToString() << ":" << url.ToString() << ":" << content_hasher(content);

    return strstream.str();
}

duckdb_httplib_openssl::Headers HttpRequest::HttplibHeaders()
{
    duckdb_httplib_openssl::Headers ret;
    // Note: std::multimap doesn't have reserve(), but we can optimize by using emplace
    for (const auto &header : headers)
    {
        ret.emplace(header.first, header.second);
    }
    return ret;
}

duckdb_httplib_openssl::Result HttpRequest::Execute(duckdb_httplib_openssl::Client &client)
{
    auto path_str = url.ToPathQuery();
    auto headers = HttplibHeaders();

    // Log HTTP request details
    ERPL_TRACE_INFO("HTTP_REQUEST", "Executing " + method.ToString() + " request to: " + path_str);
    ERPL_TRACE_DEBUG("HTTP_REQUEST", "Request headers:");
    for (const auto& header : headers) {
        ERPL_TRACE_DEBUG("HTTP_REQUEST", "  " + header.first + ": " + header.second);
    }
    if (!content.empty()) {
        ERPL_TRACE_DEBUG("HTTP_REQUEST", "Request content (" + std::to_string(content.length()) + " bytes): " + content);
        ERPL_TRACE_DEBUG("HTTP_REQUEST", "Content-Type: " + content_type);
    }

    duckdb_httplib_openssl::Result result;
    if (method == HttpMethod::GET)
    {
        result = client.Get(path_str.c_str(), headers);
    }
    else if (method == HttpMethod::POST)
    {
        result = client.Post(path_str.c_str(), headers, content, content_type.c_str());
    }
    else if (method == HttpMethod::PUT)
    {
        result = client.Put(path_str.c_str(), headers, content, content_type.c_str());
    }
    else if (method == HttpMethod::PATCH)
    {
        result = client.Patch(path_str.c_str(), headers, content, content_type.c_str());
    }
    else if (method == HttpMethod::_DELETE)
    {
        result = client.Delete(path_str.c_str(), headers, content, content_type.c_str());
    }
    else if (method == HttpMethod::HEAD)
    {
        result = client.Head(path_str.c_str(), headers);
    }
    else
    {
        throw std::runtime_error("Invalid HTTP method");
    }

    // Log HTTP response details
    if (result) {
        ERPL_TRACE_INFO("HTTP_RESPONSE", "Response status: " + std::to_string(result->status));
        ERPL_TRACE_DEBUG("HTTP_RESPONSE", "Response headers:");
        for (const auto& header : result->headers) {
            ERPL_TRACE_DEBUG("HTTP_RESPONSE", "  " + header.first + ": " + header.second);
        }
        ERPL_TRACE_DEBUG("HTTP_RESPONSE", "Response body (" + std::to_string(result->body.length()) + " bytes)");
        
        // Log the actual response body content for debugging
        if (!result->body.empty()) {
            // Truncate very long responses to avoid overwhelming the logs
            if (result->body.length() > 1000) {
                ERPL_TRACE_DEBUG("HTTP_RESPONSE", "Response body (truncated): " + result->body.substr(0, 1000) + "...");
            } else {
                ERPL_TRACE_DEBUG("HTTP_RESPONSE", "Response body: " + result->body);
            }
        }
    } else {
        ERPL_TRACE_ERROR("HTTP_RESPONSE", "Request failed: " + to_string(result.error()));
    }

    return result;
}

// ----------------------------------------------------------------------

HttpResponse::HttpResponse(HttpMethod method, HttpUrl url, int code, std::string content_type, std::string content)
    : method(method), url(std::move(url)), code(code), content_type(std::move(content_type)), content(std::move(content))
{ }

HttpResponse::HttpResponse(HttpMethod method, HttpUrl url, int code)
    : HttpResponse(method, std::move(url), code, std::string(), std::string())
{ }

std::unique_ptr<HttpResponse> HttpResponse::FromHttpLibResponse(HttpMethod &method,
                                                                HttpUrl &url,
                                                                duckdb_httplib_openssl::Response &response)
{
    auto content_type = response.get_header_value("Content-Type");
    auto ret =  std::make_unique<HttpResponse>(method, url, response.status, content_type, response.body);
    ret->headers.reserve(response.headers.size()); // Pre-allocate for efficiency
    for (const auto &header : response.headers)
    {
        ret->headers.emplace(header.first, header.second);
    }

    return ret;
}

duckdb::LogicalType HttpResponse::DuckDbResponseType()
{
    child_list_t<LogicalType> children;
    children.emplace_back("method", duckdb::LogicalTypeId::VARCHAR);
    children.emplace_back("status", duckdb::LogicalTypeId::INTEGER);
    children.emplace_back("url", duckdb::LogicalTypeId::VARCHAR);
    children.emplace_back("headers", DuckDbHeaderType());
    children.emplace_back("content_type", duckdb::LogicalTypeId::VARCHAR);
    children.emplace_back("content", duckdb::LogicalTypeId::VARCHAR);

    return duckdb::LogicalType::STRUCT(children);
}

duckdb::LogicalType HttpResponse::DuckDbHeaderType()
{
    return duckdb::LogicalType::MAP(
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)
    );
}

std::vector<std::string> HttpResponse::DuckDbResponseNames()
{
    return { "method", "status", "url", "headers", "content_type", "content" };
}

duckdb::Value HttpResponse::ToValue() const
{
    child_list_t<duckdb::Value> children;
    children.emplace_back("method", method.ToString());
    children.emplace_back("status", code);
    children.emplace_back("url", url.ToString());
    children.emplace_back("headers", CreateHeaderMap());
    children.emplace_back("content_type", content_type);
    children.emplace_back("content", content);

    return duckdb::Value::STRUCT(children);
}

duckdb::Value HttpResponse::CreateHeaderMap() const
{
    auto header_map_type = DuckDbHeaderType();

    duckdb::vector<duckdb::Value> keys;
    duckdb::vector<duckdb::Value> values;
    keys.reserve(headers.size()); // Pre-allocate for efficiency
    values.reserve(headers.size());
    
    for (const auto &header : headers)
    {
        keys.emplace_back(header.first);
        values.emplace_back(header.second);
    }

    return duckdb::Value::MAP(
        MapType::KeyType(header_map_type),
        MapType::ValueType(header_map_type),
        keys,
        values
    );
}

std::string HttpResponse::Base64Encode(const std::string &input)
{
    auto result_str = std::string();
    result_str.resize(duckdb::Blob::ToBase64Size(input));

    duckdb::Blob::ToBase64(input, &result_str.front());

    return result_str;
}

std::vector<duckdb::Value> HttpResponse::ToRow() const
{
	auto conv = CharsetConverter(content_type);

    // For binary content types, convert to base64 to avoid UTF-8 validation issues
    std::string content_to_return;
    if (content_type.find("application/octet-stream") != std::string::npos ||
        content_type.find("application/pdf") != std::string::npos ||
        content_type.find("image/") != std::string::npos ||
        content_type.find("video/") != std::string::npos ||
        content_type.find("audio/") != std::string::npos ||
        content_type.find("font/") != std::string::npos) {
        // Convert binary content to base64
        content_to_return = "BINARY_CONTENT_BASE64:" + Base64Encode(content);
    } else {
        // Use normal charset conversion for text content
        content_to_return = conv.convert(content);
    }

    return {
        duckdb::Value(method.ToString()),
        duckdb::Value(code),
        duckdb::Value(url.ToString()),
        CreateHeaderMap(),
        duckdb::Value(content_type),
        duckdb::Value(content_to_return)
    };
}

int HttpResponse::Code() const {
    return code;
}

std::string HttpResponse::ContentType() const {
    return content_type;
}

std::string HttpResponse::Content() const {
    auto conv = CharsetConverter(content_type);
    return conv.convert(content);
}

// ----------------------------------------------------------------------

HttpClient::HttpClient(const HttpParams &http_params)
    : http_params(http_params)
{ }

HttpClient::HttpClient()
    : HttpClient(HttpParams())
{ }

std::unique_ptr<HttpResponse> HttpClient::SendRequest(HttpRequest &request)
{
    idx_t n_tries = 0;
    while (true)
    {
        std::exception_ptr caught_e = nullptr;
        duckdb_httplib_openssl::Error err;
		duckdb_httplib_openssl::Response response;
		int status;

        try {
            // Use the configured HTTP parameters rather than default-constructing new ones
            auto params = this->http_params;
            auto client = CreateHttplibClient(params, request.url.ToSchemeHostAndPort());
            auto res = request.Execute(*client);
            err = res.error();
            if (err == duckdb_httplib_openssl::Error::Success) {
                    status = res->status;
                    response = res.value();
            }
        } catch (IOException &e) {
			caught_e = std::current_exception();
		}

        if (err == duckdb_httplib_openssl::Error::Success)
        {
            switch (status) {
                case 408: // Request Timeout
                case 418: // Server is pretending to be a teapot
                case 429: // Rate limiter hit
                case 503: // Server has error
                case 504: // Server has error
                    break;
                default:
                    return HttpResponse::FromHttpLibResponse(request.method, request.url, response);
			}
        }

        n_tries += 1;
        if (n_tries >= http_params.retries)
        {
            if (caught_e) {
				std::rethrow_exception(caught_e);
			} else if (err == duckdb_httplib_openssl::Error::Success) {
				throw HTTPException(response, "Request returned HTTP %d for HTTP %s to '%s'",
                                    status, request.method.ToString(), request.url.ToString());
			} else {
				throw IOException("%s error for HTTP %s to '%s'", to_string(err),
                                  request.method.ToString(), request.url.ToString());
			}
        }
        else {
            if (n_tries > 1) {
                auto sleep_amount = CalculateSleepTime(n_tries);
				std::this_thread::sleep_for(std::chrono::milliseconds(sleep_amount));
            }
        }
    }
    return nullptr;
}

uint64_t HttpClient::CalculateSleepTime(idx_t n_tries)
{
    auto ret = ((float)http_params.retry_wait_ms * pow(http_params.retry_backoff, n_tries - 2));
    return (uint64_t)ret;
}

std::unique_ptr<HttpResponse> HttpClient::Head(const std::string &url)
{
    auto req = HttpRequest(HttpMethod::HEAD, url);
    return SendRequest(req);
}

std::unique_ptr<HttpResponse> HttpClient::Get(const std::string &url)
{
    ERPL_TRACE_DEBUG("HTTP_CLIENT", "Executing HTTP GET request to: " + url);
    auto req = HttpRequest(HttpMethod::GET, url);
    auto response = SendRequest(req);
    ERPL_TRACE_INFO("HTTP_CLIENT", "HTTP GET response received with status: " + std::to_string(response->Code()));
    return response;
}

std::unique_ptr<duckdb_httplib_openssl::Client> HttpClient::CreateHttplibClient(const HttpParams &http_params,
                                                                                const std::string &scheme_host_and_port)
{
    auto c = std::make_unique<duckdb_httplib_openssl::Client>(scheme_host_and_port.c_str());
    c->set_follow_location(true);
	c->set_keep_alive(http_params.keep_alive);
	c->enable_server_certificate_verification(false);
	c->set_write_timeout(http_params.timeout);
	c->set_read_timeout(http_params.timeout);
	c->set_connection_timeout(http_params.timeout);
	c->set_decompress(true);
	return c;
}

// ----------------------------------------------------------------------

HttpCache& HttpCache::GetInstance() {
    static HttpCache instance;
    return instance;
}

HttpCache::HttpCache() {
    cleanup_thread = std::thread(&HttpCache::GarbageCollection, this);
}

HttpCache::~HttpCache() {
    {
        std::unique_lock<std::mutex> lock(cleanup_mutex);
        should_stop = true;
    }
    cleanup_cv.notify_one();
    if (cleanup_thread.joinable()) {
        cleanup_thread.join();
    }
}

std::unique_ptr<HttpResponse> HttpCache::GetCachedResponse(const HttpRequest& request) {
    std::string cache_key = request.ToCacheKey();
    
    std::lock_guard<std::mutex> lock(cache_mutex);
    auto it = cache.find(cache_key);
    if (it != cache.end() && it->second.expiry > std::chrono::steady_clock::now()) {
        return std::make_unique<HttpResponse>(*it->second.response);
    }
    return nullptr;
}

void HttpCache::EmplaceCacheResponse(const HttpRequest& request, std::unique_ptr<HttpResponse> response, 
                                   const std::chrono::duration<double>& cache_duration) {
    std::string cache_key = request.ToCacheKey();
    auto expiry = std::chrono::steady_clock::now() + 
                 std::chrono::duration_cast<std::chrono::steady_clock::duration>(cache_duration);

    std::lock_guard<std::mutex> lock(cache_mutex);
    cache.emplace(cache_key, HttpCacheEntry(std::make_unique<HttpResponse>(*response), expiry));
}

bool HttpCache::IsInCache(const HttpRequest& request) const {
    std::string cache_key = request.ToCacheKey();
    
    std::lock_guard<std::mutex> lock(cache_mutex);
    auto it = cache.find(cache_key);
    auto found = (it != cache.end());
    auto now = std::chrono::steady_clock::now();
    bool valid = found && it->second.expiry > now;
    return valid;
}

void HttpCache::GarbageCollection() {
    while (true) {
        std::unique_lock<std::mutex> cleanup_lock(cleanup_mutex);
        cleanup_cv.wait_for(cleanup_lock, std::chrono::seconds(10), [this] { return should_stop; });
        
        if (should_stop) {
            break;
        }

        // Remove expired entries
        std::lock_guard<std::mutex> cache_lock(cache_mutex);
        auto now = std::chrono::steady_clock::now();
        for (auto it = cache.begin(); it != cache.end();) {
            if (it->second.expiry <= now) {
                it = cache.erase(it);
            } else {
                ++it;
            }
        }
    }
}

// ----------------------------------------------------------------------

CachingHttpClient::CachingHttpClient(std::shared_ptr<HttpClient> http_client, 
                                   const std::chrono::duration<double>& cache_duration)
    : http_client(std::move(http_client))
    , cache_duration(cache_duration) {}

std::shared_ptr<HttpClient> CachingHttpClient::GetHttpClient() {
    return http_client;
}

std::unique_ptr<HttpResponse> CachingHttpClient::Head(const std::string& url) {
    auto request = HttpRequest(HttpMethod::HEAD, url);
    return SendRequest(request);
}

std::unique_ptr<HttpResponse> CachingHttpClient::Get(const std::string& url) {
    auto request = HttpRequest(HttpMethod::GET, url);
    return SendRequest(request);
}

std::unique_ptr<HttpResponse> CachingHttpClient::SendRequest(HttpRequest& request) {
    auto& cache = HttpCache::GetInstance();
    
    // Try to get from cache first
    auto cached_response = cache.GetCachedResponse(request);
    if (cached_response) {
        return cached_response;
    }

    // If not in cache, forward to wrapped client
    auto response = http_client->SendRequest(request);
    
    // Cache the response if successful
    if (response && response->Code() >= 200 && response->Code() < 300) {
        cache.EmplaceCacheResponse(request, std::make_unique<HttpResponse>(*response), cache_duration);
    }
    
    return response;
}

bool CachingHttpClient::IsInCache(const HttpRequest& request) const {
    return HttpCache::GetInstance().IsInCache(request);
}

} // namespace erpl_web