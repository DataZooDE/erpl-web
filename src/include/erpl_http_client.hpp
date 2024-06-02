#pragma once

#include "duckdb.hpp"
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

using namespace duckdb;

namespace erpl_web
{

using HeaderMap = case_insensitive_map_t<string>;
class HttpClient; // forward declaration

template <typename T>
std::basic_string<T> ToLower(const std::basic_string<T>& s)
{
    std::basic_string<T> s2 = s;
    std::transform(s2.begin(), s2.end(), s2.begin(),
        [](const T v){ return static_cast<T>(std::tolower(v)); });
    return s2;
}

// ----------------------------------------------------------------------

class HttpUrl {
public:
    HttpUrl(const std::string& url);
    void ParseUrl(const std::string& url);
    std::string ToSchemeHostAndPort() const;
    std::string ToPathQueryFragment() const;
    std::string ToString() const;
    bool Equals(const HttpUrl& other) const;

    void Scheme(const std::string& value);
    void Host(const std::string& value);
    void Port(const std::string& value);
    void Path(const std::string& value);
    void Query(const std::string& value);
    void Fragment(const std::string& value);
    void Username(const std::string& value);
    void Password(const std::string& value);

    std::string Scheme() const;
    std::string Host() const;
    std::string Port() const;
    std::string Path() const;
    std::string Query() const;
    std::string Fragment() const;
    std::string Username() const;
    std::string Password() const;

private:
    std::string scheme;
    std::string host;
    std::string port;
    std::string path;
    std::string query;
    std::string fragment;
    std::string username;
    std::string password;

    static std::string ToLower(const std::string& str);
};

// ----------------------------------------------------------------------

struct HttpParams {

	static constexpr uint64_t DEFAULT_TIMEOUT = 30000; // 30 sec
	static constexpr uint64_t DEFAULT_RETRIES = 3;
	static constexpr uint64_t DEFAULT_RETRY_WAIT_MS = 100;
	static constexpr float DEFAULT_RETRY_BACKOFF = 4;
	static constexpr bool DEFAULT_FORCE_DOWNLOAD = false;
	static constexpr bool DEFAULT_KEEP_ALIVE = true;

    HttpParams();

	uint64_t timeout;
	uint64_t retries;
	uint64_t retry_wait_ms;
	float retry_backoff;
	bool force_download;
	bool keep_alive;
};

// ----------------------------------------------------------------------

class HttpMethod 
{
public:
    enum Variants : uint8_t
    {
        UNDEFINED,
        GET,
        POST,
        PUT,
        DELETE,
        PATCH,
        HEAD,
        OPTIONS,
        TRACE,
        CONNECT
    };

    HttpMethod() = default;
    constexpr HttpMethod(Variants ret_type) : variant(ret_type) { }
    constexpr bool operator==(HttpMethod a) const { return variant == a.variant; }
    constexpr bool operator!=(HttpMethod a) const { return variant != a.variant; }

    constexpr bool IsUndefined() const { return variant == UNDEFINED; }

    static HttpMethod FromString(const std::string &method);
    std::string ToString() const;

private:
    Variants variant;
};

// ----------------------------------------------------------------------

class HttpRequest
{
friend class HttpClient;

public:
    HttpRequest(HttpMethod method, const std::string &url, std::string content_type, std::string content);
    HttpRequest(HttpMethod method, const std::string &url);

    void HeadersFromMapArg(duckdb::Value &header_map);
    void HeadersFromMapArg(const duckdb::Value &header_map);

public:
    HttpMethod method;
    HttpUrl url;

    HeaderMap headers;
    std::string content_type;
    std::string content;

private:
    duckdb_httplib_openssl::Headers HttplibHeaders();
    duckdb_httplib_openssl::Result Execute(duckdb_httplib_openssl::Client &client);
};

// ----------------------------------------------------------------------

class HttpResponse
{
friend class HttpClient;

public:
    HttpResponse(HttpMethod method, HttpUrl url, int code, std::string content_type, std::string content);
    HttpResponse(HttpMethod method, HttpUrl url, int code);

    static duckdb::LogicalType DuckDbResponseType();
    static std::vector<std::string> DuckDbResponseNames();
    static duckdb::LogicalType DuckDbHeaderType();
    duckdb::Value ToValue() const;
    std::vector<duckdb::Value> ToRow() const;

public:
    HttpMethod method;
    HttpUrl url;

    int code;
    HeaderMap headers;
    std::string content_type;
    std::string content;

private:
    static std::unique_ptr<HttpResponse> FromHttpLibResponse(HttpMethod &method,
                                                             HttpUrl &url,
                                                             duckdb_httplib_openssl::Response &response);
    
    duckdb::Value CreateHeaderMap() const;
};

// ----------------------------------------------------------------------

class HttpClient
{
public:
    HttpClient();
    HttpClient(const HttpParams &http_params);

public:
    std::unique_ptr<HttpResponse> Head(const std::string &url);
    std::unique_ptr<HttpResponse> Get(const std::string &url);
    
    std::unique_ptr<HttpResponse> SendRequest(HttpRequest &request);
private:
    HttpParams http_params;

private:
    std::unique_ptr<duckdb_httplib_openssl::Client> CreateHttplibClient(const HttpParams &http_params,
                                                                        const std::string &scheme_host_and_port);

    uint64_t CalculateSleepTime(idx_t n_tries);
};

} // namespace erpl_web
