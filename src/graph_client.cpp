#include "graph_client.hpp"
#include "duckdb/common/exception.hpp"
#include "tracing.hpp"

#include <cstdlib>
#include <iomanip>
#include <sstream>
#include <utility>

using namespace duckdb_yyjson;

namespace erpl_web {

namespace {
static constexpr size_t MAX_GRAPH_PAGES = 10000;

static void AppendJsonValue(std::string &target, yyjson_val *value) {
    size_t json_len = 0;
    char *json_str = yyjson_val_write(value, 0, &json_len);
    if (!json_str) {
        throw duckdb::IOException("Failed to serialize Microsoft Graph JSON value");
    }
    target.append(json_str, json_len);
    free(json_str);
}

} // namespace

GraphClient::GraphClient(std::shared_ptr<HttpAuthParams> auth_params, std::string trace_component)
    : auth_params(std::move(auth_params)), trace_component(std::move(trace_component)) {
    HttpParams http_params;
    http_params.url_encode = false;
    http_client = std::make_shared<HttpClient>(http_params);
}

std::string GraphClient::BaseUrl() {
    return "https://graph.microsoft.com/v1.0";
}

std::string GraphClient::UrlEncode(const std::string &value, bool preserve_slashes) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    for (unsigned char c : value) {
        const bool unreserved = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                                (c >= '0' && c <= '9') || c == '-' || c == '_' ||
                                c == '.' || c == '~';
        if (unreserved || (preserve_slashes && c == '/')) {
            escaped << c;
        } else {
            escaped << '%' << std::uppercase << std::setw(2) << int(c) << std::nouppercase;
        }
    }

    return escaped.str();
}

std::string GraphClient::StripLeadingSlash(const std::string &value) {
    if (!value.empty() && value[0] == '/') {
        return value.substr(1);
    }
    return value;
}

std::string GraphClient::EscapeODataStringLiteral(const std::string &value) {
    std::string result;
    result.reserve(value.size());
    for (char c : value) {
        if (c == '\'') {
            result += "''";
        } else {
            result += c;
        }
    }
    return result;
}

static void GraphCheckResponse(const std::unique_ptr<HttpResponse> &response,
                                const std::string &trace_component,
                                const std::string &method_label,
                                bool allow_no_content = false)
{
    if (!response) {
        std::string error_msg = "Microsoft Graph " + method_label + " request failed: no response";
        ERPL_TRACE_ERROR(trace_component, error_msg);
        throw duckdb::IOException(error_msg);
    }
    const int code = response->Code();
    if (code < 200 || code >= 300) {
        std::string error_msg = "Microsoft Graph " + method_label + " request failed (HTTP " +
                                std::to_string(code) + ")";
        if (!response->Content().empty()) {
            error_msg += ": " + response->Content().substr(0, 500);
        }
        ERPL_TRACE_ERROR(trace_component, error_msg);
        throw duckdb::IOException(error_msg);
    }
}

std::string GraphClient::Get(const std::string &url) {
    ERPL_TRACE_DEBUG(trace_component, "GET request to: " + url);

    HttpUrl http_url(url);
    HttpRequest request(HttpMethod::GET, http_url);
    if (auth_params) {
        request.AuthHeadersFromParams(*auth_params);
    }
    request.headers["Accept"] = "application/json";

    auto response = http_client->SendRequest(request);
    GraphCheckResponse(response, trace_component, "GET");

    ERPL_TRACE_DEBUG(trace_component, "Response received: " + std::to_string(response->Content().length()) + " bytes");
    return response->Content();
}

std::string GraphClient::Post(const std::string &url, const std::string &body) {
    return PostWithHeaders(url, body, {});
}

std::string GraphClient::PostWithHeaders(const std::string &url, const std::string &body,
                                         const std::map<std::string, std::string> &extra_headers)
{
    ERPL_TRACE_DEBUG(trace_component, "POST request to: " + url);

    HttpUrl http_url(url);
    HttpRequest request(HttpMethod::POST, http_url, "application/json", body);
    if (auth_params) {
        request.AuthHeadersFromParams(*auth_params);
    }
    request.headers["Accept"] = "application/json";
    for (const auto &kv : extra_headers) {
        request.headers[kv.first] = kv.second;
    }

    auto response = http_client->SendRequest(request);
    GraphCheckResponse(response, trace_component, "POST");

    ERPL_TRACE_DEBUG(trace_component, "POST response: " + std::to_string(response->Content().length()) + " bytes");
    return response->Content();
}

void GraphClient::Patch(const std::string &url, const std::string &body) {
    PatchWithHeaders(url, body, {});
}

void GraphClient::PatchWithHeaders(const std::string &url, const std::string &body,
                                    const std::map<std::string, std::string> &extra_headers) {
    ERPL_TRACE_DEBUG(trace_component, "PATCH request to: " + url);

    HttpUrl http_url(url);
    HttpRequest request(HttpMethod::PATCH, http_url, "application/json", body);
    if (auth_params) {
        request.AuthHeadersFromParams(*auth_params);
    }
    request.headers["Accept"] = "application/json";
    for (const auto &kv : extra_headers) {
        request.headers[kv.first] = kv.second;
    }

    auto response = http_client->SendRequest(request);
    GraphCheckResponse(response, trace_component, "PATCH");
}

void GraphClient::Delete(const std::string &url) {
    DeleteWithHeaders(url, {});
}

void GraphClient::DeleteWithHeaders(const std::string &url,
                                    const std::map<std::string, std::string> &extra_headers)
{
    ERPL_TRACE_DEBUG(trace_component, "DELETE request to: " + url);

    HttpUrl http_url(url);
    HttpRequest request(HttpMethod::_DELETE, http_url);
    if (auth_params) {
        request.AuthHeadersFromParams(*auth_params);
    }
    for (const auto &kv : extra_headers) {
        request.headers[kv.first] = kv.second;
    }

    auto response = http_client->SendRequest(request);
    GraphCheckResponse(response, trace_component, "DELETE");
}

std::optional<std::string> GraphClient::ExtractNextLink(const std::string &json_body) {
    auto doc = std::shared_ptr<yyjson_doc>(yyjson_read(json_body.c_str(), json_body.size(), 0), yyjson_doc_free);
    if (!doc) {
        throw duckdb::IOException("Failed to parse Microsoft Graph response while checking pagination");
    }

    auto root = yyjson_doc_get_root(doc.get());
    auto next = root ? yyjson_obj_get(root, "@odata.nextLink") : nullptr;
    if (next && yyjson_is_str(next)) {
        return std::string(yyjson_get_str(next));
    }
    return std::nullopt;
}

std::string GraphClient::GetAllPagesMerged(const std::string &url) {
    std::vector<std::string> pages;
    pages.reserve(1);

    std::string next_url = url;
    for (size_t page_count = 0; !next_url.empty(); page_count++) {
        if (page_count >= MAX_GRAPH_PAGES) {
            throw duckdb::IOException("Microsoft Graph pagination exceeded safety limit");
        }
        auto body = Get(next_url);
        auto next_link = ExtractNextLink(body);
        pages.push_back(std::move(body));
        next_url = next_link.value_or("");
    }

    if (pages.size() <= 1) {
        return pages.empty() ? "{}" : pages[0];
    }

    std::string merged = "{\"value\":[";
    bool first_item = true;

    for (const auto &page : pages) {
        auto doc = std::shared_ptr<yyjson_doc>(yyjson_read(page.c_str(), page.size(), 0), yyjson_doc_free);
        if (!doc) {
            throw duckdb::IOException("Failed to parse Microsoft Graph paged response");
        }

        auto root = yyjson_doc_get_root(doc.get());
        auto value_arr = root ? yyjson_obj_get(root, "value") : nullptr;
        if (!value_arr || !yyjson_is_arr(value_arr)) {
            return pages[0];
        }

        size_t idx, max;
        yyjson_val *item;
        yyjson_arr_foreach(value_arr, idx, max, item) {
            if (!first_item) {
                merged += ",";
            }
            AppendJsonValue(merged, item);
            first_item = false;
        }
    }

    merged += "]}";
    return merged;
}

std::string GraphClient::ResolveUserSegment(const std::string &user) {
    if (user.empty()) {
        return "me";
    }
    // GUID (8-4-4-4-12, 36 chars): safe for URL paths, no encoding needed.
    if (user.size() == 36 && user[8] == '-' && user[13] == '-' &&
        user[18] == '-' && user[23] == '-') {
        return "users/" + user;
    }
    // UPN (user@tenant.com) or email: percent-encode @ -> %40 so the segment
    // is valid in a URL path. Dots and hyphens are RFC 3986 unreserved.
    return "users/" + UrlEncode(user);
}

std::string GraphJsonGetString(yyjson_val *obj, const char *key) {
    auto val = obj ? yyjson_obj_get(obj, key) : nullptr;
    if (val && yyjson_is_str(val)) {
        return yyjson_get_str(val);
    }
    return "";
}

bool GraphJsonGetBool(yyjson_val *obj, const char *key, bool default_value) {
    auto val = obj ? yyjson_obj_get(obj, key) : nullptr;
    if (val && yyjson_is_bool(val)) {
        return yyjson_get_bool(val);
    }
    return default_value;
}

std::vector<std::string> GraphJsonStringArray(yyjson_val *arr) {
    std::vector<std::string> result;
    if (!arr || !yyjson_is_arr(arr)) {
        return result;
    }

    size_t idx, max;
    yyjson_val *val;
    yyjson_arr_foreach(arr, idx, max, val) {
        if (yyjson_is_str(val)) {
            result.emplace_back(yyjson_get_str(val));
        } else if (yyjson_is_null(val)) {
            result.emplace_back("");
        } else if (yyjson_is_num(val)) {
            result.emplace_back(std::to_string(yyjson_get_num(val)));
        } else if (yyjson_is_bool(val)) {
            result.emplace_back(yyjson_get_bool(val) ? "true" : "false");
        } else {
            result.emplace_back("");
        }
    }
    return result;
}

} // namespace erpl_web
