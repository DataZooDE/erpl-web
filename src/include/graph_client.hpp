#pragma once

#include "http_client.hpp"
#include "yyjson.hpp"
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace erpl_web {

class GraphClient {
public:
    GraphClient(std::shared_ptr<HttpAuthParams> auth_params, std::string trace_component);

    std::string Get(const std::string &url);
    std::string GetAllPagesMerged(const std::string &url);

    std::string Post(const std::string &url, const std::string &body);
    std::string PostWithHeaders(const std::string &url, const std::string &body,
                                const std::map<std::string, std::string> &extra_headers);
    void Patch(const std::string &url, const std::string &body);
    void PatchWithHeaders(const std::string &url, const std::string &body,
                          const std::map<std::string, std::string> &extra_headers);
    void Delete(const std::string &url);
    void DeleteWithHeaders(const std::string &url, const std::map<std::string, std::string> &extra_headers);

    static std::string BaseUrl();
    static std::string UrlEncode(const std::string &value, bool preserve_slashes = false);
    static std::string StripLeadingSlash(const std::string &value);
    static std::string EscapeODataStringLiteral(const std::string &value);
    static std::optional<std::string> ExtractNextLink(const std::string &json_body);

    // Returns the URL path component for a user: "me" when user is empty,
    // "users/{id}" for a GUID, or "users/{encoded}" for a UPN/email (@ -> %40).
    // Use as: BaseUrl() + "/" + ResolveUserSegment(user) + "/events"
    static std::string ResolveUserSegment(const std::string &user);

private:
    std::shared_ptr<HttpAuthParams> auth_params;
    std::shared_ptr<HttpClient> http_client;
    std::string trace_component;
};

std::string GraphJsonGetString(duckdb_yyjson::yyjson_val *obj, const char *key);
bool GraphJsonGetBool(duckdb_yyjson::yyjson_val *obj, const char *key, bool default_value = false);
std::vector<std::string> GraphJsonStringArray(duckdb_yyjson::yyjson_val *arr);

} // namespace erpl_web
