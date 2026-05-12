#include "graph_sharepoint_client.hpp"
#include "graph_client.hpp"
#include "tracing.hpp"
#include "duckdb/common/exception.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace erpl_web {

// URL Builder implementation
std::string GraphSharePointUrlBuilder::GetBaseUrl() {
    return GraphClient::BaseUrl();
}

std::string GraphSharePointUrlBuilder::BuildSitesSearchUrl(const std::string &search_query) {
    if (search_query.empty()) {
        return GetBaseUrl() + "/sites?search=*";
    }
    return GetBaseUrl() + "/sites?search=" + GraphClient::UrlEncode(search_query);
}

std::string GraphSharePointUrlBuilder::BuildSiteUrl(const std::string &site_id) {
    return GetBaseUrl() + "/sites/" + site_id;
}

std::string GraphSharePointUrlBuilder::BuildSiteListsUrl(const std::string &site_id) {
    return GetBaseUrl() + "/sites/" + site_id + "/lists";
}

std::string GraphSharePointUrlBuilder::BuildListUrl(const std::string &site_id, const std::string &list_id) {
    return GetBaseUrl() + "/sites/" + site_id + "/lists/" + list_id;
}

std::string GraphSharePointUrlBuilder::BuildListColumnsUrl(const std::string &site_id, const std::string &list_id) {
    return GetBaseUrl() + "/sites/" + site_id + "/lists/" + list_id + "/columns";
}

std::string GraphSharePointUrlBuilder::BuildListItemsUrl(const std::string &site_id, const std::string &list_id) {
    return GetBaseUrl() + "/sites/" + site_id + "/lists/" + list_id + "/items";
}

std::string GraphSharePointUrlBuilder::BuildListItemsWithFieldsUrl(const std::string &site_id, const std::string &list_id) {
    return GetBaseUrl() + "/sites/" + site_id + "/lists/" + list_id + "/items?expand=fields";
}

std::string GraphSharePointUrlBuilder::BuildListItemsWithSelectUrl(const std::string &site_id, const std::string &list_id,
                                                                    const std::string &select, int top) {
    std::string url = GetBaseUrl() + "/sites/" + site_id + "/lists/" + list_id + "/items?expand=fields";

    if (!select.empty()) {
        url += "&$select=" + select;
    }

    if (top > 0) {
        url += "&$top=" + std::to_string(top);
    }

    return url;
}

std::string GraphSharePointUrlBuilder::BuildItemUrl(const std::string &site_id, const std::string &list_id, const std::string &item_id) {
    return GetBaseUrl() + "/sites/" + site_id + "/lists/" + list_id + "/items/" + item_id;
}

std::string GraphSharePointUrlBuilder::BuildItemFieldsUrl(const std::string &site_id, const std::string &list_id, const std::string &item_id) {
    return GetBaseUrl() + "/sites/" + site_id + "/lists/" + list_id + "/items/" + item_id + "/fields";
}

std::string GraphSharePointUrlBuilder::BuildFollowedSitesUrl() {
    return GetBaseUrl() + "/me/followedSites";
}

std::string GraphSharePointUrlBuilder::BuildSiteDrivesUrl(const std::string &site_id) {
    return GetBaseUrl() + "/sites/" + site_id + "/drives";
}

std::string GraphSharePointUrlBuilder::BuildSiteByPathUrl(const std::string &hostname, const std::string &site_path) {
    if (site_path.empty()) {
        return GetBaseUrl() + "/sites/" + hostname;
    }
    // Ensure path doesn't start with /
    std::string clean_path = GraphClient::StripLeadingSlash(site_path);
    return GetBaseUrl() + "/sites/" + hostname + ":/" + GraphClient::UrlEncode(clean_path, true) + ":";
}

// GraphSharePointClient implementation
GraphSharePointClient::GraphSharePointClient(std::shared_ptr<HttpAuthParams> auth_params)
    : auth_params(auth_params) {
}

std::string GraphSharePointClient::DoGraphGet(const std::string &url) {
    return GraphClient(auth_params, "GRAPH_SHAREPOINT").Get(url);
}

std::string GraphSharePointClient::SearchSites(const std::string &search_query) {
    auto url = GraphSharePointUrlBuilder::BuildSitesSearchUrl(search_query);
    return GraphClient(auth_params, "GRAPH_SHAREPOINT").GetAllPagesMerged(url);
}

std::string GraphSharePointClient::GetFollowedSites() {
    auto url = GraphSharePointUrlBuilder::BuildFollowedSitesUrl();
    return GraphClient(auth_params, "GRAPH_SHAREPOINT").GetAllPagesMerged(url);
}

std::string GraphSharePointClient::GetSite(const std::string &site_id) {
    auto url = GraphSharePointUrlBuilder::BuildSiteUrl(site_id);
    return DoGraphGet(url);
}

std::string GraphSharePointClient::GetSiteByPath(const std::string &hostname, const std::string &site_path) {
    auto url = GraphSharePointUrlBuilder::BuildSiteByPathUrl(hostname, site_path);
    return DoGraphGet(url);
}

std::string GraphSharePointClient::ListDrives(const std::string &site_id) {
    auto url = GraphSharePointUrlBuilder::BuildSiteDrivesUrl(site_id);
    return GraphClient(auth_params, "GRAPH_SHAREPOINT").GetAllPagesMerged(url);
}

std::string GraphSharePointClient::ListLists(const std::string &site_id) {
    auto url = GraphSharePointUrlBuilder::BuildSiteListsUrl(site_id);
    return GraphClient(auth_params, "GRAPH_SHAREPOINT").GetAllPagesMerged(url);
}

std::string GraphSharePointClient::GetList(const std::string &site_id, const std::string &list_id) {
    auto url = GraphSharePointUrlBuilder::BuildListUrl(site_id, list_id);
    return DoGraphGet(url);
}

std::string GraphSharePointClient::GetListColumns(const std::string &site_id, const std::string &list_id) {
    auto url = GraphSharePointUrlBuilder::BuildListColumnsUrl(site_id, list_id);
    return GraphClient(auth_params, "GRAPH_SHAREPOINT").GetAllPagesMerged(url);
}

std::string GraphSharePointClient::GetListItems(const std::string &site_id, const std::string &list_id,
                                                 const std::string &select, int top) {
    auto url = GraphSharePointUrlBuilder::BuildListItemsWithSelectUrl(site_id, list_id, select, top);
    return GraphClient(auth_params, "GRAPH_SHAREPOINT").GetAllPagesMerged(url);
}

std::string GraphSharePointClient::CreateListItem(const std::string &site_id, const std::string &list_id,
                                                   const std::string &fields_json)
{
    const std::string url = GraphSharePointUrlBuilder::BuildListItemsUrl(site_id, list_id);
    const std::string body = "{\"fields\":" + fields_json + "}";
    const std::string response = GraphClient(auth_params, "GRAPH_SHAREPOINT").Post(url, body);

    // Extract the new item id from the response
    yyjson_doc *doc = yyjson_read(response.c_str(), response.size(), 0);
    if (!doc) {
        return "";
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *id_val = yyjson_obj_get(root, "id");
    std::string new_id;
    if (id_val && yyjson_is_str(id_val)) {
        new_id = yyjson_get_str(id_val);
    }
    yyjson_doc_free(doc);
    return new_id;
}

void GraphSharePointClient::UpdateListItem(const std::string &site_id, const std::string &list_id,
                                           const std::string &item_id, const std::string &fields_json)
{
    const std::string url = GraphSharePointUrlBuilder::BuildItemFieldsUrl(site_id, list_id, item_id);
    GraphClient(auth_params, "GRAPH_SHAREPOINT").Patch(url, fields_json);
}

void GraphSharePointClient::DeleteListItem(const std::string &site_id, const std::string &list_id,
                                           const std::string &item_id)
{
    const std::string url = GraphSharePointUrlBuilder::BuildItemUrl(site_id, list_id, item_id);
    GraphClient(auth_params, "GRAPH_SHAREPOINT").Delete(url);
}

bool GraphSharePointClient::LooksLikeSiteId(const std::string &s) {
    return s.find(',') != std::string::npos;
}

bool GraphSharePointClient::LooksLikeDriveId(const std::string &s) {
    // SharePoint drive IDs start with 'b!' (base64-encoded)
    if (s.size() >= 2 && s[0] == 'b' && s[1] == '!') {
        return true;
    }
    // Or they can be bare GUIDs: 36 chars with dashes at positions 8, 13, 18, 23
    if (s.size() == 36 && s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-') {
        return true;
    }
    return false;
}

std::string GraphSharePointClient::ResolveSiteId(const std::string &name_or_id) {
    if (LooksLikeSiteId(name_or_id)) {
        return name_or_id;
    }

    // Accept web URLs: https://tenant.sharepoint.com  or  https://tenant.sharepoint.com/sites/name
    const std::string https_prefix = "https://";
    const std::string http_prefix  = "http://";
    if (name_or_id.rfind(https_prefix, 0) == 0 || name_or_id.rfind(http_prefix, 0) == 0) {
        ERPL_TRACE_DEBUG("GRAPH_SHAREPOINT", "Resolving site URL to ID: " + name_or_id);

        std::string rest = name_or_id.rfind(https_prefix, 0) == 0
            ? name_or_id.substr(https_prefix.size())
            : name_or_id.substr(http_prefix.size());

        // Split off trailing slash
        if (!rest.empty() && rest.back() == '/') {
            rest.pop_back();
        }

        const auto slash_pos = rest.find('/');
        std::string hostname  = (slash_pos == std::string::npos) ? rest : rest.substr(0, slash_pos);
        std::string site_path = (slash_pos == std::string::npos) ? "" : rest.substr(slash_pos);

        auto json = GetSiteByPath(hostname, site_path);

        yyjson_doc *doc = yyjson_read(json.c_str(), json.size(), 0);
        if (!doc) {
            throw duckdb::InvalidInputException("Failed to parse site response for URL: %s", name_or_id.c_str());
        }
        yyjson_val *root = yyjson_doc_get_root(doc);
        yyjson_val *id_val = yyjson_obj_get(root, "id");
        std::string resolved;
        if (id_val && yyjson_is_str(id_val)) {
            resolved = yyjson_get_str(id_val);
        }
        yyjson_doc_free(doc);

        if (resolved.empty()) {
            throw duckdb::InvalidInputException("Could not resolve site ID for URL: %s", name_or_id.c_str());
        }
        ERPL_TRACE_DEBUG("GRAPH_SHAREPOINT", "Resolved URL '" + name_or_id + "' → " + resolved);
        return resolved;
    }

    ERPL_TRACE_DEBUG("GRAPH_SHAREPOINT", "Resolving site name to ID: " + name_or_id);
    auto json = SearchSites(name_or_id);

    yyjson_doc *doc = yyjson_read(json.c_str(), json.size(), 0);
    if (!doc) {
        throw duckdb::InvalidInputException("Failed to parse site search response for: %s", name_or_id.c_str());
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    if (!value_arr || !yyjson_is_arr(value_arr) || yyjson_arr_size(value_arr) == 0) {
        yyjson_doc_free(doc);
        throw duckdb::InvalidInputException("No SharePoint site found matching: %s", name_or_id.c_str());
    }

    yyjson_val *first = yyjson_arr_get_first(value_arr);
    yyjson_val *id_val = yyjson_obj_get(first, "id");

    std::string resolved;
    if (id_val && yyjson_is_str(id_val)) {
        resolved = yyjson_get_str(id_val);
    }
    yyjson_doc_free(doc);

    if (resolved.empty()) {
        throw duckdb::InvalidInputException("Site search returned no id for: %s", name_or_id.c_str());
    }

    ERPL_TRACE_DEBUG("GRAPH_SHAREPOINT", "Resolved site '" + name_or_id + "' → " + resolved);
    return resolved;
}

bool GraphSharePointClient::LooksLikeGuid(const std::string &s) {
    return s.size() == 36 && s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-';
}

std::string GraphSharePointClient::ResolveListId(const std::string &site_id, const std::string &name_or_id) {
    if (LooksLikeGuid(name_or_id)) {
        return name_or_id;
    }

    ERPL_TRACE_DEBUG("GRAPH_SHAREPOINT", "Resolving list name '" + name_or_id + "' on site " + site_id);
    auto json = ListLists(site_id);

    yyjson_doc *doc = yyjson_read(json.c_str(), json.size(), 0);
    if (!doc) {
        throw duckdb::InvalidInputException("Failed to parse lists response when resolving list: %s", name_or_id.c_str());
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    std::string resolved;
    if (value_arr && yyjson_is_arr(value_arr)) {
        size_t idx, max;
        yyjson_val *list;
        yyjson_arr_foreach(value_arr, idx, max, list) {
            yyjson_val *name_val    = yyjson_obj_get(list, "name");
            yyjson_val *display_name = yyjson_obj_get(list, "displayName");
            yyjson_val *id_val      = yyjson_obj_get(list, "id");
            if (!id_val || !yyjson_is_str(id_val)) {
                continue;
            }
            // Match against either the internal 'name' or the 'displayName' so
            // that both the value shown in the 'name' column by graph_show_lists
            // and the localised display name work as input.
            bool name_match    = name_val    && yyjson_is_str(name_val)    && name_or_id == yyjson_get_str(name_val);
            bool display_match = display_name && yyjson_is_str(display_name) && name_or_id == yyjson_get_str(display_name);
            if (name_match || display_match) {
                resolved = yyjson_get_str(id_val);
                break;
            }
        }
    }
    yyjson_doc_free(doc);

    if (resolved.empty()) {
        throw duckdb::InvalidInputException("No SharePoint list found with name '%s' on site '%s'",
                                            name_or_id.c_str(), site_id.c_str());
    }

    ERPL_TRACE_DEBUG("GRAPH_SHAREPOINT", "Resolved list '" + name_or_id + "' → " + resolved);
    return resolved;
}

std::string GraphSharePointClient::ResolveDriveIdFromUrl(const std::string &web_url) {
    ERPL_TRACE_DEBUG("GRAPH_SHAREPOINT", "Resolving drive ID from web URL: " + web_url);

    // Extract hostname to resolve the containing site
    const std::string https_prefix = "https://";
    const std::string http_prefix  = "http://";
    std::string rest = web_url.rfind(https_prefix, 0) == 0
        ? web_url.substr(https_prefix.size())
        : web_url.substr(http_prefix.size());

    const auto slash_pos = rest.find('/');
    const std::string hostname = (slash_pos == std::string::npos) ? rest : rest.substr(0, slash_pos);

    // Resolve the root site for this hostname, then list its drives
    const std::string site_id = ResolveSiteId("https://" + hostname);
    const auto json = ListDrives(site_id);

    yyjson_doc *doc = yyjson_read(json.c_str(), json.size(), 0);
    if (!doc) {
        throw duckdb::InvalidInputException("Failed to parse drives response when resolving URL: %s", web_url.c_str());
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    std::string resolved;
    if (value_arr && yyjson_is_arr(value_arr)) {
        size_t idx, max;
        yyjson_val *item;
        yyjson_arr_foreach(value_arr, idx, max, item) {
            yyjson_val *url_val = yyjson_obj_get(item, "webUrl");
            if (!url_val || !yyjson_is_str(url_val)) {
                continue;
            }
            // Compare URLs case-insensitively after removing trailing slashes
            std::string candidate = yyjson_get_str(url_val);
            if (!candidate.empty() && candidate.back() == '/') {
                candidate.pop_back();
            }
            std::string input_url = web_url;
            if (!input_url.empty() && input_url.back() == '/') {
                input_url.pop_back();
            }
            if (candidate == input_url) {
                yyjson_val *id_val = yyjson_obj_get(item, "id");
                if (id_val && yyjson_is_str(id_val)) {
                    resolved = yyjson_get_str(id_val);
                }
                break;
            }
        }
    }
    yyjson_doc_free(doc);

    if (resolved.empty()) {
        throw duckdb::InvalidInputException(
            "No drive found with web URL '%s'. Use graph_show_drives() to list available drives.",
            web_url.c_str());
    }

    ERPL_TRACE_DEBUG("GRAPH_SHAREPOINT", "Resolved drive URL '" + web_url + "' → " + resolved);
    return resolved;
}

std::string GraphSharePointClient::ResolveDriveId(const std::string &site_id, const std::string &name_or_id) {
    if (LooksLikeDriveId(name_or_id)) {
        return name_or_id;
    }

    ERPL_TRACE_DEBUG("GRAPH_SHAREPOINT", "Resolving drive name to ID: " + name_or_id);
    auto json = ListDrives(site_id);

    yyjson_doc *doc = yyjson_read(json.c_str(), json.size(), 0);
    if (!doc) {
        throw duckdb::InvalidInputException("Failed to parse drives response for site: %s", site_id.c_str());
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    if (!value_arr || !yyjson_is_arr(value_arr)) {
        yyjson_doc_free(doc);
        throw duckdb::InvalidInputException("No drives found for site: %s", site_id.c_str());
    }

    if (name_or_id.empty()) {
        // Auto-select when there is exactly one drive
        if (yyjson_arr_size(value_arr) == 1) {
            yyjson_val *only = yyjson_arr_get_first(value_arr);
            yyjson_val *id_val = yyjson_obj_get(only, "id");
            std::string resolved;
            if (id_val && yyjson_is_str(id_val)) {
                resolved = yyjson_get_str(id_val);
            }
            yyjson_doc_free(doc);
            ERPL_TRACE_DEBUG("GRAPH_SHAREPOINT", "Auto-selected single drive → " + resolved);
            return resolved;
        }
        yyjson_doc_free(doc);
        throw duckdb::InvalidInputException(
            "Multiple drives found for site '%s'. Specify a drive name via the 'drive' parameter.", site_id.c_str());
    }

    size_t idx, max;
    yyjson_val *item;
    std::string resolved;

    yyjson_arr_foreach(value_arr, idx, max, item) {
        yyjson_val *name_val = yyjson_obj_get(item, "name");
        if (name_val && yyjson_is_str(name_val) && name_or_id == yyjson_get_str(name_val)) {
            yyjson_val *id_val = yyjson_obj_get(item, "id");
            if (id_val && yyjson_is_str(id_val)) {
                resolved = yyjson_get_str(id_val);
            }
            break;
        }
    }
    yyjson_doc_free(doc);

    if (resolved.empty()) {
        throw duckdb::InvalidInputException("No drive named '%s' found in site '%s'.",
                                            name_or_id.c_str(), site_id.c_str());
    }

    ERPL_TRACE_DEBUG("GRAPH_SHAREPOINT", "Resolved drive '" + name_or_id + "' → " + resolved);
    return resolved;
}

} // namespace erpl_web
