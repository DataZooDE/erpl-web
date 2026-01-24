#include "graph_sharepoint_client.hpp"
#include "tracing.hpp"

namespace erpl_web {

// URL Builder implementation
std::string GraphSharePointUrlBuilder::GetBaseUrl() {
    return "https://graph.microsoft.com/v1.0";
}

std::string GraphSharePointUrlBuilder::BuildSitesSearchUrl(const std::string &search_query) {
    if (search_query.empty()) {
        return GetBaseUrl() + "/sites?search=*";
    }
    return GetBaseUrl() + "/sites?search=" + search_query;
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

std::string GraphSharePointUrlBuilder::BuildFollowedSitesUrl() {
    return GetBaseUrl() + "/me/followedSites";
}

std::string GraphSharePointUrlBuilder::BuildSiteByPathUrl(const std::string &hostname, const std::string &site_path) {
    if (site_path.empty()) {
        return GetBaseUrl() + "/sites/" + hostname;
    }
    // Ensure path doesn't start with /
    std::string clean_path = site_path;
    if (!clean_path.empty() && clean_path[0] == '/') {
        clean_path = clean_path.substr(1);
    }
    return GetBaseUrl() + "/sites/" + hostname + ":/" + clean_path + ":";
}

// GraphSharePointClient implementation
GraphSharePointClient::GraphSharePointClient(std::shared_ptr<HttpAuthParams> auth_params)
    : auth_params(auth_params) {
    HttpParams http_params;
    http_params.url_encode = false;
    http_client = std::make_shared<HttpClient>(http_params);
}

std::string GraphSharePointClient::DoGraphGet(const std::string &url) {
    ERPL_TRACE_DEBUG("GRAPH_SHAREPOINT", "GET request to: " + url);

    HttpUrl http_url(url);
    HttpRequest request(HttpMethod::GET, http_url);

    if (auth_params) {
        request.AuthHeadersFromParams(*auth_params);
    }

    request.headers["Accept"] = "application/json";

    auto response = http_client->SendRequest(request);

    if (!response || response->Code() != 200) {
        std::string error_msg = "Graph API request failed";
        if (response) {
            error_msg += " (HTTP " + std::to_string(response->Code()) + ")";
            if (!response->Content().empty()) {
                error_msg += ": " + response->Content().substr(0, 500);
            }
        }
        ERPL_TRACE_ERROR("GRAPH_SHAREPOINT", error_msg);
        throw std::runtime_error(error_msg);
    }

    ERPL_TRACE_DEBUG("GRAPH_SHAREPOINT", "Response received: " + std::to_string(response->Content().length()) + " bytes");
    return response->Content();
}

std::string GraphSharePointClient::SearchSites(const std::string &search_query) {
    auto url = GraphSharePointUrlBuilder::BuildSitesSearchUrl(search_query);
    return DoGraphGet(url);
}

std::string GraphSharePointClient::GetFollowedSites() {
    auto url = GraphSharePointUrlBuilder::BuildFollowedSitesUrl();
    return DoGraphGet(url);
}

std::string GraphSharePointClient::GetSite(const std::string &site_id) {
    auto url = GraphSharePointUrlBuilder::BuildSiteUrl(site_id);
    return DoGraphGet(url);
}

std::string GraphSharePointClient::GetSiteByPath(const std::string &hostname, const std::string &site_path) {
    auto url = GraphSharePointUrlBuilder::BuildSiteByPathUrl(hostname, site_path);
    return DoGraphGet(url);
}

std::string GraphSharePointClient::ListLists(const std::string &site_id) {
    auto url = GraphSharePointUrlBuilder::BuildSiteListsUrl(site_id);
    return DoGraphGet(url);
}

std::string GraphSharePointClient::GetList(const std::string &site_id, const std::string &list_id) {
    auto url = GraphSharePointUrlBuilder::BuildListUrl(site_id, list_id);
    return DoGraphGet(url);
}

std::string GraphSharePointClient::GetListColumns(const std::string &site_id, const std::string &list_id) {
    auto url = GraphSharePointUrlBuilder::BuildListColumnsUrl(site_id, list_id);
    return DoGraphGet(url);
}

std::string GraphSharePointClient::GetListItems(const std::string &site_id, const std::string &list_id,
                                                 const std::string &select, int top) {
    auto url = GraphSharePointUrlBuilder::BuildListItemsWithSelectUrl(site_id, list_id, select, top);
    return DoGraphGet(url);
}

} // namespace erpl_web
