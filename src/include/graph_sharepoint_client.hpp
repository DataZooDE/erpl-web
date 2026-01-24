#pragma once

#include "http_client.hpp"
#include <string>
#include <memory>
#include <vector>

namespace erpl_web {

// URL builder for Microsoft Graph SharePoint API endpoints
class GraphSharePointUrlBuilder {
public:
    // Base Graph API URL
    static std::string GetBaseUrl();

    // Build URL for listing SharePoint sites
    // /sites?search={query}
    static std::string BuildSitesSearchUrl(const std::string &search_query = "");

    // Build URL for getting a site by ID
    // /sites/{site-id}
    static std::string BuildSiteUrl(const std::string &site_id);

    // Build URL for listing lists in a site
    // /sites/{site-id}/lists
    static std::string BuildSiteListsUrl(const std::string &site_id);

    // Build URL for getting a specific list
    // /sites/{site-id}/lists/{list-id}
    static std::string BuildListUrl(const std::string &site_id, const std::string &list_id);

    // Build URL for getting list columns (schema)
    // /sites/{site-id}/lists/{list-id}/columns
    static std::string BuildListColumnsUrl(const std::string &site_id, const std::string &list_id);

    // Build URL for listing items in a list
    // /sites/{site-id}/lists/{list-id}/items
    static std::string BuildListItemsUrl(const std::string &site_id, const std::string &list_id);

    // Build URL for listing items with expand for field values
    // /sites/{site-id}/lists/{list-id}/items?expand=fields
    static std::string BuildListItemsWithFieldsUrl(const std::string &site_id, const std::string &list_id);

    // Build URL for listing items with $select and $top
    static std::string BuildListItemsWithSelectUrl(const std::string &site_id, const std::string &list_id,
                                                    const std::string &select = "", int top = 0);

    // Build URL for getting a specific item
    // /sites/{site-id}/lists/{list-id}/items/{item-id}
    static std::string BuildItemUrl(const std::string &site_id, const std::string &list_id, const std::string &item_id);

    // Build URL for my followed sites
    // /me/followedSites
    static std::string BuildFollowedSitesUrl();

    // Build URL for getting site by hostname and path
    // /sites/{hostname}:/{site-path}
    static std::string BuildSiteByPathUrl(const std::string &hostname, const std::string &site_path);
};

// Client for Microsoft Graph SharePoint API operations
class GraphSharePointClient {
public:
    GraphSharePointClient(std::shared_ptr<HttpAuthParams> auth_params);

    // Site discovery
    std::string SearchSites(const std::string &search_query = "");
    std::string GetFollowedSites();
    std::string GetSite(const std::string &site_id);
    std::string GetSiteByPath(const std::string &hostname, const std::string &site_path);

    // List operations
    std::string ListLists(const std::string &site_id);
    std::string GetList(const std::string &site_id, const std::string &list_id);
    std::string GetListColumns(const std::string &site_id, const std::string &list_id);

    // List item operations
    std::string GetListItems(const std::string &site_id, const std::string &list_id,
                             const std::string &select = "", int top = 0);

private:
    std::shared_ptr<HttpAuthParams> auth_params;
    std::shared_ptr<HttpClient> http_client;

    std::string DoGraphGet(const std::string &url);
};

} // namespace erpl_web
