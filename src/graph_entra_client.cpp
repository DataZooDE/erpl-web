#include "graph_entra_client.hpp"
#include "tracing.hpp"

namespace erpl_web {

// URL Builder implementation
std::string GraphEntraUrlBuilder::GetBaseUrl() {
    return "https://graph.microsoft.com/v1.0";
}

std::string GraphEntraUrlBuilder::BuildUsersUrl() {
    return GetBaseUrl() + "/users";
}

std::string GraphEntraUrlBuilder::BuildUserUrl(const std::string &user_id) {
    return GetBaseUrl() + "/users/" + user_id;
}

std::string GraphEntraUrlBuilder::BuildGroupsUrl() {
    return GetBaseUrl() + "/groups";
}

std::string GraphEntraUrlBuilder::BuildGroupUrl(const std::string &group_id) {
    return GetBaseUrl() + "/groups/" + group_id;
}

std::string GraphEntraUrlBuilder::BuildGroupMembersUrl(const std::string &group_id) {
    return GetBaseUrl() + "/groups/" + group_id + "/members";
}

std::string GraphEntraUrlBuilder::BuildDevicesUrl() {
    return GetBaseUrl() + "/devices";
}

std::string GraphEntraUrlBuilder::BuildDeviceUrl(const std::string &device_id) {
    return GetBaseUrl() + "/devices/" + device_id;
}

std::string GraphEntraUrlBuilder::BuildSignInLogsUrl() {
    // Note: Sign-in logs require Azure AD Premium and special permissions
    return GetBaseUrl() + "/auditLogs/signIns";
}

// GraphEntraClient implementation
GraphEntraClient::GraphEntraClient(std::shared_ptr<HttpAuthParams> auth_params)
    : auth_params(auth_params) {
    HttpParams http_params;
    http_params.url_encode = false;
    http_client = std::make_shared<HttpClient>(http_params);
}

std::string GraphEntraClient::DoGraphGet(const std::string &url) {
    ERPL_TRACE_DEBUG("GRAPH_ENTRA", "GET request to: " + url);

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
        ERPL_TRACE_ERROR("GRAPH_ENTRA", error_msg);
        throw std::runtime_error(error_msg);
    }

    ERPL_TRACE_DEBUG("GRAPH_ENTRA", "Response received: " + std::to_string(response->Content().length()) + " bytes");
    return response->Content();
}

std::string GraphEntraClient::GetUsers() {
    auto url = GraphEntraUrlBuilder::BuildUsersUrl();
    return DoGraphGet(url);
}

std::string GraphEntraClient::GetUser(const std::string &user_id) {
    auto url = GraphEntraUrlBuilder::BuildUserUrl(user_id);
    return DoGraphGet(url);
}

std::string GraphEntraClient::GetGroups() {
    auto url = GraphEntraUrlBuilder::BuildGroupsUrl();
    return DoGraphGet(url);
}

std::string GraphEntraClient::GetGroup(const std::string &group_id) {
    auto url = GraphEntraUrlBuilder::BuildGroupUrl(group_id);
    return DoGraphGet(url);
}

std::string GraphEntraClient::GetGroupMembers(const std::string &group_id) {
    auto url = GraphEntraUrlBuilder::BuildGroupMembersUrl(group_id);
    return DoGraphGet(url);
}

std::string GraphEntraClient::GetDevices() {
    auto url = GraphEntraUrlBuilder::BuildDevicesUrl();
    return DoGraphGet(url);
}

std::string GraphEntraClient::GetDevice(const std::string &device_id) {
    auto url = GraphEntraUrlBuilder::BuildDeviceUrl(device_id);
    return DoGraphGet(url);
}

std::string GraphEntraClient::GetSignInLogs() {
    auto url = GraphEntraUrlBuilder::BuildSignInLogsUrl();
    return DoGraphGet(url);
}

} // namespace erpl_web
