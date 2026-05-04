#include "graph_entra_client.hpp"
#include "graph_client.hpp"
#include "tracing.hpp"

namespace erpl_web {

// URL Builder implementation
std::string GraphEntraUrlBuilder::GetBaseUrl() {
    return GraphClient::BaseUrl();
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
}

std::string GraphEntraClient::DoGraphGet(const std::string &url) {
    return GraphClient(auth_params, "GRAPH_ENTRA").Get(url);
}

std::string GraphEntraClient::GetUsers() {
    auto url = GraphEntraUrlBuilder::BuildUsersUrl();
    return GraphClient(auth_params, "GRAPH_ENTRA").GetAllPagesMerged(url);
}

std::string GraphEntraClient::GetUser(const std::string &user_id) {
    auto url = GraphEntraUrlBuilder::BuildUserUrl(user_id);
    return DoGraphGet(url);
}

std::string GraphEntraClient::GetGroups() {
    auto url = GraphEntraUrlBuilder::BuildGroupsUrl();
    return GraphClient(auth_params, "GRAPH_ENTRA").GetAllPagesMerged(url);
}

std::string GraphEntraClient::GetGroup(const std::string &group_id) {
    auto url = GraphEntraUrlBuilder::BuildGroupUrl(group_id);
    return DoGraphGet(url);
}

std::string GraphEntraClient::GetGroupMembers(const std::string &group_id) {
    auto url = GraphEntraUrlBuilder::BuildGroupMembersUrl(group_id);
    return GraphClient(auth_params, "GRAPH_ENTRA").GetAllPagesMerged(url);
}

std::string GraphEntraClient::GetDevices() {
    auto url = GraphEntraUrlBuilder::BuildDevicesUrl();
    return GraphClient(auth_params, "GRAPH_ENTRA").GetAllPagesMerged(url);
}

std::string GraphEntraClient::GetDevice(const std::string &device_id) {
    auto url = GraphEntraUrlBuilder::BuildDeviceUrl(device_id);
    return DoGraphGet(url);
}

std::string GraphEntraClient::GetSignInLogs() {
    auto url = GraphEntraUrlBuilder::BuildSignInLogsUrl();
    return GraphClient(auth_params, "GRAPH_ENTRA").GetAllPagesMerged(url);
}

} // namespace erpl_web
