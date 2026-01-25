#pragma once

#include "duckdb.hpp"
#include "http_client.hpp"

#include <memory>
#include <string>

namespace erpl_web {

// URL builder for Microsoft Graph Entra ID Directory endpoints
class GraphEntraUrlBuilder {
public:
    static std::string GetBaseUrl();

    // Users
    static std::string BuildUsersUrl();
    static std::string BuildUserUrl(const std::string &user_id);

    // Groups
    static std::string BuildGroupsUrl();
    static std::string BuildGroupUrl(const std::string &group_id);
    static std::string BuildGroupMembersUrl(const std::string &group_id);

    // Devices
    static std::string BuildDevicesUrl();
    static std::string BuildDeviceUrl(const std::string &device_id);

    // Sign-in logs (requires Azure AD Premium)
    static std::string BuildSignInLogsUrl();
};

// Client for Microsoft Graph Entra ID Directory operations
class GraphEntraClient {
public:
    explicit GraphEntraClient(std::shared_ptr<HttpAuthParams> auth_params);

    // Users
    std::string GetUsers();
    std::string GetUser(const std::string &user_id);

    // Groups
    std::string GetGroups();
    std::string GetGroup(const std::string &group_id);
    std::string GetGroupMembers(const std::string &group_id);

    // Devices
    std::string GetDevices();
    std::string GetDevice(const std::string &device_id);

    // Sign-in logs
    std::string GetSignInLogs();

private:
    std::string DoGraphGet(const std::string &url);

    std::shared_ptr<HttpClient> http_client;
    std::shared_ptr<HttpAuthParams> auth_params;
};

} // namespace erpl_web
