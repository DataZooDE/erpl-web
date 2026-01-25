#pragma once

#include "duckdb.hpp"
#include "http_client.hpp"

#include <memory>
#include <string>

namespace erpl_web {

// URL builder for Microsoft Graph Teams endpoints
class GraphTeamsUrlBuilder {
public:
    static std::string GetBaseUrl();

    // Teams
    static std::string BuildMyTeamsUrl();
    static std::string BuildTeamUrl(const std::string &team_id);

    // Channels
    static std::string BuildTeamChannelsUrl(const std::string &team_id);
    static std::string BuildChannelUrl(const std::string &team_id, const std::string &channel_id);

    // Members
    static std::string BuildTeamMembersUrl(const std::string &team_id);

    // Messages
    static std::string BuildChannelMessagesUrl(const std::string &team_id, const std::string &channel_id);
};

// Client for Microsoft Graph Teams operations
class GraphTeamsClient {
public:
    explicit GraphTeamsClient(std::shared_ptr<HttpAuthParams> auth_params);

    // Teams
    std::string GetMyTeams();
    std::string GetTeam(const std::string &team_id);

    // Channels
    std::string GetTeamChannels(const std::string &team_id);
    std::string GetChannel(const std::string &team_id, const std::string &channel_id);

    // Members
    std::string GetTeamMembers(const std::string &team_id);

    // Messages
    std::string GetChannelMessages(const std::string &team_id, const std::string &channel_id);

private:
    std::string DoGraphGet(const std::string &url);

    std::shared_ptr<HttpClient> http_client;
    std::shared_ptr<HttpAuthParams> auth_params;
};

} // namespace erpl_web
