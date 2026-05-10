#include "graph_teams_client.hpp"
#include "graph_client.hpp"
#include "duckdb/common/exception.hpp"
#include "tracing.hpp"

namespace erpl_web {

// URL Builder implementation
std::string GraphTeamsUrlBuilder::GetBaseUrl() {
    return GraphClient::BaseUrl();
}

std::string GraphTeamsUrlBuilder::BuildMyTeamsUrl(const std::string &user) {
    return GetBaseUrl() + "/" + GraphClient::ResolveUserSegment(user) + "/joinedTeams";
}

std::string GraphTeamsUrlBuilder::BuildTeamUrl(const std::string &team_id) {
    return GetBaseUrl() + "/teams/" + team_id;
}

std::string GraphTeamsUrlBuilder::BuildTeamChannelsUrl(const std::string &team_id) {
    return GetBaseUrl() + "/teams/" + team_id + "/channels";
}

std::string GraphTeamsUrlBuilder::BuildChannelUrl(const std::string &team_id, const std::string &channel_id) {
    return GetBaseUrl() + "/teams/" + team_id + "/channels/" + GraphClient::UrlEncode(channel_id);
}

std::string GraphTeamsUrlBuilder::BuildTeamMembersUrl(const std::string &team_id) {
    return GetBaseUrl() + "/teams/" + team_id + "/members";
}

std::string GraphTeamsUrlBuilder::BuildChannelMessagesUrl(const std::string &team_id, const std::string &channel_id) {
    return GetBaseUrl() + "/teams/" + team_id + "/channels/" + GraphClient::UrlEncode(channel_id) + "/messages";
}

// GraphTeamsClient implementation
GraphTeamsClient::GraphTeamsClient(std::shared_ptr<HttpAuthParams> auth_params)
    : auth_params(auth_params) {
}

std::string GraphTeamsClient::DoGraphGet(const std::string &url) {
    return GraphClient(auth_params, "GRAPH_TEAMS").Get(url);
}

std::string GraphTeamsClient::GetMyTeams(const std::string &user) {
    auto url = GraphTeamsUrlBuilder::BuildMyTeamsUrl(user);
    return GraphClient(auth_params, "GRAPH_TEAMS").GetAllPagesMerged(url);
}

std::string GraphTeamsClient::GetTeam(const std::string &team_id) {
    auto url = GraphTeamsUrlBuilder::BuildTeamUrl(team_id);
    return DoGraphGet(url);
}

std::string GraphTeamsClient::GetTeamChannels(const std::string &team_id) {
    auto url = GraphTeamsUrlBuilder::BuildTeamChannelsUrl(team_id);
    return GraphClient(auth_params, "GRAPH_TEAMS").GetAllPagesMerged(url);
}

std::string GraphTeamsClient::GetChannel(const std::string &team_id, const std::string &channel_id) {
    auto url = GraphTeamsUrlBuilder::BuildChannelUrl(team_id, channel_id);
    return DoGraphGet(url);
}

std::string GraphTeamsClient::GetTeamMembers(const std::string &team_id) {
    auto url = GraphTeamsUrlBuilder::BuildTeamMembersUrl(team_id);
    return GraphClient(auth_params, "GRAPH_TEAMS").GetAllPagesMerged(url);
}

std::string GraphTeamsClient::GetChannelMessages(const std::string &team_id, const std::string &channel_id) {
    auto url = GraphTeamsUrlBuilder::BuildChannelMessagesUrl(team_id, channel_id);
    return GraphClient(auth_params, "GRAPH_TEAMS").GetAllPagesMerged(url);
}

// =============================================================================
// ID resolution
// =============================================================================

bool GraphTeamsClient::LooksLikeTeamId(const std::string &s) {
    return GraphClient::LooksLikeGuid(s);
}

bool GraphTeamsClient::LooksLikeChannelId(const std::string &s) {
    // Teams channel IDs start with "19:" and contain "@thread."
    return s.size() > 3 && s[0] == '1' && s[1] == '9' && s[2] == ':' &&
           s.find("@thread.") != std::string::npos;
}

std::string GraphTeamsClient::ResolveTeamId(const std::string &name_or_id, const std::string &user) {
    if (LooksLikeTeamId(name_or_id)) {
        return name_or_id;
    }

    ERPL_TRACE_DEBUG("GRAPH_TEAMS", "Resolving team name '" + name_or_id + "' to ID");
    const std::string teams_json = GetMyTeams(user);

    auto resolved = GraphJsonFindStringInArray(teams_json, "value", "displayName", name_or_id, "id",
                                               "teams response when resolving team '" + name_or_id + "'");
    if (!resolved) {
        throw duckdb::InvalidInputException(
            "No team found with displayName '%s'. Pass a team GUID to use a technical ID directly.",
            name_or_id.c_str());
    }

    ERPL_TRACE_DEBUG("GRAPH_TEAMS", "Resolved team '" + name_or_id + "' → " + *resolved);
    return *resolved;
}

std::string GraphTeamsClient::ResolveChannelId(const std::string &team_id, const std::string &name_or_id) {
    if (LooksLikeChannelId(name_or_id)) {
        return name_or_id;
    }

    ERPL_TRACE_DEBUG("GRAPH_TEAMS", "Resolving channel name '" + name_or_id + "' in team " + team_id);
    const std::string channels_json = GetTeamChannels(team_id);

    auto resolved = GraphJsonFindStringInArray(channels_json, "value", "displayName", name_or_id, "id",
                                               "channels response when resolving channel '" + name_or_id + "'");
    if (!resolved) {
        throw duckdb::InvalidInputException(
            "No channel found with displayName '%s' in team '%s'. "
            "Pass a channel ID (19:xxx@thread.tacv2) to use a technical ID directly.",
            name_or_id.c_str(), team_id.c_str());
    }

    ERPL_TRACE_DEBUG("GRAPH_TEAMS", "Resolved channel '" + name_or_id + "' → " + *resolved);
    return *resolved;
}

} // namespace erpl_web
