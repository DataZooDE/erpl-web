#include "graph_teams_client.hpp"
#include "graph_client.hpp"
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
    return GetBaseUrl() + "/teams/" + team_id + "/channels/" + channel_id;
}

std::string GraphTeamsUrlBuilder::BuildTeamMembersUrl(const std::string &team_id) {
    return GetBaseUrl() + "/teams/" + team_id + "/members";
}

std::string GraphTeamsUrlBuilder::BuildChannelMessagesUrl(const std::string &team_id, const std::string &channel_id) {
    return GetBaseUrl() + "/teams/" + team_id + "/channels/" + channel_id + "/messages";
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

} // namespace erpl_web
