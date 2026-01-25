#include "graph_teams_client.hpp"
#include "tracing.hpp"

namespace erpl_web {

// URL Builder implementation
std::string GraphTeamsUrlBuilder::GetBaseUrl() {
    return "https://graph.microsoft.com/v1.0";
}

std::string GraphTeamsUrlBuilder::BuildMyTeamsUrl() {
    return GetBaseUrl() + "/me/joinedTeams";
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
    HttpParams http_params;
    http_params.url_encode = false;
    http_client = std::make_shared<HttpClient>(http_params);
}

std::string GraphTeamsClient::DoGraphGet(const std::string &url) {
    ERPL_TRACE_DEBUG("GRAPH_TEAMS", "GET request to: " + url);

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
        ERPL_TRACE_ERROR("GRAPH_TEAMS", error_msg);
        throw std::runtime_error(error_msg);
    }

    ERPL_TRACE_DEBUG("GRAPH_TEAMS", "Response received: " + std::to_string(response->Content().length()) + " bytes");
    return response->Content();
}

std::string GraphTeamsClient::GetMyTeams() {
    auto url = GraphTeamsUrlBuilder::BuildMyTeamsUrl();
    return DoGraphGet(url);
}

std::string GraphTeamsClient::GetTeam(const std::string &team_id) {
    auto url = GraphTeamsUrlBuilder::BuildTeamUrl(team_id);
    return DoGraphGet(url);
}

std::string GraphTeamsClient::GetTeamChannels(const std::string &team_id) {
    auto url = GraphTeamsUrlBuilder::BuildTeamChannelsUrl(team_id);
    return DoGraphGet(url);
}

std::string GraphTeamsClient::GetChannel(const std::string &team_id, const std::string &channel_id) {
    auto url = GraphTeamsUrlBuilder::BuildChannelUrl(team_id, channel_id);
    return DoGraphGet(url);
}

std::string GraphTeamsClient::GetTeamMembers(const std::string &team_id) {
    auto url = GraphTeamsUrlBuilder::BuildTeamMembersUrl(team_id);
    return DoGraphGet(url);
}

std::string GraphTeamsClient::GetChannelMessages(const std::string &team_id, const std::string &channel_id) {
    auto url = GraphTeamsUrlBuilder::BuildChannelMessagesUrl(team_id, channel_id);
    return DoGraphGet(url);
}

} // namespace erpl_web
