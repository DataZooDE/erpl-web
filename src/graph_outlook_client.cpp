#include "graph_outlook_client.hpp"
#include "tracing.hpp"

namespace erpl_web {

// URL Builder implementation
std::string GraphOutlookUrlBuilder::GetBaseUrl() {
    return "https://graph.microsoft.com/v1.0";
}

std::string GraphOutlookUrlBuilder::BuildMyEventsUrl() {
    return GetBaseUrl() + "/me/events";
}

std::string GraphOutlookUrlBuilder::BuildMyCalendarsUrl() {
    return GetBaseUrl() + "/me/calendars";
}

std::string GraphOutlookUrlBuilder::BuildCalendarEventsUrl(const std::string &calendar_id) {
    return GetBaseUrl() + "/me/calendars/" + calendar_id + "/events";
}

std::string GraphOutlookUrlBuilder::BuildMyContactsUrl() {
    return GetBaseUrl() + "/me/contacts";
}

std::string GraphOutlookUrlBuilder::BuildContactFoldersUrl() {
    return GetBaseUrl() + "/me/contactFolders";
}

std::string GraphOutlookUrlBuilder::BuildFolderContactsUrl(const std::string &folder_id) {
    return GetBaseUrl() + "/me/contactFolders/" + folder_id + "/contacts";
}

std::string GraphOutlookUrlBuilder::BuildMyMessagesUrl() {
    return GetBaseUrl() + "/me/messages";
}

std::string GraphOutlookUrlBuilder::BuildMailFoldersUrl() {
    return GetBaseUrl() + "/me/mailFolders";
}

std::string GraphOutlookUrlBuilder::BuildFolderMessagesUrl(const std::string &folder_id) {
    return GetBaseUrl() + "/me/mailFolders/" + folder_id + "/messages";
}

// GraphOutlookClient implementation
GraphOutlookClient::GraphOutlookClient(std::shared_ptr<HttpAuthParams> auth_params)
    : auth_params(auth_params) {
    HttpParams http_params;
    http_params.url_encode = false;
    http_client = std::make_shared<HttpClient>(http_params);
}

std::string GraphOutlookClient::DoGraphGet(const std::string &url) {
    ERPL_TRACE_DEBUG("GRAPH_OUTLOOK", "GET request to: " + url);

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
        ERPL_TRACE_ERROR("GRAPH_OUTLOOK", error_msg);
        throw std::runtime_error(error_msg);
    }

    ERPL_TRACE_DEBUG("GRAPH_OUTLOOK", "Response received: " + std::to_string(response->Content().length()) + " bytes");
    return response->Content();
}

std::string GraphOutlookClient::GetMyEvents() {
    auto url = GraphOutlookUrlBuilder::BuildMyEventsUrl();
    return DoGraphGet(url);
}

std::string GraphOutlookClient::GetMyCalendars() {
    auto url = GraphOutlookUrlBuilder::BuildMyCalendarsUrl();
    return DoGraphGet(url);
}

std::string GraphOutlookClient::GetCalendarEvents(const std::string &calendar_id) {
    auto url = GraphOutlookUrlBuilder::BuildCalendarEventsUrl(calendar_id);
    return DoGraphGet(url);
}

std::string GraphOutlookClient::GetMyContacts() {
    auto url = GraphOutlookUrlBuilder::BuildMyContactsUrl();
    return DoGraphGet(url);
}

std::string GraphOutlookClient::GetContactFolders() {
    auto url = GraphOutlookUrlBuilder::BuildContactFoldersUrl();
    return DoGraphGet(url);
}

std::string GraphOutlookClient::GetFolderContacts(const std::string &folder_id) {
    auto url = GraphOutlookUrlBuilder::BuildFolderContactsUrl(folder_id);
    return DoGraphGet(url);
}

std::string GraphOutlookClient::GetMyMessages() {
    auto url = GraphOutlookUrlBuilder::BuildMyMessagesUrl();
    return DoGraphGet(url);
}

std::string GraphOutlookClient::GetMailFolders() {
    auto url = GraphOutlookUrlBuilder::BuildMailFoldersUrl();
    return DoGraphGet(url);
}

std::string GraphOutlookClient::GetFolderMessages(const std::string &folder_id) {
    auto url = GraphOutlookUrlBuilder::BuildFolderMessagesUrl(folder_id);
    return DoGraphGet(url);
}

} // namespace erpl_web
