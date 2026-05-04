#include "graph_outlook_client.hpp"
#include "graph_client.hpp"
#include "tracing.hpp"

namespace erpl_web {

// URL Builder implementation
std::string GraphOutlookUrlBuilder::GetBaseUrl() {
    return GraphClient::BaseUrl();
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
}

std::string GraphOutlookClient::DoGraphGet(const std::string &url) {
    return GraphClient(auth_params, "GRAPH_OUTLOOK").Get(url);
}

std::string GraphOutlookClient::GetMyEvents() {
    auto url = GraphOutlookUrlBuilder::BuildMyEventsUrl();
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetMyCalendars() {
    auto url = GraphOutlookUrlBuilder::BuildMyCalendarsUrl();
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetCalendarEvents(const std::string &calendar_id) {
    auto url = GraphOutlookUrlBuilder::BuildCalendarEventsUrl(calendar_id);
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetMyContacts() {
    auto url = GraphOutlookUrlBuilder::BuildMyContactsUrl();
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetContactFolders() {
    auto url = GraphOutlookUrlBuilder::BuildContactFoldersUrl();
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetFolderContacts(const std::string &folder_id) {
    auto url = GraphOutlookUrlBuilder::BuildFolderContactsUrl(folder_id);
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetMyMessages() {
    auto url = GraphOutlookUrlBuilder::BuildMyMessagesUrl();
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetMailFolders() {
    auto url = GraphOutlookUrlBuilder::BuildMailFoldersUrl();
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetFolderMessages(const std::string &folder_id) {
    auto url = GraphOutlookUrlBuilder::BuildFolderMessagesUrl(folder_id);
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

} // namespace erpl_web
