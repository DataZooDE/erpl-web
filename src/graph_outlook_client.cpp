#include "graph_outlook_client.hpp"
#include "graph_client.hpp"
#include "tracing.hpp"

namespace erpl_web {

// ---------------------------------------------------------------------------
// URL Builder
// ---------------------------------------------------------------------------

std::string GraphOutlookUrlBuilder::GetBaseUrl() {
    return GraphClient::BaseUrl();
}

std::string GraphOutlookUrlBuilder::UserSegment(const std::string &user_id) {
    return user_id.empty() ? "me" : "users/" + user_id;
}

std::string GraphOutlookUrlBuilder::BuildEventsUrl(const std::string &user_id) {
    return GetBaseUrl() + "/" + UserSegment(user_id) + "/events";
}

std::string GraphOutlookUrlBuilder::BuildCalendarViewUrl(const std::string &user_id,
                                                          const std::string &start_dt,
                                                          const std::string &end_dt) {
    return GetBaseUrl() + "/" + UserSegment(user_id) +
           "/calendarView?startDateTime=" + start_dt + "&endDateTime=" + end_dt;
}

std::string GraphOutlookUrlBuilder::BuildCalendarsUrl(const std::string &user_id) {
    return GetBaseUrl() + "/" + UserSegment(user_id) + "/calendars";
}

std::string GraphOutlookUrlBuilder::BuildCalendarEventsUrl(const std::string &user_id,
                                                            const std::string &calendar_id) {
    return GetBaseUrl() + "/" + UserSegment(user_id) + "/calendars/" + calendar_id + "/events";
}

std::string GraphOutlookUrlBuilder::BuildContactsUrl(const std::string &user_id) {
    return GetBaseUrl() + "/" + UserSegment(user_id) + "/contacts";
}

std::string GraphOutlookUrlBuilder::BuildMessagesUrl(const std::string &user_id) {
    return GetBaseUrl() + "/" + UserSegment(user_id) + "/messages";
}

std::string GraphOutlookUrlBuilder::BuildMailFoldersUrl(const std::string &user_id) {
    return GetBaseUrl() + "/" + UserSegment(user_id) + "/mailFolders";
}

std::string GraphOutlookUrlBuilder::BuildFolderMessagesUrl(const std::string &user_id,
                                                            const std::string &folder_id) {
    return GetBaseUrl() + "/" + UserSegment(user_id) + "/mailFolders/" + folder_id + "/messages";
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

GraphOutlookClient::GraphOutlookClient(std::shared_ptr<HttpAuthParams> auth_params)
    : auth_params(std::move(auth_params)) {
}

std::string GraphOutlookClient::GetEvents(const std::string &user_id) {
    auto url = GraphOutlookUrlBuilder::BuildEventsUrl(user_id);
    ERPL_TRACE_DEBUG("GRAPH_OUTLOOK", "GetEvents: " + url);
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetCalendarView(const std::string &user_id,
                                                 const std::string &start_dt,
                                                 const std::string &end_dt) {
    auto url = GraphOutlookUrlBuilder::BuildCalendarViewUrl(user_id, start_dt, end_dt);
    ERPL_TRACE_DEBUG("GRAPH_OUTLOOK", "GetCalendarView: " + url);
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetCalendars(const std::string &user_id) {
    auto url = GraphOutlookUrlBuilder::BuildCalendarsUrl(user_id);
    ERPL_TRACE_DEBUG("GRAPH_OUTLOOK", "GetCalendars: " + url);
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetCalendarEvents(const std::string &user_id,
                                                   const std::string &calendar_id) {
    auto url = GraphOutlookUrlBuilder::BuildCalendarEventsUrl(user_id, calendar_id);
    ERPL_TRACE_DEBUG("GRAPH_OUTLOOK", "GetCalendarEvents: " + url);
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetContacts(const std::string &user_id) {
    auto url = GraphOutlookUrlBuilder::BuildContactsUrl(user_id);
    ERPL_TRACE_DEBUG("GRAPH_OUTLOOK", "GetContacts: " + url);
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetMessages(const std::string &user_id) {
    auto url = GraphOutlookUrlBuilder::BuildMessagesUrl(user_id);
    ERPL_TRACE_DEBUG("GRAPH_OUTLOOK", "GetMessages: " + url);
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetMailFolders(const std::string &user_id) {
    auto url = GraphOutlookUrlBuilder::BuildMailFoldersUrl(user_id);
    ERPL_TRACE_DEBUG("GRAPH_OUTLOOK", "GetMailFolders: " + url);
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

std::string GraphOutlookClient::GetFolderMessages(const std::string &user_id,
                                                   const std::string &folder_id) {
    auto url = GraphOutlookUrlBuilder::BuildFolderMessagesUrl(user_id, folder_id);
    ERPL_TRACE_DEBUG("GRAPH_OUTLOOK", "GetFolderMessages: " + url);
    return GraphClient(auth_params, "GRAPH_OUTLOOK").GetAllPagesMerged(url);
}

} // namespace erpl_web
