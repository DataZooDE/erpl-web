#pragma once

#include "http_client.hpp"
#include <string>
#include <memory>

namespace erpl_web {

// URL builder for Microsoft Graph Outlook API endpoints.
// All methods accept an optional user_id:
//   - empty → /me/...        (delegated auth, authorization_code flow)
//   - non-empty → /users/{id}/...  (works with both delegated and client_credentials)
class GraphOutlookUrlBuilder {
public:
    static std::string GetBaseUrl();

    // /me/... or /users/{id}/...
    static std::string BuildEventsUrl(const std::string &user_id);
    static std::string BuildCalendarViewUrl(const std::string &user_id,
                                            const std::string &start_dt,
                                            const std::string &end_dt);
    static std::string BuildCalendarsUrl(const std::string &user_id);
    static std::string BuildCalendarEventsUrl(const std::string &user_id,
                                              const std::string &calendar_id);
    static std::string BuildContactsUrl(const std::string &user_id);
    static std::string BuildMessagesUrl(const std::string &user_id);
    static std::string BuildMailFoldersUrl(const std::string &user_id);
    static std::string BuildFolderMessagesUrl(const std::string &user_id,
                                              const std::string &folder_id);

private:
    // Returns "me" when user_id is empty, "users/{id}" otherwise.
    static std::string UserSegment(const std::string &user_id);
};

// Client for Microsoft Graph Outlook API operations.
class GraphOutlookClient {
public:
    explicit GraphOutlookClient(std::shared_ptr<HttpAuthParams> auth_params);

    // Calendar
    std::string GetEvents(const std::string &user_id);
    std::string GetCalendarView(const std::string &user_id,
                                const std::string &start_dt,
                                const std::string &end_dt);
    std::string GetCalendars(const std::string &user_id);
    std::string GetCalendarEvents(const std::string &user_id,
                                  const std::string &calendar_id);

    // Contacts
    std::string GetContacts(const std::string &user_id);

    // Mail
    std::string GetMessages(const std::string &user_id);
    std::string GetMailFolders(const std::string &user_id);
    std::string GetFolderMessages(const std::string &user_id,
                                  const std::string &folder_id);

private:
    std::shared_ptr<HttpAuthParams> auth_params;
};

} // namespace erpl_web
