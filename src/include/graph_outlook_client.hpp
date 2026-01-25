#pragma once

#include "http_client.hpp"
#include <string>
#include <memory>

namespace erpl_web {

// URL builder for Microsoft Graph Outlook API endpoints
class GraphOutlookUrlBuilder {
public:
    // Base Graph API URL
    static std::string GetBaseUrl();

    // Build URL for listing calendar events
    // /me/events or /me/calendar/events
    static std::string BuildMyEventsUrl();

    // Build URL for listing calendars
    // /me/calendars
    static std::string BuildMyCalendarsUrl();

    // Build URL for listing events in a specific calendar
    // /me/calendars/{calendar-id}/events
    static std::string BuildCalendarEventsUrl(const std::string &calendar_id);

    // Build URL for listing contacts
    // /me/contacts
    static std::string BuildMyContactsUrl();

    // Build URL for listing contact folders
    // /me/contactFolders
    static std::string BuildContactFoldersUrl();

    // Build URL for listing contacts in a folder
    // /me/contactFolders/{folder-id}/contacts
    static std::string BuildFolderContactsUrl(const std::string &folder_id);

    // Build URL for listing messages (email)
    // /me/messages
    static std::string BuildMyMessagesUrl();

    // Build URL for listing mail folders
    // /me/mailFolders
    static std::string BuildMailFoldersUrl();

    // Build URL for listing messages in a folder
    // /me/mailFolders/{folder-id}/messages
    static std::string BuildFolderMessagesUrl(const std::string &folder_id);
};

// Client for Microsoft Graph Outlook API operations
class GraphOutlookClient {
public:
    GraphOutlookClient(std::shared_ptr<HttpAuthParams> auth_params);

    // Calendar operations
    std::string GetMyEvents();
    std::string GetMyCalendars();
    std::string GetCalendarEvents(const std::string &calendar_id);

    // Contact operations
    std::string GetMyContacts();
    std::string GetContactFolders();
    std::string GetFolderContacts(const std::string &folder_id);

    // Message operations
    std::string GetMyMessages();
    std::string GetMailFolders();
    std::string GetFolderMessages(const std::string &folder_id);

private:
    std::shared_ptr<HttpAuthParams> auth_params;
    std::shared_ptr<HttpClient> http_client;

    std::string DoGraphGet(const std::string &url);
};

} // namespace erpl_web
