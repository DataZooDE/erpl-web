#include "graph_outlook_functions.hpp"
#include "graph_outlook_client.hpp"
#include "graph_excel_secret.hpp"
#include "graph_output_utils.hpp"
#include "tracing.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace erpl_web {

using namespace duckdb;

// ============================================================================
// Shared bind-data base
// ============================================================================

struct OutlookBindData : public TableFunctionData {
    std::string secret_name;
    std::string user;
    std::string json_response;
    yyjson_doc *parsed_doc  = nullptr;
    yyjson_arr_iter item_iter = {};
    bool done               = false;

    ~OutlookBindData() override {
        if (parsed_doc) {
            yyjson_doc_free(parsed_doc);
        }
    }

    bool InitIterator(const char *array_key = "value") {
        parsed_doc = yyjson_read(json_response.c_str(), json_response.size(), 0);
        json_response.clear();
        json_response.shrink_to_fit();
        if (!parsed_doc) {
            return false;
        }
        yyjson_val *root = yyjson_doc_get_root(parsed_doc);
        yyjson_val *arr  = yyjson_obj_get(root, array_key);
        if (!arr || !yyjson_is_arr(arr)) {
            return false;
        }
        yyjson_arr_iter_init(arr, &item_iter);
        return true;
    }
};

// Named-parameter helpers
static std::string GetNamedStr(TableFunctionBindInput &input, const char *name) {
    auto it = input.named_parameters.find(name);
    if (it != input.named_parameters.end() && !it->second.IsNull()) {
        return it->second.GetValue<std::string>();
    }
    return {};
}

// Normalise a bare ISO date ("2024-06-15") to the format calendarView expects
// ("2024-06-15T00:00:00"). Full datetimes pass through unchanged.
static std::string NormaliseCalendarViewDate(const std::string &s) {
    if (s.size() == 10 && s[4] == '-' && s[7] == '-') {
        return s + "T00:00:00";
    }
    return s;
}

// ============================================================================
// graph_calendars
// ============================================================================

unique_ptr<FunctionData> GraphOutlookFunctions::CalendarsBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names)
{
    auto bind_data = make_uniq<OutlookBindData>();
    bind_data->secret_name = GetNamedStr(input, "secret");
    bind_data->user        = GetNamedStr(input, "user");

    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    GraphOutlookClient client(auth_info.auth_params);
    bind_data->json_response = client.GetCalendars(bind_data->user);

    names        = {"id", "name", "color", "is_default_calendar", "can_edit"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // name
        LogicalType::VARCHAR,   // color
        LogicalType::BOOLEAN,   // isDefaultCalendar
        LogicalType::BOOLEAN,   // canEdit
    };
    return std::move(bind_data);
}

void GraphOutlookFunctions::CalendarsScan(
    ClientContext &,
    TableFunctionInput &data,
    DataChunk &output)
{
    auto &bd = data.bind_data->CastNoConst<OutlookBindData>();
    if (bd.done) { output.SetCardinality(0); return; }

    if (!bd.parsed_doc && !bd.InitIterator()) {
        bd.done = true; output.SetCardinality(0); return;
    }

    idx_t row = 0;
    yyjson_val *item;
    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bd.item_iter)) != nullptr) {
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "id"));
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "name"));
        SetStrCell(output.data[2], row, yyjson_obj_get(item, "color"));
        SetBoolCell(output.data[3], row, yyjson_obj_get(item, "isDefaultCalendar"));
        SetBoolCell(output.data[4], row, yyjson_obj_get(item, "canEdit"));
        row++;
    }

    if (row < STANDARD_VECTOR_SIZE) {
        bd.done = true;
    }
    output.SetCardinality(row);
}

// ============================================================================
// graph_calendar_events
// ============================================================================

struct CalendarEventsBindData : public OutlookBindData {
    std::string calendar_id;
    std::string start_date;
    std::string end_date;
};

unique_ptr<FunctionData> GraphOutlookFunctions::CalendarEventsBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names)
{
    auto bind_data = make_uniq<CalendarEventsBindData>();
    bind_data->secret_name  = GetNamedStr(input, "secret");
    bind_data->user      = GetNamedStr(input, "user");
    bind_data->calendar_id  = GetNamedStr(input, "calendar_id");
    bind_data->start_date   = GetNamedStr(input, "start_date");
    bind_data->end_date     = GetNamedStr(input, "end_date");

    const bool has_start = !bind_data->start_date.empty();
    const bool has_end   = !bind_data->end_date.empty();
    if (has_start != has_end) {
        throw InvalidInputException(
            "graph_calendar_events: start_date and end_date must both be provided or both omitted");
    }

    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    GraphOutlookClient client(auth_info.auth_params);

    if (has_start) {
        bind_data->json_response = client.GetCalendarView(
            bind_data->user,
            NormaliseCalendarViewDate(bind_data->start_date),
            NormaliseCalendarViewDate(bind_data->end_date));
    } else if (!bind_data->calendar_id.empty()) {
        bind_data->json_response = client.GetCalendarEvents(
            bind_data->user, bind_data->calendar_id);
    } else {
        bind_data->json_response = client.GetEvents(bind_data->user);
    }

    names        = {"id", "subject", "body_preview", "start_time", "end_time",
                    "location", "organizer_name", "organizer_email",
                    "is_all_day", "is_cancelled", "web_link"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // subject
        LogicalType::VARCHAR,   // body_preview
        LogicalType::VARCHAR,   // start_time
        LogicalType::VARCHAR,   // end_time
        LogicalType::VARCHAR,   // location
        LogicalType::VARCHAR,   // organizer_name
        LogicalType::VARCHAR,   // organizer_email
        LogicalType::BOOLEAN,   // is_all_day
        LogicalType::BOOLEAN,   // is_cancelled
        LogicalType::VARCHAR,   // web_link
    };
    return std::move(bind_data);
}

void GraphOutlookFunctions::CalendarEventsScan(
    ClientContext &,
    TableFunctionInput &data,
    DataChunk &output)
{
    auto &bd = data.bind_data->CastNoConst<CalendarEventsBindData>();
    if (bd.done) { output.SetCardinality(0); return; }

    if (!bd.parsed_doc && !bd.InitIterator()) {
        bd.done = true; output.SetCardinality(0); return;
    }

    idx_t row = 0;
    yyjson_val *item;
    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bd.item_iter)) != nullptr) {
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "id"));
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "subject"));
        SetStrCell(output.data[2], row, yyjson_obj_get(item, "bodyPreview"));

        yyjson_val *start_obj = yyjson_obj_get(item, "start");
        SetStrCell(output.data[3], row, start_obj ? yyjson_obj_get(start_obj, "dateTime") : nullptr);

        yyjson_val *end_obj = yyjson_obj_get(item, "end");
        SetStrCell(output.data[4], row, end_obj ? yyjson_obj_get(end_obj, "dateTime") : nullptr);

        yyjson_val *loc_obj = yyjson_obj_get(item, "location");
        SetStrCell(output.data[5], row, loc_obj ? yyjson_obj_get(loc_obj, "displayName") : nullptr);

        yyjson_val *org_obj   = yyjson_obj_get(item, "organizer");
        yyjson_val *org_email = org_obj ? yyjson_obj_get(org_obj, "emailAddress") : nullptr;
        SetStrCell(output.data[6], row, org_email ? yyjson_obj_get(org_email, "name")    : nullptr);
        SetStrCell(output.data[7], row, org_email ? yyjson_obj_get(org_email, "address") : nullptr);

        SetBoolCell(output.data[8], row, yyjson_obj_get(item, "isAllDay"));
        SetBoolCell(output.data[9], row, yyjson_obj_get(item, "isCancelled"));
        SetStrCell(output.data[10], row, yyjson_obj_get(item, "webLink"));
        row++;
    }

    if (row < STANDARD_VECTOR_SIZE) {
        bd.done = true;
    }
    output.SetCardinality(row);
}

// ============================================================================
// graph_contacts
// ============================================================================

unique_ptr<FunctionData> GraphOutlookFunctions::ContactsBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names)
{
    auto bind_data = make_uniq<OutlookBindData>();
    bind_data->secret_name = GetNamedStr(input, "secret");
    bind_data->user        = GetNamedStr(input, "user");

    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    GraphOutlookClient client(auth_info.auth_params);
    bind_data->json_response = client.GetContacts(bind_data->user);

    names        = {"id", "display_name", "given_name", "surname", "email",
                    "mobile_phone", "business_phone", "company_name", "job_title"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // display_name
        LogicalType::VARCHAR,   // given_name
        LogicalType::VARCHAR,   // surname
        LogicalType::VARCHAR,   // email (first address)
        LogicalType::VARCHAR,   // mobile_phone
        LogicalType::VARCHAR,   // business_phone (first)
        LogicalType::VARCHAR,   // company_name
        LogicalType::VARCHAR,   // job_title
    };
    return std::move(bind_data);
}

void GraphOutlookFunctions::ContactsScan(
    ClientContext &,
    TableFunctionInput &data,
    DataChunk &output)
{
    auto &bd = data.bind_data->CastNoConst<OutlookBindData>();
    if (bd.done) { output.SetCardinality(0); return; }

    if (!bd.parsed_doc && !bd.InitIterator()) {
        bd.done = true; output.SetCardinality(0); return;
    }

    idx_t row = 0;
    yyjson_val *item;
    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bd.item_iter)) != nullptr) {
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "id"));
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "displayName"));
        SetStrCell(output.data[2], row, yyjson_obj_get(item, "givenName"));
        SetStrCell(output.data[3], row, yyjson_obj_get(item, "surname"));

        yyjson_val *emails_arr = yyjson_obj_get(item, "emailAddresses");
        yyjson_val *first_email = emails_arr ? yyjson_arr_get_first(emails_arr) : nullptr;
        SetStrCell(output.data[4], row, first_email ? yyjson_obj_get(first_email, "address") : nullptr);

        SetStrCell(output.data[5], row, yyjson_obj_get(item, "mobilePhone"));

        yyjson_val *phones_arr = yyjson_obj_get(item, "businessPhones");
        yyjson_val *first_phone = phones_arr ? yyjson_arr_get_first(phones_arr) : nullptr;
        SetStrCell(output.data[6], row, first_phone);

        SetStrCell(output.data[7], row, yyjson_obj_get(item, "companyName"));
        SetStrCell(output.data[8], row, yyjson_obj_get(item, "jobTitle"));
        row++;
    }

    if (row < STANDARD_VECTOR_SIZE) {
        bd.done = true;
    }
    output.SetCardinality(row);
}

// ============================================================================
// graph_mail_folders
// ============================================================================

unique_ptr<FunctionData> GraphOutlookFunctions::MailFoldersBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names)
{
    auto bind_data = make_uniq<OutlookBindData>();
    bind_data->secret_name = GetNamedStr(input, "secret");
    bind_data->user        = GetNamedStr(input, "user");

    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    GraphOutlookClient client(auth_info.auth_params);
    bind_data->json_response = client.GetMailFolders(bind_data->user);

    names        = {"id", "display_name", "parent_folder_id", "total_item_count", "unread_item_count"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // displayName
        LogicalType::VARCHAR,   // parentFolderId
        LogicalType::INTEGER,   // totalItemCount
        LogicalType::INTEGER,   // unreadItemCount
    };
    return std::move(bind_data);
}

void GraphOutlookFunctions::MailFoldersScan(
    ClientContext &,
    TableFunctionInput &data,
    DataChunk &output)
{
    auto &bd = data.bind_data->CastNoConst<OutlookBindData>();
    if (bd.done) { output.SetCardinality(0); return; }

    if (!bd.parsed_doc && !bd.InitIterator()) {
        bd.done = true; output.SetCardinality(0); return;
    }

    idx_t row = 0;
    yyjson_val *item;
    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bd.item_iter)) != nullptr) {
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "id"));
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "displayName"));
        SetStrCell(output.data[2], row, yyjson_obj_get(item, "parentFolderId"));
        SetInt32Cell(output.data[3], row, yyjson_obj_get(item, "totalItemCount"));
        SetInt32Cell(output.data[4], row, yyjson_obj_get(item, "unreadItemCount"));
        row++;
    }

    if (row < STANDARD_VECTOR_SIZE) {
        bd.done = true;
    }
    output.SetCardinality(row);
}

// ============================================================================
// graph_messages
// ============================================================================

// Well-known Graph folder names that can be used directly in the URL path.
static bool IsWellKnownFolder(const std::string &name) {
    static const std::string known[] = {
        "inbox", "sentitems", "deleteditems", "drafts",
        "junkemail", "outbox", "archive", "clutter"
    };
    for (const auto &k : known) {
        if (name == k) { return true; }
    }
    return false;
}

struct MessagesBindData : public OutlookBindData {
    std::string folder;
};

unique_ptr<FunctionData> GraphOutlookFunctions::MessagesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names)
{
    auto bind_data = make_uniq<MessagesBindData>();
    bind_data->secret_name = GetNamedStr(input, "secret");
    bind_data->user        = GetNamedStr(input, "user");
    bind_data->folder      = GetNamedStr(input, "folder");

    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    GraphOutlookClient client(auth_info.auth_params);

    if (bind_data->folder.empty()) {
        bind_data->json_response = client.GetMessages(bind_data->user);
    } else if (IsWellKnownFolder(bind_data->folder)) {
        bind_data->json_response = client.GetFolderMessages(bind_data->user, bind_data->folder);
    } else {
        // Resolve display name → folder id
        const std::string folders_json = client.GetMailFolders(bind_data->user);
        yyjson_doc *doc = yyjson_read(folders_json.c_str(), folders_json.size(), 0);
        if (!doc) {
            throw IOException("graph_messages: failed to parse mail folders response");
        }
        std::string folder_id;
        yyjson_val *root = yyjson_doc_get_root(doc);
        yyjson_val *arr  = yyjson_obj_get(root, "value");
        if (arr && yyjson_is_arr(arr)) {
            size_t idx, max;
            yyjson_val *folder;
            yyjson_arr_foreach(arr, idx, max, folder) {
                yyjson_val *name_val = yyjson_obj_get(folder, "displayName");
                yyjson_val *id_val   = yyjson_obj_get(folder, "id");
                if (name_val && yyjson_is_str(name_val) &&
                    id_val   && yyjson_is_str(id_val) &&
                    bind_data->folder == yyjson_get_str(name_val)) {
                    folder_id = yyjson_get_str(id_val);
                    break;
                }
            }
        }
        yyjson_doc_free(doc);

        if (folder_id.empty()) {
            throw InvalidInputException(
                "graph_messages: no mail folder found with name '" + bind_data->folder + "'");
        }
        bind_data->json_response = client.GetFolderMessages(bind_data->user, folder_id);
    }

    names        = {"id", "subject", "body_preview", "from_name", "from_email",
                    "received_at", "has_attachments", "is_read", "importance", "web_link"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // subject
        LogicalType::VARCHAR,   // bodyPreview
        LogicalType::VARCHAR,   // from name
        LogicalType::VARCHAR,   // from address
        LogicalType::VARCHAR,   // receivedDateTime
        LogicalType::BOOLEAN,   // hasAttachments
        LogicalType::BOOLEAN,   // isRead
        LogicalType::VARCHAR,   // importance
        LogicalType::VARCHAR,   // webLink
    };
    return std::move(bind_data);
}

void GraphOutlookFunctions::MessagesScan(
    ClientContext &,
    TableFunctionInput &data,
    DataChunk &output)
{
    auto &bd = data.bind_data->CastNoConst<MessagesBindData>();
    if (bd.done) { output.SetCardinality(0); return; }

    if (!bd.parsed_doc && !bd.InitIterator()) {
        bd.done = true; output.SetCardinality(0); return;
    }

    idx_t row = 0;
    yyjson_val *item;
    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bd.item_iter)) != nullptr) {
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "id"));
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "subject"));
        SetStrCell(output.data[2], row, yyjson_obj_get(item, "bodyPreview"));

        yyjson_val *from_obj   = yyjson_obj_get(item, "from");
        yyjson_val *from_email = from_obj ? yyjson_obj_get(from_obj, "emailAddress") : nullptr;
        SetStrCell(output.data[3], row, from_email ? yyjson_obj_get(from_email, "name")    : nullptr);
        SetStrCell(output.data[4], row, from_email ? yyjson_obj_get(from_email, "address") : nullptr);

        SetStrCell(output.data[5], row, yyjson_obj_get(item, "receivedDateTime"));
        SetBoolCell(output.data[6], row, yyjson_obj_get(item, "hasAttachments"));
        SetBoolCell(output.data[7], row, yyjson_obj_get(item, "isRead"));
        SetStrCell(output.data[8], row, yyjson_obj_get(item, "importance"));
        SetStrCell(output.data[9], row, yyjson_obj_get(item, "webLink"));
        row++;
    }

    if (row < STANDARD_VECTOR_SIZE) {
        bd.done = true;
    }
    output.SetCardinality(row);
}

// ============================================================================
// Registration
// ============================================================================

void GraphOutlookFunctions::Register(ExtensionLoader &loader) {
    ERPL_TRACE_INFO("GRAPH_OUTLOOK", "Registering Microsoft Graph Outlook functions");

    {
        TableFunction fn("graph_calendars", {}, CalendarsScan, CalendarsBind);
        fn.named_parameters["user"]    = LogicalType::VARCHAR;
        fn.named_parameters["secret"]  = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(fn);
        FunctionDescription desc;
        desc.description = "List Outlook calendars for a user via Microsoft Graph.";
        desc.examples    = {
            "SELECT * FROM graph_calendars(user := 'user-id', secret := 'ms_graph')",
            "SELECT * FROM graph_calendars(secret := 'ms_graph')"
        };
        desc.categories  = {"microsoft", "graph", "outlook"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction fn("graph_calendar_events", {}, CalendarEventsScan, CalendarEventsBind);
        fn.named_parameters["user"]     = LogicalType::VARCHAR;
        fn.named_parameters["calendar_id"] = LogicalType::VARCHAR;
        fn.named_parameters["start_date"]  = LogicalType::VARCHAR;
        fn.named_parameters["end_date"]    = LogicalType::VARCHAR;
        fn.named_parameters["secret"]      = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(fn);
        FunctionDescription desc;
        desc.description = "List calendar events for a user via Microsoft Graph. "
                           "Provide start_date + end_date to use the calendarView endpoint for date-bounded queries.";
        desc.examples    = {
            "SELECT * FROM graph_calendar_events(user := 'user-id', secret := 'ms_graph')",
            "SELECT * FROM graph_calendar_events(user := 'user-id', start_date := '2024-01-01', end_date := '2024-12-31', secret := 'ms_graph')",
            "SELECT * FROM graph_calendar_events(secret := 'ms_graph')"
        };
        desc.categories  = {"microsoft", "graph", "outlook"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction fn("graph_contacts", {}, ContactsScan, ContactsBind);
        fn.named_parameters["user"]    = LogicalType::VARCHAR;
        fn.named_parameters["secret"]  = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(fn);
        FunctionDescription desc;
        desc.description = "List Outlook contacts for a user via Microsoft Graph.";
        desc.examples    = {
            "SELECT * FROM graph_contacts(user := 'user-id', secret := 'ms_graph')",
            "SELECT * FROM graph_contacts(secret := 'ms_graph')"
        };
        desc.categories  = {"microsoft", "graph", "outlook"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction fn("graph_mail_folders", {}, MailFoldersScan, MailFoldersBind);
        fn.named_parameters["user"]    = LogicalType::VARCHAR;
        fn.named_parameters["secret"]  = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(fn);
        FunctionDescription desc;
        desc.description = "List Outlook mail folders for a user via Microsoft Graph.";
        desc.examples    = {
            "SELECT * FROM graph_mail_folders(user := 'user-id', secret := 'ms_graph')",
            "SELECT * FROM graph_mail_folders(secret := 'ms_graph')"
        };
        desc.categories  = {"microsoft", "graph", "outlook"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction fn("graph_messages", {}, MessagesScan, MessagesBind);
        fn.named_parameters["user"]    = LogicalType::VARCHAR;
        fn.named_parameters["folder"]  = LogicalType::VARCHAR;
        fn.named_parameters["secret"]  = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(fn);
        FunctionDescription desc;
        desc.description = "List email messages (metadata only) for a user via Microsoft Graph. "
                           "Pass folder := 'inbox' / 'sentitems' / 'drafts' / 'deleteditems' "
                           "or any display name from graph_mail_folders().";
        desc.examples    = {
            "SELECT * FROM graph_messages(user := 'user-id', secret := 'ms_graph')",
            "SELECT * FROM graph_messages(user := 'user-id', folder := 'inbox', secret := 'ms_graph')",
            "SELECT * FROM graph_messages(secret := 'ms_graph')"
        };
        desc.categories  = {"microsoft", "graph", "outlook"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }

    ERPL_TRACE_INFO("GRAPH_OUTLOOK", "Successfully registered Microsoft Graph Outlook functions");
}

} // namespace erpl_web
