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
// Bind Data Structures
// ============================================================================

struct CalendarEventsBindData : public TableFunctionData {
    std::string secret_name;
    std::string json_response;
    yyjson_doc *parsed_doc = nullptr;
    yyjson_arr_iter item_iter = {};
    bool done = false;

    ~CalendarEventsBindData() override {
        if (parsed_doc) {
            yyjson_doc_free(parsed_doc);
        }
    }

    bool InitIterator() {
        parsed_doc = yyjson_read(json_response.c_str(), json_response.length(), 0);
        json_response.clear();
        json_response.shrink_to_fit();
        if (!parsed_doc) {
            return false;
        }
        yyjson_val *root = yyjson_doc_get_root(parsed_doc);
        yyjson_val *arr = yyjson_obj_get(root, "value");
        if (!arr || !yyjson_is_arr(arr)) {
            return false;
        }
        yyjson_arr_iter_init(arr, &item_iter);
        return true;
    }
};

struct ContactsBindData : public TableFunctionData {
    std::string secret_name;
    std::string json_response;
    yyjson_doc *parsed_doc = nullptr;
    yyjson_arr_iter item_iter = {};
    bool done = false;

    ~ContactsBindData() override {
        if (parsed_doc) {
            yyjson_doc_free(parsed_doc);
        }
    }

    bool InitIterator() {
        parsed_doc = yyjson_read(json_response.c_str(), json_response.length(), 0);
        json_response.clear();
        json_response.shrink_to_fit();
        if (!parsed_doc) {
            return false;
        }
        yyjson_val *root = yyjson_doc_get_root(parsed_doc);
        yyjson_val *arr = yyjson_obj_get(root, "value");
        if (!arr || !yyjson_is_arr(arr)) {
            return false;
        }
        yyjson_arr_iter_init(arr, &item_iter);
        return true;
    }
};

struct MessagesBindData : public TableFunctionData {
    std::string secret_name;
    std::string json_response;
    yyjson_doc *parsed_doc = nullptr;
    yyjson_arr_iter item_iter = {};
    bool done = false;

    ~MessagesBindData() override {
        if (parsed_doc) {
            yyjson_doc_free(parsed_doc);
        }
    }

    bool InitIterator() {
        parsed_doc = yyjson_read(json_response.c_str(), json_response.length(), 0);
        json_response.clear();
        json_response.shrink_to_fit();
        if (!parsed_doc) {
            return false;
        }
        yyjson_val *root = yyjson_doc_get_root(parsed_doc);
        yyjson_val *arr = yyjson_obj_get(root, "value");
        if (!arr || !yyjson_is_arr(arr)) {
            return false;
        }
        yyjson_arr_iter_init(arr, &item_iter);
        return true;
    }
};

// ============================================================================
// graph_calendar_events - List calendar events
// ============================================================================

unique_ptr<FunctionData> GraphOutlookFunctions::CalendarEventsBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<CalendarEventsBindData>();

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    // Return schema for calendar events
    names = {"id", "subject", "body_preview", "start_time", "end_time", "location",
             "organizer_name", "is_all_day", "is_cancelled", "web_link"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // subject
        LogicalType::VARCHAR,   // body_preview
        LogicalType::VARCHAR,   // start_time
        LogicalType::VARCHAR,   // end_time
        LogicalType::VARCHAR,   // location
        LogicalType::VARCHAR,   // organizer_name
        LogicalType::BOOLEAN,   // is_all_day
        LogicalType::BOOLEAN,   // is_cancelled
        LogicalType::VARCHAR    // web_link
    };

    return std::move(bind_data);
}

void GraphOutlookFunctions::CalendarEventsScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<CalendarEventsBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    // Fetch and parse JSON on first call
    if (!bind_data.parsed_doc) {
        if (bind_data.json_response.empty()) {
            auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
            GraphOutlookClient client(auth_info.auth_params);
            bind_data.json_response = client.GetMyEvents();
        }
        if (!bind_data.InitIterator()) {
            bind_data.done = true;
            output.SetCardinality(0);
            return;
        }
    }

    idx_t row = 0;
    yyjson_val *item;

    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bind_data.item_iter)) != nullptr) {
        // id
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "id"));

        // subject
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "subject"));

        // bodyPreview
        SetStrCell(output.data[2], row, yyjson_obj_get(item, "bodyPreview"));

        // start.dateTime
        yyjson_val *start_obj = yyjson_obj_get(item, "start");
        SetStrCell(output.data[3], row, start_obj ? yyjson_obj_get(start_obj, "dateTime") : nullptr);

        // end.dateTime
        yyjson_val *end_obj = yyjson_obj_get(item, "end");
        SetStrCell(output.data[4], row, end_obj ? yyjson_obj_get(end_obj, "dateTime") : nullptr);

        // location.displayName
        yyjson_val *loc_obj = yyjson_obj_get(item, "location");
        SetStrCell(output.data[5], row, loc_obj ? yyjson_obj_get(loc_obj, "displayName") : nullptr);

        // organizer.emailAddress.name
        yyjson_val *org_obj = yyjson_obj_get(item, "organizer");
        yyjson_val *org_email = org_obj ? yyjson_obj_get(org_obj, "emailAddress") : nullptr;
        SetStrCell(output.data[6], row, org_email ? yyjson_obj_get(org_email, "name") : nullptr);

        // isAllDay — default false when absent
        {
            yyjson_val *allday_val = yyjson_obj_get(item, "isAllDay");
            FlatVector::GetData<bool>(output.data[7])[row] =
                (allday_val && yyjson_is_bool(allday_val)) ? yyjson_get_bool(allday_val) : false;
        }

        // isCancelled — default false when absent
        {
            yyjson_val *cancelled_val = yyjson_obj_get(item, "isCancelled");
            FlatVector::GetData<bool>(output.data[8])[row] =
                (cancelled_val && yyjson_is_bool(cancelled_val)) ? yyjson_get_bool(cancelled_val) : false;
        }

        // webLink
        SetStrCell(output.data[9], row, yyjson_obj_get(item, "webLink"));

        row++;
    }

    // Mark done when the batch is smaller than a full vector — no more items remain.
    bind_data.done = row < STANDARD_VECTOR_SIZE;
    output.SetCardinality(row);
}

// ============================================================================
// graph_contacts - List contacts
// ============================================================================

unique_ptr<FunctionData> GraphOutlookFunctions::ContactsBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<ContactsBindData>();

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    // Return schema for contacts
    names = {"id", "display_name", "given_name", "surname", "email", "mobile_phone",
             "business_phone", "company_name", "job_title"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // display_name
        LogicalType::VARCHAR,   // given_name
        LogicalType::VARCHAR,   // surname
        LogicalType::VARCHAR,   // email
        LogicalType::VARCHAR,   // mobile_phone
        LogicalType::VARCHAR,   // business_phone
        LogicalType::VARCHAR,   // company_name
        LogicalType::VARCHAR    // job_title
    };

    return std::move(bind_data);
}

void GraphOutlookFunctions::ContactsScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<ContactsBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    // Fetch and parse JSON on first call
    if (!bind_data.parsed_doc) {
        if (bind_data.json_response.empty()) {
            auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
            GraphOutlookClient client(auth_info.auth_params);
            bind_data.json_response = client.GetMyContacts();
        }
        if (!bind_data.InitIterator()) {
            bind_data.done = true;
            output.SetCardinality(0);
            return;
        }
    }

    idx_t row = 0;
    yyjson_val *item;

    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bind_data.item_iter)) != nullptr) {
        // id
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "id"));

        // displayName
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "displayName"));

        // givenName
        SetStrCell(output.data[2], row, yyjson_obj_get(item, "givenName"));

        // surname
        SetStrCell(output.data[3], row, yyjson_obj_get(item, "surname"));

        // emailAddresses[0].address — yyjson_arr_get_first is O(1)
        yyjson_val *emails_arr = yyjson_obj_get(item, "emailAddresses");
        yyjson_val *first_email = emails_arr ? yyjson_arr_get_first(emails_arr) : nullptr;
        SetStrCell(output.data[4], row, first_email ? yyjson_obj_get(first_email, "address") : nullptr);

        // mobilePhone
        SetStrCell(output.data[5], row, yyjson_obj_get(item, "mobilePhone"));

        // businessPhones[0] — yyjson_arr_get_first is O(1)
        yyjson_val *phones_arr = yyjson_obj_get(item, "businessPhones");
        yyjson_val *first_phone = phones_arr ? yyjson_arr_get_first(phones_arr) : nullptr;
        SetStrCell(output.data[6], row, first_phone);

        // companyName
        SetStrCell(output.data[7], row, yyjson_obj_get(item, "companyName"));

        // jobTitle
        SetStrCell(output.data[8], row, yyjson_obj_get(item, "jobTitle"));

        row++;
    }

    bind_data.done = row < STANDARD_VECTOR_SIZE;
    output.SetCardinality(row);
}

// ============================================================================
// graph_messages - List email messages (metadata only)
// ============================================================================

unique_ptr<FunctionData> GraphOutlookFunctions::MessagesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<MessagesBindData>();

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    // Return schema for messages (metadata only, no body content)
    names = {"id", "subject", "body_preview", "from_name", "from_email",
             "received_at", "has_attachments", "is_read", "importance", "web_link"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // subject
        LogicalType::VARCHAR,   // body_preview
        LogicalType::VARCHAR,   // from_name
        LogicalType::VARCHAR,   // from_email
        LogicalType::VARCHAR,   // received_at
        LogicalType::BOOLEAN,   // has_attachments
        LogicalType::BOOLEAN,   // is_read
        LogicalType::VARCHAR,   // importance
        LogicalType::VARCHAR    // web_link
    };

    return std::move(bind_data);
}

void GraphOutlookFunctions::MessagesScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<MessagesBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    // Fetch and parse JSON on first call
    if (!bind_data.parsed_doc) {
        if (bind_data.json_response.empty()) {
            auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
            GraphOutlookClient client(auth_info.auth_params);
            bind_data.json_response = client.GetMyMessages();
        }
        if (!bind_data.InitIterator()) {
            bind_data.done = true;
            output.SetCardinality(0);
            return;
        }
    }

    idx_t row = 0;
    yyjson_val *item;

    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bind_data.item_iter)) != nullptr) {
        // id
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "id"));

        // subject
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "subject"));

        // bodyPreview
        SetStrCell(output.data[2], row, yyjson_obj_get(item, "bodyPreview"));

        // from.emailAddress.name
        yyjson_val *from_obj = yyjson_obj_get(item, "from");
        yyjson_val *from_email_obj = from_obj ? yyjson_obj_get(from_obj, "emailAddress") : nullptr;
        SetStrCell(output.data[3], row, from_email_obj ? yyjson_obj_get(from_email_obj, "name") : nullptr);

        // from.emailAddress.address
        SetStrCell(output.data[4], row, from_email_obj ? yyjson_obj_get(from_email_obj, "address") : nullptr);

        // receivedDateTime
        SetStrCell(output.data[5], row, yyjson_obj_get(item, "receivedDateTime"));

        // hasAttachments — default false when absent
        {
            yyjson_val *attach_val = yyjson_obj_get(item, "hasAttachments");
            FlatVector::GetData<bool>(output.data[6])[row] =
                (attach_val && yyjson_is_bool(attach_val)) ? yyjson_get_bool(attach_val) : false;
        }

        // isRead — default false when absent
        {
            yyjson_val *read_val = yyjson_obj_get(item, "isRead");
            FlatVector::GetData<bool>(output.data[7])[row] =
                (read_val && yyjson_is_bool(read_val)) ? yyjson_get_bool(read_val) : false;
        }

        // importance
        SetStrCell(output.data[8], row, yyjson_obj_get(item, "importance"));

        // webLink
        SetStrCell(output.data[9], row, yyjson_obj_get(item, "webLink"));

        row++;
    }

    bind_data.done = row < STANDARD_VECTOR_SIZE;
    output.SetCardinality(row);
}

// ============================================================================
// Registration
// ============================================================================

void GraphOutlookFunctions::Register(ExtensionLoader &loader) {
    ERPL_TRACE_INFO("GRAPH_OUTLOOK", "Registering Microsoft Graph Outlook functions");

    {
        TableFunction calendar_events("graph_calendar_events", {},
                                      CalendarEventsScan, CalendarEventsBind);
        calendar_events.named_parameters["secret"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(calendar_events);
        FunctionDescription desc;
        desc.description = "List calendar events from the authenticated user's Outlook calendar via Microsoft Graph.";
        desc.parameter_names = {};
        desc.parameter_types = {};
        desc.examples = {"SELECT * FROM graph_calendar_events(secret := 'ms_graph')"};
        desc.categories = {"microsoft", "graph", "outlook"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction contacts("graph_contacts", {},
                               ContactsScan, ContactsBind);
        contacts.named_parameters["secret"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(contacts);
        FunctionDescription desc;
        desc.description = "List contacts from the authenticated user's Outlook contacts via Microsoft Graph.";
        desc.parameter_names = {};
        desc.parameter_types = {};
        desc.examples = {"SELECT * FROM graph_contacts(secret := 'ms_graph')"};
        desc.categories = {"microsoft", "graph", "outlook"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction messages("graph_messages", {},
                               MessagesScan, MessagesBind);
        messages.named_parameters["secret"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(messages);
        FunctionDescription desc;
        desc.description = "List email messages (metadata only) from the authenticated user's Outlook inbox via Microsoft Graph.";
        desc.parameter_names = {};
        desc.parameter_types = {};
        desc.examples = {"SELECT * FROM graph_messages(secret := 'ms_graph')"};
        desc.categories = {"microsoft", "graph", "outlook"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }

    ERPL_TRACE_INFO("GRAPH_OUTLOOK", "Successfully registered Microsoft Graph Outlook functions");
}

} // namespace erpl_web
