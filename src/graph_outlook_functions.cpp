#include "graph_outlook_functions.hpp"
#include "graph_outlook_client.hpp"
#include "graph_excel_secret.hpp"
#include "tracing.hpp"
#include "duckdb/common/exception.hpp"
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
    bool done = false;
};

struct ContactsBindData : public TableFunctionData {
    std::string secret_name;
    std::string json_response;
    bool done = false;
};

struct MessagesBindData : public TableFunctionData {
    std::string secret_name;
    std::string json_response;
    bool done = false;
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
    bind_data->secret_name = input.inputs[0].GetValue<std::string>();

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

    if (bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphOutlookClient client(auth_info.auth_params);
        bind_data.json_response = client.GetMyEvents();
    }

    yyjson_doc *doc = yyjson_read(bind_data.json_response.c_str(), bind_data.json_response.length(), 0);
    if (!doc) {
        throw InvalidInputException("Failed to parse Graph API response");
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    if (!value_arr || !yyjson_is_arr(value_arr)) {
        yyjson_doc_free(doc);
        bind_data.done = true;
        output.SetCardinality(0);
        return;
    }

    size_t count = yyjson_arr_size(value_arr);
    if (count > STANDARD_VECTOR_SIZE) {
        count = STANDARD_VECTOR_SIZE;
    }

    output.SetCardinality(count);

    size_t idx, max;
    yyjson_val *item;
    idx_t row = 0;

    yyjson_arr_foreach(value_arr, idx, max, item) {
        if (row >= count) break;

        // id
        yyjson_val *id_val = yyjson_obj_get(item, "id");
        output.SetValue(0, row, id_val && yyjson_is_str(id_val) ?
            Value(yyjson_get_str(id_val)) : Value());

        // subject
        yyjson_val *subject_val = yyjson_obj_get(item, "subject");
        output.SetValue(1, row, subject_val && yyjson_is_str(subject_val) ?
            Value(yyjson_get_str(subject_val)) : Value());

        // bodyPreview
        yyjson_val *body_val = yyjson_obj_get(item, "bodyPreview");
        output.SetValue(2, row, body_val && yyjson_is_str(body_val) ?
            Value(yyjson_get_str(body_val)) : Value());

        // start.dateTime
        yyjson_val *start_obj = yyjson_obj_get(item, "start");
        yyjson_val *start_val = start_obj ? yyjson_obj_get(start_obj, "dateTime") : nullptr;
        output.SetValue(3, row, start_val && yyjson_is_str(start_val) ?
            Value(yyjson_get_str(start_val)) : Value());

        // end.dateTime
        yyjson_val *end_obj = yyjson_obj_get(item, "end");
        yyjson_val *end_val = end_obj ? yyjson_obj_get(end_obj, "dateTime") : nullptr;
        output.SetValue(4, row, end_val && yyjson_is_str(end_val) ?
            Value(yyjson_get_str(end_val)) : Value());

        // location.displayName
        yyjson_val *loc_obj = yyjson_obj_get(item, "location");
        yyjson_val *loc_val = loc_obj ? yyjson_obj_get(loc_obj, "displayName") : nullptr;
        output.SetValue(5, row, loc_val && yyjson_is_str(loc_val) ?
            Value(yyjson_get_str(loc_val)) : Value());

        // organizer.emailAddress.name
        yyjson_val *org_obj = yyjson_obj_get(item, "organizer");
        yyjson_val *org_email = org_obj ? yyjson_obj_get(org_obj, "emailAddress") : nullptr;
        yyjson_val *org_name = org_email ? yyjson_obj_get(org_email, "name") : nullptr;
        output.SetValue(6, row, org_name && yyjson_is_str(org_name) ?
            Value(yyjson_get_str(org_name)) : Value());

        // isAllDay
        yyjson_val *allday_val = yyjson_obj_get(item, "isAllDay");
        output.SetValue(7, row, allday_val && yyjson_is_bool(allday_val) ?
            Value::BOOLEAN(yyjson_get_bool(allday_val)) : Value::BOOLEAN(false));

        // isCancelled
        yyjson_val *cancelled_val = yyjson_obj_get(item, "isCancelled");
        output.SetValue(8, row, cancelled_val && yyjson_is_bool(cancelled_val) ?
            Value::BOOLEAN(yyjson_get_bool(cancelled_val)) : Value::BOOLEAN(false));

        // webLink
        yyjson_val *web_val = yyjson_obj_get(item, "webLink");
        output.SetValue(9, row, web_val && yyjson_is_str(web_val) ?
            Value(yyjson_get_str(web_val)) : Value());

        row++;
    }

    yyjson_doc_free(doc);
    bind_data.done = true;
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
    bind_data->secret_name = input.inputs[0].GetValue<std::string>();

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

    if (bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphOutlookClient client(auth_info.auth_params);
        bind_data.json_response = client.GetMyContacts();
    }

    yyjson_doc *doc = yyjson_read(bind_data.json_response.c_str(), bind_data.json_response.length(), 0);
    if (!doc) {
        throw InvalidInputException("Failed to parse Graph API response");
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    if (!value_arr || !yyjson_is_arr(value_arr)) {
        yyjson_doc_free(doc);
        bind_data.done = true;
        output.SetCardinality(0);
        return;
    }

    size_t count = yyjson_arr_size(value_arr);
    if (count > STANDARD_VECTOR_SIZE) {
        count = STANDARD_VECTOR_SIZE;
    }

    output.SetCardinality(count);

    size_t idx, max;
    yyjson_val *item;
    idx_t row = 0;

    yyjson_arr_foreach(value_arr, idx, max, item) {
        if (row >= count) break;

        // id
        yyjson_val *id_val = yyjson_obj_get(item, "id");
        output.SetValue(0, row, id_val && yyjson_is_str(id_val) ?
            Value(yyjson_get_str(id_val)) : Value());

        // displayName
        yyjson_val *display_val = yyjson_obj_get(item, "displayName");
        output.SetValue(1, row, display_val && yyjson_is_str(display_val) ?
            Value(yyjson_get_str(display_val)) : Value());

        // givenName
        yyjson_val *given_val = yyjson_obj_get(item, "givenName");
        output.SetValue(2, row, given_val && yyjson_is_str(given_val) ?
            Value(yyjson_get_str(given_val)) : Value());

        // surname
        yyjson_val *surname_val = yyjson_obj_get(item, "surname");
        output.SetValue(3, row, surname_val && yyjson_is_str(surname_val) ?
            Value(yyjson_get_str(surname_val)) : Value());

        // emailAddresses[0].address
        yyjson_val *emails_arr = yyjson_obj_get(item, "emailAddresses");
        yyjson_val *first_email = emails_arr ? yyjson_arr_get_first(emails_arr) : nullptr;
        yyjson_val *email_addr = first_email ? yyjson_obj_get(first_email, "address") : nullptr;
        output.SetValue(4, row, email_addr && yyjson_is_str(email_addr) ?
            Value(yyjson_get_str(email_addr)) : Value());

        // mobilePhone
        yyjson_val *mobile_val = yyjson_obj_get(item, "mobilePhone");
        output.SetValue(5, row, mobile_val && yyjson_is_str(mobile_val) ?
            Value(yyjson_get_str(mobile_val)) : Value());

        // businessPhones[0]
        yyjson_val *phones_arr = yyjson_obj_get(item, "businessPhones");
        yyjson_val *first_phone = phones_arr ? yyjson_arr_get_first(phones_arr) : nullptr;
        output.SetValue(6, row, first_phone && yyjson_is_str(first_phone) ?
            Value(yyjson_get_str(first_phone)) : Value());

        // companyName
        yyjson_val *company_val = yyjson_obj_get(item, "companyName");
        output.SetValue(7, row, company_val && yyjson_is_str(company_val) ?
            Value(yyjson_get_str(company_val)) : Value());

        // jobTitle
        yyjson_val *job_val = yyjson_obj_get(item, "jobTitle");
        output.SetValue(8, row, job_val && yyjson_is_str(job_val) ?
            Value(yyjson_get_str(job_val)) : Value());

        row++;
    }

    yyjson_doc_free(doc);
    bind_data.done = true;
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
    bind_data->secret_name = input.inputs[0].GetValue<std::string>();

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

    if (bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphOutlookClient client(auth_info.auth_params);
        bind_data.json_response = client.GetMyMessages();
    }

    yyjson_doc *doc = yyjson_read(bind_data.json_response.c_str(), bind_data.json_response.length(), 0);
    if (!doc) {
        throw InvalidInputException("Failed to parse Graph API response");
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value_arr = yyjson_obj_get(root, "value");

    if (!value_arr || !yyjson_is_arr(value_arr)) {
        yyjson_doc_free(doc);
        bind_data.done = true;
        output.SetCardinality(0);
        return;
    }

    size_t count = yyjson_arr_size(value_arr);
    if (count > STANDARD_VECTOR_SIZE) {
        count = STANDARD_VECTOR_SIZE;
    }

    output.SetCardinality(count);

    size_t idx, max;
    yyjson_val *item;
    idx_t row = 0;

    yyjson_arr_foreach(value_arr, idx, max, item) {
        if (row >= count) break;

        // id
        yyjson_val *id_val = yyjson_obj_get(item, "id");
        output.SetValue(0, row, id_val && yyjson_is_str(id_val) ?
            Value(yyjson_get_str(id_val)) : Value());

        // subject
        yyjson_val *subject_val = yyjson_obj_get(item, "subject");
        output.SetValue(1, row, subject_val && yyjson_is_str(subject_val) ?
            Value(yyjson_get_str(subject_val)) : Value());

        // bodyPreview
        yyjson_val *body_val = yyjson_obj_get(item, "bodyPreview");
        output.SetValue(2, row, body_val && yyjson_is_str(body_val) ?
            Value(yyjson_get_str(body_val)) : Value());

        // from.emailAddress.name
        yyjson_val *from_obj = yyjson_obj_get(item, "from");
        yyjson_val *from_email_obj = from_obj ? yyjson_obj_get(from_obj, "emailAddress") : nullptr;
        yyjson_val *from_name = from_email_obj ? yyjson_obj_get(from_email_obj, "name") : nullptr;
        output.SetValue(3, row, from_name && yyjson_is_str(from_name) ?
            Value(yyjson_get_str(from_name)) : Value());

        // from.emailAddress.address
        yyjson_val *from_addr = from_email_obj ? yyjson_obj_get(from_email_obj, "address") : nullptr;
        output.SetValue(4, row, from_addr && yyjson_is_str(from_addr) ?
            Value(yyjson_get_str(from_addr)) : Value());

        // receivedDateTime
        yyjson_val *received_val = yyjson_obj_get(item, "receivedDateTime");
        output.SetValue(5, row, received_val && yyjson_is_str(received_val) ?
            Value(yyjson_get_str(received_val)) : Value());

        // hasAttachments
        yyjson_val *attach_val = yyjson_obj_get(item, "hasAttachments");
        output.SetValue(6, row, attach_val && yyjson_is_bool(attach_val) ?
            Value::BOOLEAN(yyjson_get_bool(attach_val)) : Value::BOOLEAN(false));

        // isRead
        yyjson_val *read_val = yyjson_obj_get(item, "isRead");
        output.SetValue(7, row, read_val && yyjson_is_bool(read_val) ?
            Value::BOOLEAN(yyjson_get_bool(read_val)) : Value::BOOLEAN(false));

        // importance
        yyjson_val *imp_val = yyjson_obj_get(item, "importance");
        output.SetValue(8, row, imp_val && yyjson_is_str(imp_val) ?
            Value(yyjson_get_str(imp_val)) : Value());

        // webLink
        yyjson_val *web_val = yyjson_obj_get(item, "webLink");
        output.SetValue(9, row, web_val && yyjson_is_str(web_val) ?
            Value(yyjson_get_str(web_val)) : Value());

        row++;
    }

    yyjson_doc_free(doc);
    bind_data.done = true;
}

// ============================================================================
// Registration
// ============================================================================

void GraphOutlookFunctions::Register(ExtensionLoader &loader) {
    ERPL_TRACE_INFO("GRAPH_OUTLOOK", "Registering Microsoft Graph Outlook functions");

    // graph_calendar_events(secret_name)
    TableFunction calendar_events("graph_calendar_events", {LogicalType::VARCHAR},
                                  CalendarEventsScan, CalendarEventsBind);
    loader.RegisterFunction(calendar_events);

    // graph_contacts(secret_name)
    TableFunction contacts("graph_contacts", {LogicalType::VARCHAR},
                           ContactsScan, ContactsBind);
    loader.RegisterFunction(contacts);

    // graph_messages(secret_name)
    TableFunction messages("graph_messages", {LogicalType::VARCHAR},
                           MessagesScan, MessagesBind);
    loader.RegisterFunction(messages);

    ERPL_TRACE_INFO("GRAPH_OUTLOOK", "Successfully registered Microsoft Graph Outlook functions");
}

} // namespace erpl_web
