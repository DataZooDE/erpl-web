#include "graph_entra_functions.hpp"
#include "graph_entra_client.hpp"
#include "graph_excel_secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "tracing.hpp"
#include "yyjson.hpp"

using namespace duckdb;
using namespace duckdb_yyjson;

namespace erpl_web {

// =============================================================================
// Bind Data Structures
// =============================================================================

struct GraphUsersBindData : public TableFunctionData {
    std::string secret_name;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::vector<std::string> user_ids;
    std::vector<std::string> display_names;
    std::vector<std::string> user_principal_names;
    std::vector<std::string> mail_addresses;
    std::vector<std::string> job_titles;
    std::vector<std::string> departments;
    std::vector<bool> account_enabled;
    idx_t current_idx = 0;
    bool done = false;
};

struct GraphGroupsBindData : public TableFunctionData {
    std::string secret_name;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::vector<std::string> group_ids;
    std::vector<std::string> display_names;
    std::vector<std::string> descriptions;
    std::vector<std::string> mail_addresses;
    std::vector<bool> mail_enabled;
    std::vector<bool> security_enabled;
    idx_t current_idx = 0;
    bool done = false;
};

struct GraphDevicesBindData : public TableFunctionData {
    std::string secret_name;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::vector<std::string> device_ids;
    std::vector<std::string> display_names;
    std::vector<std::string> operating_systems;
    std::vector<std::string> os_versions;
    std::vector<std::string> trust_types;
    std::vector<bool> account_enabled;
    idx_t current_idx = 0;
    bool done = false;
};

struct GraphSignInLogsBindData : public TableFunctionData {
    std::string secret_name;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::vector<std::string> log_ids;
    std::vector<std::string> user_display_names;
    std::vector<std::string> user_principal_names;
    std::vector<std::string> app_display_names;
    std::vector<std::string> ip_addresses;
    std::vector<std::string> created_datetimes;
    std::vector<std::string> statuses;
    idx_t current_idx = 0;
    bool done = false;
};

// =============================================================================
// Helper Functions
// =============================================================================

static std::string SafeGetString(yyjson_val *obj, const char *key) {
    auto val = yyjson_obj_get(obj, key);
    if (val && yyjson_is_str(val)) {
        return yyjson_get_str(val);
    }
    return "";
}

static bool SafeGetBool(yyjson_val *obj, const char *key, bool default_val = false) {
    auto val = yyjson_obj_get(obj, key);
    if (val && yyjson_is_bool(val)) {
        return yyjson_get_bool(val);
    }
    return default_val;
}

// =============================================================================
// graph_users Implementation
// =============================================================================

unique_ptr<FunctionData> GraphEntraFunctions::UsersBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<GraphUsersBindData>();

    // Get secret name from named parameter (optional - uses first microsoft_graph secret if not provided)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    // Resolve auth
    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    bind_data->auth_params = auth_info.auth_params;

    // Define output columns
    names = {"id", "display_name", "user_principal_name", "mail", "job_title", "department", "account_enabled"};
    return_types = {
        LogicalType::VARCHAR,  // id
        LogicalType::VARCHAR,  // display_name
        LogicalType::VARCHAR,  // user_principal_name
        LogicalType::VARCHAR,  // mail
        LogicalType::VARCHAR,  // job_title
        LogicalType::VARCHAR,  // department
        LogicalType::BOOLEAN   // account_enabled
    };

    // Fetch users
    GraphEntraClient client(bind_data->auth_params);
    auto response = client.GetUsers();

    // Parse response
    auto doc = yyjson_read(response.c_str(), response.length(), 0);
    if (!doc) {
        throw IOException("Failed to parse Graph API response");
    }

    auto root = yyjson_doc_get_root(doc);
    auto value_arr = yyjson_obj_get(root, "value");

    if (value_arr && yyjson_is_arr(value_arr)) {
        size_t idx, max;
        yyjson_val *item;
        yyjson_arr_foreach(value_arr, idx, max, item) {
            bind_data->user_ids.push_back(SafeGetString(item, "id"));
            bind_data->display_names.push_back(SafeGetString(item, "displayName"));
            bind_data->user_principal_names.push_back(SafeGetString(item, "userPrincipalName"));
            bind_data->mail_addresses.push_back(SafeGetString(item, "mail"));
            bind_data->job_titles.push_back(SafeGetString(item, "jobTitle"));
            bind_data->departments.push_back(SafeGetString(item, "department"));
            bind_data->account_enabled.push_back(SafeGetBool(item, "accountEnabled", true));
        }
    }

    yyjson_doc_free(doc);
    return bind_data;
}

void GraphEntraFunctions::UsersScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<GraphUsersBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (bind_data.current_idx < bind_data.user_ids.size() && count < max_count) {
        idx_t i = bind_data.current_idx;

        output.SetValue(0, count, Value(bind_data.user_ids[i]));
        output.SetValue(1, count, Value(bind_data.display_names[i]));
        output.SetValue(2, count, Value(bind_data.user_principal_names[i]));
        output.SetValue(3, count, Value(bind_data.mail_addresses[i]));
        output.SetValue(4, count, Value(bind_data.job_titles[i]));
        output.SetValue(5, count, Value(bind_data.departments[i]));
        output.SetValue(6, count, Value(bind_data.account_enabled[i]));

        bind_data.current_idx++;
        count++;
    }

    if (bind_data.current_idx >= bind_data.user_ids.size()) {
        bind_data.done = true;
    }

    output.SetCardinality(count);
}

// =============================================================================
// graph_groups Implementation
// =============================================================================

unique_ptr<FunctionData> GraphEntraFunctions::GroupsBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<GraphGroupsBindData>();

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    bind_data->auth_params = auth_info.auth_params;

    names = {"id", "display_name", "description", "mail", "mail_enabled", "security_enabled"};
    return_types = {
        LogicalType::VARCHAR,  // id
        LogicalType::VARCHAR,  // display_name
        LogicalType::VARCHAR,  // description
        LogicalType::VARCHAR,  // mail
        LogicalType::BOOLEAN,  // mail_enabled
        LogicalType::BOOLEAN   // security_enabled
    };

    GraphEntraClient client(bind_data->auth_params);
    auto response = client.GetGroups();

    auto doc = yyjson_read(response.c_str(), response.length(), 0);
    if (!doc) {
        throw IOException("Failed to parse Graph API response");
    }

    auto root = yyjson_doc_get_root(doc);
    auto value_arr = yyjson_obj_get(root, "value");

    if (value_arr && yyjson_is_arr(value_arr)) {
        size_t idx, max;
        yyjson_val *item;
        yyjson_arr_foreach(value_arr, idx, max, item) {
            bind_data->group_ids.push_back(SafeGetString(item, "id"));
            bind_data->display_names.push_back(SafeGetString(item, "displayName"));
            bind_data->descriptions.push_back(SafeGetString(item, "description"));
            bind_data->mail_addresses.push_back(SafeGetString(item, "mail"));
            bind_data->mail_enabled.push_back(SafeGetBool(item, "mailEnabled"));
            bind_data->security_enabled.push_back(SafeGetBool(item, "securityEnabled"));
        }
    }

    yyjson_doc_free(doc);
    return bind_data;
}

void GraphEntraFunctions::GroupsScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<GraphGroupsBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (bind_data.current_idx < bind_data.group_ids.size() && count < max_count) {
        idx_t i = bind_data.current_idx;

        output.SetValue(0, count, Value(bind_data.group_ids[i]));
        output.SetValue(1, count, Value(bind_data.display_names[i]));
        output.SetValue(2, count, Value(bind_data.descriptions[i]));
        output.SetValue(3, count, Value(bind_data.mail_addresses[i]));
        output.SetValue(4, count, Value(bind_data.mail_enabled[i]));
        output.SetValue(5, count, Value(bind_data.security_enabled[i]));

        bind_data.current_idx++;
        count++;
    }

    if (bind_data.current_idx >= bind_data.group_ids.size()) {
        bind_data.done = true;
    }

    output.SetCardinality(count);
}

// =============================================================================
// graph_devices Implementation
// =============================================================================

unique_ptr<FunctionData> GraphEntraFunctions::DevicesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<GraphDevicesBindData>();

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    bind_data->auth_params = auth_info.auth_params;

    names = {"id", "display_name", "operating_system", "os_version", "trust_type", "account_enabled"};
    return_types = {
        LogicalType::VARCHAR,  // id
        LogicalType::VARCHAR,  // display_name
        LogicalType::VARCHAR,  // operating_system
        LogicalType::VARCHAR,  // os_version
        LogicalType::VARCHAR,  // trust_type
        LogicalType::BOOLEAN   // account_enabled
    };

    GraphEntraClient client(bind_data->auth_params);
    auto response = client.GetDevices();

    auto doc = yyjson_read(response.c_str(), response.length(), 0);
    if (!doc) {
        throw IOException("Failed to parse Graph API response");
    }

    auto root = yyjson_doc_get_root(doc);
    auto value_arr = yyjson_obj_get(root, "value");

    if (value_arr && yyjson_is_arr(value_arr)) {
        size_t idx, max;
        yyjson_val *item;
        yyjson_arr_foreach(value_arr, idx, max, item) {
            bind_data->device_ids.push_back(SafeGetString(item, "id"));
            bind_data->display_names.push_back(SafeGetString(item, "displayName"));
            bind_data->operating_systems.push_back(SafeGetString(item, "operatingSystem"));
            bind_data->os_versions.push_back(SafeGetString(item, "operatingSystemVersion"));
            bind_data->trust_types.push_back(SafeGetString(item, "trustType"));
            bind_data->account_enabled.push_back(SafeGetBool(item, "accountEnabled", true));
        }
    }

    yyjson_doc_free(doc);
    return bind_data;
}

void GraphEntraFunctions::DevicesScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<GraphDevicesBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (bind_data.current_idx < bind_data.device_ids.size() && count < max_count) {
        idx_t i = bind_data.current_idx;

        output.SetValue(0, count, Value(bind_data.device_ids[i]));
        output.SetValue(1, count, Value(bind_data.display_names[i]));
        output.SetValue(2, count, Value(bind_data.operating_systems[i]));
        output.SetValue(3, count, Value(bind_data.os_versions[i]));
        output.SetValue(4, count, Value(bind_data.trust_types[i]));
        output.SetValue(5, count, Value(bind_data.account_enabled[i]));

        bind_data.current_idx++;
        count++;
    }

    if (bind_data.current_idx >= bind_data.device_ids.size()) {
        bind_data.done = true;
    }

    output.SetCardinality(count);
}

// =============================================================================
// graph_signin_logs Implementation
// =============================================================================

unique_ptr<FunctionData> GraphEntraFunctions::SignInLogsBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<GraphSignInLogsBindData>();

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    bind_data->auth_params = auth_info.auth_params;

    names = {"id", "user_display_name", "user_principal_name", "app_display_name", "ip_address", "created_datetime", "status"};
    return_types = {
        LogicalType::VARCHAR,  // id
        LogicalType::VARCHAR,  // user_display_name
        LogicalType::VARCHAR,  // user_principal_name
        LogicalType::VARCHAR,  // app_display_name
        LogicalType::VARCHAR,  // ip_address
        LogicalType::VARCHAR,  // created_datetime
        LogicalType::VARCHAR   // status
    };

    GraphEntraClient client(bind_data->auth_params);
    auto response = client.GetSignInLogs();

    auto doc = yyjson_read(response.c_str(), response.length(), 0);
    if (!doc) {
        throw IOException("Failed to parse Graph API response");
    }

    auto root = yyjson_doc_get_root(doc);
    auto value_arr = yyjson_obj_get(root, "value");

    if (value_arr && yyjson_is_arr(value_arr)) {
        size_t idx, max;
        yyjson_val *item;
        yyjson_arr_foreach(value_arr, idx, max, item) {
            bind_data->log_ids.push_back(SafeGetString(item, "id"));
            bind_data->user_display_names.push_back(SafeGetString(item, "userDisplayName"));
            bind_data->user_principal_names.push_back(SafeGetString(item, "userPrincipalName"));
            bind_data->app_display_names.push_back(SafeGetString(item, "appDisplayName"));
            bind_data->ip_addresses.push_back(SafeGetString(item, "ipAddress"));
            bind_data->created_datetimes.push_back(SafeGetString(item, "createdDateTime"));

            // Status is nested object with errorCode
            auto status_obj = yyjson_obj_get(item, "status");
            if (status_obj) {
                auto error_code = yyjson_obj_get(status_obj, "errorCode");
                if (error_code && yyjson_is_int(error_code)) {
                    int code = yyjson_get_int(error_code);
                    bind_data->statuses.push_back(code == 0 ? "Success" : "Failure");
                } else {
                    bind_data->statuses.push_back("");
                }
            } else {
                bind_data->statuses.push_back("");
            }
        }
    }

    yyjson_doc_free(doc);
    return bind_data;
}

void GraphEntraFunctions::SignInLogsScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<GraphSignInLogsBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (bind_data.current_idx < bind_data.log_ids.size() && count < max_count) {
        idx_t i = bind_data.current_idx;

        output.SetValue(0, count, Value(bind_data.log_ids[i]));
        output.SetValue(1, count, Value(bind_data.user_display_names[i]));
        output.SetValue(2, count, Value(bind_data.user_principal_names[i]));
        output.SetValue(3, count, Value(bind_data.app_display_names[i]));
        output.SetValue(4, count, Value(bind_data.ip_addresses[i]));
        output.SetValue(5, count, Value(bind_data.created_datetimes[i]));
        output.SetValue(6, count, Value(bind_data.statuses[i]));

        bind_data.current_idx++;
        count++;
    }

    if (bind_data.current_idx >= bind_data.log_ids.size()) {
        bind_data.done = true;
    }

    output.SetCardinality(count);
}

// =============================================================================
// Registration
// =============================================================================

void GraphEntraFunctions::Register(ExtensionLoader &loader) {
    // graph_users - no required params, optional secret named param
    TableFunction users_func("graph_users", {}, UsersScan, UsersBind);
    users_func.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(users_func);

    // graph_groups - no required params, optional secret named param
    TableFunction groups_func("graph_groups", {}, GroupsScan, GroupsBind);
    groups_func.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(groups_func);

    // graph_devices - no required params, optional secret named param
    TableFunction devices_func("graph_devices", {}, DevicesScan, DevicesBind);
    devices_func.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(devices_func);

    // graph_signin_logs - no required params, optional secret named param
    TableFunction signin_logs_func("graph_signin_logs", {}, SignInLogsScan, SignInLogsBind);
    signin_logs_func.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(signin_logs_func);
}

} // namespace erpl_web
