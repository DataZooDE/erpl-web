#include "graph_teams_functions.hpp"
#include "graph_teams_client.hpp"
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

struct GraphMyTeamsBindData : public TableFunctionData {
    std::string secret_name;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::vector<std::string> team_ids;
    std::vector<std::string> display_names;
    std::vector<std::string> descriptions;
    std::vector<std::string> visibilities;
    idx_t current_idx = 0;
    bool done = false;
};

struct GraphTeamChannelsBindData : public TableFunctionData {
    std::string secret_name;
    std::string team_id;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::vector<std::string> channel_ids;
    std::vector<std::string> display_names;
    std::vector<std::string> descriptions;
    std::vector<std::string> membership_types;
    idx_t current_idx = 0;
    bool done = false;
};

struct GraphTeamMembersBindData : public TableFunctionData {
    std::string secret_name;
    std::string team_id;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::vector<std::string> member_ids;
    std::vector<std::string> display_names;
    std::vector<std::string> emails;
    std::vector<std::string> roles;
    idx_t current_idx = 0;
    bool done = false;
};

struct GraphChannelMessagesBindData : public TableFunctionData {
    std::string secret_name;
    std::string team_id;
    std::string channel_id;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::vector<std::string> message_ids;
    std::vector<std::string> created_datetimes;
    std::vector<std::string> from_names;
    std::vector<std::string> body_contents;
    std::vector<std::string> importance_levels;
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

// =============================================================================
// graph_my_teams Implementation
// =============================================================================

unique_ptr<FunctionData> GraphTeamsFunctions::MyTeamsBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<GraphMyTeamsBindData>();

    if (input.inputs.empty()) {
        throw BinderException("graph_my_teams requires a secret name parameter");
    }
    bind_data->secret_name = input.inputs[0].GetValue<std::string>();

    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    bind_data->auth_params = auth_info.auth_params;

    names = {"id", "display_name", "description", "visibility"};
    return_types = {
        LogicalType::VARCHAR,
        LogicalType::VARCHAR,
        LogicalType::VARCHAR,
        LogicalType::VARCHAR
    };

    GraphTeamsClient client(bind_data->auth_params);
    auto response = client.GetMyTeams();

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
            bind_data->team_ids.push_back(SafeGetString(item, "id"));
            bind_data->display_names.push_back(SafeGetString(item, "displayName"));
            bind_data->descriptions.push_back(SafeGetString(item, "description"));
            bind_data->visibilities.push_back(SafeGetString(item, "visibility"));
        }
    }

    yyjson_doc_free(doc);
    return bind_data;
}

void GraphTeamsFunctions::MyTeamsScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<GraphMyTeamsBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (bind_data.current_idx < bind_data.team_ids.size() && count < max_count) {
        idx_t i = bind_data.current_idx;

        output.SetValue(0, count, Value(bind_data.team_ids[i]));
        output.SetValue(1, count, Value(bind_data.display_names[i]));
        output.SetValue(2, count, Value(bind_data.descriptions[i]));
        output.SetValue(3, count, Value(bind_data.visibilities[i]));

        bind_data.current_idx++;
        count++;
    }

    if (bind_data.current_idx >= bind_data.team_ids.size()) {
        bind_data.done = true;
    }

    output.SetCardinality(count);
}

// =============================================================================
// graph_team_channels Implementation
// =============================================================================

unique_ptr<FunctionData> GraphTeamsFunctions::TeamChannelsBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<GraphTeamChannelsBindData>();

    if (input.inputs.size() < 2) {
        throw BinderException("graph_team_channels requires secret_name and team_id parameters");
    }
    bind_data->secret_name = input.inputs[0].GetValue<std::string>();
    bind_data->team_id = input.inputs[1].GetValue<std::string>();

    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    bind_data->auth_params = auth_info.auth_params;

    names = {"id", "display_name", "description", "membership_type"};
    return_types = {
        LogicalType::VARCHAR,
        LogicalType::VARCHAR,
        LogicalType::VARCHAR,
        LogicalType::VARCHAR
    };

    GraphTeamsClient client(bind_data->auth_params);
    auto response = client.GetTeamChannels(bind_data->team_id);

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
            bind_data->channel_ids.push_back(SafeGetString(item, "id"));
            bind_data->display_names.push_back(SafeGetString(item, "displayName"));
            bind_data->descriptions.push_back(SafeGetString(item, "description"));
            bind_data->membership_types.push_back(SafeGetString(item, "membershipType"));
        }
    }

    yyjson_doc_free(doc);
    return bind_data;
}

void GraphTeamsFunctions::TeamChannelsScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<GraphTeamChannelsBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (bind_data.current_idx < bind_data.channel_ids.size() && count < max_count) {
        idx_t i = bind_data.current_idx;

        output.SetValue(0, count, Value(bind_data.channel_ids[i]));
        output.SetValue(1, count, Value(bind_data.display_names[i]));
        output.SetValue(2, count, Value(bind_data.descriptions[i]));
        output.SetValue(3, count, Value(bind_data.membership_types[i]));

        bind_data.current_idx++;
        count++;
    }

    if (bind_data.current_idx >= bind_data.channel_ids.size()) {
        bind_data.done = true;
    }

    output.SetCardinality(count);
}

// =============================================================================
// graph_team_members Implementation
// =============================================================================

unique_ptr<FunctionData> GraphTeamsFunctions::TeamMembersBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<GraphTeamMembersBindData>();

    if (input.inputs.size() < 2) {
        throw BinderException("graph_team_members requires secret_name and team_id parameters");
    }
    bind_data->secret_name = input.inputs[0].GetValue<std::string>();
    bind_data->team_id = input.inputs[1].GetValue<std::string>();

    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    bind_data->auth_params = auth_info.auth_params;

    names = {"id", "display_name", "email", "role"};
    return_types = {
        LogicalType::VARCHAR,
        LogicalType::VARCHAR,
        LogicalType::VARCHAR,
        LogicalType::VARCHAR
    };

    GraphTeamsClient client(bind_data->auth_params);
    auto response = client.GetTeamMembers(bind_data->team_id);

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
            bind_data->member_ids.push_back(SafeGetString(item, "id"));
            bind_data->display_names.push_back(SafeGetString(item, "displayName"));
            bind_data->emails.push_back(SafeGetString(item, "email"));

            // Roles is an array, get first role if available
            auto roles_arr = yyjson_obj_get(item, "roles");
            if (roles_arr && yyjson_is_arr(roles_arr) && yyjson_arr_size(roles_arr) > 0) {
                auto first_role = yyjson_arr_get_first(roles_arr);
                if (first_role && yyjson_is_str(first_role)) {
                    bind_data->roles.push_back(yyjson_get_str(first_role));
                } else {
                    bind_data->roles.push_back("member");
                }
            } else {
                bind_data->roles.push_back("member");
            }
        }
    }

    yyjson_doc_free(doc);
    return bind_data;
}

void GraphTeamsFunctions::TeamMembersScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<GraphTeamMembersBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (bind_data.current_idx < bind_data.member_ids.size() && count < max_count) {
        idx_t i = bind_data.current_idx;

        output.SetValue(0, count, Value(bind_data.member_ids[i]));
        output.SetValue(1, count, Value(bind_data.display_names[i]));
        output.SetValue(2, count, Value(bind_data.emails[i]));
        output.SetValue(3, count, Value(bind_data.roles[i]));

        bind_data.current_idx++;
        count++;
    }

    if (bind_data.current_idx >= bind_data.member_ids.size()) {
        bind_data.done = true;
    }

    output.SetCardinality(count);
}

// =============================================================================
// graph_channel_messages Implementation
// =============================================================================

unique_ptr<FunctionData> GraphTeamsFunctions::ChannelMessagesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<GraphChannelMessagesBindData>();

    if (input.inputs.size() < 3) {
        throw BinderException("graph_channel_messages requires secret_name, team_id, and channel_id parameters");
    }
    bind_data->secret_name = input.inputs[0].GetValue<std::string>();
    bind_data->team_id = input.inputs[1].GetValue<std::string>();
    bind_data->channel_id = input.inputs[2].GetValue<std::string>();

    auto auth_info = ResolveGraphAuth(context, bind_data->secret_name);
    bind_data->auth_params = auth_info.auth_params;

    names = {"id", "created_datetime", "from_name", "body_content", "importance"};
    return_types = {
        LogicalType::VARCHAR,
        LogicalType::VARCHAR,
        LogicalType::VARCHAR,
        LogicalType::VARCHAR,
        LogicalType::VARCHAR
    };

    GraphTeamsClient client(bind_data->auth_params);
    auto response = client.GetChannelMessages(bind_data->team_id, bind_data->channel_id);

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
            bind_data->message_ids.push_back(SafeGetString(item, "id"));
            bind_data->created_datetimes.push_back(SafeGetString(item, "createdDateTime"));

            // from is a nested object
            auto from_obj = yyjson_obj_get(item, "from");
            if (from_obj) {
                auto user_obj = yyjson_obj_get(from_obj, "user");
                if (user_obj) {
                    bind_data->from_names.push_back(SafeGetString(user_obj, "displayName"));
                } else {
                    bind_data->from_names.push_back("");
                }
            } else {
                bind_data->from_names.push_back("");
            }

            // body is a nested object
            auto body_obj = yyjson_obj_get(item, "body");
            if (body_obj) {
                bind_data->body_contents.push_back(SafeGetString(body_obj, "content"));
            } else {
                bind_data->body_contents.push_back("");
            }

            bind_data->importance_levels.push_back(SafeGetString(item, "importance"));
        }
    }

    yyjson_doc_free(doc);
    return bind_data;
}

void GraphTeamsFunctions::ChannelMessagesScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<GraphChannelMessagesBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (bind_data.current_idx < bind_data.message_ids.size() && count < max_count) {
        idx_t i = bind_data.current_idx;

        output.SetValue(0, count, Value(bind_data.message_ids[i]));
        output.SetValue(1, count, Value(bind_data.created_datetimes[i]));
        output.SetValue(2, count, Value(bind_data.from_names[i]));
        output.SetValue(3, count, Value(bind_data.body_contents[i]));
        output.SetValue(4, count, Value(bind_data.importance_levels[i]));

        bind_data.current_idx++;
        count++;
    }

    if (bind_data.current_idx >= bind_data.message_ids.size()) {
        bind_data.done = true;
    }

    output.SetCardinality(count);
}

// =============================================================================
// Registration
// =============================================================================

void GraphTeamsFunctions::Register(ExtensionLoader &loader) {
    // graph_my_teams
    TableFunction my_teams_func("graph_my_teams", {LogicalType::VARCHAR}, MyTeamsScan, MyTeamsBind);
    loader.RegisterFunction(my_teams_func);

    // graph_team_channels
    TableFunction team_channels_func("graph_team_channels", {LogicalType::VARCHAR, LogicalType::VARCHAR}, TeamChannelsScan, TeamChannelsBind);
    loader.RegisterFunction(team_channels_func);

    // graph_team_members
    TableFunction team_members_func("graph_team_members", {LogicalType::VARCHAR, LogicalType::VARCHAR}, TeamMembersScan, TeamMembersBind);
    loader.RegisterFunction(team_members_func);

    // graph_channel_messages
    TableFunction channel_messages_func("graph_channel_messages", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, ChannelMessagesScan, ChannelMessagesBind);
    loader.RegisterFunction(channel_messages_func);
}

} // namespace erpl_web
