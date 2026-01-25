#include "graph_planner_functions.hpp"
#include "graph_planner_client.hpp"
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

struct PlansBindData : public TableFunctionData {
    std::string secret_name;
    std::string group_id;
    std::string json_response;
    bool done = false;
};

struct BucketsBindData : public TableFunctionData {
    std::string secret_name;
    std::string plan_id;
    std::string json_response;
    bool done = false;
};

struct TasksBindData : public TableFunctionData {
    std::string secret_name;
    std::string plan_id;
    std::string json_response;
    bool done = false;
};

// ============================================================================
// graph_planner_plans - List plans for a group
// ============================================================================

unique_ptr<FunctionData> GraphPlannerFunctions::PlansBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<PlansBindData>();

    // group_id is required positional parameter
    if (input.inputs.empty()) {
        throw BinderException("graph_planner_plans requires a group_id parameter");
    }
    bind_data->group_id = input.inputs[0].GetValue<std::string>();

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    // Return schema: id, title, owner (group id), createdDateTime
    names = {"id", "title", "owner_group_id", "created_at"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // title
        LogicalType::VARCHAR,   // owner_group_id
        LogicalType::VARCHAR    // created_at
    };

    return std::move(bind_data);
}

void GraphPlannerFunctions::PlansScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<PlansBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    if (bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphPlannerClient client(auth_info.auth_params);
        bind_data.json_response = client.GetGroupPlans(bind_data.group_id);
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

        // title
        yyjson_val *title_val = yyjson_obj_get(item, "title");
        output.SetValue(1, row, title_val && yyjson_is_str(title_val) ?
            Value(yyjson_get_str(title_val)) : Value());

        // owner (group id)
        yyjson_val *owner_val = yyjson_obj_get(item, "owner");
        output.SetValue(2, row, owner_val && yyjson_is_str(owner_val) ?
            Value(yyjson_get_str(owner_val)) : Value());

        // createdDateTime
        yyjson_val *created_val = yyjson_obj_get(item, "createdDateTime");
        output.SetValue(3, row, created_val && yyjson_is_str(created_val) ?
            Value(yyjson_get_str(created_val)) : Value());

        row++;
    }

    yyjson_doc_free(doc);
    bind_data.done = true;
}

// ============================================================================
// graph_planner_buckets - List buckets in a plan
// ============================================================================

unique_ptr<FunctionData> GraphPlannerFunctions::BucketsBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<BucketsBindData>();

    // plan_id is required positional parameter
    if (input.inputs.empty()) {
        throw BinderException("graph_planner_buckets requires a plan_id parameter");
    }
    bind_data->plan_id = input.inputs[0].GetValue<std::string>();

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    // Return schema: id, name, planId, orderHint
    names = {"id", "name", "plan_id", "order_hint"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // name
        LogicalType::VARCHAR,   // plan_id
        LogicalType::VARCHAR    // order_hint
    };

    return std::move(bind_data);
}

void GraphPlannerFunctions::BucketsScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<BucketsBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    if (bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphPlannerClient client(auth_info.auth_params);
        bind_data.json_response = client.GetPlanBuckets(bind_data.plan_id);
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

        // name
        yyjson_val *name_val = yyjson_obj_get(item, "name");
        output.SetValue(1, row, name_val && yyjson_is_str(name_val) ?
            Value(yyjson_get_str(name_val)) : Value());

        // planId
        yyjson_val *plan_val = yyjson_obj_get(item, "planId");
        output.SetValue(2, row, plan_val && yyjson_is_str(plan_val) ?
            Value(yyjson_get_str(plan_val)) : Value());

        // orderHint
        yyjson_val *order_val = yyjson_obj_get(item, "orderHint");
        output.SetValue(3, row, order_val && yyjson_is_str(order_val) ?
            Value(yyjson_get_str(order_val)) : Value());

        row++;
    }

    yyjson_doc_free(doc);
    bind_data.done = true;
}

// ============================================================================
// graph_planner_tasks - List tasks in a plan
// ============================================================================

unique_ptr<FunctionData> GraphPlannerFunctions::TasksBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    auto bind_data = make_uniq<TasksBindData>();

    // plan_id is required positional parameter
    if (input.inputs.empty()) {
        throw BinderException("graph_planner_tasks requires a plan_id parameter");
    }
    bind_data->plan_id = input.inputs[0].GetValue<std::string>();

    // Get secret name from named parameter (optional)
    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }
    bind_data->secret_name = secret_name;

    // Return schema for planner tasks
    names = {"id", "title", "bucket_id", "plan_id", "percent_complete", "priority",
             "due_at", "created_at", "completed_at"};
    return_types = {
        LogicalType::VARCHAR,   // id
        LogicalType::VARCHAR,   // title
        LogicalType::VARCHAR,   // bucket_id
        LogicalType::VARCHAR,   // plan_id
        LogicalType::INTEGER,   // percent_complete
        LogicalType::INTEGER,   // priority
        LogicalType::VARCHAR,   // due_at
        LogicalType::VARCHAR,   // created_at
        LogicalType::VARCHAR    // completed_at
    };

    return std::move(bind_data);
}

void GraphPlannerFunctions::TasksScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<TasksBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    if (bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphPlannerClient client(auth_info.auth_params);
        bind_data.json_response = client.GetPlanTasks(bind_data.plan_id);
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

        // title
        yyjson_val *title_val = yyjson_obj_get(item, "title");
        output.SetValue(1, row, title_val && yyjson_is_str(title_val) ?
            Value(yyjson_get_str(title_val)) : Value());

        // bucketId
        yyjson_val *bucket_val = yyjson_obj_get(item, "bucketId");
        output.SetValue(2, row, bucket_val && yyjson_is_str(bucket_val) ?
            Value(yyjson_get_str(bucket_val)) : Value());

        // planId
        yyjson_val *plan_val = yyjson_obj_get(item, "planId");
        output.SetValue(3, row, plan_val && yyjson_is_str(plan_val) ?
            Value(yyjson_get_str(plan_val)) : Value());

        // percentComplete
        yyjson_val *pct_val = yyjson_obj_get(item, "percentComplete");
        output.SetValue(4, row, pct_val && yyjson_is_num(pct_val) ?
            Value::INTEGER(yyjson_get_int(pct_val)) : Value());

        // priority (0=urgent, 1=important, 2=medium, 3=low)
        yyjson_val *pri_val = yyjson_obj_get(item, "priority");
        output.SetValue(5, row, pri_val && yyjson_is_num(pri_val) ?
            Value::INTEGER(yyjson_get_int(pri_val)) : Value());

        // dueDateTime
        yyjson_val *due_val = yyjson_obj_get(item, "dueDateTime");
        output.SetValue(6, row, due_val && yyjson_is_str(due_val) ?
            Value(yyjson_get_str(due_val)) : Value());

        // createdDateTime
        yyjson_val *created_val = yyjson_obj_get(item, "createdDateTime");
        output.SetValue(7, row, created_val && yyjson_is_str(created_val) ?
            Value(yyjson_get_str(created_val)) : Value());

        // completedDateTime
        yyjson_val *completed_val = yyjson_obj_get(item, "completedDateTime");
        output.SetValue(8, row, completed_val && yyjson_is_str(completed_val) ?
            Value(yyjson_get_str(completed_val)) : Value());

        row++;
    }

    yyjson_doc_free(doc);
    bind_data.done = true;
}

// ============================================================================
// Registration
// ============================================================================

void GraphPlannerFunctions::Register(ExtensionLoader &loader) {
    ERPL_TRACE_INFO("GRAPH_PLANNER", "Registering Microsoft Graph Planner functions");

    // graph_planner_plans(group_id) - optional secret named param
    TableFunction planner_plans("graph_planner_plans", {LogicalType::VARCHAR},
                                PlansScan, PlansBind);
    planner_plans.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(planner_plans);

    // graph_planner_buckets(plan_id) - optional secret named param
    TableFunction planner_buckets("graph_planner_buckets", {LogicalType::VARCHAR},
                                  BucketsScan, BucketsBind);
    planner_buckets.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(planner_buckets);

    // graph_planner_tasks(plan_id) - optional secret named param
    TableFunction planner_tasks("graph_planner_tasks", {LogicalType::VARCHAR},
                                TasksScan, TasksBind);
    planner_tasks.named_parameters["secret"] = LogicalType::VARCHAR;
    loader.RegisterFunction(planner_tasks);

    ERPL_TRACE_INFO("GRAPH_PLANNER", "Successfully registered Microsoft Graph Planner functions");
}

} // namespace erpl_web
