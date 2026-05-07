#include "graph_planner_functions.hpp"
#include "graph_client.hpp"
#include "graph_planner_client.hpp"
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

struct PlansBindData : public TableFunctionData {
    std::string secret_name;
    std::string group_id;
    std::string json_response;
    yyjson_doc *parsed_doc = nullptr;
    yyjson_arr_iter item_iter = {};
    bool done = false;

    ~PlansBindData() override {
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

struct BucketsBindData : public TableFunctionData {
    std::string secret_name;
    std::string plan_id;
    std::string json_response;
    yyjson_doc *parsed_doc = nullptr;
    yyjson_arr_iter item_iter = {};
    bool done = false;

    ~BucketsBindData() override {
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

struct TasksBindData : public TableFunctionData {
    std::string secret_name;
    std::string plan_id;
    std::string json_response;
    yyjson_doc *parsed_doc = nullptr;
    yyjson_arr_iter item_iter = {};
    bool done = false;

    ~TasksBindData() override {
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

    if (!bind_data.parsed_doc && bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphPlannerClient client(auth_info.auth_params);
        bind_data.json_response = client.GetGroupPlans(bind_data.group_id);
    }

    if (!bind_data.parsed_doc) {
        if (!bind_data.InitIterator()) {
            bind_data.done = true;
            output.SetCardinality(0);
            return;
        }
    }

    idx_t row = 0;
    yyjson_val *item;
    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bind_data.item_iter))) {
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "id"));
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "title"));
        SetStrCell(output.data[2], row, yyjson_obj_get(item, "owner"));
        SetStrCell(output.data[3], row, yyjson_obj_get(item, "createdDateTime"));
        row++;
    }

    if (row < STANDARD_VECTOR_SIZE) {
        bind_data.done = true;
    }
    output.SetCardinality(row);
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

    if (!bind_data.parsed_doc && bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphPlannerClient client(auth_info.auth_params);
        bind_data.json_response = client.GetPlanBuckets(bind_data.plan_id);
    }

    if (!bind_data.parsed_doc) {
        if (!bind_data.InitIterator()) {
            bind_data.done = true;
            output.SetCardinality(0);
            return;
        }
    }

    idx_t row = 0;
    yyjson_val *item;
    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bind_data.item_iter))) {
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "id"));
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "name"));
        SetStrCell(output.data[2], row, yyjson_obj_get(item, "planId"));
        SetStrCell(output.data[3], row, yyjson_obj_get(item, "orderHint"));
        row++;
    }

    if (row < STANDARD_VECTOR_SIZE) {
        bind_data.done = true;
    }
    output.SetCardinality(row);
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

    if (!bind_data.parsed_doc && bind_data.json_response.empty()) {
        auto auth_info = ResolveGraphAuth(context, bind_data.secret_name);
        GraphPlannerClient client(auth_info.auth_params);
        bind_data.json_response = client.GetPlanTasks(bind_data.plan_id);
    }

    if (!bind_data.parsed_doc) {
        if (!bind_data.InitIterator()) {
            bind_data.done = true;
            output.SetCardinality(0);
            return;
        }
    }

    idx_t row = 0;
    yyjson_val *item;
    while (row < STANDARD_VECTOR_SIZE && (item = yyjson_arr_iter_next(&bind_data.item_iter))) {
        SetStrCell(output.data[0], row, yyjson_obj_get(item, "id"));
        SetStrCell(output.data[1], row, yyjson_obj_get(item, "title"));
        SetStrCell(output.data[2], row, yyjson_obj_get(item, "bucketId"));
        SetStrCell(output.data[3], row, yyjson_obj_get(item, "planId"));
        SetInt32Cell(output.data[4], row, yyjson_obj_get(item, "percentComplete"));
        SetInt32Cell(output.data[5], row, yyjson_obj_get(item, "priority"));
        SetStrCell(output.data[6], row, yyjson_obj_get(item, "dueDateTime"));
        SetStrCell(output.data[7], row, yyjson_obj_get(item, "createdDateTime"));
        SetStrCell(output.data[8], row, yyjson_obj_get(item, "completedDateTime"));
        row++;
    }

    if (row < STANDARD_VECTOR_SIZE) {
        bind_data.done = true;
    }
    output.SetCardinality(row);
}

// ============================================================================
// graph_planner_create_task — create a task, suitable for lateral joins
// ============================================================================

struct CreateTaskBindData : public TableFunctionData {
    std::string task_id;
    std::string task_url;
    bool done = false;
};

unique_ptr<FunctionData> GraphPlannerFunctions::CreateTaskBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<std::string> &names) {

    if (input.inputs.size() < 2) {
        throw BinderException("graph_planner_create_task requires plan_id and title parameters");
    }

    const std::string plan_id = input.inputs[0].GetValue<std::string>();
    const std::string title   = input.inputs[1].GetValue<std::string>();

    std::string secret_name;
    if (input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }

    PlannerTaskCreateParams params;
    if (input.named_parameters.find("bucket_id") != input.named_parameters.end()) {
        params.bucket_id = input.named_parameters.at("bucket_id").GetValue<std::string>();
    }
    if (input.named_parameters.find("due_date") != input.named_parameters.end()) {
        params.due_date = input.named_parameters.at("due_date").GetValue<std::string>();
    }
    if (input.named_parameters.find("start_date") != input.named_parameters.end()) {
        params.start_date = input.named_parameters.at("start_date").GetValue<std::string>();
    }
    if (input.named_parameters.find("assigned_to") != input.named_parameters.end()) {
        params.assigned_to = input.named_parameters.at("assigned_to").GetValue<std::string>();
    }
    if (input.named_parameters.find("description") != input.named_parameters.end()) {
        params.description = input.named_parameters.at("description").GetValue<std::string>();
    }
    if (input.named_parameters.find("priority") != input.named_parameters.end()) {
        params.priority = input.named_parameters.at("priority").GetValue<int32_t>();
    }
    if (input.named_parameters.find("percent_complete") != input.named_parameters.end()) {
        params.percent_complete = input.named_parameters.at("percent_complete").GetValue<int32_t>();
    }

    auto auth_info = ResolveGraphAuth(context, secret_name);
    GraphPlannerClient client(auth_info.auth_params);

    // Resolve bucket name → GUID before creating the task (no-op if already a GUID)
    if (!params.bucket_id.empty()) {
        params.bucket_id = client.ResolveBucketId(plan_id, params.bucket_id);
    }

    const std::string task_id = client.CreateTask(plan_id, title, params);

    auto bind_data = make_uniq<CreateTaskBindData>();
    bind_data->task_id  = task_id;
    bind_data->task_url = "https://planner.cloud.microsoft/webui/open/task/" + task_id;

    names        = {"task_id", "task_url"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};

    return std::move(bind_data);
}

void GraphPlannerFunctions::CreateTaskScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->CastNoConst<CreateTaskBindData>();
    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }
    bind_data.done = true;
    output.SetValue(0, 0, Value(bind_data.task_id));
    output.SetValue(1, 0, Value(bind_data.task_url));
    output.SetCardinality(1);
}

// ============================================================================
// Registration
// ============================================================================

void GraphPlannerFunctions::Register(ExtensionLoader &loader) {
    ERPL_TRACE_INFO("GRAPH_PLANNER", "Registering Microsoft Graph Planner functions");

    {
        TableFunction planner_plans("graph_planner_plans", {LogicalType::VARCHAR},
                                    PlansScan, PlansBind);
        planner_plans.named_parameters["secret"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(planner_plans);
        FunctionDescription desc;
        desc.description = "List all Microsoft Planner plans in a Microsoft 365 group.";
        desc.parameter_names = {"group_id", "secret"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM graph_planner_plans('group-id-here', secret := 'ms_graph')"};
        desc.categories = {"microsoft", "graph", "planner"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction planner_buckets("graph_planner_buckets", {LogicalType::VARCHAR},
                                      BucketsScan, BucketsBind);
        planner_buckets.named_parameters["secret"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(planner_buckets);
        FunctionDescription desc;
        desc.description = "List all buckets in a Microsoft Planner plan.";
        desc.parameter_names = {"plan_id", "secret"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM graph_planner_buckets('plan-id-here', secret := 'ms_graph')"};
        desc.categories = {"microsoft", "graph", "planner"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }
    {
        TableFunction planner_tasks("graph_planner_tasks", {LogicalType::VARCHAR},
                                    TasksScan, TasksBind);
        planner_tasks.named_parameters["secret"] = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(planner_tasks);
        FunctionDescription desc;
        desc.description = "List all tasks in a Microsoft Planner plan.";
        desc.parameter_names = {"plan_id", "secret"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM graph_planner_tasks('plan-id-here', secret := 'ms_graph')"};
        desc.categories = {"microsoft", "graph", "planner"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }

    {
        TableFunction create_task("graph_planner_create_task",
                                   {LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   CreateTaskScan, CreateTaskBind);
        create_task.named_parameters["bucket_id"]        = LogicalType::VARCHAR;
        create_task.named_parameters["due_date"]         = LogicalType::VARCHAR;
        create_task.named_parameters["start_date"]       = LogicalType::VARCHAR;
        create_task.named_parameters["assigned_to"]      = LogicalType::VARCHAR;
        create_task.named_parameters["description"]      = LogicalType::VARCHAR;
        create_task.named_parameters["priority"]         = LogicalType::INTEGER;
        create_task.named_parameters["percent_complete"] = LogicalType::INTEGER;
        create_task.named_parameters["secret"]           = LogicalType::VARCHAR;
        CreateTableFunctionInfo info(create_task);
        FunctionDescription desc;
        desc.description =
            "Create a Microsoft Planner task. Returns (task_id, task_url). "
            "Designed for use with lateral joins to bulk-create tasks from a query result. "
            "priority: 0-10 (1=urgent, 3=important, 5=medium, 9=low). "
            "assigned_to: single Graph user ID. "
            "due_date/start_date: ISO 8601 date or datetime string.";
        desc.parameter_names = {"plan_id", "title", "bucket_id", "due_date", "start_date",
                                "assigned_to", "description", "priority", "percent_complete", "secret"};
        desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR,
                                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                LogicalType::VARCHAR, LogicalType::VARCHAR,
                                LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::VARCHAR};
        desc.examples = {
            "-- Create a single task\n"
            "SELECT * FROM graph_planner_create_task('plan-id', 'My Task',\n"
            "    bucket_id := 'bucket-id', due_date := '2024-06-30',\n"
            "    assigned_to := 'user-guid', description := 'Details here',\n"
            "    priority := 5, secret := 'ms_graph')",
            "-- Bulk-create from a table using a lateral join\n"
            "SELECT t.title, pt.task_url\n"
            "FROM todos t,\n"
            "     graph_planner_create_task('plan-id', t.title,\n"
            "         due_date := t.due_date::VARCHAR,\n"
            "         description := t.notes,\n"
            "         secret := 'ms_graph') pt"
        };
        desc.categories = {"microsoft", "graph", "planner"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(std::move(info));
    }

    ERPL_TRACE_INFO("GRAPH_PLANNER", "Successfully registered Microsoft Graph Planner functions");
}

} // namespace erpl_web
