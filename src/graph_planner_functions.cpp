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

    ERPL_TRACE_INFO("GRAPH_PLANNER", "Successfully registered Microsoft Graph Planner functions");
}

} // namespace erpl_web
