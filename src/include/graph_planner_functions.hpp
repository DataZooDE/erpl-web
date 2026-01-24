#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace erpl_web {

// Microsoft Graph Planner table functions
class GraphPlannerFunctions {
public:
    static void Register(duckdb::ExtensionLoader &loader);

private:
    // graph_planner_plans(secret_name, group_id) - List plans for a group
    static duckdb::unique_ptr<duckdb::FunctionData> PlansBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void PlansScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_planner_buckets(secret_name, plan_id) - List buckets in a plan
    static duckdb::unique_ptr<duckdb::FunctionData> BucketsBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void BucketsScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_planner_tasks(secret_name, plan_id) - List tasks in a plan
    static duckdb::unique_ptr<duckdb::FunctionData> TasksBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void TasksScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);
};

} // namespace erpl_web
