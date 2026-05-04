#include "graph_planner_client.hpp"
#include "graph_client.hpp"
#include "tracing.hpp"

namespace erpl_web {

// URL Builder implementation
std::string GraphPlannerUrlBuilder::GetBaseUrl() {
    return GraphClient::BaseUrl();
}

std::string GraphPlannerUrlBuilder::BuildGroupPlansUrl(const std::string &group_id) {
    return GetBaseUrl() + "/groups/" + group_id + "/planner/plans";
}

std::string GraphPlannerUrlBuilder::BuildPlanUrl(const std::string &plan_id) {
    return GetBaseUrl() + "/planner/plans/" + plan_id;
}

std::string GraphPlannerUrlBuilder::BuildPlanBucketsUrl(const std::string &plan_id) {
    return GetBaseUrl() + "/planner/plans/" + plan_id + "/buckets";
}

std::string GraphPlannerUrlBuilder::BuildPlanTasksUrl(const std::string &plan_id) {
    return GetBaseUrl() + "/planner/plans/" + plan_id + "/tasks";
}

std::string GraphPlannerUrlBuilder::BuildTaskDetailsUrl(const std::string &task_id) {
    return GetBaseUrl() + "/planner/tasks/" + task_id + "/details";
}

std::string GraphPlannerUrlBuilder::BuildTaskUrl(const std::string &task_id) {
    return GetBaseUrl() + "/planner/tasks/" + task_id;
}

std::string GraphPlannerUrlBuilder::BuildBucketUrl(const std::string &bucket_id) {
    return GetBaseUrl() + "/planner/buckets/" + bucket_id;
}

std::string GraphPlannerUrlBuilder::BuildBucketTasksUrl(const std::string &bucket_id) {
    return GetBaseUrl() + "/planner/buckets/" + bucket_id + "/tasks";
}

std::string GraphPlannerUrlBuilder::BuildMyTasksUrl() {
    return GetBaseUrl() + "/me/planner/tasks";
}

std::string GraphPlannerUrlBuilder::BuildMyPlansUrl() {
    // Note: This endpoint might require beta API for some features
    return GetBaseUrl() + "/me/planner/plans";
}

// GraphPlannerClient implementation
GraphPlannerClient::GraphPlannerClient(std::shared_ptr<HttpAuthParams> auth_params)
    : auth_params(auth_params) {
}

std::string GraphPlannerClient::DoGraphGet(const std::string &url) {
    return GraphClient(auth_params, "GRAPH_PLANNER").Get(url);
}

std::string GraphPlannerClient::GetGroupPlans(const std::string &group_id) {
    auto url = GraphPlannerUrlBuilder::BuildGroupPlansUrl(group_id);
    return GraphClient(auth_params, "GRAPH_PLANNER").GetAllPagesMerged(url);
}

std::string GraphPlannerClient::GetPlan(const std::string &plan_id) {
    auto url = GraphPlannerUrlBuilder::BuildPlanUrl(plan_id);
    return DoGraphGet(url);
}

std::string GraphPlannerClient::GetPlanBuckets(const std::string &plan_id) {
    auto url = GraphPlannerUrlBuilder::BuildPlanBucketsUrl(plan_id);
    return GraphClient(auth_params, "GRAPH_PLANNER").GetAllPagesMerged(url);
}

std::string GraphPlannerClient::GetBucket(const std::string &bucket_id) {
    auto url = GraphPlannerUrlBuilder::BuildBucketUrl(bucket_id);
    return DoGraphGet(url);
}

std::string GraphPlannerClient::GetPlanTasks(const std::string &plan_id) {
    auto url = GraphPlannerUrlBuilder::BuildPlanTasksUrl(plan_id);
    return GraphClient(auth_params, "GRAPH_PLANNER").GetAllPagesMerged(url);
}

std::string GraphPlannerClient::GetBucketTasks(const std::string &bucket_id) {
    auto url = GraphPlannerUrlBuilder::BuildBucketTasksUrl(bucket_id);
    return GraphClient(auth_params, "GRAPH_PLANNER").GetAllPagesMerged(url);
}

std::string GraphPlannerClient::GetTask(const std::string &task_id) {
    auto url = GraphPlannerUrlBuilder::BuildTaskUrl(task_id);
    return DoGraphGet(url);
}

std::string GraphPlannerClient::GetTaskDetails(const std::string &task_id) {
    auto url = GraphPlannerUrlBuilder::BuildTaskDetailsUrl(task_id);
    return DoGraphGet(url);
}

std::string GraphPlannerClient::GetMyTasks() {
    auto url = GraphPlannerUrlBuilder::BuildMyTasksUrl();
    return GraphClient(auth_params, "GRAPH_PLANNER").GetAllPagesMerged(url);
}

} // namespace erpl_web
