#pragma once

#include "http_client.hpp"
#include <string>
#include <memory>

namespace erpl_web {

// URL builder for Microsoft Graph Planner API endpoints
class GraphPlannerUrlBuilder {
public:
    // Base Graph API URL
    static std::string GetBaseUrl();

    // Build URL for listing plans for a group
    // /groups/{group-id}/planner/plans
    static std::string BuildGroupPlansUrl(const std::string &group_id);

    // Build URL for getting a specific plan
    // /planner/plans/{plan-id}
    static std::string BuildPlanUrl(const std::string &plan_id);

    // Build URL for listing buckets in a plan
    // /planner/plans/{plan-id}/buckets
    static std::string BuildPlanBucketsUrl(const std::string &plan_id);

    // Build URL for listing tasks in a plan
    // /planner/plans/{plan-id}/tasks
    static std::string BuildPlanTasksUrl(const std::string &plan_id);

    // Build URL for getting task details
    // /planner/tasks/{task-id}/details
    static std::string BuildTaskDetailsUrl(const std::string &task_id);

    // Build URL for getting a specific task
    // /planner/tasks/{task-id}
    static std::string BuildTaskUrl(const std::string &task_id);

    // Build URL for getting a specific bucket
    // /planner/buckets/{bucket-id}
    static std::string BuildBucketUrl(const std::string &bucket_id);

    // Build URL for listing tasks in a bucket
    // /planner/buckets/{bucket-id}/tasks
    static std::string BuildBucketTasksUrl(const std::string &bucket_id);

    // Build URL for listing my tasks
    // /me/planner/tasks
    static std::string BuildMyTasksUrl();

    // Build URL for listing plans I'm a member of
    // /me/planner/plans (requires beta API)
    static std::string BuildMyPlansUrl();
};

// Client for Microsoft Graph Planner API operations
class GraphPlannerClient {
public:
    GraphPlannerClient(std::shared_ptr<HttpAuthParams> auth_params);

    // Plan operations
    std::string GetGroupPlans(const std::string &group_id);
    std::string GetPlan(const std::string &plan_id);

    // Bucket operations
    std::string GetPlanBuckets(const std::string &plan_id);
    std::string GetBucket(const std::string &bucket_id);

    // Task operations
    std::string GetPlanTasks(const std::string &plan_id);
    std::string GetBucketTasks(const std::string &bucket_id);
    std::string GetTask(const std::string &task_id);
    std::string GetTaskDetails(const std::string &task_id);
    std::string GetMyTasks();

private:
    std::shared_ptr<HttpAuthParams> auth_params;
    std::shared_ptr<HttpClient> http_client;

    std::string DoGraphGet(const std::string &url);
};

} // namespace erpl_web
