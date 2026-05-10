#include "graph_planner_client.hpp"
#include "graph_client.hpp"
#include "tracing.hpp"
#include <map>
#include <stdexcept>

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

std::string GraphPlannerUrlBuilder::BuildTasksUrl() {
    return GetBaseUrl() + "/planner/tasks";
}

std::string GraphPlannerUrlBuilder::BuildBucketsUrl() {
    return GetBaseUrl() + "/planner/buckets";
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

static bool LooksLikePlannerGuid(const std::string &s) {
    return GraphClient::LooksLikeGuid(s);
}

std::string GraphPlannerClient::ResolveBucketId(const std::string &plan_id, const std::string &name_or_id) {
    if (LooksLikePlannerGuid(name_or_id)) {
        return name_or_id;
    }

    ERPL_TRACE_DEBUG("GRAPH_PLANNER", "Resolving bucket name '" + name_or_id + "' in plan " + plan_id);
    const std::string buckets_json = GetPlanBuckets(plan_id);

    auto resolved = GraphJsonFindStringInArray(buckets_json, "value", "name", name_or_id, "id",
                                               "buckets response when resolving bucket '" + name_or_id + "'");
    if (!resolved) {
        throw std::runtime_error(
            "No Planner bucket found with name '" + name_or_id + "' in plan '" + plan_id + "'");
    }

    ERPL_TRACE_DEBUG("GRAPH_PLANNER", "Resolved bucket '" + name_or_id + "' → " + *resolved);
    return *resolved;
}

// Normalise a user-supplied date string to the UTC datetime format Planner expects.
// "2024-06-15" → "2024-06-15T00:00:00Z"
// "2024-06-15T12:00:00Z" → unchanged
static std::string NormalisePlannerDate(const std::string &s) {
    if (s.size() == 10 && s[4] == '-' && s[7] == '-') {
        return s + "T00:00:00Z";
    }
    return s;
}

// Build a JSON string literal with basic escaping.
static std::string JsonString(const std::string &s) {
    std::string out;
    out.reserve(s.size() + 2);
    out += '"';
    for (char c : s) {
        if (c == '"')  { out += "\\\""; }
        else if (c == '\\') { out += "\\\\"; }
        else if (c == '\n')  { out += "\\n"; }
        else if (c == '\r')  { out += "\\r"; }
        else if (c == '\t')  { out += "\\t"; }
        else { out += c; }
    }
    out += '"';
    return out;
}

std::string GraphPlannerClient::CreateTask(const std::string &plan_id,
                                            const std::string &title,
                                            const PlannerTaskCreateParams &params)
{
    GraphClient graph_client(auth_params, "GRAPH_PLANNER");

    // Build the POST body for /planner/tasks
    std::string body = "{\"planId\":" + JsonString(plan_id) +
                       ",\"title\":"  + JsonString(title);

    if (!params.bucket_id.empty()) {
        body += ",\"bucketId\":" + JsonString(params.bucket_id);
    }
    if (!params.due_date.empty()) {
        body += ",\"dueDateTime\":" + JsonString(NormalisePlannerDate(params.due_date));
    }
    if (!params.start_date.empty()) {
        body += ",\"startDateTime\":" + JsonString(NormalisePlannerDate(params.start_date));
    }
    if (params.priority >= 0 && params.priority <= 10) {
        body += ",\"priority\":" + std::to_string(params.priority);
    }
    if (params.percent_complete >= 0 && params.percent_complete <= 100) {
        body += ",\"percentComplete\":" + std::to_string(params.percent_complete);
    }
    if (!params.assigned_to.empty()) {
        // assignments is a map: userId → { "@odata.type": "...", "orderHint": " !" }
        body += ",\"assignments\":{" + JsonString(params.assigned_to) +
                ":{\"@odata.type\":\"#microsoft.graph.plannerAssignment\",\"orderHint\":\" !\"}}";
    }
    body += "}";

    ERPL_TRACE_DEBUG("GRAPH_PLANNER", "Creating task in plan " + plan_id + ": " + title);
    const std::string task_json = graph_client.Post(GraphPlannerUrlBuilder::BuildTasksUrl(), body);

    // Extract task ID from response
    auto task_id = GraphJsonGetRootString(task_json, "id", "Planner task creation response");

    if (!task_id) {
        throw std::runtime_error("Planner task creation succeeded but returned no task id");
    }

    // If a description was requested, PATCH /planner/tasks/{id}/details.
    // Planner requires If-Match for optimistic concurrency; "If-Match: *" forces the update.
    if (!params.description.empty()) {
        const std::string details_url = GraphPlannerUrlBuilder::BuildTaskDetailsUrl(*task_id);
        const std::string details_body = "{\"description\":" + JsonString(params.description) + "}";
        graph_client.PatchWithHeaders(details_url, details_body, {{"If-Match", "*"}});
        ERPL_TRACE_DEBUG("GRAPH_PLANNER", "Patched description for task " + *task_id);
    }

    return *task_id;
}

} // namespace erpl_web
