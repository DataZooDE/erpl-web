#include "sac_catalog.hpp"
#include "sac_client.hpp"
#include "sac_url_builder.hpp"
#include "http_client.hpp"
#include "odata_content.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/secret/secret.hpp"
#include <algorithm>
#include <optional>

namespace erpl_web {

// Helper function to resolve SAC auth from DuckDB secret
struct SacSecretParams {
    std::string tenant;
    std::string region;
    std::shared_ptr<HttpAuthParams> auth_params;
};

SacSecretParams ResolveSacSecret(duckdb::ClientContext &context, const std::string& secret_name) {
    auto &secret_manager = duckdb::SecretManager::Get(context);
    auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
    std::unique_ptr<duckdb::SecretEntry> secret_entry;

    try {
        secret_entry = secret_manager.GetSecretByName(transaction, secret_name);
    } catch (...) {
        secret_entry = nullptr;
    }

    if (!secret_entry) {
        throw duckdb::InvalidInputException("Secret '" + secret_name + "' not found. Please create it using CREATE SECRET " +
            secret_name + " (type 'sac', provider 'oauth2', tenant_name => '...', region => '...', " +
            "client_id => '...', client_secret => '...', scope => 'openid');");
    }

    auto kv_secret = dynamic_cast<const duckdb::KeyValueSecret*>(secret_entry->secret.get());
    if (!kv_secret) {
        throw duckdb::InvalidInputException("Secret '" + secret_name + "' is not a KeyValueSecret");
    }

    SacSecretParams params;

    // Extract tenant and region
    auto tenant_it = kv_secret->secret_map.find("tenant_name");
    auto region_it = kv_secret->secret_map.find("region");
    auto access_token_it = kv_secret->secret_map.find("access_token");

    if (tenant_it == kv_secret->secret_map.end() || region_it == kv_secret->secret_map.end()) {
        throw duckdb::InvalidInputException("SAC secret must contain 'tenant_name' and 'region' fields");
    }

    params.tenant = tenant_it->second.ToString();
    params.region = region_it->second.ToString();

    // Create auth params
    params.auth_params = std::make_shared<HttpAuthParams>();
    if (access_token_it != kv_secret->secret_map.end()) {
        // Token will be used if available
        // Note: Full OAuth2 implementation would handle token refreshing here
    }

    return params;
}

// SacCatalogService implementation

SacCatalogService::SacCatalogService(
    const std::string& tenant,
    const std::string& region,
    std::shared_ptr<HttpAuthParams> auth_params)
    : tenant_(tenant), region_(region), auth_params_(auth_params) {

    // Create OData service client for metadata queries
    auto base_url = SacUrlBuilder::BuildODataServiceRootUrl(tenant, region);
    HttpUrl url(base_url);
    auto http_client = std::make_shared<HttpClient>();
    catalog_client_ = std::make_shared<ODataServiceClient>(http_client, url, auth_params);
    catalog_client_->SetODataVersionDirectly(ODataVersion::V4);
}

SacCatalogService::~SacCatalogService() = default;

std::vector<SacModel> SacCatalogService::ListModels() {
    std::vector<SacModel> models;

    try {
        // Query planning models via OData
        // The OData client will handle the actual HTTP requests and response parsing
        // This is a stub implementation - full implementation would parse OData responses
        ERPL_TRACE_INFO("SAC_CATALOG", "Listing models for tenant " + tenant_ + " in region " + region_);
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("SAC_CATALOG", "Failed to list planning models: " + std::string(e.what()));
    }

    return models;
}

std::optional<SacModel> SacCatalogService::GetModel(const std::string& model_id) {
    try {
        // Query specific planning model via OData
        auto url_str = SacUrlBuilder::BuildPlanningDataUrl(tenant_, region_, model_id);
        ERPL_TRACE_INFO("SAC_CATALOG", "Getting model " + model_id + " from " + url_str);
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("SAC_CATALOG", "Failed to get model " + model_id + ": " + std::string(e.what()));
    }

    return std::nullopt;
}

std::vector<std::string> SacCatalogService::GetModelDimensions(const std::string& model_id) {
    std::vector<std::string> dimensions;

    try {
        auto model = GetModel(model_id);
        if (model.has_value()) {
            dimensions = model->dimensions;
        }
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("SAC_CATALOG", "Failed to get model dimensions: " + std::string(e.what()));
    }

    return dimensions;
}

std::vector<std::string> SacCatalogService::GetModelMeasures(const std::string& model_id) {
    std::vector<std::string> measures;

    try {
        auto model = GetModel(model_id);
        if (model.has_value()) {
            measures = model->measures;
        }
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("SAC_CATALOG", "Failed to get model measures: " + std::string(e.what()));
    }

    return measures;
}

std::vector<SacStory> SacCatalogService::ListStories() {
    std::vector<SacStory> stories;

    try {
        // Query stories via OData story service
        auto stories_url = SacUrlBuilder::BuildStoryServiceUrl(tenant_, region_);
        ERPL_TRACE_INFO("SAC_CATALOG", "Listing stories from " + stories_url);
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("SAC_CATALOG", "Failed to list stories: " + std::string(e.what()));
    }

    return stories;
}

std::optional<SacStory> SacCatalogService::GetStory(const std::string& story_id) {
    try {
        auto stories = ListStories();
        auto it = std::find_if(stories.begin(), stories.end(),
            [&story_id](const SacStory& s) { return s.id == story_id; });
        if (it != stories.end()) {
            return *it;
        }
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("SAC_CATALOG", "Failed to get story " + story_id + ": " + std::string(e.what()));
    }

    return std::nullopt;
}

std::vector<SacStory> SacCatalogService::ListStoriesByOwner(const std::string& owner) {
    std::vector<SacStory> result;

    try {
        auto stories = ListStories();
        for (const auto& story : stories) {
            if (story.owner == owner) {
                result.push_back(story);
            }
        }
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("SAC_CATALOG", "Failed to list stories by owner: " + std::string(e.what()));
    }

    return result;
}

// Helper methods for parsing OData responses

std::vector<SacModel> SacCatalogService::ParseModelsResponse(const std::string& odata_response) {
    std::vector<SacModel> models;

    // Simple OData response parsing - extract entities from response
    // In a production system, this would use a proper JSON parser (yyjson, nlohmann/json, etc.)
    // For now, using basic string parsing compatible with OData v4 responses

    size_t value_pos = odata_response.find("\"value\":");
    if (value_pos == std::string::npos) {
        return models;
    }

    // Note: Proper implementation would parse the complete OData response structure
    // This is a simplified version that demonstrates the pattern
    // Real implementation should use OData response deserialization

    return models;
}

std::vector<SacStory> SacCatalogService::ParseStoriesResponse(const std::string& odata_response) {
    std::vector<SacStory> stories;

    // Similar to ParseModelsResponse, this would parse the OData response
    // Real implementation should use OData response deserialization

    return stories;
}

SacModel SacCatalogService::ParseModelEntity(const std::string& entity_json) {
    SacModel model;

    // Parse JSON entity
    // In production, use a proper JSON parser
    // This demonstrates the pattern structure

    return model;
}

SacStory SacCatalogService::ParseStoryEntity(const std::string& entity_json) {
    SacStory story;

    // Parse JSON entity

    return story;
}

// ===== DuckDB Table Functions =====

// Bind data structure for sac_list_models
struct SacListModelsBindData : public duckdb::TableFunctionData {
public:
    std::vector<SacModel> models;
    size_t current_index = 0;
    bool finished = false;

    SacListModelsBindData() = default;
};

// Scan function for sac_list_models
static void SacListModelsScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                              duckdb::DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<SacListModelsBindData>();

    idx_t count = 0;
    while (bind_data.current_index < bind_data.models.size() && count < output.GetCapacity()) {
        const auto &model = bind_data.models[bind_data.current_index];

        output.SetValue(0, count, duckdb::Value(model.id));                    // id
        output.SetValue(1, count, duckdb::Value(model.name));                  // name
        output.SetValue(2, count, duckdb::Value(model.description));           // description
        output.SetValue(3, count, duckdb::Value(model.type));                  // type
        output.SetValue(4, count, duckdb::Value(model.owner));                 // owner
        output.SetValue(5, count, duckdb::Value(model.created_at));            // created_at
        output.SetValue(6, count, duckdb::Value(model.last_modified_at));      // last_modified_at

        bind_data.current_index++;
        count++;
    }

    bind_data.finished = (bind_data.current_index >= bind_data.models.size());
    output.SetCardinality(count);
}

// Bind function for sac_list_models
static duckdb::unique_ptr<duckdb::FunctionData> SacListModelsBind(
    duckdb::ClientContext &context,
    duckdb::TableFunctionBindInput &input,
    duckdb::vector<duckdb::LogicalType> &return_types,
    duckdb::vector<std::string> &names) {

    // Get secret name (default: "sac")
    std::string secret_name = "sac";
    if (!input.named_parameters.empty() && input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }

    // Resolve SAC credentials from secret
    auto secret_params = ResolveSacSecret(context, secret_name);

    // Create catalog service
    auto catalog = std::make_shared<SacCatalogService>(
        secret_params.tenant,
        secret_params.region,
        secret_params.auth_params
    );

    // List models
    auto models = catalog->ListModels();

    // Set return types
    names = {"id", "name", "description", "type", "owner", "created_at", "last_modified_at"};
    return_types = {
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // id
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // name
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // description
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // type
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // owner
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // created_at
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)   // last_modified_at
    };

    // Create bind data
    auto bind = duckdb::make_uniq<SacListModelsBindData>();
    bind->models = models;
    bind->current_index = 0;
    bind->finished = models.empty();

    return std::move(bind);
}

duckdb::TableFunction CreateSacListModelsFunction() {
    duckdb::TableFunction func(
        {},  // No parameters
        SacListModelsScan,
        SacListModelsBind
    );

    // Add optional named parameter for secret name
    func.named_parameters["secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);

    return func;
}

// ===== sac_list_stories =====

struct SacListStoriesBindData : public duckdb::TableFunctionData {
public:
    std::vector<SacStory> stories;
    size_t current_index = 0;
    bool finished = false;

    SacListStoriesBindData() = default;
};

static void SacListStoriesScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                               duckdb::DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<SacListStoriesBindData>();

    idx_t count = 0;
    while (bind_data.current_index < bind_data.stories.size() && count < output.GetCapacity()) {
        const auto &story = bind_data.stories[bind_data.current_index];

        output.SetValue(0, count, duckdb::Value(story.id));                    // id
        output.SetValue(1, count, duckdb::Value(story.name));                  // name
        output.SetValue(2, count, duckdb::Value(story.description));           // description
        output.SetValue(3, count, duckdb::Value(story.owner));                 // owner
        output.SetValue(4, count, duckdb::Value(story.created_at));            // created_at
        output.SetValue(5, count, duckdb::Value(story.last_modified_at));      // last_modified_at
        output.SetValue(6, count, duckdb::Value(story.status));                // status

        bind_data.current_index++;
        count++;
    }

    bind_data.finished = (bind_data.current_index >= bind_data.stories.size());
    output.SetCardinality(count);
}

static duckdb::unique_ptr<duckdb::FunctionData> SacListStoriesBind(
    duckdb::ClientContext &context,
    duckdb::TableFunctionBindInput &input,
    duckdb::vector<duckdb::LogicalType> &return_types,
    duckdb::vector<std::string> &names) {

    std::string secret_name = "sac";
    if (!input.named_parameters.empty() && input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }

    auto secret_params = ResolveSacSecret(context, secret_name);

    auto catalog = std::make_shared<SacCatalogService>(
        secret_params.tenant,
        secret_params.region,
        secret_params.auth_params
    );

    auto stories = catalog->ListStories();

    names = {"id", "name", "description", "owner", "created_at", "last_modified_at", "status"};
    return_types = {
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // id
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // name
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // description
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // owner
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // created_at
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // last_modified_at
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)   // status
    };

    auto bind = duckdb::make_uniq<SacListStoriesBindData>();
    bind->stories = stories;
    bind->current_index = 0;
    bind->finished = stories.empty();

    return std::move(bind);
}

duckdb::TableFunction CreateSacListStoriesFunction() {
    duckdb::TableFunction func(
        {},  // No parameters
        SacListStoriesScan,
        SacListStoriesBind
    );

    func.named_parameters["secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);

    return func;
}

// ===== sac_get_model_info =====

struct SacGetModelInfoBindData : public duckdb::TableFunctionData {
public:
    SacModel model;
    std::vector<std::string> dimensions;
    size_t current_index = 0;
    bool finished = false;
    bool model_found = false;

    SacGetModelInfoBindData() = default;
};

static void SacGetModelInfoScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                                duckdb::DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<SacGetModelInfoBindData>();

    if (!bind_data.model_found) {
        output.SetCardinality(0);
        bind_data.finished = true;
        return;
    }

    idx_t count = 0;

    // Output one row for the model with dimension list
    if (bind_data.current_index == 0) {
        // Build dimension list as comma-separated string
        std::string dims_str;
        for (size_t i = 0; i < bind_data.dimensions.size(); ++i) {
            if (i > 0) dims_str += ", ";
            dims_str += bind_data.dimensions[i];
        }

        output.SetValue(0, 0, duckdb::Value(bind_data.model.id));
        output.SetValue(1, 0, duckdb::Value(bind_data.model.name));
        output.SetValue(2, 0, duckdb::Value(bind_data.model.description));
        output.SetValue(3, 0, duckdb::Value(bind_data.model.type));
        output.SetValue(4, 0, duckdb::Value(dims_str));
        output.SetValue(5, 0, duckdb::Value(bind_data.model.created_at));

        bind_data.current_index++;
        count = 1;
    }

    bind_data.finished = true;
    output.SetCardinality(count);
}

static duckdb::unique_ptr<duckdb::FunctionData> SacGetModelInfoBind(
    duckdb::ClientContext &context,
    duckdb::TableFunctionBindInput &input,
    duckdb::vector<duckdb::LogicalType> &return_types,
    duckdb::vector<std::string> &names) {

    if (input.inputs.empty()) {
        throw duckdb::InvalidInputException("sac_get_model_info requires a model_id parameter");
    }

    auto model_id = input.inputs[0].GetValue<std::string>();

    std::string secret_name = "sac";
    if (!input.named_parameters.empty() && input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }

    auto secret_params = ResolveSacSecret(context, secret_name);

    auto catalog = std::make_shared<SacCatalogService>(
        secret_params.tenant,
        secret_params.region,
        secret_params.auth_params
    );

    names = {"id", "name", "description", "type", "dimensions", "created_at"};
    return_types = {
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // id
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // name
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // description
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // type
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // dimensions
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)   // created_at
    };

    auto model_opt = catalog->GetModel(model_id);

    auto bind = duckdb::make_uniq<SacGetModelInfoBindData>();
    if (model_opt.has_value()) {
        bind->model = model_opt.value();
        bind->dimensions = catalog->GetModelDimensions(model_id);
        bind->model_found = true;
    } else {
        bind->model_found = false;
    }
    bind->current_index = 0;
    bind->finished = false;

    return std::move(bind);
}

duckdb::TableFunction CreateSacGetModelInfoFunction() {
    duckdb::TableFunction func(
        {duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)},  // model_id
        SacGetModelInfoScan,
        SacGetModelInfoBind
    );

    func.named_parameters["secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);

    return func;
}

// ===== sac_get_story_info =====

struct SacGetStoryInfoBindData : public duckdb::TableFunctionData {
public:
    SacStory story;
    bool story_found = false;
    size_t current_index = 0;
    bool finished = false;

    SacGetStoryInfoBindData() = default;
};

static void SacGetStoryInfoScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                                duckdb::DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<SacGetStoryInfoBindData>();

    if (!bind_data.story_found) {
        output.SetCardinality(0);
        bind_data.finished = true;
        return;
    }

    idx_t count = 0;

    if (bind_data.current_index == 0) {
        output.SetValue(0, 0, duckdb::Value(bind_data.story.id));
        output.SetValue(1, 0, duckdb::Value(bind_data.story.name));
        output.SetValue(2, 0, duckdb::Value(bind_data.story.description));
        output.SetValue(3, 0, duckdb::Value(bind_data.story.owner));
        output.SetValue(4, 0, duckdb::Value(bind_data.story.status));
        output.SetValue(5, 0, duckdb::Value(bind_data.story.created_at));
        output.SetValue(6, 0, duckdb::Value(bind_data.story.last_modified_at));

        bind_data.current_index++;
        count = 1;
    }

    bind_data.finished = true;
    output.SetCardinality(count);
}

static duckdb::unique_ptr<duckdb::FunctionData> SacGetStoryInfoBind(
    duckdb::ClientContext &context,
    duckdb::TableFunctionBindInput &input,
    duckdb::vector<duckdb::LogicalType> &return_types,
    duckdb::vector<std::string> &names) {

    if (input.inputs.empty()) {
        throw duckdb::InvalidInputException("sac_get_story_info requires a story_id parameter");
    }

    auto story_id = input.inputs[0].GetValue<std::string>();

    std::string secret_name = "sac";
    if (!input.named_parameters.empty() && input.named_parameters.find("secret") != input.named_parameters.end()) {
        secret_name = input.named_parameters.at("secret").GetValue<std::string>();
    }

    auto secret_params = ResolveSacSecret(context, secret_name);

    auto catalog = std::make_shared<SacCatalogService>(
        secret_params.tenant,
        secret_params.region,
        secret_params.auth_params
    );

    names = {"id", "name", "description", "owner", "status", "created_at", "last_modified_at"};
    return_types = {
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // id
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // name
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // description
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // owner
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // status
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),  // created_at
        duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)   // last_modified_at
    };

    auto story_opt = catalog->GetStory(story_id);

    auto bind = duckdb::make_uniq<SacGetStoryInfoBindData>();
    if (story_opt.has_value()) {
        bind->story = story_opt.value();
        bind->story_found = true;
    } else {
        bind->story_found = false;
    }
    bind->current_index = 0;
    bind->finished = false;

    return std::move(bind);
}

duckdb::TableFunction CreateSacGetStoryInfoFunction() {
    duckdb::TableFunction func(
        {duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)},  // story_id
        SacGetStoryInfoScan,
        SacGetStoryInfoBind
    );

    func.named_parameters["secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);

    return func;
}

} // namespace erpl_web
