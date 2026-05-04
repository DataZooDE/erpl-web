#include "sac_catalog.hpp"
#include "sac_secret_helper.hpp"
#include "sac_catalog_bind_helper.hpp"
#include "sac_client.hpp"
#include "sac_url_builder.hpp"
#include "sac_generic_bind_data.hpp"
#include "graph_output_utils.hpp"
#include "http_client.hpp"
#include "odata_content.hpp"
#include "duckdb/function/table_function.hpp"
#include "telemetry.hpp"
#include <algorithm>
#include <optional>

namespace erpl_web {

using duckdb::PostHogTelemetry;

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

std::vector<SacModel> SacCatalogService::ListModels() const {
    std::vector<SacModel> models;

    try {
        // STUB IMPLEMENTATION: Returns empty vector
        // TODO: Implement full functionality to query planning models via OData
        //
        // Expected implementation:
        // 1. Call catalog_client_->GetMetadata() or similar to fetch model list
        // 2. Parse OData response using ParseModelsResponse()
        // 3. Return populated models vector
        //
        // Required infrastructure:
        // - catalog_client_ is initialized and available
        // - OData v4 service endpoint is configured
        // - ParseModelsResponse() needs implementation (see TODO below)
        //
        // Related: GetModel(), GetModelDimensions(), GetModelMeasures()
        ERPL_TRACE_INFO("SAC_CATALOG", "Listing models for tenant " + tenant_ + " in region " + region_ + " [STUB]");
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("SAC_CATALOG", "Failed to list planning models: " + std::string(e.what()));
    }

    return models;
}

std::optional<SacModel> SacCatalogService::GetModel(const std::string& model_id) const {
    try {
        // STUB IMPLEMENTATION: Returns nullopt (no data)
        // TODO: Implement full functionality to query specific planning model via OData
        //
        // Expected implementation:
        // 1. Use catalog_client_ to query specific model endpoint
        // 2. Call ParseModelEntity() on response
        // 3. Return SacModel wrapped in optional
        //
        // Note: GetModelDimensions() and GetModelMeasures() depend on this method
        auto url_str = SacUrlBuilder::BuildPlanningDataUrl(tenant_, region_, model_id);
        ERPL_TRACE_INFO("SAC_CATALOG", "Getting model " + model_id + " from " + url_str + " [STUB]");
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("SAC_CATALOG", "Failed to get model " + model_id + ": " + std::string(e.what()));
    }

    return std::nullopt;
}

std::vector<std::string> SacCatalogService::GetModelDimensions(const std::string& model_id) const {
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

std::vector<std::string> SacCatalogService::GetModelMeasures(const std::string& model_id) const {
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

std::vector<SacStory> SacCatalogService::ListStories() const {
    std::vector<SacStory> stories;

    try {
        // STUB IMPLEMENTATION: Returns empty vector
        // TODO: Implement full functionality to query stories via OData story service
        //
        // Expected implementation:
        // 1. Call catalog_client_->Query() or similar to fetch stories
        // 2. Parse OData response using ParseStoriesResponse()
        // 3. Return populated stories vector
        //
        // Dependent methods:
        // - GetStory() depends on this
        // - ListStoriesByOwner() depends on this
        auto stories_url = SacUrlBuilder::BuildStoryServiceUrl(tenant_, region_);
        ERPL_TRACE_INFO("SAC_CATALOG", "Listing stories from " + stories_url + " [STUB]");
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("SAC_CATALOG", "Failed to list stories: " + std::string(e.what()));
    }

    return stories;
}

std::optional<SacStory> SacCatalogService::GetStory(const std::string& story_id) const {
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

std::vector<SacStory> SacCatalogService::ListStoriesByOwner(const std::string& owner) const {
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

std::vector<SacModel> SacCatalogService::ParseModelsResponse(const std::string& odata_response) const {
    std::vector<SacModel> models;

    // STUB IMPLEMENTATION: Returns empty vector (no parsing)
    // TODO: Implement OData response parsing for models
    //
    // Expected implementation:
    // 1. Parse JSON response using yyjson (consistent with Delta Share code)
    // 2. Extract "value" array from OData response
    // 3. Iterate through entities and call ParseModelEntity() for each
    // 4. Return populated models vector
    //
    // Example response structure (OData v4):
    // {
    //   "value": [
    //     { "ID": "MODEL_001", "name": "Sales Planning", "type": "PLANNING", ... },
    //     { "ID": "MODEL_002", "name": "Analytics", "type": "ANALYTICS", ... }
    //   ]
    // }
    //
    // Parser: Use yyjson like Delta Share does in delta_share_client.cpp

    size_t value_pos = odata_response.find("\"value\":");
    if (value_pos == std::string::npos) {
        ERPL_TRACE_DEBUG("SAC_CATALOG", "No 'value' array found in OData response (stub)");
        return models;
    }

    ERPL_TRACE_DEBUG("SAC_CATALOG", "ParseModelsResponse is a stub - implement full parsing [STUB]");
    return models;
}

std::vector<SacStory> SacCatalogService::ParseStoriesResponse(const std::string& odata_response) const {
    std::vector<SacStory> stories;

    // STUB IMPLEMENTATION: Returns empty vector (no parsing)
    // TODO: Implement OData response parsing for stories (similar to ParseModelsResponse)
    //
    // Expected implementation:
    // 1. Parse JSON response using yyjson
    // 2. Extract "value" array from OData response
    // 3. Iterate through entities and call ParseStoryEntity() for each
    // 4. Return populated stories vector

    return stories;
}

SacModel SacCatalogService::ParseModelEntity(const std::string& entity_json) const {
    SacModel model;

    // STUB IMPLEMENTATION: Returns default struct (no parsing)
    // TODO: Implement JSON entity parsing for models
    //
    // Expected implementation:
    // 1. Parse JSON string using yyjson
    // 2. Extract fields:
    //    - ID -> model.id
    //    - name -> model.name
    //    - description -> model.description
    //    - type -> model.type (PLANNING or ANALYTICS)
    //    - owner -> model.owner
    //    - createdAt -> model.created_at
    //    - modifiedAt -> model.last_modified_at
    //    - dimensions -> model.dimensions (array)
    //    - measures -> model.measures (array)
    // 3. Return populated SacModel

    return model;
}

SacStory SacCatalogService::ParseStoryEntity(const std::string& entity_json) const {
    SacStory story;

    // STUB IMPLEMENTATION: Returns default struct (no parsing)
    // TODO: Implement JSON entity parsing for stories
    //
    // Expected implementation:
    // 1. Parse JSON string using yyjson
    // 2. Extract fields:
    //    - ID -> story.id
    //    - name -> story.name
    //    - description -> story.description
    //    - owner -> story.owner
    //    - createdAt -> story.created_at
    //    - modifiedAt -> story.last_modified_at
    //    - status -> story.status (DRAFT or PUBLISHED)
    // 3. Return populated SacStory

    return story;
}

// ===== DuckDB Table Functions =====

// Using template: SacGenericBindData<SacModel> for sac_list_models
using SacShowModelsBindData = SacGenericBindData<SacModel>;

// Scan function for sac_list_models
static void SacShowModelsScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                              duckdb::DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<SacShowModelsBindData>();

    idx_t count = 0;
    while (bind_data.current_index < bind_data.items.size() && count < output.GetCapacity()) {
        const auto &model = bind_data.items[bind_data.current_index];

        SetStrCellNN(output.data[0], count, model.id.c_str());
        SetStrCellNN(output.data[1], count, model.name.c_str());
        SetStrCellNN(output.data[2], count, model.description.c_str());
        SetStrCellNN(output.data[3], count, model.type.c_str());
        SetStrCellNN(output.data[4], count, model.owner.c_str());
        SetStrCellNN(output.data[5], count, model.created_at.c_str());
        SetStrCellNN(output.data[6], count, model.last_modified_at.c_str());

        bind_data.current_index++;
        count++;
    }

    bind_data.finished = (bind_data.current_index >= bind_data.items.size());
    output.SetCardinality(count);
}

// Bind function for sac_list_models
static duckdb::unique_ptr<duckdb::FunctionData> SacShowModelsBind(
    duckdb::ClientContext &context,
    duckdb::TableFunctionBindInput &input,
    duckdb::vector<duckdb::LogicalType> &return_types,
    duckdb::vector<std::string> &names) {
    PostHogTelemetry::Instance().CaptureFunctionExecution("sac_show_models");

    // Extract and resolve credentials
    auto secret_name = SacCatalogBindHelper::ExtractSecretName(input);
    auto secret_data = SacCatalogBindHelper::ResolveSacCredentials(context, secret_name);
    auto catalog = SacCatalogBindHelper::CreateCatalogService(secret_data);

    // Fetch data
    auto models = catalog->ListModels();

    // Set return types and column names
    names = {"id", "name", "description", "type", "owner", "created_at", "last_modified_at"};
    return_types = SacCatalogBindHelper::CreateVarcharReturnTypes(names.size());

    // Populate bind data
    auto bind = duckdb::make_uniq<SacShowModelsBindData>();
    bind->items = models;
    bind->current_index = 0;
    bind->finished = models.empty();

    return std::move(bind);
}

duckdb::TableFunctionSet CreateSacShowModelsFunction() {
    duckdb::TableFunctionSet function_set("sac_show_models");
    
    duckdb::TableFunction func(
        {},  // No parameters
        SacShowModelsScan,
        SacShowModelsBind
    );

    // Add optional named parameter for secret name
    func.named_parameters["secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);

    function_set.AddFunction(func);
    return function_set;
}

// ===== sac_list_stories =====

// Using template: SacGenericBindData<SacStory> for sac_list_stories
using SacShowStoriesBindData = SacGenericBindData<SacStory>;

static void SacShowStoriesScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                               duckdb::DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<SacShowStoriesBindData>();

    idx_t count = 0;
    while (bind_data.current_index < bind_data.items.size() && count < output.GetCapacity()) {
        const auto &story = bind_data.items[bind_data.current_index];

        SetStrCellNN(output.data[0], count, story.id.c_str());
        SetStrCellNN(output.data[1], count, story.name.c_str());
        SetStrCellNN(output.data[2], count, story.description.c_str());
        SetStrCellNN(output.data[3], count, story.owner.c_str());
        SetStrCellNN(output.data[4], count, story.created_at.c_str());
        SetStrCellNN(output.data[5], count, story.last_modified_at.c_str());
        SetStrCellNN(output.data[6], count, story.status.c_str());

        bind_data.current_index++;
        count++;
    }

    bind_data.finished = (bind_data.current_index >= bind_data.items.size());
    output.SetCardinality(count);
}

static duckdb::unique_ptr<duckdb::FunctionData> SacShowStoriesBind(
    duckdb::ClientContext &context,
    duckdb::TableFunctionBindInput &input,
    duckdb::vector<duckdb::LogicalType> &return_types,
    duckdb::vector<std::string> &names) {
    PostHogTelemetry::Instance().CaptureFunctionExecution("sac_show_stories");

    // Extract and resolve credentials
    auto secret_name = SacCatalogBindHelper::ExtractSecretName(input);
    auto secret_data = SacCatalogBindHelper::ResolveSacCredentials(context, secret_name);
    auto catalog = SacCatalogBindHelper::CreateCatalogService(secret_data);

    // Fetch data
    auto stories = catalog->ListStories();

    // Set return types and column names
    names = {"id", "name", "description", "owner", "created_at", "last_modified_at", "status"};
    return_types = SacCatalogBindHelper::CreateVarcharReturnTypes(names.size());

    // Populate bind data
    auto bind = duckdb::make_uniq<SacShowStoriesBindData>();
    bind->items = stories;
    bind->current_index = 0;
    bind->finished = stories.empty();

    return std::move(bind);
}

duckdb::TableFunctionSet CreateSacShowStoriesFunction() {
    duckdb::TableFunctionSet function_set("sac_show_stories");
    
    duckdb::TableFunction func(
        {},  // No parameters
        SacShowStoriesScan,
        SacShowStoriesBind
    );

    func.named_parameters["secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);

    function_set.AddFunction(func);
    return function_set;
}

// ===== sac_describe_model =====

// Using template: SacItemWithDetailsBindData<SacModel> for sac_describe_model
using SacGetModelInfoBindData = SacItemWithDetailsBindData<SacModel>;

static void SacGetModelInfoScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                                duckdb::DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<SacGetModelInfoBindData>();

    if (!bind_data.item_found) {
        output.SetCardinality(0);
        bind_data.finished = true;
        return;
    }

    idx_t count = 0;

    // Output one row for the model with dimension list
    if (bind_data.current_index == 0) {
        // Build dimension list as comma-separated string
        std::string dims_str;
        for (size_t i = 0; i < bind_data.details.size(); ++i) {
            if (i > 0) dims_str += ", ";
            dims_str += bind_data.details[i];
        }

        SetStrCellNN(output.data[0], 0, bind_data.item.id.c_str());
        SetStrCellNN(output.data[1], 0, bind_data.item.name.c_str());
        SetStrCellNN(output.data[2], 0, bind_data.item.description.c_str());
        SetStrCellNN(output.data[3], 0, bind_data.item.type.c_str());
        SetStrCellNN(output.data[4], 0, dims_str.c_str());
        SetStrCellNN(output.data[5], 0, bind_data.item.created_at.c_str());

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
    PostHogTelemetry::Instance().CaptureFunctionExecution("sac_describe_model");

    // Extract and validate parameters
    auto model_id = SacCatalogBindHelper::ExtractPositionalString(input, 0, "model_id");
    auto secret_name = SacCatalogBindHelper::ExtractSecretName(input);

    // Extract and resolve credentials
    auto secret_data = SacCatalogBindHelper::ResolveSacCredentials(context, secret_name);
    auto catalog = SacCatalogBindHelper::CreateCatalogService(secret_data);

    // Set return types and column names
    names = {"id", "name", "description", "type", "dimensions", "created_at"};
    return_types = SacCatalogBindHelper::CreateVarcharReturnTypes(names.size());

    // Fetch data
    auto model_opt = catalog->GetModel(model_id);

    // Populate bind data
    auto bind = duckdb::make_uniq<SacGetModelInfoBindData>();
    if (model_opt.has_value()) {
        bind->item = model_opt.value();
        bind->details = catalog->GetModelDimensions(model_id);
        bind->item_found = true;
    } else {
        bind->item_found = false;
    }
    bind->current_index = 0;
    bind->finished = false;

    return std::move(bind);
}

duckdb::TableFunctionSet CreateSacGetModelInfoFunction() {
    duckdb::TableFunctionSet function_set("sac_describe_model");
    
    duckdb::TableFunction func(
        {duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)},  // model_id
        SacGetModelInfoScan,
        SacGetModelInfoBind
    );

    func.named_parameters["secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);

    function_set.AddFunction(func);
    return function_set;
}

// ===== sac_describe_story =====

// Using template: SacSingleItemBindData<SacStory> for sac_describe_story
using SacGetStoryInfoBindData = SacSingleItemBindData<SacStory>;

static void SacGetStoryInfoScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                                duckdb::DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<SacGetStoryInfoBindData>();

    if (!bind_data.item_found) {
        output.SetCardinality(0);
        bind_data.finished = true;
        return;
    }

    idx_t count = 0;

    if (bind_data.current_index == 0) {
        SetStrCellNN(output.data[0], 0, bind_data.item.id.c_str());
        SetStrCellNN(output.data[1], 0, bind_data.item.name.c_str());
        SetStrCellNN(output.data[2], 0, bind_data.item.description.c_str());
        SetStrCellNN(output.data[3], 0, bind_data.item.owner.c_str());
        SetStrCellNN(output.data[4], 0, bind_data.item.status.c_str());
        SetStrCellNN(output.data[5], 0, bind_data.item.created_at.c_str());
        SetStrCellNN(output.data[6], 0, bind_data.item.last_modified_at.c_str());

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
    PostHogTelemetry::Instance().CaptureFunctionExecution("sac_describe_story");

    // Extract and validate parameters
    auto story_id = SacCatalogBindHelper::ExtractPositionalString(input, 0, "story_id");
    auto secret_name = SacCatalogBindHelper::ExtractSecretName(input);

    // Extract and resolve credentials
    auto secret_data = SacCatalogBindHelper::ResolveSacCredentials(context, secret_name);
    auto catalog = SacCatalogBindHelper::CreateCatalogService(secret_data);

    // Set return types and column names
    names = {"id", "name", "description", "owner", "status", "created_at", "last_modified_at"};
    return_types = SacCatalogBindHelper::CreateVarcharReturnTypes(names.size());

    // Fetch data
    auto story_opt = catalog->GetStory(story_id);

    // Populate bind data
    auto bind = duckdb::make_uniq<SacGetStoryInfoBindData>();
    if (story_opt.has_value()) {
        bind->item = story_opt.value();
        bind->item_found = true;
    } else {
        bind->item_found = false;
    }
    bind->current_index = 0;
    bind->finished = false;

    return std::move(bind);
}

duckdb::TableFunctionSet CreateSacGetStoryInfoFunction() {
    duckdb::TableFunctionSet function_set("sac_describe_story");
    
    duckdb::TableFunction func(
        {duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR)},  // story_id
        SacGetStoryInfoScan,
        SacGetStoryInfoBind
    );

    func.named_parameters["secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);

    function_set.AddFunction(func);
    return function_set;
}

} // namespace erpl_web
