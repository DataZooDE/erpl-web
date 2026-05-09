#include "duckdb/function/table_function.hpp"

#include "http_client.hpp"
#include "odata_edm.hpp"
#include "odata_expand_parser.hpp"
#include "odata_read_functions.hpp"
#include "odata_url_helpers.hpp"
#include "yyjson.hpp"

#include "tracing.hpp"
#include "telemetry.hpp"

namespace erpl_web {

using duckdb::PostHogTelemetry;

namespace {

using JsonDoc = duckdb_yyjson::yyjson_doc;
using JsonValue = duckdb_yyjson::yyjson_val;

struct JsonDocHandle {
  explicit JsonDocHandle(JsonDoc *doc) : doc(doc) {}
  ~JsonDocHandle() {
    if (doc) {
      duckdb_yyjson::yyjson_doc_free(doc);
    }
  }

  JsonDocHandle(const JsonDocHandle &) = delete;
  JsonDocHandle &operator=(const JsonDocHandle &) = delete;

  JsonValue *Root() const {
    return doc ? duckdb_yyjson::yyjson_doc_get_root(doc) : nullptr;
  }

  explicit operator bool() const { return doc != nullptr; }

private:
  JsonDoc *doc;
};

static bool IsODataMetadataField(const std::string &field_name) {
  return field_name == "__metadata" || field_name == "__deferred";
}

static bool IsODataV2DeferredNavigationProperty(duckdb_yyjson::yyjson_val *val) {
  // In OData V2 JSON, navigation properties appear as {"__deferred": {"uri": "..."}}
  // Skip these — they are not queryable columns.
  if (!val || !duckdb_yyjson::yyjson_is_obj(val)) {
    return false;
  }
  return duckdb_yyjson::yyjson_obj_get(val, "__deferred") != nullptr;
}

static bool AppendColumnNamesFromObject(
    JsonValue *row_obj, std::vector<std::string> &column_names) {
  if (!row_obj || !duckdb_yyjson::yyjson_is_obj(row_obj)) {
    return false;
  }

  const auto initial_size = column_names.size();
  duckdb_yyjson::yyjson_obj_iter iter;
  duckdb_yyjson::yyjson_obj_iter_init(row_obj, &iter);

  duckdb_yyjson::yyjson_val *key;
  while ((key = duckdb_yyjson::yyjson_obj_iter_next(&iter))) {
    if (!duckdb_yyjson::yyjson_is_str(key)) {
      continue;
    }
    std::string column_name = duckdb_yyjson::yyjson_get_str(key);
    if (IsODataMetadataField(column_name)) {
      continue;
    }
    auto *val = duckdb_yyjson::yyjson_obj_iter_get_val(key);
    if (IsODataV2DeferredNavigationProperty(val)) {
      continue;  // Skip V2 navigation properties (__deferred links)
    }
    column_names.push_back(std::move(column_name));
  }

  return column_names.size() > initial_size;
}

static JsonValue *FirstObjectInArray(JsonValue *array_value) {
  if (!array_value || !duckdb_yyjson::yyjson_is_arr(array_value)) {
    return nullptr;
  }

  auto first_row = duckdb_yyjson::yyjson_arr_get_first(array_value);
  return first_row && duckdb_yyjson::yyjson_is_obj(first_row) ? first_row
                                                              : nullptr;
}

static JsonValue *FirstODataV4Row(JsonValue *root) {
  return FirstObjectInArray(duckdb_yyjson::yyjson_obj_get(root, "value"));
}

static JsonValue *FirstODataV2Row(JsonValue *root) {
  auto data_obj = duckdb_yyjson::yyjson_obj_get(root, "d");
  if (!data_obj || !duckdb_yyjson::yyjson_is_obj(data_obj)) {
    return nullptr;
  }
  return FirstObjectInArray(duckdb_yyjson::yyjson_obj_get(data_obj, "results"));
}

static bool AppendFirstODataRowColumnNames(
    JsonValue *root, std::vector<std::string> &column_names) {
  if (AppendColumnNamesFromObject(FirstODataV4Row(root), column_names)) {
    return true;
  }
  if (AppendColumnNamesFromObject(FirstODataV2Row(root), column_names)) {
    return true;
  }

  // Some callers already pass the first row object rather than the response
  // envelope. Accept that shape to keep extraction local and metadata-free.
  return AppendColumnNamesFromObject(root, column_names);
}

} // namespace

void ODataReadBindData::SetNestedExpandPaths(
    const std::vector<std::string> &nested_paths) {
  if (data_extractor) {
    data_extractor->SetNestedExpandPaths(nested_paths);
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

// Helper function to extract column names from a JSON object
std::vector<std::string>
ExtractColumnNamesFromJson(const std::string &json_content) {
  std::vector<std::string> column_names;

  JsonDocHandle doc(duckdb_yyjson::yyjson_read(json_content.c_str(),
                                               json_content.size(), 0));
  if (!doc) {
    return column_names;
  }

  auto root = doc.Root();
  if (!root) {
    return column_names;
  }

  if (!AppendFirstODataRowColumnNames(root, column_names)) {
    ERPL_TRACE_WARN("ODATA_READ_BIND",
                    "Could not find a row object for column-name extraction");
  }

  return column_names;
}

// ============================================================================
// ODataProgressTracker Implementation
// ============================================================================

void ODataProgressTracker::SetTotalCount(uint64_t total) {
    total_count_ = total;
    has_total_ = true;
}

void ODataProgressTracker::IncrementRowsFetched(uint64_t count) {
    rows_fetched_ += count;
}

double ODataProgressTracker::GetProgressFraction() const {
    if (!has_total_ || total_count_ == 0) {
        return -1.0; // unknown -> DuckDB will not show progress
    }
    double progress = 100.0 * (double)rows_fetched_ / (double)total_count_;
  if (progress < 0.0) {
    progress = 0.0;
  }
    return std::min(100.0, progress);
}

bool ODataProgressTracker::HasTotalCount() const { return has_total_; }

uint64_t ODataProgressTracker::GetTotalCount() const { return total_count_; }

uint64_t ODataProgressTracker::GetRowsFetched() const { return rows_fetched_; }

void ODataProgressTracker::Reset() {
    rows_fetched_ = 0;
    total_count_ = 0;
    has_total_ = false;
}

// ============================================================================
// ODataRowBuffer Implementation
// ============================================================================

void ODataRowBuffer::AddRows(std::vector<std::vector<duckdb::Value>> rows) {
    for (auto &row : rows) {
        row_buffer_.emplace_back(std::move(row));
    }
}

std::vector<duckdb::Value> ODataRowBuffer::GetNextRow() {
    if (row_buffer_.empty()) {
        return {};
    }
    auto row = std::move(row_buffer_.front());
    row_buffer_.pop_front();
    return row;
}

bool ODataRowBuffer::HasMoreRows() const { return !row_buffer_.empty(); }

size_t ODataRowBuffer::Size() const { return row_buffer_.size(); }

void ODataRowBuffer::Clear() { row_buffer_.clear(); }

void ODataRowBuffer::SetHasNextPage(bool has_next) {
    has_next_page_ = has_next;
}

bool ODataRowBuffer::HasNextPage() const { return has_next_page_; }

// ============================================================================
// ODataTypeResolver Implementation
// ============================================================================

ODataTypeResolver::ODataTypeResolver(
    std::shared_ptr<ODataEntitySetClient> odata_client)
    : odata_client(odata_client) {}

duckdb::LogicalType ODataTypeResolver::ResolveNavigationPropertyType(
    const std::string &property_name) const {
    try {
        auto entity_type = odata_client->GetCurrentEntityType();
        
    for (const auto &nav_prop : entity_type.navigation_properties) {
            if (nav_prop.name == property_name) {
                auto [is_collection, type_name] = ExtractCollectionType(nav_prop.type);
                
                duckdb::LogicalType base_type;
                if (type_name.find("Edm.") == 0) {
                    base_type = ConvertPrimitiveTypeString(type_name);
                } else {
                    base_type = ResolveEntityType(type_name);
                }
                
                if (is_collection) {
                    return duckdb::LogicalType::LIST(base_type);
                }
                
                return base_type;
            }
        }
  } catch (const std::exception &e) {
    ERPL_TRACE_WARN("TYPE_RESOLVER",
                    "Failed to resolve type for navigation property '" +
                        property_name + "': " + e.what());
    }
    
    return duckdb::LogicalTypeId::VARCHAR; // Default fallback
}

std::pair<bool, std::string> ODataTypeResolver::GetNavTargetFromCurrentEntity(
    const std::string &nav_prop) const {
  try {
    auto entity_type = odata_client->GetCurrentEntityType();
    for (const auto &np : entity_type.navigation_properties) {
      if (np.name == nav_prop) {
        auto [is_collection, type_name] = ExtractCollectionType(np.type);
        return {is_collection, type_name};
      }
    }
  } catch (...) {
  }
  return {false, std::string()};
}

std::pair<bool, std::string>
ODataTypeResolver::GetNavTargetOnEntity(const std::string &entity_type_name,
                                        const std::string &nav_prop) const {
  try {
    auto edmx = odata_client->GetMetadata();
    auto type_variant = edmx.FindType(entity_type_name);
    EntityType entity;
    if (std::holds_alternative<EntityType>(type_variant)) {
      entity = std::get<EntityType>(type_variant);
    } else {
      return {false, std::string()};
    }
    for (const auto &np : entity.navigation_properties) {
      if (np.name == nav_prop) {
        auto [is_collection, type_name] = ExtractCollectionType(np.type);
        return {is_collection, type_name};
      }
    }
  } catch (...) {
  }
  return {false, std::string()};
}

duckdb::LogicalType ODataTypeResolver::ResolveNavigationOnEntity(
    const std::string &entity_type_name, const std::string &nav_prop) const {
  auto [is_collection, type_name] =
      GetNavTargetOnEntity(entity_type_name, nav_prop);
  if (type_name.empty())
    return duckdb::LogicalTypeId::VARCHAR;
  duckdb::LogicalType base_type;
  if (type_name.find("Edm.") == 0) {
    base_type = ConvertPrimitiveTypeString(type_name);
  } else {
    base_type = ResolveEntityType(type_name);
  }
  if (is_collection) {
    return duckdb::LogicalType::LIST(base_type);
  }
  return base_type;
}

duckdb::LogicalType ODataTypeResolver::ConvertPrimitiveTypeString(
    const std::string &type_name) const {
    return DuckTypeConverter::ConvertEdmPrimitiveStringToLogicalType(type_name);
}

std::tuple<bool, std::string>
ODataTypeResolver::ExtractCollectionType(const std::string &type_name) const {
    std::regex collection_regex("Collection\\(([^\\)]+)\\)");
    std::smatch match;

    if (std::regex_search(type_name, match, collection_regex)) {
        return std::make_tuple(true, match[1]);
    } else {
        return std::make_tuple(false, type_name);
    }
}

duckdb::LogicalType
ODataTypeResolver::ResolveEntityType(const std::string &type_name) const {
    try {
        auto edmx = odata_client->GetMetadata();
        auto target_type = edmx.FindType(type_name);

        // Use DuckTypeConverter to get proper DuckDB type
        auto type_conv = DuckTypeConverter(edmx);
        return std::visit(type_conv, target_type);
    } catch (const std::exception &e) {
        ERPL_TRACE_WARN("TYPE_RESOLVER", "Failed to resolve entity type '" +
                                             type_name + "': " + e.what());
        return duckdb::LogicalTypeId::VARCHAR;
    }
}

duckdb::LogicalType
ODataTypeResolver::ResolveComplexType(const std::string &type_name) const {
    try {
        auto edmx = odata_client->GetMetadata();
        auto target_type = edmx.FindType(type_name);

        // Use DuckTypeConverter to get proper DuckDB type
        auto type_conv = DuckTypeConverter(edmx);
        return std::visit(type_conv, target_type);
    } catch (const std::exception &e) {
        ERPL_TRACE_WARN("TYPE_RESOLVER", "Failed to resolve complex type '" +
                                         type_name + "': " + e.what());
        return duckdb::LogicalTypeId::VARCHAR;
    }
}

// ============================================================================
// ODataReadBindData Implementation
// ============================================================================

ODataReadBindData::ODataReadBindData(
    std::shared_ptr<ODataEntitySetClient> odata_client)
    : odata_client(odata_client) {
    InitializeComponents();
}

ODataReadBindData::ODataReadBindData(
    std::shared_ptr<ODataEntitySetClient> odata_client,
    bool defer_initialization)
    : odata_client(odata_client) {
  // Don't call InitializeComponents() - let the factory method do it with the
  // correct mode
}

void ODataReadBindData::InitializeComponents(bool service_root_mode) {
    predicate_pushdown_helper = nullptr;

  // Only create entity-set specific components if not in service root mode
  if (!service_root_mode) {
    data_extractor = std::make_shared<ODataDataExtractor>(odata_client);
    type_resolver = std::make_shared<ODataTypeResolver>(odata_client);
  } else {
    // In service root mode, these components are not needed and would trigger
    // metadata fetching
    data_extractor = nullptr;
    type_resolver = nullptr;
    ERPL_TRACE_INFO("ODATA_READ_BIND",
                    "Skipping data_extractor and type_resolver creation in "
                    "service root mode");
  }

    progress_tracker = std::make_shared<ODataProgressTracker>();
    row_buffer = std::make_shared<ODataRowBuffer>();
}

// Helper methods for URL detection
bool ODataReadBindData::IsDatasphereUrl(const std::string &entity_set_url) {
    return entity_set_url.find("datasphere") != std::string::npos || 
           entity_set_url.find("hcs.cloud.sap") != std::string::npos;
}

bool ODataReadBindData::IsODataV2Url(const std::string &entity_set_url) {
    return entity_set_url.find("/V2/") != std::string::npos;
}

bool ODataReadBindData::ShouldUseDirectHttp(const std::string &entity_set_url) {
    return IsDatasphereUrl(entity_set_url) || IsODataV2Url(entity_set_url);
}

duckdb::unique_ptr<ODataReadBindData> ODataReadBindData::FromEntitySetRoot(
    const std::string &entity_set_url,
    std::shared_ptr<HttpAuthParams> auth_params) {
  // Create HTTP client with URL encoding disabled for OData (V2 & V4)
  HttpParams http_params;
  http_params.url_encode = false;
  auto http_client = std::make_shared<HttpClient>(http_params);

  // Ensure $format=json is present on the entity set URL; encode $filter via
  // ODataUrlCodec
  HttpUrl es_url(entity_set_url);
  ODataUrlCodec::ensureJsonFormat(es_url);
  auto odata_client =
      std::make_shared<ODataEntitySetClient>(http_client, es_url, auth_params);
    
    // Determine OData version and approach
    bool is_odata_v2_url = IsODataV2Url(entity_set_url);
    bool use_direct_http = ShouldUseDirectHttp(entity_set_url);
    
    // Set OData version directly for OData v2 to skip metadata fetching
    if (is_odata_v2_url) {
        odata_client->SetODataVersionDirectly(ODataVersion::V2);
    ERPL_TRACE_DEBUG(
        "ODATA_READ_BIND",
        "Set OData version to V2 directly to skip metadata fetching");
    }
    
    std::vector<std::string> extracted_column_names;
    
    if (use_direct_http) {
        // Make direct HTTP request to get first page for column extraction
        if (IsDatasphereUrl(entity_set_url)) {
      ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                       "Detected Datasphere URL, making direct HTTP request to "
                       "get first data page for @odata.context");
        } else {
      ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                       "Detected OData v2 URL, making direct HTTP request to "
                       "extract column names");
        }
        
        try {
      HttpRequest req(HttpMethod::GET, HttpUrl(es_url));
            req.AuthHeadersFromParams(*auth_params);
            req.headers["Accept"] = "application/json";
            auto resp = http_client->SendRequest(req);
            
            if (resp && resp->Code() == 200) {
        ERPL_TRACE_DEBUG(
            "ODATA_READ_BIND",
            "Successfully fetched first data page via direct HTTP");
                
                // Parse the response to extract @odata.context and column names
                auto content = resp->Content();
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Parsing JSON response of " +
                                                std::to_string(content.size()) +
                                                " bytes");
        JsonDocHandle doc(
            duckdb_yyjson::yyjson_read(content.c_str(), content.size(), 0));
                
                if (doc) {
          ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                           "Successfully parsed JSON document");
                    auto root = doc.Root();
                    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Got JSON root object");
                    
                    // Check for @odata.context (OData v4)
          auto context_val =
              duckdb_yyjson::yyjson_obj_get(root, "@odata.context");
                    if (context_val && duckdb_yyjson::yyjson_is_str(context_val)) {
                        auto context_url = duckdb_yyjson::yyjson_get_str(context_val);
            ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Extracted @odata.context: " +
                                                    std::string(context_url));
                        
            // Store the metadata context URL in the OData client for future
            // metadata requests
                        std::string clean_context_url = context_url;
                        auto hash_pos = clean_context_url.find('#');
                        if (hash_pos != std::string::npos) {
                            clean_context_url = clean_context_url.substr(0, hash_pos);
                        }
                        
                        if (IsDatasphereUrl(entity_set_url)) {
                            odata_client->SetMetadataContextUrl(clean_context_url);
              ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                               "Stored metadata context URL in OData client: " +
                                   clean_context_url);
                            odata_client->SetEntitySetNameFromContextFragment(context_url);
                        }
                        
                        // Parse OData V4 response format
                        ParseODataV4Response(root, odata_client, extracted_column_names);
                    } else {
                        // Parse OData V2 response format
                        ParseODataV2Response(root, odata_client, extracted_column_names);
                    }
                }
            } else {
        ERPL_TRACE_WARN("ODATA_READ_BIND",
                        "Direct HTTP request failed with status: " +
                            std::to_string(resp ? resp->Code() : 0));
      }
    } catch (const std::exception &e) {
      ERPL_TRACE_WARN("ODATA_READ_BIND",
                      "Failed to fetch first data page via direct HTTP: " +
                          std::string(e.what()));
            // Continue without first page - will fall back to conventional metadata
        }
    } else {
    ERPL_TRACE_DEBUG(
        "ODATA_READ_BIND",
        "Standard OData v4 URL detected, using conventional metadata approach");
    }
    
    auto bind_data = duckdb::make_uniq<ODataReadBindData>(odata_client);
    
    // If we successfully extracted column names, store them for later use
    if (!extracted_column_names.empty()) {
        bind_data->SetExtractedColumnNames(extracted_column_names);
    }
    
    return bind_data;
}

bool ODataReadBindData::LooksLikeServiceRootUrl(const std::string &url) {
  // Strict V2 service root detection: ends with .svc or .svc/
  auto qpos = url.find('?');
  std::string path_only = (qpos == std::string::npos) ? url : url.substr(0, qpos);
  if (path_only.rfind("/$metadata") != std::string::npos) {
    return false;
  }
  auto pos = path_only.rfind(".svc");
  if (pos == std::string::npos) {
    return false;
  }
  // Ensure there are no further path segments after .svc except optional '/'
  size_t after = pos + 4; // length of ".svc"
  if (after == path_only.size()) {
    return true;
  }
  if (after + 1 == path_only.size() && path_only[after] == '/') {
    return true;
  }
  return false;
}

duckdb::unique_ptr<ODataReadBindData> ODataReadBindData::FromServiceRoot(
    const std::string &service_root_url,
    std::shared_ptr<HttpAuthParams> auth_params) {
  // Build minimal schema: name, kind, url (absolute)
  // Reuse ODataServiceClient to fetch and parse the service document, then
  // project rows locally.
  HttpParams http_params;
  http_params.url_encode = false;
  auto http_client = std::make_shared<HttpClient>(http_params);

  HttpUrl svc_url(service_root_url);
  // Ensure JSON format for V2 roots
  ODataUrlCodec::ensureJsonFormat(svc_url);

  auto svc_client =
      std::make_shared<ODataServiceClient>(http_client, svc_url, auth_params);
  // Force V2 headers/parsing for classic service roots when URL indicates V2
  try {
    std::string url_str = service_root_url;
    if (url_str.find("/V2/") != std::string::npos) {
      svc_client->SetODataVersionDirectly(ODataVersion::V2);
      ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                       "FromServiceRoot: forced OData version V2 based on URL");
    }
  } catch (...) {
  }
  // Try to detect/fetch
  auto response = svc_client->Get();

  // Build an entity-set client wrapper so we can reuse existing bind flow but
  // override names/types via extracted list We'll populate
  // extracted_column_names with our unified schema
  auto bind_data = duckdb::make_uniq<ODataReadBindData>(
      std::make_shared<ODataEntitySetClient>(http_client, svc_url,
                                             auth_params));

  // Unified schema (name, kind, url)
  std::vector<std::string> names = {"name", "kind", "url"};
  bind_data->SetExtractedColumnNames(names);

  // Convert service entries to synthetic rows and buffer them
  auto entries = response->EntitySets();
  for (auto &ref : entries) {
    ref.MergeWithBaseUrlIfRelative(svc_url);
  }

  // Build a display base URL without query parameters and with trailing slash
  std::string display_base = service_root_url;
  auto qpos_display = display_base.find('?');
  if (qpos_display != std::string::npos) {
    display_base = display_base.substr(0, qpos_display);
  }
  if (!display_base.empty() && display_base.back() != '/') {
    display_base.push_back('/');
  }

  // Bootstrap row buffer and push rows
  auto rows = std::vector<std::vector<duckdb::Value>>();
  rows.reserve(entries.size());
  for (const auto &ref : entries) {
    // For service root listing, expose absolute URL by extending the base path
    rows.push_back({duckdb::Value(ref.name), duckdb::Value("EntitySet"),
                    duckdb::Value(display_base + ref.name)});
  }

  // Initialize internal components for buffering
  bind_data->InitializeComponents();
  bind_data->EnableServiceRootMode();
  bind_data->row_buffer->AddRows(std::move(rows));
  bind_data->row_buffer->SetHasNextPage(false);
  bind_data->first_page_cached_ = true;

  return bind_data;
}

// Factory pattern methods
duckdb::unique_ptr<ODataReadBindData> ODataReadBindData::FromProbeResult(
    const ODataClientFactory::ProbeResult &result) {
  ERPL_TRACE_INFO(
      "ODATA_READ_BIND",
      std::string("Creating bind data from probe result - is_service_root: ") +
          (result.is_service_root ? "true" : "false"));

  if (result.is_service_root) {
    ERPL_TRACE_INFO("ODATA_READ_BIND", "Taking service root path");
    auto client = ODataClientFactory::CreateServiceClient(result);
    return FromServiceClient(client, result.initial_content);
  } else {
    ERPL_TRACE_INFO("ODATA_READ_BIND", "Taking entity set path");
    auto client = ODataClientFactory::CreateEntitySetClient(result);
    return FromEntitySetClient(client, result.initial_content);
  }
}

duckdb::unique_ptr<ODataReadBindData> ODataReadBindData::FromEntitySetClient(
    std::shared_ptr<ODataEntitySetClient> client,
    const std::string &initial_content) {
  ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                   "Creating bind data from entity set client");

  auto bind_data = duckdb::make_uniq<ODataReadBindData>(client, true);

  // Initialize components for entity set mode (allows metadata fetching)
  bind_data->InitializeComponents(false);

  // If we have initial content, we can pre-populate the first page
  if (!initial_content.empty()) {
    // Parse the initial content to extract column names and types
    try {
      JsonDocHandle doc(duckdb_yyjson::yyjson_read(
          initial_content.c_str(), initial_content.length(), 0));
      if (doc) {
        auto root = doc.Root();
        if (root) {
          std::vector<std::string> extracted_column_names;

          // Determine OData version and parse accordingly
          if (client->GetODataVersion() == ODataVersion::V4) {
            ParseODataV4Response(root, client, extracted_column_names);
          } else {
            ParseODataV2Response(root, client, extracted_column_names);
          }

          bind_data->SetExtractedColumnNames(extracted_column_names);
        }
      }
    } catch (...) {
      // If parsing fails, we'll fall back to normal metadata-based approach
      ERPL_TRACE_DEBUG(
          "ODATA_READ_BIND",
          "Failed to parse initial content, will use metadata approach");
    }

    // Buffer the rows from initial_content so the next FetchNextResult does
    // not issue a redundant bare GET. Without this, callers that hand a
    // pre-fetched page in (e.g. the ODP streaming path) trigger
    // PrefetchFirstPage → odata_client->Get() with no query string, which
    // SAP ODP answers with the entire dataset, inflating results N×.
    try {
      auto synthetic = std::make_shared<ODataEntitySetResponse>(
          std::make_unique<HttpResponse>(HttpMethod::GET, HttpUrl(client->Url()),
                                         200, "application/json", initial_content),
          client->GetODataVersion());
      bind_data->BufferFirstPageFromResponse(synthetic);
    } catch (const std::exception &e) {
      ERPL_TRACE_DEBUG(
          "ODATA_READ_BIND",
          std::string("Failed to buffer initial content; "
                      "PrefetchFirstPage will refetch: ") +
              e.what());
    }
  }

  return bind_data;
}

duckdb::unique_ptr<ODataReadBindData>
ODataReadBindData::FromServiceClient(std::shared_ptr<ODataServiceClient> client,
                                     const std::string &initial_content) {
  ERPL_TRACE_INFO("ODATA_READ_BIND", "Creating bind data from service client");

  // Create a minimal stub entity set client for compatibility with existing
  // bind data structure This client will never be used for actual HTTP requests
  // in service root mode
  HttpParams http_params;
  http_params.url_encode = false;
  auto http_client = std::make_shared<HttpClient>(http_params);

  // Use a dummy URL to avoid any metadata resolution attempts
  auto stub_client = std::make_shared<ODataEntitySetClient>(
      http_client, HttpUrl("http://localhost/stub"), nullptr);
  stub_client->SetODataVersionDirectly(client->GetODataVersion());

  auto bind_data = duckdb::make_uniq<ODataReadBindData>(stub_client, true);

  // Always enable service root mode for service clients
  bind_data->EnableServiceRootMode();

  // Set unified schema (name, kind, url) BEFORE initializing components
  // This ensures GetResultNames() never falls back to calling the client
  std::vector<std::string> names = {"name", "kind", "url"};
  bind_data->SetExtractedColumnNames(names);

  // Initialize components with service root mode (this prevents metadata
  // fetching)
  bind_data->InitializeComponents(true);
  ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                   "FromServiceClient: service root mode enabled");

  // If we have initial content, parse it via ODataServiceResponse (managed
  // yyjson lifecycle)
  if (!initial_content.empty()) {
    try {
      auto temp_response = std::make_shared<ODataServiceResponse>(
          std::make_unique<HttpResponse>(HttpMethod::GET, HttpUrl(""), 200,
                                         "application/json", initial_content),
          client->GetODataVersion());

      auto entries = temp_response->EntitySets();

      // Convert service entries to synthetic rows and buffer them with absolute URLs
      auto rows = std::vector<std::vector<duckdb::Value>>();
      rows.reserve(entries.size());

      // Derive display base from the service client URL
      std::string display_base = client->Url();
      auto qpos_display = display_base.find('?');
      if (qpos_display != std::string::npos) {
        display_base = display_base.substr(0, qpos_display);
      }
      if (!display_base.empty() && display_base.back() != '/') {
        display_base.push_back('/');
      }

      for (const auto &ref : entries) {
        std::string kind = "EntitySet"; // could be extended later for Singletons, etc.
        rows.push_back({duckdb::Value(ref.name), duckdb::Value(kind),
                        duckdb::Value(display_base + ref.name)});
      }

      // Buffer rows (components already initialized in service root mode)
      bind_data->row_buffer->AddRows(std::move(rows));
      bind_data->row_buffer->SetHasNextPage(false);
      bind_data->first_page_cached_ = true;
    } catch (...) {
      ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                       "Failed to parse service root initial content; "
                       "continuing without prebuffer");
    }
  } else {
    // Fallback: fetch now and buffer entries
    try {
      auto response = client->Get();
      auto entries = response->EntitySets();

      auto rows = std::vector<std::vector<duckdb::Value>>();
      rows.reserve(entries.size());
      for (const auto &ref : entries) {
        std::string kind = "EntitySet";
        rows.push_back({duckdb::Value(ref.name), duckdb::Value(kind),
                        duckdb::Value(ref.name)});
      }

      bind_data->row_buffer->AddRows(std::move(rows));
      bind_data->row_buffer->SetHasNextPage(false);
      bind_data->first_page_cached_ = true;
    } catch (...) {
      ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                       "FromServiceClient fallback fetch failed");
    }
    }
    
    return bind_data;
}

// Helper methods for parsing OData responses
void ODataReadBindData::ParseODataV4Response(
    duckdb_yyjson::yyjson_val *root,
    std::shared_ptr<ODataEntitySetClient> odata_client,
    std::vector<std::string> &extracted_column_names) {
  (void)odata_client;
  ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Parsing OData v4 response format");
  if (AppendColumnNamesFromObject(FirstODataV4Row(root),
                                  extracted_column_names)) {
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     "Extracted column names from OData v4 first row");
    return;
  }

  ERPL_TRACE_WARN("ODATA_READ_BIND",
                  "Could not extract column names from OData v4 response");
}

void ODataReadBindData::ParseODataV2Response(
    duckdb_yyjson::yyjson_val *root,
    std::shared_ptr<ODataEntitySetClient> odata_client,
    std::vector<std::string> &extracted_column_names) {
  (void)odata_client;
  ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                   "No @odata.context found, trying OData v2 format");
  if (AppendColumnNamesFromObject(FirstODataV2Row(root),
                                  extracted_column_names)) {
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     "Extracted column names from OData v2 first row");
    return;
  }

  ERPL_TRACE_WARN("ODATA_READ_BIND",
                  "Could not extract column names from OData v2 response");
}

std::vector<std::string> ODataReadBindData::GetResultNames(bool all_columns) {
  ERPL_TRACE_INFO("ODATA_READ_BIND",
                  std::string("GetResultNames called - service_root_mode: ") +
                      (service_root_mode_ ? "true" : "false"));

    std::vector<std::string> base_names;

    // If we have extracted column names from the first data row, use those
    if (!extracted_column_names.empty()) {
        base_names = extracted_column_names;
    ERPL_TRACE_INFO("ODATA_READ_BIND", "Using extracted column names, count: " +
                                           std::to_string(base_names.size()));
  } else if (service_root_mode_) {
    // In service root mode, we should never reach here, but provide a fallback
    ERPL_TRACE_WARN("ODATA_READ_BIND", "GetResultNames called in service root "
                                       "mode without extracted column names!");
    base_names = {"name", "kind", "url"};
    } else {
        // Fall back to getting names from the OData client (metadata)
        if (all_result_names.empty()) {
      ERPL_TRACE_INFO("ODATA_READ_BIND",
                      "Calling odata_client->GetResultNames() for metadata");
            all_result_names = odata_client->GetResultNames();
        }
        base_names = all_result_names;
    }
    
    // Add expanded data columns if we have any, avoiding duplicates
    if (HasExpandedData()) {
    ERPL_TRACE_DEBUG(
        "ODATA_READ_BIND",
        "Adding expanded data columns. Schema size: " +
            std::to_string(data_extractor->GetExpandedDataSchema().size()) +
                        ", Base names size: " + std::to_string(base_names.size()));
        
    for (const auto &expand_name : data_extractor->GetExpandedDataSchema()) {
      // Check if this column name already exists in base_names to avoid
      // duplicates
            bool exists = false;
      for (const auto &base_name : base_names) {
                if (base_name == expand_name) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
        ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                         "Adding expanded column: " + expand_name);
                base_names.push_back(expand_name);
            } else {
        ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                         "Skipping duplicate expanded column: " + expand_name);
            }
        }
        
    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Final base names size: " +
                                            std::to_string(base_names.size()));
    }
    
    // Ensure we always return the same size as GetResultTypes would return
    // This is critical for DuckDB binding consistency
    if (all_columns || active_column_ids.empty()) {
        return base_names;
    }

    std::vector<std::string> active_result_names;
    for (auto &column_id : active_column_ids) {
        if (duckdb::IsRowIdColumnId(column_id)) {
            continue;
        }

        if (column_id < base_names.size()) {
            active_result_names.push_back(base_names[column_id]);
        }
    }

    return active_result_names;
}

std::vector<duckdb::LogicalType>
ODataReadBindData::GetResultTypes(bool all_columns) {
  ERPL_TRACE_INFO("ODATA_READ_BIND",
                  std::string("GetResultTypes called - service_root_mode: ") +
                      (service_root_mode_ ? "true" : "false"));

  // Only get types from the OData client (metadata) if we haven't mapped them
  // yet
    if (all_result_types.empty()) {
    if (service_root_mode_) {
      // In service root mode, use fixed VARCHAR types for the unified schema
      ERPL_TRACE_INFO("ODATA_READ_BIND",
                      "Using fixed VARCHAR types for service root mode");
      all_result_types = {duckdb::LogicalTypeId::VARCHAR,
                          duckdb::LogicalTypeId::VARCHAR,
                          duckdb::LogicalTypeId::VARCHAR};
    } else {
      ERPL_TRACE_INFO("ODATA_READ_BIND",
                      "Calling odata_client->GetResultTypes() for metadata");
        all_result_types = odata_client->GetResultTypes();
    }
        
        // If we have extracted column names, align types to those names
    if (!extracted_column_names.empty() && !service_root_mode_) {
            if (extracted_column_names.size() == all_result_types.size()) {
                // Create a mapping from extracted column names to metadata types
                std::vector<duckdb::LogicalType> mapped_types;
                mapped_types.reserve(extracted_column_names.size());
                
                // Get the metadata column names to map them to extracted names
                auto metadata_names = odata_client->GetResultNames();
                
        for (const auto &extracted_name : extracted_column_names) {
                    // Find the index of this column in the metadata
          auto it = std::find(metadata_names.begin(), metadata_names.end(),
                              extracted_name);
                    if (it != metadata_names.end()) {
                        size_t metadata_index = std::distance(metadata_names.begin(), it);
                        if (metadata_index < all_result_types.size()) {
                            mapped_types.push_back(all_result_types[metadata_index]);
              ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                               "Mapped column '" + extracted_name +
                                   "' to type: " +
                                   all_result_types[metadata_index].ToString());
                        } else {
                            // Fallback to VARCHAR if metadata index is out of bounds
                            mapped_types.push_back(duckdb::LogicalTypeId::VARCHAR);
              ERPL_TRACE_WARN(
                  "ODATA_READ_BIND",
                  "Column '" + extracted_name +
                      "' not found in metadata, using VARCHAR fallback");
                        }
                    } else {
                        // Fallback to VARCHAR if column not found in metadata
                        mapped_types.push_back(duckdb::LogicalTypeId::VARCHAR);
            ERPL_TRACE_WARN(
                "ODATA_READ_BIND",
                "Column '" + extracted_name +
                    "' not found in metadata, using VARCHAR fallback");
                    }
                }
                
                // Replace all_result_types with the mapped types
                all_result_types = mapped_types;
            } else {
        // Sizes mismatch (common in OData v2 when inferring from data) ->
        // default all to VARCHAR
        ERPL_TRACE_INFO(
            "ODATA_READ_BIND",
            duckdb::StringUtil::Format(
                "Metadata column count (%d) does not match extracted column "
                "count (%d); defaulting all types to VARCHAR",
                    all_result_types.size(), extracted_column_names.size()));
        all_result_types.assign(extracted_column_names.size(),
                                duckdb::LogicalTypeId::VARCHAR);
            }
        }
    }
    
    // Create a combined types vector that includes expanded data types
    std::vector<duckdb::LogicalType> combined_types = all_result_types;

    // Compute the base column names to support duplicate detection
    std::vector<std::string> base_names_local;
    if (!extracted_column_names.empty()) {
        base_names_local = extracted_column_names;
    } else {
        if (all_result_names.empty()) {
            all_result_names = odata_client->GetResultNames();
        }
        base_names_local = all_result_names;
    }
    
  // Add/merge expanded data types if we have any, ensuring alignment with
  // schema
    if (HasExpandedData()) {
    ERPL_TRACE_DEBUG(
        "ODATA_READ_BIND",
        "Merging expanded data types. Schema size: " +
            std::to_string(data_extractor->GetExpandedDataSchema().size()) +
            ", Types size: " +
            std::to_string(data_extractor->GetExpandedDataTypes().size()));
        
        // Ensure we have the same number of types as schema columns where appended
        const auto &exp_schema = data_extractor->GetExpandedDataSchema();
    const auto &exp_types = data_extractor->GetExpandedDataTypes();
        for (size_t i = 0; i < exp_schema.size(); ++i) {
            const auto &exp_name = exp_schema[i];
      duckdb::LogicalType exp_type =
          (i < exp_types.size()) ? exp_types[i] : duckdb::LogicalTypeId::VARCHAR;
            
      // Check if this type has been updated from the fallback LIST(VARCHAR) to
      // a proper struct type
            if (exp_type.id() == duckdb::LogicalTypeId::LIST && 
          duckdb::ListType::GetChildType(exp_type).id() !=
              duckdb::LogicalTypeId::VARCHAR) {
        ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                         "Using updated inferred type for expanded column '" +
                             exp_name + "': " + exp_type.ToString());
      }

      auto it =
          std::find(base_names_local.begin(), base_names_local.end(), exp_name);
            if (it != base_names_local.end()) {
                size_t idx = std::distance(base_names_local.begin(), it);
                if (idx < combined_types.size()) {
                    // Replace base type with expanded type
                    combined_types[idx] = exp_type;
          ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                           "Replaced base type for expanded column '" +
                               exp_name + "' at index " + std::to_string(idx) +
                               " with " + exp_type.ToString());
                }
            } else {
                // Append new expanded column type
                combined_types.push_back(exp_type);
        ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                         "Appended expanded type for new column '" + exp_name +
                             "': " + exp_type.ToString());
            }
        }
    }
    
  ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                   "Final combined types size: " +
                       std::to_string(combined_types.size()));

    if (all_columns || active_column_ids.empty()) {
        return combined_types;
    }

    std::vector<duckdb::LogicalType> active_result_types;
    for (auto &column_id : active_column_ids) {
        if (duckdb::IsRowIdColumnId(column_id)) {
            continue;
        }

        if (column_id < combined_types.size()) {
            active_result_types.push_back(combined_types[column_id]);
        }
    }

    return active_result_types;
}

// ============================================================================
// FetchNextResult Helper Methods
// ============================================================================

void ODataReadBindData::EnsureInitialized() {
    // Ensure input parameters are set on client before any request
    if (!input_parameters.empty()) {
        odata_client->SetInputParameters(input_parameters);
    }

    // Make sure first page is prefetched once and buffered
    if (!first_page_cached_) {
        PrefetchFirstPage();
    }
}

SchemaInfo ODataReadBindData::PrepareSchemaInfo() {
    SchemaInfo info;
    
    // Always use full schema for buffering; projection is applied at emission
    info.all_result_names = GetResultNames(true);
    info.all_result_types = GetResultTypes(true);
    info.has_expand = HasExpandedData();
    
    // Build name->index map for robust lookups
    info.name_to_index.reserve(info.all_result_names.size());
    for (duckdb::idx_t k = 0; k < (duckdb::idx_t)info.all_result_names.size(); ++k) {
        info.name_to_index.emplace(info.all_result_names[k], k);
    }
    
    return info;
}

void ODataReadBindData::FetchAdditionalPagesIfNeeded(const SchemaInfo& schema_info) {
    const idx_t target = STANDARD_VECTOR_SIZE;
    
    // Fetch additional pages until we have enough buffered rows to fill the
    // vector or no more pages
    while (row_buffer->Size() < target && row_buffer->HasNextPage()) {
        auto next_response = odata_client->Get(true);
        if (!next_response) {
            row_buffer->SetHasNextPage(false);
            break;
        }
        
        ProcessPageResponse(next_response, schema_info);
    }
}

void ODataReadBindData::ProcessPageResponse(
    std::shared_ptr<ODataEntitySetResponse> response, 
    const SchemaInfo& schema_info) {
    
    // Capture total count once for progress
    if (response->GetODataVersion() == ODataVersion::V4) {
        auto total = response->Content()->TotalCount();
        if (total.has_value() && (!progress_tracker->HasTotalCount() ||
                                  progress_tracker->GetTotalCount() == 0)) {
            progress_tracker->SetTotalCount(total.value());
        }
    }
    
    // Extract expanded data from subsequent pages as well
    if (schema_info.has_expand && !data_extractor->GetExpandedDataSchema().empty()) {
        try {
            ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                           "Extracting expanded data from subsequent page");
            data_extractor->ExtractExpandedDataFromResponse(response->RawContent());
        } catch (const std::exception &e) {
            ERPL_TRACE_WARN(
                "ODATA_READ_BIND",
                "Failed to extract expanded data from subsequent page: " +
                    std::string(e.what()));
        }
    }

    // Buffer full rows using full schema to keep indices stable
    auto column_names = schema_info.all_result_names;
    auto column_types = schema_info.all_result_types;
    auto page_rows = response->ToRows(column_names, column_types);
    const auto row_count = page_rows.size();
    row_buffer->AddRows(std::move(page_rows));
    row_buffer->SetHasNextPage(response->NextUrl().has_value());
    progress_tracker->IncrementRowsFetched(row_count);
}

idx_t ODataReadBindData::EmitRowsToOutput(duckdb::DataChunk &output, const SchemaInfo& schema_info) {
    const idx_t target = STANDARD_VECTOR_SIZE;
    idx_t to_emit = std::min<idx_t>(row_buffer->Size(), target);
    
    for (idx_t i = 0; i < to_emit; i++) {
        const auto &row = row_buffer->GetNextRow();
        EmitSingleRowToOutput(output, row, i, schema_info);
        emitted_row_index_++;
    }
    
    output.SetCardinality(to_emit);
    return to_emit;
}

void ODataReadBindData::EmitSingleRowToOutput(
    duckdb::DataChunk &output, 
    const std::vector<duckdb::Value> &row, 
    idx_t row_index,
    const SchemaInfo& schema_info) {
    
    auto null_value = duckdb::Value();
    
    for (idx_t j = 0; j < output.ColumnCount(); j++) {
        duckdb::idx_t original_column_index = GetOriginalColumnIndex(j);
        
        if (original_column_index < schema_info.all_result_names.size()) {
            auto value = GetColumnValue(original_column_index, row, schema_info);
            output.SetValue(j, row_index, value);
        } else {
            auto col_type = output.data[j].GetType();
            output.SetValue(j, row_index, null_value.DefaultCastAs(col_type));
        }
    }
}

duckdb::idx_t ODataReadBindData::GetOriginalColumnIndex(idx_t activated_column_index) const {
    return (activated_column_index < activated_to_original_mapping.size())
               ? activated_to_original_mapping[activated_column_index]
               : activated_column_index;
}

duckdb::Value ODataReadBindData::GetColumnValue(
    duckdb::idx_t original_column_index,
    const std::vector<duckdb::Value> &row,
    const SchemaInfo& schema_info) {
    
    auto null_value = duckdb::Value();
    
    // Check if this original index points to an expanded column
    if (IsExpandedColumn(original_column_index, schema_info)) {
        return GetExpandedColumnValue(original_column_index, schema_info);
    } else {
        return GetRegularColumnValue(original_column_index, row, schema_info);
    }
}

bool ODataReadBindData::IsExpandedColumn(duckdb::idx_t original_column_index, const SchemaInfo& schema_info) const {
    return schema_info.has_expand && 
           !data_extractor->GetExpandedDataSchema().empty() &&
           original_column_index >= (schema_info.all_result_names.size() - 
                                   data_extractor->GetExpandedDataSchema().size());
}

duckdb::Value ODataReadBindData::GetExpandedColumnValue(
    duckdb::idx_t original_column_index, 
    const SchemaInfo& schema_info) {
    
    auto null_value = duckdb::Value();
    
    size_t expand_index = original_column_index - 
                         (schema_info.all_result_names.size() - 
                          data_extractor->GetExpandedDataSchema().size());
    
    if (expand_index < data_extractor->GetExpandedDataSchema().size()) {
        std::string expand_path = data_extractor->GetExpandedDataSchema()[expand_index];
        
        // Use global emitted_row_index_ to align with how cache was filled (per input row)
        auto expand_data = data_extractor->ExtractExpandedDataForRow(
            std::to_string(emitted_row_index_), expand_path);
        
        if (!expand_data.IsNull()) {
            ERPL_TRACE_DEBUG("ODATA_SCAN", duckdb::StringUtil::Format("Setting expanded data for path '%s'", expand_path.c_str()));
            return expand_data;
        } else {
            ERPL_TRACE_DEBUG("ODATA_SCAN", duckdb::StringUtil::Format("Setting expanded data to NULL for path '%s'", expand_path.c_str()));
            return null_value;
        }
    }
    
    return null_value;
}

duckdb::Value ODataReadBindData::GetRegularColumnValue(
    duckdb::idx_t original_column_index,
    const std::vector<duckdb::Value> &row,
    const SchemaInfo& schema_info) {
    
    auto null_value = duckdb::Value();
    
    // Regular base column from buffered full row: resolve by name to guard against index drift
    const std::string &col_name = schema_info.all_result_names[original_column_index];
    auto it = schema_info.name_to_index.find(col_name);
    
    if (it != schema_info.name_to_index.end() && it->second < row.size()) {
        return row[it->second];
    } else {
        return null_value;
    }
}

void ODataReadBindData::UpdateProgressTracking(idx_t rows_emitted) {
    progress_tracker->IncrementRowsFetched(rows_emitted);
    
    if (progress_tracker->HasTotalCount() && progress_tracker->GetTotalCount() > 0) {
        double pct = 100.0 * (double)progress_tracker->GetRowsFetched() /
                     (double)progress_tracker->GetTotalCount();
        if (pct > 100.0) {
            pct = 100.0;
        }
        
        ERPL_TRACE_INFO("ODATA_SCAN",
                        duckdb::StringUtil::Format(
                            "Progress: %.2f%% (%llu/%llu)", pct,
                            (unsigned long long)progress_tracker->GetRowsFetched(),
                            (unsigned long long)progress_tracker->GetTotalCount()));
    }
}

unsigned int ODataReadBindData::FetchNextResult(duckdb::DataChunk &output) {
    EnsureInitialized();
    
    auto schema_info = PrepareSchemaInfo();
    FetchAdditionalPagesIfNeeded(schema_info);
    
    idx_t rows_emitted = EmitRowsToOutput(output, schema_info);
    UpdateProgressTracking(rows_emitted);
    
    return rows_emitted;
}

bool ODataReadBindData::HasMoreResults() {
    // If buffer still has rows, we have results to emit
    if (row_buffer->HasMoreRows()) {
        return true;
    }
    // If first page hasn't been cached yet, we need to deliver it
    if (!first_page_cached_) {
        return true;
    }
    // Otherwise, only if server indicated a next page
    return row_buffer->HasNextPage();
}

void ODataReadBindData::ActivateColumns(
    const std::vector<duckdb::column_t> &column_ids) {
    std::stringstream column_ids_str;
    for (auto &column_id : column_ids) {
        column_ids_str << column_id << ", ";
    }
  ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                   duckdb::StringUtil::Format("Activating columns: %s",
                                              column_ids_str.str().c_str()));
    
    // Filter out ROW_ID columns from activation
    std::vector<duckdb::column_t> visible_ids;
    visible_ids.reserve(column_ids.size());
    for (auto &column_id : column_ids) {
        if (duckdb::IsRowIdColumnId(column_id)) {
      ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                       "Skipping ROW_ID column from activation mapping");
            continue;
        }
        visible_ids.push_back(column_id);
    }

    active_column_ids = visible_ids;
    
    // Build the mapping from activated column index to original column name index
    activated_to_original_mapping.clear();
    activated_to_original_mapping.resize(visible_ids.size());
    
    for (size_t i = 0; i < visible_ids.size(); ++i) {
        activated_to_original_mapping[i] = visible_ids[i];
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     duckdb::StringUtil::Format(
                         "Mapping activated index %d to original index %d",
                         (int)i, (int)visible_ids[i]));
  }

  // Preserve existing predicate pushdown state (top/skip/expand/filter) across
  // activation Do NOT reset the helper; just update the column selection
  ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                   std::string("ActivateColumns: service_root_mode_ = ") +
                       (service_root_mode_ ? "true" : "false"));
  if (!service_root_mode_) {
    PredicatePushdownHelper()->ConsumeColumnSelection(visible_ids);
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     duckdb::StringUtil::Format(
                         "Select clause: %s",
                         PredicatePushdownHelper()->SelectClause().c_str()));
  } else {
    ERPL_TRACE_DEBUG(
        "ODATA_READ_BIND",
        "Service-root mode: skipping predicate pushdown helper creation");
  }
}

void ODataReadBindData::AddFilters(
    const duckdb::optional_ptr<duckdb::TableFilterSet> &filters) {
  if (service_root_mode_) {
    ERPL_TRACE_DEBUG(
        "ODATA_READ_BIND",
        "Service-root mode: ignoring filters for predicate pushdown");
    return;
  }
    if (filters && !filters->filters.empty()) {
        std::stringstream filters_str;
    for (auto &[projected_column_idx, filter] : filters->filters) {
      filters_str << "Column " << projected_column_idx << ": "
                  << filter->DebugToString() << std::endl;
    }
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     duckdb::StringUtil::Format("Adding %d filters: %s",
                                                filters->filters.size(),
                                                filters_str.str().c_str()));
        
        PredicatePushdownHelper()->ConsumeFilters(filters);
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     duckdb::StringUtil::Format(
                         "Filter clause: %s",
                         PredicatePushdownHelper()->FilterClause().c_str()));
    } else {
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "No filters to add");
    }
}

void ODataReadBindData::AddResultModifiers(
    const std::vector<duckdb::unique_ptr<duckdb::BoundResultModifier>>
        &modifiers) {
  if (service_root_mode_) {
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     "Service-root mode: ignoring result modifiers");
    return;
  }
    if (!modifiers.empty()) {
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     duckdb::StringUtil::Format("Adding %d result modifiers",
                                                modifiers.size()));
        PredicatePushdownHelper()->ConsumeResultModifiers(modifiers);
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Result modifiers processed");
    } else {
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "No result modifiers to add");
    }
}

void ODataReadBindData::UpdateUrlFromPredicatePushdown() {
  if (service_root_mode_) {
    ERPL_TRACE_DEBUG(
        "ODATA_READ_BIND",
        "Service-root mode: skipping URL update from predicate pushdown");
    return;
  }
    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Updating URL from predicate pushdown");
    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Original URL: " + odata_client->Url());
    
    auto http_client = odata_client->GetHttpClient();
    auto auth_params = odata_client->AuthParams();
    auto prev_url_str = odata_client->Url();
  auto updated_url =
      PredicatePushdownHelper()->ApplyFiltersToUrl(odata_client->Url());
    
    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Updated URL: " + updated_url.ToString());
    
    // Store the current OData version before creating new client
    auto current_version = odata_client->GetODataVersion();

  odata_client = std::make_shared<ODataEntitySetClient>(
      http_client, updated_url, auth_params);
    
    // Preserve the OData version to avoid metadata fetching
    if (current_version != ODataVersion::UNKNOWN) {
        odata_client->SetODataVersionDirectly(current_version);
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     duckdb::StringUtil::Format(
                         "Preserved OData version %s on new client",
                         current_version == ODataVersion::V2 ? "V2" : "V4"));
    }

  // If the finalized URL changed compared to the prefetched one, discard
  // buffered data so we don't emit unfiltered/unprojected rows. The scan init
  // will prefetch again.
    if (first_page_cached_ && prev_url_str != updated_url.ToString()) {
    ERPL_TRACE_INFO("ODATA_READ_BIND",
                    "Final URL changed after predicate pushdown; discarding "
                    "prefetched buffer and caches");
        if (row_buffer) {
            row_buffer->Clear();
            row_buffer->SetHasNextPage(false);
        }
        if (progress_tracker) {
            progress_tracker->Reset();
        }
        if (data_extractor) {
            data_extractor->ClearCache();
        }
        emitted_row_index_ = 0;
        first_page_cached_ = false;
    }
}

std::shared_ptr<ODataPredicatePushdownHelper>
ODataReadBindData::PredicatePushdownHelper() {
    if (predicate_pushdown_helper == nullptr) {
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     "Creating new predicate pushdown helper");
        
    // Use extracted column names if available, otherwise fall back to OData client
        std::vector<std::string> column_names;
        if (!extracted_column_names.empty()) {
            column_names = extracted_column_names;
      ERPL_TRACE_DEBUG(
          "ODATA_READ_BIND",
          duckdb::StringUtil::Format(
              "Using extracted column names for predicate pushdown: %d columns",
              column_names.size()));
        } else {
            column_names = odata_client->GetResultNames();
      ERPL_TRACE_DEBUG(
          "ODATA_READ_BIND",
          duckdb::StringUtil::Format("Using OData client column names for "
                                     "predicate pushdown: %d columns",
                                     column_names.size()));
    }
    predicate_pushdown_helper =
        std::make_shared<ODataPredicatePushdownHelper>(column_names);
        
        // Set the OData version for proper filter syntax generation
        auto odata_version = odata_client->GetODataVersion();
        predicate_pushdown_helper->SetODataVersion(odata_version);
    ERPL_TRACE_DEBUG(
        "ODATA_READ_BIND",
        "Set OData version " +
            std::string(odata_version == ODataVersion::V2 ? "V2" : "V4") +
            " on predicate pushdown helper");

    // Column name resolver: DuckDB passes the OUTPUT position (index within the
    // projected column list from InitGlobal). We must translate that through
    // activated_to_original_mapping to get the schema index before looking up
    // the column name, because the activated column order may differ from the
    // schema order (e.g. column_ids=[8,0] puts Country at output pos 0).
    predicate_pushdown_helper->SetColumnNameResolver([this](duckdb::column_t
                                                                output_index)
                                                         -> std::string {
      // Translate output position → full-schema index
      size_t schema_index = output_index;
      if (output_index < this->activated_to_original_mapping.size()) {
          schema_index = this->activated_to_original_mapping[output_index];
      }

      if (!this->extracted_column_names.empty()) {
        if (schema_index < this->extracted_column_names.size()) {
          return this->extracted_column_names[schema_index];
        } else {
          ERPL_TRACE_ERROR(
              "ODATA_READ_BIND",
              duckdb::StringUtil::Format("Schema column index %d (from output "
                                         "index %d) is out of bounds for "
                                         "extracted column names",
                                         (int)schema_index, (int)output_index));
          return std::string();
        }
      }
      if (this->all_result_names.empty()) {
        this->all_result_names = this->odata_client->GetResultNames();
      }
      if (schema_index < this->all_result_names.size()) {
        return this->all_result_names[schema_index];
      } else {
        ERPL_TRACE_ERROR(
            "ODATA_READ_BIND",
            duckdb::StringUtil::Format(
                "Schema column index %d (from output index %d) is out of "
                "bounds for result names",
                (int)schema_index, (int)output_index));
        return std::string();
      }
        });
    }

    return predicate_pushdown_helper;
}

double ODataReadBindData::GetProgressFraction() const {
    return progress_tracker->GetProgressFraction();
}

void ODataReadBindData::PrefetchFirstPage() {
  ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                   std::string("PrefetchFirstPage: service_root_mode_ = ") +
                       (service_root_mode_ ? "true" : "false"));
    if (first_page_cached_) {
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     "PrefetchFirstPage: first page already cached, skipping");
        return;
    }
  if (service_root_mode_) {
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     "Service-root mode: prefetch skipped (rows prebuffered)");
    first_page_cached_ = true;
    row_buffer->SetHasNextPage(false);
    return;
  }
  ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                   "PrefetchFirstPage: starting first page fetch");

    // Ensure input parameters are set on client before any request
    if (!input_parameters.empty()) {
        odata_client->SetInputParameters(input_parameters);
    }

    auto response = odata_client->Get();
    BufferFirstPageFromResponse(response);

  ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                   duckdb::StringUtil::Format(
        "PrefetchFirstPage: buffered %d rows, has_next_page=%s",
                       (int)row_buffer->Size(),
                       row_buffer->HasNextPage() ? "true" : "false"));
}

void ODataReadBindData::BufferFirstPageFromResponse(
    std::shared_ptr<ODataEntitySetResponse> response) {
    if (!response) {
        ERPL_TRACE_WARN("ODATA_READ_BIND",
                        "BufferFirstPageFromResponse: no response received");
        first_page_cached_ = true;
        row_buffer->SetHasNextPage(false);
        return;
    }

    // Extract expanded data if we have expand clauses
    if (HasExpandedData() && !data_extractor->GetExpandedDataSchema().empty()) {
        try {
            ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                             "Extracting expanded data from buffered first page");
            data_extractor->ExtractExpandedDataFromResponse(response->RawContent());
        } catch (const std::exception &e) {
            ERPL_TRACE_WARN("ODATA_READ_BIND",
                            std::string("Failed to extract expanded data from "
                                        "buffered first page: ") +
                                e.what());
        }
    }

    // Capture total count once for progress (v4 only, when available)
    if (response->GetODataVersion() == ODataVersion::V4) {
        auto total = response->Content()->TotalCount();
        if (total.has_value()) {
            progress_tracker->SetTotalCount(total.value());
            ERPL_TRACE_INFO(
                "ODATA_READ_BIND",
                duckdb::StringUtil::Format(
                    "Service reported total row count: %llu",
                    (unsigned long long)progress_tracker->GetTotalCount()));
        }
    }

    // Buffer full schema rows to keep indices stable across projections
    auto all_result_names_local = GetResultNames(true);
    auto all_result_types_local = GetResultTypes(true);

    auto page_rows = response->ToRows(all_result_names_local, all_result_types_local);
    row_buffer->AddRows(std::move(page_rows));
    row_buffer->SetHasNextPage(response->NextUrl().has_value());
    first_page_cached_ = true;
}

void ODataReadBindData::SetExtractedColumnNames(
    const std::vector<std::string> &column_names) {
    extracted_column_names = column_names;
  ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Stored " +
                                          std::to_string(column_names.size()) +
                                          " extracted column names");
}

std::string ODataReadBindData::GetOriginalColumnName(
    duckdb::column_t activated_column_index) const {
    if (activated_column_index >= activated_to_original_mapping.size()) {
    // This is not an error - it's a column that wasn't activated (not selected
    // by user) Just return empty string for columns we don't have mapping for
        return "";
    }
    
    auto original_index = activated_to_original_mapping[activated_column_index];
    
  // Always use all_result_names (EDMX-derived, no navigation properties).
  // extracted_column_names comes from JSON key order and may include nav
  // properties at lower indices, causing off-by-one mismatches against the
  // DuckDB catalog column indices which are built from EDMX only.
    if (!all_result_names.empty()) {
        if (original_index >= all_result_names.size()) {
      ERPL_TRACE_ERROR(
          "ODATA_READ_BIND",
          duckdb::StringUtil::Format(
              "Original column index %d is out of bounds for result names (%d total)",
              original_index, (int)all_result_names.size()));
            return "";
        }
        auto column_name = all_result_names[original_index];
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     duckdb::StringUtil::Format(
                         "Activated index %d maps to original index %d which "
                         "is column '%s' (from EDMX result names)",
                         (int)activated_column_index, (int)original_index,
                         column_name.c_str()));
        return column_name;
    }
    if (original_index >= extracted_column_names.size()) {
      ERPL_TRACE_ERROR(
          "ODATA_READ_BIND",
          duckdb::StringUtil::Format("Original column index %d is out of "
                                     "bounds for extracted column names (%d total)",
                                     original_index, (int)extracted_column_names.size()));
        return "";
    }
    auto column_name = extracted_column_names[original_index];
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     duckdb::StringUtil::Format(
                         "Activated index %d maps to original index %d which "
                         "is column '%s' (from JSON-extracted names, EDMX unavailable)",
                         (int)activated_column_index, (int)original_index,
                         column_name.c_str()));
    return column_name;
}

void ODataReadBindData::SetInputParameters(
    const std::map<std::string, std::string> &input_params) {
    input_parameters = input_params;
  ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Stored " +
                                          std::to_string(input_params.size()) +
                                          " input parameters");
}

const std::map<std::string, std::string> &
ODataReadBindData::GetInputParameters() const {
    return input_parameters;
}

std::shared_ptr<ODataEntitySetClient>
ODataReadBindData::GetODataClient() const {
    return odata_client;
}

void ODataReadBindData::SetExpandClause(const std::string &expand_clause) {
    this->expand_clause = expand_clause;
}

std::string ODataReadBindData::GetExpandClause() const { return expand_clause; }

void ODataReadBindData::SetExpandedDataSchema(
    const std::vector<std::string> &expand_paths) {
  if (data_extractor) {
    data_extractor->SetExpandedDataSchema(expand_paths);
    ERPL_TRACE_INFO("ODATA_READ_BIND", "Set expanded data schema with " +
                                           std::to_string(expand_paths.size()) +
                                           " navigation properties");
  } else {
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     "Skipping SetExpandedDataSchema in service root mode");
  }
}

// duplicate definition removed; implementation is at top of file to ensure
// availability before use

bool ODataReadBindData::HasExpandedData() const {
  return data_extractor && data_extractor->HasExpandedData();
}

void ODataReadBindData::UpdateExpandedColumnType(
    const std::string &expand_path, const duckdb::LogicalType &new_type) {
    // Find the expanded column in our schema and update its type
  if (HasExpandedData() && data_extractor) {
    const auto &exp_schema = data_extractor->GetExpandedDataSchema();
        for (size_t i = 0; i < exp_schema.size(); ++i) {
            if (exp_schema[i] == expand_path) {
                // Update the type in the data extractor
                data_extractor->UpdateExpandedColumnType(i, new_type);
                
                // Also update our local all_result_types if this column exists there
                if (!extracted_column_names.empty()) {
          auto it = std::find(extracted_column_names.begin(),
                              extracted_column_names.end(), expand_path);
                    if (it != extracted_column_names.end()) {
                        size_t col_idx = std::distance(extracted_column_names.begin(), it);
                        if (col_idx < all_result_types.size()) {
                            all_result_types[col_idx] = new_type;
              ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                               "Updated all_result_types[" +
                                   std::to_string(col_idx) + "] for '" +
                                   expand_path + "' to " + new_type.ToString());
                        }
                    }
                }
                break;
            }
        }
    }
}

// -------------------------------------------------------------------------------------------------

static std::shared_ptr<HttpAuthParams>
AuthParamsFromInput(duckdb::ClientContext &context,
                    TableFunctionBindInput &input) {
    auto args = input.inputs;
    auto url = args[0].ToString();
    return HttpAuthParams::FromDuckDbSecrets(context, url);
}

// ============================================================================
// Helper Functions Implementation
// ============================================================================
namespace ODataReadBindHelpers {

void ProcessExpandClause(ODataReadBindData *bind_data,
                         const std::string &expand_clause) {
  if (expand_clause.empty()) {
    return;
  }

  ERPL_TRACE_DEBUG("ODATA_BIND",
                   std::string("Processing expand clause: ") + expand_clause);
  bind_data->PredicatePushdownHelper()->ConsumeExpand(expand_clause);

  // Parse the expand clause to set up expanded data schema
  auto expand_paths = ODataExpandParser::ParseExpandClause(expand_clause);
  std::vector<std::string> navigation_properties;
  std::vector<std::string> nested_full_paths;

  for (const auto &path : expand_paths) {
    navigation_properties.push_back(path.navigation_property);
    // Capture nested full expand paths for recursive inference
    for (const auto &sub : path.sub_expands) {
      nested_full_paths.push_back(path.navigation_property + "/" + sub);
    }
    // Fallback: parse nested expands from options if not captured
    if (path.sub_expands.empty()) {
      const std::string &raw = path.full_expand_path;
      size_t lp = raw.find('(');
      size_t rp = raw.rfind(')');
      if (lp != std::string::npos && rp != std::string::npos && rp > lp) {
        std::string opts = raw.substr(lp + 1, rp - lp - 1);
        size_t k = opts.find("$expand=");
        if (k != std::string::npos) {
          std::string subs = opts.substr(k + 8);
          size_t semi = subs.find(';');
          if (semi != std::string::npos)
            subs = subs.substr(0, semi);
          std::stringstream ss(subs);
          std::string item;
          while (std::getline(ss, item, ',')) {
            // trim
            item.erase(0, item.find_first_not_of(" \t\r\n"));
            item.erase(item.find_last_not_of(" \t\r\n") + 1);
            if (!item.empty()) {
              nested_full_paths.push_back(path.navigation_property + "/" +
                                          item);
            }
          }
        }
      }
    }
  }

  bind_data->SetExpandedDataSchema(navigation_properties);
  bind_data->SetNestedExpandPaths(nested_full_paths);
}

std::string ExtractExpandClauseFromUrl(const std::string &url) {
  ERPL_TRACE_DEBUG("ODATA_BIND",
                   std::string("Processing URL for expand clause: ") + url);

  // Try standard $expand= pattern first
    size_t expand_pos = url.find("$expand=");
    if (expand_pos != std::string::npos) {
        size_t start_pos = expand_pos + 9; // length of "$expand="
        size_t end_pos = url.find('&', start_pos);
        if (end_pos == std::string::npos) {
            end_pos = url.length();
        }
    std::string expand_clause = url.substr(start_pos, end_pos - start_pos);
    ERPL_TRACE_DEBUG("ODATA_BIND", std::string("Found expand clause in URL: ") +
                                       expand_clause);
    return expand_clause;
  }

  // Try alternative expand= pattern
        expand_pos = url.find("expand=");
        if (expand_pos != std::string::npos) {
            size_t start_pos = expand_pos + 8; // length of "expand="
            size_t end_pos = url.find('&', start_pos);
            if (end_pos == std::string::npos) {
                end_pos = url.length();
            }
    std::string expand_clause = url.substr(start_pos, end_pos - start_pos);
    ERPL_TRACE_DEBUG("ODATA_BIND",
                     std::string("Found expand clause in URL (alternative): ") +
                         expand_clause);
    return expand_clause;
  }

  return "";
}

void ProcessNamedParameters(ODataReadBindData *bind_data,
                            const TableFunctionBindInput &input) {
  // Handle TOP parameter
    if (input.named_parameters.find("top") != input.named_parameters.end()) {
        auto limit_value = input.named_parameters["top"].GetValue<duckdb::idx_t>();
    ERPL_TRACE_DEBUG("ODATA_BIND",
                     duckdb::StringUtil::Format(
                         "Named parameter 'top' set to: %d", limit_value));
        bind_data->PredicatePushdownHelper()->ConsumeLimit(limit_value);
    }
    
  // Handle SKIP parameter
    if (input.named_parameters.find("skip") != input.named_parameters.end()) {
    auto offset_value =
        input.named_parameters["skip"].GetValue<duckdb::idx_t>();
    ERPL_TRACE_DEBUG("ODATA_BIND",
                     duckdb::StringUtil::Format(
                         "Named parameter 'skip' set to: %d", offset_value));
        bind_data->PredicatePushdownHelper()->ConsumeOffset(offset_value);
    }
    
  // Handle EXPAND parameter
    if (input.named_parameters.find("expand") != input.named_parameters.end()) {
    auto expand_value =
        input.named_parameters["expand"].GetValue<std::string>();
    ERPL_TRACE_DEBUG("ODATA_BIND", duckdb::StringUtil::Format(
                                       "Named parameter 'expand' set to: %s",
                                       expand_value.c_str()));
    ProcessExpandClause(bind_data, expand_value);
  }

  // Handle COUNT parameter
  if (input.named_parameters.find("count") != input.named_parameters.end()) {
      auto count_value =
          input.named_parameters["count"].GetValue<bool>();
      ERPL_TRACE_DEBUG("ODATA_BIND",
                       duckdb::StringUtil::Format(
                           "Named parameter 'count' set to: %s",
                           count_value ? "true" : "false"));
      if (count_value) {
          bind_data->PredicatePushdownHelper()->EnableInlineCount(true);
      }
  }
}

void SetupSchemaFromProbeResult(
    const ODataClientFactory::ProbeResult &probe_result,
    ODataReadBindData *bind_data, vector<LogicalType> &return_types,
    vector<string> &names) {
  if (probe_result.is_service_root) {
    names = {"name", "kind", "url"};
    return_types = {duckdb::LogicalTypeId::VARCHAR, 
                    duckdb::LogicalTypeId::VARCHAR,
                    duckdb::LogicalTypeId::VARCHAR};
  } else {
    // Use existing entity-set schema logic
    names = bind_data->GetResultNames();
    return_types = bind_data->GetResultTypes();
  }
}

} // namespace ODataReadBindHelpers

// ============================================================================
// Shared Error Handling Utilities
// ============================================================================

namespace ODataErrorHandling {

/**
 * @brief Convert runtime errors (especially HTTP errors) to user-friendly InvalidInputException
 * 
 * @param e The runtime error to convert
 * @param url The URL that caused the error
 * @param service_type Type of service ("OData" or "ODP OData") for error messages
 * @param discovery_function Name of discovery function to suggest (e.g., "sap_odata_show()" or "sap_odp_odata_show()")
 * @return InvalidInputException with user-friendly error message
 */
duckdb::InvalidInputException ConvertHttpErrorToUserFriendly(const std::runtime_error& e, 
                                                           const std::string& url,
                                                           const std::string& service_type,
                                                           const std::string& discovery_function) {
    std::string error_msg = e.what();
    
    if (error_msg.find("HTTP 404") != std::string::npos) {
        return duckdb::InvalidInputException(service_type + " service not found at URL: " + url + 
            ". Please check if the URL path is correct, especially the entity set name. " +
            "Use " + discovery_function + " to discover available entity sets.");
    } else if (error_msg.find("HTTP 401") != std::string::npos) {
        return duckdb::InvalidInputException("Authentication failed for " + service_type + " service at: " + url + 
            ". Please check your credentials in the secret.");
    } else if (error_msg.find("HTTP 403") != std::string::npos) {
        return duckdb::InvalidInputException("Access forbidden to " + service_type + " service at: " + url + 
            ". Please check if your user has permission to access this service.");
    } else if (error_msg.find("Connection failed") != std::string::npos) {
        return duckdb::InvalidInputException("Failed to connect to " + service_type + " service at: " + url + 
            ". Please check if the server is running and accessible.");
    } else {
        // Re-throw with additional context
        return duckdb::InvalidInputException("Failed to access " + service_type + " service at: " + url + 
            ". Error: " + error_msg);
    }
}

} // namespace ODataErrorHandling

duckdb::unique_ptr<FunctionData>
ODataReadBind(ClientContext &context, TableFunctionBindInput &input,
              vector<LogicalType> &return_types, vector<string> &names) {
  PostHogTelemetry::Instance().CaptureFunctionExecution("odata_read");
  auto auth_params = AuthParamsFromInput(context, input);
  auto url = input.inputs[0].GetValue<std::string>();

  ERPL_TRACE_INFO("ODATA_BIND",
                  duckdb::StringUtil::Format(
                      "Binding OData read function for URL: %s", url.c_str()));

  try {
    // Single probe to determine content type and version
    auto probe_result = ODataClientFactory::ProbeUrl(url, auth_params);

  // Create appropriate bind data based on probe result with fallback heuristic
  duckdb::unique_ptr<ODataReadBindData> bind_data;
  if (ODataReadBindData::LooksLikeServiceRootUrl(url)) {
    ERPL_TRACE_INFO("ODATA_BIND", "Routing via service-root path based on URL heuristic");
    bind_data = ODataReadBindData::FromServiceRoot(url, auth_params);
  } else {
    bind_data = ODataReadBindData::FromProbeResult(probe_result);
  }

  // Set return types and names based on content type
  ODataReadBindHelpers::SetupSchemaFromProbeResult(
      probe_result, bind_data.get(), return_types, names);

  // Handle named parameters and URL expand clause (only for entity-set mode)
  if (!probe_result.is_service_root) {
    // Process named parameters (top, skip, expand)
    ODataReadBindHelpers::ProcessNamedParameters(bind_data.get(), input);

    // Process expand clause from URL if present
    auto url_expand_clause =
        ODataReadBindHelpers::ExtractExpandClauseFromUrl(url);
    if (!url_expand_clause.empty()) {
      ERPL_TRACE_DEBUG("ODATA_BIND",
                       std::string("Processing expand clause from URL: ") +
                           url_expand_clause);
      ODataReadBindHelpers::ProcessExpandClause(bind_data.get(),
                                                url_expand_clause);
    }

    // Update names and types after processing expand clauses
    names = bind_data->GetResultNames();
    return_types = bind_data->GetResultTypes();
  }

  ERPL_TRACE_INFO("ODATA_BIND",
                  duckdb::StringUtil::Format("Bound function with %d columns",
                                             return_types.size()));
    
    // More efficient string concatenation for debug logging
    if (!names.empty()) {
        std::string column_names;
    column_names.reserve(names.size() *
                         20); // Estimate average column name length
        for (size_t i = 0; i < names.size(); ++i) {
      if (i > 0)
        column_names += ", ";
            column_names += names[i];
        }
    ERPL_TRACE_DEBUG(
        "ODATA_BIND",
        duckdb::StringUtil::Format("Column names: %s", column_names.c_str()));
    }
    
    return std::move(bind_data);
    
  } catch (const duckdb::InvalidInputException& e) {
    // Re-throw DuckDB exceptions as-is (these are already well-formatted)
    throw;
  } catch (const std::runtime_error& e) {
    // Use shared error handling utility
    throw ODataErrorHandling::ConvertHttpErrorToUserFriendly(e, url, "OData", "sap_odata_show()");
  } catch (const std::exception& e) {
    // Handle other exceptions with context
    throw duckdb::InvalidInputException("Failed to bind OData function for URL: " + url + 
      ". Error: " + std::string(e.what()));
  }
}

unique_ptr<GlobalTableFunctionState>
ODataReadTableInitGlobalState(ClientContext &context,
                              TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->CastNoConst<ODataReadBindData>();
    auto column_ids = input.column_ids;

    bind_data.ActivateColumns(column_ids);
    bind_data.AddFilters(input.filters);
    
    bind_data.UpdateUrlFromPredicatePushdown();
  // Prefetch first page after URL is finalized so progress can show early and
  // tiny scans return immediately
    bind_data.PrefetchFirstPage();

    return duckdb::make_uniq<GlobalTableFunctionState>();
}

double ODataReadTableProgress(ClientContext &, const FunctionData *func_data,
                              const GlobalTableFunctionState *) {
    auto &bind_data = func_data->CastNoConst<ODataReadBindData>();
    return bind_data.GetProgressFraction();
}

void ODataReadScan(ClientContext &context, TableFunctionInput &data,
                   DataChunk &output) {
    auto &bind_data = data.bind_data->CastNoConst<ODataReadBindData>();
    
    ERPL_TRACE_DEBUG("ODATA_SCAN", "Starting OData scan operation");
    
  if (!bind_data.HasMoreResults()) {
        ERPL_TRACE_DEBUG("ODATA_SCAN", "No more results available");
        return;
    }
    
    ERPL_TRACE_DEBUG("ODATA_SCAN", "Fetching next result set");
    auto rows_fetched = bind_data.FetchNextResult(output);
  ERPL_TRACE_INFO("ODATA_SCAN",
                  duckdb::StringUtil::Format("Fetched %d rows", rows_fetched));
}

TableFunctionSet CreateODataReadFunction() {
    TableFunctionSet function_set("odata_read");
    
    TableFunction read_entity_set({LogicalTypeId::VARCHAR}, ODataReadScan, ODataReadBind, ODataReadTableInitGlobalState);
    read_entity_set.filter_pushdown = true;
    read_entity_set.projection_pushdown = true;
    read_entity_set.table_scan_progress = ODataReadTableProgress;
    
    // Add named parameters for TOP, SKIP, EXPAND, and COUNT
    read_entity_set.named_parameters["top"] = LogicalTypeId::UBIGINT;
    read_entity_set.named_parameters["skip"] = LogicalTypeId::UBIGINT;
    read_entity_set.named_parameters["expand"] = LogicalTypeId::VARCHAR;
    read_entity_set.named_parameters["count"] = LogicalTypeId::BOOLEAN;

    function_set.AddFunction(read_entity_set);
    return function_set;
}


} // namespace erpl_web
