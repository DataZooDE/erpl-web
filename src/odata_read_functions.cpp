#include "duckdb/function/table_function.hpp"

#include "http_client.hpp"
#include "odata_edm.hpp"
#include "odata_expand_parser.hpp"
#include "odata_read_functions.hpp"
#include "odata_url_helpers.hpp"
#include "yyjson.hpp"

#include "tracing.hpp"
#include <set>
#include <unordered_map>

namespace erpl_web {
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
    
  auto doc =
      duckdb_yyjson::yyjson_read(json_content.c_str(), json_content.size(), 0);
    if (!doc) {
        return column_names;
    }
    
    auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
    if (!root) {
        duckdb_yyjson::yyjson_doc_free(doc);
        return column_names;
    }
    
    // Try OData V4 format first
    auto value_arr = duckdb_yyjson::yyjson_obj_get(root, "value");
    if (value_arr && duckdb_yyjson::yyjson_is_arr(value_arr)) {
        // Get first row to extract column names
        auto first_row = duckdb_yyjson::yyjson_arr_get_first(value_arr);
        if (first_row && duckdb_yyjson::yyjson_is_obj(first_row)) {
            duckdb_yyjson::yyjson_obj_iter iter;
            duckdb_yyjson::yyjson_obj_iter_init(first_row, &iter);
      duckdb_yyjson::yyjson_val *key;
            while ((key = duckdb_yyjson::yyjson_obj_iter_next(&iter))) {
                if (duckdb_yyjson::yyjson_is_str(key)) {
                    column_names.push_back(duckdb_yyjson::yyjson_get_str(key));
                }
            }
        }
    } else {
        // Try OData V2 format
        auto data_obj = duckdb_yyjson::yyjson_obj_get(root, "d");
        if (data_obj && duckdb_yyjson::yyjson_is_obj(data_obj)) {
            auto results_arr = duckdb_yyjson::yyjson_obj_get(data_obj, "results");
            if (results_arr && duckdb_yyjson::yyjson_is_arr(results_arr)) {
                auto first_row = duckdb_yyjson::yyjson_arr_get_first(results_arr);
                if (first_row && duckdb_yyjson::yyjson_is_obj(first_row)) {
                    duckdb_yyjson::yyjson_obj_iter iter;
                    duckdb_yyjson::yyjson_obj_iter_init(first_row, &iter);
          duckdb_yyjson::yyjson_val *key;
                    while ((key = duckdb_yyjson::yyjson_obj_iter_next(&iter))) {
                        if (duckdb_yyjson::yyjson_is_str(key)) {
                            column_names.push_back(duckdb_yyjson::yyjson_get_str(key));
                        }
                    }
                }
            } else {
        ERPL_TRACE_WARN("ODATA_READ_BIND",
                        "Could not find 'results' array in OData v2 response");
            }
        } else {
      ERPL_TRACE_WARN("ODATA_READ_BIND",
                      "Could not find 'd' object in OData v2 response");
        }
    }
    
    duckdb_yyjson::yyjson_doc_free(doc);
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

void ODataRowBuffer::AddRows(
    const std::vector<std::vector<duckdb::Value>> &rows) {
  for (const auto &row : rows) {
        row_buffer_.emplace_back(row);
    }
}

std::vector<duckdb::Value> ODataRowBuffer::GetNextRow() {
    if (row_buffer_.empty()) {
        return {};
    }
    
    auto row = row_buffer_.front();
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
    if (type_name == "Edm.Binary") {
        return duckdb::LogicalTypeId::BLOB;
    } else if (type_name == "Edm.Boolean") {
        return duckdb::LogicalTypeId::BOOLEAN;
    } else if (type_name == "Edm.Byte" || type_name == "Edm.SByte") {
        return duckdb::LogicalTypeId::TINYINT;
    } else if (type_name == "Edm.Date") {
        return duckdb::LogicalTypeId::DATE;
    } else if (type_name == "Edm.DateTime" || type_name == "Edm.DateTimeOffset") {
        return duckdb::LogicalTypeId::TIMESTAMP;
    } else if (type_name == "Edm.Decimal") {
        return duckdb::LogicalTypeId::DECIMAL;
    } else if (type_name == "Edm.Double") {
        return duckdb::LogicalTypeId::DOUBLE;
    } else if (type_name == "Edm.Duration") {
        return duckdb::LogicalTypeId::INTERVAL;
    } else if (type_name == "Edm.Guid") {
        return duckdb::LogicalTypeId::VARCHAR;
    } else if (type_name == "Edm.Int16") {
        return duckdb::LogicalTypeId::SMALLINT;
    } else if (type_name == "Edm.Int32") {
        return duckdb::LogicalTypeId::INTEGER;
    } else if (type_name == "Edm.Int64") {
        return duckdb::LogicalTypeId::BIGINT;
    } else if (type_name == "Edm.Single") {
        return duckdb::LogicalTypeId::FLOAT;
    } else if (type_name == "Edm.Stream") {
        return duckdb::LogicalTypeId::BLOB;
    } else if (type_name == "Edm.String") {
        return duckdb::LogicalTypeId::VARCHAR;
    } else if (type_name == "Edm.TimeOfDay") {
        return duckdb::LogicalTypeId::TIME;
  } else if (type_name.find("Edm.Geography") == 0 ||
             type_name.find("Edm.Geometry") == 0) {
    return duckdb::LogicalTypeId::VARCHAR; // Geography/Geometry types as
                                           // VARCHAR for now
    } else {
        // Fallback for unknown types - treat as VARCHAR
        return duckdb::LogicalTypeId::VARCHAR;
    }
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
// ODataDataExtractor Implementation
// ============================================================================

ODataDataExtractor::ODataDataExtractor(
    std::shared_ptr<ODataEntitySetClient> odata_client)
    : odata_client(odata_client) {
    type_resolver = std::make_shared<ODataTypeResolver>(odata_client);
}

void ODataDataExtractor::SetExpandedDataSchema(
    const std::vector<std::string> &expand_paths) {
    expanded_data_schema.clear();
    expanded_data_types.clear();
    this->expand_paths.clear();
    expanded_data_cache.clear();
    
    // Determine proper types for expanded columns using EDM metadata
    expanded_data_types.reserve(expand_paths.size());
    
  for (const auto &expand_path : expand_paths) {
    duckdb::LogicalType column_type =
        type_resolver->ResolveNavigationPropertyType(expand_path);
        
        // Check if type resolution failed (likely V2 without proper metadata)
        if (column_type.id() == duckdb::LogicalTypeId::VARCHAR) {
            // For V2 expands, we know the structure: { "results": [...] }
            // Since we can't determine the exact entity structure from metadata,
      // we'll use LIST(VARCHAR) as a safe fallback that can hold the JSON
      // strings This allows the expand to work while preserving the data
      // structure
            column_type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
      ERPL_TRACE_INFO(
          "DATA_EXTRACTOR",
          "Using V2 fallback type LIST(VARCHAR) for expanded column '" +
              expand_path + "' (EDM metadata unavailable)");
    }

    // If there are nested paths such as "DefaultSystem/Services", and this is
    // the top-level (e.g., "DefaultSystem"), augment the STRUCT type to include
    // a field for each nested navigation as an embedded list/struct, without
    // creating new top-level columns.
    if (!nested_expand_paths.empty()) {
      // Collect immediate nested props for this top-level path
      std::vector<std::string> nested_for_top;
      for (const auto &nested : nested_expand_paths) {
        if (nested.find(expand_path + "/") == 0) {
          auto rest = nested.substr(expand_path.size() + 1);
          auto slash = rest.find('/');
          std::string child =
              (slash == std::string::npos) ? rest : rest.substr(0, slash);
          if (!child.empty())
            nested_for_top.push_back(child);
        }
      }
      if (!nested_for_top.empty()) {
        // Delegate to EDM type builder for augmentation
        try {
          auto edmx_opt = EdmCache::GetInstance().Get(
              odata_client->GetMetadataContextUrl());
          if (edmx_opt) {
            ODataEdmTypeBuilder builder(*edmx_opt.get());
            // Resolve root entity type name from client
            auto root_entity = odata_client->GetCurrentEntityType().name;
            auto built = builder.BuildExpandedColumnType(
                root_entity, expand_path, nested_for_top);
            if (built.id() != duckdb::LogicalTypeId::INVALID) {
              column_type = built;
            }
          }
        } catch (...) {
          // keep previous column_type on failure
        }
      }
        }
        
        expanded_data_schema.push_back(expand_path);
        expanded_data_types.push_back(column_type);
        this->expand_paths.push_back(expand_path);
        
    ERPL_TRACE_INFO("DATA_EXTRACTOR", "Resolved expanded column type for '" +
                                          expand_path +
                                          "': " + column_type.ToString());
  }

  ERPL_TRACE_INFO("DATA_EXTRACTOR",
                  "Set expanded data schema with " +
                      std::to_string(expand_paths.size()) +
                      " navigation properties and proper types");
}

void ODataDataExtractor::SetNestedExpandPaths(
    const std::vector<std::string> &nested_paths) {
  nested_expand_paths = nested_paths;
}

void ODataDataExtractor::UpdateExpandedColumnType(
    size_t index, const duckdb::LogicalType &new_type) {
    if (index < expanded_data_types.size()) {
    ERPL_TRACE_DEBUG("DATA_EXTRACTOR",
                     "Updating expanded column type at index " +
                         std::to_string(index) + " from " +
                         expanded_data_types[index].ToString() + " to " +
                         new_type.ToString());
        expanded_data_types[index] = new_type;
    } else {
    ERPL_TRACE_WARN(
        "DATA_EXTRACTOR",
        "Attempted to update expanded column type at invalid index " +
            std::to_string(index));
    }
}

std::vector<std::string> ODataDataExtractor::GetExpandedDataSchema() const {
    return expanded_data_schema;
}

std::vector<duckdb::LogicalType>
ODataDataExtractor::GetExpandedDataTypes() const {
    return expanded_data_types;
}

std::vector<std::string> ODataDataExtractor::GetNestedExpandPaths() const {
  return nested_expand_paths;
}

bool ODataDataExtractor::HasExpandedData() const {
    return !expanded_data_schema.empty();
}

void ODataDataExtractor::ExtractExpandedDataFromResponse(
    const std::string &response_content) {
    if (!HasExpandedData() || expand_paths.empty()) {
        return;
    }
    
  ERPL_TRACE_DEBUG("DATA_EXTRACTOR",
                   "Extracting expanded data from response of " +
                       std::to_string(response_content.size()) + " bytes");
    
    // Parse the JSON response
  auto doc = duckdb_yyjson::yyjson_read(response_content.c_str(),
                                        response_content.size(), 0);
    if (!doc) {
    ERPL_TRACE_WARN(
        "DATA_EXTRACTOR",
        "Failed to parse JSON response for expanded data extraction");
        return;
    }
    
    auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
    
    try {
        // Handle OData V4 format
        auto value_arr = duckdb_yyjson::yyjson_obj_get(root, "value");
        if (value_arr && duckdb_yyjson::yyjson_is_arr(value_arr)) {
      ERPL_TRACE_DEBUG("DATA_EXTRACTOR",
                       "Processing OData V4 response format for expanded data");
            ProcessODataV4ExpandedData(value_arr);
        } else {
            // Handle OData V2 format
            auto data_obj = duckdb_yyjson::yyjson_obj_get(root, "d");
            if (data_obj && duckdb_yyjson::yyjson_is_obj(data_obj)) {
                auto results_arr = duckdb_yyjson::yyjson_obj_get(data_obj, "results");
                if (results_arr && duckdb_yyjson::yyjson_is_arr(results_arr)) {
          ERPL_TRACE_DEBUG(
              "DATA_EXTRACTOR",
              "Processing OData V2 response format for expanded data");
                    ProcessODataV2ExpandedData(results_arr);
                }
            }
        }
  } catch (const std::exception &e) {
    ERPL_TRACE_WARN("DATA_EXTRACTOR",
                    "Error processing expanded data: " + std::string(e.what()));
    }
    
    duckdb_yyjson::yyjson_doc_free(doc);
}

duckdb::Value
ODataDataExtractor::ExtractExpandedDataForRow(const std::string &row_id,
                                              const std::string &expand_path) {
    auto it = expanded_data_cache.find(expand_path);
    if (it != expanded_data_cache.end()) {
        // Return value for this row index if available
        try {
            size_t row_index = static_cast<size_t>(std::stoull(row_id));
            if (row_index < it->second.size()) {
                return it->second[row_index];
            }
        } catch (...) {
            // ignore parse errors, fall through to NULL
        }
    }
    return duckdb::Value();
}

void ODataDataExtractor::ProcessODataV4ExpandedData(
    duckdb_yyjson::yyjson_val *value_arr) {
  ERPL_TRACE_INFO("DATA_EXTRACTOR", "ProcessODataV4ExpandedData called with " +
                                        std::to_string(expand_paths.size()) +
                                        " expand paths");
    
    duckdb_yyjson::yyjson_arr_iter arr_it;
    duckdb_yyjson::yyjson_arr_iter_init(value_arr, &arr_it);
    
  duckdb_yyjson::yyjson_val *row;
    size_t row_index = 0;
    while ((row = duckdb_yyjson::yyjson_arr_iter_next(&arr_it))) {
        if (duckdb_yyjson::yyjson_is_obj(row)) {
      for (const auto &expand_path : expand_paths) {
        auto expand_data =
            duckdb_yyjson::yyjson_obj_get(row, expand_path.c_str());
                if (expand_data) {
                    try {
            duckdb::LogicalType target_type =
                duckdb::LogicalTypeId::VARCHAR; // Default fallback
                        for (size_t i = 0; i < expanded_data_schema.size(); ++i) {
                            if (expanded_data_schema[i] == expand_path) {
                                target_type = expanded_data_types[i];
                                break;
                            }
                        }
            // Recursively parse nested expands for V4 similarly to V2 flow
            // Ensure nested fields inside the parent struct (e.g., Services
            // list) are preserved
            duckdb::Value parsed_value = ParseExpandedDataRecursively(
                expand_data, expand_path, target_type);
                            expanded_data_cache[expand_path].push_back(parsed_value);
          } catch (const std::exception &e) {
            ERPL_TRACE_WARN("DATA_EXTRACTOR",
                            "Failed to parse expand data for path '" +
                                expand_path + "': " + e.what());
                        expanded_data_cache[expand_path].push_back(duckdb::Value());
                    }
                } else {
                    // Keep alignment
                    expanded_data_cache[expand_path].push_back(duckdb::Value());
                }
            }
        }
        row_index++;
    }
    
  ERPL_TRACE_INFO("DATA_EXTRACTOR",
                  "ProcessODataV4ExpandedData completed, processed " +
                      std::to_string(row_index) + " rows");
}

void ODataDataExtractor::ProcessODataV2ExpandedData(
    duckdb_yyjson::yyjson_val *results_arr) {
    duckdb_yyjson::yyjson_arr_iter arr_it;
    duckdb_yyjson::yyjson_arr_iter_init(results_arr, &arr_it);
    
  duckdb_yyjson::yyjson_val *row;
    size_t row_index = 0;
    while ((row = duckdb_yyjson::yyjson_arr_iter_next(&arr_it))) {
        if (duckdb_yyjson::yyjson_is_obj(row)) {
      for (const auto &expand_path : expand_paths) {
        auto expand_data =
            duckdb_yyjson::yyjson_obj_get(row, expand_path.c_str());
                if (expand_data) {
                    try {
                        // Find the corresponding type for this expand path
            duckdb::LogicalType target_type =
                duckdb::LogicalTypeId::VARCHAR; // Default fallback
                        for (size_t i = 0; i < expanded_data_schema.size(); ++i) {
                            if (expanded_data_schema[i] == expand_path) {
                                target_type = expanded_data_types[i];
                                break;
                            }
                        }
                        
                        // Parse the expanded data recursively, handling nested expands
            duckdb::Value parsed_value = ParseExpandedDataRecursively(
                expand_data, expand_path, target_type);
                        
                        expanded_data_cache[expand_path].push_back(parsed_value);
            ERPL_TRACE_DEBUG("DATA_EXTRACTOR",
                             "Extracted and parsed expanded data for path '" +
                                 expand_path + "' at row " +
                                 std::to_string(row_index));
          } catch (const std::exception &e) {
            ERPL_TRACE_WARN("DATA_EXTRACTOR",
                            "Failed to parse expand data for path '" +
                                expand_path + "': " + e.what());
                        expanded_data_cache[expand_path].push_back(duckdb::Value());
                    }
                } else {
          // No expanded data for this path at this row, add NULL value to keep
          // row alignment
                    expanded_data_cache[expand_path].push_back(duckdb::Value());
                }
            }
        }
        row_index++;
    }
}

// Parse expanded data recursively, handling nested expands like
// Products/Suppliers
duckdb::Value ODataDataExtractor::ParseExpandedDataRecursively(
    duckdb_yyjson::yyjson_val *expand_data, const std::string &expand_path,
    const duckdb::LogicalType &target_type) {
    if (!expand_data) {
        return duckdb::Value();
    }
    
    // OData V2 often wraps expanded data in a 'results' array
  duckdb_yyjson::yyjson_val *payload = expand_data;
    if (duckdb_yyjson::yyjson_is_obj(expand_data)) {
        auto results_field = duckdb_yyjson::yyjson_obj_get(expand_data, "results");
        if (results_field && duckdb_yyjson::yyjson_is_arr(results_field)) {
            payload = results_field;
        }
    }
    
    // Handle LIST types - for OData V2, we need to infer the proper struct type
    if (target_type.id() == duckdb::LogicalTypeId::LIST) {
        if (duckdb_yyjson::yyjson_is_arr(payload)) {
            // For OData V2, we need to infer the struct type from the first item
            // and then parse each item as that struct type
            auto child_type = duckdb::ListType::GetChildType(target_type);
            
      // If the child type is still VARCHAR (fallback), infer the actual struct
      // type
            if (child_type.id() == duckdb::LogicalTypeId::VARCHAR) {
                // Get the first item to infer the struct type
                duckdb_yyjson::yyjson_arr_iter sample_it;
                duckdb_yyjson::yyjson_arr_iter_init(payload, &sample_it);
                auto first_item = duckdb_yyjson::yyjson_arr_iter_next(&sample_it);
                
                if (first_item && duckdb_yyjson::yyjson_is_obj(first_item)) {
                    // Infer struct type from the first item, handling nested expands
          child_type = InferStructTypeFromJsonObjectWithNestedExpands(
              first_item, expand_path);
                    // Update the target type with the inferred child type
                    auto new_target_type = duckdb::LogicalType::LIST(child_type);
                    
                    // Update the stored type for future use
                    for (size_t i = 0; i < expanded_data_types.size(); ++i) {
                        if (expanded_data_types[i].id() == duckdb::LogicalTypeId::LIST && 
                duckdb::ListType::GetChildType(expanded_data_types[i]).id() ==
                    duckdb::LogicalTypeId::VARCHAR) {
                            expanded_data_types[i] = new_target_type;
                            break;
                        }
                    }
                    
          ERPL_TRACE_DEBUG("DATA_EXTRACTOR", "Inferred struct type for '" +
                                                 expand_path +
                                                 "': " + child_type.ToString());
                    
                    // Now parse the array with the proper child type
                    return ParseJsonArray(payload, new_target_type);
                }
            }
            
            // Now parse the array with the proper child type
            return ParseJsonArray(payload, target_type);
        } else {
            // If payload is not an array, create an empty list
      return duckdb::Value::LIST(duckdb::ListType::GetChildType(target_type),
                                 {});
        }
    } else {
        // For non-LIST types, convert to JSON string and parse
    char *json_str = duckdb_yyjson::yyjson_val_write(payload, 0, nullptr);
        if (json_str) {
            std::string json_string(json_str);
            free(json_str);
            return ParseJsonToDuckDBValue(json_string, target_type);
        } else {
            return duckdb::Value();
        }
    }
}

duckdb::Value ODataDataExtractor::ParseJsonToDuckDBValue(
    const std::string &json_str, const duckdb::LogicalType &target_type) {
    if (json_str.empty()) {
        return duckdb::Value();
    }
    
    // Parse the JSON string
    auto doc = duckdb_yyjson::yyjson_read(json_str.c_str(), json_str.length(), 0);
    if (!doc) {
        throw std::runtime_error("Failed to parse JSON string");
    }
    
    auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
    if (!root) {
        duckdb_yyjson::yyjson_doc_free(doc);
        throw std::runtime_error("Failed to get JSON root");
    }
    
    try {
        auto result = ParseJsonValueToDuckDBValue(root, target_type);
        duckdb_yyjson::yyjson_doc_free(doc);
        return result;
    } catch (...) {
        duckdb_yyjson::yyjson_doc_free(doc);
        throw;
    }
}

duckdb::Value ODataDataExtractor::ParseJsonValueToDuckDBValue(
    duckdb_yyjson::yyjson_val *value, const duckdb::LogicalType &target_type) {
    if (!value) {
        ERPL_TRACE_WARN("DATA_EXTRACTOR", "Null JSON value passed to parser");
        return duckdb::Value();
    }
    
    // Handle NULL values
    if (duckdb_yyjson::yyjson_is_null(value)) {
        return duckdb::Value();
    }
    
    try {
    // Delegate complex types to small helpers for readability and reuse
        if (target_type.id() == duckdb::LogicalTypeId::LIST) {
      return ConvertList(value, target_type);
    }

        if (target_type.id() == duckdb::LogicalTypeId::STRUCT) {
      return ConvertStruct(value, target_type);
        }
        
        // Enhanced primitive type handling with robust fallbacks
        if (target_type.id() == duckdb::LogicalTypeId::VARCHAR) {
            if (duckdb_yyjson::yyjson_is_str(value)) {
                std::string s = duckdb_yyjson::yyjson_get_str(value);
                // Normalize OData V2 legacy date format /Date(ms[+/-HHMM])/
        if (s.size() >= 8 && s.rfind("/Date(", 0) == 0 &&
            s.substr(s.size() - 2) == ")/") {
                    std::string inner = s.substr(6, s.size() - 8);
                    size_t pos = inner.find_first_of("+-", 1);
          std::string ms_str =
              (pos == std::string::npos) ? inner : inner.substr(0, pos);
                    try {
                        long long ms = std::stoll(ms_str);
                        long long sec = ms / 1000;
            auto ts_val = duckdb::Value::TIMESTAMP(
                duckdb::Timestamp::FromEpochSeconds(sec));
                        return ts_val.DefaultCastAs(duckdb::LogicalType::VARCHAR);
                    } catch (...) {
                        // fall through to raw string
                    }
                }
                return duckdb::Value(s);
            } else {
                // Convert any type to string for VARCHAR
        char *json_str = duckdb_yyjson::yyjson_val_write(value, 0, nullptr);
                if (json_str) {
                    std::string result(json_str);
                    free(json_str);
                    return duckdb::Value(result);
                }
                return duckdb::Value("");
            }
        } else if (target_type.id() == duckdb::LogicalTypeId::INTEGER) {
            if (duckdb_yyjson::yyjson_is_int(value)) {
        return duckdb::Value(
            static_cast<int32_t>(duckdb_yyjson::yyjson_get_int(value)));
            } else if (duckdb_yyjson::yyjson_is_uint(value)) {
                uint64_t uval = duckdb_yyjson::yyjson_get_uint(value);
                if (uval <= static_cast<uint64_t>(INT32_MAX)) {
                    return duckdb::Value(static_cast<int32_t>(uval));
                } else {
                    return CreateFallbackValue(target_type);
                }
            } else if (duckdb_yyjson::yyjson_is_str(value)) {
                // Enhanced string-to-integer conversion with error handling
                try {
                    std::string str_val = duckdb_yyjson::yyjson_get_str(value);
                    return duckdb::Value(std::stoi(str_val));
        } catch (const std::exception &e) {
          LogError("INTEGER_CONVERSION",
                   "Failed to convert string '" +
                       std::string(duckdb_yyjson::yyjson_get_str(value)) +
                       "' to integer: " + e.what());
                    return CreateFallbackValue(target_type);
                }
            }
        } else if (target_type.id() == duckdb::LogicalTypeId::BIGINT) {
            if (duckdb_yyjson::yyjson_is_int(value)) {
                return duckdb::Value(duckdb_yyjson::yyjson_get_int(value));
            } else if (duckdb_yyjson::yyjson_is_uint(value)) {
        return duckdb::Value(
            static_cast<int64_t>(duckdb_yyjson::yyjson_get_uint(value)));
            } else if (duckdb_yyjson::yyjson_is_str(value)) {
                // Enhanced string-to-bigint conversion with error handling
                try {
                    std::string str_val = duckdb_yyjson::yyjson_get_str(value);
                    return duckdb::Value(static_cast<int64_t>(std::stoll(str_val)));
        } catch (const std::exception &e) {
          LogError("BIGINT_CONVERSION",
                   "Failed to convert string '" +
                       std::string(duckdb_yyjson::yyjson_get_str(value)) +
                       "' to bigint: " + e.what());
                    return CreateFallbackValue(target_type);
                }
            }
    } else if (target_type.id() == duckdb::LogicalTypeId::FLOAT ||
               target_type.id() == duckdb::LogicalTypeId::DOUBLE) {
            if (duckdb_yyjson::yyjson_is_real(value)) {
                return duckdb::Value(duckdb_yyjson::yyjson_get_real(value));
            } else if (duckdb_yyjson::yyjson_is_int(value)) {
        return duckdb::Value(
            static_cast<double>(duckdb_yyjson::yyjson_get_int(value)));
            } else if (duckdb_yyjson::yyjson_is_uint(value)) {
        return duckdb::Value(
            static_cast<double>(duckdb_yyjson::yyjson_get_uint(value)));
            } else if (duckdb_yyjson::yyjson_is_str(value)) {
                // Enhanced string-to-float conversion with error handling
                try {
                    std::string str_val = duckdb_yyjson::yyjson_get_str(value);
                    return duckdb::Value(std::stod(str_val));
        } catch (const std::exception &e) {
          LogError("FLOAT_CONVERSION",
                   "Failed to convert string '" +
                       std::string(duckdb_yyjson::yyjson_get_str(value)) +
                       "' to float: " + e.what());
                    return CreateFallbackValue(target_type);
                }
            }
        } else if (target_type.id() == duckdb::LogicalTypeId::DECIMAL) {
            // Accept string/int/real and cast to DECIMAL(p,s)
            try {
                if (duckdb_yyjson::yyjson_is_str(value)) {
                    std::string s = duckdb_yyjson::yyjson_get_str(value);
                    return duckdb::Value(s).DefaultCastAs(target_type);
                } else if (duckdb_yyjson::yyjson_is_int(value)) {
                    int64_t v = duckdb_yyjson::yyjson_get_int(value);
                    return duckdb::Value::BIGINT(v).DefaultCastAs(target_type);
                } else if (duckdb_yyjson::yyjson_is_uint(value)) {
                    uint64_t v = duckdb_yyjson::yyjson_get_uint(value);
                    return duckdb::Value::UBIGINT(v).DefaultCastAs(target_type);
                } else if (duckdb_yyjson::yyjson_is_real(value)) {
                    double v = duckdb_yyjson::yyjson_get_real(value);
                    return duckdb::Value::DOUBLE(v).DefaultCastAs(target_type);
                }
            } catch (const std::exception &e) {
                LogError("DECIMAL_CONVERSION", e.what());
                return CreateFallbackValue(target_type);
            }
        } else if (target_type.id() == duckdb::LogicalTypeId::BOOLEAN) {
            if (duckdb_yyjson::yyjson_is_bool(value)) {
                return duckdb::Value(duckdb_yyjson::yyjson_get_bool(value));
            } else if (duckdb_yyjson::yyjson_is_str(value)) {
                // Enhanced string-to-boolean conversion
                std::string str_val = duckdb_yyjson::yyjson_get_str(value);
        std::transform(str_val.begin(), str_val.end(), str_val.begin(),
                       ::tolower);
        if (str_val == "true" || str_val == "1" || str_val == "yes" ||
            str_val == "on") {
                    return duckdb::Value(true);
        } else if (str_val == "false" || str_val == "0" || str_val == "no" ||
                   str_val == "off") {
                    return duckdb::Value(false);
                } else {
          LogError("BOOLEAN_CONVERSION",
                   "Failed to convert string '" + str_val + "' to boolean");
                    return CreateFallbackValue(target_type);
                }
            }
        } else if (target_type.id() == duckdb::LogicalTypeId::TIMESTAMP) {
            if (duckdb_yyjson::yyjson_is_str(value)) {
                // Parse ISO 8601 timestamp format (e.g., "2014-01-01T00:00:00Z")
                std::string timestamp_str = duckdb_yyjson::yyjson_get_str(value);
                // Handle OData V2 legacy format: /Date(ms[+/-HHMM])/ -> UTC
        if (timestamp_str.size() >= 8 &&
            timestamp_str.rfind("/Date(", 0) == 0 &&
            timestamp_str.substr(timestamp_str.size() - 2) == ")/") {
                    std::string inner = timestamp_str.substr(6, timestamp_str.size() - 8);
                    size_t pos = inner.find_first_of("+-", 1);
          std::string ms_str =
              (pos == std::string::npos) ? inner : inner.substr(0, pos);
                    try {
                        long long ms = std::stoll(ms_str);
                        long long sec = ms / 1000;
            return duckdb::Value::TIMESTAMP(
                duckdb::Timestamp::FromEpochSeconds(sec));
                    } catch (...) {
                        // fall through to ISO handling below
                    }
                }
                try {
          // Handle ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ or
          // YYYY-MM-DDTHH:MM:SS.SSSZ
                    if (timestamp_str.length() >= 19) { // Minimum: "2014-01-01T00:00:00"
                        // Replace 'T' with space and remove 'Z' for DuckDB parsing
                        std::string duckdb_format = timestamp_str.substr(0, 19);
                        std::replace(duckdb_format.begin(), duckdb_format.end(), 'T', ' ');
                        
                        // Parse using DuckDB's timestamp constructor
            auto timestamp_value = duckdb::Value::TIMESTAMP(
                duckdb::Timestamp::FromString(duckdb_format));
            ERPL_TRACE_DEBUG("DATA_EXTRACTOR",
                             "Successfully parsed timestamp: '" +
                                 timestamp_str + "' -> '" + duckdb_format +
                                 "'");
                        return timestamp_value;
                    } else {
            LogError("TIMESTAMP_CONVERSION",
                     "Timestamp string too short: '" + timestamp_str + "'");
                        return CreateFallbackValue(target_type);
                    }
        } catch (const std::exception &e) {
          LogError("TIMESTAMP_CONVERSION", "Failed to parse timestamp '" +
                                               timestamp_str +
                                               "': " + e.what());
                    return CreateFallbackValue(target_type);
                }
            } else if (duckdb_yyjson::yyjson_is_int(value)) {
                // Handle Unix timestamp (seconds since epoch)
                try {
                    int64_t unix_timestamp = duckdb_yyjson::yyjson_get_int(value);
          auto timestamp_value = duckdb::Value::TIMESTAMP(
              duckdb::Timestamp::FromEpochSeconds(unix_timestamp));
          ERPL_TRACE_DEBUG("DATA_EXTRACTOR",
                           "Successfully parsed Unix timestamp: " +
                               std::to_string(unix_timestamp));
                    return timestamp_value;
        } catch (const std::exception &e) {
          LogError("TIMESTAMP_CONVERSION",
                   std::string("Failed to parse Unix timestamp: ") + e.what());
                    return CreateFallbackValue(target_type);
                }
            }
        }
        
        // Final fallback: convert to string representation
    char *fallback_json_str =
        duckdb_yyjson::yyjson_val_write(value, 0, nullptr);
        if (fallback_json_str) {
            std::string result(fallback_json_str);
            free(fallback_json_str);
            if (target_type.id() == duckdb::LogicalTypeId::VARCHAR) {
                return duckdb::Value(result);
            } else {
                try {
                    return duckdb::Value(result).DefaultCastAs(target_type);
                } catch (...) {
                    return CreateFallbackValue(target_type);
                }
            }
        }
        
        return CreateFallbackValue(target_type);
        
  } catch (const std::exception &e) {
    LogError("JSON_PARSING",
             "Unexpected error parsing JSON value: " + std::string(e.what()));
        return CreateFallbackValue(target_type);
    }
}

// Small modular helpers for complex types
duckdb::Value ODataDataExtractor::ConvertList(duckdb_yyjson::yyjson_val *value,
                                              const duckdb::LogicalType &target_type) {
    if (duckdb_yyjson::yyjson_is_arr(value)) {
        return ParseJsonArray(value, target_type);
    }
    ERPL_TRACE_WARN("DATA_EXTRACTOR",
                    "Expected array for LIST type but got different JSON type");
    return CreateFallbackValue(target_type);
}

duckdb::Value
ODataDataExtractor::ConvertStruct(duckdb_yyjson::yyjson_val *value,
                                  const duckdb::LogicalType &target_type) {
    if (duckdb_yyjson::yyjson_is_obj(value)) {
        return ParseJsonObject(value, target_type);
    }
    ERPL_TRACE_WARN("DATA_EXTRACTOR",
                    "Expected object for STRUCT type but got different JSON type");
    return CreateFallbackValue(target_type);
}

// Enhanced parsing methods for better type resolution
duckdb::Value
ODataDataExtractor::ParseJsonArray(duckdb_yyjson::yyjson_val *array_val,
                                   const duckdb::LogicalType &target_type) {
    auto child_type = duckdb::ListType::GetChildType(target_type);
    duckdb::vector<duckdb::Value> list_values;
    
    try {
        duckdb_yyjson::yyjson_arr_iter arr_it;
        duckdb_yyjson::yyjson_arr_iter_init(array_val, &arr_it);
        
    duckdb_yyjson::yyjson_val *item;
        size_t item_count = 0;
        while ((item = duckdb_yyjson::yyjson_arr_iter_next(&arr_it))) {
            if (item_count >= batch_size_) {
        ERPL_TRACE_WARN("DATA_EXTRACTOR", "Array too large, truncating at " +
                                              std::to_string(batch_size_) +
                                              " items");
                break;
            }
            
            list_values.push_back(ParseJsonValueToDuckDBValue(item, child_type));
            item_count++;
        }
        
        return duckdb::Value::LIST(child_type, list_values);
        
  } catch (const std::exception &e) {
    LogError("ARRAY_PARSING",
             "Failed to parse JSON array: " + std::string(e.what()));
        return CreateFallbackValue(target_type);
    }
}

duckdb::Value
ODataDataExtractor::ParseJsonObject(duckdb_yyjson::yyjson_val *obj_val,
                                    const duckdb::LogicalType &target_type) {
    try {
    auto &struct_types = duckdb::StructType::GetChildTypes(target_type);
        duckdb::vector<duckdb::Value> struct_values;
        struct_values.resize(struct_types.size(), duckdb::Value());
        
        // Build child name map (lowercase and underscore-stripped) -> index
        std::unordered_map<std::string, size_t> child_index;
        for (size_t i = 0; i < struct_types.size(); ++i) {
            const auto &field_name = struct_types[i].first;
            std::string lower = field_name;
            std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
            child_index.emplace(lower, i);
            std::string nos = lower;
            nos.erase(std::remove(nos.begin(), nos.end(), '_'), nos.end());
            child_index.emplace(nos, i);
        }
        
        // Iterate JSON object keys and assign where matching
        size_t idx, max;
        duckdb_yyjson::yyjson_val *json_key, *json_val;
        yyjson_obj_foreach(obj_val, idx, max, json_key, json_val) {
      if (!duckdb_yyjson::yyjson_is_str(json_key))
        continue;
            std::string key = duckdb_yyjson::yyjson_get_str(json_key);
            std::string lower = key;
            std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
            auto it = child_index.find(lower);
            if (it == child_index.end()) {
                std::string nos = lower;
                nos.erase(std::remove(nos.begin(), nos.end(), '_'), nos.end());
                it = child_index.find(nos);
            }
            if (it != child_index.end()) {
                size_t pos = it->second;
                const auto &field_type = struct_types[pos].second;
                struct_values[pos] = ParseJsonValueToDuckDBValue(json_val, field_type);
            }
        }
        
        return duckdb::Value::STRUCT(target_type, struct_values);
        
  } catch (const std::exception &e) {
    LogError("OBJECT_PARSING",
             "Failed to parse JSON object: " + std::string(e.what()));
        return CreateFallbackValue(target_type);
    }
}

// Infer DuckDB struct type from JSON object
duckdb::LogicalType ODataDataExtractor::InferStructTypeFromJsonObject(
    duckdb_yyjson::yyjson_val *obj_val) {
    if (!obj_val || !duckdb_yyjson::yyjson_is_obj(obj_val)) {
        return duckdb::LogicalType::VARCHAR; // Fallback to VARCHAR if not an object
    }
    
    duckdb::child_list_t<duckdb::LogicalType> struct_fields;
    
    size_t idx, max;
    duckdb_yyjson::yyjson_val *json_key, *json_val;
    yyjson_obj_foreach(obj_val, idx, max, json_key, json_val) {
    if (!duckdb_yyjson::yyjson_is_str(json_key))
      continue;
        
        std::string field_name = duckdb_yyjson::yyjson_get_str(json_key);
        
        // Skip metadata fields that are OData-specific
        if (field_name == "__metadata" || field_name == "__deferred") {
            continue;
        }
        
        // Infer the type for this field
        duckdb::LogicalType field_type = InferTypeFromJsonValue(json_val);
        
        // Add the field to our struct
        struct_fields.emplace_back(field_name, field_type);
    }
    
    if (struct_fields.empty()) {
        return duckdb::LogicalType::VARCHAR; // Fallback if no fields
    }
    
    return duckdb::LogicalType::STRUCT(struct_fields);
}

// Infer DuckDB type from JSON value
duckdb::LogicalType
ODataDataExtractor::InferTypeFromJsonValue(duckdb_yyjson::yyjson_val *value) {
    if (!value) {
        return duckdb::LogicalType::VARCHAR;
    }
    
    if (duckdb_yyjson::yyjson_is_null(value)) {
        return duckdb::LogicalType::VARCHAR; // NULL values get VARCHAR type
    }
    
    if (duckdb_yyjson::yyjson_is_str(value)) {
        std::string str_val = duckdb_yyjson::yyjson_get_str(value);
        
        // Check for special OData V2 date format: "/Date(timestamp)/"
    if (str_val.length() > 8 && str_val.substr(0, 6) == "/Date(" &&
        str_val.substr(str_val.length() - 2) == ")/") {
            return duckdb::LogicalType::TIMESTAMP;
        }
        
        // Check if it looks like a date
        if (str_val.length() == 10 && str_val[4] == '-' && str_val[7] == '-') {
            return duckdb::LogicalType::DATE;
        }
        
        // Check if it looks like a timestamp
    if (str_val.length() >= 19 && str_val[4] == '-' && str_val[7] == '-' &&
        str_val[10] == 'T') {
            return duckdb::LogicalType::TIMESTAMP;
        }
        
        return duckdb::LogicalType::VARCHAR;
    }
    
    if (duckdb_yyjson::yyjson_is_int(value)) {
        int64_t int_val = duckdb_yyjson::yyjson_get_int(value);
    if (int_val >= std::numeric_limits<int32_t>::min() &&
        int_val <= std::numeric_limits<int32_t>::max()) {
            return duckdb::LogicalType::INTEGER;
        } else {
            return duckdb::LogicalType::BIGINT;
        }
    }
    
    if (duckdb_yyjson::yyjson_is_uint(value)) {
        uint64_t uint_val = duckdb_yyjson::yyjson_get_uint(value);
        if (uint_val <= std::numeric_limits<int32_t>::max()) {
            return duckdb::LogicalType::INTEGER;
        } else {
            return duckdb::LogicalType::BIGINT;
        }
    }
    
    if (duckdb_yyjson::yyjson_is_real(value)) {
        return duckdb::LogicalType::DOUBLE;
    }
    
    if (duckdb_yyjson::yyjson_is_bool(value)) {
        return duckdb::LogicalType::BOOLEAN;
    }
    
    if (duckdb_yyjson::yyjson_is_arr(value)) {
        // For arrays, we'll use LIST(VARCHAR) as a safe default
        // The actual item types could be inferred later if needed
        return duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
    }
    
    if (duckdb_yyjson::yyjson_is_obj(value)) {
        // For objects, we could recursively infer the struct type
        // But for now, we'll use VARCHAR to avoid infinite recursion
        return duckdb::LogicalType::VARCHAR;
    }
    
    return duckdb::LogicalType::VARCHAR; // Default fallback
}

// This method was removed as it required HTTP requests that aren't available in
// the current architecture

// Error handling helpers
void ODataDataExtractor::LogError(const std::string &context,
                                  const std::string &error_msg) const {
    std::lock_guard<std::mutex> lock(error_mutex_);
    
    last_error_ = "[" + context + "] " + error_msg;
    error_counts_[context]++;
    
    // Rate-limit error logging to avoid spam
    if (error_counts_[context] <= 10 || error_counts_[context] % 100 == 0) {
    ERPL_TRACE_ERROR("DATA_EXTRACTOR",
                     last_error_ + " (count: " +
                         std::to_string(error_counts_[context]) + ")");
    }
}

bool ODataDataExtractor::ShouldRetryAfterError(
    const std::string &context) const {
    std::lock_guard<std::mutex> lock(error_mutex_);
    auto it = error_counts_.find(context);
    return it == error_counts_.end() || it->second < 3; // Retry up to 3 times
}

duckdb::Value ODataDataExtractor::CreateFallbackValue(
    const duckdb::LogicalType &target_type) const {
    switch (target_type.id()) {
        case duckdb::LogicalTypeId::VARCHAR:
            return duckdb::Value("");
        case duckdb::LogicalTypeId::INTEGER:
            return duckdb::Value(0);
        case duckdb::LogicalTypeId::BIGINT:
            return duckdb::Value(static_cast<int64_t>(0));
        case duckdb::LogicalTypeId::FLOAT:
        case duckdb::LogicalTypeId::DOUBLE:
            return duckdb::Value(0.0);
        case duckdb::LogicalTypeId::BOOLEAN:
            return duckdb::Value(false);
        case duckdb::LogicalTypeId::LIST:
            return duckdb::Value::LIST(duckdb::ListType::GetChildType(target_type), {});
  case duckdb::LogicalTypeId::STRUCT: {
    auto &struct_types = duckdb::StructType::GetChildTypes(target_type);
            duckdb::vector<duckdb::Value> null_values;
            for (size_t i = 0; i < struct_types.size(); ++i) {
                null_values.push_back(duckdb::Value());
            }
            return duckdb::Value::STRUCT(target_type, null_values);
        }
        default:
            return duckdb::Value(); // NULL for unknown types
    }
}

// Performance and memory management methods
void ODataDataExtractor::SetBatchSize(size_t batch_size) {
    batch_size_ = batch_size;
  ERPL_TRACE_DEBUG("DATA_EXTRACTOR",
                   "Set batch size to " + std::to_string(batch_size));
}

void ODataDataExtractor::EnableCompression(bool enable) {
    compression_enabled_ = enable;
  ERPL_TRACE_DEBUG("DATA_EXTRACTOR",
                   "Compression " +
                       std::string(enable ? "enabled" : "disabled"));
}

void ODataDataExtractor::ClearCache() {
    expanded_data_cache.clear();
    ERPL_TRACE_DEBUG("DATA_EXTRACTOR", "Cleared expanded data cache");
}

size_t ODataDataExtractor::GetCacheSize() const {
    size_t total_size = 0;
  for (const auto &[path, values] : expanded_data_cache) {
        total_size += values.size();
    }
    return total_size;
}

bool ODataDataExtractor::ValidateExpandedData(
    const std::string &expand_path) const {
    auto it = expanded_data_cache.find(expand_path);
    return it != expanded_data_cache.end() && !it->second.empty();
}

std::string ODataDataExtractor::GetLastError() const {
    std::lock_guard<std::mutex> lock(error_mutex_);
    return last_error_;
}

void ODataDataExtractor::ResetErrorState() {
    std::lock_guard<std::mutex> lock(error_mutex_);
    last_error_.clear();
    error_counts_.clear();
}

void ODataDataExtractor::OptimizeCacheMemory() {
    // Remove empty cache entries
  for (auto it = expanded_data_cache.begin();
       it != expanded_data_cache.end();) {
        if (it->second.empty()) {
            it = expanded_data_cache.erase(it);
        } else {
            ++it;
        }
    }
    
    ERPL_TRACE_DEBUG("DATA_EXTRACTOR", "Optimized cache memory usage");
}

void ODataDataExtractor::CompressCacheData() {
    if (!compression_enabled_) {
        return;
    }
    
    // This is a placeholder for potential compression logic
    // In a real implementation, we could serialize and compress the data
    ERPL_TRACE_DEBUG("DATA_EXTRACTOR", "Cache compression completed");
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
        auto doc =
            duckdb_yyjson::yyjson_read(content.c_str(), content.size(), 0);
                
                if (doc) {
          ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                           "Successfully parsed JSON document");
                    auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
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
                    
                    duckdb_yyjson::yyjson_doc_free(doc);
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
  (void)response;

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
  bind_data->row_buffer->AddRows(rows);
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
      auto doc = duckdb_yyjson::yyjson_read(initial_content.c_str(),
                                            initial_content.length(), 0);
      if (doc) {
        auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
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
        duckdb_yyjson::yyjson_doc_free(doc);
      }
    } catch (...) {
      // If parsing fails, we'll fall back to normal metadata-based approach
      ERPL_TRACE_DEBUG(
          "ODATA_READ_BIND",
          "Failed to parse initial content, will use metadata approach");
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
      bind_data->row_buffer->AddRows(rows);
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

      bind_data->row_buffer->AddRows(rows);
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
                        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Parsing OData v4 response format");
                        auto value_arr = duckdb_yyjson::yyjson_obj_get(root, "value");
                        if (value_arr && duckdb_yyjson::yyjson_is_arr(value_arr)) {
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     "Found 'value' array in OData v4 response");
                            duckdb_yyjson::yyjson_arr_iter arr_it;
                            duckdb_yyjson::yyjson_arr_iter_init(value_arr, &arr_it);
    duckdb_yyjson::yyjson_val *first_row =
        duckdb_yyjson::yyjson_arr_iter_next(&arr_it);
        
                            if (first_row && duckdb_yyjson::yyjson_is_obj(first_row)) {
      ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                       "Found first row object in OData v4 response");
                                
            // Extract column names from first row
      char *json_str = duckdb_yyjson::yyjson_val_write(first_row, 0, nullptr);
            if (json_str) {
        extracted_column_names =
            ExtractColumnNamesFromJson(std::string(json_str));
                free(json_str);
                                }
                            } else {
      ERPL_TRACE_WARN("ODATA_READ_BIND",
                      "First row is not an object in OData v4 response");
                            }
                        } else {
    ERPL_TRACE_WARN("ODATA_READ_BIND",
                    "Could not find 'value' array in OData v4 response");
                        }
}

void ODataReadBindData::ParseODataV2Response(
    duckdb_yyjson::yyjson_val *root,
                                           std::shared_ptr<ODataEntitySetClient> odata_client,
    std::vector<std::string> &extracted_column_names) {
  ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                   "No @odata.context found, trying OData v2 format");
                        auto data_obj = duckdb_yyjson::yyjson_obj_get(root, "d");
                        if (data_obj && duckdb_yyjson::yyjson_is_obj(data_obj)) {
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     "Found 'd' object in OData v2 response");
                            auto results_arr = duckdb_yyjson::yyjson_obj_get(data_obj, "results");
                            if (results_arr && duckdb_yyjson::yyjson_is_arr(results_arr)) {
      ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                       "Found 'results' array in OData v2 response");
                                duckdb_yyjson::yyjson_arr_iter arr_it;
                                duckdb_yyjson::yyjson_arr_iter_init(results_arr, &arr_it);
      duckdb_yyjson::yyjson_val *first_row =
          duckdb_yyjson::yyjson_arr_iter_next(&arr_it);
            
                                if (first_row && duckdb_yyjson::yyjson_is_obj(first_row)) {
        ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                         "Found first row object in OData v2 response");
                                    
                // Extract column names from first row
        char *json_str = duckdb_yyjson::yyjson_val_write(first_row, 0, nullptr);
                if (json_str) {
          extracted_column_names =
              ExtractColumnNamesFromJson(std::string(json_str));
                    free(json_str);
                }
                                } else {
        ERPL_TRACE_WARN("ODATA_READ_BIND",
                        "First row is not an object in OData v2 response");
                                }
                            } else {
      ERPL_TRACE_WARN("ODATA_READ_BIND",
                      "Could not find 'results' array in OData v2 response");
                            }
                        } else {
    ERPL_TRACE_WARN("ODATA_READ_BIND",
                    "Could not find 'd' object in OData v2 response");
  }
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
      all_result_types = {duckdb::LogicalType::VARCHAR,
                          duckdb::LogicalType::VARCHAR,
                          duckdb::LogicalType::VARCHAR};
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
                            mapped_types.push_back(duckdb::LogicalType::VARCHAR);
              ERPL_TRACE_WARN(
                  "ODATA_READ_BIND",
                  "Column '" + extracted_name +
                      "' not found in metadata, using VARCHAR fallback");
                        }
                    } else {
                        // Fallback to VARCHAR if column not found in metadata
                        mapped_types.push_back(duckdb::LogicalType::VARCHAR);
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
                                duckdb::LogicalType::VARCHAR);
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
          (i < exp_types.size()) ? exp_types[i] : duckdb::LogicalType::VARCHAR;
            
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

unsigned int ODataReadBindData::FetchNextResult(duckdb::DataChunk &output) {
    // Ensure input parameters are set on client before any request
    if (!input_parameters.empty()) {
        odata_client->SetInputParameters(input_parameters);
    }

    // Make sure first page is prefetched once and buffered
    if (!first_page_cached_) {
        PrefetchFirstPage();
    }

    // Always use full schema for buffering; projection is applied at emission
    auto all_result_names = GetResultNames(true);
    auto all_result_types = GetResultTypes(true);
    
    // Build name->index map for robust lookups
    std::unordered_map<std::string, duckdb::idx_t> name_to_index;
    name_to_index.reserve(all_result_names.size());
    for (duckdb::idx_t k = 0; k < (duckdb::idx_t)all_result_names.size(); ++k) {
        name_to_index.emplace(all_result_names[k], k);
    }
    
    // Check if we have expanded data to process
    bool has_expand = HasExpandedData();

    const idx_t target = STANDARD_VECTOR_SIZE;
    idx_t emitted = 0;
    auto null_value = duckdb::Value();

  // Fetch additional pages until we have enough buffered rows to fill the
  // vector or no more pages
    while (row_buffer->Size() < target && row_buffer->HasNextPage()) {
        auto next_response = odata_client->Get(true);
        if (!next_response) {
            row_buffer->SetHasNextPage(false);
            break;
        }
        // Capture total count once for progress
        if (next_response->GetODataVersion() == ODataVersion::V4) {
            auto total = next_response->Content()->TotalCount();
      if (total.has_value() && (!progress_tracker->HasTotalCount() ||
                                progress_tracker->GetTotalCount() == 0)) {
                progress_tracker->SetTotalCount(total.value());
            }
        }
        // Extract expanded data from subsequent pages as well
        if (has_expand && !data_extractor->GetExpandedDataSchema().empty()) {
            try {
        ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                         "Extracting expanded data from subsequent page");
        data_extractor->ExtractExpandedDataFromResponse(
            next_response->RawContent());
      } catch (const std::exception &e) {
        ERPL_TRACE_WARN(
            "ODATA_READ_BIND",
            "Failed to extract expanded data from subsequent page: " +
                std::string(e.what()));
            }
        }

        // Buffer full rows using full schema to keep indices stable
        auto page_rows = next_response->ToRows(all_result_names, all_result_types);
        row_buffer->AddRows(page_rows);
        row_buffer->SetHasNextPage(next_response->NextUrl().has_value());
        progress_tracker->IncrementRowsFetched(page_rows.size());
    }

    // Emit up to target rows from buffer
    idx_t to_emit = std::min<idx_t>(row_buffer->Size(), target);
    for (idx_t i = 0; i < to_emit; i++) {
        const auto &row = row_buffer->GetNextRow();
        for (idx_t j = 0; j < output.ColumnCount(); j++) {
            // Map activated index j to original column index in full schema
      duckdb::idx_t original_column_index =
          (j < activated_to_original_mapping.size())
              ? activated_to_original_mapping[j]
              : j;

            if (original_column_index < all_result_names.size()) {
                // Check if this original index points to an expanded column
                if (has_expand && !data_extractor->GetExpandedDataSchema().empty() &&
            original_column_index >=
                (all_result_names.size() -
                 data_extractor->GetExpandedDataSchema().size())) {
          size_t expand_index =
              original_column_index -
              (all_result_names.size() -
               data_extractor->GetExpandedDataSchema().size());
                    if (expand_index < data_extractor->GetExpandedDataSchema().size()) {
            std::string expand_path =
                data_extractor->GetExpandedDataSchema()[expand_index];
            // Use global emitted_row_index_ to align with how cache was filled
            // (per input row)
            auto expand_data = data_extractor->ExtractExpandedDataForRow(
                std::to_string(emitted_row_index_), expand_path);
                        if (!expand_data.IsNull()) {
                            output.SetValue(j, i, expand_data);
              ERPL_TRACE_INFO("ODATA_SCAN", "Setting expanded data column " +
                                                std::to_string(j) +
                                                " with data for path '" +
                                                expand_path + "'");
                        } else {
                            output.SetValue(j, i, null_value);
              ERPL_TRACE_INFO(
                  "ODATA_SCAN",
                  "Setting expanded data column " + std::to_string(j) +
                      " to NULL (no data) for path '" + expand_path + "'");
                        }
                    } else {
                auto col_type = output.data[j].GetType();
                output.SetValue(j, i, null_value.DefaultCastAs(col_type));
                    }
            } else {
          // Regular base column from buffered full row: resolve by name to
          // guard against index drift
                    const std::string &col_name = all_result_names[original_column_index];
                    auto it = name_to_index.find(col_name);
                    if (it != name_to_index.end() && it->second < row.size()) {
                        output.SetValue(j, i, row[it->second]);
                    } else {
                        auto col_type = output.data[j].GetType();
                        output.SetValue(j, i, null_value.DefaultCastAs(col_type));
                    }
                }
            } else {
                auto col_type = output.data[j].GetType();
                output.SetValue(j, i, null_value.DefaultCastAs(col_type));
            }
        }
        emitted++;
        emitted_row_index_++;
    }

    output.SetCardinality(emitted);

    // Update cumulative progress
    progress_tracker->IncrementRowsFetched(emitted);
  if (progress_tracker->HasTotalCount() &&
      progress_tracker->GetTotalCount() > 0) {
    double pct = 100.0 * (double)progress_tracker->GetRowsFetched() /
                 (double)progress_tracker->GetTotalCount();
    if (pct > 100.0)
      pct = 100.0;
    ERPL_TRACE_INFO("ODATA_SCAN",
                    duckdb::StringUtil::Format(
                        "Progress: %.2f%% (%llu/%llu)", pct,
            (unsigned long long)progress_tracker->GetRowsFetched(),
            (unsigned long long)progress_tracker->GetTotalCount()));
    }

    return emitted;
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
        
    // Use extracted column names if available, otherwise fall back to OData
    // client
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

    // Column name resolver should interpret indices as ORIGINAL schema indices
    // BuildSelectClause/BuildFilterClause pass original indices
    predicate_pushdown_helper->SetColumnNameResolver([this](duckdb::column_t
                                                                original_index)
                                                         -> std::string {
      if (!this->extracted_column_names.empty()) {
        if ((size_t)original_index < this->extracted_column_names.size()) {
          return this->extracted_column_names[original_index];
        } else {
          ERPL_TRACE_ERROR(
              "ODATA_READ_BIND",
              duckdb::StringUtil::Format("Original column index %d is out of "
                                         "bounds for extracted column names",
                                         (int)original_index));
          return std::string();
        }
      }
      if (this->all_result_names.empty()) {
        this->all_result_names = this->odata_client->GetResultNames();
      }
      if ((size_t)original_index < this->all_result_names.size()) {
        return this->all_result_names[original_index];
      } else {
        ERPL_TRACE_ERROR(
            "ODATA_READ_BIND",
            duckdb::StringUtil::Format(
                "Original column index %d is out of bounds for result names",
                (int)original_index));
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
    if (!response) {
    ERPL_TRACE_WARN("ODATA_READ_BIND",
                    "PrefetchFirstPage: no response received");
        first_page_cached_ = true;
        row_buffer->SetHasNextPage(false);
        return;
    }
    
    // Extract expanded data if we have expand clauses
    if (HasExpandedData() && !data_extractor->GetExpandedDataSchema().empty()) {
        try {
      ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                       "Extracting expanded data from first page");
            data_extractor->ExtractExpandedDataFromResponse(response->RawContent());
    } catch (const std::exception &e) {
      ERPL_TRACE_WARN("ODATA_READ_BIND",
                      "Failed to extract expanded data from first page: " +
                          std::string(e.what()));
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
    auto all_result_names = GetResultNames(true);
    auto all_result_types = GetResultTypes(true);

    auto page_rows = response->ToRows(all_result_names, all_result_types);
    row_buffer->AddRows(page_rows);
    row_buffer->SetHasNextPage(response->NextUrl().has_value());
    first_page_cached_ = true;

  ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                   duckdb::StringUtil::Format(
        "PrefetchFirstPage: buffered %d rows, has_next_page=%s",
                       (int)row_buffer->Size(),
                       row_buffer->HasNextPage() ? "true" : "false"));
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
    
  // Use extracted column names if available, otherwise fall back to
  // all_result_names
    if (!extracted_column_names.empty()) {
        if (original_index >= extracted_column_names.size()) {
      ERPL_TRACE_ERROR(
          "ODATA_READ_BIND",
          duckdb::StringUtil::Format("Original column index %d is out of "
                                     "bounds for extracted column names",
                                     original_index));
            return "";
        }
        auto column_name = extracted_column_names[original_index];
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     duckdb::StringUtil::Format(
                         "Activated index %d maps to original index %d which "
                         "is column '%s' (from extracted names)",
                         activated_column_index, original_index,
                         column_name.c_str()));
        return column_name;
    } else {
        if (original_index >= all_result_names.size()) {
      ERPL_TRACE_ERROR(
          "ODATA_READ_BIND",
          duckdb::StringUtil::Format(
              "Original column index %d is out of bounds for result names",
              original_index));
            return "";
        }
        auto column_name = all_result_names[original_index];
    ERPL_TRACE_DEBUG("ODATA_READ_BIND",
                     duckdb::StringUtil::Format(
                         "Activated index %d maps to original index %d which "
                         "is column '%s' (from result names)",
                         activated_column_index, original_index,
                         column_name.c_str()));
        return column_name;
    }
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

// Infer struct type from JSON object, handling nested expands
duckdb::LogicalType
ODataDataExtractor::InferStructTypeFromJsonObjectWithNestedExpands(
    duckdb_yyjson::yyjson_val *obj_val, const std::string &expand_path) {
    if (!obj_val || !duckdb_yyjson::yyjson_is_obj(obj_val)) {
        return duckdb::LogicalType::VARCHAR; // Fallback to VARCHAR if not an object
    }
    
    duckdb::child_list_t<duckdb::LogicalType> struct_fields;
    
    size_t idx, max;
    duckdb_yyjson::yyjson_val *json_key, *json_val;
    yyjson_obj_foreach(obj_val, idx, max, json_key, json_val) {
    if (!duckdb_yyjson::yyjson_is_str(json_key))
      continue;
        
        std::string field_name = duckdb_yyjson::yyjson_get_str(json_key);
        
        // Skip metadata fields that are OData-specific
        if (field_name == "__metadata" || field_name == "__deferred") {
            continue;
        }
        
    // Check if this field is a nested expand (e.g., if expand_path is
    // "Products/Suppliers" and we're processing "Products")
        bool is_nested_expand = false;
    for (const auto &path : expand_paths) {
      if (path.find(expand_path + "/") == 0 &&
          path.substr(expand_path.length() + 1) == field_name) {
                is_nested_expand = true;
                break;
            }
        }
        
        duckdb::LogicalType field_type;
        if (is_nested_expand) {
            // This is a nested expand, so we need to handle it recursively
            if (duckdb_yyjson::yyjson_is_obj(json_val)) {
                // Check if it has a 'results' array (OData V2 collection)
                auto results_field = duckdb_yyjson::yyjson_obj_get(json_val, "results");
                if (results_field && duckdb_yyjson::yyjson_is_arr(results_field)) {
                    // It's a collection, so use LIST(STRUCT)
          auto nested_struct_type =
              InferStructTypeFromJsonObjectWithNestedExpands(
                  json_val, expand_path + "/" + field_name);
                    field_type = duckdb::LogicalType::LIST(nested_struct_type);
                } else {
                    // It's a single entity, so use STRUCT
          field_type = InferStructTypeFromJsonObjectWithNestedExpands(
              json_val, expand_path + "/" + field_name);
                }
            } else {
                field_type = duckdb::LogicalType::VARCHAR; // Fallback
            }
        } else {
            // Regular field, infer the type normally
            field_type = InferTypeFromJsonValue(json_val);
        }
        
        // Add the field to our struct
        struct_fields.emplace_back(field_name, field_type);
    }
    
    if (struct_fields.empty()) {
        return duckdb::LogicalType::VARCHAR; // Fallback if no fields
    }
    
    return duckdb::LogicalType::STRUCT(struct_fields);
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
    return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
                    duckdb::LogicalType::VARCHAR};
  } else {
    // Use existing entity-set schema logic
    names = bind_data->GetResultNames();
    return_types = bind_data->GetResultTypes();
  }
}

} // namespace ODataReadBindHelpers

duckdb::unique_ptr<FunctionData>
ODataReadBind(ClientContext &context, TableFunctionBindInput &input,
              vector<LogicalType> &return_types, vector<string> &names) {
  auto auth_params = AuthParamsFromInput(context, input);
  auto url = input.inputs[0].GetValue<std::string>();

  ERPL_TRACE_INFO("ODATA_BIND",
                  duckdb::StringUtil::Format(
                      "Binding OData read function for URL: %s", url.c_str()));

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
  ERPL_TRACE_DEBUG("ODATA_READ_TABLE_PROGRESS",
                   "Progress fraction: " +
                       std::to_string(bind_data.GetProgressFraction()));

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
    
  TableFunction read_entity_set({LogicalType::VARCHAR}, ODataReadScan,
                                ODataReadBind, ODataReadTableInitGlobalState);
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

// ============================================================================
// OData Describe Function Implementation
// ============================================================================

class ODataDescribeBindData : public TableFunctionData {
public:
    std::string url;
    std::string resource_type;  // "service" or "entity_set"
    std::string entity_set_name;
    std::shared_ptr<ODataEntitySetClient> entity_client;
    std::shared_ptr<ODataServiceClient> service_client;
    std::shared_ptr<HttpAuthParams> auth_params;
    bool data_loaded = false;
    std::vector<duckdb::Value> result_row;  // Single row result
};

// Helper to create empty property list with proper schema
static duckdb::Value CreateEmptyPropertyList() {
    child_list_t<LogicalType> property_struct;
    property_struct.emplace_back("name", LogicalTypeId::VARCHAR);
    property_struct.emplace_back("duckdb_type", LogicalTypeId::VARCHAR);
    property_struct.emplace_back("edm_type", LogicalTypeId::VARCHAR);
    property_struct.emplace_back("is_nullable", LogicalTypeId::BOOLEAN);
    property_struct.emplace_back("is_key", LogicalTypeId::BOOLEAN);
    property_struct.emplace_back("max_length", LogicalTypeId::INTEGER);
    property_struct.emplace_back("precision", LogicalTypeId::INTEGER);
    property_struct.emplace_back("scale", LogicalTypeId::INTEGER);
    return Value::LIST(LogicalType::STRUCT(property_struct), vector<Value>());
}

// Helper to create empty navigation property list with proper schema
static duckdb::Value CreateEmptyNavigationList() {
    child_list_t<LogicalType> nav_struct;
    nav_struct.emplace_back("name", LogicalTypeId::VARCHAR);
    nav_struct.emplace_back("target_entity", LogicalTypeId::VARCHAR);
    
    child_list_t<LogicalType> target_type_struct;
    target_type_struct.emplace_back("name", LogicalTypeId::VARCHAR);
    target_type_struct.emplace_back("property_count", LogicalTypeId::BIGINT);
    target_type_struct.emplace_back("nav_property_count", LogicalTypeId::BIGINT);
    nav_struct.emplace_back("target_entity_type", LogicalType::STRUCT(target_type_struct));
    
    nav_struct.emplace_back("is_collection", LogicalTypeId::BOOLEAN);
    nav_struct.emplace_back("partner", LogicalTypeId::VARCHAR);
    
    child_list_t<LogicalType> constraint_struct;
    constraint_struct.emplace_back("property", LogicalTypeId::VARCHAR);
    constraint_struct.emplace_back("referenced_property", LogicalTypeId::VARCHAR);
    nav_struct.emplace_back("referential_constraints", LogicalType::LIST(LogicalType::STRUCT(constraint_struct)));
    
    return Value::LIST(LogicalType::STRUCT(nav_struct), vector<Value>());
}

// Helper to create empty entity sets list with proper schema
static duckdb::Value CreateEmptyEntitySetsList() {
    child_list_t<LogicalType> entity_set_struct;
    entity_set_struct.emplace_back("name", LogicalTypeId::VARCHAR);
    entity_set_struct.emplace_back("entity_type", LogicalTypeId::VARCHAR);
    entity_set_struct.emplace_back("url", LogicalTypeId::VARCHAR);
    return Value::LIST(LogicalType::STRUCT(entity_set_struct), vector<Value>());
}

// Helper to create empty functions list with proper schema
static duckdb::Value CreateEmptyFunctionsList() {
    child_list_t<LogicalType> function_struct;
    function_struct.emplace_back("name", LogicalTypeId::VARCHAR);
    function_struct.emplace_back("return_type", LogicalTypeId::VARCHAR);
    
    child_list_t<LogicalType> param_struct;
    param_struct.emplace_back("name", LogicalTypeId::VARCHAR);
    param_struct.emplace_back("type", LogicalTypeId::VARCHAR);
    param_struct.emplace_back("nullable", LogicalTypeId::BOOLEAN);
    function_struct.emplace_back("parameters", LogicalType::LIST(LogicalType::STRUCT(param_struct)));
    
    return Value::LIST(LogicalType::STRUCT(function_struct), vector<Value>());
}

// Helper to build property struct
static duckdb::Value BuildPropertyStruct(const Property& prop, bool is_key = false) {
    auto duck_type_str = DuckTypeConverter::ConvertEdmTypeStringToDuckDbTypeString(prop.type_name);
    
    child_list_t<Value> property_struct;
    property_struct.emplace_back("name", Value(prop.name));
    property_struct.emplace_back("duckdb_type", Value(duck_type_str));
    property_struct.emplace_back("edm_type", Value(prop.type_name));
    property_struct.emplace_back("is_nullable", Value(prop.nullable));
    property_struct.emplace_back("is_key", Value(is_key));
    property_struct.emplace_back("max_length", prop.max_length > 0 ? Value(prop.max_length) : Value());
    property_struct.emplace_back("precision", prop.precision > 0 ? Value(prop.precision) : Value());
    property_struct.emplace_back("scale", prop.scale > 0 ? Value(prop.scale) : Value());
    
    return Value::STRUCT(property_struct);
}

// Helper to recursively build navigation property info
static duckdb::Value BuildNavigationPropertyStruct(
    const NavigationProperty& nav_prop, 
    const Edmx& edmx,
    std::set<std::string>& visited_types  // Prevent infinite recursion
) {
    child_list_t<Value> nav_struct;
    nav_struct.emplace_back("name", Value(nav_prop.name));
    
    // Extract target entity type
    DuckTypeConverter converter(const_cast<Edmx&>(edmx));
    auto [is_collection, target_type] = converter.ExtractCollectionType(nav_prop.type);
    nav_struct.emplace_back("target_entity", Value(target_type));
    
    // Recursive: Get target entity type info (if not already visited)
    child_list_t<Value> type_info;
    if (visited_types.find(target_type) == visited_types.end()) {
        visited_types.insert(target_type);
        
        auto entity_type_variant = edmx.FindType(target_type);
        if (std::holds_alternative<EntityType>(entity_type_variant)) {
            // Build a simplified struct of the target entity
            auto& target_entity = std::get<EntityType>(entity_type_variant);
            
            type_info.emplace_back("name", Value(target_entity.name));
            type_info.emplace_back("property_count", Value((int64_t)target_entity.properties.size()));
            type_info.emplace_back("nav_property_count", Value((int64_t)target_entity.navigation_properties.size()));
        } else {
            // Provide empty values when target entity not found
            type_info.emplace_back("name", Value(""));
            type_info.emplace_back("property_count", Value((int64_t)0));
            type_info.emplace_back("nav_property_count", Value((int64_t)0));
        }
    } else {
        // Provide empty values when already visited (to avoid infinite recursion)
        type_info.emplace_back("name", Value(""));
        type_info.emplace_back("property_count", Value((int64_t)0));
        type_info.emplace_back("nav_property_count", Value((int64_t)0));
    }
    Value target_type_value = Value::STRUCT(type_info);
    nav_struct.emplace_back("target_entity_type", target_type_value);
    nav_struct.emplace_back("is_collection", Value(is_collection));
    nav_struct.emplace_back("partner", nav_prop.partner.empty() ? Value("") : Value(nav_prop.partner));
    
    // Add referential constraints
    vector<Value> constraints;
    for (const auto& constraint : nav_prop.referential_constraints) {
        child_list_t<Value> constraint_struct;
        constraint_struct.emplace_back("property", Value(constraint.property));
        constraint_struct.emplace_back("referenced_property", Value(constraint.referenced_property));
        constraints.push_back(Value::STRUCT(constraint_struct));
    }
    
    // Handle empty constraints list
    if (constraints.empty()) {
        child_list_t<LogicalType> constraint_struct;
        constraint_struct.emplace_back("property", LogicalTypeId::VARCHAR);
        constraint_struct.emplace_back("referenced_property", LogicalTypeId::VARCHAR);
        nav_struct.emplace_back("referential_constraints", Value::LIST(LogicalType::STRUCT(constraint_struct), vector<Value>()));
    } else {
        nav_struct.emplace_back("referential_constraints", Value::LIST(constraints));
    }
    
    return Value::STRUCT(nav_struct);
}

// Bind function for odata_describe
static unique_ptr<FunctionData> ODataDescribeBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names
) {
    ERPL_TRACE_INFO("ODATA_DESCRIBE_BIND", "Starting OData describe bind");
    
    // Reuse authentication handling from ODataReadBind
    auto auth_params = AuthParamsFromInput(context, input);
    
    // Get URL argument
    auto url = input.inputs[0].GetValue<string>();
    ERPL_TRACE_INFO("ODATA_DESCRIBE_BIND", "Describing URL: " + url);
    
    // Reuse probe logic from ODataClientFactory
    auto probe_result = ODataClientFactory::ProbeUrl(url, auth_params);
    
    auto bind_data = make_uniq<ODataDescribeBindData>();
    bind_data->url = url;
    bind_data->auth_params = auth_params;
    
    // Create appropriate client based on probe result
    if (probe_result.is_service_root) {
        bind_data->resource_type = "service";
        bind_data->service_client = ODataClientFactory::CreateServiceClient(probe_result);
        ERPL_TRACE_INFO("ODATA_DESCRIBE_BIND", "Detected service root");
    } else {
        bind_data->resource_type = "entity_set";
        bind_data->entity_client = ODataClientFactory::CreateEntitySetClient(probe_result);
        
        // Extract entity set name from URL
        HttpUrl parsed_url(url);
        auto path = parsed_url.Path();
        auto last_slash = path.find_last_of('/');
        if (last_slash != std::string::npos) {
            auto entity_set_part = path.substr(last_slash + 1);
            // Remove any query parameters
            auto query_pos = entity_set_part.find('?');
            if (query_pos != std::string::npos) {
                entity_set_part = entity_set_part.substr(0, query_pos);
            }
            // Remove any parentheses (for specific entity access)
            auto paren_pos = entity_set_part.find('(');
            if (paren_pos != std::string::npos) {
                entity_set_part = entity_set_part.substr(0, paren_pos);
            }
            bind_data->entity_set_name = entity_set_part;
        }
        ERPL_TRACE_INFO("ODATA_DESCRIBE_BIND", "Detected entity set: " + bind_data->entity_set_name);
    }
    
    // Define output schema
    names = {"url", "resource_type", "entity_set_name", "entity_type_name", 
             "properties", "navigation_properties", "entity_sets", "functions"};
    
    // Build types for the complex columns
    // Properties struct
    child_list_t<LogicalType> property_struct;
    property_struct.emplace_back("name", LogicalTypeId::VARCHAR);
    property_struct.emplace_back("duckdb_type", LogicalTypeId::VARCHAR);
    property_struct.emplace_back("edm_type", LogicalTypeId::VARCHAR);
    property_struct.emplace_back("is_nullable", LogicalTypeId::BOOLEAN);
    property_struct.emplace_back("is_key", LogicalTypeId::BOOLEAN);
    property_struct.emplace_back("max_length", LogicalTypeId::INTEGER);
    property_struct.emplace_back("precision", LogicalTypeId::INTEGER);
    property_struct.emplace_back("scale", LogicalTypeId::INTEGER);
    auto property_type = LogicalType::STRUCT(property_struct);
    
    // Navigation properties struct
    child_list_t<LogicalType> nav_struct;
    nav_struct.emplace_back("name", LogicalTypeId::VARCHAR);
    nav_struct.emplace_back("target_entity", LogicalTypeId::VARCHAR);
    
    // Target entity type sub-struct (nullable)
    child_list_t<LogicalType> target_type_struct;
    target_type_struct.emplace_back("name", LogicalTypeId::VARCHAR);
    target_type_struct.emplace_back("property_count", LogicalTypeId::BIGINT);
    target_type_struct.emplace_back("nav_property_count", LogicalTypeId::BIGINT);
    nav_struct.emplace_back("target_entity_type", LogicalType::STRUCT(target_type_struct));
    
    nav_struct.emplace_back("is_collection", LogicalTypeId::BOOLEAN);
    nav_struct.emplace_back("partner", LogicalTypeId::VARCHAR);
    
    // Referential constraints sub-struct
    child_list_t<LogicalType> constraint_struct;
    constraint_struct.emplace_back("property", LogicalTypeId::VARCHAR);
    constraint_struct.emplace_back("referenced_property", LogicalTypeId::VARCHAR);
    nav_struct.emplace_back("referential_constraints", LogicalType::LIST(LogicalType::STRUCT(constraint_struct)));
    
    auto nav_type = LogicalType::STRUCT(nav_struct);
    
    // Entity sets struct (for service root)
    child_list_t<LogicalType> entity_set_struct;
    entity_set_struct.emplace_back("name", LogicalTypeId::VARCHAR);
    entity_set_struct.emplace_back("entity_type", LogicalTypeId::VARCHAR);
    entity_set_struct.emplace_back("url", LogicalTypeId::VARCHAR);
    auto entity_set_type = LogicalType::STRUCT(entity_set_struct);
    
    // Functions struct
    child_list_t<LogicalType> function_struct;
    function_struct.emplace_back("name", LogicalTypeId::VARCHAR);
    function_struct.emplace_back("return_type", LogicalTypeId::VARCHAR);
    
    // Function parameters sub-struct
    child_list_t<LogicalType> param_struct;
    param_struct.emplace_back("name", LogicalTypeId::VARCHAR);
    param_struct.emplace_back("type", LogicalTypeId::VARCHAR);
    param_struct.emplace_back("nullable", LogicalTypeId::BOOLEAN);
    function_struct.emplace_back("parameters", LogicalType::LIST(LogicalType::STRUCT(param_struct)));
    
    auto function_type = LogicalType::STRUCT(function_struct);
    
    return_types = {
        LogicalTypeId::VARCHAR,                    // url
        LogicalTypeId::VARCHAR,                    // resource_type
        LogicalTypeId::VARCHAR,                    // entity_set_name
        LogicalTypeId::VARCHAR,                    // entity_type_name
        LogicalType::LIST(property_type),        // properties
        LogicalType::LIST(nav_type),            // navigation_properties
        LogicalType::LIST(entity_set_type),     // entity_sets
        LogicalType::LIST(function_type)        // functions
    };
    
    return bind_data;
}

// Helper to build properties list for an entity type
static duckdb::Value BuildPropertiesList(const EntityType& entity_type, const std::set<std::string>& key_properties) {
    vector<Value> properties;
    for (const auto& prop : entity_type.properties) {
        bool is_key = key_properties.count(prop.name) > 0;
        properties.push_back(BuildPropertyStruct(prop, is_key));
    }
    return properties.empty() ? CreateEmptyPropertyList() : Value::LIST(properties);
}

// Helper to build navigation properties list for an entity type
static duckdb::Value BuildNavigationPropertiesList(
    const EntityType& entity_type, 
    const Edmx& metadata,
    const std::string& entity_type_name
) {
    vector<Value> nav_properties;
    std::set<std::string> visited_types;
    visited_types.insert(entity_type_name);
    
    for (const auto& nav_prop : entity_type.navigation_properties) {
        nav_properties.push_back(BuildNavigationPropertyStruct(nav_prop, metadata, visited_types));
    }
    
    return nav_properties.empty() ? CreateEmptyNavigationList() : Value::LIST(nav_properties);
}

// Helper to build entity sets list for service root
static duckdb::Value BuildEntitySetsList(const Edmx& metadata, const std::string& base_url) {
    vector<Value> entity_sets;
    
    for (const auto& schema : metadata.data_services.schemas) {
        for (const auto& container : schema.entity_containers) {
            for (const auto& entity_set : container.entity_sets) {
                child_list_t<Value> es_struct;
                es_struct.emplace_back("name", Value(entity_set.name));
                es_struct.emplace_back("entity_type", Value(entity_set.entity_type_name));
                
                // Build full URL for entity set
                std::string entity_url = base_url;
                if (!entity_url.empty() && entity_url.back() != '/') {
                    entity_url += "/";
                }
                entity_url += entity_set.name;
                
                es_struct.emplace_back("url", Value(entity_url));
                entity_sets.push_back(Value::STRUCT(es_struct));
            }
        }
    }
    
    return entity_sets.empty() ? CreateEmptyEntitySetsList() : Value::LIST(entity_sets);
}

// Helper to build functions list
static duckdb::Value BuildFunctionsList(const Edmx& metadata) {
    vector<Value> functions;
    
    for (const auto& schema : metadata.data_services.schemas) {
        for (const auto& func : schema.functions) {
            child_list_t<Value> func_struct;
            func_struct.emplace_back("name", Value(func.name));
            func_struct.emplace_back("return_type", Value(func.return_type));
            
            vector<Value> params;
            for (const auto& param : func.parameters) {
                child_list_t<Value> param_struct;
                param_struct.emplace_back("name", Value(param.name));
                param_struct.emplace_back("type", Value(param.type));
                param_struct.emplace_back("nullable", Value(param.nullable));
                params.push_back(Value::STRUCT(param_struct));
            }
            
            // Handle empty params list
            if (params.empty()) {
                child_list_t<LogicalType> param_struct;
                param_struct.emplace_back("name", LogicalType::VARCHAR);
                param_struct.emplace_back("type", LogicalType::VARCHAR);
                param_struct.emplace_back("nullable", LogicalType::BOOLEAN);
                func_struct.emplace_back("parameters", Value::LIST(LogicalType::STRUCT(param_struct), vector<Value>()));
            } else {
                func_struct.emplace_back("parameters", Value::LIST(params));
            }
            functions.push_back(Value::STRUCT(func_struct));
        }
    }
    
    return functions.empty() ? CreateEmptyFunctionsList() : Value::LIST(functions);
}

// Helper to find entity set in metadata
static std::optional<EntitySet> FindEntitySet(const Edmx& metadata, const std::string& entity_set_name) {
    // Search in all schemas and containers
    for (const auto& schema : metadata.data_services.schemas) {
        for (const auto& container : schema.entity_containers) {
            for (const auto& es : container.entity_sets) {
                if (es.name == entity_set_name) {
                    return es;
                }
            }
        }
    }
    
    // Try alternative search method
    auto all_entity_sets = metadata.FindEntitySets();
    for (const auto& es : all_entity_sets) {
        if (es.name == entity_set_name) {
            return es;
        }
    }
    
    return std::nullopt;
}

// Helper to process entity set description
static void ProcessEntitySetDescription(
    ODataDescribeBindData& bind_data,
    const Edmx& metadata,
    vector<Value>& row_values
) {
    row_values.push_back(Value(bind_data.entity_set_name));
    
    auto entity_set_opt = FindEntitySet(metadata, bind_data.entity_set_name);
    
    if (entity_set_opt.has_value()) {
        const auto& entity_set = entity_set_opt.value();
        row_values.push_back(Value(entity_set.entity_type_name));
        
        // Get entity type
        auto entity_type_variant = metadata.FindType(entity_set.entity_type_name);
        if (std::holds_alternative<EntityType>(entity_type_variant)) {
            const auto& entity_type = std::get<EntityType>(entity_type_variant);
            
            // Get key properties
            std::set<std::string> key_properties;
            for (const auto& key_ref : entity_type.key.property_refs) {
                key_properties.insert(key_ref.name);
            }
            
            // Build properties and navigation properties
            row_values.push_back(BuildPropertiesList(entity_type, key_properties));
            row_values.push_back(BuildNavigationPropertiesList(entity_type, metadata, entity_set.entity_type_name));
        } else {
            // Entity type not found
            row_values.push_back(CreateEmptyPropertyList());
            row_values.push_back(CreateEmptyNavigationList());
        }
    } else {
        // Entity set not found
        row_values.push_back(Value(""));  // entity_type_name
        row_values.push_back(CreateEmptyPropertyList());
        row_values.push_back(CreateEmptyNavigationList());
    }
    
    // Empty entity_sets and functions for entity set description
    row_values.push_back(CreateEmptyEntitySetsList());
    row_values.push_back(CreateEmptyFunctionsList());
}

// Helper to process service root description
static void ProcessServiceRootDescription(
    ODataDescribeBindData& bind_data,
    const Edmx& metadata,
    vector<Value>& row_values
) {
    row_values.push_back(Value(""));  // entity_set_name
    row_values.push_back(Value(""));  // entity_type_name
    row_values.push_back(CreateEmptyPropertyList());
    row_values.push_back(CreateEmptyNavigationList());
    row_values.push_back(BuildEntitySetsList(metadata, bind_data.url));
    row_values.push_back(BuildFunctionsList(metadata));
}

// Scan function for odata_describe
static void ODataDescribeScan(
    ClientContext &context,
    TableFunctionInput &data_p,
    DataChunk &output
) {
    auto &bind_data = data_p.bind_data->CastNoConst<ODataDescribeBindData>();
    
    if (!bind_data.data_loaded) {
        ERPL_TRACE_INFO("ODATA_DESCRIBE_SCAN", "Loading metadata for: " + bind_data.url);
        
        // Load metadata
        Edmx metadata;
        try {
            metadata = (bind_data.resource_type == "service") 
                ? bind_data.service_client->GetMetadata()
                : bind_data.entity_client->GetMetadata();
        } catch (const std::exception& e) {
            throw InvalidInputException("Failed to fetch metadata: " + std::string(e.what()));
        }
        
        // Build result row
        vector<Value> row_values;
        row_values.push_back(Value(bind_data.url));
        row_values.push_back(Value(bind_data.resource_type));
        
        if (bind_data.resource_type == "entity_set") {
            ProcessEntitySetDescription(bind_data, metadata, row_values);
        } else {
            ProcessServiceRootDescription(bind_data, metadata, row_values);
        }
        
        bind_data.result_row = row_values;
        bind_data.data_loaded = true;
        ERPL_TRACE_INFO("ODATA_DESCRIBE_SCAN", "Metadata loaded successfully");
    }
    
    // Output single row
    if (!bind_data.result_row.empty()) {
        for (idx_t i = 0; i < bind_data.result_row.size(); i++) {
            output.SetValue(i, 0, bind_data.result_row[i]);
        }
        output.SetCardinality(1);
        bind_data.result_row.clear();  // Clear to indicate we've returned the row
    } else {
        output.SetCardinality(0);
    }
}

// Create the odata_describe function
TableFunctionSet CreateODataDescribeFunction() {
    TableFunctionSet describe_func("odata_describe");
    
    TableFunction describe_function({LogicalType::VARCHAR}, ODataDescribeScan, ODataDescribeBind);
    describe_function.named_parameters["secret"] = LogicalType::VARCHAR;
    
    describe_func.AddFunction(describe_function);
    return describe_func;
}

} // namespace erpl_web
