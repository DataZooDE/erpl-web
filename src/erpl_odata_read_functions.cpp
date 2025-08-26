
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <numeric>

#include "erpl_odata_read_functions.hpp"
#include "erpl_http_client.hpp"
#include "yyjson.hpp"

#include "telemetry.hpp"
#include "erpl_tracing.hpp"

namespace erpl_web {

// Helper function to extract column names from a JSON object
static void ExtractColumnNames(duckdb_yyjson::yyjson_val *first_row, 
                              std::vector<std::string>& extracted_column_names) {
    // Extract column names from first row
    duckdb_yyjson::yyjson_obj_iter it;
    duckdb_yyjson::yyjson_obj_iter_init(first_row, &it);
    duckdb_yyjson::yyjson_val *k;
    
    // Use a map to store key-value pairs to maintain order
    std::map<std::string, std::string> column_map;
    
    while ((k = duckdb_yyjson::yyjson_obj_iter_next(&it))) {
        auto key = duckdb_yyjson::yyjson_get_str(k);
        
        // Skip metadata fields that start with "__"
        if (key[0] == '_' && key[1] == '_') {
            continue;
        }
        
        column_map[key] = key; // Store in map to maintain order
    }
    
    // Extract column names in sorted order for consistency
    for (const auto& pair : column_map) {
        extracted_column_names.emplace_back(pair.first);
    }
    
    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Extracted " + std::to_string(extracted_column_names.size()) + " column names from first data row");
    
    // Log the extracted column names for debugging
    for (size_t i = 0; i < extracted_column_names.size(); ++i) {
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Column " + std::to_string(i) + ": " + extracted_column_names[i]);
    }
}

duckdb::unique_ptr<ODataReadBindData> ODataReadBindData::FromEntitySetRoot(const std::string& entity_set_url, 
                                                                           std::shared_ptr<HttpAuthParams> auth_params)
{
    auto http_client = std::make_shared<HttpClient>();
    auto odata_client = std::make_shared<ODataEntitySetClient>(http_client, entity_set_url, auth_params);
    
    // Check if this is a Datasphere URL (contains specific Datasphere patterns)
    bool is_datasphere_url = entity_set_url.find("datasphere") != std::string::npos || 
                             entity_set_url.find("hcs.cloud.sap") != std::string::npos;
    
    // For OData v2 services, metadata might not be available, so we'll use direct HTTP approach
    bool is_odata_v2_url = entity_set_url.find("/V2/") != std::string::npos;
    
    // Use direct HTTP approach for both Datasphere and OData v2 services
    bool use_direct_http = is_datasphere_url || is_odata_v2_url;
    
    // Set OData version directly for OData v2 to skip metadata fetching
    if (is_odata_v2_url) {
        odata_client->SetODataVersionDirectly(ODataVersion::V2);
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Set OData version to V2 directly to skip metadata fetching");
    }
    
    std::vector<std::string> extracted_column_names;
    
    if (use_direct_http) {
        // For Datasphere dual-URL pattern or OData v2 services, make a direct HTTP request to get first page
        // This bypasses the metadata requirement and allows us to extract @odata.context or extract column names
        if (is_datasphere_url) {
            ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Detected Datasphere URL, making direct HTTP request to get first data page for @odata.context");
        } else {
            ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Detected OData v2 URL, making direct HTTP request to extract column names");
        }
        try {
            HttpRequest req(HttpMethod::GET, HttpUrl(entity_set_url));
            req.AuthHeadersFromParams(*auth_params);
            req.headers["Accept"] = "application/json";
            auto resp = http_client->SendRequest(req);
            if (resp && resp->Code() == 200) {
                ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Successfully fetched first data page via direct HTTP");
                
                // Parse the response to extract @odata.context and column names
                auto content = resp->Content();
                ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Parsing JSON response of " + std::to_string(content.size()) + " bytes");
                auto doc = duckdb_yyjson::yyjson_read(content.c_str(), content.size(), 0);
                if (doc) {
                    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Successfully parsed JSON document");
                    auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
                    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Got JSON root object");
                    // Check for @odata.context (OData v4)
                    auto context_val = duckdb_yyjson::yyjson_obj_get(root, "@odata.context");
                    if (context_val && duckdb_yyjson::yyjson_is_str(context_val)) {
                        auto context_url = duckdb_yyjson::yyjson_get_str(context_val);
                        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Extracted @odata.context: " + std::string(context_url));
                        
                        // Store the metadata context URL in the OData client for future metadata requests
                        // Remove hash fragment if present
                        std::string clean_context_url = context_url;
                        auto hash_pos = clean_context_url.find('#');
                        if (hash_pos != std::string::npos) {
                            clean_context_url = clean_context_url.substr(0, hash_pos);
                        }
                        if (is_datasphere_url) {
                            odata_client->SetMetadataContextUrl(clean_context_url);
                            ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Stored metadata context URL in OData client: " + clean_context_url);
                            // Also extract and store entity set name from context fragment
                            odata_client->SetEntitySetNameFromContextFragment(context_url);
                        }
                        
                        // Extract column names and infer types from the first data row (OData v4 format)
                        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Parsing OData v4 response format");
                        auto value_arr = duckdb_yyjson::yyjson_obj_get(root, "value");
                        if (value_arr && duckdb_yyjson::yyjson_is_arr(value_arr)) {
                            ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Found 'value' array in OData v4 response");
                            duckdb_yyjson::yyjson_arr_iter arr_it;
                            duckdb_yyjson::yyjson_arr_iter_init(value_arr, &arr_it);
                            duckdb_yyjson::yyjson_val *first_row = duckdb_yyjson::yyjson_arr_iter_next(&arr_it);
                            if (first_row && duckdb_yyjson::yyjson_is_obj(first_row)) {
                                ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Found first row object in OData v4 response");
                                ExtractColumnNames(first_row, extracted_column_names);
                                // Guard: service document typically returns only name/url entries
                                if (extracted_column_names.size() == 2 &&
                                    ((extracted_column_names[0] == "name" && extracted_column_names[1] == "url") ||
                                     (extracted_column_names[0] == "url" && extracted_column_names[1] == "name"))) {
                                    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Detected service document columns (name,url); ignoring extracted columns to defer to metadata");
                                    extracted_column_names.clear();
                                    // Additionally, try to pick entity name from service document entries
                                    // Prefer exact match with last segment of URL, otherwise first entry
                                    std::string service_entity_name;
                                    // Determine last segment hint from entity_set_url
                                    {
                                        std::string path_hint = HttpUrl(entity_set_url).Path();
                                        if (!path_hint.empty() && path_hint.back() == '/') {
                                            path_hint.pop_back();
                                        }
                                        auto pos = path_hint.find_last_of('/');
                                        if (pos != std::string::npos) {
                                            path_hint = path_hint.substr(pos + 1);
                                        }
                                        // Iterate value array to find matching name/url
                                        duckdb_yyjson::yyjson_arr_iter it2;
                                        duckdb_yyjson::yyjson_arr_iter_init(value_arr, &it2);
                                        duckdb_yyjson::yyjson_val *row;
                                        while ((row = duckdb_yyjson::yyjson_arr_iter_next(&it2))) {
                                            auto name_v = duckdb_yyjson::yyjson_obj_get(row, "name");
                                            auto url_v = duckdb_yyjson::yyjson_obj_get(row, "url");
                                            std::string name_s = (name_v && duckdb_yyjson::yyjson_is_str(name_v)) ? duckdb_yyjson::yyjson_get_str(name_v) : "";
                                            std::string url_s = (url_v && duckdb_yyjson::yyjson_is_str(url_v)) ? duckdb_yyjson::yyjson_get_str(url_v) : "";
                                            if (!path_hint.empty() && (name_s == path_hint || url_s == path_hint)) {
                                                service_entity_name = name_s.empty() ? url_s : name_s;
                                                break;
                                            }
                                            if (service_entity_name.empty() && !name_s.empty()) {
                                                service_entity_name = name_s; // fallback to first
                                            }
                                        }
                                    }
                                    if (!service_entity_name.empty()) {
                                        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Setting entity set name from service document: " + service_entity_name);
                                        odata_client->SetEntitySetName(service_entity_name);
                                    }
                                }
                            } else {
                                ERPL_TRACE_WARN("ODATA_READ_BIND", "First row is not an object in OData v4 response");
                            }
                        } else {
                            ERPL_TRACE_WARN("ODATA_READ_BIND", "Could not find 'value' array in OData v4 response");
                        }
                    } else {
                        // No @odata.context found, try OData v2 format
                        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "No @odata.context found, trying OData v2 format");
                        auto data_obj = duckdb_yyjson::yyjson_obj_get(root, "d");
                        if (data_obj && duckdb_yyjson::yyjson_is_obj(data_obj)) {
                            ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Found 'd' object in OData v2 response");
                            auto results_arr = duckdb_yyjson::yyjson_obj_get(data_obj, "results");
                            if (results_arr && duckdb_yyjson::yyjson_is_arr(results_arr)) {
                                ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Found 'results' array in OData v2 response");
                                duckdb_yyjson::yyjson_arr_iter arr_it;
                                duckdb_yyjson::yyjson_arr_iter_init(results_arr, &arr_it);
                                duckdb_yyjson::yyjson_val *first_row = duckdb_yyjson::yyjson_arr_iter_next(&arr_it);
                                if (first_row && duckdb_yyjson::yyjson_is_obj(first_row)) {
                                    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Found first row object in OData v2 response");
                                    ExtractColumnNames(first_row, extracted_column_names);
                                } else {
                                    ERPL_TRACE_WARN("ODATA_READ_BIND", "First row is not an object in OData v2 response");
                                }
                            } else {
                                ERPL_TRACE_WARN("ODATA_READ_BIND", "Could not find 'results' array in OData v2 response");
                            }
                        } else {
                            ERPL_TRACE_WARN("ODATA_READ_BIND", "Could not find 'd' object in OData v2 response");
                        }
                    }
                    duckdb_yyjson::yyjson_doc_free(doc);
                }
            } else {
                ERPL_TRACE_WARN("ODATA_READ_BIND", "Direct HTTP request failed with status: " + std::to_string(resp ? resp->Code() : 0));
            }
        } catch (const std::exception& e) {
            ERPL_TRACE_WARN("ODATA_READ_BIND", "Failed to fetch first data page via direct HTTP: " + std::string(e.what()));
            // Continue without first page - will fall back to conventional metadata
        }
    } else {
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Standard OData v4 URL detected, using conventional metadata approach");
    }
    
    auto bind_data = duckdb::make_uniq<ODataReadBindData>(odata_client);
    
    // If we successfully extracted column names, store them for later use
    if (!extracted_column_names.empty()) {
        bind_data->SetExtractedColumnNames(extracted_column_names);
    }
    
    return bind_data;
}

ODataReadBindData::ODataReadBindData(std::shared_ptr<ODataEntitySetClient> odata_client)
    : odata_client(odata_client)
{ 
    predicate_pushdown_helper = nullptr;
}

std::vector<std::string> ODataReadBindData::GetResultNames(bool all_columns)
{
    // If we have extracted column names from the first data row, use those
    if (!extracted_column_names.empty()) {
        if (all_columns || active_column_ids.empty()) {
            return extracted_column_names;
        }

        std::vector<std::string> active_result_names;
        for (auto &column_id : active_column_ids) {
            if (duckdb::IsRowIdColumnId(column_id)) {
                continue;
            }

            active_result_names.push_back(extracted_column_names[column_id]);
        }

        return active_result_names;
    }

    // Fall back to getting names from the OData client (metadata)
    if (all_result_names.empty()) {
        all_result_names = odata_client->GetResultNames();
    }

    if (all_columns || active_column_ids.empty()) {
        return all_result_names;
    }

    std::vector<std::string> active_result_names;
    for (auto &column_id : active_column_ids) {
        if (duckdb::IsRowIdColumnId(column_id)) {
            continue;
        }

        active_result_names.push_back(all_result_names[column_id]);
    }

    return active_result_names;
}

std::vector<duckdb::LogicalType> ODataReadBindData::GetResultTypes(bool all_columns)
{
    // Only get types from the OData client (metadata) if we haven't mapped them yet
    if (all_result_types.empty()) {
        all_result_types = odata_client->GetResultTypes();
        
        // If we have extracted column names, align types to those names
        if (!extracted_column_names.empty()) {
            if (extracted_column_names.size() == all_result_types.size()) {
                // Create a mapping from extracted column names to metadata types
                std::vector<duckdb::LogicalType> mapped_types;
                mapped_types.reserve(extracted_column_names.size());
                
                // Get the metadata column names to map them to extracted names
                auto metadata_names = odata_client->GetResultNames();
                
                for (const auto& extracted_name : extracted_column_names) {
                    // Find the index of this column in the metadata
                    auto it = std::find(metadata_names.begin(), metadata_names.end(), extracted_name);
                    if (it != metadata_names.end()) {
                        size_t metadata_index = std::distance(metadata_names.begin(), it);
                        if (metadata_index < all_result_types.size()) {
                            mapped_types.push_back(all_result_types[metadata_index]);
                            ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Mapped column '" + extracted_name + "' to type: " + all_result_types[metadata_index].ToString());
                        } else {
                            // Fallback to VARCHAR if metadata index is out of bounds
                            mapped_types.push_back(duckdb::LogicalType::VARCHAR);
                            ERPL_TRACE_WARN("ODATA_READ_BIND", "Column '" + extracted_name + "' metadata index out of bounds, using VARCHAR fallback");
                        }
                    } else {
                        // Fallback to VARCHAR if column not found in metadata
                        mapped_types.push_back(duckdb::LogicalType::VARCHAR);
                        ERPL_TRACE_WARN("ODATA_READ_BIND", "Column '" + extracted_name + "' not found in metadata, using VARCHAR fallback");
                    }
                }
                
                // Replace all_result_types with the mapped types
                all_result_types = mapped_types;
            } else {
                // Sizes mismatch (common in OData v2 when inferring from data) -> default all to VARCHAR
                ERPL_TRACE_INFO("ODATA_READ_BIND", duckdb::StringUtil::Format(
                    "Metadata column count (%d) does not match extracted column count (%d); defaulting all types to VARCHAR",
                    all_result_types.size(), extracted_column_names.size()));
                all_result_types.assign(extracted_column_names.size(), duckdb::LogicalType::VARCHAR);
            }
        }
    }

    if (all_columns || active_column_ids.empty()) {
        return all_result_types;
    }

    std::vector<duckdb::LogicalType> active_result_types;
    for (auto &column_id : active_column_ids) {
        if (duckdb::IsRowIdColumnId(column_id)) {
            continue;
        }

        active_result_types.push_back(all_result_types[column_id]);
    }

    return active_result_types;
}

unsigned int ODataReadBindData::FetchNextResult(duckdb::DataChunk &output)
{
    // Ensure input parameters are set on client before any request
    if (!input_parameters.empty()) {
        odata_client->SetInputParameters(input_parameters);
    }

    // Make sure first page is prefetched once and buffered
    if (!first_page_cached_) {
        PrefetchFirstPage();
    }

    auto result_names = GetResultNames();
    auto result_types = GetResultTypes();

    const idx_t target = STANDARD_VECTOR_SIZE;
    idx_t emitted = 0;
    auto null_value = duckdb::Value();

    // Fetch additional pages until we have enough buffered rows to fill the vector or no more pages
    while (row_buffer_.size() < target && has_next_page_) {
        auto next_response = odata_client->Get(true);
        if (!next_response) {
            has_next_page_ = false;
            break;
        }
        // Capture total count once for progress
        if (next_response->GetODataVersion() == ODataVersion::V4) {
            auto total = next_response->Content()->TotalCount();
            if (total.has_value() && (!progress_has_total_ || progress_total_count_ == 0)) {
                progress_total_count_ = total.value();
                progress_has_total_ = true;
            }
        }
        auto page_rows = next_response->ToRows(result_names, result_types);
        for (auto &r : page_rows) {
            row_buffer_.emplace_back(std::move(r));
        }
        has_next_page_ = next_response->NextUrl().has_value();
    }

    // Emit up to target rows from buffer
    idx_t to_emit = std::min<idx_t>(row_buffer_.size(), target);
    for (idx_t i = 0; i < to_emit; i++) {
        const auto &row = row_buffer_.front();
        for (idx_t j = 0; j < output.ColumnCount(); j++) {
            if (j >= result_names.size()) {
                auto col_type = output.data[j].GetType();
                output.SetValue(j, i, null_value.DefaultCastAs(col_type));
            } else {
                output.SetValue(j, i, row[j]);
            }
        }
        row_buffer_.pop_front();
        emitted++;
    }

    output.SetCardinality(emitted);

    // Update cumulative progress
    progress_rows_fetched_ += static_cast<uint64_t>(emitted);
    if (progress_has_total_ && progress_total_count_ > 0) {
        double pct = 100.0 * (double)progress_rows_fetched_ / (double)progress_total_count_;
        if (pct > 100.0) pct = 100.0;
        ERPL_TRACE_INFO("ODATA_SCAN", duckdb::StringUtil::Format(
            "Progress: %.2f%% (%llu/%llu)",
            pct,
            (unsigned long long)progress_rows_fetched_,
            (unsigned long long)progress_total_count_));
    }

    return emitted;
}

bool ODataReadBindData::HasMoreResults()
{
    // If buffer still has rows, we have results to emit
    if (!row_buffer_.empty()) {
        return true;
    }
    // If first page hasn't been cached yet, we need to deliver it
    if (!first_page_cached_) {
        return true;
    }
    // Otherwise, only if server indicated a next page
    return has_next_page_;
}

void ODataReadBindData::ActivateColumns(const std::vector<duckdb::column_t> &column_ids)
{
    std::stringstream column_ids_str;
    for (auto &column_id : column_ids) {
        column_ids_str << column_id << ", ";
    }
    ERPL_TRACE_DEBUG("ODATA_READ_BIND", duckdb::StringUtil::Format("Activating columns: %s", column_ids_str.str().c_str()));
    
    active_column_ids = column_ids;
    
    // Build the mapping from activated column index to original column name index
    activated_to_original_mapping.clear();
    activated_to_original_mapping.resize(column_ids.size());
    
    for (size_t i = 0; i < column_ids.size(); ++i) {
        activated_to_original_mapping[i] = column_ids[i];
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", duckdb::StringUtil::Format("Mapping activated index %d to original index %d", i, column_ids[i]));
    }
    
    PredicatePushdownHelper()->ConsumeColumnSelection(column_ids);

    ERPL_TRACE_DEBUG("ODATA_READ_BIND", duckdb::StringUtil::Format("Select clause: %s", PredicatePushdownHelper()->SelectClause().c_str()));
}

void ODataReadBindData::AddFilters(const duckdb::optional_ptr<duckdb::TableFilterSet> &filters)
{
    if (filters && !filters->filters.empty()) {
        std::stringstream filters_str;
        for (auto &[projected_column_idx, filter] : filters->filters) 
        {
            filters_str << "Column " << projected_column_idx << ": " << filter->DebugToString() << std::endl;
        }
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", duckdb::StringUtil::Format("Adding %d filters: %s", filters->filters.size(), filters_str.str().c_str()));
        
        //ERPL_TRACE_DEBUG("ODATA_READ_BIND", duckdb::StringUtil::Format("Adding %d filters", filters->filters.size()));
        PredicatePushdownHelper()->ConsumeFilters(filters);
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", duckdb::StringUtil::Format("Filter clause: %s", PredicatePushdownHelper()->FilterClause().c_str()));
    } else {
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "No filters to add");
    }
}

void ODataReadBindData::AddResultModifiers(const std::vector<duckdb::unique_ptr<duckdb::BoundResultModifier>> &modifiers)
{
    if (!modifiers.empty()) {
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", duckdb::StringUtil::Format("Adding %d result modifiers", modifiers.size()));
        PredicatePushdownHelper()->ConsumeResultModifiers(modifiers);
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Result modifiers processed");
    } else {
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "No result modifiers to add");
    }
}

void ODataReadBindData::UpdateUrlFromPredicatePushdown()
{
    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Updating URL from predicate pushdown");
    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Original URL: " + odata_client->Url());
    
    auto http_client = odata_client->GetHttpClient();
    auto auth_params = odata_client->AuthParams();
    auto updated_url = PredicatePushdownHelper()->ApplyFiltersToUrl(odata_client->Url());
    
    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Updated URL: " + updated_url.ToString());
    
    // Store the current OData version before creating new client
    auto current_version = odata_client->GetODataVersion();

    odata_client = std::make_shared<ODataEntitySetClient>(http_client, updated_url, auth_params);
    
    // Preserve the OData version to avoid metadata fetching
    if (current_version != ODataVersion::UNKNOWN) {
        odata_client->SetODataVersionDirectly(current_version);
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", duckdb::StringUtil::Format("Preserved OData version %s on new client", current_version == ODataVersion::V2 ? "V2" : "V4"));
    }
}

std::shared_ptr<ODataPredicatePushdownHelper> ODataReadBindData::PredicatePushdownHelper()
{
    if (predicate_pushdown_helper == nullptr) {
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Creating new predicate pushdown helper");
        
        // Use extracted column names if available, otherwise fall back to OData client
        std::vector<std::string> column_names;
        if (!extracted_column_names.empty()) {
            column_names = extracted_column_names;
            ERPL_TRACE_DEBUG("ODATA_READ_BIND", duckdb::StringUtil::Format("Using extracted column names for predicate pushdown: %d columns", column_names.size()));
        } else {
            column_names = odata_client->GetResultNames();
            ERPL_TRACE_DEBUG("ODATA_READ_BIND", duckdb::StringUtil::Format("Using OData client column names for predicate pushdown: %d columns", column_names.size()));
        }
        predicate_pushdown_helper = std::make_shared<ODataPredicatePushdownHelper>(column_names);
        
        // Set the column name resolver to use our mapping
        predicate_pushdown_helper->SetColumnNameResolver([this](duckdb::column_t activated_column_index) -> std::string {
            return this->GetOriginalColumnName(activated_column_index);
        });
    }

    return predicate_pushdown_helper;
}

double ODataReadBindData::GetProgressFraction() const
{
    if (!progress_has_total_ || progress_total_count_ == 0) {
        return -1.0; // unknown -> DuckDB will not show progress
    }
    // Include buffered rows so progress moves while we are prefetching pages to fill the chunk
    uint64_t rows_seen = progress_rows_fetched_ + (uint64_t)row_buffer_.size();
    double progress = 100 * (double)rows_seen / (double)progress_total_count_;
    if (progress < 0.0) progress = 0.0;
	return MinValue<double>(100, progress);
}

void ODataReadBindData::PrefetchFirstPage()
{
    if (first_page_cached_) {
        return;
    }
    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "PrefetchFirstPage: starting first page fetch");

    // Ensure input parameters are set on client before any request
    if (!input_parameters.empty()) {
        odata_client->SetInputParameters(input_parameters);
    }

    auto response = odata_client->Get();
    if (!response) {
        ERPL_TRACE_WARN("ODATA_READ_BIND", "PrefetchFirstPage: no response received");
        first_page_cached_ = true;
        has_next_page_ = false;
        return;
    }

    // Capture total count once for progress (v4 only, when available)
    if (response->GetODataVersion() == ODataVersion::V4) {
        auto total = response->Content()->TotalCount();
        if (total.has_value()) {
            progress_total_count_ = total.value();
            progress_has_total_ = true;
            ERPL_TRACE_INFO("ODATA_READ_BIND", duckdb::StringUtil::Format(
                "Service reported total row count: %llu",
                (unsigned long long)progress_total_count_));
        }
    }

    auto result_names = GetResultNames();
    auto result_types = GetResultTypes();

    auto page_rows = response->ToRows(result_names, result_types);
    for (auto &r : page_rows) {
        row_buffer_.emplace_back(std::move(r));
    }

    has_next_page_ = response->NextUrl().has_value();
    first_page_cached_ = true;

    ERPL_TRACE_DEBUG("ODATA_READ_BIND", duckdb::StringUtil::Format(
        "PrefetchFirstPage: buffered %d rows, has_next_page=%s",
        (int)row_buffer_.size(), has_next_page_ ? "true" : "false"));
}

void ODataReadBindData::SetExtractedColumnNames(const std::vector<std::string>& column_names)
{
    extracted_column_names = column_names;
    ERPL_TRACE_DEBUG("ODATA_READ_BIND", duckdb::StringUtil::Format("Stored %d extracted column names", column_names.size()));
}

std::string ODataReadBindData::GetOriginalColumnName(duckdb::column_t activated_column_index) const
{
    if (activated_column_index >= activated_to_original_mapping.size()) {
        // This is not an error - it's a column that wasn't activated (not selected by user)
        // Just return empty string for columns we don't have mapping for
        return "";
    }
    
    auto original_index = activated_to_original_mapping[activated_column_index];
    
    // Use extracted column names if available, otherwise fall back to all_result_names
    if (!extracted_column_names.empty()) {
        if (original_index >= extracted_column_names.size()) {
            ERPL_TRACE_ERROR("ODATA_READ_BIND", duckdb::StringUtil::Format("Original column index %d is out of bounds for extracted column names", original_index));
            return "";
        }
        auto column_name = extracted_column_names[original_index];
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", duckdb::StringUtil::Format("Activated index %d maps to original index %d which is column '%s' (from extracted names)", activated_column_index, original_index, column_name.c_str()));
        return column_name;
    } else {
        if (original_index >= all_result_names.size()) {
            ERPL_TRACE_ERROR("ODATA_READ_BIND", duckdb::StringUtil::Format("Original column index %d is out of bounds for result names", original_index));
            return "";
        }
        auto column_name = all_result_names[original_index];
        ERPL_TRACE_DEBUG("ODATA_READ_BIND", duckdb::StringUtil::Format("Activated index %d maps to original index %d which is column '%s' (from result names)", activated_column_index, original_index, column_name.c_str()));
        return column_name;
    }
}

void ODataReadBindData::SetInputParameters(const std::map<std::string, std::string>& input_params)
{
    input_parameters = input_params;
    ERPL_TRACE_DEBUG("ODATA_READ_BIND", "Stored " + std::to_string(input_params.size()) + " input parameters");
}

const std::map<std::string, std::string>& ODataReadBindData::GetInputParameters() const
{
    return input_parameters;
}

std::shared_ptr<ODataEntitySetClient> ODataReadBindData::GetODataClient() const
{
    return odata_client;
}



// -------------------------------------------------------------------------------------------------

static std::shared_ptr<HttpAuthParams> AuthParamsFromInput(duckdb::ClientContext &context, TableFunctionBindInput &input)
{
    auto args = input.inputs;
    auto url = args[0].ToString();
    return HttpAuthParams::FromDuckDbSecrets(context, url);
}

duckdb::unique_ptr<FunctionData> ODataReadBind(ClientContext &context, 
                                               TableFunctionBindInput &input, 
                                               vector<LogicalType> &return_types, 
                                               vector<string> &names) 
{
    auto auth_params = AuthParamsFromInput(context, input);
    auto url = input.inputs[0].GetValue<std::string>();
    
    ERPL_TRACE_INFO("ODATA_BIND", duckdb::StringUtil::Format("Binding OData read function for entity set: %s", url.c_str()));
    if (auth_params) {
        ERPL_TRACE_DEBUG("ODATA_BIND", "Using authentication parameters");
    }
    
    auto bind_data = ODataReadBindData::FromEntitySetRoot(url, auth_params);

    // Handle named parameters for TOP and SKIP
    if (input.named_parameters.find("top") != input.named_parameters.end()) {
        auto limit_value = input.named_parameters["top"].GetValue<duckdb::idx_t>();
        ERPL_TRACE_DEBUG("ODATA_BIND", duckdb::StringUtil::Format("Named parameter 'top' set to: %d", limit_value));
        bind_data->PredicatePushdownHelper()->ConsumeLimit(limit_value);
    }
    
    if (input.named_parameters.find("skip") != input.named_parameters.end()) {
        auto offset_value = input.named_parameters["skip"].GetValue<duckdb::idx_t>();
        ERPL_TRACE_DEBUG("ODATA_BIND", duckdb::StringUtil::Format("Named parameter 'skip' set to: %d", offset_value));
        bind_data->PredicatePushdownHelper()->ConsumeOffset(offset_value);
    }

    names = bind_data->GetResultNames();
    return_types = bind_data->GetResultTypes();
    
    ERPL_TRACE_INFO("ODATA_BIND", duckdb::StringUtil::Format("Bound function with %d columns", return_types.size()));
    
    // More efficient string concatenation for debug logging
    if (!names.empty()) {
        std::string column_names;
        column_names.reserve(names.size() * 20); // Estimate average column name length
        for (size_t i = 0; i < names.size(); ++i) {
            if (i > 0) column_names += ", ";
            column_names += names[i];
        }
        ERPL_TRACE_DEBUG("ODATA_BIND", duckdb::StringUtil::Format("Column names: %s", column_names.c_str()));
    }
    
    return std::move(bind_data);
}

unique_ptr<GlobalTableFunctionState> ODataReadTableInitGlobalState(ClientContext &context,
                                                                   TableFunctionInitInput &input) 
{
    auto &bind_data = input.bind_data->CastNoConst<ODataReadBindData>();
    auto column_ids = input.column_ids;

    bind_data.ActivateColumns(column_ids);
    bind_data.AddFilters(input.filters);
    
    bind_data.UpdateUrlFromPredicatePushdown();
    // Prefetch first page after URL is finalized so progress can show early and tiny scans return immediately
    bind_data.PrefetchFirstPage();

    return duckdb::make_uniq<GlobalTableFunctionState>();
}

double ODataReadTableProgress(ClientContext &, const FunctionData *func_data, const GlobalTableFunctionState *)
{
    auto &bind_data = func_data->CastNoConst<ODataReadBindData>();
    ERPL_TRACE_DEBUG("ODATA_READ_TABLE_PROGRESS", "Progress fraction: " + std::to_string(bind_data.GetProgressFraction()));

    return bind_data.GetProgressFraction();
}

void ODataReadScan(ClientContext &context, 
                   TableFunctionInput &data, 
                   DataChunk &output) 
{
    auto &bind_data = data.bind_data->CastNoConst<ODataReadBindData>();
    
    ERPL_TRACE_DEBUG("ODATA_SCAN", "Starting OData scan operation");
    
    if (! bind_data.HasMoreResults()) {
        ERPL_TRACE_DEBUG("ODATA_SCAN", "No more results available");
        return;
    }
    
    ERPL_TRACE_DEBUG("ODATA_SCAN", "Fetching next result set");
    auto rows_fetched = bind_data.FetchNextResult(output);
    ERPL_TRACE_INFO("ODATA_SCAN", duckdb::StringUtil::Format("Fetched %d rows", rows_fetched));
}

TableFunctionSet CreateODataReadFunction() 
{
    TableFunctionSet function_set("odata_read");
    
    TableFunction read_entity_set({LogicalType::VARCHAR}, ODataReadScan, ODataReadBind, ODataReadTableInitGlobalState);
    read_entity_set.filter_pushdown = true;
    read_entity_set.projection_pushdown = true;
    read_entity_set.table_scan_progress = ODataReadTableProgress;
    
    // Add named parameters for TOP and SKIP
    read_entity_set.named_parameters["top"] = LogicalType::UBIGINT;
    read_entity_set.named_parameters["skip"] = LogicalType::UBIGINT;

    function_set.AddFunction(read_entity_set);
    return function_set;
}


} // namespace erpl_web
