#include <cpptrace/cpptrace.hpp>
#include <regex>

#include "erpl_odata_client.hpp"
#include "erpl_tracing.hpp"

namespace erpl_web {


// ----------------------------------------------------------------------

ODataEntitySetResponse::ODataEntitySetResponse(std::unique_ptr<HttpResponse> http_response, ODataVersion odata_version)
    : ODataResponse(std::move(http_response))
    , odata_version(odata_version)
{ 
    ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Created OData entity set response");
    ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Response content type: " + ContentType());
    ERPL_TRACE_DEBUG("ODATA_RESPONSE", std::string("OData version: ") + (odata_version == ODataVersion::V2 ? "V2" : "V4"));
}

std::string ODataEntitySetResponse::MetadataContextUrl()
{
    return Content()->MetadataContextUrl();
}

std::optional<std::string> ODataEntitySetResponse::NextUrl()
{
    return Content()->NextUrl();
}

std::vector<std::vector<duckdb::Value>> ODataEntitySetResponse::ToRows(std::vector<std::string> &column_names, 
                                                             std::vector<duckdb::LogicalType> &column_types)
{
    return Content()->ToRows(column_names, column_types);
}

// ----------------------------------------------------------------------

// ODataClient base class implementation is in erpl_odata_client.hpp

template<typename TResponse>
void ODataClient<TResponse>::DetectODataVersion()
{
    // If we already know the version, don't fetch metadata again
    if (odata_version != ODataVersion::UNKNOWN) {
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "OData version already detected, skipping metadata fetch");
        return;
    }
    
    // For Datasphere, prefer using @odata.context from prior responses; do not skip metadata
    
    // Get the current metadata to detect version
    auto metadata_url = GetMetadataContextUrl();
    
    // Check if we already have cached metadata for this URL
    auto cached_edmx = EdmCache::GetInstance().Get(metadata_url);
    if (cached_edmx) {
        // Version is already detected from the cached metadata
        // Extract the version from cached metadata and update client version
        odata_version = cached_edmx->GetVersion();
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "Using cached metadata, detected version: " + std::string(odata_version == ODataVersion::V2 ? "V2" : "V4"));
        return;
    }

    ERPL_TRACE_INFO("ODATA_CLIENT", "Fetching metadata to detect OData version from: " + metadata_url);
    
    // Fetch metadata to detect version
    auto metadata_response = DoMetadataHttpGet(metadata_url);
    auto content = metadata_response->Content();
    
    // Parse metadata to detect version
    auto edmx = Edmx::FromXml(content);
    odata_version = edmx.GetVersion();
    
    ERPL_TRACE_INFO("ODATA_CLIENT", "Detected OData version: " + std::string(odata_version == ODataVersion::V2 ? "V2" : "V4"));
    
    // Cache the metadata with detected version
    EdmCache::GetInstance().Set(metadata_url, edmx);
}

// ----------------------------------------------------------------------

ODataEntitySetClient::ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url, const Edmx& edmx)
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url, nullptr)
{ }

ODataEntitySetClient::ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url)
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url, nullptr)
{ }

ODataEntitySetClient::ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url, const Edmx& edmx, std::shared_ptr<HttpAuthParams> auth_params)
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url, auth_params)
{ }

ODataEntitySetClient::ODataEntitySetClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url, std::shared_ptr<HttpAuthParams> auth_params)
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url, auth_params)
{ }

std::string ODataEntitySetClient::GetMetadataContextUrl()
{
    // If we have input parameters, we need to regenerate the metadata URL
    // because the cached URL might be incorrect for the parameterized request
    if (!input_parameters.empty()) {
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "Input parameters present, clearing cached metadata URL");
        metadata_context_url.clear();
    }
    
    // First, check if we have a stored metadata context URL (for Datasphere dual-URL pattern)
    if (!metadata_context_url.empty()) {
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "Using stored metadata context URL: " + metadata_context_url);
        return metadata_context_url;
    }
    
    // Prefer @odata.context from last data/service response if available
    if (current_response) {
        auto ctx = current_response->MetadataContextUrl();
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "Found @odata.context: " + ctx);
        if (!ctx.empty()) {
            auto hash_pos = ctx.find('#');
            if (hash_pos != std::string::npos) {
                ctx = ctx.substr(0, hash_pos);
                ERPL_TRACE_DEBUG("ODATA_CLIENT", "Removed hash fragment, context URL: " + ctx);
            }
            HttpUrl meta_url = HttpUrl::MergeWithBaseUrlIfRelative(url, ctx);
            auto final_url = meta_url.ToString();
            ERPL_TRACE_INFO("ODATA_CLIENT", "Using @odata.context metadata URL: " + final_url);
            // Store this URL for future use to avoid repeated fallback URL generation
            metadata_context_url = final_url;
            return final_url;
        } else {
            ERPL_TRACE_DEBUG("ODATA_CLIENT", "No @odata.context found in current response");
        }
    } else {
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "No current response available for @odata.context");
    }
    
    // Fallback to conventional $metadata next to service root
    ERPL_TRACE_INFO("ODATA_CLIENT", "Determining metadata URL for: " + url.ToString());
    ERPL_TRACE_INFO("ODATA_CLIENT", "Using fallback metadata URL generation");
    
    HttpUrl base(url);
    
    // For Datasphere URLs, metadata is at the service root level
    // Extract the service root by removing entity set and query parameters
    auto path = base.Path();
    auto query = base.Query();
    
    ERPL_TRACE_INFO("ODATA_CLIENT", "Processing path: " + path);
    ERPL_TRACE_INFO("ODATA_CLIENT", "Path contains /api/v1/dwc/consumption/relational/: " + std::string(path.find("/api/v1/dwc/consumption/relational/") != std::string::npos ? "true" : "false"));
    
    // Remove query parameters from metadata URL
    base.Query("");
    
    // For Datasphere URLs, the structure is typically:
    // /api/v1/dwc/consumption/relational/{tenant}/{space_id}/{asset_id}...
    // We want to extract the service root: /api/v1/dwc/consumption/relational/{tenant}
    if (!path.empty()) {
        // Find the service root by looking for common OData patterns
        if (path.find("/V2/") != std::string::npos) {
            // OData v2: metadata is at service root
            auto v2_pos = path.find("/V2/");
            auto service_pos = path.find("/", v2_pos + 4);
            if (service_pos != std::string::npos) {
                auto service_root = path.substr(0, service_pos);
                base.Path(service_root + "/$metadata");
            } else {
                base.Path(path + "/$metadata");
            }
        } else if (path.find("/V4/") != std::string::npos) {
            // OData v4: metadata is at service root
            auto v4_pos = path.find("/V4/");
            auto service_pos = path.find("/", v4_pos + 4);
            if (service_pos != std::string::npos) {
                auto service_root = path.substr(0, service_pos);
                base.Path(service_root + "/$metadata");
            } else {
                base.Path(path + "/$metadata");
            }
        } else if (path.find("/api/v1/dwc/consumption/relational/") != std::string::npos) {
            // Datasphere: metadata is at the asset service root level
            // Extract /api/v1/dwc/consumption/relational/{tenant}/{asset}
            ERPL_TRACE_INFO("ODATA_CLIENT", "Detected Datasphere URL pattern");
            auto datasphere_pos = path.find("/api/v1/dwc/consumption/relational/");
            ERPL_TRACE_INFO("ODATA_CLIENT", "Datasphere pattern found at position: " + std::to_string(datasphere_pos));
            if (datasphere_pos != std::string::npos) {
                auto after_relational = datasphere_pos + 35; // length of "/api/v1/dwc/consumption/relational/"
                ERPL_TRACE_INFO("ODATA_CLIENT", "After /relational/ at position: " + std::to_string(after_relational));
                
                // Collect path segments after /relational/
                std::vector<std::string> segments;
                {
                    size_t i = after_relational;
                    while (i < path.size()) {
                        size_t j = path.find('/', i);
                        if (j == std::string::npos) j = path.size();
                        if (j > i) segments.push_back(path.substr(i, j - i));
                        i = j + 1;
                    }
                }
                if (segments.size() >= 2) {
                    auto tenant = segments[0];
                    auto asset  = segments[1];
                    auto service_root = std::string("/api/v1/dwc/consumption/relational/") + tenant + "/" + asset;
                    ERPL_TRACE_INFO("ODATA_CLIENT", "Extracted Datasphere service root: " + service_root);
                    base.Path(service_root + "/$metadata");
                } else {
                    // Fallback to tenant-level if asset not present
                    auto tenant_end = path.find("/", after_relational);
                    if (tenant_end != std::string::npos) {
                        auto service_root = path.substr(0, tenant_end);
                        ERPL_TRACE_INFO("ODATA_CLIENT", "Fallback service root (tenant): " + service_root);
                        base.Path(service_root + "/$metadata");
                    } else {
                        base.Path(path + "/$metadata");
                    }
                }
            } else {
                ERPL_TRACE_INFO("ODATA_CLIENT", "Datasphere pattern not found in path");
                // Generic fallback: remove last segment and append $metadata
                auto last_slash = path.find_last_of('/');
                if (last_slash != std::string::npos && last_slash > 0) {
                    auto service_root = path.substr(0, last_slash);
                    base.Path(service_root + "/$metadata");
                } else {
                    base.Path("/$metadata");
                }
            }
        } else {
            // Generic fallback: remove last segment and append $metadata
            auto last_slash = path.find_last_of('/');
            if (last_slash != std::string::npos && last_slash > 0) {
                auto service_root = path.substr(0, last_slash);
                base.Path(service_root + "/$metadata");
            } else {
                base.Path("/$metadata");
            }
        }
    } else {
        base.Path("/$metadata");
    }
    
    auto fallback_url = base.ToString();
    
    // Store the fallback URL to avoid regenerating it multiple times
    if (metadata_context_url.empty()) {
        metadata_context_url = fallback_url;
        ERPL_TRACE_INFO("ODATA_CLIENT", "Generated and stored fallback metadata URL: " + fallback_url);
    }
    
    return fallback_url;
}

std::shared_ptr<ODataEntitySetContent> ODataEntitySetResponse::CreateODataContent(const std::string& content, ODataVersion odata_version)
{
    ERPL_TRACE_DEBUG("ODATA_CONTENT", "Creating OData content from response");
    ERPL_TRACE_DEBUG("ODATA_CONTENT", "Content type: " + ContentType());
    ERPL_TRACE_DEBUG("ODATA_CONTENT", "Content size: " + std::to_string(content.length()) + " bytes");
    
    if (ODataJsonContentMixin::IsJsonContentType(ContentType())) {
        // For JSON content, detect the actual version from the response content
        // This is more reliable than using the metadata version since the response format
        // might differ from what the metadata suggests
        auto detected_version = ODataJsonContentMixin::DetectODataVersion(content);
        ERPL_TRACE_DEBUG("ODATA_CONTENT", std::string("Detected OData version from response: ") + (detected_version == ODataVersion::V2 ? "V2" : "V4"));
        ERPL_TRACE_DEBUG("ODATA_CONTENT", std::string("Metadata suggested version: ") + (odata_version == ODataVersion::V2 ? "V2" : "V4"));
        
        auto content_obj = std::make_shared<ODataEntitySetJsonContent>(content);
        content_obj->SetODataVersion(detected_version);
        return content_obj;
    }
        
    ERPL_TRACE_ERROR("ODATA_CONTENT", "Unsupported content type: " + ContentType());
    throw std::runtime_error("Unsupported OData content type: " + ContentType());
}

std::shared_ptr<ODataEntitySetResponse> ODataEntitySetClient::Get(bool get_next)
{
    if (! get_next && current_response != nullptr) {
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "Returning cached response");
        return current_response;
    }

    ERPL_TRACE_INFO("ODATA_CLIENT", "Fetching OData request from: " + url.ToString() + " (get_next: " + std::to_string(get_next) + ")");

    // Ensure OData version is detected before making any requests
    if (odata_version == ODataVersion::UNKNOWN) {
        DetectODataVersion();
    }

    if (get_next && current_response != nullptr) {
        auto next_url = current_response->NextUrl();
        if (!next_url) {
            ERPL_TRACE_DEBUG("ODATA_CLIENT", "No next URL available for pagination");
            return nullptr;
        }

        url = HttpUrl::MergeWithBaseUrlIfRelative(url, next_url.value());
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "Using next URL: " + url.ToString());
    }

    // Add input parameters to the URL if they exist
    HttpUrl request_url = url;
    if (!input_parameters.empty()) {
        request_url = AddInputParametersToUrl(url);
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "Modified URL with input parameters: " + request_url.ToString());
    }

    ERPL_TRACE_DEBUG("ODATA_CLIENT", "Executing HTTP GET request");
    auto http_response = DoHttpGet(request_url);
    
    // Detect OData version from raw HTTP response content if not already known
    if (odata_version == ODataVersion::UNKNOWN) {
        auto content_str = http_response->Content();
        if (ODataJsonContentMixin::IsJsonContentType(http_response->ContentType())) {
            auto detected_version = ODataJsonContentMixin::DetectODataVersion(content_str);
            odata_version = detected_version;
            std::string version_str = (odata_version == ODataVersion::V2 ? "V2" : "V4");
            ERPL_TRACE_DEBUG("ODATA_CLIENT", "Detected OData version from response: " + version_str);
        }
    }
    
    ERPL_TRACE_DEBUG("ODATA_CLIENT", "Creating OData response object");
    current_response = std::make_shared<ODataEntitySetResponse>(std::move(http_response), odata_version);
    
    ERPL_TRACE_DEBUG("ODATA_CLIENT", "Successfully created OData response");
    
    // After getting a response, try to extract and store the metadata context URL
    // This will be used for future metadata requests instead of generating fallback URLs
    if (current_response) {
        auto ctx = current_response->MetadataContextUrl();
        if (!ctx.empty()) {
            ERPL_TRACE_DEBUG("ODATA_CLIENT", "Raw metadata context URL: " + ctx);
            
            // Parse the fragment to extract entity information
            auto hash_pos = ctx.find('#');
            if (hash_pos != std::string::npos) {
                auto fragment = ctx.substr(hash_pos + 1);
                ERPL_TRACE_DEBUG("ODATA_CLIENT", "Metadata context fragment: " + fragment);
                
                // Extract entity name from fragment. Supported forms:
                //  - "Entity(params)/Set"
                //  - "Entity/Set"
                //  - "Entity"
                std::string entity_name;
                auto open_paren_pos = fragment.find('(');
                if (open_paren_pos != std::string::npos) {
                    entity_name = fragment.substr(0, open_paren_pos);
                } else {
                    auto slash_pos = fragment.find('/');
                    if (slash_pos != std::string::npos) {
                        entity_name = fragment.substr(0, slash_pos);
                    } else {
                        entity_name = fragment; // plain entity name
                    }
                }
                if (!entity_name.empty()) {
                    ERPL_TRACE_DEBUG("ODATA_CLIENT", "Extracted entity name from fragment: " + entity_name);
                    current_entity_name_from_fragment = entity_name;
                }
                
                // Strip the fragment for the metadata URL
                ctx = ctx.substr(0, hash_pos);
            }
            
            HttpUrl meta_url = HttpUrl::MergeWithBaseUrlIfRelative(url, ctx);
            auto final_url = meta_url.ToString();
            if (metadata_context_url != final_url) {
                ERPL_TRACE_INFO("ODATA_CLIENT", "Updated metadata context URL from response: " + final_url);
                metadata_context_url = final_url;
            }
        } else if (!input_parameters.empty()) {
            // When input parameters are used but no metadata context URL is provided,
            // extract the entity name from the URL path since we know the structure
            ERPL_TRACE_DEBUG("ODATA_CLIENT", "No metadata context URL, extracting entity name from URL path with input parameters");
            
            auto path = url.Path();
            if (!path.empty() && path != "/") {
                // Remove leading slash if present
                if (path[0] == '/') {
                    path = path.substr(1);
                }
                
                // Find the last path segment
                auto last_slash_pos = path.find_last_of('/');
                if (last_slash_pos != std::string::npos) {
                    auto last_segment = path.substr(last_slash_pos + 1);
                    
                    // Extract entity name from segment like "flights_view(CARRIER='AA')/Set"
                    auto open_paren_pos = last_segment.find('(');
                    if (open_paren_pos != std::string::npos) {
                        auto entity_name = last_segment.substr(0, open_paren_pos);
                        current_entity_name_from_fragment = entity_name;
                        ERPL_TRACE_DEBUG("ODATA_CLIENT", "Extracted entity name from URL path with input parameters: " + entity_name);
                    }
                }
            }
            
            // Also try to construct a proper metadata context URL with the fragment
            // This might help with metadata lookup
            if (!current_entity_name_from_fragment.empty()) {
                // Construct the fragment like "flights_view('AA')/Set"
                std::string fragment = current_entity_name_from_fragment + "(";
                bool first = true;
                for (const auto& [key, value] : input_parameters) {
                    if (!first) {
                        fragment += ",";
                    }
                    first = false;
                    
                    // Handle different value types according to SAP Datasphere documentation
                    if (value.find_first_not_of("0123456789.-") == std::string::npos && value.find('.') != std::string::npos) {
                        // Numeric value (no quotes needed)
                        fragment += key + "=" + value;
                    } else if (value.find_first_not_of("0123456789-") == std::string::npos && value.find('-') != std::string::npos && value.length() == 10) {
                        // Date value (no quotes needed)
                        fragment += key + "=" + value;
                    } else {
                        // Text value (enclosed in single quotes)
                        fragment += key + "='" + value + "'";
                    }
                }
                fragment += ")/Set";
                
                ERPL_TRACE_DEBUG("ODATA_CLIENT", "Constructed metadata context fragment: " + fragment);
            }
        }
    }

    return current_response;
}



// (duplicate definition removed)

void ODataEntitySetClient::SetEntitySetNameFromContextFragment(const std::string &context_or_fragment)
{
    // Only extract from a fragment part after '#'. If none, do nothing.
    auto hash_pos = context_or_fragment.find('#');
    if (hash_pos == std::string::npos) {
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "No fragment present in @odata.context; skipping entity name extraction");
        return;
    }
    std::string fragment = context_or_fragment.substr(hash_pos + 1);
    // Extract entity name as in Get(): before '(' or before '/' or whole fragment
    std::string entity_name;
    auto open_paren_pos = fragment.find('(');
    if (open_paren_pos != std::string::npos) {
        entity_name = fragment.substr(0, open_paren_pos);
    } else {
        auto slash_pos = fragment.find('/');
        if (slash_pos != std::string::npos) {
            entity_name = fragment.substr(0, slash_pos);
        } else {
            entity_name = fragment;
        }
    }
    if (!entity_name.empty()) {
        ERPL_TRACE_INFO("ODATA_CLIENT", "Set entity set name from @odata.context: " + entity_name);
        current_entity_name_from_fragment = entity_name;
    } else {
        ERPL_TRACE_WARN("ODATA_CLIENT", "Failed to extract entity name from @odata.context fragment: " + context_or_fragment);
    }
}

EntitySet ODataEntitySetClient::GetCurrentEntitySetType()
{
    ERPL_TRACE_DEBUG("ODATA_CLIENT", "GetCurrentEntitySetType called");
    ERPL_TRACE_DEBUG("ODATA_CLIENT", "Current URL path: " + url.Path());
    ERPL_TRACE_DEBUG("ODATA_CLIENT", "Input parameters count: " + std::to_string(input_parameters.size()));
    ERPL_TRACE_DEBUG("ODATA_CLIENT", "Current entity name from fragment: " + (current_entity_name_from_fragment.empty() ? "empty" : current_entity_name_from_fragment));
    
    std::string entity_set_name;
    
    // First, try to use the entity name extracted from the metadata context fragment
    if (!current_entity_name_from_fragment.empty()) {
        entity_set_name = current_entity_name_from_fragment;
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "Using entity name from metadata context fragment: " + entity_set_name);
    } else {
        // No fragment-based name available; do not guess from URL. Defer to metadata lookup below.
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "No entity name from @odata.context; deferring to metadata lookup");
    }
    
    ERPL_TRACE_DEBUG("ODATA_CLIENT", "Final entity set name: " + entity_set_name);
    
    // If still empty, use metadata to find the single entity set name if unique
    if (entity_set_name.empty()) {
        auto edmx_probe = GetMetadata();
        auto sets = edmx_probe.FindEntitySets();
        if (sets.size() == 1) {
            entity_set_name = sets[0].name;
            ERPL_TRACE_INFO("ODATA_CLIENT", "Resolved single entity set from metadata: " + entity_set_name);
        } else {
            // Prefer the entity set name extracted earlier from @odata.context fragment if available
            if (!current_entity_name_from_fragment.empty()) {
                entity_set_name = current_entity_name_from_fragment;
                ERPL_TRACE_INFO("ODATA_CLIENT", "Using entity set from @odata.context fragment: " + entity_set_name);
            } else {
                // For non-Datasphere services, derive entity set name from URL path when multiple sets exist
                const auto url_str = url.ToString();
                const bool is_datasphere = (url_str.find("hcs.cloud.sap") != std::string::npos) ||
                                           (url_str.find("/api/v1/dwc/consumption/relational/") != std::string::npos);
                if (!is_datasphere) {
                    auto path = url.Path();
                    // Remove trailing slash
                    if (!path.empty() && path.back() == '/') {
                        path.pop_back();
                    }
                    // Extract last segment
                    auto last_slash = path.find_last_of('/');
                    std::string candidate = (last_slash == std::string::npos) ? path : path.substr(last_slash + 1);
                    ERPL_TRACE_INFO("ODATA_CLIENT", "Derived entity set candidate from URL: " + candidate);
                    // Validate against metadata entity sets
                    bool found = false;
                    for (const auto &es : sets) {
                        if (es.name == candidate) { found = true; break; }
                    }
                    if (found) {
                        entity_set_name = candidate;
                        ERPL_TRACE_INFO("ODATA_CLIENT", "Resolved entity set from URL path: " + entity_set_name);
                    } else {
                        throw std::runtime_error("Unable to resolve entity set from @odata.context or URL; metadata has multiple sets");
                    }
                } else {
                    throw std::runtime_error("Unable to resolve entity set from @odata.context and metadata has multiple sets");
                }
            }
        }
    }
    auto edmx = GetMetadata();
    auto entity_set_type = edmx.FindEntitySet(entity_set_name);
    return entity_set_type;
}

EntityType ODataEntitySetClient::GetCurrentEntityType()
{
    auto entity_set_type = GetCurrentEntitySetType();
    auto edmx = GetMetadata();

    // Resolve the base entity type from the entity set
    std::string resolved_entity_type_name = entity_set_type.entity_type_name; // e.g., StandaloneService.flights_viewParameters

    // Datasphere parameterized pattern: when addressing ...(<params>)/Set, the result type is the
    // navigation property "Set" of the parameters entity, typically Collection(StandaloneService.<entity>Type)
    auto path_has_set = false;
    {
        auto p = url.Path();
        if (!p.empty()) {
            if (p.size() >= 4 && p.substr(p.size() - 4) == "/Set") path_has_set = true;
            if (p.find(")/Set") != std::string::npos) path_has_set = true;
        }
    }

    // Prefer explicit signal from @odata.context fragment if present
    if (current_response) {
        auto ctx = current_response->MetadataContextUrl();
        auto hash_pos = ctx.find('#');
        if (hash_pos != std::string::npos) {
            auto fragment = ctx.substr(hash_pos + 1);
            if (fragment.find("/Set") != std::string::npos) {
                path_has_set = true;
            }
        }
    }

    if (path_has_set || HasInputParameters()) {
        // Resolve parameters entity and then follow nav property "Set"
        ERPL_TRACE_DEBUG("ODATA_CLIENT", "Resolving entity type via navigation property 'Set' from: " + resolved_entity_type_name);
        auto params_entity_type = std::get<EntityType>(edmx.FindType(resolved_entity_type_name));
        std::string nav_type_name;
        for (const auto &nav_prop : params_entity_type.navigation_properties) {
            if (nav_prop.name == "Set") {
                nav_type_name = nav_prop.type; // e.g., Collection(StandaloneService.flights_viewType)
                break;
            }
        }
        if (!nav_type_name.empty()) {
            // Strip Collection(...) if present
            if (nav_type_name.find("Collection(") == 0 && nav_type_name.back() == ')') {
                nav_type_name = nav_type_name.substr(std::string("Collection(").size(), nav_type_name.size() - std::string("Collection(").size() - 1);
            }
            resolved_entity_type_name = nav_type_name;
            ERPL_TRACE_INFO("ODATA_CLIENT", "Resolved result entity type via 'Set': " + resolved_entity_type_name);
        } else {
            ERPL_TRACE_WARN("ODATA_CLIENT", "Navigation property 'Set' not found on type: " + params_entity_type.name + "; falling back to entity set type");
        }
    }

    auto entity_type = std::get<EntityType>(edmx.FindType(resolved_entity_type_name));
    return entity_type;
}

std::vector<std::string> ODataEntitySetClient::GetResultNames()
{
    auto edmx = GetMetadata();
    auto entity_set_type = GetCurrentEntitySetType();
    auto entity_type = GetCurrentEntityType();

    auto type_conv = DuckTypeConverter(edmx);
    auto entity_struct = type_conv(GetCurrentEntityType());

    std::vector<std::string> ret_names;
    for (const auto& child : StructType::GetChildTypes(entity_struct)) {
        ret_names.push_back(child.first);
    }

    return ret_names;
}

std::vector<duckdb::LogicalType> ODataEntitySetClient::GetResultTypes()
{
    auto edmx = GetMetadata();
    auto type_conv = DuckTypeConverter(edmx);
    auto entity_struct = type_conv(GetCurrentEntityType());

    std::vector<duckdb::LogicalType> ret_types;
    for (const auto& child : StructType::GetChildTypes(entity_struct)) {
        ret_types.push_back(child.second);
    }

    return ret_types;
}

void ODataEntitySetClient::SetInputParameters(const std::map<std::string, std::string>& input_params)
{
    input_parameters = input_params;
    ERPL_TRACE_INFO("ODATA_CLIENT", "SetInputParameters called - storing " + std::to_string(input_params.size()) + " input parameters for OData client at " + std::to_string(reinterpret_cast<uintptr_t>(this)));
    for (const auto& [key, value] : input_params) {
        ERPL_TRACE_INFO("ODATA_CLIENT", "  Parameter: " + key + " = " + value);
    }
}

HttpUrl ODataEntitySetClient::AddInputParametersToUrl(const HttpUrl& url) const
{
    ERPL_TRACE_INFO("ODATA_CLIENT", "AddInputParametersToUrl called with " + std::to_string(input_parameters.size()) + " parameters on client at " + std::to_string(reinterpret_cast<uintptr_t>(this)));
    
    if (input_parameters.empty()) {
        ERPL_TRACE_INFO("ODATA_CLIENT", "No input parameters to add");
        return url;
    }
    
    // Create a copy of the URL to modify
    HttpUrl modified_url = url;
    std::string current_path = modified_url.Path();
    std::string new_path = current_path;
    
    // Build the input parameters string in SAP Datasphere format: (param1=value1,param2=value2)/Set
    std::string params_string = "(";
    bool first = true;
    
    for (const auto& [key, value] : input_parameters) {
        if (!first) {
            params_string += ",";
        }
        first = false;
        
        // Handle different value types according to SAP Datasphere documentation
        if (value.find_first_not_of("0123456789.-") == std::string::npos && value.find('.') != std::string::npos) {
            // Numeric value (no quotes needed)
            params_string += key + "=" + value;
        } else if (value.find_first_not_of("0123456789-") == std::string::npos && value.find('-') != std::string::npos && value.length() == 10) {
            // Date value (no quotes needed)
            params_string += key + "=" + value;
        } else {
            // Text value (enclosed in single quotes)
            params_string += key + "='" + value + "'";
        }
    }
    
    params_string += ")";
    
    // Check if the URL already ends with /Set to avoid duplicates
    bool already_has_set = (current_path.length() >= 4 && 
                           current_path.substr(current_path.length() - 4) == "/Set");
    
    // If the URL already has /Set, we need to insert parameters before it
    if (already_has_set) {
        // Remove /Set suffix temporarily
        new_path = current_path.substr(0, current_path.length() - 4);
        // Add parameters
        new_path += params_string;
        // Add /Set back
        new_path += "/Set";
    } else {
        // URL doesn't have /Set, add parameters with /Set
        new_path += params_string + "/Set";
    }
    
    // Check if parameters are already in the path to avoid duplicates
    // Remove /Set suffix from params_string for comparison since the URL might already have it
    std::string params_without_set = params_string;
    if (params_without_set.length() >= 4 && 
        params_without_set.substr(params_without_set.length() - 4) == "/Set") {
        params_without_set = params_without_set.substr(0, params_without_set.length() - 4);
    }
    
    if (current_path.find(params_without_set) != std::string::npos) {
        ERPL_TRACE_INFO("ODATA_CLIENT", "Input parameters already exist in path, skipping");
        return url;
    }
    
    // Add the parameters to the path
    modified_url.Path(new_path);
    
    if (new_path != current_path) {
        ERPL_TRACE_INFO("ODATA_CLIENT", "Added input parameters to URL path: " + modified_url.ToString());
    }
    
    return modified_url;
}

// ----------------------------------------------------------------------

ODataServiceResponse::ODataServiceResponse(std::unique_ptr<HttpResponse> http_response, ODataVersion odata_version)
    : ODataResponse(std::move(http_response))
    , odata_version(odata_version)
{ 
    ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Created OData service response");
    ERPL_TRACE_DEBUG("ODATA_RESPONSE", "Response content type: " + ContentType());
    ERPL_TRACE_DEBUG("ODATA_RESPONSE", std::string("OData version: ") + (odata_version == ODataVersion::V2 ? "V2" : "V4"));
}

std::shared_ptr<ODataServiceContent> ODataServiceResponse::CreateODataContent(const std::string& content, ODataVersion odata_version)
{
    if (ODataJsonContentMixin::IsJsonContentType(ContentType())) {
        ERPL_TRACE_DEBUG("ODATA_CONTENT", std::string("Creating JSON content with OData version: ") + (odata_version == ODataVersion::V2 ? "V2" : "V4"));
        auto content_obj = std::make_shared<ODataServiceJsonContent>(content);
        content_obj->SetODataVersion(odata_version);
        return content_obj;
    }
        
    throw std::runtime_error("Unsupported OData content type: " + ContentType());
}

std::string ODataServiceResponse::MetadataContextUrl()
{
    return Content()->MetadataContextUrl();
}

std::vector<ODataEntitySetReference> ODataServiceResponse::EntitySets()
{
    return Content()->EntitySets();
}

// ----------------------------------------------------------------------

ODataServiceClient::ODataServiceClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url)
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url, nullptr)
{ }

ODataServiceClient::ODataServiceClient(std::shared_ptr<HttpClient> http_client, const HttpUrl& url, std::shared_ptr<HttpAuthParams> auth_params)
    : ODataClient(std::make_shared<CachingHttpClient>(http_client), url, auth_params)
{ }

std::shared_ptr<ODataServiceResponse> ODataServiceClient::Get(bool get_next)
{
    if (current_response != nullptr) {
        return current_response;
    }

    auto http_response = DoHttpGet(url);
    current_response = std::make_shared<ODataServiceResponse>(std::move(http_response), odata_version);

    return current_response;
}

std::string ODataServiceClient::GetMetadataContextUrl()
{
    Get();
    return current_response->MetadataContextUrl();
}

} // namespace erpl_web
