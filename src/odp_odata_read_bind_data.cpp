#include "odp_odata_read_bind_data.hpp"
#include "secret_functions.hpp"
#include "tracing.hpp"
#include <regex>

namespace erpl_web {

OdpODataReadBindData::OdpODataReadBindData(duckdb::ClientContext& context,
                                         const std::string& entity_set_url,
                                         const std::string& secret_name,
                                         bool force_full_load,
                                         const std::string& import_delta_token,
                                         std::optional<uint32_t> max_page_size)
    : context_(context)
    , entity_set_url_(entity_set_url)
    , secret_name_(secret_name.empty() ? "default" : secret_name)
    , max_page_size_(max_page_size)
    , initialized_(false)
    , first_fetch_completed_(false)
    , current_audit_id_(-1)
{
    ERPL_TRACE_INFO("ODP_BIND_DATA", duckdb::StringUtil::Format(
        "Creating ODP bind data - URL: %s, Secret: %s, ForceFullLoad: %s, ImportToken: %s",
        entity_set_url_, secret_name_, force_full_load ? "true" : "false",
        import_delta_token.empty() ? "NONE" : "PROVIDED"));

    // Just validate URL in constructor - defer initialization to explicit Initialize() call
    ValidateEntitySetUrl();
}

// ============================================================================
// Core DuckDB Table Function Interface (Delegated)
// ============================================================================

std::vector<std::string> OdpODataReadBindData::GetResultNames(bool all_columns) {
    if (!odata_bind_data_) {
        throw duckdb::InternalException("OData bind data not initialized");
    }
    return odata_bind_data_->GetResultNames(all_columns);
}

std::vector<duckdb::LogicalType> OdpODataReadBindData::GetResultTypes(bool all_columns) {
    if (!odata_bind_data_) {
        throw duckdb::InternalException("OData bind data not initialized");
    }
    return odata_bind_data_->GetResultTypes(all_columns);
}

bool OdpODataReadBindData::HasMoreResults() {
    if (!odata_bind_data_) {
        return false;
    }
    
    // Check if underlying OData has more results
    bool odata_has_more = odata_bind_data_->HasMoreResults();
    
    // If OData is done but we're in delta mode, we might have more delta fetches
    if (!odata_has_more && state_manager_->GetCurrentPhase() == OdpSubscriptionStateManager::SubscriptionPhase::DELTA_FETCH) {
        // For now, we don't automatically trigger more delta fetches
        // This would be handled by a scheduler or explicit user request
        return false;
    }
    
    return odata_has_more;
}

unsigned int OdpODataReadBindData::FetchNextResult(duckdb::DataChunk &output) {
    ERPL_TRACE_DEBUG("ODP_BIND_DATA", "FetchNextResult called");
    
    if (!initialized_) {
        throw duckdb::InternalException("ODP bind data not initialized");
    }
    
    try {
        // Handle first fetch - determine if we need initial load or delta fetch
        if (!first_fetch_completed_) {
            bool success = false;
            
            if (state_manager_->ShouldPerformInitialLoad()) {
                ERPL_TRACE_INFO("ODP_BIND_DATA", "Performing initial load");
                success = HandleInitialLoad();
            } else if (state_manager_->ShouldPerformDeltaFetch()) {
                ERPL_TRACE_INFO("ODP_BIND_DATA", "Performing delta fetch");
                success = HandleDeltaFetch();
            } else {
                throw duckdb::InternalException("Invalid subscription state for data fetch");
            }
            
            if (!success) {
                throw duckdb::InternalException("Failed to perform ODP data fetch");
            }
            
            first_fetch_completed_ = true;
        }
        
        // Delegate to underlying OData bind data
        unsigned int rows_fetched = odata_bind_data_->FetchNextResult(output);
        
        ERPL_TRACE_DEBUG("ODP_BIND_DATA", duckdb::StringUtil::Format("Fetched %u rows", rows_fetched));
        
        // Update audit entry if we have one
        if (current_audit_id_ > 0 && rows_fetched > 0) {
            state_manager_->UpdateAuditEntry(current_audit_id_, 200, rows_fetched, 
                                           output.size() * output.GetTypes().size() * 8); // Rough size estimate
        }
        
        return rows_fetched;
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_BIND_DATA", "Error in FetchNextResult: " + std::string(e.what()));
        HandleRequestError(e, first_fetch_completed_ ? "data_fetch" : "initial_setup");
        throw;
    }
}

void OdpODataReadBindData::ActivateColumns(const std::vector<duckdb::column_t> &column_ids) {
    if (odata_bind_data_) {
        odata_bind_data_->ActivateColumns(column_ids);
    }
}

void OdpODataReadBindData::AddFilters(const duckdb::optional_ptr<duckdb::TableFilterSet> &filters) {
    if (odata_bind_data_) {
        odata_bind_data_->AddFilters(filters);
    }
}

void OdpODataReadBindData::AddResultModifiers(const std::vector<duckdb::unique_ptr<duckdb::BoundResultModifier>> &modifiers) {
    if (odata_bind_data_) {
        odata_bind_data_->AddResultModifiers(modifiers);
    }
}

double OdpODataReadBindData::GetProgressFraction() const {
    if (odata_bind_data_) {
        return odata_bind_data_->GetProgressFraction();
    }
    return 0.0;
}

// ============================================================================
// ODP-Specific Interface
// ============================================================================

std::string OdpODataReadBindData::GetSubscriptionId() const {
    return state_manager_->GetSubscriptionId();
}

std::string OdpODataReadBindData::GetCurrentDeltaToken() const {
    return state_manager_->GetCurrentDeltaToken();
}

bool OdpODataReadBindData::IsSubscriptionActive() const {
    return state_manager_->IsSubscriptionActive();
}

OdpSubscriptionStateManager::SubscriptionPhase OdpODataReadBindData::GetCurrentPhase() const {
    return state_manager_->GetCurrentPhase();
}

void OdpODataReadBindData::ForceInitialLoad() {
    ERPL_TRACE_INFO("ODP_BIND_DATA", "Forcing initial load");
    
    state_manager_->TransitionToInitialLoad();
    first_fetch_completed_ = false;
    
    // Recreate OData bind data to reset state
    CreateODataBindData();
}

void OdpODataReadBindData::TerminateSubscription() {
    ERPL_TRACE_INFO("ODP_BIND_DATA", "Terminating subscription");
    
    state_manager_->TransitionToTerminated();
    
    // TODO: Call SAP termination endpoint if needed
    // This would require implementing the termination request in the orchestrator
}

std::vector<OdpAuditEntry> OdpODataReadBindData::GetAuditHistory(int days_back) const {
    // This would be implemented by the repository
    // For now, return empty vector
    return std::vector<OdpAuditEntry>();
}

// ============================================================================
// Private Implementation Methods
// ============================================================================

void OdpODataReadBindData::Initialize() {
    ERPL_TRACE_DEBUG("ODP_BIND_DATA", "Initializing ODP bind data");
    
    try {
        // Setup authentication
        SetupAuthentication();
        
        // Create state manager
        std::string entity_set_name = ExtractEntitySetName(entity_set_url_);
        state_manager_ = std::make_unique<OdpSubscriptionStateManager>(
            context_, entity_set_url_, entity_set_name, secret_name_, 
            false, ""); // Use defaults for now
        
        // Create request orchestrator
        request_orchestrator_ = std::make_unique<OdpRequestOrchestrator>(
            auth_params_, max_page_size_.value_or(15000));
        
        // Create the underlying OData bind data
        CreateODataBindData();
        
        initialized_ = true;
        LogCurrentState();
        
        ERPL_TRACE_INFO("ODP_BIND_DATA", "ODP bind data initialized successfully");
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_BIND_DATA", "Failed to initialize ODP bind data: " + std::string(e.what()));
        throw;
    }
}

void OdpODataReadBindData::SetupAuthentication() {
    ERPL_TRACE_DEBUG("ODP_BIND_DATA", "Setting up authentication with secret: " + secret_name_);
    
    try {
        auth_params_ = HttpAuthParams::FromDuckDbSecrets(context_, entity_set_url_);
        if (!auth_params_) {
            ERPL_TRACE_WARN("ODP_BIND_DATA", "No authentication parameters found for URL: " + entity_set_url_);
        } else {
            ERPL_TRACE_INFO("ODP_BIND_DATA", "Authentication configured successfully");
        }
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_BIND_DATA", "Failed to setup authentication: " + std::string(e.what()));
        throw;
    }
}

void OdpODataReadBindData::CreateODataBindData() {
    ERPL_TRACE_DEBUG("ODP_BIND_DATA", "Creating underlying OData bind data");
    
    try {
        // Create OData bind data using the entity set URL
        odata_bind_data_ = ODataReadBindData::FromEntitySetRoot(entity_set_url_, auth_params_);
        
        if (!odata_bind_data_) {
            throw duckdb::InternalException("Failed to create OData bind data");
        }
        
        ERPL_TRACE_INFO("ODP_BIND_DATA", "OData bind data created successfully");
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_BIND_DATA", "Failed to create OData bind data: " + std::string(e.what()));
        throw;
    }
}

void OdpODataReadBindData::ValidateEntitySetUrl() const {
    ERPL_TRACE_DEBUG("ODP_BIND_DATA", "Validating entity set URL: " + entity_set_url_);
    
    if (entity_set_url_.empty()) {
        throw duckdb::InvalidInputException("Entity set URL cannot be empty");
    }
    
    if (!OdpSubscriptionRepository::IsValidOdpUrl(entity_set_url_)) {
        throw duckdb::InvalidInputException("Invalid ODP URL: " + entity_set_url_);
    }
    
    ERPL_TRACE_DEBUG("ODP_BIND_DATA", "Entity set URL validation passed");
}

bool OdpODataReadBindData::HandleInitialLoad() {
    ERPL_TRACE_INFO("ODP_BIND_DATA", "Handling initial load request");
    
    try {
        // Create audit entry
        current_audit_id_ = state_manager_->CreateAuditEntry("initial_load", entity_set_url_);
        
        // Execute initial load request
        auto result = request_orchestrator_->ExecuteInitialLoad(entity_set_url_, max_page_size_);
        
        // Process the result
        ProcessRequestResult(result, "initial_load");
        
        // Update OData client with the response
        if (result.response) {
            // For initial load, we use the original URL
            // The delta token will be used for subsequent requests
            UpdateODataClient(entity_set_url_);
            return true;
        }
        
        return false;
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_BIND_DATA", "Initial load failed: " + std::string(e.what()));
        state_manager_->TransitionToError("Initial load failed: " + std::string(e.what()));
        return false;
    }
}

bool OdpODataReadBindData::HandleDeltaFetch() {
    ERPL_TRACE_INFO("ODP_BIND_DATA", "Handling delta fetch request");
    
    std::string current_token = state_manager_->GetCurrentDeltaToken();
    if (current_token.empty()) {
        ERPL_TRACE_WARN("ODP_BIND_DATA", "No delta token available, falling back to initial load");
        state_manager_->TransitionToInitialLoad();
        return HandleInitialLoad();
    }
    
    try {
        // Create audit entry
        current_audit_id_ = state_manager_->CreateAuditEntry("delta_fetch", entity_set_url_);
        
        // Execute delta fetch request
        auto result = request_orchestrator_->ExecuteDeltaFetch(entity_set_url_, current_token, max_page_size_);
        
        // Process the result
        ProcessRequestResult(result, "delta_fetch");
        
        // Update OData client with delta URL
        if (result.response) {
            std::string delta_url = OdpRequestOrchestrator::BuildDeltaUrl(entity_set_url_, current_token);
            UpdateODataClient(delta_url);
            return true;
        }
        
        return false;
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_BIND_DATA", "Delta fetch failed: " + std::string(e.what()));
        
        // Check if this is a token error that requires fallback to initial load
        if (IsTokenError(e)) {
            ERPL_TRACE_INFO("ODP_BIND_DATA", "Token error detected, falling back to initial load");
            state_manager_->TransitionToInitialLoad();
            return HandleInitialLoad();
        }
        
        state_manager_->TransitionToError("Delta fetch failed: " + std::string(e.what()));
        return false;
    }
}

void OdpODataReadBindData::ProcessRequestResult(const OdpRequestOrchestrator::OdpRequestResult& result,
                                              const std::string& operation_type) {
    ERPL_TRACE_DEBUG("ODP_BIND_DATA", duckdb::StringUtil::Format(
        "Processing %s result - Status: %d, DeltaToken: %s, PreferenceApplied: %s",
        operation_type, result.http_status_code,
        result.extracted_delta_token.empty() ? "NONE" : "PRESENT",
        result.preference_applied ? "YES" : "NO"));
    
    // Update audit entry with response details
    if (current_audit_id_ > 0) {
        state_manager_->UpdateAuditEntry(current_audit_id_, result.http_status_code, 0, 
                                       result.response_size_bytes, result.extracted_delta_token);
    }
    
    // Handle state transitions based on operation type
    if (operation_type == "initial_load") {
        if (!result.extracted_delta_token.empty() && result.preference_applied) {
            // Successful initial load with change tracking
            state_manager_->TransitionToDeltaFetch(result.extracted_delta_token, result.preference_applied);
        } else {
            // Initial load without change tracking - stay in initial load mode
            ERPL_TRACE_WARN("ODP_BIND_DATA", "Initial load completed but change tracking not established");
        }
    } else if (operation_type == "delta_fetch") {
        if (!result.extracted_delta_token.empty()) {
            // Update delta token for next fetch
            state_manager_->UpdateDeltaToken(result.extracted_delta_token);
        }
    }
}

void OdpODataReadBindData::UpdateODataClient(const std::string& url) {
    ERPL_TRACE_DEBUG("ODP_BIND_DATA", "Updating OData client with URL: " + url);
    
    try {
        // Recreate the OData bind data with the new URL
        odata_bind_data_ = ODataReadBindData::FromEntitySetRoot(url, auth_params_);
        
        if (!odata_bind_data_) {
            throw duckdb::InternalException("Failed to update OData client");
        }
        
        ERPL_TRACE_DEBUG("ODP_BIND_DATA", "OData client updated successfully");
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_BIND_DATA", "Failed to update OData client: " + std::string(e.what()));
        throw;
    }
}

void OdpODataReadBindData::HandleRequestError(const std::exception& error, const std::string& operation_type) {
    ERPL_TRACE_ERROR("ODP_BIND_DATA", duckdb::StringUtil::Format(
        "Handling error in %s: %s", operation_type, error.what()));
    
    // Update audit entry with error
    if (current_audit_id_ > 0) {
        state_manager_->UpdateAuditEntry(current_audit_id_, std::nullopt, 0, 0, "", error.what());
    }
    
    // Transition to error state
    state_manager_->TransitionToError(error.what());
}

bool OdpODataReadBindData::IsTokenError(const std::exception& error) const {
    std::string error_msg = error.what();
    
    // Check for common token-related error patterns
    return error_msg.find("410") != std::string::npos ||  // Gone - token expired
           error_msg.find("404") != std::string::npos ||  // Not found - token invalid
           error_msg.find("token") != std::string::npos || // Generic token error
           error_msg.find("delta") != std::string::npos;   // Delta-related error
}

void OdpODataReadBindData::LogCurrentState() const {
    ERPL_TRACE_INFO("ODP_BIND_DATA", duckdb::StringUtil::Format(
        "ODP Bind Data State - Subscription: %s, Phase: %s, Initialized: %s, FirstFetch: %s",
        GetSubscriptionId(),
        OdpSubscriptionStateManager::PhaseToString(GetCurrentPhase()),
        initialized_ ? "YES" : "NO",
        first_fetch_completed_ ? "YES" : "NO"));
}

std::string OdpODataReadBindData::ExtractEntitySetName(const std::string& url) {
    ERPL_TRACE_DEBUG("ODP_BIND_DATA", "Extracting entity set name from URL: " + url);
    
    // Extract the last path segment as entity set name
    std::regex entity_set_regex(R"(/([^/\?]+)(?:\?|$))");
    std::smatch match;
    
    if (std::regex_search(url, match, entity_set_regex)) {
        std::string entity_set_name = match[1].str();
        ERPL_TRACE_DEBUG("ODP_BIND_DATA", "Extracted entity set name: " + entity_set_name);
        return entity_set_name;
    }
    
    // Fallback: use the full URL as entity set name (cleaned)
    std::string cleaned_name = OdpSubscriptionRepository::CleanUrlForId(url);
    ERPL_TRACE_WARN("ODP_BIND_DATA", "Could not extract entity set name, using cleaned URL: " + cleaned_name);
    return cleaned_name;
}

ODataReadBindData& OdpODataReadBindData::GetODataBindData() {
    if (!odata_bind_data_) {
        throw duckdb::InternalException("OData bind data not initialized");
    }
    return *odata_bind_data_;
}

const ODataReadBindData& OdpODataReadBindData::GetODataBindData() const {
    if (!odata_bind_data_) {
        throw duckdb::InternalException("OData bind data not initialized");
    }
    return *odata_bind_data_;
}

void OdpODataReadBindData::ProcessScanResult(const duckdb::DataChunk& output) {
    ERPL_TRACE_DEBUG("ODP_BIND_DATA", duckdb::StringUtil::Format(
        "Processing scan result with %zu rows", output.size()));
    
    try {
        // Update audit entry with row count
        if (current_audit_id_ > 0) {
            state_manager_->UpdateAuditEntry(current_audit_id_, std::nullopt, 
                                           static_cast<int64_t>(output.size()), 0);
        }
        
        // Check if this completes a fetch operation
        if (!first_fetch_completed_) {
            first_fetch_completed_ = true;
            
            // If we're in initial load and got data, we might need to transition to delta fetch
            if (state_manager_->GetCurrentPhase() == OdpSubscriptionStateManager::SubscriptionPhase::INITIAL_LOAD) {
                // The transition will be handled by the request orchestrator when it extracts delta tokens
                ERPL_TRACE_DEBUG("ODP_BIND_DATA", "First fetch completed in initial load phase");
            }
        }
        
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODP_BIND_DATA", "Error processing scan result: " + std::string(e.what()));
        // Don't throw - this is just for tracking, shouldn't break the scan
    }
}

} // namespace erpl_web
