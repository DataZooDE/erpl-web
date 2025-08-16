#include "erpl_datasphere_secret.hpp"
#include "erpl_oauth2_flow_v2.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"
#include <fstream>
#include <sstream>
#include <chrono>

namespace erpl_web {

// DatasphereSecretData implementation
bool DatasphereSecretData::HasValidToken() const {
    return !access_token.empty() && !IsTokenExpired();
}

bool DatasphereSecretData::IsTokenExpired() const {
    if (expires_at.empty()) {
        return true;
    }
    
    try {
        auto expiration_time = GetExpirationTime();
        auto now = std::chrono::system_clock::now();
        return now >= expiration_time;
    } catch (...) {
        return true;
    }
}

std::chrono::time_point<std::chrono::system_clock> DatasphereSecretData::GetExpirationTime() const {
    if (expires_at.empty()) {
        return std::chrono::system_clock::now();
    }
    
    try {
        std::time_t expiration_timestamp = std::stoll(expires_at);
        return std::chrono::system_clock::from_time_t(expiration_timestamp);
    } catch (...) {
        return std::chrono::system_clock::now();
    }
}

// Secret creation functions implementation

void CreateDatasphereSecretFunctions::Register(duckdb::DatabaseInstance &db) {
    std::string type = "datasphere";
    
    // Register the new type
    duckdb::SecretType secret_type;
    secret_type.name = type;
    secret_type.deserializer = duckdb::KeyValueSecret::Deserialize<duckdb::KeyValueSecret>;
    secret_type.default_provider = "oauth2";
    
    // Register the oauth2 secret provider
    duckdb::CreateSecretFunction oauth2_function = {type, "oauth2", CreateDatasphereSecretFromOAuth2, {}};
    oauth2_function.named_parameters["client_id"] = duckdb::LogicalType::VARCHAR;
    oauth2_function.named_parameters["client_secret"] = duckdb::LogicalType::VARCHAR;
    oauth2_function.named_parameters["tenant_name"] = duckdb::LogicalType::VARCHAR;
    oauth2_function.named_parameters["data_center"] = duckdb::LogicalType::VARCHAR;
    oauth2_function.named_parameters["scope"] = duckdb::LogicalType::VARCHAR;
    oauth2_function.named_parameters["redirect_uri"] = duckdb::LogicalType::VARCHAR;
    RegisterCommonSecretParameters(oauth2_function);
    
    // Register the config secret provider
    duckdb::CreateSecretFunction config_function = {type, "config", CreateDatasphereSecretFromConfig, {}};
    config_function.named_parameters["config_file"] = duckdb::LogicalType::VARCHAR;
    RegisterCommonSecretParameters(config_function);
    
    // Register the file secret provider
    duckdb::CreateSecretFunction file_function = {type, "file", CreateDatasphereSecretFromFile, {}};
    file_function.named_parameters["filepath"] = duckdb::LogicalType::VARCHAR;
    RegisterCommonSecretParameters(file_function);
    
    duckdb::ExtensionUtil::RegisterSecretType(db, secret_type);
    duckdb::ExtensionUtil::RegisterFunction(db, oauth2_function);
    duckdb::ExtensionUtil::RegisterFunction(db, config_function);
    duckdb::ExtensionUtil::RegisterFunction(db, file_function);
}

duckdb::unique_ptr<duckdb::BaseSecret> CreateDatasphereSecretFunctions::CreateDatasphereSecretFromOAuth2(
    duckdb::ClientContext &context, 
    duckdb::CreateSecretInput &input) {
    
    auto scope = input.scope;
    auto result = duckdb::make_uniq<duckdb::KeyValueSecret>(scope, input.type, input.provider, input.name);
    
    // Copy OAuth2 configuration parameters
    auto copy_param = [&](const std::string &key) {
        auto val = input.options.find(key);
        if (val != input.options.end()) {
            result->secret_map[key] = val->second;
        }
    };
    
    copy_param("client_id");
    copy_param("client_secret");
    copy_param("tenant_name");
    copy_param("data_center");
    copy_param("scope");
    copy_param("redirect_uri");
    
    // Set default values if not provided
    if (result->secret_map.find("scope") == result->secret_map.end()) {
        result->secret_map["scope"] = duckdb::Value("default");
    }
    if (result->secret_map.find("redirect_uri") == result->secret_map.end()) {
        result->secret_map["redirect_uri"] = duckdb::Value("http://localhost:65000");
    }
    
    // Redact sensitive keys
    RedactCommonKeys(*result);
    result->redact_keys.insert("client_secret");
    
    return std::move(result);
}

duckdb::unique_ptr<duckdb::BaseSecret> CreateDatasphereSecretFunctions::CreateDatasphereSecretFromConfig(
    duckdb::ClientContext &context, 
    duckdb::CreateSecretInput &input) {
    
    auto scope = input.scope;
    auto result = duckdb::make_uniq<duckdb::KeyValueSecret>(scope, input.type, input.provider, input.name);
    
    // Get config file path
    auto config_file_val = input.options.find("config_file");
    if (config_file_val == input.options.end()) {
        throw duckdb::InvalidInputException("'config_file' parameter is required for config provider");
    }
    
    std::string config_file = config_file_val->second.ToString();
    
    // Read and parse config file
    std::ifstream ifs(config_file);
    if (!ifs.is_open()) {
        throw duckdb::IOException("Could not open config file at: " + config_file);
    }
    
    // For now, implement basic config file parsing
    // This can be enhanced to support JSON, INI, or other formats
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.empty() || line[0] == '#') continue;
        
        size_t pos = line.find('=');
        if (pos != std::string::npos) {
            std::string key = line.substr(0, pos);
            std::string value = line.substr(pos + 1);
            
            // Trim whitespace
            key.erase(0, key.find_first_not_of(" \t"));
            key.erase(key.find_last_not_of(" \t") + 1);
            value.erase(0, value.find_first_not_of(" \t"));
            value.erase(value.find_last_not_of(" \t") + 1);
            
            result->secret_map[key] = duckdb::Value(value);
        }
    }
    
    // Redact sensitive keys
    RedactCommonKeys(*result);
    result->redact_keys.insert("client_secret");
    
    return std::move(result);
}

duckdb::unique_ptr<duckdb::BaseSecret> CreateDatasphereSecretFunctions::CreateDatasphereSecretFromFile(
    duckdb::ClientContext &context, 
    duckdb::CreateSecretInput &input) {
    
    auto scope = input.scope;
    auto result = duckdb::make_uniq<duckdb::KeyValueSecret>(scope, input.type, input.provider, input.name);
    
    // Get file path
    auto filepath_val = input.options.find("filepath");
    if (filepath_val == input.options.end()) {
        throw duckdb::InvalidInputException("'filepath' parameter is required for file provider");
    }
    
    std::string filepath = filepath_val->second.ToString();
    
    // Store filepath for reference
    result->secret_map["filepath"] = duckdb::Value(filepath);
    
    // For now, just store the filepath
    // In a full implementation, you might want to read and parse the file
    // similar to how gsheets handles service account JSON files
    
    // Redact sensitive keys
    RedactCommonKeys(*result);
    result->redact_keys.insert("filepath");
    
    return std::move(result);
}

void CreateDatasphereSecretFunctions::RegisterCommonSecretParameters(duckdb::CreateSecretFunction &function) {
    // Register common parameters for all Datasphere secret providers
    function.named_parameters["name"] = duckdb::LogicalType::VARCHAR;
    function.named_parameters["scope"] = duckdb::LogicalType::VARCHAR;
}

void CreateDatasphereSecretFunctions::RedactCommonKeys(duckdb::KeyValueSecret &result) {
    // Redact common sensitive keys
    result.redact_keys.insert("client_secret");
    result.redact_keys.insert("access_token");
    result.redact_keys.insert("refresh_token");
}

// Token management implementation
std::string DatasphereTokenManager::GetToken(duckdb::ClientContext &context, const duckdb::KeyValueSecret *kv_secret) {
    // Check if we have a valid cached token
    if (HasValidCachedToken(kv_secret)) {
        return GetCachedToken(kv_secret);
    }
    
    // Perform OAuth2 flow to get new tokens
    OAuth2Tokens new_tokens = PerformOAuth2Flow(context, kv_secret);
    
    // Update the secret with new tokens
    UpdateSecretWithTokens(context, kv_secret, new_tokens);
    
    return new_tokens.access_token;
}

void DatasphereTokenManager::RefreshTokens(duckdb::ClientContext &context, const duckdb::KeyValueSecret *kv_secret) {
    // Perform OAuth2 flow to get new tokens
    OAuth2Tokens new_tokens = PerformOAuth2Flow(context, kv_secret);
    
    // Update the secret with new tokens
    UpdateSecretWithTokens(context, kv_secret, new_tokens);
}

bool DatasphereTokenManager::IsTokenValid(const duckdb::KeyValueSecret *kv_secret) {
    return HasValidCachedToken(kv_secret);
}

void DatasphereTokenManager::UpdateSecretWithTokens(
    duckdb::ClientContext &context, 
    const duckdb::KeyValueSecret *kv_secret,
    const OAuth2Tokens &tokens) {
    
    // Get the secret manager
    auto &secret_manager = duckdb::SecretManager::Get(context);
    auto secret_name = kv_secret->GetName();
    
    // Get the current secret to preserve metadata
    auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
    auto old_secret = secret_manager.GetSecretByName(transaction, secret_name);
    auto persist_type = old_secret->persist_type;
    auto storage_mode = old_secret->storage_mode;
    
    // Clone the old secret to get metadata
    auto new_secret = old_secret->secret->Clone();
    auto new_secret_kv = dynamic_cast<const duckdb::KeyValueSecret*>(new_secret.get());
    
    if (!new_secret_kv) {
        throw duckdb::InvalidInputException("Failed to clone secret as KeyValueSecret");
    }
    
    // Create a new KeyValueSecret by copying the existing one
    duckdb::KeyValueSecret updated_secret(*new_secret_kv);
    
    // Add the new tokens
    updated_secret.secret_map["access_token"] = duckdb::Value(tokens.access_token);
    updated_secret.secret_map["refresh_token"] = duckdb::Value(tokens.refresh_token);
    updated_secret.secret_map["expires_at"] = duckdb::Value(std::to_string(tokens.expires_after));
    updated_secret.secret_map["token_type"] = duckdb::Value(tokens.token_type);
    updated_secret.secret_map["scope"] = duckdb::Value(tokens.scope);
    
    // Register the updated secret
    secret_manager.RegisterSecret(transaction, duckdb::make_uniq<duckdb::KeyValueSecret>(updated_secret),
                                  duckdb::OnCreateConflict::REPLACE_ON_CONFLICT, persist_type, storage_mode);
}

bool DatasphereTokenManager::HasValidCachedToken(const duckdb::KeyValueSecret *kv_secret) {
    // Check if token exists
    auto token_val = kv_secret->secret_map.find("access_token");
    if (token_val == kv_secret->secret_map.end()) {
        return false;
    }
    
    // Check if token is expired
    auto expires_at_val = kv_secret->secret_map.find("expires_at");
    if (expires_at_val == kv_secret->secret_map.end()) {
        return false;
    }
    
    try {
        std::time_t expiration_time = std::stoll(expires_at_val->second.ToString());
        std::time_t current_time = std::time(nullptr);
        return expiration_time > current_time;
    } catch (...) {
        return false;
    }
}

std::string DatasphereTokenManager::GetCachedToken(const duckdb::KeyValueSecret *kv_secret) {
    auto token_val = kv_secret->secret_map.find("access_token");
    if (token_val == kv_secret->secret_map.end()) {
        throw duckdb::InvalidInputException("'access_token' not found in 'datasphere' secret");
    }
    return token_val->second.ToString();
}

OAuth2Tokens DatasphereTokenManager::PerformOAuth2Flow(duckdb::ClientContext &context, const duckdb::KeyValueSecret *kv_secret) {
    // Extract OAuth2 configuration from secret
    auto client_id_val = kv_secret->secret_map.find("client_id");
    auto client_secret_val = kv_secret->secret_map.find("client_secret");
    auto tenant_name_val = kv_secret->secret_map.find("tenant_name");
    auto data_center_val = kv_secret->secret_map.find("data_center");
    auto scope_val = kv_secret->secret_map.find("scope");
    auto redirect_uri_val = kv_secret->secret_map.find("redirect_uri");
    
    if (client_id_val == kv_secret->secret_map.end()) {
        throw duckdb::InvalidInputException("'client_id' not found in 'datasphere' secret");
    }
    if (client_secret_val == kv_secret->secret_map.end()) {
        throw duckdb::InvalidInputException("'client_secret' not found in 'datasphere' secret");
    }
    if (tenant_name_val == kv_secret->secret_map.end()) {
        throw duckdb::InvalidInputException("'tenant_name' not found in 'datasphere' secret");
    }
    if (data_center_val == kv_secret->secret_map.end()) {
        throw duckdb::InvalidInputException("'data_center' not found in 'datasphere' secret");
    }
    
    // Create OAuth2 config
    OAuth2Config config;
    config.client_id = client_id_val->second.ToString();
    config.client_secret = client_secret_val->second.ToString();
    config.tenant_name = tenant_name_val->second.ToString();
    config.data_center = data_center_val->second.ToString();
    // Optional fields with sensible defaults
    config.scope = (scope_val != kv_secret->secret_map.end()) ? scope_val->second.ToString() : std::string("default");
    config.redirect_uri = (redirect_uri_val != kv_secret->secret_map.end()) ? redirect_uri_val->second.ToString() : std::string("http://localhost:65000");
    
    // Perform the OAuth2 flow using the clean implementation
    OAuth2FlowV2 oauth2_flow;
    return oauth2_flow.ExecuteFlow(config);
}

} // namespace erpl_web
