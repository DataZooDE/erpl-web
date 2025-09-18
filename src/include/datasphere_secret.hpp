#pragma once

#include "duckdb/main/secret/secret_manager.hpp"
#include "oauth2_flow_v2.hpp"
#include "http_client.hpp"

namespace erpl_web {

// Datasphere secret data structure (matching gsheets pattern)
struct DatasphereSecretData {
    std::string client_id;
    std::string client_secret;
    std::string tenant_name;
    std::string data_center;
    std::string access_token;
    std::string refresh_token;
    std::string expires_at;
    std::string scope;
    std::string redirect_uri;
    
    // OAuth2 configuration
    std::string authorization_url;
    std::string token_url;
    bool custom_client;
    
    // Token management
    bool HasValidToken() const;
    bool IsTokenExpired() const;
    std::chrono::time_point<std::chrono::system_clock> GetExpirationTime() const;
};

// Secret creation functions (matching gsheets pattern)
class CreateDatasphereSecretFunctions {
public:
    static void Register(duckdb::ExtensionLoader &loader);
    
private:
    // Secret creation from different providers
    static duckdb::unique_ptr<duckdb::BaseSecret> CreateDatasphereSecretFromOAuth2(
        duckdb::ClientContext &context, 
        duckdb::CreateSecretInput &input
    );
    
    static duckdb::unique_ptr<duckdb::BaseSecret> CreateDatasphereSecretFromConfig(
        duckdb::ClientContext &context, 
        duckdb::CreateSecretInput &input
    );
    
    static duckdb::unique_ptr<duckdb::BaseSecret> CreateDatasphereSecretFromFile(
        duckdb::ClientContext &context, 
        duckdb::CreateSecretInput &input
    );
    
    // Helper functions
    static void RegisterCommonSecretParameters(duckdb::CreateSecretFunction &function);
    static void RedactCommonKeys(duckdb::KeyValueSecret &result);
};

// Token management functions (matching gsheets pattern)
class DatasphereTokenManager {
public:
    // Get token from secret (with automatic refresh if needed)
    static std::string GetToken(duckdb::ClientContext &context, const duckdb::KeyValueSecret *kv_secret);
    
    // Refresh tokens and update secret
    static void RefreshTokens(duckdb::ClientContext &context, const duckdb::KeyValueSecret *kv_secret);
    
    // Check if token is valid
    static bool IsTokenValid(const duckdb::KeyValueSecret *kv_secret);
    
    // Update secret with new tokens
    static void UpdateSecretWithTokens(
        duckdb::ClientContext &context, 
        const duckdb::KeyValueSecret *kv_secret,
        const OAuth2Tokens &tokens
    );
    
private:
    // Helper functions
    static bool HasValidCachedToken(const duckdb::KeyValueSecret *kv_secret);
    static std::string GetCachedToken(const duckdb::KeyValueSecret *kv_secret);
    static OAuth2Tokens PerformOAuth2Flow(duckdb::ClientContext &context, const duckdb::KeyValueSecret *kv_secret);
};

// Unified auth retrieval utilities
struct DatasphereAuthInfo {
    std::string tenant_name;
    std::string data_center;
    std::string access_token;
    std::shared_ptr<HttpAuthParams> auth_params;
};

// Returns a cloned KeyValueSecret for a given secret name (throws on errors)
duckdb::unique_ptr<duckdb::KeyValueSecret> GetDatasphereKeyValueSecret(duckdb::ClientContext &context, const std::string &secret_name = "datasphere");

// Resolves tenant, data_center and access_token, and prepares HttpAuthParams
DatasphereAuthInfo ResolveDatasphereAuth(duckdb::ClientContext &context, const std::string &secret_name = "datasphere");

} // namespace erpl_web
