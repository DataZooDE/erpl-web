#pragma once

#include "duckdb/main/secret/secret_manager.hpp"
#include "http_client.hpp"
#include <chrono>
#include <optional>

namespace erpl_web {

// Microsoft Entra ID (Azure AD) authentication secret data structure
struct MicrosoftEntraSecretData {
    std::string tenant_id;          // Azure AD tenant ID (GUID or domain)
    std::string client_id;          // Application (client) ID
    std::string client_secret;      // Client secret
    std::string scope;              // API scopes (e.g., "https://graph.microsoft.com/.default")
    std::string access_token;       // OAuth2 access token
    std::string refresh_token;      // OAuth2 refresh token (for authorization_code flow)
    std::string expires_at;         // Token expiration timestamp (Unix epoch)
    std::string redirect_uri;       // Redirect URI for authorization_code flow
    std::string grant_type;         // "client_credentials" or "authorization_code"

    // Token management
    bool HasValidToken() const;
    bool IsTokenExpired() const;
    std::chrono::time_point<std::chrono::system_clock> GetExpirationTime() const;
};

// Secret creation functions for Microsoft Entra ID
class CreateMicrosoftEntraSecretFunctions {
public:
    static void Register(duckdb::ExtensionLoader &loader);

private:
    // Secret creation from different providers
    static duckdb::unique_ptr<duckdb::BaseSecret> CreateMicrosoftEntraSecretFromClientCredentials(
        duckdb::ClientContext &context,
        duckdb::CreateSecretInput &input
    );

    static duckdb::unique_ptr<duckdb::BaseSecret> CreateMicrosoftEntraSecretFromConfig(
        duckdb::ClientContext &context,
        duckdb::CreateSecretInput &input
    );

    // Helper functions
    static void RegisterCommonSecretParameters(duckdb::CreateSecretFunction &function);
    static void RedactCommonKeys(duckdb::KeyValueSecret &result);
};

// Token management for Microsoft Entra ID
class MicrosoftEntraTokenManager {
public:
    // Get token from secret (with automatic refresh if needed)
    static std::string GetToken(duckdb::ClientContext &context, const duckdb::KeyValueSecret *kv_secret);

    // Refresh tokens and update secret
    static void RefreshTokens(duckdb::ClientContext &context, const duckdb::KeyValueSecret *kv_secret);

    // Check if token is valid
    static bool IsTokenValid(const duckdb::KeyValueSecret *kv_secret);

    // Get token URL for a tenant
    static std::string GetTokenUrl(const std::string &tenant_id);

    // Get authorization URL for a tenant
    static std::string GetAuthorizationUrl(const std::string &tenant_id);

private:
    // Token acquisition methods
    static std::string AcquireTokenWithClientCredentials(
        const std::string &tenant_id,
        const std::string &client_id,
        const std::string &client_secret,
        const std::string &scope
    );

    // Helper functions
    static bool HasValidCachedToken(const duckdb::KeyValueSecret *kv_secret);
    static std::string GetCachedToken(const duckdb::KeyValueSecret *kv_secret);

    // Update secret with new tokens
    static void UpdateSecretWithTokens(
        duckdb::ClientContext &context,
        const duckdb::KeyValueSecret *kv_secret,
        const std::string &access_token,
        int expires_in
    );
};

// Unified auth retrieval utilities for Microsoft services
struct MicrosoftEntraAuthInfo {
    std::string tenant_id;
    std::string access_token;
    std::shared_ptr<HttpAuthParams> auth_params;
};

// Returns a cloned KeyValueSecret for a given secret name (throws on errors)
duckdb::unique_ptr<duckdb::KeyValueSecret> GetMicrosoftEntraKeyValueSecret(
    duckdb::ClientContext &context,
    const std::string &secret_name = "microsoft_entra"
);

// Resolves tenant_id and access_token, and prepares HttpAuthParams
MicrosoftEntraAuthInfo ResolveMicrosoftEntraAuth(
    duckdb::ClientContext &context,
    const std::string &secret_name = "microsoft_entra"
);

} // namespace erpl_web
