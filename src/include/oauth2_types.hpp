#pragma once

#include <string>
#include <chrono>

namespace erpl_web {

// OAuth2 grant types
enum class GrantType {
    authorization_code,
    client_credentials,
    refresh_token
};

// OAuth2 client types
enum class OAuth2ClientType {
    pre_delivered,
    custom
};

// OAuth2 configuration structure (supports SAP and Microsoft identity platforms)
struct OAuth2Config {
    std::string tenant_name;
    std::string data_center;
    std::string client_id;
    std::string client_secret;
    std::string scope;
    std::string redirect_uri;
    GrantType authorization_flow;
    bool custom_client;          // Whether this is a custom OAuth client

    // Custom URL overrides (for non-SAP identity providers like Microsoft Entra)
    std::string custom_auth_url;   // If set, overrides GetAuthorizationUrl()
    std::string custom_token_url;  // If set, overrides GetTokenUrl()

    // Constructor to properly initialize fields
    OAuth2Config() :
        tenant_name(""),
        data_center(""),
        client_id(""),
        client_secret(""),
        scope(""),
        redirect_uri(""),
        authorization_flow(GrantType::authorization_code),
        custom_client(false),
        custom_auth_url(""),
        custom_token_url("") {}
    
    // Get authorization URL
    std::string GetAuthorizationUrl() const;
    
    // Get token URL
    std::string GetTokenUrl() const;
    
    // Get default port based on client type
    int GetDefaultPort() const;
    
    // Get client type based on client ID
    OAuth2ClientType GetClientType() const;
};

// OAuth2 tokens structure (matching SAP CLI exactly)
struct OAuth2Tokens {
    std::string access_token;
    std::string refresh_token;
    std::string token_type;
    std::string scope;
    int expires_in;              // Seconds until token expires
    int expires_after;           // Unix timestamp when token expires
    
    // Constructor to properly initialize fields
    OAuth2Tokens() : expires_in(0), expires_after(0) {}
    
    // Check if token is expired
    bool IsExpired() const;
    
    // Check if token needs refresh
    bool NeedsRefresh() const;
    
    // Calculate expires_after based on expires_in
    void CalculateExpiresAfter();
};

// Utility functions for OAuth2 operations
namespace OAuth2Utils {
    // Generate PKCE code verifier
    std::string GenerateCodeVerifier();
    
    // Generate PKCE code challenge from verifier
    std::string GenerateCodeChallenge(const std::string& code_verifier);
    
    // Generate random state parameter
    std::string GenerateState();
    
    // Validate state parameter
    bool ValidateState(const std::string& received_state, const std::string& expected_state);
}

} // namespace erpl_web
