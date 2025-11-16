#include "sac_secret_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace erpl_web {

SacSecretData ResolveSacSecretData(duckdb::ClientContext& context, const std::string& secret_name) {
    auto& secret_manager = duckdb::SecretManager::Get(context);
    auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
    std::unique_ptr<duckdb::SecretEntry> secret_entry;

    try {
        secret_entry = secret_manager.GetSecretByName(transaction, secret_name);
    } catch (...) {
        secret_entry = nullptr;
    }

    if (!secret_entry) {
        throw duckdb::InvalidInputException("Secret '" + secret_name + "' not found. Please create it using CREATE SECRET " +
            secret_name + " (type 'sac', provider 'oauth2', tenant_name => '...', region => '...', " +
            "client_id => '...', client_secret => '...', scope => 'openid');");
    }

    auto kv_secret = dynamic_cast<const duckdb::KeyValueSecret*>(secret_entry->secret.get());
    if (!kv_secret) {
        throw duckdb::InvalidInputException("Secret '" + secret_name + "' is not a KeyValueSecret");
    }

    // Extract tenant and region
    auto tenant_it = kv_secret->secret_map.find("tenant_name");
    auto region_it = kv_secret->secret_map.find("region");
    auto access_token_it = kv_secret->secret_map.find("access_token");

    if (tenant_it == kv_secret->secret_map.end() || region_it == kv_secret->secret_map.end()) {
        throw duckdb::InvalidInputException("SAC secret must contain 'tenant_name' and 'region' fields");
    }

    SacSecretData secret_data;
    secret_data.tenant = tenant_it->second.ToString();
    secret_data.region = region_it->second.ToString();
    secret_data.auth_params = std::make_shared<HttpAuthParams>();

    // Note: Full OAuth2 implementation would handle token refreshing for access_token field
    if (access_token_it != kv_secret->secret_map.end()) {
        // Token will be used if available
    }

    return secret_data;
}

} // namespace erpl_web
