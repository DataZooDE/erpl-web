#pragma once

#include "microsoft_entra_secret.hpp"

namespace erpl_web {

// Business Central secret creation functions
// Extends Microsoft Entra secret with environment parameter
class CreateBusinessCentralSecretFunctions {
public:
    static void Register(duckdb::ExtensionLoader &loader);

private:
    static duckdb::unique_ptr<duckdb::BaseSecret> CreateFromClientCredentials(
        duckdb::ClientContext &context,
        duckdb::CreateSecretInput &input);

    static duckdb::unique_ptr<duckdb::BaseSecret> CreateFromConfig(
        duckdb::ClientContext &context,
        duckdb::CreateSecretInput &input);

    static void RegisterCommonSecretParameters(duckdb::CreateSecretFunction &function);
    static void RedactCommonKeys(duckdb::KeyValueSecret &result);
};

// Business Central auth info with environment
struct BusinessCentralAuthInfo {
    std::string tenant_id;
    std::string environment;      // "production" or "sandbox"
    std::string access_token;
    std::shared_ptr<HttpAuthParams> auth_params;
};

// Get Business Central KeyValueSecret by name
duckdb::unique_ptr<duckdb::KeyValueSecret> GetBusinessCentralKeyValueSecret(
    duckdb::ClientContext &context,
    const std::string &secret_name);

// Resolve Business Central auth from secret
BusinessCentralAuthInfo ResolveBusinessCentralAuth(
    duckdb::ClientContext &context,
    const std::string &secret_name);

} // namespace erpl_web
