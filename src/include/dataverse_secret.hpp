#pragma once

#include "duckdb.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "microsoft_entra_secret.hpp"
#include "http_client.hpp"
#include <string>
#include <memory>

namespace erpl_web {

// Secret management for Dataverse/CRM authentication
class CreateDataverseSecretFunctions {
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

// Authentication info resolved from a Dataverse secret
struct DataverseAuthInfo {
    std::string environment_url;  // e.g., https://myorg.crm.dynamics.com
    std::string access_token;
    std::shared_ptr<HttpAuthParams> auth_params;
};

// Resolve authentication from a Dataverse secret
DataverseAuthInfo ResolveDataverseAuth(
    duckdb::ClientContext &context,
    const std::string &secret_name);

// Helper to get a KeyValueSecret by name
duckdb::unique_ptr<duckdb::KeyValueSecret> GetDataverseKeyValueSecret(
    duckdb::ClientContext &context,
    const std::string &secret_name);

} // namespace erpl_web
