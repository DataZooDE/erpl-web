#pragma once

#include "duckdb.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "microsoft_entra_secret.hpp"
#include "http_client.hpp"
#include <string>
#include <memory>

namespace erpl_web {

// Secret management for Microsoft Graph API authentication
class CreateGraphSecretFunctions {
public:
    static void Register(duckdb::ExtensionLoader &loader);

private:
    static duckdb::unique_ptr<duckdb::BaseSecret> CreateFromClientCredentials(
        duckdb::ClientContext &context,
        duckdb::CreateSecretInput &input);

    static duckdb::unique_ptr<duckdb::BaseSecret> CreateFromConfig(
        duckdb::ClientContext &context,
        duckdb::CreateSecretInput &input);

    static duckdb::unique_ptr<duckdb::BaseSecret> CreateFromAuthorizationCode(
        duckdb::ClientContext &context,
        duckdb::CreateSecretInput &input);

    static void RegisterCommonSecretParameters(duckdb::CreateSecretFunction &function);
    static void RedactCommonKeys(duckdb::KeyValueSecret &result);
};

// Authentication info resolved from a Graph secret
struct GraphAuthInfo {
    std::string access_token;
    std::shared_ptr<HttpAuthParams> auth_params;
};

// Resolve authentication from a Graph secret
GraphAuthInfo ResolveGraphAuth(
    duckdb::ClientContext &context,
    const std::string &secret_name);

// Helper to get a KeyValueSecret by name
duckdb::unique_ptr<duckdb::KeyValueSecret> GetGraphKeyValueSecret(
    duckdb::ClientContext &context,
    const std::string &secret_name);

} // namespace erpl_web
