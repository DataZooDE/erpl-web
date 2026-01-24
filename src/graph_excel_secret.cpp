#include "graph_excel_secret.hpp"
#include "tracing.hpp"
#include "duckdb/common/exception.hpp"

namespace erpl_web {

void CreateGraphSecretFunctions::Register(duckdb::ExtensionLoader &loader) {
    ERPL_TRACE_INFO("GRAPH_SECRET", "Registering Microsoft Graph secret functions");

    std::string type = "microsoft_graph";

    // Register the secret type
    duckdb::SecretType secret_type;
    secret_type.name = type;
    secret_type.deserializer = duckdb::KeyValueSecret::Deserialize<duckdb::KeyValueSecret>;
    secret_type.default_provider = "client_credentials";

    // Register the client_credentials provider
    duckdb::CreateSecretFunction client_creds_function = {type, "client_credentials", CreateFromClientCredentials, {}};
    client_creds_function.named_parameters["tenant_id"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    client_creds_function.named_parameters["client_id"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    client_creds_function.named_parameters["client_secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    client_creds_function.named_parameters["scope"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    RegisterCommonSecretParameters(client_creds_function);

    // Register the config provider
    duckdb::CreateSecretFunction config_function = {type, "config", CreateFromConfig, {}};
    config_function.named_parameters["tenant_id"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    config_function.named_parameters["client_id"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    config_function.named_parameters["client_secret"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    config_function.named_parameters["access_token"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    config_function.named_parameters["refresh_token"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    config_function.named_parameters["expires_at"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    config_function.named_parameters["scope"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
    RegisterCommonSecretParameters(config_function);

    loader.RegisterSecretType(secret_type);
    loader.RegisterFunction(client_creds_function);
    loader.RegisterFunction(config_function);

    ERPL_TRACE_INFO("GRAPH_SECRET", "Successfully registered Microsoft Graph secret functions");
}

duckdb::unique_ptr<duckdb::BaseSecret> CreateGraphSecretFunctions::CreateFromClientCredentials(
    duckdb::ClientContext &context,
    duckdb::CreateSecretInput &input) {

    ERPL_TRACE_DEBUG("GRAPH_SECRET", "Creating Microsoft Graph secret with client_credentials provider");

    auto scope = input.scope;
    auto result = duckdb::make_uniq<duckdb::KeyValueSecret>(scope, input.type, input.provider, input.name);

    // Copy parameters
    auto copy_param = [&](const std::string &key) {
        auto val = input.options.find(key);
        if (val != input.options.end()) {
            result->secret_map[key] = val->second;
            ERPL_TRACE_DEBUG("GRAPH_SECRET", "Set parameter: " + key);
        }
    };

    copy_param("tenant_id");
    copy_param("client_id");
    copy_param("client_secret");

    // Validate required parameters
    if (result->secret_map.find("tenant_id") == result->secret_map.end()) {
        throw duckdb::InvalidInputException("'tenant_id' is required for Microsoft Graph authentication");
    }
    if (result->secret_map.find("client_id") == result->secret_map.end()) {
        throw duckdb::InvalidInputException("'client_id' is required for Microsoft Graph authentication");
    }
    if (result->secret_map.find("client_secret") == result->secret_map.end()) {
        throw duckdb::InvalidInputException("'client_secret' is required for Microsoft Graph authentication");
    }

    // Set the scope for Microsoft Graph API (use custom scope if provided, otherwise default)
    auto scope_val = input.options.find("scope");
    if (scope_val != input.options.end()) {
        result->secret_map["scope"] = scope_val->second;
    } else {
        // Default scope for Microsoft Graph
        result->secret_map["scope"] = duckdb::Value("https://graph.microsoft.com/.default");
    }

    // Store the grant type
    result->secret_map["grant_type"] = duckdb::Value("client_credentials");

    // Redact sensitive keys
    RedactCommonKeys(*result);

    ERPL_TRACE_INFO("GRAPH_SECRET", "Successfully created Microsoft Graph secret");
    return std::move(result);
}

duckdb::unique_ptr<duckdb::BaseSecret> CreateGraphSecretFunctions::CreateFromConfig(
    duckdb::ClientContext &context,
    duckdb::CreateSecretInput &input) {

    ERPL_TRACE_DEBUG("GRAPH_SECRET", "Creating Microsoft Graph secret with config provider");

    auto scope = input.scope;
    auto result = duckdb::make_uniq<duckdb::KeyValueSecret>(scope, input.type, input.provider, input.name);

    // Copy all provided parameters
    auto copy_param = [&](const std::string &key) {
        auto val = input.options.find(key);
        if (val != input.options.end()) {
            result->secret_map[key] = val->second;
        }
    };

    copy_param("tenant_id");
    copy_param("client_id");
    copy_param("client_secret");
    copy_param("access_token");
    copy_param("refresh_token");
    copy_param("expires_at");
    copy_param("scope");

    // Set default scope if not provided
    if (result->secret_map.find("scope") == result->secret_map.end()) {
        result->secret_map["scope"] = duckdb::Value("https://graph.microsoft.com/.default");
    }

    // Redact sensitive keys
    RedactCommonKeys(*result);

    ERPL_TRACE_INFO("GRAPH_SECRET", "Successfully created Microsoft Graph config secret");
    return std::move(result);
}

void CreateGraphSecretFunctions::RegisterCommonSecretParameters(duckdb::CreateSecretFunction &function) {
    function.named_parameters["name"] = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
}

void CreateGraphSecretFunctions::RedactCommonKeys(duckdb::KeyValueSecret &result) {
    result.redact_keys.insert("client_secret");
    result.redact_keys.insert("access_token");
    result.redact_keys.insert("refresh_token");
}

duckdb::unique_ptr<duckdb::KeyValueSecret> GetGraphKeyValueSecret(
    duckdb::ClientContext &context,
    const std::string &secret_name) {

    auto &secret_manager = duckdb::SecretManager::Get(context);
    auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
    auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name);

    if (!secret_entry) {
        throw duckdb::InvalidInputException("Microsoft Graph secret '" + secret_name + "' not found. Use CREATE SECRET to create it.");
    }

    auto kv_secret = dynamic_cast<const duckdb::KeyValueSecret *>(secret_entry->secret.get());
    if (!kv_secret) {
        throw duckdb::InvalidInputException("Secret '" + secret_name + "' is not a KeyValueSecret");
    }

    // Clone to extend lifetime beyond SecretEntry unique_ptr
    return duckdb::make_uniq<duckdb::KeyValueSecret>(*kv_secret);
}

GraphAuthInfo ResolveGraphAuth(
    duckdb::ClientContext &context,
    const std::string &secret_name) {

    ERPL_TRACE_DEBUG("GRAPH_AUTH", "Resolving Microsoft Graph authentication for secret: " + secret_name);

    auto kv_secret_up = GetGraphKeyValueSecret(context, secret_name);
    const auto *kv_secret = kv_secret_up.get();

    // Get access token using Microsoft Entra token manager
    // The Graph secret has the same structure as Entra secret, so we can reuse the token manager
    std::string access_token = MicrosoftEntraTokenManager::GetToken(context, kv_secret);

    if (access_token.empty()) {
        throw duckdb::InvalidInputException("Microsoft Graph secret '" + secret_name + "' could not provide a valid access token.");
    }

    auto auth_params = std::make_shared<HttpAuthParams>();
    auth_params->bearer_token = access_token;

    ERPL_TRACE_INFO("GRAPH_AUTH", "Successfully resolved Microsoft Graph authentication");
    return GraphAuthInfo{access_token, auth_params};
}

} // namespace erpl_web
