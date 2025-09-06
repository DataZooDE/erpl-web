#include "secret_functions.hpp"
#include "tracing.hpp"

using namespace duckdb;

namespace erpl {

void CreateBasicSecretFunctions::Register(duckdb::DatabaseInstance &instance) 
{
    ERPL_TRACE_INFO("SECRET_FUNCTIONS", "Registering HTTP Basic authentication secret functions");
    
    std::string type = "http_basic";
	SecretType secret_type;
	secret_type.name = type;
	secret_type.deserializer = KeyValueSecret::Deserialize<duckdb::KeyValueSecret>;
	secret_type.default_provider = "config";

	ExtensionUtil::RegisterSecretType(instance, secret_type);

	CreateSecretFunction secret_fun = {type, "config", CreateBasicSecretFromConfig};
	secret_fun.named_parameters["username"] = LogicalType::VARCHAR;
    secret_fun.named_parameters["password"] = LogicalType::VARCHAR;
	ExtensionUtil::RegisterFunction(instance, secret_fun);
    
    ERPL_TRACE_INFO("SECRET_FUNCTIONS", "Successfully registered HTTP Basic authentication secret functions");
}

unique_ptr<BaseSecret> CreateBasicSecretFunctions::CreateBasicSecretFromConfig(ClientContext &context, CreateSecretInput &input) 
{
    ERPL_TRACE_DEBUG("SECRET_BASIC", "Creating HTTP Basic authentication secret");
    
    auto scope = input.scope;
    if (scope.empty()) {
        scope.push_back("https://");
        ERPL_TRACE_DEBUG("SECRET_BASIC", "Using default scope: https://");
    } else {
        ERPL_TRACE_DEBUG("SECRET_BASIC", "Using custom scope with " + std::to_string(scope.size()) + " entries");
    }

    // Create the secret
    auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
    secret->redact_keys = {"username", "password"};

    // Apply any overridden settings
    for (const auto &named_param : input.options) {
        auto lower_name = StringUtil::Lower(named_param.first);

        if (lower_name == "username") {
            secret->secret_map["username"] = named_param.second;
            ERPL_TRACE_DEBUG("SECRET_BASIC", "Set username parameter");
        } else if (lower_name == "password") {
            secret->secret_map["password"] = named_param.second;
            ERPL_TRACE_DEBUG("SECRET_BASIC", "Set password parameter");
        } else {
            ERPL_TRACE_ERROR("SECRET_BASIC", "Unknown named parameter: " + lower_name);
            throw InternalException("Unknown named parameter passed to CreateBasicSecretFromConfig: " + lower_name);
        }
    }
    
    ERPL_TRACE_INFO("SECRET_BASIC", "Successfully created HTTP Basic authentication secret");
    return std::move(secret);
}

// ----------------------------------------------------------------------

void CreateBearerTokenSecretFunctions::Register(duckdb::DatabaseInstance &instance) {   
    ERPL_TRACE_INFO("SECRET_FUNCTIONS", "Registering HTTP Bearer token secret functions");
    
    std::string type = "http_bearer";
	SecretType secret_type;
	secret_type.name = type;
	secret_type.deserializer = KeyValueSecret::Deserialize<duckdb::KeyValueSecret>;
	secret_type.default_provider = "config";

	ExtensionUtil::RegisterSecretType(instance, secret_type);

	CreateSecretFunction secret_fun = {type, "config", CreateBearerSecretFromConfig};
	secret_fun.named_parameters["token"] = LogicalType::VARCHAR;
	ExtensionUtil::RegisterFunction(instance, secret_fun);
    
    ERPL_TRACE_INFO("SECRET_FUNCTIONS", "Successfully registered HTTP Bearer token secret functions");
}

unique_ptr<BaseSecret> CreateBearerTokenSecretFunctions::CreateBearerSecretFromConfig(ClientContext &context, CreateSecretInput &input) {
    ERPL_TRACE_DEBUG("SECRET_BEARER", "Creating HTTP Bearer token secret");
    
    auto scope = input.scope;
    if (scope.empty()) {
        scope.push_back("https://");
        ERPL_TRACE_DEBUG("SECRET_BEARER", "Using default scope: https://");
    } else {
        ERPL_TRACE_DEBUG("SECRET_BEARER", "Using custom scope with " + std::to_string(scope.size()) + " entries");
    }

    // Create the secret
    auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
    secret->redact_keys = {"token"};    

    // Apply any overridden settings
    for (const auto &named_param : input.options) {
        auto lower_name = StringUtil::Lower(named_param.first);

        if (lower_name == "token") {
            secret->secret_map["token"] = named_param.second;
            ERPL_TRACE_DEBUG("SECRET_BEARER", "Set token parameter");
        } else {
            ERPL_TRACE_ERROR("SECRET_BEARER", "Unknown named parameter: " + lower_name);
            throw InternalException("Unknown named parameter passed to CreateBearerSecretFromConfig: " + lower_name);
        }
    }   
    
    ERPL_TRACE_INFO("SECRET_BEARER", "Successfully created HTTP Bearer token secret");
    return std::move(secret);
}

}