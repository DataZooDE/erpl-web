#include "erpl_secret_functions.hpp"

using namespace duckdb;

namespace erpl {

void CreateBasicSecretFunctions::Register(duckdb::DatabaseInstance &instance) 
{
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
}

unique_ptr<BaseSecret> CreateBasicSecretFunctions::CreateBasicSecretFromConfig(ClientContext &context, CreateSecretInput &input) 
{
    auto scope = input.scope;
    if (scope.empty()) {
        scope.push_back("https://");
    }

    // Create the secret
    auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
    secret->redact_keys = {"username", "password"};

    // Apply any overridden settings
    for (const auto &named_param : input.options) {
        auto lower_name = StringUtil::Lower(named_param.first);

        if (lower_name == "username") {
            secret->secret_map["username"] = named_param.second;
        } else if (lower_name == "password") {
            secret->secret_map["password"] = named_param.second;
        } else {
            throw InternalException("Unknown named parameter passed to CreateBasicSecretFromConfig: " + lower_name);
        }
    }

    return std::move(secret);
}


// ----------------------------------------------------------------------

void CreateBearerTokenSecretFunctions::Register(duckdb::DatabaseInstance &instance) {   
    std::string type = "http_bearer";
	SecretType secret_type;
	secret_type.name = type;
	secret_type.deserializer = KeyValueSecret::Deserialize<duckdb::KeyValueSecret>;
	secret_type.default_provider = "config";

	ExtensionUtil::RegisterSecretType(instance, secret_type);

	CreateSecretFunction secret_fun = {type, "config", CreateBearerSecretFromConfig};
	secret_fun.named_parameters["token"] = LogicalType::VARCHAR;
	ExtensionUtil::RegisterFunction(instance, secret_fun);
}

unique_ptr<BaseSecret> CreateBearerTokenSecretFunctions::CreateBearerSecretFromConfig(ClientContext &context, CreateSecretInput &input) {
    auto scope = input.scope;
    if (scope.empty()) {
        scope.push_back("https://");
    }

    // Create the secret
    auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
    secret->redact_keys = {"token"};    

    // Apply any overridden settings
    for (const auto &named_param : input.options) {
        auto lower_name = StringUtil::Lower(named_param.first);

        if (lower_name == "token") {
            secret->secret_map["token"] = named_param.second;
        } else {
            throw InternalException("Unknown named parameter passed to CreateBearerSecretFromConfig: " + lower_name);
        }
    }   

    return std::move(secret);
}

}