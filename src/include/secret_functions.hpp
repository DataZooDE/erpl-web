#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"


namespace erpl {

struct CreateBasicSecretFunctions {
    static void Register(duckdb::DatabaseInstance &instance);

protected:
	static duckdb::unique_ptr<duckdb::BaseSecret> CreateBasicSecretFromConfig(duckdb::ClientContext &context, 
                                                                              duckdb::CreateSecretInput &input);
};

struct CreateBearerTokenSecretFunctions {
    static void Register(duckdb::DatabaseInstance &instance);

protected:
	static duckdb::unique_ptr<duckdb::BaseSecret> CreateBearerSecretFromConfig(duckdb::ClientContext &context, 
                                                                               duckdb::CreateSecretInput &input);
};

}
