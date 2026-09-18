#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace erpl_web {

// Registers the `sac` secret type.
//
// SAC's own helper has always told users to run
//   CREATE SECRET <name> (type 'sac', provider 'oauth2', ...)
// but no such type was ever registered, so that statement could not parse and every SAC
// function was reachable only with a hand-built secret - which then went out with no
// Authorization header at all. See GitHub #244.
class CreateSacSecretFunctions {
public:
    static void Register(duckdb::ExtensionLoader &loader);

private:
    static duckdb::unique_ptr<duckdb::BaseSecret> CreateFromAccessToken(duckdb::ClientContext &context,
                                                                       duckdb::CreateSecretInput &input);
    static duckdb::unique_ptr<duckdb::BaseSecret> CreateFromConfig(duckdb::ClientContext &context,
                                                                   duckdb::CreateSecretInput &input);
    static void RegisterCommonSecretParameters(duckdb::CreateSecretFunction &function);
};

} // namespace erpl_web
