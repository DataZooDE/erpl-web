#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"

#include "odata_client.hpp"
#include "odata_edm.hpp"
#include "odata_storage.hpp"

using namespace duckdb;

namespace erpl_web {

/**
 * SAC Storage Extension Bind Data
 * Stores metadata and configuration for attached SAC instances
 * Reuses ODataAttachBindData pattern
 */
class SacAttachBindData : public TableFunctionData {
public:
    static duckdb::unique_ptr<SacAttachBindData> FromUrl(
        const std::string& url,
        std::shared_ptr<HttpAuthParams> auth_params);

public:
    SacAttachBindData(std::shared_ptr<ODataServiceClient> odata_client);

    bool IsFinished() const;
    void SetFinished();

    std::vector<std::string> IgnorePatterns() const;
    void IgnorePatterns(const std::vector<std::string>& ignore);

    bool Overwrite() const;
    void SetOverwrite(bool overwrite);

    std::vector<ODataEntitySetReference> EntitySets();

    static bool MatchPattern(const std::string& str, const std::string& ignore_pattern);
    static bool MatchPattern(const std::string& str, const std::vector<std::string>& ignore_patterns);

private:
    bool finished = false;
    bool overwrite = false;
    std::shared_ptr<ODataServiceClient> odata_client;
    std::vector<std::string> ignore_patterns;
};

/**
 * Create the SAC ATTACH table function
 * Enables:
 * ATTACH 'https://tenant.region.sapanalytics.cloud' AS sac (TYPE sac, SECRET sac_secret)
 *
 * This follows the OData ATTACH pattern but with SAC-specific URL handling
 */
TableFunctionSet CreateSacAttachFunction();

/**
 * SAC Storage Extension
 * Extends DuckDB's StorageExtension for SAC OData support
 * Reuses ODataStorageExtension pattern
 */
class SacStorageExtension : public duckdb::StorageExtension {
public:
    SacStorageExtension();
};

/**
 * Create SAC Storage Extension instance
 */
duckdb::unique_ptr<SacStorageExtension> CreateSacStorageExtension();

} // namespace erpl_web
